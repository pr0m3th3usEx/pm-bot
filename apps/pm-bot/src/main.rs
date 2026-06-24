//! pm-bot: supervisor binary.
//! Start-up sequence (§15):
//!   1. Init tracing (first, before anything else)
//!   2. Load keys / secrets
//!   3. Build adapters → clients
//!   4. Wire channels and spawn tasks
//!   5. sleep_until(next window) then begin trading
//!   6. Ctrl-C → cancel token → join → exit

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

use pm_core::{
    clock::MarketClock,
    domain::ActiveMarket,
    ports::{Admission, EntryPolicy},
    state::RoundSlotState,
    strategy::V1BasicStrategy,
    tasks::{
        decision_center::decision_center_task, executor::executor_task, heartbeat::heartbeat_task,
        market_rotation::market_rotation_task, order_status_poller::order_status_poller_task,
        persistence::persistence_task, settlement::settlement_task,
    },
    types::Shares,
};

// ─── V1 fixed sizing model ────────────────────────────────────────────────────

struct FixedSizingModel {
    shares: Shares,
}

impl pm_core::ports::SizingModel for FixedSizingModel {
    fn size(
        &self,
        _bankroll: &pm_core::types::Usdc,
        _limit_price: &pm_core::types::Price,
    ) -> Shares {
        self.shares.clone()
    }
}

// ─── V1 entry policy: max one open position per round ─────────────────────────

struct OnePositionPolicy;

impl EntryPolicy for OnePositionPolicy {
    fn admit(&self, slot: &RoundSlotState, _intent: &pm_core::domain::Intent) -> Admission {
        if slot.is_empty() {
            Admission::Admit
        } else {
            Admission::Reject
        }
    }
}

// ─── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. Init tracing — MUST be first.
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(fmt::layer())
        .init();

    info!("pm-bot starting");

    // 2. Load secrets.
    let _private_key =
        std::env::var("POLYGON_PRIVATE_KEY").expect("POLYGON_PRIVATE_KEY must be set");

    // 3. Build adapters.
    // TODO: construct real CLOB and Gamma clients here (see pm-explorer for auth pattern).
    let store: Arc<dyn pm_core::ports::Store> = Arc::new({
        adapters::sqlite_store::SqliteStore::open("pm-bot.db").expect("failed to open SQLite store")
    });
    let catalog: Arc<dyn pm_core::ports::MarketCatalog> = Arc::new(
        // TODO: adapters::gamma_market_catalog::GammaMarketCatalog::new(gamma_client)
        todo_catalog(),
    );
    let client: Arc<dyn pm_core::ports::MarketClient> = Arc::new(
        // TODO: adapters::clob_market_client::ClobMarketClient::new(clob_client)
        todo_client(),
    );

    let strategy = Arc::new(V1BasicStrategy::new(
        120, // enter within 2 minutes of cutoff
        rust_decimal_macros::dec!(0.02),
    ));
    let sizing: Arc<dyn pm_core::ports::SizingModel> = Arc::new(FixedSizingModel {
        shares: Shares(rust_decimal_macros::dec!(5)),
    });
    let policy: Arc<dyn EntryPolicy> = Arc::new(OnePositionPolicy);
    let clock = MarketClock::btc_5m();

    // 4. Wire channels.
    let (tick_tx, tick_rx) = broadcast::channel::<pm_core::domain::Tick>(256);
    let _ = tick_tx; // passed into price_feed_task once ChainlinkPriceFeed is wired below.
    let (market_tx, market_rx) = watch::channel::<Option<ActiveMarket>>(None);
    let (intent_tx, intent_rx) = mpsc::channel::<pm_core::domain::Intent>(8);
    let (order_update_tx, order_update_rx) = mpsc::channel::<pm_core::domain::OrderUpdate>(64);
    let (settled_tx, settled_rx) = mpsc::channel::<pm_core::domain::Settled>(16);
    let (slot_tx, slot_rx) = watch::channel::<RoundSlotState>(RoundSlotState::Empty);

    let cancel = CancellationToken::new();

    // 5. Wait until next window start.
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let next_window = clock.next_window_ts(now_secs);
    let wait_ms = (next_window.0 - pm_core::types::Timestamp::now_ms().0).max(0) as u64;
    if wait_ms > 0 {
        info!(
            wait_secs = wait_ms / 1000,
            "waiting for next window to start trading"
        );
        tokio::time::sleep(Duration::from_millis(wait_ms)).await;
    }

    // 6. Spawn tasks.
    // TODO: wire ChainlinkPriceFeed (V1 price source; V2 may aggregate multiple feeds)
    // let price_feed = Box::new(adapters::chainlink_price_feed::ChainlinkPriceFeed::connect().await?);
    // let h_price = tokio::spawn(price_feed_task(price_feed, tick_tx, cancel.clone()));

    let h_market = tokio::spawn(market_rotation_task(
        clock,
        catalog.clone(),
        market_tx,
        cancel.clone(),
    ));

    let h_decision = tokio::spawn(decision_center_task(
        strategy,
        sizing,
        tick_rx,
        market_rx.clone(),
        intent_tx,
        slot_rx.clone(),
        cancel.clone(),
    ));

    let h_executor = tokio::spawn(executor_task(
        policy,
        client.clone(),
        store.clone(),
        market_rx.clone(),
        intent_rx,
        order_update_tx.clone(),
        slot_tx,
        cancel.clone(),
    ));

    let h_poller = tokio::spawn(order_status_poller_task(
        client.clone(),
        store.clone(),
        order_update_tx,
        slot_rx,
        cancel.clone(),
    ));

    let h_settlement = tokio::spawn(settlement_task(
        client.clone(),
        store.clone(),
        market_rx,
        settled_tx,
        cancel.clone(),
    ));

    let h_persistence = tokio::spawn(persistence_task(
        store,
        order_update_rx,
        settled_rx,
        cancel.clone(),
    ));

    let h_heartbeat = tokio::spawn(heartbeat_task(
        client,
        Duration::from_secs(30),
        cancel.clone(),
    ));

    info!("all tasks spawned — trading");

    // 7. Graceful shutdown on Ctrl-C.
    tokio::signal::ctrl_c().await?;
    info!("shutdown signal received");
    cancel.cancel();

    // Join all handles (ignore individual errors — tasks log their own).
    // let _ = h_price.await;
    let _ = h_market.await;
    let _ = h_decision.await;
    let _ = h_executor.await;
    let _ = h_poller.await;
    let _ = h_settlement.await;
    let _ = h_persistence.await;
    let _ = h_heartbeat.await;

    info!("pm-bot shut down cleanly");
    Ok(())
}

// Placeholder helpers for todo adapters — remove when real ones are wired in.
fn todo_catalog() -> impl pm_core::ports::MarketCatalog {
    struct TodoCatalog;
    #[async_trait::async_trait]
    impl pm_core::ports::MarketCatalog for TodoCatalog {
        async fn resolve(
            &self,
            _slug: &pm_core::types::MarketSlug,
        ) -> pm_core::error::Result<pm_core::domain::Market> {
            todo!("wire real GammaMarketCatalog")
        }
    }
    TodoCatalog
}

fn todo_client() -> impl pm_core::ports::MarketClient {
    struct TodoClient;
    #[async_trait::async_trait]
    impl pm_core::ports::MarketClient for TodoClient {
        async fn quote(
            &self,
            _token_id: &pm_core::types::TokenId,
            _side: pm_core::types::Side,
        ) -> pm_core::error::Result<pm_core::types::Price> {
            todo!()
        }
        async fn place_order(
            &self,
            _intent: &pm_core::domain::Intent,
            _token_id: &pm_core::types::TokenId,
        ) -> pm_core::error::Result<String> {
            todo!()
        }
        async fn cancel_order(&self, _order_id: &str) -> pm_core::error::Result<()> {
            todo!()
        }
        async fn order_status(
            &self,
            _order_id: &str,
            _position_id: i64,
        ) -> pm_core::error::Result<pm_core::domain::OrderUpdate> {
            todo!()
        }
        async fn redeem(
            &self,
            _position: &pm_core::domain::PositionRecord,
        ) -> pm_core::error::Result<pm_core::types::Usdc> {
            todo!()
        }
    }
    TodoClient
}

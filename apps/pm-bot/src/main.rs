//! pm-bot: supervisor binary.
//! Start-up sequence (§15):
//!   1. Init tracing (first, before anything else)
//!   2. Load keys / secrets
//!   3. Build adapters → clients
//!   4. Wire channels and spawn tasks
//!   5. sleep_until(next window) then begin trading
//!   6. Ctrl-C → cancel token → join → exit

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use adapters::clob_market_client::{ClobMarketClient, CLOB_API_URL};
use adapters::gamma_market_catalog::GammaMarketCatalog;
use pm_core::tasks::decision_center::decision_center_task;
use pm_core::tasks::executor::executor_task;
use pm_core::tasks::price_feed::price_feed_task;
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

use pm_core::{
    clock::MarketClock,
    domain::{ActiveMarket, PendingRedemption, Redeemed},
    ports::{Admission, EntryPolicy},
    state::{BankrollState, RoundSlotState},
    tasks::{
        bankroll::bankroll_task, heartbeat::heartbeat_task, market_rotation::market_rotation_task,
        order_status_poller::order_status_poller_task, persistence::persistence_task,
        redeem_status_poller::redeem_status_poller_task, settlement::settlement_task,
    },
};
use pm_strategy::sizing::{FixedFractionSizingModel, SIZING_FRACTION};
use pm_strategy::strategy::V1BasicStrategy;
use tokio::sync::RwLock;

use polymarket_client_sdk_v2::auth::{LocalSigner, Signer};
use polymarket_client_sdk_v2::clob::types::SignatureType;
use polymarket_client_sdk_v2::clob::{Client as ClobClient, Config};
use polymarket_client_sdk_v2::{derive_safe_wallet, POLYGON};

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
    let private_key =
        std::env::var("POLYGON_PRIVATE_KEY").expect("POLYGON_PRIVATE_KEY must be set");
    let relayer_api_key = std::env::var("RELAYER_API_KEY").expect("RELAYER_API_KEY must be set");
    let rpc_url = std::env::var("POLYGON_RPC_URL").expect("POLYGON_RPC_URL must be set");

    // 3. Build adapters.
    // let gamma_client = GammaClient::new(GAMMA_API_URL)?;

    // Authenticate the user and obtain the API key
    let signer = LocalSigner::from_str(&private_key)
        .expect("error with local signer")
        .with_chain_id(Some(POLYGON));

    // Creates new credentials or derives existing ones,
    // then initializes the authenticated client — all in one step
    let clob_client = ClobClient::new(CLOB_API_URL, Config::default())
        .expect("error build clob client")
        .authentication_builder(&signer)
        .signature_type(SignatureType::GnosisSafe)
        .authenticate()
        .await
        .expect("error authenticating clob client");

    let safe_address = derive_safe_wallet(clob_client.address(), POLYGON)
        .expect("error deriving safe wallet address");

    let store: Arc<dyn pm_core::ports::Store> = Arc::new({
        adapters::sqlite_store::SqliteStore::open("pm-bot.db").expect("failed to open SQLite store")
    });

    // // USING MockStore for now to avoid wiring up a real database. Remove when real store is wired in.
    // let store: Arc<dyn pm_core::ports::Store> = adapters::mock_store::MockStore::new();

    let catalog: Arc<dyn pm_core::ports::MarketCatalog> = Arc::new(GammaMarketCatalog::new());
    let client: Arc<dyn pm_core::ports::MarketClient> = Arc::new(ClobMarketClient::new(
        clob_client,
        signer,
        safe_address,
        relayer_api_key,
        rpc_url,
    ));

    let strategy = Arc::new(V1BasicStrategy::new(
        120, // enter within 2 minutes of cutoff
        rust_decimal_macros::dec!(0.02),
    ));
    let sizing: Arc<dyn pm_core::ports::SizingModel> =
        Arc::new(FixedFractionSizingModel::new(SIZING_FRACTION));
    let policy: Arc<dyn EntryPolicy> = Arc::new(OnePositionPolicy);
    let clock = MarketClock::btc_5m();

    // 4. Wire channels.
    let (tick_tx, _) = broadcast::channel::<pm_core::domain::Tick>(256);
    let (market_tx, market_rx) = watch::channel::<Option<ActiveMarket>>(None);
    let (intent_tx, intent_rx) = mpsc::channel::<pm_core::domain::Intent>(8);
    let (order_update_tx, _) = broadcast::channel::<pm_core::domain::OrderUpdate>(64);
    let (settled_tx, _) = broadcast::channel::<pm_core::domain::Settled>(16);
    let (redeemed_tx, redeemed_rx) = mpsc::channel::<Redeemed>(16);
    let (pending_tx, pending_rx) = mpsc::channel::<PendingRedemption>(16);
    let (slot_tx, slot_rx) = watch::channel::<RoundSlotState>(RoundSlotState::Empty);

    let cancel = CancellationToken::new();

    // // 5. Wait until next window start.
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

    // 6. Warm up
    let starting = client.balance().await?;
    info!(balance = %starting.0, "starting USDC balance");
    let bankroll = Arc::new(RwLock::new(BankrollState::new(starting)));

    let price_feed = Box::new(adapters::chainlink_price_feed::ChainlinkPriceFeed::connect());

    // 6. Spawn tasks.
    // TODO: wire ChainlinkPriceFeed (V1 price source; V2 may aggregate multiple feeds)
    let h_price = tokio::spawn(price_feed_task(price_feed, tick_tx.clone(), cancel.clone()));

    let h_market = tokio::spawn(market_rotation_task(
        clock,
        catalog.clone(),
        market_tx,
        cancel.clone(),
    ));

    let h_decision = tokio::spawn(decision_center_task(
        strategy,
        sizing,
        client.clone(),
        bankroll.clone(),
        tick_tx.subscribe(),
        market_rx.clone(),
        intent_tx,
        slot_rx.clone(),
        cancel.clone(),
    ));

    let _h_executor = tokio::spawn(executor_task(
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
        order_update_tx.clone(),
        slot_rx,
        cancel.clone(),
    ));

    let h_settlement = tokio::spawn(settlement_task(
        client.clone(),
        store.clone(),
        market_rx,
        tick_tx.subscribe(),
        settled_tx.clone(),
        pending_tx,
        cancel.clone(),
    ));

    let h_persistence = tokio::spawn(persistence_task(
        store.clone(),
        order_update_tx.subscribe(),
        settled_tx.subscribe(),
        cancel.clone(),
    ));

    let h_bankroll = tokio::spawn(bankroll_task(
        bankroll.clone(),
        order_update_tx.subscribe(),
        settled_tx.subscribe(),
        redeemed_rx,
        cancel.clone(),
    ));

    let h_redeem_poller = tokio::spawn(redeem_status_poller_task(
        client.clone(),
        store.clone(),
        pending_rx,
        redeemed_tx,
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
    let _ = h_price.await;
    let _ = h_market.await;
    let _ = h_decision.await;
    // let _ = h_executor.await;
    let _ = h_poller.await;
    let _ = h_settlement.await;
    let _ = h_persistence.await;
    let _ = h_bankroll.await;
    let _ = h_redeem_poller.await;
    let _ = h_heartbeat.await;

    // Final win/loss summary from the store before exiting.
    match store.success_rate_counts().await {
        Ok((wins, resolved)) => {
            pm_core::stats::WinLossStats::from_counts(wins, resolved).log("final")
        }
        Err(e) => tracing::warn!(error = %e, "failed to read final win/loss counts"),
    }

    info!("pm-bot shut down cleanly");
    Ok(())
}


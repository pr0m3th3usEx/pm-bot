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

use pm_core::config::{ExecutionMode, SimConfig};
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
    state::{BankrollState, OutcomeBookCache, RoundSlotState},
    tasks::{
        bankroll::bankroll_task, heartbeat::heartbeat_task, market_data::market_data_task,
        market_rotation::market_rotation_task, order_status_poller::order_status_poller_task,
        persistence::persistence_task, redeem_status_poller::redeem_status_poller_task,
        settlement::settlement_task,
    },
};
use pm_strategy::sizing::{FixedFractionSizingModel, SIZING_FRACTION};
use tokio::sync::RwLock;

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

    // 2. Read execution mode.
    let mode = ExecutionMode::from_env().expect("invalid EXECUTION_MODE env var");
    let sim_cfg = SimConfig::from_env();
    let record_session = std::env::var("RECORD_SESSION")
        .map(|s| matches!(s.as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);

    info!(mode = ?mode, record_session, "execution mode");

    // 3. Build adapters — different paths for Live vs DryRun.
    let store: Arc<dyn pm_core::ports::Store> = pm_bot::bootstrap::open_store(mode, &sim_cfg);

    let catalog_inner: Arc<dyn pm_core::ports::MarketCatalog> = pm_bot::bootstrap::build_catalog();

    let book_cache = Arc::new(RwLock::new(OutcomeBookCache::default()));

    // Build the recorder sink (only used when record_session=true).
    let recorder: Option<Arc<dyn pm_core::ports::MarketDataRecorder>> = if record_session {
        let recorder_path = std::env::var("RECORD_DB_PATH")
            .unwrap_or_else(|_| "pm-bot.recordings.sqlite".to_owned());
        match adapters::recording_feeds::SqliteMarketDataRecorder::open(&recorder_path) {
            Ok(r) => {
                info!(path = %recorder_path, "recording session to SQLite");
                Some(Arc::new(r))
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to open recorder DB — recording disabled");
                None
            }
        }
    } else {
        None
    };

    let (client, starting_bankroll): (Arc<dyn pm_core::ports::MarketClient>, _) =
        pm_bot::bootstrap::build_client(mode, &sim_cfg, book_cache.clone()).await?;

    let catalog: Arc<dyn pm_core::ports::MarketCatalog> = if record_session {
        if let Some(ref sink) = recorder {
            Arc::new(adapters::recording_feeds::RecordingCatalog::new(
                catalog_inner.clone(),
                sink.clone(),
            ))
        } else {
            catalog_inner
        }
    } else {
        catalog_inner
    };

    let strategy_kind =
        pm_core::config::StrategyKind::from_env().expect("invalid STRATEGY env var");
    info!(strategy = ?strategy_kind, "selected strategy");
    let strategy = pm_bot::bootstrap::build_strategy(strategy_kind);
    let sizing: Arc<dyn pm_core::ports::SizingModel> =
        Arc::new(FixedFractionSizingModel::new(SIZING_FRACTION));
    let policy: Arc<dyn EntryPolicy> = Arc::new(OnePositionPolicy);
    let clock = MarketClock::btc_5m();

    // 4. Wire channels.
    let (tick_tx, _) = broadcast::channel::<pm_core::domain::Tick>(256);
    let (market_tx, market_rx) = watch::channel::<Option<ActiveMarket>>(None);
    // Lossless lifecycle stream to settlement (watch coalesces; settlement must see every event).
    let (lifecycle_tx, lifecycle_rx) =
        mpsc::channel::<pm_core::domain::MarketLifecycle>(16);
    let (intent_tx, intent_rx) = mpsc::channel::<pm_core::domain::Intent>(8);
    let (order_update_tx, _) = broadcast::channel::<pm_core::domain::OrderUpdate>(64);
    let (settled_tx, _) = broadcast::channel::<pm_core::domain::Settled>(16);
    let (redeemed_tx, redeemed_rx) = mpsc::channel::<Redeemed>(16);
    let (pending_tx, pending_rx) = mpsc::channel::<PendingRedemption>(16);
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

    // 6. Warm up.
    info!(balance = %starting_bankroll.0, "virtual/actual USDC balance at start");
    let bankroll = Arc::new(RwLock::new(BankrollState::new(starting_bankroll)));

    // Build price feed (optionally wrapped in recorder).
    let price_feed: Box<dyn pm_core::ports::PriceFeed> = {
        let inner = Box::new(adapters::chainlink_price_feed::ChainlinkPriceFeed::connect());
        if record_session {
            if let Some(ref sink) = recorder {
                Box::new(adapters::recording_feeds::RecordingPriceFeed::new(
                    inner,
                    sink.clone(),
                    market_rx.clone(),
                ))
            } else {
                inner
            }
        } else {
            inner
        }
    };

    // 6. Spawn tasks.
    let h_price = tokio::spawn(price_feed_task(price_feed, tick_tx.clone(), cancel.clone()));

    let h_market = tokio::spawn(market_rotation_task(
        clock,
        catalog.clone(),
        market_tx,
        lifecycle_tx,
        cancel.clone(),
    ));

    // Build the market data connect closure, optionally wrapped with recorder.
    let recorder_for_feed = recorder.clone();
    let market_rx_for_feed = market_rx.clone();
    let connect_fn = move |ids: Vec<String>| -> Box<dyn pm_core::ports::MarketDataFeed> {
        let inner: Box<dyn pm_core::ports::MarketDataFeed> = Box::new(
            adapters::polymarket_market_feed::PolymarketMarketFeed::connect(ids),
        );
        if let Some(ref sink) = recorder_for_feed {
            Box::new(adapters::recording_feeds::RecordingMarketDataFeed::new(
                inner,
                sink.clone(),
                market_rx_for_feed.clone(),
            ))
        } else {
            inner
        }
    };

    let h_market_data = tokio::spawn(market_data_task(
        market_rx.clone(),
        book_cache.clone(),
        connect_fn,
        cancel.clone(),
    ));

    let h_decision = tokio::spawn(decision_center_task(
        strategy,
        sizing,
        book_cache.clone(),
        bankroll.clone(),
        tick_tx.subscribe(),
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
        order_update_tx.clone(),
        slot_rx,
        cancel.clone(),
    ));

    let h_settlement = tokio::spawn(settlement_task(
        client.clone(),
        store.clone(),
        lifecycle_rx,
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
    let _ = h_market_data.await;
    let _ = h_decision.await;
    let _ = h_executor.await;
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

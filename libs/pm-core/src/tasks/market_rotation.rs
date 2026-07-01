use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::clock::MarketClock;
use crate::domain::ActiveMarket;
use crate::ports::MarketCatalog;
use crate::state::MarketState;
use crate::types::{MarketStatus, Timestamp};

pub async fn market_rotation_task(
    clock: MarketClock,
    catalog: Arc<dyn MarketCatalog>,
    market_tx: watch::Sender<Option<ActiveMarket>>,
    cancel: CancellationToken,
) {
    info!("market_rotation_task started");
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("market_rotation_task cancelled");
                break;
            }
            _ = run_round(&clock, &catalog, &market_tx) => {}
        }
    }
}

fn delay_until_ms(target_ms: i64) -> Duration {
    let now = Timestamp::now_ms().0;
    Duration::from_millis((target_ms - now).max(0) as u64)
}

async fn run_round(
    clock: &MarketClock,
    catalog: &Arc<dyn MarketCatalog>,
    market_tx: &watch::Sender<Option<ActiveMarket>>,
) {
    // 1. Resolve current market, retrying on transient errors.
    let now_secs = Timestamp::now_ms().as_secs() as u64;
    let slug = clock.current_slug(now_secs);
    info!(market_slug = %slug, "🔍 resolving current market");

    let market = loop {
        match catalog.resolve(&slug).await {
            Ok(m) => break m,
            Err(e) => {
                error!(error = %e, "failed to resolve market; retrying in 5s");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    };

    // Edge case: bot started after resolution (e.g. mid-settlement window).
    if market.status == MarketStatus::Resolved {
        info!(market_slug = %slug, "🏁 market already resolved; publishing and rotating");
        market_tx.send(Some(market)).ok();
        return;
    }

    // Publish initial Pending market so consumers have metadata immediately.
    market_tx.send(Some(market.clone())).ok();
    let mut state = MarketState::new();

    // 2. Sleep until opens_at → Open (clock-derived).
    let delay = delay_until_ms(market.opens_at.0);
    if !delay.is_zero() {
        info!(market_slug = %slug, opens_at = ?market.opens_at, "⏳ waiting for market to open");
        tokio::time::sleep(delay).await;
    }
    state = state
        .transition(MarketStatus::Open)
        .expect("Pending → Open");
    let market = ActiveMarket {
        status: state.status(),
        ..market
    };
    market_tx.send(Some(market.clone())).ok();
    info!(market_slug = %slug, status = ?state.status(),
        opens_at = ?market.opens_at, closes_at = ?market.closes_at, resolves_at = ?market.resolves_at,
        status = ?state.status(),
        strike = ?market.strike, "▶ market open");

    // 3. Sleep until closes_at → TradingCutoff (clock-derived).
    let delay = delay_until_ms(market.closes_at.0);
    if !delay.is_zero() {
        info!(market_slug = %slug, closes_at = ?market.closes_at, "⏳ waiting for trading cutoff");
        tokio::time::sleep(delay).await;
    }
    state = state
        .transition(MarketStatus::TradingCutoff)
        .expect("Open → TradingCutoff");
    let market = ActiveMarket {
        status: state.status(),
        ..market
    };
    market_tx.send(Some(market.clone())).ok();
    info!(market_slug = %slug, "⛔ trading cutoff reached");

    // 4. Kick off prefetch of next round in background so it overlaps resolution polling.
    let next_slug = clock.next_slug(Timestamp::now_ms().as_secs() as u64);
    let prefetch = tokio::spawn({
        let catalog: Arc<dyn MarketCatalog> = Arc::clone(catalog);
        let slug = next_slug.clone();
        async move { catalog.resolve(&slug).await }
    });

    // 5. Publish Resolving, then poll the catalog until it confirms Resolved.
    state = state
        .transition(MarketStatus::Resolving)
        .expect("TradingCutoff → Resolving");
    let market = ActiveMarket {
        status: state.status(),
        ..market
    };
    market_tx.send(Some(market)).ok();
    info!(market_slug = %slug, "⏳ waiting for resolution");

    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;
        match catalog.resolve(&slug).await {
            Ok(m) if m.status == MarketStatus::Resolved => {
                state
                    .transition(MarketStatus::Resolved)
                    .expect("Resolving → Resolved");
                market_tx.send(Some(m)).ok();
                info!(market_slug = %slug, "🏁 market resolved");
                break;
            }
            Ok(_) => {}
            Err(e) => error!(error = %e, "resolution poll failed; retrying"),
        }
    }

    // 6. Publish prefetched next market immediately if the background fetch succeeded.
    match prefetch.await {
        Ok(Ok(next_market)) => {
            info!(next_slug = %next_slug, "⏭ publishing prefetched next market");
            market_tx.send(Some(next_market)).ok();
        }
        Ok(Err(e)) => {
            warn!(next_slug = %next_slug, error = %e, "next-market prefetch failed; outer loop will retry")
        }
        Err(e) => warn!(next_slug = %next_slug, error = %e, "next-market prefetch task panicked"),
    }
}

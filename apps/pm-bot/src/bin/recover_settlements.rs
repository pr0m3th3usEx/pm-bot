//! pm-recover-settlements: manual maintenance tool.
//!
//! Finds positions stuck in `Filled`/`Settling` for markets that have already resolved (i.e. that
//! missed live settlement) and settles them against the market's authoritative resolved outcome.
//!
//! Usage:
//!   pm-recover-settlements            # dry-run: report what WOULD settle (no DB writes)
//!   pm-recover-settlements --apply    # actually redeem winners / mark losers
//!
//! Honours the same `EXECUTION_MODE` / `SIM_*` env vars as `pm-bot`, so it opens the same DB and
//! uses the matching market client (Sim for dry-run, Clob for live).

use std::collections::BTreeMap;
use std::sync::Arc;

use pm_core::config::{ExecutionMode, SimConfig};
use pm_core::ports::{MarketClient, MarketCatalog, Store};
use pm_core::state::OutcomeBookCache;
use pm_core::tasks::settlement::{settle_market_positions, winning_outcome_name};
use pm_core::types::{MarketStatus, PositionStatus};
use tokio::sync::RwLock;
use tracing::{info, warn};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(fmt::layer())
        .init();

    let apply = std::env::args().any(|a| a == "--apply");
    let mode = ExecutionMode::from_env().expect("invalid EXECUTION_MODE env var");
    let sim_cfg = SimConfig::from_env();

    info!(mode = ?mode, apply, "pm-recover-settlements starting");
    if !apply {
        info!("DRY-RUN: no DB writes will be made. Re-run with --apply to settle.");
    }

    let store: Arc<dyn Store> = pm_bot::bootstrap::open_store(mode, &sim_cfg);
    let catalog: Arc<dyn MarketCatalog> = pm_bot::bootstrap::build_catalog();
    let book_cache = Arc::new(RwLock::new(OutcomeBookCache::default()));
    let (client, _bankroll): (Arc<dyn MarketClient>, _) =
        pm_bot::bootstrap::build_client(mode, &sim_cfg, book_cache).await?;

    // 1. Collect positions still awaiting settlement, grouped by market slug.
    let positions = store.open_positions().await?;
    let mut by_slug: BTreeMap<String, usize> = BTreeMap::new();
    for p in positions
        .iter()
        .filter(|p| matches!(p.status, PositionStatus::Filled | PositionStatus::Settling))
    {
        *by_slug.entry(p.market_slug.to_string()).or_default() += 1;
    }

    if by_slug.is_empty() {
        info!("no positions in Filled/Settling — nothing to recover");
        return Ok(());
    }

    info!(markets = by_slug.len(), "found unsettled positions across markets");

    let mut recovered = 0usize;
    let mut skipped = 0usize;

    // 2. For each market, resolve it and settle if the catalog confirms it is Resolved.
    for (slug, count) in by_slug {
        let market_slug = pm_core::types::MarketSlug(slug.clone());
        let market = match catalog.resolve(&market_slug).await {
            Ok(m) => m,
            Err(e) => {
                warn!(market_slug = %slug, error = %e, "failed to resolve market — skipping");
                skipped += count;
                continue;
            }
        };

        if market.status != MarketStatus::Resolved {
            info!(market_slug = %slug, status = ?market.status, positions = count,
                "market not resolved yet — skipping");
            skipped += count;
            continue;
        }

        // Recovery relies on the authoritative resolved_outcome; the local price>strike fallback
        // needs a live tick buffer we don't have here.
        let Some(winner) = winning_outcome_name(&market, None) else {
            warn!(market_slug = %slug, positions = count,
                "resolved market has no resolved_outcome and no price available — skipping");
            skipped += count;
            continue;
        };

        info!(
            market_slug = %slug,
            winner = %winner,
            positions = count,
            "{}",
            if apply { "settling positions" } else { "WOULD settle positions" }
        );

        if apply {
            // No live channels: settlement broadcasts are irrelevant to the offline tool.
            settle_market_positions(&client, &store, &market, None, None, None).await;
        }
        recovered += count;
    }

    info!(
        recovered,
        skipped,
        applied = apply,
        "pm-recover-settlements done"
    );
    Ok(())
}

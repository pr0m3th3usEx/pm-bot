use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::domain::{ActiveMarket, Intent, Tick};
use crate::ports::{SizingModel, Strategy};
use crate::state::RoundSlotState;
use crate::strategy::{StrategyContext, StrategyDecision};
use crate::types::{MarketStatus, Usdc};

pub async fn decision_center_task(
    strategy: Arc<dyn Strategy>,
    sizing: Arc<dyn SizingModel>,
    mut tick_rx: broadcast::Receiver<Tick>,
    market_rx: watch::Receiver<Option<ActiveMarket>>,
    intent_tx: mpsc::Sender<Intent>,
    slot_rx: watch::Receiver<RoundSlotState>,
    cancel: CancellationToken,
) {
    info!("decision_center_task started");

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("decision_center_task cancelled");
                break;
            }
            result = tick_rx.recv() => {
                let tick = match result {
                    Ok(t) => t,
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(skipped = n, "tick channel lagged");
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                };

                // ── Per-tick hold/enter dispatch ──────────────────────────
                // 1. Efficiency check: skip if slot is already claimed.
                let slot = *slot_rx.borrow();
                if !slot.is_empty() {
                    debug!("slot occupied — holding");
                    continue;
                }

                // 2. Read latest market snapshot.
                let market = match market_rx.borrow().clone() {
                    Some(m) => m,
                    None => {
                        debug!("no active market yet — holding");
                        continue;
                    }
                };

                // 3. Past trading cutoff — no point evaluating.
                if !matches!(market.status, MarketStatus::Open) {
                    debug!(status = ?market.status, "market not open — holding");
                    continue;
                }

                // 4. Get strike — TODO(confirm): source of strike.
                let strike = match market.strike.clone() {
                    Some(s) => s,
                    None => {
                        debug!("strike unknown — holding");
                        continue;
                    }
                };

                // 5. Ask strategy.
                let ctx = StrategyContext {
                    price: tick.price.clone(),
                    strike,
                    now: tick.at,
                    closes_at: market.closes_at,
                    resolves_at: market.resolves_at,
                    market: &market,
                };

                match strategy.evaluate(&ctx) {
                    StrategyDecision::Hold => {
                        debug!(
                            price = %tick.price.0,
                            secs_to_cutoff = ctx.secs_to_cutoff(),
                            "strategy: hold"
                        );
                    }
                    StrategyDecision::Enter { outcome } => {
                        // TODO(confirm): bankroll — load from store or config?
                        let bankroll = Usdc(rust_decimal::Decimal::new(100, 0)); // placeholder
                        let shares = sizing.size(&bankroll, &tick.price);

                        let intent = Intent {
                            outcome,
                            side: crate::types::Side::Buy,
                            shares,
                            limit_price: tick.price,
                        };

                        debug!(
                            outcome = ?intent.outcome,
                            shares = %intent.shares.0,
                            price = %intent.limit_price.0,
                            "strategy: enter — emitting intent"
                        );

                        // Non-blocking: executor is the race-free authority on the gate.
                        if let Err(_) = intent_tx.try_send(intent) {
                            warn!("intent channel full or closed — dropping");
                        }
                    }
                }
            }
        }
    }
}

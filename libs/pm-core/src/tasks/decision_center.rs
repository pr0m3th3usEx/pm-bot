use std::sync::Arc;
use tokio::sync::{RwLock, broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::domain::{ActiveMarket, Intent, Tick};
use crate::ports::{MarketClient, SizingModel, Strategy};
use crate::state::{BankrollState, RoundSlotState};
use crate::strategy::{StrategyContext, StrategyDecision};
use crate::types::{MarketStatus, Price, Side};

// Maximum ask price above which we refuse to enter (shares would be near-certain losers).
// Defined as let-bindings inside the branch rather than top-level const because
// rust_decimal_macros::dec! does not produce a value that satisfies the `const` requirement
// in all compiler versions supported by this workspace.

pub async fn decision_center_task(
    strategy: Arc<dyn Strategy>,
    sizing: Arc<dyn SizingModel>,
    client: Arc<dyn MarketClient>,
    bankroll_state: Arc<RwLock<BankrollState>>,
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
                    StrategyDecision::Enter { outcome, confidence } => {
                        // a. Resolve token_id from the already-fetched market.
                        debug!(outcomes = ?market.outcomes.iter().map(|o| o.name.clone()).collect::<Vec<_>>(), "strategy: enter — outcome chosen");
                        let Some(mo) = market.outcomes.iter().find(|o| o.name == outcome.as_str()) else {
                            warn!(outcome = outcome.as_str(), "outcome not in market — holding");
                            continue;
                        };
                        let token_id = mo.token_id.clone();

                        // b. Fetch the best-ask (marketable buy price) via quote.
                        // NOTE: Side::Buy walks the asks side of the orderbook in the Polymarket
                        // CLOB SDK (confirmed in clob/utilities.rs: Side::Buy => &orderbook.asks).
                        // So Side::Buy correctly returns the ask price (what a buyer must pay).
                        let ask = match client.quote(&token_id, Side::Buy).await {
                            Ok(p) => p,
                            Err(e) => {
                                warn!(error = %e, "quote failed — holding");
                                continue;
                            }
                        };

                        // c. Edge guard: skip entry if ask is already very high.
                        let max_entry_price = rust_decimal_macros::dec!(0.95);
                        if ask.0 >= max_entry_price {
                            debug!(ask = %ask.0, "ask too high — holding");
                            continue;
                        }

                        // d. Read the real bankroll AFTER the await, holding the lock briefly.
                        let bankroll = { bankroll_state.read().await.bankroll.clone() };

                        // e. Compute share count; skip if zero, else enforce the market minimum.
                        let raw_shares = sizing.size(&bankroll, &ask);
                        if raw_shares.0 <= rust_decimal::Decimal::ZERO {
                            debug!(bankroll = %bankroll.0, ask = %ask.0, "size is zero — holding");
                            continue;
                        }
                        // Bump up to the CLOB's minimum order size (Gamma orderMinSize). On a small
                        // bankroll this can deploy more than the sizing model intended — accepted
                        // trade-off to ensure participation.
                        let shares = if raw_shares.0 < market.order_min_size.0 {
                            debug!(
                                raw_shares = %raw_shares.0,
                                order_min_size = %market.order_min_size.0,
                                "sized below market minimum — bumping to order_min_size"
                            );
                            market.order_min_size.clone()
                        } else {
                            raw_shares
                        };

                        // f. Limit price: best ask + one-tick buffer, rounded to the market's price
                        // tick (Gamma orderPriceMinTickSize) and capped one tick below 1.
                        let tick = market.order_price_min_tick_size.0;
                        let buffered = std::cmp::min(ask.0 + tick, rust_decimal::Decimal::ONE - tick);
                        let limit = if tick > rust_decimal::Decimal::ZERO {
                            (buffered / tick).round() * tick
                        } else {
                            buffered
                        };
                        let limit_price = Price(limit);

                        // g. Build and send the intent.
                        let intent = Intent {
                            outcome,
                            side: Side::Buy,
                            shares,
                            limit_price,
                        };

                        debug!(
                            outcome = ?intent.outcome,
                            shares = %intent.shares.0,
                            price = %intent.limit_price.0,
                            confidence = confidence,
                            "strategy: enter — emitting intent"
                        );

                        // Non-blocking: executor is the race-free authority on the gate.
                        if intent_tx.try_send(intent).is_err() {
                            warn!("intent channel full or closed — dropping");
                        }
                    }
                }
            }
        }
    }
}

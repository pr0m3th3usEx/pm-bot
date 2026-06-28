use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::domain::{ActiveMarket, Intent, OrderUpdate, PositionRecord, PositionUpdate};
use crate::ports::{Admission, EntryPolicy, MarketClient, Store};
use crate::state::RoundSlotState;
use crate::types::{PositionStatus, Timestamp};

pub async fn executor_task(
    policy: Arc<dyn EntryPolicy>,
    client: Arc<dyn MarketClient>,
    store: Arc<dyn Store>,
    mut market_rx: watch::Receiver<Option<ActiveMarket>>,
    mut intent_rx: mpsc::Receiver<Intent>,
    order_update_tx: broadcast::Sender<OrderUpdate>,
    slot_tx: watch::Sender<RoundSlotState>,
    cancel: CancellationToken,
) {
    info!("executor_task started");

    let mut slot = RoundSlotState::Empty;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("executor_task cancelled");
                break;
            }

            // ── New market rotation: reset the slot ───────────────────────
            Ok(()) = market_rx.changed() => {
                let prev_slot = slot;
                slot = slot.rotate();
                if slot_tx.send(slot).is_err() { break; }
                if let Some(ref m) = *market_rx.borrow_and_update() {
                    info!(market_slug = %m.slug, prev_slot = ?prev_slot, "round rotated — slot reset");
                }
            }

            // ── Intent received: apply the entry policy gate ──────────────
            Some(intent) = intent_rx.recv() => {
                match policy.admit(&slot, &intent) {
                    Admission::Reject => {
                        debug_assert!(!slot.is_empty(), "policy rejected from empty slot?");
                        warn!(slot = ?slot, "entry policy rejected intent");
                    }
                    Admission::Admit => {
                        // Resolve outcome → token_id from active market
                        let market = market_rx.borrow().clone();
                        let Some(market) = market else {
                            warn!("no active market — dropping intent");
                            continue;
                        };

                        let Some(mo) = market.outcomes.iter()
                            .find(|o| o.name == intent.outcome.as_str())
                        else {
                            warn!(outcome = intent.outcome.as_str(), "outcome not in market — dropping intent");
                            continue;
                        };

                        let token_id = mo.token_id.clone();
                        let now = Timestamp::now_ms();

                        // Insert submitted row — gives us position_id immediately.
                        let record = PositionRecord {
                            id: None,
                            market_slug: market.slug.clone(),
                            side: intent.side,
                            outcome_name: mo.name.clone(),
                            token_id: token_id.clone(),
                            condition_id: market.condition_id,
                            order_id: None,
                            shares: intent.shares.clone(),
                            limit_price: intent.limit_price.clone(),
                            avg_price: None,
                            strike: market.strike.clone(),
                            status: PositionStatus::Submitted,
                            realized_pnl: None,
                            submitted_at: now,
                            updated_at: now,
                        };

                        let position_id = match store.insert_position(&record).await {
                            Ok(id) => id,
                            Err(e) => {
                                error!(error = %e, "failed to insert position record");
                                continue;
                            }
                        };

                        // Claim slot before hitting the CLOB wire.
                        slot = match slot.claim(position_id) {
                            Ok(s) => s,
                            Err(e) => {
                                error!(error = %e, "slot claim failed — should not happen");
                                continue;
                            }
                        };
                        if slot_tx.send(slot).is_err() { break; }

                        // Place order on the CLOB.
                        match client.place_order(&intent, &token_id).await {
                            Ok(order_id) => {
                                info!(
                                    position_id,
                                    order_id = %order_id,
                                    outcome = intent.outcome.as_str(),
                                    shares = %intent.shares.0,
                                    "order submitted"
                                );
                                // Emit Submitted event for persistence task.
                                let _ = order_update_tx.send(OrderUpdate::Submitted {
                                    order_id: order_id.clone(),
                                    position_id,
                                });
                                // Also update the order_id in the store.
                                let _ = store.update_position(position_id, &PositionUpdate::Submitted {
                                    order_id,
                                    updated_at: Timestamp::now_ms(),
                                }).await;
                            }
                            Err(e) => {
                                error!(position_id, error = %e, "place_order failed — freeing slot");
                                // Treat as immediate rejection — free the slot.
                                slot = slot.free().unwrap_or(RoundSlotState::Empty);
                                if slot_tx.send(slot).is_err() { break; }
                                let _ = store.update_position(position_id, &PositionUpdate::Rejected {
                                    updated_at: Timestamp::now_ms(),
                                }).await;
                            }
                        }
                    }
                }
            }
        }
    }
}

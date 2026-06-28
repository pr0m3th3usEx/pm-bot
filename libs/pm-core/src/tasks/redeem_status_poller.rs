use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::domain::{PendingRedemption, PositionUpdate, Redeemed};
use crate::ports::{MarketClient, RedemptionStatus, Store};
use crate::types::{Timestamp, Usdc};

pub async fn redeem_status_poller_task(
    client: Arc<dyn MarketClient>,
    store: Arc<dyn Store>,
    mut pending_rx: mpsc::Receiver<PendingRedemption>,
    redeemed_tx: mpsc::Sender<Redeemed>,
    cancel: CancellationToken,
) {
    info!("redeem_status_poller_task started");

    // position_id -> (transaction_id, payout)
    let mut tracked: HashMap<i64, (String, Usdc)> = HashMap::new();

    let mut tick = tokio::time::interval(Duration::from_secs(5));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("redeem_status_poller_task cancelled");
                break;
            }

            Some(pending) = pending_rx.recv() => {
                info!(
                    position_id = pending.position_id,
                    transaction_id = %pending.transaction_id,
                    "tracking pending redemption"
                );
                tracked.insert(pending.position_id, (pending.transaction_id, pending.payout));
            }

            _ = tick.tick() => {
                if tracked.is_empty() {
                    continue;
                }

                let mut done = Vec::new();
                for (&position_id, (tx_id, payout)) in tracked.iter() {
                    match client.redemption_status(tx_id).await {
                        Ok(RedemptionStatus::Confirmed) => {
                            info!(position_id, transaction_id = %tx_id, "redemption confirmed");
                            if let Err(e) = store
                                .update_position(
                                    position_id,
                                    &PositionUpdate::Redeemed {
                                        updated_at: Timestamp::now_ms(),
                                    },
                                )
                                .await
                            {
                                error!(position_id, error = %e, "failed to persist Redeemed update");
                            }
                            if redeemed_tx
                                .send(Redeemed {
                                    position_id,
                                    payout: payout.clone(),
                                })
                                .await
                                .is_err()
                            {
                                // bankroll_task gone; stop
                                return;
                            }
                            done.push(position_id);
                        }
                        Ok(RedemptionStatus::Failed) => {
                            error!(position_id, transaction_id = %tx_id, "redemption failed on-chain — dropping");
                            done.push(position_id);
                        }
                        Ok(RedemptionStatus::Pending) => {
                            // keep tracking
                        }
                        Err(e) => {
                            error!(position_id, transaction_id = %tx_id, error = %e, "redemption_status poll failed");
                        }
                    }
                }
                for pid in done {
                    tracked.remove(&pid);
                }
            }
        }
    }
}

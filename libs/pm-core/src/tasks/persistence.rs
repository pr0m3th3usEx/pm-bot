use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::domain::{OrderUpdate, PositionUpdate, Settled};
use crate::ports::Store;
use crate::stats::WinLossStats;
use crate::types::{PositionStatus, Timestamp};

pub async fn persistence_task(
    store: Arc<dyn Store>,
    mut order_update_rx: broadcast::Receiver<OrderUpdate>,
    mut settled_rx: broadcast::Receiver<Settled>,
    cancel: CancellationToken,
) {
    info!("persistence_task started");
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("persistence_task cancelled");
                break;
            }
            result = order_update_rx.recv() => {
                let update = match result {
                    Ok(u) => u,
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(skipped = n, "persistence order_update_rx lagged");
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => continue,
                };
                let (position_id, pu) = match update {
                    // Executor already persists Submitted inline; skip to avoid double-write.
                    OrderUpdate::Submitted { .. } => continue,
                    OrderUpdate::Filled { position_id, avg_price, size_matched, .. } => (
                        position_id,
                        PositionUpdate::Filled { avg_price, size_matched, updated_at: Timestamp::now_ms() },
                    ),
                    OrderUpdate::Rejected { position_id, .. } => (
                        position_id,
                        PositionUpdate::Rejected { updated_at: Timestamp::now_ms() },
                    ),
                    OrderUpdate::Cancelled { position_id, .. } => (
                        position_id,
                        PositionUpdate::Cancelled { updated_at: Timestamp::now_ms() },
                    ),
                };
                if let Err(e) = store.update_position(position_id, &pu).await {
                    error!(position_id, error = %e, "failed to persist order update");
                }
            }
            result = settled_rx.recv() => {
                let settled = match result {
                    Ok(s) => s,
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(skipped = n, "persistence settled_rx lagged");
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => continue,
                };
                let pu = match settled.status {
                    PositionStatus::Won => PositionUpdate::Won {
                        realized_pnl: settled.realized_pnl,
                        updated_at: Timestamp::now_ms(),
                    },
                    PositionStatus::Lost => PositionUpdate::Lost {
                        realized_pnl: settled.realized_pnl,
                        updated_at: Timestamp::now_ms(),
                    },
                    other => {
                        warn!(status = ?other, position_id = settled.position_id, "unexpected status in Settled; skipping");
                        continue;
                    }
                };
                match store.update_position(settled.position_id, &pu).await {
                    Err(e) => {
                        error!(position_id = settled.position_id, error = %e, "failed to persist settled event");
                    }
                    Ok(()) => {
                        // Settled write has landed — report the running win/loss tally
                        // from the store, with the just-settled position included.
                        match store.success_rate_counts().await {
                            Ok((wins, resolved)) => {
                                WinLossStats::from_counts(wins, resolved).log("after-settlement");
                            }
                            Err(e) => warn!(error = %e, "failed to read win/loss counts after settlement"),
                        }
                    }
                }
            }
        }
    }
}

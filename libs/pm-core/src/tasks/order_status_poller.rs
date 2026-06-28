use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::watch;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::domain::OrderUpdate;
use crate::ports::{MarketClient, Store};
use crate::state::RoundSlotState;

async fn run_poll(
    client: Arc<dyn MarketClient>,
    store: Arc<dyn Store>,
    order_update_tx: broadcast::Sender<OrderUpdate>,
    mut slot_rx: watch::Receiver<RoundSlotState>,
) {
    // position_ids whose order_id we haven't resolved yet (race with executor's store write)
    let mut awaiting: HashSet<i64> = HashSet::new();
    // position_id → order_id: the set we actually poll against
    let mut tracked: HashMap<i64, String> = HashMap::new();

    let mut tick = tokio::time::interval(Duration::from_secs(2));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            // New slot state: queue position_id for order_id resolution on next tick.
            Ok(()) = slot_rx.changed() => {
                if let RoundSlotState::Pending { position_id } = *slot_rx.borrow_and_update() {
                    awaiting.insert(position_id);
                    info!(position_id, "new pending order — queued for tracking");
                }
                // Empty (rotation) or Filled don't need action here:
                // existing tracked entries remain until they reach a terminal status.
            }

            _ = tick.tick() => {
                // Resolve awaiting entries: the executor writes order_id to the store
                // slightly after setting the slot, so this may take 1–2 ticks.
                if !awaiting.is_empty() {
                    match store.open_positions().await {
                        Ok(positions) => {
                            for pos in positions.iter().filter(|p| p.order_id.is_some()) {
                                if let Some(pid) = pos.id {
                                    if awaiting.remove(&pid) {
                                        let oid = pos.order_id.clone().unwrap();
                                        info!(pid, order_id = %oid, "order_id resolved; now tracking");
                                        tracked.insert(pid, oid);
                                    }
                                }
                            }
                        }
                        Err(e) => error!(error = %e, "store query failed; will retry next tick"),
                    }
                }

                // Poll every tracked order and emit the result.
                let mut done = Vec::new();
                for (&pid, oid) in tracked.iter() {
                    match client.order_status(oid, pid).await {
                        Ok(update) => {
                            let terminal = matches!(
                                &update,
                                OrderUpdate::Filled { .. }
                                    | OrderUpdate::Rejected { .. }
                                    | OrderUpdate::Cancelled { .. }
                            );
                            info!(position_id = pid, order_id = %oid, update = ?update, "order status received");
                            if order_update_tx.send(update).is_err() {
                                return; // no receivers left; nothing left to do
                            }
                            if terminal {
                                done.push(pid);
                            }
                        }
                        Err(e) => {
                            error!(position_id = pid, order_id = %oid, error = %e, "order_status poll failed");
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

pub async fn order_status_poller_task(
    client: Arc<dyn MarketClient>,
    store: Arc<dyn Store>,
    order_update_tx: broadcast::Sender<OrderUpdate>,
    slot_rx: watch::Receiver<RoundSlotState>,
    cancel: CancellationToken,
) {
    info!("order_status_poller_task started");
    tokio::select! {
        _ = cancel.cancelled() => info!("order_status_poller_task cancelled"),
        _ = run_poll(client, store, order_update_tx, slot_rx) => {}
    }
}

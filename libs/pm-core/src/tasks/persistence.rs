use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::domain::{OrderUpdate, Settled};
use crate::ports::Store;

pub async fn persistence_task(
    store: Arc<dyn Store>,
    mut order_update_rx: mpsc::Receiver<OrderUpdate>,
    mut settled_rx: mpsc::Receiver<Settled>,
    cancel: CancellationToken,
) {
    info!("persistence_task started");
    let _ = &store;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("persistence_task cancelled");
                break;
            }
            Some(update) = order_update_rx.recv() => {
                // TODO: map OrderUpdate variants to PositionUpdate and call store.update_position
                todo!("persist order update: {:?}", update)
            }
            Some(settled) = settled_rx.recv() => {
                // TODO: map Settled to PositionUpdate::Won/Lost and call store.update_position
                todo!("persist settled event: {:?}", settled)
            }
        }
    }
}

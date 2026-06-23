use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::domain::OrderUpdate;
use crate::ports::{MarketClient, Store};
use crate::state::RoundSlotState;

pub async fn order_status_poller_task(
    client: Arc<dyn MarketClient>,
    store: Arc<dyn Store>,
    order_update_tx: mpsc::Sender<OrderUpdate>,
    mut slot_rx: watch::Receiver<RoundSlotState>,
    cancel: CancellationToken,
) {
    info!("order_status_poller_task started");
    // Bind the moved-in handles so the signature stays the real shape even while stubbed.
    let _ = (&client, &store, &order_update_tx, &mut slot_rx);
    // TODO: poll CLOB for all pending order IDs and emit OrderUpdate events.
    // On fill:   emit Filled  → triggers slot.fill() in executor (or handle here)
    // On reject: emit Rejected → triggers slot.free() so executor can retry
    // On cancel: emit Cancelled → same
    tokio::select! {
        _ = cancel.cancelled() => info!("order_status_poller_task cancelled"),
        _ = async { loop { tokio::time::sleep(Duration::from_secs(2)).await; todo!("poll pending orders") } } => {}
    }
}

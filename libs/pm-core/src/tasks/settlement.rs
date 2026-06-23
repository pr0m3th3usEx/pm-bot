use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::domain::{ActiveMarket, Settled};
use crate::ports::{MarketClient, Store};

pub async fn settlement_task(
    client: Arc<dyn MarketClient>,
    store: Arc<dyn Store>,
    market_rx: watch::Receiver<Option<ActiveMarket>>,
    settled_tx: mpsc::Sender<Settled>,
    cancel: CancellationToken,
) {
    info!("settlement_task started");
    let _ = (&client, &store, &market_rx, &settled_tx);
    tokio::select! {
        _ = cancel.cancelled() => info!("settlement_task cancelled"),
        _ = async {
            // TODO: watch market_rx for Resolved status, query open Filled positions,
            // call client.redeem(), emit Settled events.
            todo!("settlement and redemption logic")
        } => {}
    }
}

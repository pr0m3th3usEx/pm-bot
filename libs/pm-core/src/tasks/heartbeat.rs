use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::ports::MarketClient;

pub async fn heartbeat_task(
    client: Arc<dyn MarketClient>,
    interval: Duration,
    cancel: CancellationToken,
) {
    info!("heartbeat_task started");
    let mut ticker = tokio::time::interval(interval);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("heartbeat_task cancelled");
                break;
            }
            _ = ticker.tick() => {
                // polymarket_client_sdk_v2 heartbeats: Enables automatic heartbeat mechanism for authenticated sessions
                // continue
                // if let Err(e) = client.heartbeat().await {
                //     error!(error = %e, "heartbeat failed");
                // }
            }
        }
    }
}

use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::domain::Tick;
use crate::ports::PriceFeed;

pub async fn price_feed_task(
    mut feed: Box<dyn PriceFeed>,
    tick_tx: broadcast::Sender<Tick>,
    cancel: CancellationToken,
) {
    info!("price_feed_task started");
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("price_feed_task cancelled");
                break;
            }
            result = feed.next_tick() => {
                match result {
                    Ok(tick) => {
                        tracing::debug!(price = %tick.price.0, timestamp = %tick.at.0, "tick");
                        // Ignore SendError — receivers may have lagged
                        let _ = tick_tx.send(tick);
                    }
                    Err(e) => {
                        error!(error = %e, "price feed error; retrying");
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }
        }
    }
}

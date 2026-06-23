use std::sync::Arc;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::clock::MarketClock;
use crate::domain::ActiveMarket;
use crate::ports::MarketCatalog;

pub async fn market_rotation_task(
    clock: MarketClock,
    catalog: Arc<dyn MarketCatalog>,
    market_tx: watch::Sender<Option<ActiveMarket>>,
    cancel: CancellationToken,
) {
    info!("market_rotation_task started");
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("market_rotation_task cancelled");
                break;
            }
            _ = async {
                let now_secs = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                let slug = clock.current_slug(now_secs);
                info!(market_slug = %slug, "resolving current market");

                match catalog.resolve(&slug).await {
                    Ok(market) => {
                        market_tx.send(Some(market)).ok();
                        // TODO: drive MarketState machine transitions here:
                        //   - wait until closes_at → publish TradingCutoff
                        //   - wait until resolves_at then poll for Resolved
                        //   - on Resolved, republish and hand off to settlement
                        //   - on rotation, derive next slug and loop
                        todo!("drive market state transitions and rotation timing")
                    }
                    Err(e) => {
                        error!(error = %e, "failed to resolve market; will retry");
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    }
                }
            } => {}
        }
    }
}

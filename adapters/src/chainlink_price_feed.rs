use async_trait::async_trait;
use futures::StreamExt;
use pm_core::{
    domain::Tick,
    error::{CoreError, Result},
    ports::PriceFeed,
    types::{Price, Timestamp},
};
use polymarket_client_sdk_v2::rtds::Client;

/// Price feed backed by Chainlink Data Streams (BTC/USD).
///
/// V2 note: multiple feed implementations (Chainlink + others) can be combined
/// at the decision-center level to build an aggregated signal for edge detection.
pub struct ChainlinkPriceFeed {
    rx: tokio::sync::mpsc::Receiver<Result<Tick>>,
}

impl ChainlinkPriceFeed {
    pub fn connect() -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(64);

        // Spawn a forwarder task: client + stream live entirely inside the task,
        // so there's no cross-scope borrow. Messages are forwarded as Ticks.
        tokio::spawn(async move {
            let client = Client::default();
            let raw = match client.subscribe_chainlink_prices(Some("btc/usd".to_owned())) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("Chainlink subscription failed: {e}");
                    return;
                }
            };
            let mut stream = Box::pin(raw);
            while let Some(item) = stream.next().await {
                let tick = item
                    .map(|p| Tick {
                        price: Price(p.value),
                        at: Timestamp(p.timestamp),
                    })
                    .map_err(|e| CoreError::Adapter(format!("Chainlink feed: {e}")));
                if tx.send(tick).await.is_err() {
                    break;
                }
            }
        });

        Self { rx }
    }
}

#[async_trait]
impl PriceFeed for ChainlinkPriceFeed {
    async fn next_tick(&mut self) -> Result<Tick> {
        self.rx
            .recv()
            .await
            .ok_or_else(|| CoreError::Adapter("Chainlink feed closed unexpectedly".to_owned()))?
    }
}

#[cfg(test)]
mod tests {
    use pm_core::ports::PriceFeed;
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn receives_btc_usd_tick_within_timeout() {
        let mut feed = super::ChainlinkPriceFeed::connect();

        let result = timeout(Duration::from_secs(15), feed.next_tick())
            .await
            .expect("ChainlinkPriceFeed did not produce a tick within 15 seconds");

        let tick = result.expect("next_tick returned an error");

        assert!(
            tick.price.0 > rust_decimal::Decimal::ZERO,
            "tick price must be positive, got {:?}",
            tick.price
        );
        // Chainlink RTDS delivers timestamps as Unix seconds; threshold is Nov 2023.
        assert!(
            tick.at.0 > 1_700_000_000,
            "tick timestamp looks wrong: {:?}",
            tick.at
        );
    }
}

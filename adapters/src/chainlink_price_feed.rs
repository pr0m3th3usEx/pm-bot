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

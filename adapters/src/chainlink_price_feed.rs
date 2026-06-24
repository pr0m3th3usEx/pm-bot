use async_trait::async_trait;
use pm_core::{domain::Tick, error::Result, ports::PriceFeed};

/// Price feed backed by Chainlink Data Streams.
///
/// V2 note: multiple feed implementations (Chainlink + others) can be combined
/// at the decision-center level to build an aggregated signal for edge detection.
pub struct ChainlinkPriceFeed {
    // TODO: Chainlink Data Streams WebSocket/gRPC connection for BTC/USD feed
}

impl ChainlinkPriceFeed {
    pub async fn connect() -> anyhow::Result<Self> {
        todo!("connect to Chainlink Data Streams BTC/USD feed")
    }
}

#[async_trait]
impl PriceFeed for ChainlinkPriceFeed {
    async fn next_tick(&mut self) -> Result<Tick> {
        todo!("receive next BTC tick from Chainlink Data Streams")
    }
}

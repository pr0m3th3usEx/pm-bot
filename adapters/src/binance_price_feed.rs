use async_trait::async_trait;
use pm_core::{domain::Tick, error::Result, ports::PriceFeed};

pub struct BinancePriceFeed {
    // TODO: WS connection to Binance BTC/USDT stream
}

impl BinancePriceFeed {
    pub async fn connect() -> anyhow::Result<Self> {
        todo!("connect to Binance BTC WS feed")
    }
}

#[async_trait]
impl PriceFeed for BinancePriceFeed {
    async fn next_tick(&mut self) -> Result<Tick> {
        todo!("receive next BTC tick from Binance WS")
    }
}

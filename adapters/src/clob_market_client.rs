use async_trait::async_trait;
use pm_core::{
    domain::{Intent, OrderUpdate, PositionRecord},
    error::Result,
    ports::MarketClient,
    types::{Price, Shares, Side, TokenId, Usdc},
};
use polymarket_client_sdk_v2::clob::Client as ClobClient;

pub struct ClobMarketClient {
    #[allow(dead_code)]
    client: ClobClient,
}

impl ClobMarketClient {
    pub fn new(client: ClobClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl MarketClient for ClobMarketClient {
    async fn quote(&self, token_id: &TokenId, side: Side, shares: Shares) -> Result<Price> {
        let _ = (side, shares);
        todo!("fetch best quote from CLOB for token {}", token_id.0)
    }

    async fn place_order(&self, intent: &Intent, token_id: &TokenId) -> Result<String> {
        let _ = (intent, token_id);
        todo!("sign and post limit order to CLOB")
    }

    async fn cancel_order(&self, order_id: &str) -> Result<()> {
        todo!("cancel CLOB order {order_id}")
    }

    async fn order_status(&self, order_id: &str) -> Result<OrderUpdate> {
        todo!("fetch order status from CLOB for {order_id}")
    }

    async fn redeem(&self, position: &PositionRecord) -> Result<Usdc> {
        todo!("redeem winning position {:?}", position.id)
    }

    async fn heartbeat(&self) -> Result<()> {
        todo!("post CLOB heartbeat")
    }
}

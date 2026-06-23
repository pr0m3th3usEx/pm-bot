use async_trait::async_trait;
use pm_core::{
    domain::Market,
    error::Result,
    ports::MarketCatalog,
    types::MarketSlug,
};
use polymarket_client_sdk_v2::gamma::Client as GammaClient;

pub struct GammaMarketCatalog {
    #[allow(dead_code)]
    client: GammaClient,
}

impl GammaMarketCatalog {
    pub fn new(client: GammaClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl MarketCatalog for GammaMarketCatalog {
    async fn resolve(&self, slug: &MarketSlug) -> Result<Market> {
        // TODO: call gamma client.market_by_slug, map response to pm_core::Market
        todo!("resolve market slug via Gamma API: {slug}")
    }
}

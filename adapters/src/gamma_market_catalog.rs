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
        // TODO: call gamma_client.market_by_slug(slug), then:
        // - map clob_token_ids + outcomes → Vec<MarketOutcome>
        // - read strike from the Market Event data (event.strike or equivalent field)
        // - set closes_at = resolves_at (they are the same in Polymarket markets)
        // - map opens_at / resolves_at from the API timestamps
        todo!("resolve market slug via Gamma API: {slug}")
    }
}

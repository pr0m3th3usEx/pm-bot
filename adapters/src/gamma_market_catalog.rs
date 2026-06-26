use async_trait::async_trait;
use chrono::{DateTime, Utc};
use pm_core::{
    domain::{Market, MarketOutcome},
    error::{CoreError, Result},
    ports::MarketCatalog,
    types::{MarketSlug, MarketStatus, MarketType, Price, Timestamp, TokenId},
};
use polymarket_client_sdk_v2::gamma::{
    types::{request::MarketBySlugRequest, response::Market as PMMarket},
    Client as GammaClient,
};
use rust_decimal::Decimal;
use std::str::FromStr;
use tracing::info;

pub const GAMMA_API_URL: &str = "https://gamma-api.polymarket.com";
const CRYPTO_PRICE_URL: &str = "https://polymarket.com/api/crypto/crypto-price";

pub struct GammaMarketCatalog {
    client: GammaClient,
    http: reqwest::Client,
}

impl GammaMarketCatalog {
    pub fn new() -> Self {
        Self {
            client: GammaClient::new(GAMMA_API_URL).expect("could build Gamma API client"),
            http: reqwest::Client::new(),
        }
    }

    fn map_outcomes(&self, slug: &MarketSlug, response: &PMMarket) -> Result<Vec<MarketOutcome>> {
        let token_ids = response
            .clob_token_ids
            .clone()
            .ok_or_else(|| CoreError::Adapter(format!("missing clob_token_ids for {slug}")))?;
        let names = response
            .outcomes
            .clone()
            .ok_or_else(|| CoreError::Adapter(format!("missing outcomes for {slug}")))?;

        if token_ids.len() != names.len() {
            return Err(CoreError::Adapter(format!(
                "clob_token_ids/outcomes length mismatch for {slug}: {} vs {}",
                token_ids.len(),
                names.len()
            )));
        }

        Ok(token_ids
            .into_iter()
            .zip(names)
            .map(|(id, name)| MarketOutcome {
                name,
                token_id: TokenId(id),
            })
            .collect())
    }

    fn detect_market_type(response: &PMMarket) -> MarketType {
        response
            .tags
            .as_deref()
            .unwrap_or_default()
            .iter()
            .any(|t| t.slug.as_deref() == Some("up-or-down"))
            .then_some(MarketType::UpDown)
            .unwrap_or(MarketType::Other)
    }

    // Fetch open / close prices for UpDown markets from the crypto-prices API. This is used to determine the "price to beat" (openPrice) for the NEXT round, which is needed during resolution of the current round.
    async fn fetch_crypto_prices(
        &self,
        slug: &MarketSlug,
        round_start: DateTime<Utc>,
    ) -> Result<CryptoPricesResponse> {
        info!(slug = %slug, start = %round_start, "fetching crypto-prices (openPrice) for UpDown strike");

        let resp = self
            .http
            .get(CRYPTO_PRICE_URL)
            .query(&[
                ("symbol", "BTC"),
                ("variant", "fiveminute"),
                (
                    "eventStartTime",
                    round_start.timestamp().to_string().as_str(),
                ),
            ])
            .send()
            .await
            .map_err(|e| CoreError::Adapter(format!("crypto-prices request failed: {e}")))?;

        let status = resp.status();

        if !status.is_success() {
            return Err(CoreError::Adapter(format!(
                "crypto-prices request failed with status {status}"
            )));
        }

        let response = resp
            .json::<CryptoPricesResponse>()
            .await
            .map_err(|e| CoreError::Adapter(format!("crypto-prices parse failed: {e}")))?;

        Ok(response)
    }

    /// Fetches the "openPrice" (price to beat) for the NEXT round. Called during resolve of the current market.
    /// This is a separate API call because the Gamma market-by-slug response does not include
    fn updown_strike(&self, strike: Option<f64>) -> Result<Option<Price>> {
        let Some(strike) = strike else {
            return Ok(None);
        };

        let d = Decimal::from_str(&strike.to_string())
            .map_err(|e| CoreError::Adapter(format!("bad openPrice '{strike}': {e}")))?;

        Ok(Some(Price(d)))
    }
}

// ─── Crypto-prices response types ─────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct CryptoPricesResponse {
    #[serde(rename = "openPrice")]
    open_price: f64,
    #[allow(dead_code)]
    #[serde(rename = "closePrice")]
    close_price: Option<f64>,
    #[allow(dead_code)]
    timestamp: i64,
    #[allow(dead_code)]
    completed: bool,
    #[allow(dead_code)]
    incomplete: bool,
    #[allow(dead_code)]
    cached: bool,
}

// ─── MarketCatalog impl ───────────────────────────────────────────────────────

#[async_trait]
impl MarketCatalog for GammaMarketCatalog {
    async fn resolve(&self, slug: &MarketSlug) -> Result<Market> {
        let request = MarketBySlugRequest::builder()
            .slug(slug.to_string())
            .include_tag(true)
            .build();

        let response = self
            .client
            .market_by_slug(&request)
            .await
            .map_err(|e| CoreError::Adapter(format!("Gamma API error: {e}")))?;

        let market_id = response.id.clone();
        let question_id = response
            .question_id
            .clone()
            .ok_or_else(|| CoreError::Adapter(format!("missing question_id for {slug}")))?;
        let condition_id = response
            .condition_id
            .clone()
            .ok_or_else(|| CoreError::Adapter(format!("missing condition_id for {slug}")))?;

        let outcomes = self.map_outcomes(slug, &response)?;
        let market_type = Self::detect_market_type(&response);

        // Get exact start time from the event, which is the opens_at for this market.
        let Some(events) = response.events else {
            return Err(CoreError::Adapter(format!("missing events for {slug}")));
        };
        let event = events
            .get(0)
            .ok_or_else(|| CoreError::Adapter(format!("missing event for {slug}")))?;

        let opens_at = event
            .start_time
            .ok_or_else(|| CoreError::Adapter(format!("missing start_time for {slug}")))?;
        let closes_at = event
            .end_date
            .ok_or_else(|| CoreError::Adapter(format!("missing end_date for {slug}")))?;
        let resolves_at = closes_at;

        let active = response
            .active
            .ok_or_else(|| CoreError::Adapter(format!("missing active for {slug}")))?;

        let crypto_prices = self.fetch_crypto_prices(slug, opens_at).await?;

        let status = match (
            crypto_prices.completed,
            crypto_prices.incomplete,
            active,
            closes_at,
        ) {
            (completed, _, _, close_time) if completed || close_time <= Utc::now() => {
                MarketStatus::Resolved
            }
            (false, true, true, _) => MarketStatus::Open,
            (complete, _, _, _) => {
                if !complete && closes_at > Utc::now() {
                    MarketStatus::Resolving
                } else {
                    MarketStatus::Pending
                }
            }
        };

        // Fetch strike for UpDown markets from the previous round's past-results.
        let strike = if market_type == MarketType::UpDown {
            self.updown_strike(Some(crypto_prices.open_price))?
        } else {
            None
        };

        Ok(Market {
            slug: slug.clone(),
            market_type,
            event_id: market_id,
            question_id,
            condition_id,
            outcomes,
            strike,
            opens_at: Timestamp(opens_at.timestamp_millis()),
            closes_at: Timestamp(closes_at.timestamp_millis()),
            resolves_at: Timestamp(resolves_at.timestamp_millis()),
            status,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use pm_core::ports::MarketCatalog;
    use rust_decimal::Decimal;

    fn catalog() -> super::GammaMarketCatalog {
        super::GammaMarketCatalog::new()
    }

    #[tokio::test]
    async fn resolves_updown_market() {
        let catalog = catalog();
        let slug = super::MarketSlug("btc-updown-5m-1782287100".into());
        let market = catalog.resolve(&slug).await.expect("resolve failed");

        assert_eq!(market.slug, slug);
        assert_eq!(market.market_type, super::MarketType::UpDown);
        assert_eq!(market.status, super::MarketStatus::Resolved);
        assert_eq!(
            market.strike,
            Some(super::Price(
                Decimal::from_str("62613.822688538385").unwrap()
            ))
        );
    }
}

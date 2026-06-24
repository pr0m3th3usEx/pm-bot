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

pub const GAMMA_API_URL: &str = "https://gamma-api.polymarket.com";
const PAST_RESULTS_URL: &str = "https://polymarket.com/api/past-results";
const INTERVAL_SECS: i64 = 300; // matches MarketClock::btc_5m

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

    /// Fetch the closePrice of the current round's past-result, which is the strike
    /// (price to beat) for the NEXT round. Called during resolve of the current market.
    async fn fetch_updown_strike(
        &self,
        slug: &MarketSlug,
        round_start: DateTime<Utc>,
    ) -> Result<Option<Price>> {
        let round_end = round_start + chrono::Duration::seconds(INTERVAL_SECS);
        let start_iso = round_start.format("%Y-%m-%dT%H:%M:%SZ").to_string();
        let end_iso = round_end.format("%Y-%m-%dT%H:%M:%SZ").to_string();

        println!("fetch_updown_strike: slug={slug}, start={start_iso}, end={end_iso}");

        let resp = self
            .http
            .get(PAST_RESULTS_URL)
            .query(&[
                ("symbol", "BTC"),
                ("variant", "fiveminute"),
                ("assetType", "crypto"),
                ("currentEventStartTime", start_iso.as_str()),
                ("count", "1"),
                ("endDate", end_iso.as_str()),
                ("includeOutcomesBySlug", "true"),
                ("pastEventSlugs", slug.0.as_str()),
            ])
            .send()
            .await
            .map_err(|e| CoreError::Adapter(format!("past-results request failed: {e}")))?
            .json::<PastResultsResponse>()
            .await
            .map_err(|e| CoreError::Adapter(format!("past-results parse failed: {e}")))?;

        let close_price = resp.data.results.into_iter().next().map(|r| r.close_price);

        match close_price {
            None => Ok(None),
            Some(f) => {
                let d = Decimal::from_str(&f.to_string())
                    .map_err(|e| CoreError::Adapter(format!("bad closePrice '{f}': {e}")))?;
                Ok(Some(Price(d)))
            }
        }
    }
}

// ─── Past-results response types ─────────────────────────────────────────────

#[derive(serde::Deserialize)]
struct PastResultsResponse {
    data: PastResultsData,
}

#[derive(serde::Deserialize)]
struct PastResultsData {
    results: Vec<PastResult>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct PastResult {
    close_price: f64,
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
            .closed_time
            .ok_or_else(|| CoreError::Adapter(format!("missing end_time for {slug}")))?;
        let resolves_at = closes_at;

        let active = response
            .active
            .ok_or_else(|| CoreError::Adapter(format!("missing active for {slug}")))?;
        let closed = response
            .closed
            .ok_or_else(|| CoreError::Adapter(format!("missing closed for {slug}")))?;

        let status = match (active, closed) {
            (true, false) => MarketStatus::Open,
            (false, false) => MarketStatus::Pending,
            (_, true) => MarketStatus::Resolved,
        };

        println!("opens_at: {opens_at}, closes_at: {closes_at}, resolves_at: {resolves_at}, status: {status:?}, market_type: {market_type:?}");

        // Fetch strike for UpDown markets from the previous round's past-results.
        let strike = if market_type == MarketType::UpDown {
            self.fetch_updown_strike(slug, opens_at).await?
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

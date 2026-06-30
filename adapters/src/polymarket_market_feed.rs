use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use pm_core::{
    domain::OutcomeBook,
    error::{CoreError, Result},
    ports::MarketDataFeed,
    types::{Price, Timestamp, TokenId},
};
use polymarket_client_sdk_v2::types::U256;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::str::FromStr;
use tokio_tungstenite::tungstenite::Message;

const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

/// Real-time outcome-price feed backed by the Polymarket market WebSocket channel.
///
/// Connects to `wss://ws-subscriptions-clob.polymarket.com/ws/market`, subscribes to
/// the given asset ids, and emits one `OutcomeBook` per update. Reconnects automatically
/// with exponential backoff on disconnection.
pub struct PolymarketMarketFeed {
    rx: tokio::sync::mpsc::Receiver<Result<OutcomeBook>>,
}

// ─── Wire protocol types ─────────────────────────────────────────────────────

/// One level in a book snapshot (price + size strings; size is ignored for our purposes).
#[derive(Debug, Deserialize)]
struct Lvl {
    price: String,
    #[allow(dead_code)]
    size: String,
}

/// An element inside `price_changes`.
#[derive(Debug, Deserialize)]
struct PxChange {
    asset_id: String,
    best_bid: Option<String>,
    best_ask: Option<String>,
}

/// Internally-tagged enum covering all message types we care about.
#[derive(Debug, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
enum MarketMsg {
    Book {
        asset_id: String,
        bids: Vec<Lvl>,
        asks: Vec<Lvl>,
        timestamp: String,
    },
    BestBidAsk {
        asset_id: String,
        best_bid: Option<String>,
        best_ask: Option<String>,
        timestamp: String,
    },
    PriceChange {
        price_changes: Vec<PxChange>,
        timestamp: String,
    },
    #[serde(other)]
    Other,
}

// ─── Price parsing helper ─────────────────────────────────────────────────────

/// Parse a decimal string from Polymarket to a `Price`.
///
/// Tolerates leading-dot form like ".48" by prepending "0" before retrying.
/// Returns `None` on parse failure or empty string.
fn parse_price(s: &str) -> Option<Price> {
    if s.is_empty() {
        return None;
    }
    match Decimal::from_str(s) {
        Ok(d) => Some(Price(d)),
        Err(_) if s.starts_with('.') => {
            let prefixed = format!("0{s}");
            Decimal::from_str(&prefixed).ok().map(Price)
        }
        Err(_) => None,
    }
}

/// Parse the decimal-ms timestamp string to a `Timestamp`.
fn parse_timestamp(s: &str) -> Timestamp {
    Timestamp(s.parse::<i64>().unwrap_or(0))
}

/// Parse a Polymarket asset-id decimal string into a TokenId. None on failure.
fn parse_token_id(s: &str) -> Option<TokenId> {
    U256::from_str(s).ok().map(TokenId)
}

// ─── Message → OutcomeBook conversion ────────────────────────────────────────

/// Convert a `MarketMsg` into zero or more `OutcomeBook` values.
fn to_outcome_books(msg: MarketMsg) -> Vec<OutcomeBook> {
    match msg {
        MarketMsg::Book {
            asset_id,
            bids,
            asks,
            timestamp,
        } => {
            let Some(token_id) = parse_token_id(&asset_id) else {
                return vec![];
            };
            // best ask = min price over asks; best bid = max price over bids (do NOT assume ordering).
            let buy_price = asks
                .iter()
                .filter_map(|lvl| parse_price(&lvl.price))
                .min_by(|a, b| a.0.cmp(&b.0));
            let sell_price = bids
                .iter()
                .filter_map(|lvl| parse_price(&lvl.price))
                .max_by(|a, b| a.0.cmp(&b.0));

            vec![OutcomeBook {
                token_id,
                buy_price,
                sell_price,
                at: parse_timestamp(&timestamp),
            }]
        }
        MarketMsg::BestBidAsk {
            asset_id,
            best_bid,
            best_ask,
            timestamp,
        } => {
            let Some(token_id) = parse_token_id(&asset_id) else {
                return vec![];
            };
            let buy_price = best_ask.as_deref().and_then(parse_price);
            let sell_price = best_bid.as_deref().and_then(parse_price);
            vec![OutcomeBook {
                token_id,
                buy_price,
                sell_price,
                at: parse_timestamp(&timestamp),
            }]
        }
        MarketMsg::PriceChange {
            price_changes,
            timestamp,
        } => {
            let at = parse_timestamp(&timestamp);
            price_changes
                .into_iter()
                .filter_map(|pc| {
                    let token_id = parse_token_id(&pc.asset_id)?;
                    Some(OutcomeBook {
                        token_id,
                        buy_price: pc.best_ask.as_deref().and_then(parse_price),
                        sell_price: pc.best_bid.as_deref().and_then(parse_price),
                        at,
                    })
                })
                .collect()
        }
        MarketMsg::Other => vec![],
    }
}

// ─── Connection + background task ────────────────────────────────────────────

impl PolymarketMarketFeed {
    pub fn connect(asset_ids: Vec<String>) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(256);

        tokio::spawn(async move {
            let subscribe_payload = serde_json::json!({
                "assets_ids": asset_ids,
                "type": "market",
                "custom_feature_enabled": true,
            });
            let subscribe_frame = subscribe_payload.to_string();

            let mut backoff_ms: u64 = 1_000;
            const MAX_BACKOFF_MS: u64 = 30_000;

            'outer: loop {
                // ── Connect ───────────────────────────────────────────────
                let stream = match tokio_tungstenite::connect_async(WS_URL).await {
                    Ok((s, _)) => {
                        tracing::info!("Connected to Polymarket market WS");
                        backoff_ms = 1_000; // reset on successful connect
                        s
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "Failed to connect to Polymarket market WS — retrying in {}ms", backoff_ms);
                        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                        backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
                        continue;
                    }
                };

                let (mut write, mut read) = stream.split();

                // ── Subscribe ─────────────────────────────────────────────
                if let Err(e) = write
                    .send(Message::Text(subscribe_frame.clone().into()))
                    .await
                {
                    tracing::error!(error = %e, "Failed to send Polymarket subscribe frame — retrying");
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
                    continue;
                }

                // ── Read loop ─────────────────────────────────────────────
                loop {
                    match read.next().await {
                        Some(Ok(Message::Text(text))) => {
                            match serde_json::from_str::<MarketMsg>(&text) {
                                Ok(msg) => {
                                    for book in to_outcome_books(msg) {
                                        if tx.send(Ok(book)).await.is_err() {
                                            // Receiver dropped — exit cleanly.
                                            break 'outer;
                                        }
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!(error = %e, raw = %text, "Failed to parse Polymarket market message — skipping");
                                }
                            }
                        }
                        Some(Ok(Message::Ping(p))) => {
                            if let Err(e) = write.send(Message::Pong(p)).await {
                                tracing::error!(error = %e, "Failed to send pong to Polymarket WS — reconnecting");
                                break; // inner loop → reconnect
                            }
                        }
                        Some(Ok(Message::Close(_))) => {
                            tracing::warn!("Polymarket market WS closed by server — reconnecting");
                            break; // inner loop → reconnect
                        }
                        Some(Ok(_)) => {} // ignore other frame types
                        Some(Err(e)) => {
                            tracing::error!(error = %e, "Polymarket market WS error — reconnecting");
                            break; // inner loop → reconnect
                        }
                        None => {
                            tracing::warn!("Polymarket market WS stream ended — reconnecting");
                            break; // inner loop → reconnect
                        }
                    }
                }

                // Backoff before reconnect attempt.
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
            }
        });

        Self { rx }
    }
}

#[async_trait]
impl MarketDataFeed for PolymarketMarketFeed {
    async fn next_update(&mut self) -> Result<OutcomeBook> {
        self.rx.recv().await.ok_or_else(|| {
            CoreError::Adapter("Polymarket market feed closed unexpectedly".to_owned())
        })?
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse-only unit test — no network required.
    ///
    /// Deserialises literal `best_bid_ask` and `book` sample frames and asserts
    /// that `buy_price`/`sell_price`/`timestamp` are mapped correctly, including
    /// the leading-dot ".48"/".52" book case.
    #[test]
    fn parse_market_messages() {
        use pm_core::types::TokenId;
        use polymarket_client_sdk_v2::types::U256;

        // 1. best_bid_ask frame
        let raw_bba = r#"{
            "event_type": "best_bid_ask",
            "asset_id": "12345",
            "best_bid": "0.48",
            "best_ask": "0.52",
            "timestamp": "1700000000000"
        }"#;

        let msg: MarketMsg = serde_json::from_str(raw_bba).expect("should parse best_bid_ask");
        let books = to_outcome_books(msg);
        assert_eq!(books.len(), 1);
        let b = &books[0];
        assert_eq!(b.token_id, TokenId(U256::from(12345u64)));
        // buy_price = best_ask = 0.52
        assert_eq!(b.buy_price.as_ref().map(|p| p.0.to_string()), Some("0.52".to_string()));
        // sell_price = best_bid = 0.48
        assert_eq!(b.sell_price.as_ref().map(|p| p.0.to_string()), Some("0.48".to_string()));
        assert_eq!(b.at.0, 1700000000000_i64);

        // 2. book frame with leading-dot prices (".48" / ".52")
        let raw_book = r#"{
            "event_type": "book",
            "asset_id": "99999",
            "bids": [
                {"price": ".45", "size": "100"},
                {"price": ".48", "size": "200"},
                {"price": ".40", "size": "50"}
            ],
            "asks": [
                {"price": ".55", "size": "80"},
                {"price": ".52", "size": "120"},
                {"price": ".60", "size": "30"}
            ],
            "timestamp": "1700000001000"
        }"#;

        let msg: MarketMsg = serde_json::from_str(raw_book).expect("should parse book");
        let books = to_outcome_books(msg);
        assert_eq!(books.len(), 1);
        let b = &books[0];
        assert_eq!(b.token_id, TokenId(U256::from(99999u64)));
        // best bid = max(0.45, 0.48, 0.40) = 0.48
        assert_eq!(
            b.sell_price.as_ref().map(|p| p.0.to_string()),
            Some("0.48".to_string()),
            "sell_price should be best bid (max of bids)"
        );
        // best ask = min(0.55, 0.52, 0.60) = 0.52
        assert_eq!(
            b.buy_price.as_ref().map(|p| p.0.to_string()),
            Some("0.52".to_string()),
            "buy_price should be best ask (min of asks)"
        );
        assert_eq!(b.at.0, 1700000001000_i64);

        // 3. price_change frame with multiple asset ids (numeric)
        let raw_pc = r#"{
            "event_type": "price_change",
            "price_changes": [
                {"asset_id": "111", "best_bid": "0.30", "best_ask": "0.70", "price": "0.50", "size": "10", "side": "buy", "hash": "abc"},
                {"asset_id": "222", "best_bid": "0.40", "best_ask": "0.60", "price": "0.45", "size": "5", "side": "sell", "hash": "def"}
            ],
            "timestamp": "1700000002000"
        }"#;

        let msg: MarketMsg = serde_json::from_str(raw_pc).expect("should parse price_change");
        let books = to_outcome_books(msg);
        assert_eq!(books.len(), 2);
        assert_eq!(books[0].token_id, TokenId(U256::from(111u64)));
        assert_eq!(books[0].buy_price.as_ref().map(|p| p.0.to_string()), Some("0.70".to_string()));
        assert_eq!(books[0].sell_price.as_ref().map(|p| p.0.to_string()), Some("0.30".to_string()));
        assert_eq!(books[1].token_id, TokenId(U256::from(222u64)));
        assert_eq!(books[1].buy_price.as_ref().map(|p| p.0.to_string()), Some("0.60".to_string()));
        assert_eq!(books[1].sell_price.as_ref().map(|p| p.0.to_string()), Some("0.40".to_string()));
        // Both use the same top-level timestamp
        assert_eq!(books[0].at.0, 1700000002000_i64);
        assert_eq!(books[1].at.0, 1700000002000_i64);

        // 4. parse_price helper: leading-dot tolerance
        let p = parse_price(".48").expect("should parse .48");
        assert_eq!(p.0.to_string(), "0.48");

        // 5. Other variant is a no-op
        let raw_other = r#"{"event_type": "tick_size_change", "asset_id": "xxx"}"#;
        let msg: MarketMsg = serde_json::from_str(raw_other).expect("should parse as Other");
        let books = to_outcome_books(msg);
        assert!(books.is_empty());
    }
}

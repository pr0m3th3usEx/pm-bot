use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use pm_core::{
    domain::{BookSnapshot, ExchangeId, TopOfBook},
    error::{CoreError, Result},
    ports::OrderBookFeed,
    types::Timestamp,
};
use serde::Deserialize;
use tokio_tungstenite::tungstenite::Message;

/// Order-book feed backed by the Binance Futures `bookTicker` stream (BTC, L1 only).
///
/// Connects to the combined-stream endpoint for `btcusdt@bookTicker` and emits one
/// `BookSnapshot` per update. Volumes are expressed as **USD notional** (`price × qty`).
pub struct BinanceOrderBookFeed {
    rx: tokio::sync::mpsc::Receiver<Result<BookSnapshot>>,
}

/// Wrapper produced by the combined-stream endpoint:
/// `{"stream":"btcusdt@bookTicker","data":{...}}`
#[derive(Debug, Deserialize)]
struct StreamEnvelope {
    #[allow(dead_code)]
    stream: String,
    data: BinanceBookTickerData,
}

/// Payload of a `bookTicker` event.
#[derive(Debug, Deserialize)]
struct BinanceBookTickerData {
    /// Best bid price (String).
    #[serde(rename = "b")]
    best_bid_price: String,
    /// Best bid qty in BTC (String).
    #[serde(rename = "B")]
    best_bid_qty: String,
    /// Best ask price (String).
    #[serde(rename = "a")]
    best_ask_price: String,
    /// Best ask qty in BTC (String).
    #[serde(rename = "A")]
    best_ask_qty: String,
    /// Event time (Unix milliseconds).
    #[serde(rename = "E")]
    event_time: i64,
}

/// Map a deserialized `BinanceBookTickerData` to a `BookSnapshot`.
///
/// Volumes are computed as USD notional: `bid_vol = bid_price × bid_qty`,
/// `ask_vol = ask_price × ask_qty`.
fn to_book_snapshot(data: BinanceBookTickerData) -> BookSnapshot {
    let bid_price: f64 = data.best_bid_price.parse().unwrap_or(0.0);
    let bid_qty: f64 = data.best_bid_qty.parse().unwrap_or(0.0);
    let ask_price: f64 = data.best_ask_price.parse().unwrap_or(0.0);
    let ask_qty: f64 = data.best_ask_qty.parse().unwrap_or(0.0);

    BookSnapshot {
        exchange: ExchangeId::Binance,
        top: TopOfBook {
            bid_price,
            bid_vol: bid_price * bid_qty,
            ask_price,
            ask_vol: ask_price * ask_qty,
        },
        at: Timestamp(data.event_time),
    }
}

impl BinanceOrderBookFeed {
    pub fn connect() -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(64);

        tokio::spawn(async move {
            let stream = match tokio_tungstenite::connect_async(
                "wss://fstream.binance.com/public/stream?streams=btcusdt@bookTicker",
            )
            .await
            {
                Ok((stream, _)) => {
                    tracing::info!("Connected to Binance bookTicker stream");
                    stream
                }
                Err(e) => {
                    tracing::error!("Failed to connect to Binance bookTicker stream: {e}");
                    return;
                }
            };

            let (mut write, mut read) = stream.split();

            loop {
                match read.next().await {
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<StreamEnvelope>(&text) {
                            Ok(envelope) => {
                                let snapshot = to_book_snapshot(envelope.data);
                                if tx.send(Ok(snapshot)).await.is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                tracing::error!(
                                    "Failed to parse Binance bookTicker message: {e}"
                                );
                            }
                        }
                    }
                    Some(Ok(Message::Ping(p))) => {
                        if let Err(e) = write.send(Message::Pong(p)).await {
                            tracing::error!("Failed to send pong to Binance: {e}");
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) => {
                        tracing::error!("Binance bookTicker stream closed by server");
                        break;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        tracing::error!("Binance bookTicker WebSocket error: {e}");
                        break;
                    }
                    None => {
                        tracing::error!("Binance bookTicker stream ended");
                        break;
                    }
                }
            }
        });

        Self { rx }
    }
}

#[async_trait]
impl OrderBookFeed for BinanceOrderBookFeed {
    async fn next_book(&mut self) -> Result<BookSnapshot> {
        self.rx
            .recv()
            .await
            .ok_or_else(|| CoreError::Adapter("Binance order-book feed closed unexpectedly".to_owned()))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pm_core::ports::OrderBookFeed;
    use std::time::Duration;
    use tokio::time::timeout;

    /// Parse-only unit test — no network required.
    #[test]
    fn parse_book_ticker_payload() {
        let raw = r#"{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","u":10928791213494,"s":"BTCUSDT","ps":"BTCUSDT","b":"59546.40","B":"6.582","a":"59546.50","A":"10.697","T":1782801379879,"E":1782801379879,"st":0}}"#;

        let envelope: StreamEnvelope =
            serde_json::from_str(raw).expect("should deserialize envelope");
        let snapshot = to_book_snapshot(envelope.data);

        let expected_bid_vol = 59546.40_f64 * 6.582_f64;
        let expected_ask_vol = 59546.50_f64 * 10.697_f64;
        let epsilon = 0.01;

        assert!(
            (snapshot.top.bid_vol - expected_bid_vol).abs() < epsilon,
            "bid_vol mismatch: got {}, expected {}",
            snapshot.top.bid_vol,
            expected_bid_vol,
        );
        assert!(
            (snapshot.top.ask_vol - expected_ask_vol).abs() < epsilon,
            "ask_vol mismatch: got {}, expected {}",
            snapshot.top.ask_vol,
            expected_ask_vol,
        );
        assert!(
            (snapshot.top.bid_price - 59546.40).abs() < epsilon,
            "bid_price mismatch: got {}",
            snapshot.top.bid_price,
        );
        assert_eq!(snapshot.at.0, 1782801379879_i64, "event time mismatch");
        assert_eq!(snapshot.exchange, ExchangeId::Binance);
    }

    /// Live smoke test — requires network. Mirrors BinancePriceFeed's timeout test.
    #[tokio::test]
    async fn receives_book_snapshot_within_timeout() {
        let mut feed = BinanceOrderBookFeed::connect();

        let result = timeout(Duration::from_secs(15), feed.next_book())
            .await
            .expect("BinanceOrderBookFeed did not produce a snapshot within 15 seconds");

        let snapshot = result.expect("next_book returned an error");

        assert!(
            snapshot.top.bid_vol > 0.0,
            "bid_vol must be positive, got {}",
            snapshot.top.bid_vol,
        );
        assert!(
            snapshot.top.ask_vol > 0.0,
            "ask_vol must be positive, got {}",
            snapshot.top.ask_vol,
        );
        assert_eq!(snapshot.exchange, ExchangeId::Binance);
        assert!(
            snapshot.at.0 > 1_700_000_000_000_i64,
            "timestamp looks wrong (expected ms): {}",
            snapshot.at.0,
        );
    }
}

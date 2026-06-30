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

/// Order-book feed backed by the OKX `bbo-tbt` WebSocket channel (BTC-USDT spot, L1 only).
///
/// Connects to `wss://ws.okx.com:8443/ws/v5/public`, sends a subscribe frame for the
/// `bbo-tbt` channel, and emits one `BookSnapshot` per tick. Volumes are expressed as
/// **USD notional** (`price × size`).
pub struct OkxOrderBookFeed {
    rx: tokio::sync::mpsc::Receiver<Result<BookSnapshot>>,
}

/// Top-level OKX WebSocket message. Covers both data frames and control frames
/// (subscribe ack, error) without deserialisation errors on non-data messages.
#[derive(Debug, Deserialize)]
struct OkxMessage {
    /// Set on control frames (`"subscribe"`, `"error"`, etc.).
    #[serde(default)]
    event: Option<String>,
    /// Set on data frames; absent (or empty) on acks and errors.
    #[serde(default)]
    data: Option<Vec<OkxBboData>>,
}

/// Payload of a single `bbo-tbt` data element.
#[derive(Debug, Deserialize)]
struct OkxBboData {
    /// Best-ask levels: each level is `[price, size, liquidated_qty, order_count]`
    /// as strings.
    asks: Vec<Vec<String>>,
    /// Best-bid levels: same layout as `asks`.
    bids: Vec<Vec<String>>,
    /// Event timestamp as a string of Unix milliseconds.
    ts: String,
}

/// Map a deserialized `OkxBboData` to a `BookSnapshot`.
///
/// Returns `None` when either side of the book is empty (one-sided snapshot).
/// Volumes are computed as USD notional: `bid_vol = bid_price × bid_size`,
/// `ask_vol = ask_price × ask_size`.
fn to_book_snapshot(data: &OkxBboData) -> Option<BookSnapshot> {
    let bid_level = data.bids.first()?;
    let ask_level = data.asks.first()?;

    let bid_price: f64 = bid_level.first()?.parse().unwrap_or(0.0);
    let bid_size: f64 = bid_level.get(1)?.parse().unwrap_or(0.0);
    let ask_price: f64 = ask_level.first()?.parse().unwrap_or(0.0);
    let ask_size: f64 = ask_level.get(1)?.parse().unwrap_or(0.0);
    let ts: i64 = data.ts.parse().unwrap_or(0);

    Some(BookSnapshot {
        exchange: ExchangeId::Okx,
        top: TopOfBook {
            bid_price,
            bid_vol: bid_price * bid_size,
            ask_price,
            ask_vol: ask_price * ask_size,
        },
        at: Timestamp(ts),
    })
}

impl OkxOrderBookFeed {
    pub fn connect() -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(64);

        tokio::spawn(async move {
            let stream = match tokio_tungstenite::connect_async(
                "wss://ws.okx.com:8443/ws/v5/public",
            )
            .await
            {
                Ok((stream, _)) => {
                    tracing::info!("Connected to OKX bbo-tbt stream");
                    stream
                }
                Err(e) => {
                    tracing::error!("Failed to connect to OKX bbo-tbt stream: {e}");
                    return;
                }
            };

            let (mut write, mut read) = stream.split();

            // OKX requires an explicit subscribe frame after connecting.
            let subscribe_msg =
                r#"{"op":"subscribe","args":[{"channel":"bbo-tbt","instId":"BTC-USDT"}]}"#;
            if let Err(e) = write
                .send(Message::Text(subscribe_msg.to_owned().into()))
                .await
            {
                tracing::error!("Failed to send OKX subscribe frame: {e}");
                return;
            }

            loop {
                match read.next().await {
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<OkxMessage>(&text) {
                            Ok(msg) => {
                                if let Some(event) = &msg.event {
                                    if event == "error" {
                                        tracing::error!("OKX WebSocket error frame: {text}");
                                    }
                                    // subscribe ack and other non-data events: skip silently.
                                    continue;
                                }
                                match msg.data {
                                    Some(ref frames) if !frames.is_empty() => {
                                        if let Some(snapshot) = to_book_snapshot(&frames[0]) {
                                            if tx.send(Ok(snapshot)).await.is_err() {
                                                break;
                                            }
                                        }
                                    }
                                    _ => {} // empty data array or None: skip silently.
                                }
                            }
                            Err(e) => {
                                tracing::error!("Failed to parse OKX bbo-tbt message: {e}");
                            }
                        }
                    }
                    Some(Ok(Message::Ping(p))) => {
                        if let Err(e) = write.send(Message::Pong(p)).await {
                            tracing::error!("Failed to send pong to OKX: {e}");
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) => {
                        tracing::error!("OKX bbo-tbt stream closed by server");
                        break;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        tracing::error!("OKX bbo-tbt WebSocket error: {e}");
                        break;
                    }
                    None => {
                        tracing::error!("OKX bbo-tbt stream ended");
                        break;
                    }
                }
            }
        });

        Self { rx }
    }
}

#[async_trait]
impl OrderBookFeed for OkxOrderBookFeed {
    async fn next_book(&mut self) -> Result<BookSnapshot> {
        self.rx.recv().await.ok_or_else(|| {
            CoreError::Adapter("OKX order-book feed closed unexpectedly".to_owned())
        })?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pm_core::ports::OrderBookFeed;
    use std::time::Duration;
    use tokio::time::timeout;

    /// Parse-only unit test — no network required.
    ///
    /// Deserialises a literal `bbo-tbt` sample frame and verifies that
    /// `to_book_snapshot` produces correct USD-notional volumes and timestamp.
    #[test]
    fn parse_bbo_payload() {
        let raw = r#"{
            "arg": {"channel": "bbo-tbt", "instId": "BTC-USDT"},
            "data": [{
                "asks": [["69010.5", "0.5432", "0", "1"]],
                "bids": [["69000.0", "1.2140", "0", "2"]],
                "ts": "1638230123456"
            }]
        }"#;

        let msg: OkxMessage = serde_json::from_str(raw).expect("should deserialize OkxMessage");
        let frames = msg.data.expect("data must be present");
        let snapshot = to_book_snapshot(&frames[0]).expect("to_book_snapshot must return Some");

        let expected_bid_vol = 69000.0_f64 * 1.2140_f64;
        let expected_ask_vol = 69010.5_f64 * 0.5432_f64;
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
            (snapshot.top.bid_price - 69000.0).abs() < epsilon,
            "bid_price mismatch: got {}",
            snapshot.top.bid_price,
        );
        assert_eq!(snapshot.at.0, 1638230123456_i64, "timestamp mismatch");
        assert_eq!(snapshot.exchange, ExchangeId::Okx);
    }

    /// Live smoke test — requires network. Connects to OKX and waits up to 15 s for
    /// the first `BookSnapshot`.
    #[tokio::test]
    async fn receives_book_snapshot_within_timeout() {
        let mut feed = OkxOrderBookFeed::connect();

        let result = timeout(Duration::from_secs(15), feed.next_book())
            .await
            .expect("OkxOrderBookFeed did not produce a snapshot within 15 seconds");

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
        assert_eq!(snapshot.exchange, ExchangeId::Okx);
        assert!(
            snapshot.at.0 > 1_700_000_000_000_i64,
            "timestamp looks wrong (expected ms): {}",
            snapshot.at.0,
        );
    }
}

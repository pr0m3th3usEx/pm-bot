use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use pm_core::{
    domain::Tick,
    error::{CoreError, Result},
    ports::PriceFeed,
    types::{Price, Timestamp},
};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio_tungstenite::tungstenite::Message;

/// Price feed backed by the Binance Futures mark-price stream (BTC index price, 1 s cadence).
///
/// Connects to the combined-stream endpoint for `btcusdt@markPrice@1s` and
/// emits one `Tick` per update. Index price (`i`) is used for resolution parity;
/// event time (`E`) is the tick timestamp in milliseconds.
pub struct BinancePriceFeed {
    rx: tokio::sync::mpsc::Receiver<Result<Tick>>,
}

/// Wrapper produced by the combined-stream endpoint:
/// `{"stream":"btcusdt@markPrice@1s","data":{...}}`
#[derive(Debug, Deserialize)]
struct StreamEnvelope {
    #[allow(dead_code)]
    stream: String,
    data: BinanceMarkPriceData,
}

/// Payload of a `markPriceUpdate` event.
#[derive(Debug, Deserialize)]
struct BinanceMarkPriceData {
    /// Event time (Unix milliseconds). Use this as `Tick.at`.
    #[serde(rename = "E")]
    event_time: i64,
    /// Index price. Use this as `Tick.price` for resolution parity.
    #[serde(rename = "i")]
    index_price: String,
}

impl BinancePriceFeed {
    pub fn connect() -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(64);

        tokio::spawn(async move {
            let stream = match tokio_tungstenite::connect_async(
                "wss://fstream.binance.com/market/stream?streams=btcusdt@markPrice@1s",
            )
            .await
            {
                Ok((stream, _)) => {
                    tracing::info!("Connected to Binance mark-price stream");
                    stream
                }
                Err(e) => {
                    tracing::error!("Failed to connect to Binance mark-price stream: {e}");
                    return;
                }
            };

            let (mut write, mut read) = stream.split();

            loop {
                match read.next().await {
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<StreamEnvelope>(&text) {
                            Ok(envelope) => {
                                let data = envelope.data;
                                let price = Price(
                                    data.index_price
                                        .parse::<Decimal>()
                                        .unwrap_or(Decimal::ZERO),
                                );
                                let tick = Tick {
                                    price,
                                    at: Timestamp(data.event_time),
                                };
                                if tx.send(Ok(tick)).await.is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                tracing::error!("Failed to parse Binance mark-price message: {e}");
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
                        tracing::error!("Binance mark-price stream closed by server");
                        break;
                    }
                    Some(Ok(_)) => {}
                    Some(Err(e)) => {
                        tracing::error!("Binance mark-price WebSocket error: {e}");
                        break;
                    }
                    None => {
                        tracing::error!("Binance mark-price stream ended");
                        break;
                    }
                }
            }
        });

        Self { rx }
    }
}

#[async_trait]
impl PriceFeed for BinancePriceFeed {
    async fn next_tick(&mut self) -> Result<Tick> {
        self.rx
            .recv()
            .await
            .ok_or_else(|| CoreError::Adapter("Binance feed closed unexpectedly".to_owned()))?
    }
}

#[cfg(test)]
mod tests {
    use pm_core::ports::PriceFeed;
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn receives_btc_usd_tick_within_timeout() {
        let mut feed = super::BinancePriceFeed::connect();

        let result = timeout(Duration::from_secs(15), feed.next_tick())
            .await
            .expect("BinancePriceFeed did not produce a tick within 15 seconds");

        let tick = result.expect("next_tick returned an error");

        assert!(
            tick.price.0 > rust_decimal::Decimal::ZERO,
            "tick price must be positive, got {:?}",
            tick.price
        );
        // Binance delivers timestamps as Unix milliseconds; threshold is Nov 2023.
        assert!(
            tick.at.0 > 1_700_000_000,
            "tick timestamp looks wrong: {:?}",
            tick.at
        );
    }
}

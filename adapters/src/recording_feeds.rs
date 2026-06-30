//! Recording decorators for live feeds + a SQLite-backed `MarketDataRecorder`.
//!
//! Wrapping a feed with `RecordingMarketDataFeed` / `RecordingPriceFeed` /
//! `RecordingCatalog` is transparent to downstream tasks — they see the same data
//! — but also writes every event to a SQLite sink for Phase 2 replay.
//!
//! Gate with `RECORD_SESSION=1` before constructing the decorators.

use async_trait::async_trait;
use pm_core::{
    domain::{Market, OutcomeBook, Tick},
    error::{CoreError, Result},
    ports::{MarketCatalog, MarketDataFeed, MarketDataRecorder, PriceFeed},
    types::{MarketSlug, Timestamp},
};
use rusqlite::{params, Connection};
use std::sync::{Arc, Mutex};

// ─── SQLite recorder ──────────────────────────────────────────────────────────

const RECORDER_DDL: &str = "
CREATE TABLE IF NOT EXISTS outcome_books (
    id          INTEGER PRIMARY KEY,
    session_id  TEXT    NOT NULL,
    token_id    TEXT    NOT NULL,
    buy_price   TEXT,
    sell_price  TEXT,
    recorded_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ob_session ON outcome_books(session_id);
CREATE INDEX IF NOT EXISTS idx_ob_token   ON outcome_books(token_id, recorded_at);

CREATE TABLE IF NOT EXISTS ticks (
    id          INTEGER PRIMARY KEY,
    session_id  TEXT    NOT NULL,
    price       TEXT    NOT NULL,
    recorded_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ticks_session ON ticks(session_id);

CREATE TABLE IF NOT EXISTS markets (
    id          INTEGER PRIMARY KEY,
    session_id  TEXT    NOT NULL,
    slug        TEXT    NOT NULL,
    opens_at    INTEGER NOT NULL,
    closes_at   INTEGER NOT NULL,
    resolves_at INTEGER NOT NULL,
    status      TEXT    NOT NULL,
    recorded_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_markets_session ON markets(session_id);
";

pub struct SqliteMarketDataRecorder {
    conn: Mutex<Connection>,
}

impl SqliteMarketDataRecorder {
    pub fn open(path: &str) -> anyhow::Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch(RECORDER_DDL)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn open_in_memory() -> anyhow::Result<Self> {
        Self::open(":memory:")
    }
}

#[async_trait]
impl MarketDataRecorder for SqliteMarketDataRecorder {
    async fn record_outcome_book(&self, book: &OutcomeBook, session_id: &str) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| CoreError::Store(e.to_string()))?;
        conn.execute(
            "INSERT INTO outcome_books (session_id, token_id, buy_price, sell_price, recorded_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                session_id,
                book.token_id.0.to_string(),
                book.buy_price.as_ref().map(|p| p.0.to_string()),
                book.sell_price.as_ref().map(|p| p.0.to_string()),
                book.at.0,
            ],
        )
        .map_err(|e| CoreError::Store(format!("record_outcome_book: {e}")))?;
        Ok(())
    }

    async fn record_tick(&self, tick: &Tick, session_id: &str) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| CoreError::Store(e.to_string()))?;
        conn.execute(
            "INSERT INTO ticks (session_id, price, recorded_at)
             VALUES (?1, ?2, ?3)",
            params![session_id, tick.price.0.to_string(), tick.at.0,],
        )
        .map_err(|e| CoreError::Store(format!("record_tick: {e}")))?;
        Ok(())
    }

    async fn record_market(&self, market: &Market, session_id: &str) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| CoreError::Store(e.to_string()))?;
        conn.execute(
            "INSERT INTO markets (session_id, slug, opens_at, closes_at, resolves_at, status, recorded_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                session_id,
                market.slug.0,
                market.opens_at.0,
                market.closes_at.0,
                market.resolves_at.0,
                format!("{:?}", market.status),
                Timestamp::now_ms().0,
            ],
        )
        .map_err(|e| CoreError::Store(format!("record_market: {e}")))?;
        Ok(())
    }
}

// ─── RecordingMarketDataFeed ──────────────────────────────────────────────────

/// Wraps a `MarketDataFeed` and writes each `OutcomeBook` to a `MarketDataRecorder`.
/// Transparent to downstream: `next_update()` returns the same value.
pub struct RecordingMarketDataFeed {
    inner: Box<dyn MarketDataFeed>,
    sink: Arc<dyn MarketDataRecorder>,
    session_id: String,
}

impl RecordingMarketDataFeed {
    pub fn new(
        inner: Box<dyn MarketDataFeed>,
        sink: Arc<dyn MarketDataRecorder>,
        session_id: String,
    ) -> Self {
        Self {
            inner,
            sink,
            session_id,
        }
    }
}

#[async_trait]
impl MarketDataFeed for RecordingMarketDataFeed {
    async fn next_update(&mut self) -> Result<OutcomeBook> {
        let book = self.inner.next_update().await?;
        if let Err(e) = self.sink.record_outcome_book(&book, &self.session_id).await {
            tracing::warn!(error = %e, "RecordingMarketDataFeed: failed to record outcome book");
        }
        Ok(book)
    }
}

// ─── RecordingPriceFeed ───────────────────────────────────────────────────────

/// Wraps a `PriceFeed` and writes each `Tick` to a `MarketDataRecorder`.
pub struct RecordingPriceFeed {
    inner: Box<dyn PriceFeed>,
    sink: Arc<dyn MarketDataRecorder>,
    session_id: String,
}

impl RecordingPriceFeed {
    pub fn new(
        inner: Box<dyn PriceFeed>,
        sink: Arc<dyn MarketDataRecorder>,
        session_id: String,
    ) -> Self {
        Self {
            inner,
            sink,
            session_id,
        }
    }
}

#[async_trait]
impl PriceFeed for RecordingPriceFeed {
    async fn next_tick(&mut self) -> Result<Tick> {
        let tick = self.inner.next_tick().await?;
        if let Err(e) = self.sink.record_tick(&tick, &self.session_id).await {
            tracing::warn!(error = %e, "RecordingPriceFeed: failed to record tick");
        }
        Ok(tick)
    }
}

// ─── RecordingCatalog ─────────────────────────────────────────────────────────

/// Wraps a `MarketCatalog` and writes each resolved `Market` to a `MarketDataRecorder`.
pub struct RecordingCatalog {
    inner: Arc<dyn MarketCatalog>,
    sink: Arc<dyn MarketDataRecorder>,
    session_id: String,
}

impl RecordingCatalog {
    pub fn new(
        inner: Arc<dyn MarketCatalog>,
        sink: Arc<dyn MarketDataRecorder>,
        session_id: String,
    ) -> Self {
        Self {
            inner,
            sink,
            session_id,
        }
    }
}

#[async_trait]
impl MarketCatalog for RecordingCatalog {
    async fn resolve(&self, slug: &MarketSlug) -> Result<Market> {
        let market = self.inner.resolve(slug).await?;
        if let Err(e) = self.sink.record_market(&market, &self.session_id).await {
            tracing::warn!(error = %e, "RecordingCatalog: failed to record market");
        }
        Ok(market)
    }
}

// ─── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use pm_core::{
        domain::OutcomeBook,
        ports::MarketDataFeed,
        types::{Price, Timestamp, TokenId},
    };
    use polymarket_client_sdk_v2::types::U256;
    use rust_decimal_macros::dec;
    use std::sync::Arc;

    /// Stub feed that yields a fixed sequence of OutcomeBook values.
    struct StubFeed {
        items: Vec<OutcomeBook>,
        idx: usize,
    }

    impl StubFeed {
        fn new(items: Vec<OutcomeBook>) -> Self {
            Self { items, idx: 0 }
        }
    }

    #[async_trait]
    impl MarketDataFeed for StubFeed {
        async fn next_update(&mut self) -> Result<OutcomeBook> {
            if self.idx < self.items.len() {
                let b = self.items[self.idx].clone();
                self.idx += 1;
                Ok(b)
            } else {
                Err(CoreError::Adapter("stub exhausted".to_owned()))
            }
        }
    }

    fn make_book(token_n: u64, buy: Decimal, sell: Decimal) -> OutcomeBook {
        OutcomeBook {
            token_id: TokenId(U256::from(token_n)),
            buy_price: Some(Price(buy)),
            sell_price: Some(Price(sell)),
            at: Timestamp(0),
        }
    }

    use rust_decimal::Decimal;

    #[tokio::test]
    async fn recording_feed_passes_through_and_records() {
        let recorder = Arc::new(SqliteMarketDataRecorder::open_in_memory().unwrap());

        let book1 = make_book(1, dec!(0.55), dec!(0.45));
        let book2 = make_book(2, dec!(0.60), dec!(0.40));

        let stub = StubFeed::new(vec![book1.clone(), book2.clone()]);
        let mut feed = RecordingMarketDataFeed::new(
            Box::new(stub),
            recorder.clone() as Arc<dyn MarketDataRecorder>,
            "test-session".to_owned(),
        );

        // First update passes through.
        let got1 = feed.next_update().await.unwrap();
        assert_eq!(got1.token_id, book1.token_id);

        // Second update passes through.
        let got2 = feed.next_update().await.unwrap();
        assert_eq!(got2.token_id, book2.token_id);

        // Check that exactly 2 rows were written.
        let conn = recorder.conn.lock().unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM outcome_books WHERE session_id='test-session'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 2, "expected 2 recorded rows, got {count}");
    }

    #[tokio::test]
    async fn sqlite_recorder_records_tick() {
        let recorder = SqliteMarketDataRecorder::open_in_memory().unwrap();
        let tick = Tick {
            price: Price(dec!(65000.0)),
            at: Timestamp(12345),
        };
        recorder.record_tick(&tick, "s1").await.unwrap();

        let conn = recorder.conn.lock().unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM ticks WHERE session_id='s1'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(count, 1);
    }
}

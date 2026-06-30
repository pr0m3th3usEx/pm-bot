//! Recording decorators for live feeds + a SQLite-backed `MarketDataRecorder`.
//!
//! Wrapping a feed with `RecordingMarketDataFeed` / `RecordingPriceFeed` /
//! `RecordingCatalog` is transparent to downstream tasks — they see the same data
//! — but also writes every event to a SQLite sink for Phase 2 replay.
//!
//! Gate with `RECORD_SESSION=1` before constructing the decorators.

use async_trait::async_trait;
use pm_core::{
    domain::{ActiveMarket, Market, OutcomeBook, Tick},
    error::{CoreError, Result},
    ports::{MarketCatalog, MarketDataFeed, MarketDataRecorder, PriceFeed},
    types::{MarketSlug, Timestamp},
};
use rusqlite::{params, Connection};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

// ─── SQLite recorder ──────────────────────────────────────────────────────────

const RECORDER_DDL: &str = "
CREATE TABLE IF NOT EXISTS outcome_books (
    id           INTEGER PRIMARY KEY,
    market_slug  TEXT    NOT NULL,
    token_id     TEXT    NOT NULL,
    buy_price    TEXT,
    sell_price   TEXT,
    recorded_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ob_slug  ON outcome_books(market_slug);
CREATE INDEX IF NOT EXISTS idx_ob_token ON outcome_books(token_id, recorded_at);

CREATE TABLE IF NOT EXISTS ticks (
    id           INTEGER PRIMARY KEY,
    market_slug  TEXT    NOT NULL,
    price        TEXT    NOT NULL,
    recorded_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ticks_slug ON ticks(market_slug);

CREATE TABLE IF NOT EXISTS markets (
    id               INTEGER PRIMARY KEY,
    slug             TEXT    NOT NULL,
    opens_at         INTEGER NOT NULL,
    closes_at        INTEGER NOT NULL,
    resolves_at      INTEGER NOT NULL,
    status           TEXT    NOT NULL,
    resolved_outcome TEXT,
    recorded_at      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_markets_slug ON markets(slug);
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
    async fn record_outcome_book(&self, book: &OutcomeBook, market_slug: &str) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| CoreError::Store(e.to_string()))?;
        conn.execute(
            "INSERT INTO outcome_books (market_slug, token_id, buy_price, sell_price, recorded_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                market_slug,
                book.token_id.0.to_string(),
                book.buy_price.as_ref().map(|p| p.0.to_string()),
                book.sell_price.as_ref().map(|p| p.0.to_string()),
                book.at.0,
            ],
        )
        .map_err(|e| CoreError::Store(format!("record_outcome_book: {e}")))?;
        Ok(())
    }

    async fn record_tick(&self, tick: &Tick, market_slug: &str) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| CoreError::Store(e.to_string()))?;
        conn.execute(
            "INSERT INTO ticks (market_slug, price, recorded_at)
             VALUES (?1, ?2, ?3)",
            params![market_slug, tick.price.0.to_string(), tick.at.0,],
        )
        .map_err(|e| CoreError::Store(format!("record_tick: {e}")))?;
        Ok(())
    }

    async fn record_market(&self, market: &Market) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|e| CoreError::Store(e.to_string()))?;
        conn.execute(
            "INSERT INTO markets (slug, opens_at, closes_at, resolves_at, status, resolved_outcome, recorded_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                market.slug.0,
                market.opens_at.0,
                market.closes_at.0,
                market.resolves_at.0,
                format!("{:?}", market.status),
                market.resolved_outcome,
                Timestamp::now_ms().0,
            ],
        )
        .map_err(|e| CoreError::Store(format!("record_market: {e}")))?;
        Ok(())
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Returns the current market slug from the watch receiver, or `None` if no
/// active market is set. Skipping on `None` avoids orphan rows in the DB.
fn current_slug(rx: &watch::Receiver<Option<ActiveMarket>>) -> Option<String> {
    rx.borrow().as_ref().map(|m| m.slug.0.clone())
}

// ─── RecordingMarketDataFeed ──────────────────────────────────────────────────

/// Wraps a `MarketDataFeed` and writes each `OutcomeBook` to a `MarketDataRecorder`.
/// Transparent to downstream: `next_update()` returns the same value.
/// Recording is skipped when no active market is set on the watch channel.
pub struct RecordingMarketDataFeed {
    inner: Box<dyn MarketDataFeed>,
    sink: Arc<dyn MarketDataRecorder>,
    market_rx: watch::Receiver<Option<ActiveMarket>>,
}

impl RecordingMarketDataFeed {
    pub fn new(
        inner: Box<dyn MarketDataFeed>,
        sink: Arc<dyn MarketDataRecorder>,
        market_rx: watch::Receiver<Option<ActiveMarket>>,
    ) -> Self {
        Self {
            inner,
            sink,
            market_rx,
        }
    }
}

#[async_trait]
impl MarketDataFeed for RecordingMarketDataFeed {
    async fn next_update(&mut self) -> Result<OutcomeBook> {
        let book = self.inner.next_update().await?;
        if let Some(slug) = current_slug(&self.market_rx) {
            if let Err(e) = self.sink.record_outcome_book(&book, &slug).await {
                tracing::warn!(error = %e, "RecordingMarketDataFeed: failed to record outcome book");
            }
        }
        Ok(book)
    }
}

// ─── RecordingPriceFeed ───────────────────────────────────────────────────────

/// Wraps a `PriceFeed` and writes each `Tick` to a `MarketDataRecorder`.
/// Recording is skipped when no active market is set on the watch channel.
pub struct RecordingPriceFeed {
    inner: Box<dyn PriceFeed>,
    sink: Arc<dyn MarketDataRecorder>,
    market_rx: watch::Receiver<Option<ActiveMarket>>,
}

impl RecordingPriceFeed {
    pub fn new(
        inner: Box<dyn PriceFeed>,
        sink: Arc<dyn MarketDataRecorder>,
        market_rx: watch::Receiver<Option<ActiveMarket>>,
    ) -> Self {
        Self {
            inner,
            sink,
            market_rx,
        }
    }
}

#[async_trait]
impl PriceFeed for RecordingPriceFeed {
    async fn next_tick(&mut self) -> Result<Tick> {
        let tick = self.inner.next_tick().await?;
        if let Some(slug) = current_slug(&self.market_rx) {
            if let Err(e) = self.sink.record_tick(&tick, &slug).await {
                tracing::warn!(error = %e, "RecordingPriceFeed: failed to record tick");
            }
        }
        Ok(tick)
    }
}

// ─── RecordingCatalog ─────────────────────────────────────────────────────────

/// Wraps a `MarketCatalog` and writes each resolved `Market` to a `MarketDataRecorder`.
pub struct RecordingCatalog {
    inner: Arc<dyn MarketCatalog>,
    sink: Arc<dyn MarketDataRecorder>,
}

impl RecordingCatalog {
    pub fn new(inner: Arc<dyn MarketCatalog>, sink: Arc<dyn MarketDataRecorder>) -> Self {
        Self { inner, sink }
    }
}

#[async_trait]
impl MarketCatalog for RecordingCatalog {
    async fn resolve(&self, slug: &MarketSlug) -> Result<Market> {
        let market = self.inner.resolve(slug).await?;
        if let Err(e) = self.sink.record_market(&market).await {
            tracing::warn!(error = %e, "RecordingCatalog: failed to record market");
        }
        Ok(market)
    }
}

// ─── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::FixedBytes;
    use pm_core::{
        domain::{Market, MarketOutcome, OutcomeBook},
        ports::MarketDataFeed,
        types::{
            MarketSlug, MarketStatus, MarketType, Price, Shares, Timestamp, TokenId,
        },
    };
    use polymarket_client_sdk_v2::types::U256;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use std::sync::Arc;
    use tokio::sync::watch;

    // ── Stub feed ────────────────────────────────────────────────────────────

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

    // ── Stub PriceFeed ────────────────────────────────────────────────────────

    struct StubPriceFeed {
        items: Vec<Tick>,
        idx: usize,
    }

    impl StubPriceFeed {
        fn new(items: Vec<Tick>) -> Self {
            Self { items, idx: 0 }
        }
    }

    #[async_trait]
    impl PriceFeed for StubPriceFeed {
        async fn next_tick(&mut self) -> Result<Tick> {
            if self.idx < self.items.len() {
                let t = self.items[self.idx].clone();
                self.idx += 1;
                Ok(t)
            } else {
                Err(CoreError::Adapter("stub exhausted".to_owned()))
            }
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn make_book(token_n: u64, buy: Decimal, sell: Decimal) -> OutcomeBook {
        OutcomeBook {
            token_id: TokenId(U256::from(token_n)),
            buy_price: Some(Price(buy)),
            sell_price: Some(Price(sell)),
            at: Timestamp(0),
        }
    }

    fn make_tick(price: Decimal) -> Tick {
        Tick {
            price: Price(price),
            at: Timestamp(12345),
        }
    }

    /// Build a minimal `Market` with the given slug string.
    fn make_market(slug: &str) -> Market {
        Market {
            slug: MarketSlug(slug.to_owned()),
            market_type: MarketType::UpDown,
            event_id: "evt-1".to_owned(),
            question_id: FixedBytes::default(),
            condition_id: FixedBytes::default(),
            outcomes: vec![
                MarketOutcome {
                    name: "up".to_owned(),
                    token_id: TokenId(U256::from(1u64)),
                },
                MarketOutcome {
                    name: "down".to_owned(),
                    token_id: TokenId(U256::from(2u64)),
                },
            ],
            strike: None,
            opens_at: Timestamp(0),
            closes_at: Timestamp(300_000),
            resolves_at: Timestamp(300_000),
            status: MarketStatus::Open,
            resolved_outcome: None,
            order_price_min_tick_size: Price(dec!(0.01)),
            order_min_size: Shares(dec!(5)),
        }
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn recording_feed_passes_through_and_records() {
        let recorder = Arc::new(SqliteMarketDataRecorder::open_in_memory().unwrap());

        let slug = "btc-updown-5m-1735689600";
        let market = make_market(slug);
        let (_, market_rx) = watch::channel(Some(market));

        let book1 = make_book(1, dec!(0.55), dec!(0.45));
        let book2 = make_book(2, dec!(0.60), dec!(0.40));

        let stub = StubFeed::new(vec![book1.clone(), book2.clone()]);
        let mut feed = RecordingMarketDataFeed::new(
            Box::new(stub),
            recorder.clone() as Arc<dyn MarketDataRecorder>,
            market_rx,
        );

        // First update passes through.
        let got1 = feed.next_update().await.unwrap();
        assert_eq!(got1.token_id, book1.token_id);

        // Second update passes through.
        let got2 = feed.next_update().await.unwrap();
        assert_eq!(got2.token_id, book2.token_id);

        // Check that exactly 2 rows were written with the correct market_slug.
        let conn = recorder.conn.lock().unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM outcome_books WHERE market_slug=?1",
                [slug],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 2, "expected 2 recorded rows, got {count}");
    }

    #[tokio::test]
    async fn sqlite_recorder_records_tick() {
        let recorder = SqliteMarketDataRecorder::open_in_memory().unwrap();
        let tick = make_tick(dec!(65000.0));
        let slug = "btc-updown-5m-123";
        recorder.record_tick(&tick, slug).await.unwrap();

        let conn = recorder.conn.lock().unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM ticks WHERE market_slug=?1",
                [slug],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn recording_skips_when_no_active_market() {
        let recorder = Arc::new(SqliteMarketDataRecorder::open_in_memory().unwrap());

        // No active market — channel holds None.
        let (_, market_rx) = watch::channel::<Option<ActiveMarket>>(None);

        // MarketDataFeed: should pass value through but record nothing.
        let book = make_book(1, dec!(0.55), dec!(0.45));
        let stub = StubFeed::new(vec![book.clone()]);
        let mut feed = RecordingMarketDataFeed::new(
            Box::new(stub),
            recorder.clone() as Arc<dyn MarketDataRecorder>,
            market_rx.clone(),
        );
        let got = feed.next_update().await.unwrap();
        assert_eq!(got.token_id, book.token_id, "value still passes through");

        // PriceFeed: should pass value through but record nothing.
        let tick = make_tick(dec!(65000.0));
        let stub_price = StubPriceFeed::new(vec![tick.clone()]);
        let mut price_feed = RecordingPriceFeed::new(
            Box::new(stub_price),
            recorder.clone() as Arc<dyn MarketDataRecorder>,
            market_rx,
        );
        let got_tick = price_feed.next_tick().await.unwrap();
        assert_eq!(got_tick.price.0, tick.price.0, "tick still passes through");

        // Verify zero rows recorded.
        let conn = recorder.conn.lock().unwrap();
        let ob_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM outcome_books", [], |r| r.get(0))
            .unwrap();
        let tick_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM ticks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(ob_count, 0, "no outcome_books rows when market is None");
        assert_eq!(tick_count, 0, "no ticks rows when market is None");
    }

    #[tokio::test]
    async fn record_market_persists_resolved_outcome() {
        let recorder = SqliteMarketDataRecorder::open_in_memory().unwrap();

        // Market with a known winning outcome.
        let mut market = make_market("btc-updown-5m-resolved");
        market.status = MarketStatus::Resolved;
        market.resolved_outcome = Some("Up".to_string());
        recorder.record_market(&market).await.unwrap();

        // Market with no resolved outcome (None → SQL NULL).
        let market_none = make_market("btc-updown-5m-open");
        recorder.record_market(&market_none).await.unwrap();

        let conn = recorder.conn.lock().unwrap();

        // Check that the resolved market stored "Up".
        let outcome: Option<String> = conn
            .query_row(
                "SELECT resolved_outcome FROM markets WHERE slug=?1",
                ["btc-updown-5m-resolved"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(outcome, Some("Up".to_string()), "expected resolved_outcome = 'Up'");

        // Check that the open market stored NULL.
        let outcome_null: Option<String> = conn
            .query_row(
                "SELECT resolved_outcome FROM markets WHERE slug=?1",
                ["btc-updown-5m-open"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(outcome_null, None, "expected resolved_outcome = NULL for open market");
    }
}

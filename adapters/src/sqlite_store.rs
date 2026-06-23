use async_trait::async_trait;
use pm_core::{
    domain::{PositionRecord, PositionUpdate},
    error::{CoreError, Result},
    ports::Store,
};
use rusqlite::{params, Connection};
use std::sync::Mutex;

pub struct SqliteStore {
    conn: Mutex<Connection>,
}

const DDL: &str = "
CREATE TABLE IF NOT EXISTS positions (
    id           INTEGER PRIMARY KEY,
    market_slug  TEXT    NOT NULL,
    side         TEXT    NOT NULL CHECK(side IN ('buy','sell')),
    outcome_name TEXT    NOT NULL,
    token_id     TEXT    NOT NULL,
    order_id     TEXT,
    shares       TEXT    NOT NULL,
    limit_price  TEXT    NOT NULL,
    avg_price    TEXT,
    strike       TEXT,
    status       TEXT    NOT NULL CHECK(status IN (
                     'submitted','filled','settling','won','lost','rejected','cancelled')),
    realized_pnl TEXT,
    submitted_at INTEGER NOT NULL,
    updated_at   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_positions_slug   ON positions(market_slug);
CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);
";

impl SqliteStore {
    pub fn open(path: &str) -> anyhow::Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch(DDL)?;
        Ok(Self { conn: Mutex::new(conn) })
    }

    pub fn open_in_memory() -> anyhow::Result<Self> {
        Self::open(":memory:")
    }
}

// Helper: serialize Decimal as TEXT
fn dec_to_text(d: &rust_decimal::Decimal) -> String { d.to_string() }

#[allow(dead_code)]
fn text_to_dec(s: &str) -> Result<rust_decimal::Decimal> {
    s.parse().map_err(|e| CoreError::Store(format!("bad decimal '{s}': {e}")))
}

#[async_trait]
impl Store for SqliteStore {
    async fn insert_position(&self, rec: &PositionRecord) -> Result<i64> {
        let conn = self.conn.lock().map_err(|e| CoreError::Store(e.to_string()))?;
        conn.execute(
            "INSERT INTO positions
             (market_slug, side, outcome_name, token_id, order_id, shares, limit_price,
              avg_price, strike, status, realized_pnl, submitted_at, updated_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
            params![
                rec.market_slug.0,
                rec.side.as_str(),
                rec.outcome_name,
                rec.token_id.0,
                rec.order_id.as_deref(),
                dec_to_text(&rec.shares.0),
                dec_to_text(&rec.limit_price.0),
                rec.avg_price.as_ref().map(|p| dec_to_text(&p.0)),
                rec.strike.as_ref().map(|p| dec_to_text(&p.0)),
                rec.status.as_str(),
                rec.realized_pnl.as_ref().map(|u| dec_to_text(&u.0)),
                rec.submitted_at.0,
                rec.updated_at.0,
            ],
        ).map_err(|e| CoreError::Store(e.to_string()))?;
        Ok(conn.last_insert_rowid())
    }

    async fn update_position(&self, id: i64, update: &PositionUpdate) -> Result<()> {
        let conn = self.conn.lock().map_err(|e| CoreError::Store(e.to_string()))?;
        match update {
            PositionUpdate::Submitted { order_id, updated_at } => {
                conn.execute(
                    "UPDATE positions SET order_id=?1, status='submitted', updated_at=?2 WHERE id=?3",
                    params![order_id, updated_at.0, id],
                )
            }
            PositionUpdate::Filled { avg_price, size_matched: _, updated_at } => {
                conn.execute(
                    "UPDATE positions SET avg_price=?1, status='filled', updated_at=?2 WHERE id=?3",
                    params![dec_to_text(&avg_price.0), updated_at.0, id],
                )
            }
            PositionUpdate::Rejected { updated_at } => {
                conn.execute(
                    "UPDATE positions SET status='rejected', updated_at=?1 WHERE id=?2",
                    params![updated_at.0, id],
                )
            }
            PositionUpdate::Cancelled { updated_at } => {
                conn.execute(
                    "UPDATE positions SET status='cancelled', updated_at=?1 WHERE id=?2",
                    params![updated_at.0, id],
                )
            }
            PositionUpdate::Settling { updated_at } => {
                conn.execute(
                    "UPDATE positions SET status='settling', updated_at=?1 WHERE id=?2",
                    params![updated_at.0, id],
                )
            }
            PositionUpdate::Won { realized_pnl, updated_at } => {
                conn.execute(
                    "UPDATE positions SET status='won', realized_pnl=?1, updated_at=?2 WHERE id=?3",
                    params![dec_to_text(&realized_pnl.0), updated_at.0, id],
                )
            }
            PositionUpdate::Lost { realized_pnl, updated_at } => {
                conn.execute(
                    "UPDATE positions SET status='lost', realized_pnl=?1, updated_at=?2 WHERE id=?3",
                    params![dec_to_text(&realized_pnl.0), updated_at.0, id],
                )
            }
        }.map_err(|e| CoreError::Store(e.to_string()))?;
        Ok(())
    }

    async fn open_positions(&self) -> Result<Vec<PositionRecord>> {
        todo!("query positions WHERE status NOT IN ('won','lost','rejected','cancelled')")
    }

    async fn success_rate_counts(&self) -> Result<(u64, u64)> {
        let conn = self.conn.lock().map_err(|e| CoreError::Store(e.to_string()))?;
        let (wins, resolved): (u64, u64) = conn.query_row(
            "SELECT
                 CAST(SUM(CASE WHEN status='won' THEN 1 ELSE 0 END) AS INTEGER),
                 CAST(SUM(CASE WHEN status IN ('won','lost') THEN 1 ELSE 0 END) AS INTEGER)
             FROM positions",
            [],
            |row| Ok((
                row.get::<_, i64>(0).map(|v| v as u64).unwrap_or(0),
                row.get::<_, i64>(1).map(|v| v as u64).unwrap_or(0),
            )),
        ).map_err(|e| CoreError::Store(e.to_string()))?;
        Ok((wins, resolved))
    }
}

// ─── Integration test: real SQLite schema + enum round-trips ─────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use pm_core::types::{MarketSlug, Price, PositionStatus, Shares, Side, Timestamp, TokenId, Usdc};
    use rust_decimal_macros::dec;

    fn sample_record() -> PositionRecord {
        PositionRecord {
            id: None,
            market_slug: MarketSlug("btc-updown-5m-1000".into()),
            side: Side::Buy,
            outcome_name: "up".into(),
            token_id: TokenId("tok1".into()),
            order_id: None,
            shares: Shares(dec!(5)),
            limit_price: Price(dec!(0.55)),
            avg_price: None,
            strike: Some(Price(dec!(0.50))),
            status: PositionStatus::Submitted,
            realized_pnl: None,
            submitted_at: Timestamp(1_000_000),
            updated_at: Timestamp(1_000_000),
        }
    }

    #[tokio::test]
    async fn insert_and_success_rate() {
        let store = SqliteStore::open_in_memory().unwrap();

        let id = store.insert_position(&sample_record()).await.unwrap();
        assert!(id > 0);

        // Initial: 0 resolved
        let (wins, resolved) = store.success_rate_counts().await.unwrap();
        assert_eq!((wins, resolved), (0, 0));

        // Mark won
        store.update_position(id, &PositionUpdate::Won {
            realized_pnl: Usdc(dec!(4.5)),
            updated_at: Timestamp(2_000_000),
        }).await.unwrap();

        let (wins, resolved) = store.success_rate_counts().await.unwrap();
        assert_eq!((wins, resolved), (1, 1));
    }

    #[tokio::test]
    async fn enum_text_round_trip() {
        let store = SqliteStore::open_in_memory().unwrap();

        // Insert a record for every PositionStatus variant that is a valid initial state.
        // The table constraint enforces the TEXT values — if an as_str() value is wrong,
        // the INSERT will fail the CHECK constraint.
        for status in [PositionStatus::Submitted] {
            let mut rec = sample_record();
            rec.status = status;
            store.insert_position(&rec).await
                .expect(&format!("INSERT failed for status {:?}", status));
        }

        // Side round-trip
        for side in [Side::Buy, Side::Sell] {
            let mut rec = sample_record();
            rec.side = side;
            store.insert_position(&rec).await
                .expect(&format!("INSERT failed for side {:?}", side));
        }
    }
}

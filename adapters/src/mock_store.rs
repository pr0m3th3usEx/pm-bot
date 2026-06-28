use async_trait::async_trait;
use pm_core::{
    domain::{PositionRecord, PositionUpdate},
    error::{CoreError, Result},
    ports::Store,
    types::{PositionStatus, Timestamp},
};
use std::sync::{Arc, Mutex};

#[derive(Debug, Default)]
pub struct MockStore {
    inner: Mutex<MockStoreInner>,
}

#[derive(Debug, Default)]
struct MockStoreInner {
    positions: Vec<PositionRecord>,
    next_id: i64,
}

impl MockStore {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

#[async_trait]
impl Store for MockStore {
    async fn insert_position(&self, record: &PositionRecord) -> Result<i64> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| CoreError::Store(e.to_string()))?;
        inner.next_id += 1;
        let id = inner.next_id;
        let mut rec = record.clone();
        rec.id = Some(id);
        inner.positions.push(rec);
        Ok(id)
    }

    async fn update_position(&self, id: i64, update: &PositionUpdate) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| CoreError::Store(e.to_string()))?;
        let rec = inner
            .positions
            .iter_mut()
            .find(|r| r.id == Some(id))
            .ok_or_else(|| CoreError::Store(format!("position {id} not found")))?;
        let now = Timestamp::now_ms();
        match update {
            PositionUpdate::Submitted { order_id, .. } => {
                rec.order_id = Some(order_id.clone());
            }
            PositionUpdate::Filled { avg_price, .. } => {
                rec.avg_price = Some(avg_price.clone());
                rec.status = PositionStatus::Filled;
            }
            PositionUpdate::Rejected { .. } => rec.status = PositionStatus::Rejected,
            PositionUpdate::Cancelled { .. } => rec.status = PositionStatus::Cancelled,
            PositionUpdate::Settling { .. } => rec.status = PositionStatus::Settling,
            PositionUpdate::Won { realized_pnl, .. } => {
                rec.status = PositionStatus::Won;
                rec.realized_pnl = Some(realized_pnl.clone());
            }
            PositionUpdate::Lost { realized_pnl, .. } => {
                rec.status = PositionStatus::Lost;
                rec.realized_pnl = Some(realized_pnl.clone());
            }
            PositionUpdate::Redeemed { .. } => {
                rec.status = PositionStatus::Redeemed;
            }
        }
        rec.updated_at = now;
        Ok(())
    }

    async fn open_positions(&self) -> Result<Vec<PositionRecord>> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| CoreError::Store(e.to_string()))?;
        Ok(inner
            .positions
            .iter()
            .filter(|r| !r.status.is_terminal())
            .cloned()
            .collect())
    }

    async fn success_rate_counts(&self) -> Result<(u64, u64)> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| CoreError::Store(e.to_string()))?;
        // A redeemed position is a past winner, so it counts as a win.
        let wins = inner
            .positions
            .iter()
            .filter(|r| {
                matches!(r.status, PositionStatus::Won | PositionStatus::Redeemed)
            })
            .count() as u64;
        let resolved = inner
            .positions
            .iter()
            .filter(|r| {
                matches!(
                    r.status,
                    PositionStatus::Won | PositionStatus::Redeemed | PositionStatus::Lost
                )
            })
            .count() as u64;
        Ok((wins, resolved))
    }
}

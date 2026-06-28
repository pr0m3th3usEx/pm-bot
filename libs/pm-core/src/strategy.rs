use crate::domain::Market;
use crate::types::{Outcome, Price, Timestamp};

/// All inputs the strategy needs to decide hold-or-enter on a single tick.
/// Pure data — no I/O, no state.
pub struct StrategyContext<'a> {
    /// Latest BTC tick price.
    pub price: Price,
    /// Price to beat, sourced from Gamma API Market Event data via Market.strike.
    pub strike: Price,
    pub now: Timestamp,
    /// Trading cutoff: order must submit before this.
    pub closes_at: Timestamp,
    pub resolves_at: Timestamp,
    pub market: &'a Market,
}

impl<'a> StrategyContext<'a> {
    /// Seconds remaining until the trading window closes.
    pub fn secs_to_cutoff(&self) -> i64 {
        self.closes_at.as_secs() - self.now.as_secs()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StrategyDecision {
    Hold,
    Enter { outcome: Outcome },
}

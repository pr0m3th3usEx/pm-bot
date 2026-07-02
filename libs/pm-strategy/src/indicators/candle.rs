use std::collections::VecDeque;

/// A single OHLC candle bucketed by time.
#[derive(Debug, Clone, PartialEq)]
pub struct Candle {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    /// Unix timestamp of the bucket start, in seconds.
    pub start_secs: i64,
}

/// Aggregates price ticks into fixed-duration candles.
///
/// Ticks are bucketed by `floor(ts_secs / bucket_secs)`. When a tick belongs
/// to a new bucket, the current in-progress candle is finalised and pushed
/// into the closed ring buffer (capped at `cap`).
///
/// # Timestamp note
/// Timestamps are UNIX **seconds**. If you are working with `pm_core::types::Timestamp`
/// (which stores milliseconds), call `.as_secs()` before passing to `on_tick`.
pub struct CandleAggregator {
    /// Candle duration in seconds.
    pub bucket_secs: i64,
    /// The candle currently being built (not yet closed).
    current: Option<(i64, Candle)>, // (bucket_id, candle)
    /// Ring buffer of completed candles, oldest first.
    closed: VecDeque<Candle>,
    /// Maximum number of closed candles to retain.
    cap: usize,
}

impl CandleAggregator {
    /// Create a new aggregator.
    ///
    /// * `bucket_secs` — width of each candle in seconds (e.g., 60).
    /// * `cap` — maximum history to keep; older candles are dropped.
    pub fn new(bucket_secs: i64, cap: usize) -> Self {
        Self {
            bucket_secs,
            current: None,
            closed: VecDeque::with_capacity(cap + 1),
            cap,
        }
    }

    /// Feed a price tick at a given Unix-second timestamp.
    ///
    /// Returns `Some(candle)` when a candle is **closed** (i.e., this tick
    /// belongs to a newer bucket than the in-progress one). Returns `None`
    /// while the current candle is still accumulating ticks.
    pub fn on_tick(&mut self, price: f64, ts_secs: i64) -> Option<Candle> {
        let bucket_id = ts_secs / self.bucket_secs;

        match &mut self.current {
            None => {
                // Start first candle.
                self.current = Some((
                    bucket_id,
                    Candle {
                        open: price,
                        high: price,
                        low: price,
                        close: price,
                        start_secs: bucket_id * self.bucket_secs,
                    },
                ));
                None
            }
            Some((current_bucket, candle)) => {
                if bucket_id == *current_bucket {
                    // Same bucket — update in-progress candle.
                    candle.high = candle.high.max(price);
                    candle.low = candle.low.min(price);
                    candle.close = price;
                    None
                } else {
                    // New bucket — close current candle.
                    let finished = candle.clone();
                    if self.closed.len() >= self.cap {
                        self.closed.pop_front();
                    }
                    self.closed.push_back(finished.clone());

                    // Start fresh candle for the new bucket.
                    self.current = Some((
                        bucket_id,
                        Candle {
                            open: price,
                            high: price,
                            low: price,
                            close: price,
                            start_secs: bucket_id * self.bucket_secs,
                        },
                    ));
                    Some(finished)
                }
            }
        }
    }

    /// Read-only access to the closed-candle history (oldest → newest).
    pub fn closed(&self) -> &VecDeque<Candle> {
        &self.closed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_candle_on_first_tick() {
        let mut agg = CandleAggregator::new(60, 10);
        assert!(agg.on_tick(100.0, 0).is_none());
        assert!(agg.closed().is_empty());
    }

    #[test]
    fn same_bucket_updates_high_low_close() {
        let mut agg = CandleAggregator::new(60, 10);
        agg.on_tick(100.0, 0);
        agg.on_tick(110.0, 30);
        agg.on_tick(90.0, 59);
        assert!(agg.closed().is_empty());
    }

    #[test]
    fn new_bucket_closes_previous_candle() {
        let mut agg = CandleAggregator::new(60, 10);
        agg.on_tick(100.0, 0);
        agg.on_tick(110.0, 30);
        agg.on_tick(90.0, 59);
        let closed = agg.on_tick(95.0, 60);
        assert!(closed.is_some());
        let c = closed.unwrap();
        assert_eq!(c.open, 100.0);
        assert_eq!(c.high, 110.0);
        assert_eq!(c.low, 90.0);
        assert_eq!(c.close, 90.0);
        assert_eq!(c.start_secs, 0);
        assert_eq!(agg.closed().len(), 1);
    }

    #[test]
    fn cap_evicts_oldest_candle() {
        let mut agg = CandleAggregator::new(60, 2);
        // Produce 3 closed candles.
        agg.on_tick(100.0, 0);
        agg.on_tick(101.0, 60);
        agg.on_tick(102.0, 120);
        agg.on_tick(103.0, 180);
        // cap=2: only the two most recent closed candles are kept.
        assert_eq!(agg.closed().len(), 2);
        assert_eq!(agg.closed()[0].open, 101.0);
        assert_eq!(agg.closed()[1].open, 102.0);
    }

    #[test]
    fn candle_start_secs_is_bucket_aligned() {
        let mut agg = CandleAggregator::new(60, 5);
        agg.on_tick(100.0, 75);
        let closed = agg.on_tick(105.0, 135);
        assert!(closed.is_some());
        assert_eq!(closed.unwrap().start_secs, 60);
    }
}

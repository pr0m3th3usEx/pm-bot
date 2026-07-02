/// Wilder's Relative Strength Index (RSI).
///
/// Uses Wilder's smoothing (equivalent to an EMA with `α = 1/period`) for
/// the running average gain and loss. `value()` returns a reading in `[0, 100]`
/// once `count >= period` closed candles have been observed.
///
/// # Usage
/// Call `update(close)` for each **closed** candle's closing price. Do **not**
/// call with the in-progress (open) candle.
pub struct Rsi {
    period: usize,
    avg_gain: f64,
    avg_loss: f64,
    last_close: Option<f64>,
    count: usize,
}

impl Rsi {
    /// Create a new RSI with the given period (typically 14).
    pub fn new(period: usize) -> Self {
        assert!(period >= 2, "RSI period must be >= 2");
        Self {
            period,
            avg_gain: 0.0,
            avg_loss: 0.0,
            last_close: None,
            count: 0,
        }
    }

    /// Feed the closing price of a completed candle.
    ///
    /// Uses Wilder smoothing: `avg = prev_avg * (period-1)/period + current/period`.
    pub fn update(&mut self, close: f64) {
        if let Some(prev) = self.last_close {
            let change = close - prev;
            let gain = if change > 0.0 { change } else { 0.0 };
            let loss = if change < 0.0 { -change } else { 0.0 };

            if self.count == 0 {
                // Seed with the first change.
                self.avg_gain = gain;
                self.avg_loss = loss;
            } else {
                let alpha = 1.0 / self.period as f64;
                self.avg_gain = alpha * gain + (1.0 - alpha) * self.avg_gain;
                self.avg_loss = alpha * loss + (1.0 - alpha) * self.avg_loss;
            }
            self.count += 1;
        }
        self.last_close = Some(close);
    }

    /// Returns `true` once at least `period` candle closes have been fed.
    pub fn warm(&self) -> bool {
        self.count >= self.period
    }

    /// RSI value in `[0.0, 100.0]`, or `None` if not yet warm.
    pub fn value(&self) -> Option<f64> {
        if !self.warm() {
            return None;
        }
        if self.avg_loss == 0.0 {
            // All gains, no losses → RSI = 100.
            return Some(100.0);
        }
        let rs = self.avg_gain / self.avg_loss;
        Some(100.0 - 100.0 / (1.0 + rs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cold_start_returns_none() {
        let r = Rsi::new(14);
        assert!(r.value().is_none());
        assert!(!r.warm());
    }

    #[test]
    fn warm_after_period_updates() {
        let mut r = Rsi::new(5);
        // Need 5 count increments: each close after the first increments count.
        // So we need period+1 closes total.
        for i in 0..=5usize {
            r.update(100.0 + i as f64);
        }
        assert!(r.warm());
        assert!(r.value().is_some());
    }

    #[test]
    fn all_gains_gives_rsi_100() {
        let mut r = Rsi::new(3);
        // Feed enough to warm: need period+1 closes.
        r.update(100.0);
        r.update(101.0);
        r.update(102.0);
        r.update(103.0);
        // avg_loss will be 0 → RSI = 100.
        assert_eq!(r.value(), Some(100.0));
    }

    #[test]
    fn all_losses_gives_low_rsi() {
        let mut r = Rsi::new(3);
        r.update(103.0);
        r.update(102.0);
        r.update(101.0);
        r.update(100.0);
        let v = r.value().unwrap();
        // No gains → avg_gain = 0 → RS = 0 → RSI = 0.
        assert!(v < 1.0, "expected near 0, got {v}");
    }

    #[test]
    fn rsi_within_bounds() {
        let mut r = Rsi::new(14);
        let prices = [
            65000.0, 65200.0, 64800.0, 65100.0, 64900.0, 65300.0, 65000.0, 64700.0, 65100.0,
            65400.0, 65200.0, 65500.0, 65300.0, 65600.0, 65400.0,
        ];
        for p in prices {
            r.update(p);
        }
        if let Some(v) = r.value() {
            assert!((0.0..=100.0).contains(&v), "RSI out of bounds: {v}");
        }
    }
}

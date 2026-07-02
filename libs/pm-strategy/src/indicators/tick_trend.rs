use std::collections::VecDeque;

/// Tick-level directional trend indicator.
///
/// Keeps a rolling window of the last `window` prices and computes a trend
/// signal based on two criteria:
///
/// 1. **Directional consistency** — fraction of consecutive up-moves vs. total
///    consecutive moves. A value ≥ 0.60 is required.
/// 2. **Net move** — `(last − first) / first * 100` (percent). The absolute
///    value must exceed 0.005% (5 basis points) to filter noise.
///
/// Returns `+1.0` (bullish) or `-1.0` (bearish) when both thresholds are met
/// and the direction is consistent; otherwise `0.0`.
pub struct TickTrend {
    buf: VecDeque<f64>,
    window: usize,
}

impl TickTrend {
    /// Create a new `TickTrend` with the given rolling-window size.
    pub fn new(window: usize) -> Self {
        assert!(window >= 2, "TickTrend window must be >= 2");
        Self {
            buf: VecDeque::with_capacity(window + 1),
            window,
        }
    }

    /// Push the latest tick price into the rolling window.
    pub fn push(&mut self, price: f64) {
        if self.buf.len() >= self.window {
            self.buf.pop_front();
        }
        self.buf.push_back(price);
    }

    /// Compute the directional trend signal.
    ///
    /// Requires at least `window / 2` samples (rounded down). Returns:
    /// * `+1.0` — consistent upward trend (consistency ≥ 0.60, net move > +0.005%).
    /// * `-1.0` — consistent downward trend.
    /// * `0.0` — insufficient data, inconsistent, or near-flat.
    pub fn signal(&self) -> f64 {
        let min_samples = (self.window / 2).max(2);
        if self.buf.len() < min_samples {
            return 0.0;
        }

        let prices: Vec<f64> = self.buf.iter().copied().collect();
        let n = prices.len();

        // Count up-moves and down-moves in consecutive pairs.
        let mut up = 0usize;
        let mut down = 0usize;
        for i in 1..n {
            if prices[i] > prices[i - 1] {
                up += 1;
            } else if prices[i] < prices[i - 1] {
                down += 1;
            }
        }
        let total_moves = up + down;
        if total_moves == 0 {
            return 0.0;
        }

        // Net move percentage (first to last).
        let first = prices[0];
        if first == 0.0 {
            return 0.0;
        }
        let net_pct = (prices[n - 1] - first) / first * 100.0;

        // Directional consistency: fraction of moves in the dominant direction.
        let dominant = up.max(down);
        let consistency = dominant as f64 / total_moves as f64;

        if consistency < 0.60 || net_pct.abs() <= 0.005 {
            return 0.0;
        }

        if net_pct > 0.0 { 1.0 } else { -1.0 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_on_empty_buffer() {
        let tt = TickTrend::new(10);
        assert_eq!(tt.signal(), 0.0);
    }

    #[test]
    fn zero_below_min_samples() {
        let mut tt = TickTrend::new(10);
        // min_samples = 10/2 = 5; push only 4.
        for i in 0..4 {
            tt.push(100.0 + i as f64);
        }
        assert_eq!(tt.signal(), 0.0);
    }

    #[test]
    fn positive_signal_on_consistent_uptrend() {
        let mut tt = TickTrend::new(10);
        // Strong uptrend: consistent rises and notable net move.
        let prices = [
            100.0, 100.1, 100.2, 100.3, 100.4, 100.5, 100.6, 100.7, 100.8, 100.9,
        ];
        for p in prices {
            tt.push(p);
        }
        assert_eq!(tt.signal(), 1.0);
    }

    #[test]
    fn negative_signal_on_consistent_downtrend() {
        let mut tt = TickTrend::new(10);
        let prices = [
            100.9, 100.8, 100.7, 100.6, 100.5, 100.4, 100.3, 100.2, 100.1, 100.0,
        ];
        for p in prices {
            tt.push(p);
        }
        assert_eq!(tt.signal(), -1.0);
    }

    #[test]
    fn zero_on_noisy_flat_prices() {
        let mut tt = TickTrend::new(10);
        // Alternating up/down with tiny net move → no consistent trend.
        let prices = [100.0, 100.001, 99.999, 100.001, 99.999, 100.0, 100.001, 99.999, 100.0, 100.001];
        for p in prices {
            tt.push(p);
        }
        // Either net_pct too small or consistency too low → 0.
        // (consistency may be 0.5 on a perfectly alternating series)
        assert_eq!(tt.signal(), 0.0);
    }

    #[test]
    fn window_caps_at_window_size() {
        let mut tt = TickTrend::new(5);
        // Push 10 values: window should only hold last 5.
        for i in 0..10 {
            tt.push(100.0 + i as f64);
        }
        assert_eq!(tt.buf.len(), 5);
    }
}

/// Exponential Moving Average (EMA) with standard `k = 2/(period+1)` smoothing.
///
/// The first sample seeds the EMA (no warm-up offset). `warm()` returns `true`
/// once at least `period` samples have been fed — at that point the EMA has
/// had enough history to be considered reliable.
pub struct Ema {
    period: usize,
    value: Option<f64>,
    count: usize,
}

impl Ema {
    /// Create a new EMA with the given period (must be ≥ 1).
    pub fn new(period: usize) -> Self {
        assert!(period >= 1, "EMA period must be >= 1");
        Self {
            period,
            value: None,
            count: 0,
        }
    }

    /// Feed the next sample.
    pub fn update(&mut self, x: f64) {
        self.count += 1;
        match self.value {
            None => self.value = Some(x),
            Some(prev) => {
                let k = 2.0 / (self.period as f64 + 1.0);
                self.value = Some(k * x + (1.0 - k) * prev);
            }
        }
    }

    /// Returns `true` once `count >= period` samples have been observed.
    pub fn warm(&self) -> bool {
        self.count >= self.period
    }

    /// Current EMA value, or `None` if no samples have been fed yet.
    pub fn value(&self) -> Option<f64> {
        self.value
    }
}

/// Crossover signal between a fast and a slow EMA.
///
/// Returns:
/// * `+1.0` — fast > slow (bullish cross / trending up).
/// * `-1.0` — fast < slow (bearish cross / trending down).
/// * `0.0`  — equal or either EMA is not yet warm.
pub fn crossover_sign(fast: &Ema, slow: &Ema) -> f64 {
    if !fast.warm() || !slow.warm() {
        return 0.0;
    }
    match (fast.value(), slow.value()) {
        (Some(f), Some(s)) if f > s => 1.0,
        (Some(f), Some(s)) if f < s => -1.0,
        _ => 0.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cold_start_returns_none() {
        let e = Ema::new(5);
        assert!(e.value().is_none());
        assert!(!e.warm());
    }

    #[test]
    fn first_sample_seeds_ema() {
        let mut e = Ema::new(5);
        e.update(100.0);
        assert_eq!(e.value(), Some(100.0));
    }

    #[test]
    fn warm_after_period_samples() {
        let mut e = Ema::new(3);
        for _ in 0..3 {
            e.update(50.0);
        }
        assert!(e.warm());
    }

    #[test]
    fn not_warm_before_period_samples() {
        let mut e = Ema::new(5);
        for _ in 0..4 {
            e.update(50.0);
        }
        assert!(!e.warm());
    }

    #[test]
    fn ema_converges_upward_on_rising_prices() {
        let mut e = Ema::new(3);
        for i in 0..20 {
            e.update(i as f64 * 10.0);
        }
        // EMA should lag behind the last price but trend upward.
        let v = e.value().unwrap();
        assert!(v > 0.0);
        assert!(v < 200.0); // lags behind the last value
    }

    #[test]
    fn crossover_sign_cold_returns_zero() {
        let fast = Ema::new(9);
        let slow = Ema::new(21);
        assert_eq!(crossover_sign(&fast, &slow), 0.0);
    }

    #[test]
    fn crossover_sign_fast_above_slow() {
        let mut fast = Ema::new(1);
        let mut slow = Ema::new(1);
        // Feed fast a higher value and slow a lower value.
        for _ in 0..5 {
            fast.update(110.0);
            slow.update(90.0);
        }
        assert_eq!(crossover_sign(&fast, &slow), 1.0);
    }

    #[test]
    fn crossover_sign_fast_below_slow() {
        let mut fast = Ema::new(1);
        let mut slow = Ema::new(1);
        for _ in 0..5 {
            fast.update(90.0);
            slow.update(110.0);
        }
        assert_eq!(crossover_sign(&fast, &slow), -1.0);
    }
}

/// Exponentially-weighted variance estimator for sequential log-returns.
///
/// # Microstructure noise
/// At sub-second tick rates, bid-ask bounce inflates realised variance.
/// Setting `stride > 1` downsamples — e.g., stride=10 at 500ms ticks means
/// one sample every 5 s, substantially reducing that upward bias.
pub struct VolEstimator {
    alpha: f64,
    ewma_var: f64,
    last_price: Option<f64>,
    /// Effective interval in seconds (nominal_interval_secs * stride).
    effective_interval_secs: f64,
    stride: u32,
    tick_counter: u32,
    update_count: u32,
}

impl VolEstimator {
    /// `alpha` — EWMA smoothing factor for squared log-returns.
    ///           Centre-of-mass ≈ (1−α)/α updates.
    /// `nominal_interval_secs` — expected time between price ticks (e.g., 0.5).
    /// `stride` — subsample: only update variance every `stride` ticks.
    pub fn new(alpha: f64, nominal_interval_secs: f64, stride: u32) -> Self {
        let stride = stride.max(1);
        Self {
            alpha,
            ewma_var: 0.0,
            last_price: None,
            effective_interval_secs: nominal_interval_secs * stride as f64,
            stride,
            tick_counter: 0,
            update_count: 0,
        }
    }

    /// Feed the next price tick. Only updates the variance estimate every `stride` ticks.
    pub fn update(&mut self, price: f64) {
        self.tick_counter += 1;
        if !self.tick_counter.is_multiple_of(self.stride) {
            return;
        }
        if let Some(prev) = self.last_price {
            if prev > 0.0 && price > 0.0 {
                let log_ret = (price / prev).ln();
                let sq = log_ret * log_ret;
                self.ewma_var = self.alpha * sq + (1.0 - self.alpha) * self.ewma_var;
                self.update_count += 1;
            }
        }
        self.last_price = Some(price);
    }

    /// Annualised volatility (σ).
    ///
    /// Returns `cold_fallback` until at least `min_updates` variance samples
    /// have been collected — prevents the cold-start period from producing
    /// misleadingly low vol estimates.
    pub fn annualized_vol(&self, cold_fallback: f64, min_updates: u32) -> f64 {
        if self.update_count < min_updates {
            return cold_fallback;
        }
        const SECS_PER_YEAR: f64 = 365.25 * 24.0 * 3600.0;
        let periods_per_year = SECS_PER_YEAR / self.effective_interval_secs;
        (self.ewma_var * periods_per_year).sqrt()
    }

    pub fn update_count(&self) -> u32 {
        self.update_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cold_start_returns_fallback() {
        let mut v = VolEstimator::new(0.1, 0.5, 1);
        v.update(100.0);
        assert_eq!(v.annualized_vol(0.7, 5), 0.7);
    }

    #[test]
    fn flat_price_gives_near_zero_vol() {
        let mut v = VolEstimator::new(0.1, 0.5, 1);
        for _ in 0..60 {
            v.update(65000.0);
        }
        let sigma = v.annualized_vol(0.7, 5);
        assert!(sigma < 1e-6);
    }

    #[test]
    fn stride_subsamples_correctly() {
        let mut v = VolEstimator::new(0.1, 0.5, 5);
        // 15 ticks with stride=5:
        //   tick 5  → sets last_price (no prior → no variance update)
        //   tick 10 → computes log_ret, update_count = 1
        //   tick 15 → computes log_ret, update_count = 2
        for _ in 0..15 {
            v.update(65000.0);
        }
        assert_eq!(v.update_count(), 2);
    }

    #[test]
    fn volatile_prices_give_positive_vol() {
        let mut v = VolEstimator::new(0.1, 0.5, 1);
        let prices = [65000.0, 65100.0, 64900.0, 65050.0, 65200.0, 64800.0, 65100.0];
        for p in prices {
            v.update(p);
        }
        let sigma = v.annualized_vol(0.7, 3);
        assert!(sigma > 0.0);
    }
}

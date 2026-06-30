use pm_core::domain::{ExchangeId, TopOfBook};

/// Exponentially-weighted moving-average order book imbalance for one exchange.
///
/// OBI = (V_bid − V_ask) / (V_bid + V_ask) ∈ [−1, +1]
///
/// EWMA smoothing filters spoof orders that flash and cancel within the 500ms window.
pub struct EwmaObi {
    alpha: f64,
    smoothed: f64,
}

impl EwmaObi {
    pub fn new(alpha: f64) -> Self {
        Self { alpha, smoothed: 0.0 }
    }

    /// Update with a new top-of-book snapshot. No-op if the book is empty (both sides zero).
    pub fn update(&mut self, tob: &TopOfBook) {
        if tob.is_empty() {
            return;
        }
        let total = tob.bid_vol + tob.ask_vol;
        if total < 1e-12 {
            return;
        }
        let raw = (tob.bid_vol - tob.ask_vol) / total;
        self.smoothed = self.alpha * raw + (1.0 - self.alpha) * self.smoothed;
    }

    pub fn smoothed(&self) -> f64 {
        self.smoothed
    }

    /// Additive probability drift nudge, capped to ±`max_drift`.
    ///
    /// OBI is a secondary signal — it should nudge P_base by a few probability
    /// points at most, not override the core binary-option price.
    pub fn drift_nudge(&self, max_drift: f64) -> f64 {
        self.smoothed * max_drift
    }
}

/// Volume-weighted OBI across all connected exchanges.
///
/// Volume weights are updated each time a new snapshot arrives for an exchange.
/// When no order-book data has been received (weights all zero), falls back
/// to an unweighted average — which is zero until any feed connects.
pub struct MultiExchangeObi {
    trackers: Vec<(ExchangeId, EwmaObi, f64)>,
    max_drift: f64,
}

impl MultiExchangeObi {
    pub fn new(alpha: f64, max_drift: f64) -> Self {
        let trackers = ExchangeId::ALL
            .iter()
            .map(|&id| (id, EwmaObi::new(alpha), 0.0_f64))
            .collect();
        Self { trackers, max_drift }
    }

    /// Update one exchange's top-of-book and recalculate its volume weight.
    pub fn update(&mut self, id: ExchangeId, tob: &TopOfBook) {
        let vol = tob.bid_vol + tob.ask_vol;
        for (eid, obi, weight) in &mut self.trackers {
            if *eid == id {
                obi.update(tob);
                *weight = vol;
                break;
            }
        }
    }

    /// Aggregate drift nudge: volume-weighted across exchanges.
    /// Falls back to an unweighted mean when no volume data is available,
    /// which resolves to 0.0 before any feed connects.
    pub fn drift_nudge(&self) -> f64 {
        let total_weight: f64 = self.trackers.iter().map(|(_, _, w)| w).sum();
        if total_weight < 1e-12 {
            let n = self.trackers.len() as f64;
            if n == 0.0 {
                return 0.0;
            }
            return self
                .trackers
                .iter()
                .map(|(_, obi, _)| obi.drift_nudge(self.max_drift))
                .sum::<f64>()
                / n;
        }
        self.trackers
            .iter()
            .map(|(_, obi, w)| obi.drift_nudge(self.max_drift) * w / total_weight)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bid_heavy_gives_positive_drift() {
        let mut obi = EwmaObi::new(1.0);
        obi.update(&TopOfBook {
            bid_price: 100.0,
            bid_vol: 50.0,
            ask_price: 101.0,
            ask_vol: 10.0,
        });
        // OBI = (50-10)/(50+10) = 40/60 ≈ 0.667; nudge = 0.667 * 0.03 ≈ 0.02
        let nudge = obi.drift_nudge(0.03);
        assert!(nudge > 0.0);
        assert!(nudge <= 0.03);
    }

    #[test]
    fn ask_heavy_gives_negative_drift() {
        let mut obi = EwmaObi::new(1.0);
        obi.update(&TopOfBook {
            bid_price: 100.0,
            bid_vol: 10.0,
            ask_price: 101.0,
            ask_vol: 50.0,
        });
        assert!(obi.drift_nudge(0.03) < 0.0);
    }

    #[test]
    fn balanced_book_is_neutral() {
        let mut obi = EwmaObi::new(1.0);
        obi.update(&TopOfBook {
            bid_price: 100.0,
            bid_vol: 25.0,
            ask_price: 101.0,
            ask_vol: 25.0,
        });
        assert!(obi.smoothed().abs() < 1e-10);
    }

    #[test]
    fn empty_book_is_no_op() {
        let mut obi = EwmaObi::new(1.0);
        let before = obi.smoothed();
        obi.update(&TopOfBook::default());
        assert_eq!(obi.smoothed(), before);
    }

    #[test]
    fn multi_exchange_zero_before_feeds_connect() {
        let multi = MultiExchangeObi::new(0.15, 0.03);
        assert_eq!(multi.drift_nudge(), 0.0);
    }
}

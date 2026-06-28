pub mod normal;
pub mod obi;
pub mod signal;
pub mod vol;

pub use obi::ExchangeId;
pub use signal::QuantSignal;

use normal::{d2, phi};
use obi::{MultiExchangeObi, TopOfBook};
use vol::VolEstimator;

const SECS_PER_YEAR: f64 = 365.25 * 24.0 * 3600.0;

/// Tunable parameters for `QuantModel`.
#[derive(Debug, Clone)]
pub struct QuantModelConfig {
    /// EWMA alpha for the variance estimator. Centre-of-mass ≈ (1−α)/α updates.
    /// Default 0.05 → ~19 updates of memory.
    pub vol_alpha: f64,
    /// Subsample the vol estimator every N ticks to reduce microstructure noise bias.
    /// At 500 ms ticks, stride=10 → one sample per 5 s.
    pub vol_update_stride: u32,
    /// Fallback annualised σ used before `vol_warmup_updates` samples are available.
    /// Conservative BTC estimate: 0.70 = 70% annual.
    pub cold_vol_fallback: f64,
    /// How many variance updates are needed before the estimator is trusted.
    pub vol_warmup_updates: u32,
    /// EWMA alpha for OBI smoothing. Default 0.15 → ~5.7 updates of memory.
    pub obi_alpha: f64,
    /// Maximum absolute probability shift from the OBI drift nudge.
    /// Kept small: OBI is a secondary signal, not the pricing backbone.
    /// 0.03 = 3 probability points.
    pub max_obi_drift: f64,
}

impl Default for QuantModelConfig {
    fn default() -> Self {
        Self {
            vol_alpha: 0.05,
            vol_update_stride: 10,
            cold_vol_fallback: 0.70,
            vol_warmup_updates: 6,
            obi_alpha: 0.15,
            max_obi_drift: 0.03,
        }
    }
}

/// Stateful quant signal aggregator.
///
/// Combines two components:
/// 1. **Binary-option probability** — `Φ(d₂)` where `d₂ = ln(S/K) / (σ√T)`.
///    This is the dominant signal; it encodes the current spot-vs-strike
///    displacement and how much time is left for the price to move.
/// 2. **EWMA OBI drift nudge** — small additive correction from order-book
///    imbalance, capped at ±`max_obi_drift`. Acts as a tie-breaker near 50%,
///    never as the primary thesis.
///
/// # Call order per tick
/// 1. `update_price(spot)` — must be called on every price tick to keep
///    the vol estimator current, even during the hold phase.
/// 2. `update_order_book(id, tob)` — call whenever a new order-book snapshot
///    arrives (placeholder: no-op until streaming is connected).
/// 3. `compute_signal(spot, strike, secs_to_cutoff)` — returns `QuantSignal`
///    with all intermediate outputs.
pub struct QuantModel {
    vol: VolEstimator,
    obi: MultiExchangeObi,
    config: QuantModelConfig,
}

impl QuantModel {
    pub fn new(config: QuantModelConfig) -> Self {
        Self {
            vol: VolEstimator::new(config.vol_alpha, 0.5, config.vol_update_stride),
            obi: MultiExchangeObi::new(config.obi_alpha, config.max_obi_drift),
            config,
        }
    }

    /// Feed the latest BTC price. Call on every tick.
    pub fn update_price(&mut self, price: f64) {
        self.vol.update(price);
    }

    /// Feed an order-book snapshot for one exchange.
    ///
    /// **Placeholder** — no-op until WebSocket order-book streaming is connected.
    /// Wire this into the exchange connectors when they are implemented.
    pub fn update_order_book(&mut self, id: ExchangeId, tob: TopOfBook) {
        self.obi.update(id, &tob);
    }

    /// Compute the current `QuantSignal` given latest spot, strike, and time.
    pub fn compute_signal(&self, spot: f64, strike: f64, secs_to_cutoff: f64) -> QuantSignal {
        let sigma = self
            .vol
            .annualized_vol(self.config.cold_vol_fallback, self.config.vol_warmup_updates);
        let vol_cold = self.vol.update_count() < self.config.vol_warmup_updates;

        let t_years = secs_to_cutoff.max(0.0) / SECS_PER_YEAR;
        let sigma_sqrt_t = sigma * t_years.sqrt();
        let log_moneyness = if spot > 0.0 && strike > 0.0 {
            (spot / strike).ln()
        } else {
            0.0
        };

        let d2_val = d2(spot, strike, sigma, secs_to_cutoff);
        let p_base = phi(d2_val);
        let obi_drift = self.obi.drift_nudge();
        let p_model = (p_base + obi_drift).clamp(0.0, 1.0);

        QuantSignal {
            log_moneyness,
            sigma_sqrt_t,
            sigma,
            secs_to_cutoff,
            d2: d2_val,
            p_base,
            obi_drift,
            p_model,
            vol_cold,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn at_strike_cold_is_near_half() {
        let model = QuantModel::new(QuantModelConfig::default());
        let sig = model.compute_signal(65000.0, 65000.0, 300.0);
        assert!((sig.p_base - 0.5).abs() < 1e-6);
        assert!((sig.p_model - 0.5).abs() < 1e-3);
        assert!(sig.vol_cold);
    }

    #[test]
    fn below_strike_gives_less_than_half() {
        let model = QuantModel::new(QuantModelConfig::default());
        let sig = model.compute_signal(64990.0, 65000.0, 300.0);
        assert!(sig.p_model < 0.5);
        assert!(sig.d2 < 0.0);
    }

    #[test]
    fn above_strike_gives_more_than_half() {
        let model = QuantModel::new(QuantModelConfig::default());
        let sig = model.compute_signal(65010.0, 65000.0, 300.0);
        assert!(sig.p_model > 0.5);
        assert!(sig.d2 > 0.0);
    }

    #[test]
    fn vol_warms_up_from_price_updates() {
        let mut model = QuantModel::new(QuantModelConfig {
            vol_update_stride: 1,
            vol_warmup_updates: 3,
            ..QuantModelConfig::default()
        });
        for i in 0..20 {
            model.update_price(65000.0 + i as f64 * 5.0);
        }
        let sig = model.compute_signal(65000.0, 65000.0, 300.0);
        assert!(!sig.vol_cold);
        assert!(sig.sigma > 0.0);
    }

    #[test]
    fn obi_drift_within_bounds() {
        let model = QuantModel::new(QuantModelConfig::default());
        let sig = model.compute_signal(65000.0, 65000.0, 300.0);
        assert!(sig.obi_drift.abs() <= 0.03);
    }

    #[test]
    fn zero_time_is_deterministic() {
        let model = QuantModel::new(QuantModelConfig::default());
        let sig_up = model.compute_signal(65010.0, 65000.0, 0.0);
        let sig_down = model.compute_signal(64990.0, 65000.0, 0.0);
        assert!(sig_up.p_base > 0.99);
        assert!(sig_down.p_base < 0.01);
    }
}

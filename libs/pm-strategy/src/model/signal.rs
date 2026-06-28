use std::fmt;

/// All intermediate outputs of one `QuantModel` evaluation.
///
/// Carry all fields into your logs and store them for calibration.
/// The central check is: when `p_model` was N%, did the Up outcome
/// occur N% of the time? That's what tells you whether the model is real.
#[derive(Debug, Clone)]
pub struct QuantSignal {
    /// ln(spot / strike) — signed displacement from the strike in log-space.
    pub log_moneyness: f64,
    /// σ√T — one-sigma price move of the underlying over the remaining window.
    pub sigma_sqrt_t: f64,
    /// Annualised volatility estimate (σ).
    pub sigma: f64,
    /// Seconds remaining until market cutoff.
    pub secs_to_cutoff: f64,
    /// d₂ = log_moneyness / sigma_sqrt_t. The argument of Φ.
    pub d2: f64,
    /// Φ(d₂) — pure binary-option probability, before any OBI correction.
    pub p_base: f64,
    /// Additive drift nudge from EWMA order-book imbalance.
    /// Small by design: max ±`max_obi_drift` (default 0.03 = 3pp).
    pub obi_drift: f64,
    /// Final model probability: p_base + obi_drift, clamped to [0, 1].
    pub p_model: f64,
    /// True when the vol estimator has not yet warmed up.
    /// When cold, `sigma` is the configured fallback, not a live estimate.
    pub vol_cold: bool,
}

impl QuantSignal {
    /// Signed edge: p_model − 0.5.
    /// Positive → model favours Up. Negative → model favours Down.
    pub fn edge(&self) -> f64 {
        self.p_model - 0.5
    }
}

impl fmt::Display for QuantSignal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "p_model={:.3} p_base={:.3} obi={:+.3} d2={:.3} σ={:.1}% σ√T={:.4} T={}s{}",
            self.p_model,
            self.p_base,
            self.obi_drift,
            self.d2,
            self.sigma * 100.0,
            self.sigma_sqrt_t,
            self.secs_to_cutoff as i64,
            if self.vol_cold { " [cold]" } else { "" },
        )
    }
}

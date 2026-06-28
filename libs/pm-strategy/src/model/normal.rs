use std::f64::consts;

/// Standard normal CDF, accurate to ~7.5 × 10⁻⁸ (Abramowitz & Stegun 26.2.17).
pub fn phi(x: f64) -> f64 {
    const P: f64 = 0.2316419;
    const B: [f64; 5] = [
        0.319381530,
        -0.356563782,
        1.781477937,
        -1.821255978,
        1.330274429,
    ];
    let t = 1.0 / (1.0 + P * x.abs());
    let t2 = t * t;
    let t3 = t2 * t;
    let t4 = t3 * t;
    let t5 = t4 * t;
    let poly = B[0] * t + B[1] * t2 + B[2] * t3 + B[3] * t4 + B[4] * t5;
    let pdf = (-0.5 * x * x).exp() / (2.0 * consts::PI).sqrt();
    let cdf_upper = 1.0 - pdf * poly;
    if x >= 0.0 {
        cdf_upper
    } else {
        1.0 - cdf_upper
    }
}

/// d₂ for a zero-drift binary call: ln(S/K) / (σ√T).
///
/// As T → 0 the outcome becomes deterministic:
///   spot > strike → +∞ → P_up = 1.0
///   spot < strike → -∞ → P_up = 0.0
///   spot = strike → 0.0 → P_up = 0.5
pub fn d2(spot: f64, strike: f64, sigma: f64, t_secs: f64) -> f64 {
    const SECS_PER_YEAR: f64 = 365.25 * 24.0 * 3600.0;
    if spot <= 0.0 || strike <= 0.0 || sigma <= 0.0 {
        return 0.0;
    }
    let sigma_sqrt_t = sigma * (t_secs.max(0.0) / SECS_PER_YEAR).sqrt();
    if sigma_sqrt_t < 1e-12 {
        return if spot > strike {
            f64::INFINITY
        } else if spot < strike {
            f64::NEG_INFINITY
        } else {
            0.0
        };
    }
    (spot / strike).ln() / sigma_sqrt_t
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phi_standard_values() {
        assert!((phi(0.0) - 0.5).abs() < 1e-6);
        assert!((phi(1.0) - 0.8413).abs() < 1e-4);
        assert!((phi(-1.0) - 0.1587).abs() < 1e-4);
        assert!((phi(1.96) - 0.9750).abs() < 1e-3);
    }

    #[test]
    fn phi_symmetry() {
        for x in [-2.0_f64, -1.0, -0.5, 0.5, 1.0, 2.0] {
            assert!((phi(x) + phi(-x) - 1.0).abs() < 1e-10);
        }
    }

    #[test]
    fn d2_at_the_money() {
        assert_eq!(d2(65000.0, 65000.0, 0.5, 300.0), 0.0);
    }

    #[test]
    fn d2_below_strike_is_negative() {
        let v = d2(64990.0, 65000.0, 0.5, 300.0);
        assert!(v < 0.0);
        // ln(64990/65000) / (0.5 * sqrt(300/SECS_PER_YEAR)) ≈ -0.1
        assert!((v - (-0.1)).abs() < 0.02);
    }

    #[test]
    fn d2_zero_time_deterministic() {
        assert_eq!(d2(65001.0, 65000.0, 0.5, 0.0), f64::INFINITY);
        assert_eq!(d2(64999.0, 65000.0, 0.5, 0.0), f64::NEG_INFINITY);
    }
}

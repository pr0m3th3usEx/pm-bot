/// Fractional Kelly position sizing utilities.
///
/// # Integration note
/// The current `SizingModel::size()` interface takes only `(bankroll, limit_price)`.
/// Full Kelly requires the model probability `p`, which lives in
/// `StrategyDecision::Enter { confidence }`. The decision center can call
/// `kelly_shares()` directly when `confidence` is `Some`, bypassing the
/// `SizingModel` trait for Kelly-aware strategies.
///
/// These functions are the pure math layer; wiring them into the execution
/// path is a separate step.

/// Full Kelly fraction for a binary bet.
///
/// `p` — model probability of the favourable outcome.
/// `b` — net odds per unit risked: `(1 / ask_price) − 1` for a binary outcome.
///
/// Returns the Kelly-optimal bankroll fraction to wager, clamped to [0, 1].
/// A negative value means no edge — do not trade.
pub fn kelly_fraction(p: f64, b: f64) -> f64 {
    if b <= 0.0 {
        return 0.0;
    }
    let q = 1.0 - p;
    ((p * b - q) / b).clamp(0.0, 1.0)
}

/// Fractional Kelly: scale the full Kelly fraction by `multiplier`.
///
/// Typical choices: 0.25 (quarter-Kelly) or 0.5 (half-Kelly).
/// Full Kelly (multiplier = 1.0) maximises long-run growth but requires
/// a perfectly calibrated model probability — use fractional Kelly in practice.
pub fn fractional_kelly(p: f64, b: f64, multiplier: f64) -> f64 {
    kelly_fraction(p, b) * multiplier.clamp(0.0, 1.0)
}

/// Compute Kelly-sized shares from a bankroll, model probability, and execution price.
///
/// Returns `(budget_usdc, shares)`. Budget is zero when there is no edge
/// (p_model ≤ 0.5 at fair odds, or p_model doesn't cover the bid-ask spread).
pub fn kelly_shares(
    bankroll_usdc: f64,
    p: f64,
    ask_price: f64,
    kelly_multiplier: f64,
) -> (f64, f64) {
    if ask_price <= 0.0 || ask_price >= 1.0 {
        return (0.0, 0.0);
    }
    let b = 1.0 / ask_price - 1.0;
    let frac = fractional_kelly(p, b, kelly_multiplier);
    let budget = bankroll_usdc * frac;
    let shares = budget / ask_price;
    (budget, shares)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fair_bet_zero_edge() {
        // p=0.5, ask=0.5 → b=1.0 → kelly = (0.5×1 − 0.5)/1 = 0.0
        assert!((kelly_fraction(0.5, 1.0)).abs() < 1e-10);
    }

    #[test]
    fn positive_edge() {
        // p=0.6, ask=0.5 → b=1.0 → kelly = (0.6×1 − 0.4)/1 = 0.2
        assert!((kelly_fraction(0.6, 1.0) - 0.2).abs() < 1e-10);
    }

    #[test]
    fn fractional_kelly_scales() {
        // 0.2 × 0.25 = 0.05
        assert!((fractional_kelly(0.6, 1.0, 0.25) - 0.05).abs() < 1e-10);
    }

    #[test]
    fn negative_edge_clamped_to_zero() {
        // p=0.4 → kelly = (0.4 − 0.6)/1 = -0.2, clamped to 0
        assert_eq!(kelly_fraction(0.4, 1.0), 0.0);
    }

    #[test]
    fn kelly_shares_example() {
        // bankroll=1000, p=0.6, ask=0.5, multiplier=0.25
        // b=1.0, kelly=0.2, frac=0.05, budget=50, shares=100
        let (budget, shares) = kelly_shares(1000.0, 0.6, 0.5, 0.25);
        assert!((budget - 50.0).abs() < 1e-6);
        assert!((shares - 100.0).abs() < 1e-6);
    }

    #[test]
    fn invalid_ask_price_returns_zero() {
        assert_eq!(kelly_shares(1000.0, 0.6, 0.0, 0.25), (0.0, 0.0));
        assert_eq!(kelly_shares(1000.0, 0.6, 1.0, 0.25), (0.0, 0.0));
    }
}

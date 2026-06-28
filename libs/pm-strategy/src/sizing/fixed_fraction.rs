use pm_core::ports::SizingModel;
use pm_core::types::{Price, Shares, Usdc};
use rust_decimal::Decimal;
use tracing::debug;

// ─── V1 fixed-fraction sizing model ──────────────────────────────────────────

/// Fraction of bankroll to deploy per trade (5%).
// Note: rust_decimal_macros::dec! does not produce a const-compatible value on all
// toolchains, so SIZING_FRACTION is built via Decimal::from_parts (5 × 10^-2 = 0.05).
pub const SIZING_FRACTION: Decimal = Decimal::from_parts(5, 0, 0, false, 2);

/// Sizes each trade as `(fraction × bankroll) / price` shares.
pub struct FixedFractionSizingModel {
    pub fraction: Decimal,
}

impl FixedFractionSizingModel {
    pub fn new(fraction: Decimal) -> Self {
        Self { fraction }
    }
}

impl SizingModel for FixedFractionSizingModel {
    fn size(&self, bankroll: &Usdc, limit_price: &Price) -> Shares {
        if limit_price.0 <= Decimal::ZERO {
            return Shares(Decimal::ZERO);
        }

        debug!(
            bankroll = %bankroll.0,
            limit_price = %limit_price.0,
            fraction = %self.fraction,
            "sizing model: computing shares"
        );

        let budget = self.fraction * bankroll.0;
        let raw = budget / limit_price.0;

        debug!(
            budget = %budget,
            raw_shares = %raw,
            "sizing model: computed raw shares"
        );

        Shares(raw)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn model(fraction: Decimal) -> FixedFractionSizingModel {
        FixedFractionSizingModel::new(fraction)
    }

    #[test]
    fn sizing_standard_case() {
        // bankroll=100, fraction=0.05, price=0.50 => budget=5, shares=5/0.5=10
        let m = model(dec!(0.05));
        let shares = m.size(&Usdc(dec!(100)), &Price(dec!(0.50)));
        assert_eq!(shares, Shares(dec!(10)));
    }

    #[test]
    fn sizing_fractional_shares() {
        // bankroll=100, fraction=0.05, price=0.40 => budget=5, shares=5/0.40=12.5 (no flooring)
        let m = model(dec!(0.05));
        let shares = m.size(&Usdc(dec!(100)), &Price(dec!(0.40)));
        assert_eq!(shares, Shares(dec!(12.5)));
    }

    #[test]
    fn sizing_small_bankroll() {
        // bankroll=10, fraction=0.05, price=0.50 => budget=0.5, shares=0.5/0.5=1
        let m = model(dec!(0.05));
        let shares = m.size(&Usdc(dec!(10)), &Price(dec!(0.50)));
        assert_eq!(shares, Shares(dec!(1)));
    }

    #[test]
    fn sizing_sub_one_fraction() {
        // bankroll=10, fraction=0.05, price=0.80 => budget=0.5, shares=0.5/0.8=0.625 (no flooring to 0)
        let m = model(dec!(0.05));
        let shares = m.size(&Usdc(dec!(10)), &Price(dec!(0.80)));
        assert_eq!(shares, Shares(dec!(0.625)));
    }

    #[test]
    fn sizing_zero_price_guard() {
        // price=0 must return 0 without panicking
        let m = model(dec!(0.05));
        let shares = m.size(&Usdc(dec!(100)), &Price(dec!(0)));
        assert_eq!(shares, Shares(dec!(0)));
    }

    #[test]
    fn sizing_fraction_constant_is_five_percent() {
        assert_eq!(SIZING_FRACTION, dec!(0.05));
    }
}

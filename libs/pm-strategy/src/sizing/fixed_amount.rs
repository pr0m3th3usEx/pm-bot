use pm_core::ports::SizingModel;
use pm_core::types::{Price, Shares, Usdc};
use rust_decimal::Decimal;
use tracing::debug;

// ─── V1 fixed-amount sizing model ──────────────────────────────────────────

/// Sizes each trade as `amount / price` shares — a fixed USDC notional per trade,
/// independent of bankroll (fractional shares allowed).
pub struct FixedAmountSizingModel {
    pub amount: Usdc,
}

impl FixedAmountSizingModel {
    pub fn new(amount: Usdc) -> Self {
        Self { amount }
    }
}

impl SizingModel for FixedAmountSizingModel {
    fn size(&self, _bankroll: &Usdc, limit_price: &Price) -> Shares {
        if limit_price.0 <= Decimal::ZERO {
            return Shares(Decimal::ZERO);
        }

        debug!(
            amount = %self.amount.0,
            limit_price = %limit_price.0,
            "sizing model: computing shares"
        );

        let raw = self.amount.0 / limit_price.0;

        debug!(
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

    fn model(amount: Decimal) -> FixedAmountSizingModel {
        FixedAmountSizingModel::new(Usdc(amount))
    }

    #[test]
    fn sizing_standard_case() {
        // amount=$10, price=0.50 => 10/0.50 = 20 shares
        let m = model(dec!(10));
        let shares = m.size(&Usdc(dec!(100)), &Price(dec!(0.50)));
        assert_eq!(shares, Shares(dec!(20)));
    }

    #[test]
    fn sizing_fractional_shares() {
        // amount=$5, price=0.40 => 5/0.40 = 12.5 shares (no flooring)
        let m = model(dec!(5));
        let shares = m.size(&Usdc(dec!(100)), &Price(dec!(0.40)));
        assert_eq!(shares, Shares(dec!(12.5)));
    }

    #[test]
    fn sizing_ignores_bankroll() {
        // Bankroll is irrelevant: same amount + price => same shares regardless of bankroll.
        let m = model(dec!(10));
        let small = m.size(&Usdc(dec!(1)), &Price(dec!(0.50)));
        let large = m.size(&Usdc(dec!(1_000_000)), &Price(dec!(0.50)));
        assert_eq!(small, Shares(dec!(20)));
        assert_eq!(large, Shares(dec!(20)));
    }

    #[test]
    fn sizing_zero_price_guard() {
        // price=0 must return 0 without panicking (division by zero)
        let m = model(dec!(10));
        let shares = m.size(&Usdc(dec!(100)), &Price(dec!(0)));
        assert_eq!(shares, Shares(dec!(0)));
    }
}

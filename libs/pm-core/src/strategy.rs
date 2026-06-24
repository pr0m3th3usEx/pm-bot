use crate::domain::Market;
use crate::ports::Strategy;
use crate::types::{Outcome, Price, Timestamp};
use rust_decimal::Decimal;

/// All inputs the strategy needs to decide hold-or-enter on a single tick.
/// Pure data — no I/O, no state.
pub struct StrategyContext<'a> {
    /// Latest BTC tick price.
    pub price: Price,
    /// Price to beat, sourced from Gamma API Market Event data via Market.strike.
    pub strike: Price,
    pub now: Timestamp,
    /// Trading cutoff: order must submit before this.
    pub closes_at: Timestamp,
    pub resolves_at: Timestamp,
    pub market: &'a Market,
}

impl<'a> StrategyContext<'a> {
    /// Seconds remaining until the trading window closes.
    pub fn secs_to_cutoff(&self) -> i64 {
        self.closes_at.as_secs() - self.now.as_secs()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StrategyDecision {
    Hold,
    Enter { outcome: Outcome },
}

// ─── V1 basic strategy ────────────────────────────────────────────────────────

/// Enter only when:
///   - time remaining ≤ `entry_window_secs`, AND
///   - |price − strike| ≥ `margin`
/// Picks Up if price > strike, Down otherwise.
pub struct V1BasicStrategy {
    /// Seconds-to-cutoff threshold below which we consider entering.
    pub entry_window_secs: i64,
    /// Minimum absolute price−strike gap required to enter.
    pub margin: Decimal,
}

impl V1BasicStrategy {
    pub fn new(entry_window_secs: i64, margin: Decimal) -> Self {
        Self {
            entry_window_secs,
            margin,
        }
    }
}

impl Strategy for V1BasicStrategy {
    fn evaluate(&self, ctx: &StrategyContext) -> StrategyDecision {
        let secs_left = ctx.secs_to_cutoff();

        // Outside our entry window — always hold.
        if secs_left > self.entry_window_secs || secs_left <= 0 {
            return StrategyDecision::Hold;
        }

        let diff = (ctx.price.0 - ctx.strike.0).abs();

        if diff < self.margin {
            return StrategyDecision::Hold;
        }

        let outcome = if ctx.price.0 > ctx.strike.0 {
            Outcome::Up
        } else {
            Outcome::Down
        };

        StrategyDecision::Enter { outcome }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Market, MarketOutcome};
    use crate::types::{MarketSlug, MarketStatus, MarketType, TokenId};
    use alloy::hex::FromHex;
    use alloy::primitives::FixedBytes;
    use polymarket_client_sdk_v2::types::U256;
    use rust_decimal_macros::dec;

    fn dummy_market() -> Market {
        Market {
            slug: MarketSlug("btc-updown-5m-1000".into()),
            market_type: MarketType::UpDown,
            event_id: "e1".into(),
            question_id: FixedBytes::from_hex(
                "0x0000000000000000000000000000000000000000000000000000000000000001",
            )
            .unwrap(),
            condition_id: FixedBytes::from_hex(
                "0x0000000000000000000000000000000000000000000000000000000000000001",
            )
            .unwrap(),
            outcomes: vec![
                MarketOutcome {
                    name: "up".into(),
                    token_id: TokenId(U256::from(1u64)),
                },
                MarketOutcome {
                    name: "down".into(),
                    token_id: TokenId(U256::from(2u64)),
                },
            ],
            strike: None,
            opens_at: Timestamp(0),
            closes_at: Timestamp::from_secs(1000),
            resolves_at: Timestamp::from_secs(1300),
            status: MarketStatus::Open,
        }
    }

    #[test]
    fn enters_when_price_above_strike_in_window() {
        let s = V1BasicStrategy::new(120, dec!(0.02));
        let market = dummy_market();
        let ctx = StrategyContext {
            price: Price(dec!(0.60)),
            strike: Price(dec!(0.55)),
            now: Timestamp::from_secs(900), // 100 s before closes_at 1000
            closes_at: Timestamp::from_secs(1000),
            resolves_at: Timestamp::from_secs(1300),
            market: &market,
        };
        assert_eq!(
            s.evaluate(&ctx),
            StrategyDecision::Enter {
                outcome: Outcome::Up
            }
        );
    }

    #[test]
    fn holds_when_outside_entry_window() {
        let s = V1BasicStrategy::new(120, dec!(0.02));
        let market = dummy_market();
        let ctx = StrategyContext {
            price: Price(dec!(0.60)),
            strike: Price(dec!(0.55)),
            now: Timestamp::from_secs(700), // 300 s before cutoff — outside 120 s window
            closes_at: Timestamp::from_secs(1000),
            resolves_at: Timestamp::from_secs(1300),
            market: &market,
        };
        assert_eq!(s.evaluate(&ctx), StrategyDecision::Hold);
    }

    #[test]
    fn holds_when_margin_too_small() {
        let s = V1BasicStrategy::new(120, dec!(0.05));
        let market = dummy_market();
        let ctx = StrategyContext {
            price: Price(dec!(0.57)),
            strike: Price(dec!(0.55)), // diff = 0.02 < margin 0.05
            now: Timestamp::from_secs(900),
            closes_at: Timestamp::from_secs(1000),
            resolves_at: Timestamp::from_secs(1300),
            market: &market,
        };
        assert_eq!(s.evaluate(&ctx), StrategyDecision::Hold);
    }

    #[test]
    fn enters_down_when_price_below_strike() {
        let s = V1BasicStrategy::new(120, dec!(0.02));
        let market = dummy_market();
        let ctx = StrategyContext {
            price: Price(dec!(0.40)),
            strike: Price(dec!(0.55)),
            now: Timestamp::from_secs(900),
            closes_at: Timestamp::from_secs(1000),
            resolves_at: Timestamp::from_secs(1300),
            market: &market,
        };
        assert_eq!(
            s.evaluate(&ctx),
            StrategyDecision::Enter {
                outcome: Outcome::Down
            }
        );
    }
}

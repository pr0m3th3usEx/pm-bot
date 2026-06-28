use pm_core::ports::Strategy;
use pm_core::strategy::{StrategyContext, StrategyDecision};
use pm_core::types::Outcome;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

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

        StrategyDecision::Enter { outcome, confidence: None }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pm_core::domain::{Market, MarketOutcome};
    use pm_core::types::{MarketSlug, MarketStatus, MarketType, Price, Shares, Timestamp, TokenId};
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
            order_price_min_tick_size: Price(dec!(0.01)),
            order_min_size: Shares(dec!(5)),
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
                outcome: Outcome::Up,
                confidence: None,
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
                outcome: Outcome::Down,
                confidence: None,
            }
        );
    }
}

// ─── V2 quantitative strategy ─────────────────────────────────────────────────

use crate::model::{QuantModel, QuantModelConfig, QuantSignal};
use std::sync::Mutex;

/// Configuration for `QuantStrategy`.
#[derive(Debug, Clone)]
pub struct QuantStrategyConfig {
    pub model: QuantModelConfig,
    /// Only consider entering within this many seconds before cutoff (e.g., 180).
    pub entry_window_secs: i64,
    /// Never enter within this many seconds of cutoff — liquidity dries up (e.g., 30).
    pub danger_zone_secs: i64,
    /// Minimum |P_model − 0.5| required to generate an Enter signal (e.g., 0.05 = 5pp).
    pub min_edge: f64,
}

impl Default for QuantStrategyConfig {
    fn default() -> Self {
        Self {
            model: QuantModelConfig::default(),
            entry_window_secs: 180,
            danger_zone_secs: 30,
            min_edge: 0.05,
        }
    }
}

/// Quantitative strategy based on binary-option pricing and EWMA order-book imbalance.
///
/// # Model summary
/// The core probability is `P_up = Φ(d₂)` where `d₂ = ln(S/K) / (σ√T)`:
/// - S = current BTC spot
/// - K = market strike (previous window close price)
/// - σ = live EWMA realised vol (annualised)
/// - T = seconds to cutoff (converted to years)
///
/// A small additive OBI drift nudge (capped at ±3pp) is then applied.
/// The nudge is proportional to the EWMA-smoothed order-book imbalance across
/// connected exchanges — which are **placeholder / zero** until the order-book
/// streaming layer is connected.
///
/// # Entry rule
/// Signal enters when `|P_model − 0.5| ≥ min_edge` and time is within
/// `(danger_zone_secs, entry_window_secs]` seconds of cutoff.
///
/// `confidence` is returned in `StrategyDecision::Enter` so the decision center
/// can use it for Kelly sizing and calibration logging.
pub struct QuantStrategy {
    model: Mutex<QuantModel>,
    entry_window_secs: i64,
    danger_zone_secs: i64,
    min_edge: f64,
}

impl QuantStrategy {
    pub fn new(config: QuantStrategyConfig) -> Self {
        Self {
            model: Mutex::new(QuantModel::new(config.model)),
            entry_window_secs: config.entry_window_secs,
            danger_zone_secs: config.danger_zone_secs,
            min_edge: config.min_edge,
        }
    }
}

impl Strategy for QuantStrategy {
    fn evaluate(&self, ctx: &StrategyContext) -> StrategyDecision {
        let secs_left = ctx.secs_to_cutoff();

        let spot = match ctx.price.0.to_f64() {
            Some(v) if v > 0.0 => v,
            _ => return StrategyDecision::Hold,
        };
        let strike = match ctx.strike.0.to_f64() {
            Some(v) if v > 0.0 => v,
            _ => return StrategyDecision::Hold,
        };

        // Update the vol estimator on every tick so it stays warm even
        // outside the entry window (the 5-minute window starts at T=300s;
        // we need historical returns to have a reliable sigma by T=180s).
        let signal: Option<QuantSignal> = {
            let mut model = self.model.lock().expect("QuantModel lock poisoned");
            model.update_price(spot);

            if secs_left > self.entry_window_secs || secs_left <= self.danger_zone_secs {
                None
            } else {
                Some(model.compute_signal(spot, strike, secs_left as f64))
            }
        };

        let signal = match signal {
            Some(s) => s,
            None => return StrategyDecision::Hold,
        };

        tracing::debug!(
            p_model = signal.p_model,
            p_base = signal.p_base,
            obi_drift = signal.obi_drift,
            d2 = signal.d2,
            sigma_pct = signal.sigma * 100.0,
            sigma_sqrt_t = signal.sigma_sqrt_t,
            secs_left,
            vol_cold = signal.vol_cold,
            "quant_signal"
        );

        let edge = signal.edge();
        if edge.abs() < self.min_edge {
            return StrategyDecision::Hold;
        }

        let outcome = if edge > 0.0 { Outcome::Up } else { Outcome::Down };
        StrategyDecision::Enter {
            outcome,
            confidence: Some(signal.p_model),
        }
    }
}

#[cfg(test)]
mod quant_tests {
    use super::*;
    use pm_core::domain::{Market, MarketOutcome};
    use pm_core::types::{
        MarketSlug, MarketStatus, MarketType, Price, Shares, Timestamp, TokenId,
    };
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
            order_price_min_tick_size: Price(dec!(0.01)),
            order_min_size: Shares(dec!(5)),
        }
    }

    fn make_ctx(price: Decimal, strike: Decimal, secs_before_close: i64, market: &Market) -> StrategyContext<'_> {
        let close = 1000_i64;
        StrategyContext {
            price: Price(price),
            strike: Price(strike),
            now: Timestamp::from_secs(close - secs_before_close),
            closes_at: Timestamp::from_secs(close),
            resolves_at: Timestamp::from_secs(1300),
            market,
        }
    }

    #[test]
    fn holds_outside_entry_window() {
        let s = QuantStrategy::new(QuantStrategyConfig {
            entry_window_secs: 180,
            danger_zone_secs: 30,
            min_edge: 0.0, // remove edge filter
            ..QuantStrategyConfig::default()
        });
        let market = dummy_market();
        let ctx = make_ctx(dec!(65100), dec!(65000), 300, &market);
        assert_eq!(s.evaluate(&ctx), StrategyDecision::Hold);
    }

    #[test]
    fn holds_in_danger_zone() {
        let s = QuantStrategy::new(QuantStrategyConfig {
            entry_window_secs: 180,
            danger_zone_secs: 30,
            min_edge: 0.0,
            ..QuantStrategyConfig::default()
        });
        let market = dummy_market();
        let ctx = make_ctx(dec!(65100), dec!(65000), 15, &market);
        assert_eq!(s.evaluate(&ctx), StrategyDecision::Hold);
    }

    #[test]
    fn holds_when_edge_below_threshold() {
        let s = QuantStrategy::new(QuantStrategyConfig {
            entry_window_secs: 180,
            danger_zone_secs: 30,
            min_edge: 0.10, // require 10pp edge
            ..QuantStrategyConfig::default()
        });
        let market = dummy_market();
        // spot ≈ strike → P_model ≈ 0.5 → edge ≈ 0 < 0.10
        let ctx = make_ctx(dec!(65000), dec!(65000), 60, &market);
        assert_eq!(s.evaluate(&ctx), StrategyDecision::Hold);
    }

    #[test]
    fn enters_up_with_confidence_when_far_above_strike() {
        let s = QuantStrategy::new(QuantStrategyConfig {
            entry_window_secs: 180,
            danger_zone_secs: 30,
            min_edge: 0.01, // low threshold to test direction
            ..QuantStrategyConfig::default()
        });
        let market = dummy_market();
        // Feed warm-up prices so the vol estimator isn't cold.
        // The strategy will see the same prices in previous evaluate() calls.
        // Here we just check direction — spot far above strike → should enter Up.
        // Cold-vol fallback (70%) + 10s to cutoff + $500 above strike → P_up high.
        let ctx = make_ctx(dec!(65500), dec!(65000), 60, &market);
        match s.evaluate(&ctx) {
            StrategyDecision::Enter { outcome, confidence } => {
                assert_eq!(outcome, Outcome::Up);
                assert!(confidence.is_some());
                let p = confidence.unwrap();
                assert!(p > 0.5, "p_model={p}");
            }
            StrategyDecision::Hold => panic!("expected Enter, got Hold"),
        }
    }

    #[test]
    fn enters_down_when_far_below_strike() {
        let s = QuantStrategy::new(QuantStrategyConfig {
            entry_window_secs: 180,
            danger_zone_secs: 30,
            min_edge: 0.01,
            ..QuantStrategyConfig::default()
        });
        let market = dummy_market();
        let ctx = make_ctx(dec!(64500), dec!(65000), 60, &market);
        match s.evaluate(&ctx) {
            StrategyDecision::Enter { outcome, confidence } => {
                assert_eq!(outcome, Outcome::Down);
                let p = confidence.unwrap();
                assert!(p < 0.5, "p_model={p}");
            }
            StrategyDecision::Hold => panic!("expected Enter, got Hold"),
        }
    }
}

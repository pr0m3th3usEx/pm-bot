use crate::indicators::{
    acceleration, crossover_sign, micro_momentum, CandleAggregator, Ema, Rsi, TickTrend,
};
use pm_core::ports::Strategy;
use pm_core::strategy::{StrategyContext, StrategyDecision};
use pm_core::types::Outcome;
use rust_decimal::prelude::ToPrimitive;
use std::sync::Mutex;

// ─── Configuration ─────────────────────────────────────────────────────────────

/// Tiered weight thresholds and per-component weights for [`CompositeStrategy`].
#[derive(Debug, Clone)]
pub struct CompositeWeights {
    // Window-delta tier thresholds and weights (applied by |wd_pct|, signed by wd_pct).
    pub wd_t1_pct: f64, // e.g. 0.10 → |wd%| > 0.10 → weight 7
    pub wd_t1_w: f64,
    pub wd_t2_pct: f64, // e.g. 0.02
    pub wd_t2_w: f64,
    pub wd_t3_pct: f64, // e.g. 0.005
    pub wd_t3_w: f64,
    pub wd_t4_pct: f64, // e.g. 0.001
    pub wd_t4_w: f64,
    /// Weight for the micro-momentum signal.
    pub micro_momentum: f64,
    /// Weight for the acceleration signal.
    pub acceleration: f64,
    /// Weight for the EMA crossover signal.
    pub ema_cross: f64,
    /// Weight for the RSI extreme signal (applied only at overbought >75 / oversold <25).
    pub rsi: f64,
    /// Weight for the tick-trend signal.
    pub tick_trend: f64,
}

impl Default for CompositeWeights {
    fn default() -> Self {
        Self {
            wd_t1_pct: 0.10,
            wd_t1_w: 7.0,
            wd_t2_pct: 0.02,
            wd_t2_w: 5.0,
            wd_t3_pct: 0.005,
            wd_t3_w: 3.0,
            wd_t4_pct: 0.001,
            wd_t4_w: 1.0,
            micro_momentum: 2.0,
            acceleration: 1.5,
            ema_cross: 1.0,
            rsi: 2.0,
            tick_trend: 2.0,
        }
    }
}

/// Configuration for [`CompositeStrategy`].
#[derive(Debug, Clone)]
pub struct CompositeConfig {
    /// Only consider entering within this many seconds before cutoff.
    pub entry_window_secs: i64,
    /// Never enter within this many seconds of cutoff — liquidity dries up.
    pub danger_zone_secs: i64,
    /// Minimum directional confidence required to emit an Enter signal.
    ///
    /// `confidence = (|score| / score_norm).min(1.0)`; must exceed this gate.
    pub min_confidence: f64,
    /// Width of each candle in seconds for the internal OHLC aggregator.
    pub candle_secs: i64,
    /// Maximum number of closed candles to retain in the rolling history.
    pub candle_history: usize,
    /// Rolling window size for the tick-trend indicator.
    pub tick_trend_window: usize,
    /// Per-component weights.
    pub weights: CompositeWeights,
    /// Divisor used to normalise the raw score into a `[0, 1]` confidence value.
    ///
    /// Set to the approximate maximum achievable score so that a fully-aligned
    /// signal yields `confidence ≈ 1.0`.
    pub score_norm: f64,
}

impl Default for CompositeConfig {
    fn default() -> Self {
        Self {
            entry_window_secs: 180,
            danger_zone_secs: 30,
            min_confidence: 0.40,
            candle_secs: 60,
            candle_history: 30,
            tick_trend_window: 30,
            weights: CompositeWeights::default(),
            score_norm: 7.0,
        }
    }
}

// ─── State ─────────────────────────────────────────────────────────────────────

struct CompositeState {
    agg: CandleAggregator,
    ema_fast: Ema,
    ema_slow: Ema,
    rsi: Rsi,
    tick_trend: TickTrend,
}

// ─── Strategy ──────────────────────────────────────────────────────────────────

/// Multi-signal composite strategy combining:
/// - **Window-delta** (spot vs. strike displacement, tiered weight).
/// - **Micro-momentum** (last-2-candle body sum direction).
/// - **Acceleration** (latest candle body vs. 2 candles back).
/// - **EMA crossover** (9-period fast vs. 21-period slow).
/// - **RSI extremes** (overbought >75 → bearish; oversold <25 → bullish).
/// - **Tick-trend** (rolling directional consistency over raw price ticks).
///
/// # Confidence interpretation
/// `confidence = min(|score| / score_norm, 1.0)` is a **directional-strength
/// magnitude**, NOT a calibrated probability like `QuantStrategy`'s `p_model`.
/// It is used only for the entry gate (`min_confidence`) and is passed through
/// to `StrategyDecision::Enter` for logging. Do **not** feed it directly into a
/// Kelly-sizing formula that expects a calibrated probability.
pub struct CompositeStrategy {
    cfg: CompositeConfig,
    state: Mutex<CompositeState>,
}

impl CompositeStrategy {
    /// Build a `CompositeStrategy` with the given configuration.
    pub fn new(cfg: CompositeConfig) -> Self {
        let state = CompositeState {
            agg: CandleAggregator::new(cfg.candle_secs, cfg.candle_history),
            ema_fast: Ema::new(9),
            ema_slow: Ema::new(21),
            rsi: Rsi::new(14),
            tick_trend: TickTrend::new(cfg.tick_trend_window),
        };
        Self {
            cfg,
            state: Mutex::new(state),
        }
    }
}

impl Strategy for CompositeStrategy {
    fn evaluate(&self, ctx: &StrategyContext) -> StrategyDecision {
        // ── 1. Convert price types ────────────────────────────────────────────
        let spot = match ctx.price.0.to_f64() {
            Some(v) if v > 0.0 => v,
            _ => return StrategyDecision::Hold,
        };
        let strike = match ctx.strike.0.to_f64() {
            Some(v) if v > 0.0 => v,
            _ => return StrategyDecision::Hold,
        };

        // ── 2. Update indicators (always — keep warm outside the window too) ──
        let secs_left = ctx.secs_to_cutoff();
        let candles = {
            let mut st = self.state.lock().expect("CompositeState lock poisoned");
            st.tick_trend.push(spot);
            if let Some(closed) = st.agg.on_tick(spot, ctx.now.as_secs()) {
                st.ema_fast.update(closed.close);
                st.ema_slow.update(closed.close);
                st.rsi.update(closed.close);
            }
            // Clone the closed-candle deque for signal computation below.
            st.agg.closed().clone()
        };

        // ── 3. Time gate ──────────────────────────────────────────────────────
        if secs_left > self.cfg.entry_window_secs || secs_left <= self.cfg.danger_zone_secs {
            return StrategyDecision::Hold;
        }

        // ── 4. Compute signed score ───────────────────────────────────────────
        let w = &self.cfg.weights;

        // Window delta contribution.
        let wd_pct = (spot - strike) / strike * 100.0;
        let wd_abs = wd_pct.abs();
        let wd_tier_w = if wd_abs > w.wd_t1_pct {
            w.wd_t1_w
        } else if wd_abs > w.wd_t2_pct {
            w.wd_t2_w
        } else if wd_abs > w.wd_t3_pct {
            w.wd_t3_w
        } else if wd_abs > w.wd_t4_pct {
            w.wd_t4_w
        } else {
            0.0
        };
        let wd_sign = if wd_pct > 0.0 {
            1.0_f64
        } else if wd_pct < 0.0 {
            -1.0_f64
        } else {
            0.0_f64
        };
        let wd_contribution = wd_sign * wd_tier_w;

        // Micro-momentum contribution.
        let mm_val = micro_momentum(&candles);
        let mm_contribution = mm_val * w.micro_momentum;

        // Acceleration contribution — signed by the acceleration() return value,
        // which already encodes direction (positive when accelerating up, negative
        // when accelerating down or reversing from up).
        let acc_val = acceleration(&candles);
        let acc_contribution = acc_val * w.acceleration;

        // EMA crossover contribution (0 if not yet warm).
        let ema_cross_val = {
            let st = self.state.lock().expect("CompositeState lock poisoned");
            crossover_sign(&st.ema_fast, &st.ema_slow)
        };
        let ema_contribution = ema_cross_val * w.ema_cross;

        // RSI extreme contribution (overbought → bearish; oversold → bullish).
        let rsi_contribution = {
            let st = self.state.lock().expect("CompositeState lock poisoned");
            match st.rsi.value() {
                Some(rsi_val) if st.rsi.warm() => {
                    if rsi_val > 75.0 {
                        -w.rsi // overbought → expect downward reversal
                    } else if rsi_val < 25.0 {
                        w.rsi // oversold → expect upward reversal
                    } else {
                        0.0
                    }
                }
                _ => 0.0,
            }
        };

        // Tick-trend contribution.
        let tt_val = {
            let st = self.state.lock().expect("CompositeState lock poisoned");
            st.tick_trend.signal()
        };
        let tt_contribution = tt_val * w.tick_trend;

        let score = wd_contribution
            + mm_contribution
            + acc_contribution
            + ema_contribution
            + rsi_contribution
            + tt_contribution;

        // ── 5. Normalise and log ──────────────────────────────────────────────
        let confidence = (score.abs() / self.cfg.score_norm).min(1.0);

        tracing::debug!(
            score,
            confidence,
            wd_pct,
            wd_contribution,
            mm_contribution,
            acc_contribution,
            ema_contribution,
            rsi_contribution,
            tt_contribution,
            secs_left,
            "composite_signal"
        );

        // ── 6. Entry gate ─────────────────────────────────────────────────────
        if confidence < self.cfg.min_confidence {
            return StrategyDecision::Hold;
        }

        // ── 7. Direction ──────────────────────────────────────────────────────
        let outcome = if score > 0.0 {
            Outcome::Up
        } else {
            Outcome::Down
        };
        StrategyDecision::Enter {
            outcome,
            confidence: Some(confidence),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::hex::FromHex;
    use alloy::primitives::FixedBytes;
    use pm_core::domain::{Market, MarketOutcome};
    use pm_core::types::{
        MarketSlug, MarketStatus, MarketType, Price, Shares, Timestamp, TokenId,
    };
    use polymarket_client_sdk_v2::types::U256;
    use rust_decimal::Decimal;
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
            resolved_outcome: None,
            order_price_min_tick_size: Price(dec!(0.01)),
            order_min_size: Shares(dec!(5)),
        }
    }

    fn make_ctx(
        price: Decimal,
        strike: Decimal,
        secs_before_close: i64,
        market: &Market,
    ) -> StrategyContext<'_> {
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
        let s = CompositeStrategy::new(CompositeConfig::default());
        let market = dummy_market();
        // 300 s before close — outside the 180 s entry window.
        let ctx = make_ctx(dec!(65100), dec!(65000), 300, &market);
        assert_eq!(s.evaluate(&ctx), StrategyDecision::Hold);
    }

    #[test]
    fn holds_inside_danger_zone() {
        let s = CompositeStrategy::new(CompositeConfig::default());
        let market = dummy_market();
        // 15 s before close — inside the 30 s danger zone.
        let ctx = make_ctx(dec!(65100), dec!(65000), 15, &market);
        assert_eq!(s.evaluate(&ctx), StrategyDecision::Hold);
    }

    #[test]
    fn enters_up_on_decisive_window_delta_above() {
        let s = CompositeStrategy::new(CompositeConfig::default());
        let market = dummy_market();
        // spot 65100, strike 65000 → wd_pct ≈ +0.154% → tier-1 weight 7.
        // score = 7.0; confidence = 7.0/7.0 = 1.0 ≥ 0.40 → Enter Up.
        let ctx = make_ctx(dec!(65100), dec!(65000), 60, &market);
        match s.evaluate(&ctx) {
            StrategyDecision::Enter { outcome, confidence } => {
                assert_eq!(outcome, Outcome::Up);
                assert!(confidence.is_some());
                let c = confidence.unwrap();
                assert!(c >= 0.40, "confidence={c}");
            }
            StrategyDecision::Hold => panic!("expected Enter(Up), got Hold"),
        }
    }

    #[test]
    fn enters_down_on_decisive_window_delta_below() {
        let s = CompositeStrategy::new(CompositeConfig::default());
        let market = dummy_market();
        // spot 64900, strike 65000 → wd_pct ≈ -0.154% → tier-1 weight 7, negative.
        let ctx = make_ctx(dec!(64900), dec!(65000), 60, &market);
        match s.evaluate(&ctx) {
            StrategyDecision::Enter { outcome, confidence } => {
                assert_eq!(outcome, Outcome::Down);
                assert!(confidence.is_some());
                let c = confidence.unwrap();
                assert!(c >= 0.40, "confidence={c}");
            }
            StrategyDecision::Hold => panic!("expected Enter(Down), got Hold"),
        }
    }

    #[test]
    fn holds_on_near_strike_tiny_delta() {
        let s = CompositeStrategy::new(CompositeConfig::default());
        let market = dummy_market();
        // spot 65000.2, strike 65000 → wd_pct ≈ 0.0003% → below all tiers.
        // score ≈ 0 (cold indicators) → confidence < 0.40 → Hold.
        let ctx = make_ctx(dec!(65000.2), dec!(65000), 60, &market);
        assert_eq!(s.evaluate(&ctx), StrategyDecision::Hold);
    }
}

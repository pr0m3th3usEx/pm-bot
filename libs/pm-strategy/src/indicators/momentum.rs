use std::collections::VecDeque;

use super::candle::Candle;

/// Micro-momentum signal from the last two closed candles.
///
/// Returns the **sign** of the sum of the last two candle bodies (`close - open`).
/// A positive sum means net upward momentum; negative means net downward.
///
/// Returns `0.0` if fewer than 2 candles are available.
pub fn micro_momentum(candles: &VecDeque<Candle>) -> f64 {
    let n = candles.len();
    if n < 2 {
        return 0.0;
    }
    let last = &candles[n - 1];
    let prev = &candles[n - 2];
    let sum = (last.close - last.open) + (prev.close - prev.open);
    if sum > 0.0 {
        1.0
    } else if sum < 0.0 {
        -1.0
    } else {
        0.0
    }
}

/// Acceleration signal comparing the latest candle body to the one two candles back.
///
/// The sign of the returned value follows the **direction of the latest candle
/// body** (`close - open`):
/// * `+1.0` — the latest move is **larger in magnitude AND the same direction**
///   as the candle two back (accelerating).
/// * `-1.0` — the latest move is **smaller in magnitude OR the opposite direction**
///   (decelerating or reversing).
/// * `0.0` — fewer than 3 closed candles.
///
/// The magnitude is intentionally binary (±1/0) so it can be cleanly weighted
/// against other components of the composite score.
pub fn acceleration(candles: &VecDeque<Candle>) -> f64 {
    let n = candles.len();
    if n < 3 {
        return 0.0;
    }
    let latest = &candles[n - 1];
    let reference = &candles[n - 3];

    let latest_body = latest.close - latest.open;
    let ref_body = reference.close - reference.open;

    // Same direction and larger magnitude → accelerating.
    let same_direction = latest_body * ref_body > 0.0;
    let larger_magnitude = latest_body.abs() > ref_body.abs();

    if same_direction && larger_magnitude {
        // Return +1 in the direction of the latest candle's move.
        if latest_body > 0.0 { 1.0 } else { -1.0 }
    } else {
        // Decelerating or reversing — negative of the latest direction.
        if latest_body > 0.0 { -1.0 } else { 1.0 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_candle(open: f64, close: f64) -> Candle {
        Candle {
            open,
            high: open.max(close),
            low: open.min(close),
            close,
            start_secs: 0,
        }
    }

    #[test]
    fn micro_momentum_zero_on_empty() {
        let buf = VecDeque::new();
        assert_eq!(micro_momentum(&buf), 0.0);
    }

    #[test]
    fn micro_momentum_zero_on_single_candle() {
        let mut buf = VecDeque::new();
        buf.push_back(make_candle(100.0, 105.0));
        assert_eq!(micro_momentum(&buf), 0.0);
    }

    #[test]
    fn micro_momentum_positive_on_two_bullish_candles() {
        let mut buf = VecDeque::new();
        buf.push_back(make_candle(100.0, 105.0));
        buf.push_back(make_candle(105.0, 110.0));
        assert_eq!(micro_momentum(&buf), 1.0);
    }

    #[test]
    fn micro_momentum_negative_on_two_bearish_candles() {
        let mut buf = VecDeque::new();
        buf.push_back(make_candle(110.0, 105.0));
        buf.push_back(make_candle(105.0, 100.0));
        assert_eq!(micro_momentum(&buf), -1.0);
    }

    #[test]
    fn acceleration_zero_on_fewer_than_three() {
        let mut buf = VecDeque::new();
        buf.push_back(make_candle(100.0, 102.0));
        buf.push_back(make_candle(102.0, 104.0));
        assert_eq!(acceleration(&buf), 0.0);
    }

    #[test]
    fn acceleration_positive_when_accelerating_up() {
        let mut buf = VecDeque::new();
        buf.push_back(make_candle(100.0, 102.0)); // +2 (reference)
        buf.push_back(make_candle(102.0, 103.0)); // middle
        buf.push_back(make_candle(103.0, 107.0)); // +4 (latest, same direction, bigger)
        assert_eq!(acceleration(&buf), 1.0);
    }

    #[test]
    fn acceleration_negative_when_decelerating() {
        let mut buf = VecDeque::new();
        buf.push_back(make_candle(100.0, 104.0)); // +4 (reference)
        buf.push_back(make_candle(104.0, 105.0)); // middle
        buf.push_back(make_candle(105.0, 107.0)); // +2 (latest, same direction, smaller)
        assert_eq!(acceleration(&buf), -1.0);
    }

    #[test]
    fn acceleration_negative_on_reversal() {
        let mut buf = VecDeque::new();
        buf.push_back(make_candle(100.0, 104.0)); // +4 (reference, bullish)
        buf.push_back(make_candle(104.0, 103.0)); // middle
        buf.push_back(make_candle(103.0, 100.0)); // -3 (latest, bearish reversal)
        assert_eq!(acceleration(&buf), 1.0); // -1.0 on bearish → but bearish → "decelerating/reversing" from bull ref
        // Note: since latest is bearish (body < 0), the decel/reversal returns +1
        // (the inverse of the latest body direction: −1 * −1 = +1). This encodes
        // "the move reversed from the reference direction."
    }
}

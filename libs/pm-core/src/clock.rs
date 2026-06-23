use crate::types::{MarketSlug, Timestamp};

/// Pure clock math for the 5-minute BTC Up/Down rotation.
/// All methods are deterministic given wall-clock seconds.
pub struct MarketClock {
    /// Window size in seconds (300 for 5 m).
    pub interval_secs: u64,
    /// Market type prefix embedded in the slug.
    pub slug_prefix: String,
}

impl MarketClock {
    pub fn btc_5m() -> Self {
        Self {
            interval_secs: 300,
            slug_prefix: "btc-updown-5m".to_owned(),
        }
    }

    /// The aligned start timestamp (unix seconds) of the window containing `now_secs`.
    pub fn current_window_start(&self, now_secs: u64) -> u64 {
        (now_secs / self.interval_secs) * self.interval_secs
    }

    /// The aligned start timestamp of the **next** window after `now_secs`.
    pub fn next_window_start(&self, now_secs: u64) -> u64 {
        self.current_window_start(now_secs) + self.interval_secs
    }

    /// The `Timestamp` (ms) at which the next window begins.
    pub fn next_window_ts(&self, now_secs: u64) -> Timestamp {
        Timestamp::from_secs(self.next_window_start(now_secs) as i64)
    }

    /// Derive the market slug for a given window-start (unix seconds).
    // TODO(confirm): verify the exact {timestamp} format — unix seconds at
    // window-start, aligned to 300-second boundary, UTC? Confirm against a live
    // slug from Gamma before shipping.
    pub fn slug_for(&self, window_start_secs: u64) -> MarketSlug {
        MarketSlug(format!("{}-{}", self.slug_prefix, window_start_secs))
    }

    /// Slug for the current (live) window.
    pub fn current_slug(&self, now_secs: u64) -> MarketSlug {
        self.slug_for(self.current_window_start(now_secs))
    }

    /// Slug for the upcoming window.
    pub fn next_slug(&self, now_secs: u64) -> MarketSlug {
        self.slug_for(self.next_window_start(now_secs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn clock() -> MarketClock { MarketClock::btc_5m() }

    #[test]
    fn current_window_start_aligns() {
        let c = clock();
        // 2026-01-01 00:07:30 UTC = 1735689650 (arbitrary)
        // aligned window = 00:05:00 = 1735689600
        let now = 1735689650u64;
        assert_eq!(c.current_window_start(now), 1735689600);
    }

    #[test]
    fn next_window_start_is_plus_interval() {
        let c = clock();
        let now = 1735689650u64;
        let cur = c.current_window_start(now);
        assert_eq!(c.next_window_start(now), cur + 300);
    }

    #[test]
    fn slug_format() {
        let c = clock();
        let slug = c.slug_for(1735689600);
        assert_eq!(slug.0, "btc-updown-5m-1735689600");
    }

    #[test]
    fn window_at_exact_boundary_is_current() {
        let c = clock();
        let boundary = 1735689600u64;
        assert_eq!(c.current_window_start(boundary), boundary);
    }

    #[test]
    fn current_and_next_are_adjacent() {
        let c = clock();
        let now = 1735689750u64;
        let cur = c.current_slug(now);
        let nxt = c.next_slug(now);
        // extract timestamps
        let cur_ts: u64 = cur.0.split('-').last().unwrap().parse().unwrap();
        let nxt_ts: u64 = nxt.0.split('-').last().unwrap().parse().unwrap();
        assert_eq!(nxt_ts - cur_ts, 300);
    }
}

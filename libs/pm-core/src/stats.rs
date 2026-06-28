//! Win/loss reporting derived from settled positions in the store.

use tracing::info;

/// Settled-position outcome snapshot pulled from the store.
///
/// Built from `Store::success_rate_counts() -> (wins, resolved)`, where a
/// position counts as a win once it is `Won` *or* `Redeemed` (a redeemed
/// position is a past winner). `resolved` is wins + losses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WinLossStats {
    pub wins: u64,
    pub losses: u64,
    pub resolved: u64,
}

impl WinLossStats {
    pub fn from_counts(wins: u64, resolved: u64) -> Self {
        Self {
            wins,
            losses: resolved.saturating_sub(wins),
            resolved,
        }
    }

    /// wins / resolved in `[0, 1]`; `None` when nothing has resolved yet.
    pub fn win_rate(&self) -> Option<f64> {
        (self.resolved > 0).then(|| self.wins as f64 / self.resolved as f64)
    }

    /// wins / losses; `None` when there are no losses (avoid div-by-zero).
    pub fn win_loss_ratio(&self) -> Option<f64> {
        (self.losses > 0).then(|| self.wins as f64 / self.losses as f64)
    }

    /// Structured `info!` summary tagged with a `context` (e.g. "after-settlement").
    pub fn log(&self, context: &str) {
        info!(
            context,
            wins = self.wins,
            losses = self.losses,
            resolved = self.resolved,
            win_rate_pct = self.win_rate().map(|r| r * 100.0),
            w_l_ratio = self.win_loss_ratio(),
            "win/loss summary"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_counts_derives_losses() {
        let s = WinLossStats::from_counts(3, 5);
        assert_eq!(s.wins, 3);
        assert_eq!(s.losses, 2);
        assert_eq!(s.resolved, 5);
    }

    #[test]
    fn win_rate_and_ratio() {
        let s = WinLossStats::from_counts(3, 5);
        assert_eq!(s.win_rate(), Some(0.6));
        assert_eq!(s.win_loss_ratio(), Some(1.5));
    }

    #[test]
    fn no_resolved_yields_none() {
        let s = WinLossStats::from_counts(0, 0);
        assert_eq!(s.win_rate(), None);
        assert_eq!(s.win_loss_ratio(), None);
    }

    #[test]
    fn no_losses_yields_none_ratio() {
        let s = WinLossStats::from_counts(4, 4);
        assert_eq!(s.losses, 0);
        assert_eq!(s.win_rate(), Some(1.0));
        assert_eq!(s.win_loss_ratio(), None);
    }
}

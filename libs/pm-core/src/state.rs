use rust_decimal_macros::dec;

use crate::error::{CoreError, Result};
use crate::types::{MarketStatus, Usdc};

// ─── 1. Round slot (one-position-per-round invariant) ─────────────────────────

/// Tracks whether the current round's position slot is free, in-flight, or filled.
/// The executor is the sole writer; the decision center reads it for efficiency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoundSlotState {
    /// No position this round.
    Empty,
    /// Order submitted, awaiting fill (retryable on reject/cancel).
    Pending { position_id: i64 },
    /// Order filled; slot locked until next rotation.
    Filled { position_id: i64 },
}

impl RoundSlotState {
    /// Claim the slot at submission time. Errors if already claimed.
    pub fn claim(self, position_id: i64) -> Result<Self> {
        match self {
            Self::Empty => Ok(Self::Pending { position_id }),
            other => Err(CoreError::IllegalTransition(format!(
                "cannot claim from {other:?}"
            ))),
        }
    }

    /// Transition Pending → Filled on order fill.
    pub fn fill(self) -> Result<Self> {
        match self {
            Self::Pending { position_id } => Ok(Self::Filled { position_id }),
            other => Err(CoreError::IllegalTransition(format!(
                "cannot fill from {other:?}"
            ))),
        }
    }

    /// Free the slot on reject or cancel (Pending → Empty).
    pub fn free(self) -> Result<Self> {
        match self {
            Self::Pending { .. } => Ok(Self::Empty),
            other => Err(CoreError::IllegalTransition(format!(
                "cannot free from {other:?}"
            ))),
        }
    }

    /// Rotate: new market round always resets to Empty, regardless of current state.
    pub fn rotate(self) -> Self {
        Self::Empty
    }

    pub fn is_empty(self) -> bool {
        matches!(self, Self::Empty)
    }
}

// ─── 2. Market round status ───────────────────────────────────────────────────

/// Drives the market rotation task's view of one 5-minute window.
/// Open/TradingCutoff are clock-derived; Resolving/Resolved are poll-confirmed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketState(pub MarketStatus);

impl MarketState {
    pub fn new() -> Self {
        Self(MarketStatus::Pending)
    }

    pub fn transition(self, next: MarketStatus) -> Result<Self> {
        use MarketStatus::*;
        let valid = matches!(
            (self.0, next),
            (Pending, Open)
                | (Open, TradingCutoff)
                | (TradingCutoff, Resolving)
                | (Resolving, Resolved)
                // Allow Pending → Resolved for pre-resolved markets (edge case)
                | (Pending, Resolved)
        );
        if valid {
            Ok(Self(next))
        } else {
            Err(CoreError::IllegalTransition(format!(
                "MarketStatus {:?} → {:?} not allowed",
                self.0, next
            )))
        }
    }

    pub fn status(self) -> MarketStatus {
        self.0
    }
}

impl Default for MarketState {
    fn default() -> Self {
        Self::new()
    }
}

// ─── 3. Position lifecycle ────────────────────────────────────────────────────

use crate::types::PositionStatus;

/// Wraps PositionStatus with guarded transitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PositionState(pub PositionStatus);

impl PositionState {
    pub fn new() -> Self {
        Self(PositionStatus::Submitted)
    }

    pub fn transition(self, next: PositionStatus) -> Result<Self> {
        use PositionStatus::*;
        let valid = matches!(
            (self.0, next),
            (Submitted, Filled)
                | (Submitted, Rejected)
                | (Submitted, Cancelled)
                | (Filled, Settling)
                | (Settling, Won)
                | (Settling, Lost)
        );
        if valid {
            Ok(Self(next))
        } else {
            Err(CoreError::IllegalTransition(format!(
                "PositionStatus {:?} → {:?} not allowed",
                self.0, next
            )))
        }
    }
}

impl Default for PositionState {
    fn default() -> Self {
        Self::new()
    }
}

/// 4. Bankroll state: Tracks the bot's current bankroll (pUSD), current money-in-play, and about to be-redeemed pUSD. This is used to compute the Kelly fraction for sizing new positions.
/// The bankroll is updated on every position fill, settlement, and redemption.
/// The money-in-play is updated on every position fill and settlement. The about-to-be-redeemed pUSD is updated on every position settlement and redemption.
pub struct BankrollState {
    pub bankroll: Usdc,
    pub money_in_play: Usdc,
    pub about_to_be_redeemed: Usdc,
}

impl BankrollState {
    pub fn new(bankroll: Usdc) -> Self {
        Self {
            bankroll,
            money_in_play: Usdc(dec!(0)),
            about_to_be_redeemed: Usdc(dec!(0)),
        }
    }

    pub fn update_on_fill(&mut self, cost: Usdc) {
        self.money_in_play.0 += cost.0;
        self.bankroll.0 -= cost.0;
    }

    pub fn update_on_settlement(&mut self, realized_pnl: Usdc) {
        self.money_in_play.0 -= realized_pnl.0;
        self.about_to_be_redeemed.0 += realized_pnl.0;
    }

    pub fn update_on_redemption(&mut self, payout: Usdc) {
        self.bankroll.0 += payout.0;
        self.about_to_be_redeemed.0 -= payout.0;
    }
}

impl Default for BankrollState {
    fn default() -> Self {
        Self::new(Usdc(dec!(0)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use MarketStatus::*;
    use PositionStatus::*;

    #[test]
    fn slot_happy_path() {
        let s = RoundSlotState::Empty;
        let s = s.claim(42).unwrap();
        assert_eq!(s, RoundSlotState::Pending { position_id: 42 });
        let s = s.fill().unwrap();
        assert_eq!(s, RoundSlotState::Filled { position_id: 42 });
        assert_eq!(s.rotate(), RoundSlotState::Empty);
    }

    #[test]
    fn slot_reject_frees() {
        let s = RoundSlotState::Empty.claim(1).unwrap();
        let s = s.free().unwrap();
        assert_eq!(s, RoundSlotState::Empty);
    }

    #[test]
    fn slot_double_claim_errors() {
        let s = RoundSlotState::Empty.claim(1).unwrap();
        assert!(s.claim(2).is_err());
    }

    #[test]
    fn market_state_transitions() {
        let s = MarketState::new();
        let s = s.transition(Open).unwrap();
        let s = s.transition(TradingCutoff).unwrap();
        let s = s.transition(Resolving).unwrap();
        let s = s.transition(Resolved).unwrap();
        assert_eq!(s.status(), Resolved);
    }

    #[test]
    fn market_state_illegal_transition() {
        // Open → Pending is genuinely illegal (Pending → Resolved is an allowed edge case).
        let s = MarketState(Open);
        assert!(s.transition(Pending).is_err());
    }

    #[test]
    fn position_state_transitions() {
        let s = PositionState::new();
        let s = s.transition(Filled).unwrap();
        let s = s.transition(Settling).unwrap();
        let s = s.transition(Won).unwrap();
        assert!(s.0.is_resolved());
    }

    #[test]
    fn position_illegal_transition() {
        let s = PositionState::new(); // Submitted
        assert!(s.transition(Won).is_err());
    }

    #[test]
    fn bankroll_updates() {
        let mut b = BankrollState::new(Usdc(dec!(100)));
        b.update_on_fill(Usdc(dec!(10)));
        assert_eq!(b.bankroll, Usdc(dec!(90)));
        assert_eq!(b.money_in_play, Usdc(dec!(10)));
        b.update_on_settlement(Usdc(dec!(5)));
        assert_eq!(b.money_in_play, Usdc(dec!(5)));
        assert_eq!(b.about_to_be_redeemed, Usdc(dec!(5)));
        b.update_on_redemption(Usdc(dec!(5)));
        assert_eq!(b.bankroll, Usdc(dec!(95)));
        assert_eq!(b.about_to_be_redeemed, Usdc(dec!(0)));
    }
}

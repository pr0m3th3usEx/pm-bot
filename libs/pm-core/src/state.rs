use rust_decimal_macros::dec;

use crate::domain::OutcomeBook;
use crate::error::{CoreError, Result};
use crate::types::{MarketStatus, Price, Side, TokenId, Usdc};

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
                | (Won, Redeemed)
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

/// Bankroll state: tracks the bot's current bankroll (pUSD), current money-in-play,
/// and about-to-be-redeemed pUSD.
///
/// Used to compute the Kelly fraction for sizing new positions. Updated on every
/// position fill, settlement, and redemption.
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

    pub fn update_on_settlement(&mut self, cost: Usdc, realized_pnl: Usdc) {
        self.money_in_play.0 -= cost.0;
        self.about_to_be_redeemed.0 += cost.0 + realized_pnl.0;
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

// ─── 5. Outcome-book cache ────────────────────────────────────────────────────

/// In-memory cache of the latest `OutcomeBook` for each `TokenId` in the current round.
/// Written by `market_data_task`; read by `decision_center_task`.
#[derive(Default)]
pub struct OutcomeBookCache {
    books: std::collections::HashMap<TokenId, OutcomeBook>,
}

impl OutcomeBookCache {
    pub fn update(&mut self, book: OutcomeBook) {
        self.books.insert(book.token_id.clone(), book);
    }

    /// Mirrors `MarketClient::quote`: Buy → buy_price (ask), Sell → sell_price (bid).
    pub fn price(&self, token_id: &TokenId, side: Side) -> Option<Price> {
        let b = self.books.get(token_id)?;
        match side {
            Side::Buy => b.buy_price.clone(),
            Side::Sell => b.sell_price.clone(),
        }
    }

    pub fn clear(&mut self) {
        self.books.clear();
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
    fn outcome_book_cache_buy_sell_mapping() {
        use polymarket_client_sdk_v2::types::U256;
        use rust_decimal::Decimal;
        use std::str::FromStr;

        let token_id = TokenId(U256::from(42u64));

        let buy_price = Price(Decimal::from_str("0.55").unwrap());
        let sell_price = Price(Decimal::from_str("0.48").unwrap());

        let book = OutcomeBook {
            token_id: token_id.clone(),
            buy_price: Some(buy_price.clone()),
            sell_price: Some(sell_price.clone()),
            at: crate::types::Timestamp(0),
        };

        let mut cache = OutcomeBookCache::default();
        cache.update(book);

        assert_eq!(cache.price(&token_id, Side::Buy), Some(buy_price));
        assert_eq!(cache.price(&token_id, Side::Sell), Some(sell_price));

        // Unknown token → None
        let other_token = TokenId(U256::from(99u64));
        assert_eq!(cache.price(&other_token, Side::Buy), None);

        // After clear, lookups return None
        cache.clear();
        assert_eq!(cache.price(&token_id, Side::Buy), None);
        assert_eq!(cache.price(&token_id, Side::Sell), None);
    }

    #[test]
    fn bankroll_updates() {
        // Win case: cost=10, pnl=+5 => money_in_play goes to 0, about_to_be_redeemed=15
        let mut b = BankrollState::new(Usdc(dec!(100)));
        b.update_on_fill(Usdc(dec!(10)));
        assert_eq!(b.bankroll, Usdc(dec!(90)));
        assert_eq!(b.money_in_play, Usdc(dec!(10)));
        b.update_on_settlement(Usdc(dec!(10)), Usdc(dec!(5)));
        assert_eq!(b.money_in_play, Usdc(dec!(0)));
        assert_eq!(b.about_to_be_redeemed, Usdc(dec!(15)));
        b.update_on_redemption(Usdc(dec!(15)));
        assert_eq!(b.bankroll, Usdc(dec!(105)));
        assert_eq!(b.about_to_be_redeemed, Usdc(dec!(0)));

        // Loss case: cost=10, pnl=-10 => money_in_play 0, about_to_be_redeemed 0
        let mut b2 = BankrollState::new(Usdc(dec!(100)));
        b2.update_on_fill(Usdc(dec!(10)));
        b2.update_on_settlement(Usdc(dec!(10)), Usdc(dec!(-10)));
        assert_eq!(b2.money_in_play, Usdc(dec!(0)));
        assert_eq!(b2.about_to_be_redeemed, Usdc(dec!(0)));
    }
}

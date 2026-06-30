use crate::domain::{BookSnapshot, Intent, Market, OutcomeBook, PositionRecord, PositionUpdate, RedeemReceipt, Tick};
use crate::error::Result;
use crate::types::{MarketSlug, Price, Shares, Side, TokenId, Usdc};
use async_trait::async_trait;

/// Status of an in-flight relayer redemption transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedemptionStatus {
    Pending,
    Confirmed,
    Failed,
}

// ─── Price feed ──────────────────────────────────────────────────────────────

/// Source of BTC price ticks. V1: Binance WS. Swap source without touching strategy.
#[async_trait]
pub trait PriceFeed: Send + Sync {
    /// Block until the next tick is available.
    async fn next_tick(&mut self) -> Result<Tick>;
}

// ─── Order-book feed ─────────────────────────────────────────────────────────

/// Source of top-of-book snapshots. One implementation per venue.
#[async_trait]
pub trait OrderBookFeed: Send + Sync {
    /// Block until the next top-of-book snapshot is available.
    async fn next_book(&mut self) -> Result<BookSnapshot>;
}

// ─── Market data feed ────────────────────────────────────────────────────────

/// Source of real-time outcome-price (top-of-book) updates for a fixed set of
/// Polymarket asset ids. One instance per round's subscription set.
#[async_trait]
pub trait MarketDataFeed: Send + Sync {
    /// Block until the next outcome-price update is available.
    async fn next_update(&mut self) -> Result<OutcomeBook>;
}

// ─── Market catalog ───────────────────────────────────────────────────────────

/// Fetches market metadata from Gamma. Separate from MarketClient (metadata vs orders).
#[async_trait]
pub trait MarketCatalog: Send + Sync {
    async fn resolve(&self, slug: &MarketSlug) -> Result<Market>;
}

// ─── Market client (CLOB) ────────────────────────────────────────────────────

/// The CLOB surface: quotes, orders, status, redemption, keepalive.
#[async_trait]
pub trait MarketClient: Send + Sync {
    /// Best bid (BUY) or best ask (SELL) price for the given token.
    async fn quote(&self, token_id: &TokenId, side: Side) -> Result<Price>;

    /// Submit a limit order. Returns the CLOB order ID.
    async fn place_order(&self, intent: &Intent, token_id: &TokenId) -> Result<String>;

    /// Cancel a resting order.
    async fn cancel_order(&self, order_id: &str) -> Result<()>;

    /// Fetch current order state (used by the order-status poller).
    /// `position_id` is our internal DB row ID — the CLOB doesn't know it, but
    /// the poller does (it comes from the store query), so we thread it here.
    async fn order_status(
        &self,
        order_id: &str,
        position_id: i64,
    ) -> Result<crate::domain::OrderUpdate>;

    /// Redeem a winning position. Returns a receipt with optional transaction id and payout.
    async fn redeem(&self, position: &PositionRecord) -> Result<RedeemReceipt>;

    /// Check the on-chain status of a previously submitted redemption transaction.
    async fn redemption_status(&self, transaction_id: &str) -> Result<RedemptionStatus>;

    /// Return the current spendable USDC balance of the Safe wallet.
    async fn balance(&self) -> Result<Usdc>;

    /// Heartbeat to keep the CLOB session alive. Returns the server timestamp.
    async fn heartbeat(&self) -> Result<()>;
}

// ─── Strategy ─────────────────────────────────────────────────────────────────

/// Evaluated on every Tick. Pure: no I/O, no mutable state.
#[async_trait]
pub trait Strategy: Send + Sync {
    fn evaluate(&self, ctx: &crate::strategy::StrategyContext)
        -> crate::strategy::StrategyDecision;
}

// ─── Sizing model ─────────────────────────────────────────────────────────────

/// Given an enter decision, bankroll, and limit price → share count.
pub trait SizingModel: Send + Sync {
    /// V1: fixed. V2: Kelly / arithmetic.
    fn size(&self, bankroll: &Usdc, limit_price: &Price) -> Shares;
}

// ─── Entry policy ─────────────────────────────────────────────────────────────

/// One-position-per-round gate expressed as a policy object (not a hardcoded if).
pub trait EntryPolicy: Send + Sync {
    fn admit(&self, slot: &crate::state::RoundSlotState, intent: &Intent) -> Admission;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    Admit,
    Reject,
}

// ─── Market data recorder ─────────────────────────────────────────────────────

/// Sink for recording live market data (outcome books, price ticks, market metadata)
/// to durable storage. Used by the recording decorator adapters.
#[async_trait]
pub trait MarketDataRecorder: Send + Sync {
    /// Record an outcome-book snapshot.
    async fn record_outcome_book(
        &self,
        book: &crate::domain::OutcomeBook,
        session_id: &str,
    ) -> crate::error::Result<()>;

    /// Record a price tick.
    async fn record_tick(
        &self,
        tick: &crate::domain::Tick,
        session_id: &str,
    ) -> crate::error::Result<()>;

    /// Record market metadata.
    async fn record_market(
        &self,
        market: &crate::domain::Market,
        session_id: &str,
    ) -> crate::error::Result<()>;
}

// ─── Store ────────────────────────────────────────────────────────────────────

/// Durable audit + PnL storage. SQLite behind it; mockable.
#[async_trait]
pub trait Store: Send + Sync {
    /// Insert a new position attempt. Returns the assigned row ID.
    async fn insert_position(&self, record: &PositionRecord) -> Result<i64>;

    /// Apply a partial update to an existing row.
    async fn update_position(&self, id: i64, update: &PositionUpdate) -> Result<()>;

    /// All positions that have not yet reached a terminal status.
    async fn open_positions(&self) -> Result<Vec<PositionRecord>>;

    /// (wins, resolved) — used to compute success/win rate. A position counts as
    /// a win once it is `Won` *or* `Redeemed` (a redeemed position is a past
    /// winner). `resolved` = wins + losses = Won + Redeemed + Lost.
    async fn success_rate_counts(&self) -> Result<(u64, u64)>;
}

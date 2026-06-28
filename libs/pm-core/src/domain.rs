use crate::types::{
    MarketSlug, MarketStatus, MarketType, Outcome, PositionStatus, Price, Shares, Side, Timestamp,
    TokenId, Usdc,
};
use alloy::primitives::FixedBytes;
use serde::{Deserialize, Serialize};

/// A single tradeable outcome within a market.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketOutcome {
    pub name: String,      // e.g. "up", "down", "yes"
    pub token_id: TokenId, // opaque CLOB id, rotates every round
}

/// The fully-resolved instrument returned by MarketCatalog::resolve.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Market {
    pub slug: MarketSlug,
    pub market_type: MarketType,
    pub event_id: String,
    pub question_id: FixedBytes<32>,
    pub condition_id: FixedBytes<32>,
    pub outcomes: Vec<MarketOutcome>,
    /// Price to beat. For UpDown markets: the closePrice of the previous round
    /// from the Polymarket past-results API. None while still Pending.
    pub strike: Option<Price>,
    pub opens_at: Timestamp,
    /// Same as resolves_at — Polymarket closes the book at resolution time.
    pub closes_at: Timestamp,
    pub resolves_at: Timestamp,
    pub status: MarketStatus,
}

/// Alias: the market rotation task publishes Market on a watch channel.
/// Consumers read its `status` field to know if trading is open/cutoff/resolved.
pub type ActiveMarket = Market;

// ─── Channel messages ─────────────────────────────────────────────────────────

/// High-frequency BTC price tick from PriceFeed.
#[derive(Debug, Clone)]
pub struct Tick {
    pub price: Price,
    pub at: Timestamp,
}

/// Decision from the decision center → executor.
#[derive(Debug, Clone)]
pub struct Intent {
    pub outcome: Outcome,
    pub side: Side,
    pub shares: Shares,
    pub limit_price: Price,
}

/// Order lifecycle events from the CLOB / order-status poller.
#[derive(Debug, Clone)]
pub enum OrderUpdate {
    Submitted {
        order_id: String,
        position_id: i64,
    },
    Filled {
        order_id: String,
        position_id: i64,
        avg_price: Price,
        size_matched: Shares,
    },
    Rejected {
        order_id: String,
        position_id: i64,
        reason: Option<String>,
    },
    Cancelled {
        order_id: String,
        position_id: i64,
    },
}

/// Emitted by the settlement task after a position resolves.
#[derive(Debug, Clone)]
pub struct Settled {
    pub position_id: i64,
    pub status: PositionStatus,
    pub realized_pnl: Usdc,
    pub cost: Usdc,
}

/// Returned by `MarketClient::redeem` — carries the relayer transaction id (if any) and payout.
#[derive(Debug, Clone)]
pub struct RedeemReceipt {
    pub transaction_id: Option<String>,
    pub payout: Usdc,
}

/// Sent to the redeem_status_poller when a redeem has been submitted but not yet confirmed.
#[derive(Debug, Clone)]
pub struct PendingRedemption {
    pub position_id: i64,
    pub transaction_id: String,
    pub payout: Usdc,
}

/// Emitted by the redeem_status_poller once a redemption is confirmed on-chain.
#[derive(Debug, Clone)]
pub struct Redeemed {
    pub position_id: i64,
    pub payout: Usdc,
}

// ─── Persistence types ───────────────────────────────────────────────────────

/// One row in the `positions` table (grain: one attempt).
#[derive(Debug, Clone)]
pub struct PositionRecord {
    pub id: Option<i64>,
    pub market_slug: MarketSlug,
    pub side: Side,
    pub outcome_name: String, // free text — matches market vocabulary
    pub token_id: TokenId,
    pub condition_id: FixedBytes<32>,
    pub order_id: Option<String>,
    pub shares: Shares,
    pub limit_price: Price,
    pub avg_price: Option<Price>,
    pub strike: Option<Price>,
    pub status: PositionStatus,
    pub realized_pnl: Option<Usdc>,
    pub submitted_at: Timestamp,
    pub updated_at: Timestamp,
}

/// Partial update applied to an existing row.
#[derive(Debug, Clone)]
pub enum PositionUpdate {
    Submitted {
        order_id: String,
        updated_at: Timestamp,
    },
    Filled {
        avg_price: Price,
        size_matched: Shares,
        updated_at: Timestamp,
    },
    Rejected {
        updated_at: Timestamp,
    },
    Cancelled {
        updated_at: Timestamp,
    },
    Settling {
        updated_at: Timestamp,
    },
    Won {
        realized_pnl: Usdc,
        updated_at: Timestamp,
    },
    Lost {
        realized_pnl: Usdc,
        updated_at: Timestamp,
    },
    Redeemed {
        updated_at: Timestamp,
    },
}

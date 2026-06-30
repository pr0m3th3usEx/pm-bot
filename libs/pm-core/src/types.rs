use polymarket_client_sdk_v2::types::U256;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// Unix timestamp in milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Timestamp(pub i64);

impl Timestamp {
    pub fn now_ms() -> Self {
        let ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time before epoch")
            .as_millis() as i64;
        Self(ms)
    }
    pub fn as_secs(&self) -> i64 {
        self.0 / 1000
    }
    pub fn from_secs(s: i64) -> Self {
        Self(s * 1000)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Price(pub Decimal);
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Shares(pub Decimal);
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usdc(pub Decimal);
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TokenId(pub U256);
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MarketSlug(pub String);

impl std::fmt::Display for MarketSlug {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// CLOB order direction. Orthogonal to outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}
impl Side {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "buy",
            Self::Sell => "sell",
        }
    }
}

impl From<Side> for polymarket_client_sdk_v2::clob::types::Side {
    fn from(val: Side) -> Self {
        match val {
            Side::Buy => polymarket_client_sdk_v2::clob::types::Side::Buy,
            Side::Sell => polymarket_client_sdk_v2::clob::types::Side::Sell,
        }
    }
}

/// V1-scoped: BTC Up/Down vocabulary. Not stored directly — outcome_name is free TEXT.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    Up,
    Down,
}
impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Up => "Up",
            Self::Down => "Down",
        }
    }
}

/// Determines how a market's strike is sourced and how resolution is detected.
/// Detected from Gamma market tags at resolve time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MarketType {
    /// BTC/crypto Up/Down N-minute market (tag slug "up-or-down").
    /// Strike fetched from Polymarket past-results API using the previous round's closePrice.
    UpDown,
    /// All other market types. Strike not applicable; resolution detected via Gamma.
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MarketStatus {
    Pending,
    Open,
    TradingCutoff,
    Resolving,
    Resolved,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PositionStatus {
    Submitted,
    Filled,
    Settling,
    Won,
    Lost,
    Rejected,
    Cancelled,
    Redeemed,
}

impl PositionStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Submitted => "submitted",
            Self::Filled => "filled",
            Self::Settling => "settling",
            Self::Won => "won",
            Self::Lost => "lost",
            Self::Rejected => "rejected",
            Self::Cancelled => "cancelled",
            Self::Redeemed => "redeemed",
        }
    }

    /// True for Won/Lost — counts toward success rate.
    pub fn is_resolved(self) -> bool {
        matches!(self, Self::Won | Self::Lost)
    }

    /// True for Won/Lost/Rejected/Cancelled/Redeemed.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Won | Self::Lost | Self::Rejected | Self::Cancelled | Self::Redeemed
        )
    }
}

impl FromStr for PositionStatus {
    type Err = crate::error::CoreError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "submitted" => Ok(Self::Submitted),
            "filled" => Ok(Self::Filled),
            "settling" => Ok(Self::Settling),
            "won" => Ok(Self::Won),
            "lost" => Ok(Self::Lost),
            "rejected" => Ok(Self::Rejected),
            "cancelled" => Ok(Self::Cancelled),
            "redeemed" => Ok(Self::Redeemed),
            other => Err(crate::error::CoreError::UnknownVariant(other.to_owned())),
        }
    }
}

impl FromStr for Side {
    type Err = crate::error::CoreError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "buy" => Ok(Self::Buy),
            "sell" => Ok(Self::Sell),
            other => Err(crate::error::CoreError::UnknownVariant(other.to_owned())),
        }
    }
}

// ─── drift tests: Rust enums must match SQLite CHECK lists ───────────────────

/// The values that must appear in `CHECK(side IN (...))`  — edit both together.
pub const SIDE_CHECK_VALUES: &[&str] = &["buy", "sell"];
/// The values that must appear in `CHECK(status IN (...))` — edit both together.
pub const STATUS_CHECK_VALUES: &[&str] = &[
    "submitted",
    "filled",
    "settling",
    "won",
    "lost",
    "rejected",
    "cancelled",
    "redeemed",
];

#[cfg(test)]
mod drift_tests {
    use super::*;

    #[test]
    fn side_as_str_matches_check_constraint() {
        let variants = [Side::Buy, Side::Sell];
        for v in variants {
            assert!(
                SIDE_CHECK_VALUES.contains(&v.as_str()),
                "Side::{v:?} as_str '{}' missing from SIDE_CHECK_VALUES",
                v.as_str()
            );
        }
        assert_eq!(
            variants.len(),
            SIDE_CHECK_VALUES.len(),
            "SIDE_CHECK_VALUES length mismatch"
        );
    }

    #[test]
    fn position_status_as_str_matches_check_constraint() {
        let variants = [
            PositionStatus::Submitted,
            PositionStatus::Filled,
            PositionStatus::Settling,
            PositionStatus::Won,
            PositionStatus::Lost,
            PositionStatus::Rejected,
            PositionStatus::Cancelled,
            PositionStatus::Redeemed,
        ];
        for v in variants {
            assert!(
                STATUS_CHECK_VALUES.contains(&v.as_str()),
                "PositionStatus::{v:?} as_str '{}' missing from STATUS_CHECK_VALUES",
                v.as_str()
            );
        }
        assert_eq!(
            variants.len(),
            STATUS_CHECK_VALUES.len(),
            "STATUS_CHECK_VALUES length mismatch"
        );
    }
}

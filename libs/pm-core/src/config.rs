use crate::error::{CoreError, Result};
use crate::types::Usdc;
use rust_decimal::Decimal;
use std::str::FromStr;

// ─── Execution mode ───────────────────────────────────────────────────────────

/// Selects which `MarketClient` adapter to wire in `main.rs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExecutionMode {
    #[default]
    Live,
    DryRun,
}

impl FromStr for ExecutionMode {
    type Err = CoreError;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "live" => Ok(Self::Live),
            "dry-run" | "dryrun" | "dry_run" => Ok(Self::DryRun),
            other => Err(CoreError::UnknownVariant(format!(
                "unknown EXECUTION_MODE '{other}'; expected 'live' or 'dry-run'"
            ))),
        }
    }
}

impl ExecutionMode {
    /// Read `EXECUTION_MODE` env var; default `Live`.
    pub fn from_env() -> Result<Self> {
        match std::env::var("EXECUTION_MODE") {
            Ok(val) => val.parse(),
            Err(_) => Ok(Self::Live),
        }
    }
}

// ─── Strategy selection ───────────────────────────────────────────────────────

/// Selects which `Strategy` implementation `main.rs` wires in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StrategyKind {
    /// Composite weighted-signal strategy (current default).
    #[default]
    Composite,
    /// Quantitative binary-option pricing strategy.
    Quant,
    /// V1 heuristic (time-window + price−strike margin).
    V1Basic,
}

impl FromStr for StrategyKind {
    type Err = CoreError;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "composite" => Ok(Self::Composite),
            "quant" => Ok(Self::Quant),
            "v1-basic" | "v1basic" | "basic" => Ok(Self::V1Basic),
            other => Err(CoreError::UnknownVariant(format!(
                "unknown STRATEGY '{other}'; expected 'composite', 'quant', or 'v1-basic'"
            ))),
        }
    }
}

impl StrategyKind {
    /// Read `STRATEGY` env var; default `Composite`.
    pub fn from_env() -> Result<Self> {
        match std::env::var("STRATEGY") {
            Ok(val) => val.parse(),
            Err(_) => Ok(Self::Composite),
        }
    }
}

// ─── Sim config ───────────────────────────────────────────────────────────────

/// Configuration for `SimMarketClient` (dry-run mode).
/// All fields are env-driven with sensible defaults.
#[derive(Debug, Clone)]
pub struct SimConfig {
    /// Starting virtual USDC balance (default: 1000).
    pub virtual_bankroll: Usdc,
    /// Minimum ms that must elapse between order submission and first fill check (default: 300).
    pub fill_latency_ms: i64,
    /// Taker fee in basis points applied to filled notional (default: 0).
    pub taker_fee_bps: u32,
    /// If true, every resting order fills on the next status poll (debug escape hatch).
    pub always_fill: bool,
    /// Path to the SQLite file used in dry-run mode (default: "pm-bot.dryrun.sqlite").
    pub dryrun_db_path: String,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            virtual_bankroll: Usdc(Decimal::from(1000u32)),
            fill_latency_ms: 300,
            taker_fee_bps: 0,
            always_fill: false,
            dryrun_db_path: "pm-bot.dryrun.sqlite".to_owned(),
        }
    }
}

impl SimConfig {
    /// Build a `SimConfig` from environment variables, falling back to defaults.
    pub fn from_env() -> Self {
        let virtual_bankroll = std::env::var("SIM_VIRTUAL_BANKROLL")
            .ok()
            .and_then(|s| s.parse::<Decimal>().ok())
            .map(Usdc)
            .unwrap_or_else(|| Usdc(Decimal::from(1000u32)));

        let fill_latency_ms = std::env::var("SIM_FILL_LATENCY_MS")
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(300);

        let taker_fee_bps = std::env::var("SIM_TAKER_FEE_BPS")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(0);

        let always_fill = std::env::var("SIM_ALWAYS_FILL")
            .map(|s| matches!(s.as_str(), "1" | "true" | "yes"))
            .unwrap_or(false);

        let dryrun_db_path = std::env::var("SIM_DRYRUN_DB_PATH")
            .unwrap_or_else(|_| "pm-bot.dryrun.sqlite".to_owned());

        Self {
            virtual_bankroll,
            fill_latency_ms,
            taker_fee_bps,
            always_fill,
            dryrun_db_path,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strategy_kind_parses_known_values() {
        assert_eq!("composite".parse::<StrategyKind>().unwrap(), StrategyKind::Composite);
        assert_eq!("Quant".parse::<StrategyKind>().unwrap(), StrategyKind::Quant);
        assert_eq!("v1-basic".parse::<StrategyKind>().unwrap(), StrategyKind::V1Basic);
        assert_eq!("v1basic".parse::<StrategyKind>().unwrap(), StrategyKind::V1Basic);
    }

    #[test]
    fn strategy_kind_rejects_garbage() {
        assert!("nope".parse::<StrategyKind>().is_err());
    }

    #[test]
    fn strategy_kind_default_is_composite() {
        assert_eq!(StrategyKind::default(), StrategyKind::Composite);
    }
}

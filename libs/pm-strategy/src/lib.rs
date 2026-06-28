//! pm-strategy: concrete strategy and sizing-model implementations.
//!
//! The `Strategy` / `SizingModel` ports and the `StrategyContext` / `StrategyDecision`
//! contract types live in `pm-core` (so `pm-core` tasks can depend on them without a
//! cycle). This crate holds the concrete implementations that get wired in by the app.

pub mod model;
pub mod sizing;
pub mod strategy;

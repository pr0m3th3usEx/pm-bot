use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::domain::{
    ActiveMarket, MarketLifecycle, PendingRedemption, PositionUpdate, Settled, Tick,
};
use crate::format::{banner, signed_usd, usd};
use crate::ports::{MarketClient, Store};
use crate::types::{Outcome, PositionStatus, Price, Timestamp, Usdc};

const TICK_CACHE: usize = 50;

/// Newest cached tick at or before the cutoff — the price the market resolves on.
fn resolution_price(ticks: &VecDeque<Tick>, closes_at: Timestamp) -> Option<Price> {
    ticks
        .iter()
        .rfind(|t| t.at.0 <= closes_at.0)
        .map(|t| t.price.clone())
}

/// Determine the winning outcome NAME for a resolved market.
///
/// Prefers the authoritative `resolved_outcome` reported by the market catalog (Gamma), which is
/// what actually pays out. Falls back to a local `price > strike` decision only when the catalog
/// did not report a winner (e.g. a tie or parse failure) and a resolution price is available.
///
/// Compare the result against a position's `outcome_name` case-insensitively — the catalog and the
/// local fallback (`Outcome::as_str`) may not match the stored vocabulary's exact casing.
pub fn winning_outcome_name(market: &ActiveMarket, resolution_price: Option<&Price>) -> Option<String> {
    if let Some(outcome) = &market.resolved_outcome {
        return Some(outcome.clone());
    }
    // Fallback: local price>strike (mirrors V1BasicStrategy; price==strike → Down).
    let strike = market.strike.as_ref()?;
    let price = resolution_price?;
    let outcome = if price.0 > strike.0 {
        Outcome::Up
    } else {
        Outcome::Down
    };
    Some(outcome.as_str().to_string())
}

/// Settle every `Filled`/`Settling` position for `market`: redeem winners, mark losers `Lost`.
///
/// Reusable by both the live settlement task and the offline recovery binary, hence the optional
/// channels (the recovery tool has no live broadcast/mpsc consumers).
pub async fn settle_market_positions(
    client: &Arc<dyn MarketClient>,
    store: &Arc<dyn Store>,
    market: &ActiveMarket,
    resolution_price: Option<Price>,
    settled_tx: Option<&broadcast::Sender<Settled>>,
    pending_tx: Option<&mpsc::Sender<PendingRedemption>>,
) {
    // Authoritative winner: prefer catalog `resolved_outcome`, else local price>strike.
    let Some(winning_name) = winning_outcome_name(market, resolution_price.as_ref()) else {
        warn!(
            market_slug = %market.slug,
            has_strike = market.strike.is_some(),
            has_price = resolution_price.is_some(),
            "no resolved_outcome and no price fallback — cannot settle"
        );
        return;
    };
    let winner_source = if market.resolved_outcome.is_some() {
        "resolved_outcome"
    } else {
        "price_fallback"
    };
    info!(
        market_slug = %market.slug,
        winning = %winning_name,
        winner_source,
        resolution_price = resolution_price.as_ref().map(|p| p.0.to_string()),
        strike = market.strike.as_ref().map(|p| p.0.to_string()),
        "🏁 market resolved — settling positions"
    );

    let positions = match store.open_positions().await {
        Ok(p) => p,
        Err(e) => {
            error!(error = %e, "failed to fetch positions for settlement");
            return;
        }
    };

    for pos in positions.into_iter().filter(|p| {
        matches!(p.status, PositionStatus::Filled | PositionStatus::Settling)
            && p.market_slug == market.slug
    }) {
        let position_id = pos.id.expect("stored position must have id");
        // Case-insensitive: catalog / local-fallback casing may differ from stored vocabulary.
        let won = pos.outcome_name.eq_ignore_ascii_case(&winning_name);

        // Cost basis from the actual fill (fall back to limit price defensively).
        let entry = pos
            .avg_price
            .clone()
            .unwrap_or_else(|| pos.limit_price.clone());
        let cost = entry.0 * pos.shares.0;

        // Filled → Settling before any terminal write (crash-recoverable marker).
        if let Err(e) = store
            .update_position(
                position_id,
                &PositionUpdate::Settling {
                    updated_at: Timestamp::now_ms(),
                },
            )
            .await
        {
            error!(position_id, error = %e, "failed to mark Settling; skipping");
            continue;
        }

        if won {
            match client.redeem(&pos).await {
                Ok(receipt) => {
                    let pnl = receipt.payout.0 - cost; // net profit
                    info!(
                        position_id,
                        "\n{}",
                        banner(
                            "🟢 WON",
                            &[
                                ("market", market.slug.to_string()),
                                ("outcome", pos.outcome_name.clone()),
                                ("shares", pos.shares.0.to_string()),
                                ("cost → payout", format!("{} → {}", usd(cost), usd(receipt.payout.0))),
                                ("profit", signed_usd(pnl)),
                            ],
                        )
                    );
                    if let Some(settled_tx) = settled_tx {
                        let _ = settled_tx.send(Settled {
                            position_id,
                            status: PositionStatus::Won,
                            realized_pnl: Usdc(pnl),
                            cost: Usdc(cost),
                        });
                    }
                    if let Some(tx_id) = receipt.transaction_id {
                        if let Some(pending_tx) = pending_tx {
                            let _ = pending_tx
                                .send(PendingRedemption {
                                    position_id,
                                    transaction_id: tx_id,
                                    payout: receipt.payout,
                                })
                                .await;
                        }
                    } else {
                        warn!(
                            position_id,
                            "redeem returned no transaction_id; cannot confirm redemption"
                        );
                    }
                }
                Err(e) => {
                    error!(position_id, error = %e, "redeem failed; position left in Settling")
                }
            }
        } else {
            let pnl = -cost; // full loss
            info!(
                position_id,
                "\n{}",
                banner(
                    "🔴 LOST",
                    &[
                        ("market", market.slug.to_string()),
                        ("outcome", pos.outcome_name.clone()),
                        ("shares", pos.shares.0.to_string()),
                        ("cost", usd(cost)),
                        ("loss", signed_usd(pnl)),
                    ],
                )
            );
            if let Some(settled_tx) = settled_tx {
                let _ = settled_tx.send(Settled {
                    position_id,
                    status: PositionStatus::Lost,
                    realized_pnl: Usdc(pnl),
                    cost: Usdc(cost),
                });
            }
        }
    }
}

pub async fn settlement_task(
    client: Arc<dyn MarketClient>,
    store: Arc<dyn Store>,
    mut lifecycle_rx: mpsc::Receiver<MarketLifecycle>,
    mut tick_rx: broadcast::Receiver<Tick>,
    settled_tx: broadcast::Sender<Settled>,
    pending_tx: mpsc::Sender<PendingRedemption>,
    cancel: CancellationToken,
) {
    info!("settlement_task started");
    let mut ticks: VecDeque<Tick> = VecDeque::with_capacity(TICK_CACHE);
    // Resolution price snapshotted at TradingCutoff; consumed at Resolved.
    let mut cutoff_price: Option<Price> = None;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("settlement_task cancelled");
                break;
            }

            result = tick_rx.recv() => {
                match result {
                    Ok(t) => {
                        if ticks.len() == TICK_CACHE {
                            ticks.pop_front();
                        }
                        ticks.push_back(t);
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(skipped = n, "settlement tick lagged")
                    }
                    Err(broadcast::error::RecvError::Closed) => {}
                }
            }

            // Lossless mpsc — every TradingCutoff/Resolved is delivered exactly once, in order.
            ev = lifecycle_rx.recv() => {
                let Some(ev) = ev else { break }; // sender dropped → shut down
                match ev {
                    // Capture the resolution price while the cache straddles the boundary.
                    MarketLifecycle::TradingCutoff(market) => {
                        cutoff_price = resolution_price(&ticks, market.closes_at);
                        match &cutoff_price {
                            Some(p) => info!(market_slug = %market.slug, price = %p.0, "captured resolution price at cutoff"),
                            None => warn!(market_slug = %market.slug, "no pre-cutoff tick cached at TradingCutoff"),
                        }
                    }
                    MarketLifecycle::Resolved(market) => {
                        // Prefer the cutoff snapshot; fall back to a buffer scan (cold start).
                        let price = cutoff_price
                            .take()
                            .or_else(|| resolution_price(&ticks, market.closes_at));
                        settle_market_positions(
                            &client, &store, &market, price, Some(&settled_tx), Some(&pending_tx),
                        ).await;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Market, MarketOutcome};
    use crate::types::{MarketSlug, MarketStatus, MarketType, Shares, TokenId};
    use alloy::hex::FromHex;
    use alloy::primitives::FixedBytes;
    use polymarket_client_sdk_v2::types::U256;
    use rust_decimal_macros::dec;

    fn market(strike: Option<Price>, resolved_outcome: Option<&str>) -> Market {
        Market {
            slug: MarketSlug("btc-updown-5m-1000".into()),
            market_type: MarketType::UpDown,
            event_id: "e1".into(),
            question_id: FixedBytes::from_hex(
                "0x0000000000000000000000000000000000000000000000000000000000000001",
            )
            .unwrap(),
            condition_id: FixedBytes::from_hex(
                "0x0000000000000000000000000000000000000000000000000000000000000001",
            )
            .unwrap(),
            outcomes: vec![
                MarketOutcome { name: "Up".into(), token_id: TokenId(U256::from(1u64)) },
                MarketOutcome { name: "Down".into(), token_id: TokenId(U256::from(2u64)) },
            ],
            strike,
            opens_at: Timestamp(0),
            closes_at: Timestamp::from_secs(1000),
            resolves_at: Timestamp::from_secs(1000),
            status: MarketStatus::Resolved,
            resolved_outcome: resolved_outcome.map(|s| s.to_string()),
            order_price_min_tick_size: Price(dec!(0.01)),
            order_min_size: Shares(dec!(5)),
        }
    }

    #[test]
    fn prefers_resolved_outcome_over_price() {
        // resolved_outcome says Down even though price(65010) > strike(65000) would say Up.
        let m = market(Some(Price(dec!(65000))), Some("Down"));
        let winner = winning_outcome_name(&m, Some(&Price(dec!(65010))));
        assert_eq!(winner.as_deref(), Some("Down"));
    }

    #[test]
    fn falls_back_to_price_above_strike() {
        let m = market(Some(Price(dec!(65000))), None);
        let winner = winning_outcome_name(&m, Some(&Price(dec!(65010))));
        assert_eq!(winner.as_deref(), Some("Up"));
    }

    #[test]
    fn falls_back_to_price_at_or_below_strike_is_down() {
        let m = market(Some(Price(dec!(65000))), None);
        assert_eq!(
            winning_outcome_name(&m, Some(&Price(dec!(64990)))).as_deref(),
            Some("Down")
        );
        // price == strike → Down (tie-break).
        assert_eq!(
            winning_outcome_name(&m, Some(&Price(dec!(65000)))).as_deref(),
            Some("Down")
        );
    }

    #[test]
    fn none_when_no_outcome_and_no_price() {
        let m = market(Some(Price(dec!(65000))), None);
        assert_eq!(winning_outcome_name(&m, None), None);
    }

    #[test]
    fn none_when_no_outcome_and_no_strike() {
        let m = market(None, None);
        assert_eq!(winning_outcome_name(&m, Some(&Price(dec!(65010)))), None);
    }

    #[test]
    fn resolved_outcome_used_even_without_strike_or_price() {
        // Recovery-tool path: no live price, no strike, but catalog gave the winner.
        let m = market(None, Some("Up"));
        assert_eq!(winning_outcome_name(&m, None).as_deref(), Some("Up"));
    }
}

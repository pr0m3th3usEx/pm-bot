use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::domain::{ActiveMarket, PendingRedemption, PositionUpdate, Settled, Tick};
use crate::format::{banner, signed_usd, usd};
use crate::ports::{MarketClient, Store};
use crate::types::{MarketStatus, Outcome, PositionStatus, Price, Timestamp, Usdc};

const TICK_CACHE: usize = 50;

/// Newest cached tick at or before the cutoff — the price the market resolves on.
fn resolution_price(ticks: &VecDeque<Tick>, closes_at: Timestamp) -> Option<Price> {
    ticks
        .iter()
        .rfind(|t| t.at.0 <= closes_at.0)
        .map(|t| t.price.clone())
}

async fn handle_settlement(
    client: &Arc<dyn MarketClient>,
    store: &Arc<dyn Store>,
    market: &ActiveMarket,
    resolution_price: Option<Price>,
    settled_tx: &broadcast::Sender<Settled>,
    pending_tx: &mpsc::Sender<PendingRedemption>,
) {
    // Need both a strike and a resolution price to decide outcomes.
    let Some(strike) = market.strike.clone() else {
        warn!(market_slug = %market.slug, "no strike on resolved market — cannot settle");
        return;
    };
    let Some(price) = resolution_price else {
        warn!(market_slug = %market.slug, "no pre-cutoff price available — cannot settle");
        return;
    };

    // Winning outcome mirrors V1BasicStrategy: Up if price > strike, else Down.
    let winning = if price.0 > strike.0 {
        Outcome::Up
    } else {
        Outcome::Down
    };
    info!(
        market_slug = %market.slug,
        resolution_price = %price.0,
        strike = %strike.0,
        winning = winning.as_str(),
        "🏁 market resolved — settling positions"
    );

    let positions = match store.open_positions().await {
        Ok(p) => p,
        Err(e) => {
            error!(error = %e, "failed to fetch positions for settlement");
            return;
        }
    };

    for pos in positions
        .into_iter()
        .filter(|p| p.status == PositionStatus::Filled && p.market_slug == market.slug)
    {
        let position_id = pos.id.expect("stored position must have id");
        let won = pos.outcome_name == winning.as_str();

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
                    let _ = settled_tx.send(Settled {
                        position_id,
                        status: PositionStatus::Won,
                        realized_pnl: Usdc(pnl),
                        cost: Usdc(cost),
                    });
                    if let Some(tx_id) = receipt.transaction_id {
                        let _ = pending_tx
                            .send(PendingRedemption {
                                position_id,
                                transaction_id: tx_id,
                                payout: receipt.payout,
                            })
                            .await;
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
            let _ = settled_tx.send(Settled {
                position_id,
                status: PositionStatus::Lost,
                realized_pnl: Usdc(pnl),
                cost: Usdc(cost),
            });
        }
    }
}

pub async fn settlement_task(
    client: Arc<dyn MarketClient>,
    store: Arc<dyn Store>,
    mut market_rx: watch::Receiver<Option<ActiveMarket>>,
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

            Ok(()) = market_rx.changed() => {
                let market = market_rx.borrow_and_update().clone();
                let Some(market) = market else { continue };
                match market.status {
                    // Capture the resolution price while the cache straddles the boundary.
                    MarketStatus::TradingCutoff => {
                        cutoff_price = resolution_price(&ticks, market.closes_at);
                        match &cutoff_price {
                            Some(p) => info!(market_slug = %market.slug, price = %p.0, "captured resolution price at cutoff"),
                            None => warn!(market_slug = %market.slug, "no pre-cutoff tick cached at TradingCutoff"),
                        }
                    }
                    MarketStatus::Resolved => {
                        // Prefer the cutoff snapshot; fall back to a buffer scan (cold start).
                        let price = cutoff_price
                            .take()
                            .or_else(|| resolution_price(&ticks, market.closes_at));
                        handle_settlement(&client, &store, &market, price, &settled_tx, &pending_tx).await;
                    }
                    _ => {}
                }
            }
        }
    }
}

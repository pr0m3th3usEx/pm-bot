use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::domain::{OrderUpdate, Redeemed, Settled};
use crate::state::BankrollState;
use crate::types::Usdc;

pub async fn bankroll_task(
    state: Arc<RwLock<BankrollState>>,
    mut order_update_rx: broadcast::Receiver<OrderUpdate>,
    mut settled_rx: broadcast::Receiver<Settled>,
    mut redeemed_rx: mpsc::Receiver<Redeemed>,
    cancel: CancellationToken,
) {
    info!("bankroll_task started");
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("bankroll_task cancelled");
                break;
            }

            result = order_update_rx.recv() => {
                let update = match result {
                    Ok(u) => u,
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(skipped = n, "bankroll order_update_rx lagged");
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => continue,
                };
                if let OrderUpdate::Filled { avg_price, size_matched, .. } = update {
                    let cost = Usdc(avg_price.0 * size_matched.0);
                    let mut b = state.write().await;
                    b.update_on_fill(cost);
                    info!(
                        bankroll = %b.bankroll.0,
                        money_in_play = %b.money_in_play.0,
                        about_to_be_redeemed = %b.about_to_be_redeemed.0,
                        "bankroll updated on fill"
                    );
                }
                // Submitted / Rejected / Cancelled do not affect bankroll
            }

            result = settled_rx.recv() => {
                let settled = match result {
                    Ok(s) => s,
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(skipped = n, "bankroll settled_rx lagged");
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => continue,
                };
                let mut b = state.write().await;
                b.update_on_settlement(settled.cost, settled.realized_pnl);
                info!(
                    bankroll = %b.bankroll.0,
                    money_in_play = %b.money_in_play.0,
                    about_to_be_redeemed = %b.about_to_be_redeemed.0,
                    "bankroll updated on settlement"
                );
            }

            Some(redeemed) = redeemed_rx.recv() => {
                let mut b = state.write().await;
                b.update_on_redemption(redeemed.payout);
                info!(
                    bankroll = %b.bankroll.0,
                    money_in_play = %b.money_in_play.0,
                    about_to_be_redeemed = %b.about_to_be_redeemed.0,
                    "bankroll updated on redemption"
                );
            }
        }
    }
}

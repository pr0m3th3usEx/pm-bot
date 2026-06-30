use std::sync::Arc;
use tokio::sync::{watch, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::domain::ActiveMarket;
use crate::ports::MarketDataFeed;
use crate::state::OutcomeBookCache;

/// Derive a sorted Vec<String> of asset ids from a market's outcomes.
///
/// Sorted so that set-equality comparison is order-independent.
fn asset_ids_for_market(market: &ActiveMarket) -> Vec<String> {
    let mut ids: Vec<String> = market
        .outcomes
        .iter()
        .map(|o| o.token_id.0.to_string())
        .collect();
    ids.sort();
    ids
}

/// Helper: poll `feed.next_update()` only when `feed` is `Some`.
///
/// The `if feed.is_some()` guard in the `select!` arm is not enough on its own
/// when the arm's future has already been polled — wrapping it here avoids
/// a potential busy-loop on a permanently-None feed.
async fn feed_next(feed: &mut Option<Box<dyn MarketDataFeed>>) -> crate::error::Result<crate::domain::OutcomeBook> {
    // SAFETY: callers gate on `feed.is_some()`.
    feed.as_mut().unwrap().next_update().await
}

/// Maintains a live `OutcomeBookCache` fed by Polymarket's market WebSocket channel.
///
/// Watches the `market_rx` channel and rebuilds the feed whenever the active market
/// changes (i.e. when the token ids rotate). Populates `cache` with each incoming
/// `OutcomeBook`; clears it on market rotation.
pub async fn market_data_task(
    mut market_rx: watch::Receiver<Option<ActiveMarket>>,
    cache: Arc<RwLock<OutcomeBookCache>>,
    connect: impl Fn(Vec<String>) -> Box<dyn MarketDataFeed> + Send,
    cancel: CancellationToken,
) {
    info!("market_data_task started");

    let mut feed: Option<Box<dyn MarketDataFeed>> = None;
    let mut current_ids: Vec<String> = Vec::new();

    // Check for an already-present market on startup.
    {
        let guard = market_rx.borrow();
        if let Some(market) = guard.as_ref() {
            let ids = asset_ids_for_market(market);
            if !ids.is_empty() {
                debug!(ids = ?ids, "market_data_task: initial market present — connecting feed");
                current_ids = ids.clone();
                feed = Some(connect(ids));
            }
        }
    }

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("market_data_task cancelled");
                break;
            }

            _ = market_rx.changed() => {
                let new_ids = {
                    let guard = market_rx.borrow();
                    match guard.as_ref() {
                        Some(market) => asset_ids_for_market(market),
                        None => Vec::new(),
                    }
                };

                if new_ids == current_ids {
                    // Same token ids — rotation published a status transition (e.g. Open → TradingCutoff).
                    // Do not rebuild the feed or clear the cache.
                    debug!("market_data_task: market update with unchanged ids — keeping feed");
                    continue;
                }

                debug!(old = ?current_ids, new = ?new_ids, "market_data_task: market ids changed — rebuilding feed");

                // Clear stale cache entries from the previous round.
                cache.write().await.clear();
                current_ids = new_ids.clone();

                // Drop the old feed (ends its background WS task) and create a fresh one.
                feed = if new_ids.is_empty() {
                    None
                } else {
                    Some(connect(new_ids))
                };
            }

            res = feed_next(&mut feed), if feed.is_some() => {
                match res {
                    Ok(book) => {
                        debug!(token_id = %book.token_id.0, "market_data_task: received OutcomeBook update");
                        debug!(token_id = %book.token_id.0, buy = ?book.buy_price, sell = ?book.sell_price, "market_data_task: updating cache");

                        cache.write().await.update(book);
                    }
                    Err(e) => {
                        // The adapter reconnects internally; if it returns Err the channel
                        // closed (receiver dropped). Clearing feed + ids prevents a busy-loop
                        // and lets the next market_rx.changed() rebuild cleanly.
                        warn!(error = %e, "market_data_task: feed error — clearing feed until next market rotation");
                        feed = None;
                        current_ids.clear();
                    }
                }
            }
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{MarketOutcome, OutcomeBook};
    use crate::error::Result;
    use crate::state::OutcomeBookCache;
    use crate::types::{
        MarketSlug, MarketStatus, MarketType, Price, Shares, Side, Timestamp, TokenId,
    };
    use crate::error::CoreError;
    use async_trait::async_trait;
    use polymarket_client_sdk_v2::types::U256;
    use rust_decimal::Decimal;
    use std::str::FromStr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::{watch, RwLock};
    use tokio::time::Duration;
    use tokio_util::sync::CancellationToken;

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn make_token_id(n: u64) -> TokenId {
        TokenId(U256::from(n))
    }

    fn make_price(s: &str) -> Price {
        Price(Decimal::from_str(s).unwrap())
    }

    fn make_outcome_book(token_id: TokenId, buy: &str, sell: &str) -> OutcomeBook {
        OutcomeBook {
            token_id,
            buy_price: Some(make_price(buy)),
            sell_price: Some(make_price(sell)),
            at: Timestamp(0),
        }
    }

    fn make_market(token_ids: &[u64]) -> crate::domain::Market {
        use alloy::primitives::FixedBytes;
        crate::domain::Market {
            slug: MarketSlug("test-slug".to_string()),
            market_type: MarketType::UpDown,
            event_id: "evt".to_string(),
            question_id: FixedBytes::default(),
            condition_id: FixedBytes::default(),
            outcomes: token_ids
                .iter()
                .enumerate()
                .map(|(i, &id)| MarketOutcome {
                    name: format!("outcome-{i}"),
                    token_id: make_token_id(id),
                })
                .collect(),
            strike: None,
            opens_at: Timestamp(0),
            closes_at: Timestamp(i64::MAX),
            resolves_at: Timestamp(i64::MAX),
            status: MarketStatus::Open,
            order_price_min_tick_size: make_price("0.01"),
            order_min_size: Shares(Decimal::from_str("5").unwrap()),
        }
    }

    // ── Fake MarketDataFeed ───────────────────────────────────────────────────

    struct FakeFeed {
        rx: tokio::sync::mpsc::Receiver<OutcomeBook>,
    }

    #[async_trait]
    impl MarketDataFeed for FakeFeed {
        async fn next_update(&mut self) -> Result<OutcomeBook> {
            self.rx
                .recv()
                .await
                .ok_or_else(|| CoreError::Adapter("FakeFeed closed".to_string()))
        }
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    /// Cache is populated after sending a market + a book update.
    /// Sending the SAME market again does not rebuild (connect counter stays at 1).
    /// Sending a DIFFERENT market clears stale entries.
    #[tokio::test]
    async fn market_data_task_populates_cache_and_rotates() {
        let connect_count = Arc::new(AtomicUsize::new(0));
        let cancel = CancellationToken::new();

        // Channels for injecting fake book updates.
        let (book_tx1, book_rx1) = tokio::sync::mpsc::channel::<OutcomeBook>(8);
        let (book_tx2, book_rx2) = tokio::sync::mpsc::channel::<OutcomeBook>(8);

        // Use std::sync::Mutex so the sync closure can lock without entering async.
        let book_rx1 = Arc::new(std::sync::Mutex::new(Some(book_rx1)));
        let book_rx2 = Arc::new(std::sync::Mutex::new(Some(book_rx2)));

        let connect_count_clone = connect_count.clone();
        let book_rx1_clone = book_rx1.clone();
        let book_rx2_clone = book_rx2.clone();

        let connect = move |_ids: Vec<String>| -> Box<dyn MarketDataFeed> {
            let n = connect_count_clone.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                let rx = book_rx1_clone.lock().unwrap().take().unwrap();
                Box::new(FakeFeed { rx })
            } else {
                let rx = book_rx2_clone.lock().unwrap().take().unwrap();
                Box::new(FakeFeed { rx })
            }
        };

        let (market_tx, market_rx) = watch::channel::<Option<crate::domain::ActiveMarket>>(None);
        let cache = Arc::new(RwLock::new(OutcomeBookCache::default()));

        let task_cache = cache.clone();
        let task_cancel = cancel.clone();
        tokio::spawn(market_data_task(market_rx, task_cache, connect, task_cancel));

        // Send market with token id = 1 and 2.
        let market1 = make_market(&[1, 2]);
        market_tx.send(Some(market1.clone())).unwrap();
        // Give the task time to process.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(connect_count.load(Ordering::SeqCst), 1, "first market should connect once");

        // Push a book update for token 1.
        let book1 = make_outcome_book(make_token_id(1), "0.55", "0.45");
        book_tx1.send(book1.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        {
            let c = cache.read().await;
            let tid1 = make_token_id(1);
            assert_eq!(
                c.price(&tid1, Side::Buy),
                Some(make_price("0.55")),
                "cache should contain buy price for token 1"
            );
            assert_eq!(
                c.price(&tid1, Side::Sell),
                Some(make_price("0.45")),
                "cache should contain sell price for token 1"
            );
        }

        // Send the SAME market again (same ids, different status) — should NOT rebuild.
        let mut market1_cutoff = market1.clone();
        market1_cutoff.status = MarketStatus::TradingCutoff;
        market_tx.send(Some(market1_cutoff)).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(connect_count.load(Ordering::SeqCst), 1, "same ids should NOT rebuild feed");

        // Cache entry is still present.
        {
            let c = cache.read().await;
            assert!(c.price(&make_token_id(1), Side::Buy).is_some(), "cache should still have entry");
        }

        // Send a DIFFERENT market (token ids 3 and 4) — should clear stale entry and rebuild.
        let market2 = make_market(&[3, 4]);
        market_tx.send(Some(market2)).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(connect_count.load(Ordering::SeqCst), 2, "different market should rebuild feed");

        // Stale entry for token 1 must be gone.
        {
            let c = cache.read().await;
            assert!(c.price(&make_token_id(1), Side::Buy).is_none(), "stale cache entry should be cleared");
        }

        // Push an update for token 3 via the second feed.
        let book3 = make_outcome_book(make_token_id(3), "0.60", "0.40");
        book_tx2.send(book3).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        {
            let c = cache.read().await;
            assert!(
                c.price(&make_token_id(3), Side::Buy).is_some(),
                "cache should contain entry for token 3 after second market"
            );
        }

        cancel.cancel();
    }
}

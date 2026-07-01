//! Dry-run `MarketClient` adapter — no real network calls, no wallet required.
//!
//! `SimMarketClient` reads live Polymarket outcome prices from the shared
//! `OutcomeBookCache` (populated by `market_data_task`) and simulates order
//! fills using a configurable latency + quote-crossing model.

use async_trait::async_trait;
use pm_core::format::usd;
use pm_core::{
    config::SimConfig,
    domain::{Intent, OrderUpdate, PositionRecord, RedeemReceipt},
    error::{CoreError, Result},
    ports::{MarketClient, RedemptionStatus},
    state::OutcomeBookCache,
    types::{Price, Shares, Side, Timestamp, TokenId, Usdc},
};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Mutex, RwLock};

// ─── Internal order representation ────────────────────────────────────────────

#[derive(Debug, Clone)]
struct SimOrder {
    #[allow(dead_code)]
    order_id: String,
    position_id: i64,
    token_id: TokenId,
    side: Side,
    limit_price: Price,
    shares: Shares,
    submitted_at: Timestamp,
}

// ─── Inner mutable state ──────────────────────────────────────────────────────

struct SimState {
    resting: HashMap<String, SimOrder>,
    /// Virtual USDC balance (seeded from SimConfig.virtual_bankroll).
    balance: Usdc,
    /// Running total reserved by resting orders.
    reserved: Usdc,
}

impl SimState {
    fn new(balance: Usdc) -> Self {
        Self {
            resting: HashMap::new(),
            balance,
            reserved: Usdc(Decimal::ZERO),
        }
    }

    fn reserve(&mut self, amount: Usdc) {
        self.reserved.0 += amount.0;
        self.balance.0 -= amount.0;
    }

    fn release_reservation(&mut self, amount: Usdc) {
        self.reserved.0 -= amount.0;
        self.balance.0 += amount.0;
    }
}

// ─── SimMarketClient ──────────────────────────────────────────────────────────

pub struct SimMarketClient {
    book_cache: Arc<RwLock<OutcomeBookCache>>,
    config: SimConfig,
    state: Mutex<SimState>,
    counter: AtomicU64,
}

impl SimMarketClient {
    pub fn new(book_cache: Arc<RwLock<OutcomeBookCache>>, config: SimConfig) -> Self {
        let balance = config.virtual_bankroll.clone();
        Self {
            book_cache,
            state: Mutex::new(SimState::new(balance)),
            config,
            counter: AtomicU64::new(1),
        }
    }

    fn mint_id(&self) -> String {
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        format!("sim-{n}")
    }

    /// Compute reservation cost for an order: limit_price × shares.
    fn reservation(limit_price: &Price, shares: &Shares) -> Usdc {
        Usdc(limit_price.0 * shares.0)
    }

    /// Apply taker fee to the net fill cost and return the fee amount.
    fn apply_fee(&self, cost: Usdc) -> (Usdc, Usdc) {
        if self.config.taker_fee_bps == 0 {
            return (Usdc(Decimal::ZERO), cost);
        }
        let fee = cost.0 * Decimal::from(self.config.taker_fee_bps) / Decimal::from(10_000u32);
        (Usdc(fee), Usdc(cost.0 + fee))
    }
}

#[async_trait]
impl MarketClient for SimMarketClient {
    async fn place_order(&self, intent: &Intent, token_id: &TokenId) -> Result<String> {
        let order_id = self.mint_id();
        let now = Timestamp::now_ms();

        let order = SimOrder {
            order_id: order_id.clone(),
            position_id: 0, // will be set when order_status is polled via position_id arg
            token_id: token_id.clone(),
            side: intent.side,
            limit_price: intent.limit_price.clone(),
            shares: intent.shares.clone(),
            submitted_at: now,
        };

        let reservation = Self::reservation(&intent.limit_price, &intent.shares);

        let mut state = self.state.lock().await;
        state.reserve(reservation);
        state.resting.insert(order_id.clone(), order);

        tracing::info!(
            order_id = %order_id,
            side = ?intent.side,
            limit_price = %intent.limit_price.0,
            shares = %intent.shares.0,
            token_id = %token_id.0,
            "📤 [dry-run] placed simulated order · {} @ {}",
            intent.shares.0,
            usd(intent.limit_price.0)
        );

        Ok(order_id)
    }

    /// The fill model. Called by `order_status_poller_task` every 2 seconds.
    ///
    /// 1. Latency gate: `now - submitted_at >= fill_latency_ms`.
    /// 2. Cross check via `OutcomeBookCache`:
    ///    - BUY fills when `buy_price (ask) <= limit_price`
    ///    - SELL fills when `sell_price (bid) >= limit_price`
    /// 3. On fill: remove from resting, return `Filled`; else return `Submitted`.
    async fn order_status(&self, order_id: &str, position_id: i64) -> Result<OrderUpdate> {
        let now_ms = Timestamp::now_ms().0;

        let mut state = self.state.lock().await;

        // Update the position_id in the resting order if it hasn't been set
        // (position_id is known at poll time, not at submit time).
        if let Some(order) = state.resting.get_mut(order_id) {
            order.position_id = position_id;
        }

        let order = match state.resting.get(order_id) {
            Some(o) => o.clone(),
            None => {
                // Order not found → was already filled/cancelled or never existed.
                return Ok(OrderUpdate::Cancelled {
                    order_id: order_id.to_owned(),
                    position_id,
                });
            }
        };

        // 1. Latency gate.
        let elapsed_ms = now_ms - order.submitted_at.0;
        if elapsed_ms < self.config.fill_latency_ms && !self.config.always_fill {
            return Ok(OrderUpdate::Submitted {
                order_id: order_id.to_owned(),
                position_id,
            });
        }

        // 2. Cross check.
        let should_fill = if self.config.always_fill {
            true
        } else {
            // We must release the mutex to read book_cache (different lock).
            drop(state);
            let book_price = {
                let cache = self.book_cache.read().await;
                cache.price(&order.token_id, order.side)
            };
            // Re-acquire state.
            state = self.state.lock().await;

            match book_price {
                None => false, // cache not populated yet
                Some(market_price) => match order.side {
                    // BUY: we want to buy at limit_price. Fill when ask <= limit_price.
                    Side::Buy => market_price.0 <= order.limit_price.0,
                    // SELL: we want to sell at limit_price. Fill when bid >= limit_price.
                    Side::Sell => market_price.0 >= order.limit_price.0,
                },
            }
        };

        if !should_fill {
            return Ok(OrderUpdate::Submitted {
                order_id: order_id.to_owned(),
                position_id,
            });
        }

        // 3. Fill: remove from resting, release reservation, return Filled.
        let order = state.resting.remove(order_id).unwrap();
        let reservation = Self::reservation(&order.limit_price, &order.shares);
        state.release_reservation(reservation);

        let avg_price = order.limit_price.clone();

        let (_fee, _total_cost) = self.apply_fee(Usdc(avg_price.0 * order.shares.0));

        tracing::info!(
            order_id = %order_id,
            position_id = position_id,
            avg_price = %avg_price.0,
            shares = %order.shares.0,
            "📥 [dry-run] simulated order filled · {} @ {} · cost {}",
            order.shares.0,
            usd(avg_price.0),
            usd(avg_price.0 * order.shares.0)
        );

        Ok(OrderUpdate::Filled {
            order_id: order_id.to_owned(),
            position_id,
            avg_price,
            size_matched: order.shares,
        })
    }

    async fn cancel_order(&self, order_id: &str) -> Result<()> {
        let mut state = self.state.lock().await;
        if let Some(order) = state.resting.remove(order_id) {
            let reservation = Self::reservation(&order.limit_price, &order.shares);
            state.release_reservation(reservation);
            tracing::info!(order_id = %order_id, "🚫 [dry-run] cancelled simulated order");
        }
        Ok(())
    }

    async fn quote(&self, token_id: &TokenId, side: Side) -> Result<Price> {
        self.book_cache
            .read()
            .await
            .price(token_id, side)
            .ok_or_else(|| CoreError::Adapter(format!("no cached price for token {}", token_id.0)))
    }

    async fn balance(&self) -> Result<Usdc> {
        let state = self.state.lock().await;
        Ok(state.balance.clone())
    }

    async fn redeem(&self, position: &PositionRecord) -> Result<RedeemReceipt> {
        // In dry-run we don't know at this point whether it's a win or loss —
        // settlement_task already determined that and marked the position Won/Lost.
        // The payout is 1 USDC per share for a winner (settlement confirms the Win).
        // Since we don't have the resolved status here, we use the convention:
        // the live client always returns shares * 1 USDC for a redeem call;
        // settlement only calls redeem for won positions.
        let payout = Usdc(position.shares.0);
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        let transaction_id = format!("sim-tx-{n}");

        tracing::info!(
            position_id = ?position.id,
            payout = %payout.0,
            transaction_id = %transaction_id,
            "🎁 [dry-run] synthetic redemption · {}",
            usd(payout.0)
        );

        Ok(RedeemReceipt {
            transaction_id: Some(transaction_id),
            payout,
        })
    }

    async fn redemption_status(&self, _transaction_id: &str) -> Result<RedemptionStatus> {
        Ok(RedemptionStatus::Confirmed)
    }

    async fn heartbeat(&self) -> Result<()> {
        Ok(())
    }
}

// ─── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use pm_core::{domain::OutcomeBook, types::Timestamp};
    use polymarket_client_sdk_v2::types::U256;
    use rust_decimal_macros::dec;

    fn token(n: u64) -> TokenId {
        TokenId(U256::from(n))
    }

    fn make_cache_with_price(
        token_id: TokenId,
        buy_price: Option<Price>,
        sell_price: Option<Price>,
    ) -> Arc<RwLock<OutcomeBookCache>> {
        let cache = Arc::new(RwLock::new(OutcomeBookCache::default()));
        {
            let mut c = cache.try_write().unwrap();
            c.update(OutcomeBook {
                token_id,
                buy_price,
                sell_price,
                at: Timestamp(0),
            });
        }
        cache
    }

    fn make_intent(side: Side, limit_price: Decimal, shares: Decimal) -> Intent {
        Intent {
            outcome: pm_core::types::Outcome::Up,
            side,
            shares: Shares(shares),
            limit_price: Price(limit_price),
        }
    }

    /// Helper: build a client with default latency=0 for immediate-fill tests.
    fn instant_client(
        cache: Arc<RwLock<OutcomeBookCache>>,
        always_fill: bool,
    ) -> SimMarketClient {
        SimMarketClient::new(
            cache,
            SimConfig {
                virtual_bankroll: Usdc(dec!(1000)),
                fill_latency_ms: 0,
                taker_fee_bps: 0,
                always_fill,
                dryrun_db_path: ":memory:".to_owned(),
            },
        )
    }

    /// Helper: place an order and return its id.
    async fn place(client: &SimMarketClient, intent: &Intent, token_id: &TokenId) -> String {
        client.place_order(intent, token_id).await.unwrap()
    }

    #[tokio::test]
    async fn buy_fills_when_ask_crosses() {
        // Market ask = 0.52, limit = 0.55 → ask <= limit → should fill.
        let tok = token(1);
        let cache = make_cache_with_price(tok.clone(), Some(Price(dec!(0.52))), None);
        let client = instant_client(cache, false);

        let intent = make_intent(Side::Buy, dec!(0.55), dec!(10));
        let oid = place(&client, &intent, &tok).await;
        let update = client.order_status(&oid, 1).await.unwrap();
        assert!(
            matches!(update, OrderUpdate::Filled { .. }),
            "expected Filled, got {update:?}"
        );
    }

    #[tokio::test]
    async fn buy_stays_resting_when_ask_above_limit() {
        // Market ask = 0.60, limit = 0.55 → ask > limit → should NOT fill.
        let tok = token(2);
        let cache = make_cache_with_price(tok.clone(), Some(Price(dec!(0.60))), None);
        let client = instant_client(cache, false);

        let intent = make_intent(Side::Buy, dec!(0.55), dec!(10));
        let oid = place(&client, &intent, &tok).await;
        let update = client.order_status(&oid, 1).await.unwrap();
        assert!(
            matches!(update, OrderUpdate::Submitted { .. }),
            "expected Submitted, got {update:?}"
        );
    }

    #[tokio::test]
    async fn sell_fills_when_bid_crosses() {
        // Market bid = 0.55, limit = 0.50 → bid >= limit → should fill.
        let tok = token(3);
        let cache = make_cache_with_price(tok.clone(), None, Some(Price(dec!(0.55))));
        let client = instant_client(cache, false);

        let intent = make_intent(Side::Sell, dec!(0.50), dec!(10));
        let oid = place(&client, &intent, &tok).await;
        let update = client.order_status(&oid, 1).await.unwrap();
        assert!(
            matches!(update, OrderUpdate::Filled { .. }),
            "expected Filled, got {update:?}"
        );
    }

    #[tokio::test]
    async fn sell_stays_resting_when_bid_below_limit() {
        // Market bid = 0.40, limit = 0.55 → bid < limit → should NOT fill.
        let tok = token(4);
        let cache = make_cache_with_price(tok.clone(), None, Some(Price(dec!(0.40))));
        let client = instant_client(cache, false);

        let intent = make_intent(Side::Sell, dec!(0.55), dec!(10));
        let oid = place(&client, &intent, &tok).await;
        let update = client.order_status(&oid, 1).await.unwrap();
        assert!(
            matches!(update, OrderUpdate::Submitted { .. }),
            "expected Submitted, got {update:?}"
        );
    }

    #[tokio::test]
    async fn no_fill_before_latency() {
        // Fill latency = 5000ms (5 seconds) → should NOT fill immediately.
        let tok = token(5);
        // Ask crosses the limit (would otherwise fill).
        let cache = make_cache_with_price(tok.clone(), Some(Price(dec!(0.50))), None);
        let client = SimMarketClient::new(
            cache,
            SimConfig {
                virtual_bankroll: Usdc(dec!(1000)),
                fill_latency_ms: 5_000,
                taker_fee_bps: 0,
                always_fill: false,
                dryrun_db_path: ":memory:".to_owned(),
            },
        );

        let intent = make_intent(Side::Buy, dec!(0.55), dec!(10));
        let oid = place(&client, &intent, &tok).await;
        let update = client.order_status(&oid, 1).await.unwrap();
        assert!(
            matches!(update, OrderUpdate::Submitted { .. }),
            "should be Submitted before latency expires, got {update:?}"
        );
    }

    #[tokio::test]
    async fn cancel_releases_reservation() {
        let tok = token(6);
        let cache = Arc::new(RwLock::new(OutcomeBookCache::default()));
        let client = instant_client(cache, false);

        let initial_balance = client.balance().await.unwrap().0;

        let intent = make_intent(Side::Buy, dec!(0.50), dec!(10));
        let oid = place(&client, &intent, &tok).await;

        // After place, balance should be reduced by 0.50 * 10 = 5.
        let post_place = client.balance().await.unwrap().0;
        assert_eq!(post_place, initial_balance - dec!(5));

        client.cancel_order(&oid).await.unwrap();

        // After cancel, reservation released → balance restored.
        let post_cancel = client.balance().await.unwrap().0;
        assert_eq!(post_cancel, initial_balance);
    }

    #[tokio::test]
    async fn always_fill_bypasses_price_check() {
        // No price in cache, but always_fill=true → still fills.
        let tok = token(7);
        let cache = Arc::new(RwLock::new(OutcomeBookCache::default())); // empty cache
        let client = instant_client(cache, true);

        let intent = make_intent(Side::Buy, dec!(0.55), dec!(5));
        let oid = place(&client, &intent, &tok).await;
        let update = client.order_status(&oid, 1).await.unwrap();
        assert!(
            matches!(update, OrderUpdate::Filled { .. }),
            "expected Filled with always_fill, got {update:?}"
        );
    }

    #[tokio::test]
    async fn balance_starts_at_virtual_bankroll() {
        let cache = Arc::new(RwLock::new(OutcomeBookCache::default()));
        let client = SimMarketClient::new(
            cache,
            SimConfig {
                virtual_bankroll: Usdc(dec!(500)),
                ..SimConfig::default()
            },
        );
        let bal = client.balance().await.unwrap();
        assert_eq!(bal.0, dec!(500));
    }

    #[tokio::test]
    async fn unknown_order_returns_cancelled() {
        let cache = Arc::new(RwLock::new(OutcomeBookCache::default()));
        let client = instant_client(cache, false);
        let update = client.order_status("nonexistent-id", 99).await.unwrap();
        assert!(matches!(update, OrderUpdate::Cancelled { position_id: 99, .. }));
    }

    #[tokio::test]
    async fn heartbeat_is_noop() {
        let cache = Arc::new(RwLock::new(OutcomeBookCache::default()));
        let client = instant_client(cache, false);
        client.heartbeat().await.unwrap();
    }

    #[tokio::test]
    async fn redemption_status_is_confirmed() {
        let cache = Arc::new(RwLock::new(OutcomeBookCache::default()));
        let client = instant_client(cache, false);
        let status = client.redemption_status("sim-tx-1").await.unwrap();
        assert_eq!(status, RedemptionStatus::Confirmed);
    }
}

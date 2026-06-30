# Dry-Run & Simulation Modes for the pm-bot Trading Engine

> **Status:** Phase 1 (Dry-Run) is the active implementation target.
> Phase 2 (Historical Simulation) is **design-only — not implemented in this pass.**

## Context

The engine (`apps/pm-bot`) currently trades **Polymarket BTC Up/Down 5m** markets with
real orders against the live CLOB only. There is no risk-free way to (a) run the full
stack on **live data without spending money** (dry-run) or (b) **backtest strategies on
historical data** (simulation). This blocks safe iteration on `V1BasicStrategy` /
`QuantStrategy` and on sizing/entry policies.

The codebase is already **ports-and-adapters**, so most of this is *new adapters + mode
wiring*, not surgery on the engine. The goal is for dry-run/sim to imitate live conditions
as closely as possible, with the **same business logic, state machine, and tasks** across
all modes — only the adapters change.

**Decisions:**
- **Sequence:** Dry-run first (Phase 1). Simulation (Phase 2) designed but deferred.
- **Fill model (v1):** Quote-crossing + configurable latency (no Polymarket CLOB depth needed).
- **Historical data:** None exists today → build a **recorder** (piggybacks on dry-run's live feeds).

### Note: current price-quote mechanism (PR #9)

Marketable outcome prices are obtained via a **streaming** path, not a pull:

- `MarketDataFeed` port → streams `OutcomeBook { token_id, buy_price, sell_price }`
  (live Polymarket outcome top-of-book) via `adapters/src/polymarket_market_feed.rs`.
- `market_data_task` (`libs/pm-core/src/tasks/market_data.rs`) drives the feed per round and
  writes a shared `OutcomeBookCache` (`libs/pm-core/src/state.rs`, `Arc<RwLock<…>>`), keyed
  by `TokenId`.
- `decision_center_task` reads the marketable price from `OutcomeBookCache::price(token_id,
  side)` — it **no longer calls `MarketClient::quote()`**.
- `executor_task` is **currently commented out** in `main.rs` — re-enabling it under the sim
  client is part of Phase 1.

Net effect: the **live quote source for the fill model is the `OutcomeBookCache`**, and the
**recorder wraps `MarketDataFeed`**, not CLOB `quote()`.

## Architecture: where modes plug in

All engine tasks (`executor`, `order_status_poller`, `settlement`, `bankroll`,
`persistence`, `decision_center`) depend **only on traits** in
`libs/pm-core/src/ports.rs`. Concrete adapters are wired in exactly one place:
`apps/pm-bot/src/main.rs`. The seams we exploit:

| Concern | Trait (`ports.rs`) | Live adapter | New for dry-run |
|---|---|---|---|
| Execution | `MarketClient` | `ClobMarketClient` | **`SimMarketClient`** |
| Outcome prices | `MarketDataFeed` | `PolymarketMarketFeed` | `RecordingMarketDataFeed` (decorator) |
| Price ticks | `PriceFeed` | Chainlink/Binance | `RecordingPriceFeed` (decorator) |
| Market meta | `MarketCatalog` | `GammaMarketCatalog` | `RecordingCatalog` (decorator) |
| Audit/PnL | `Store` | `SqliteStore` | `SqliteStore` (separate dry-run DB file) |
| Strategy/sizing | `Strategy`/`SizingModel` | pure | **unchanged** |

**Key insight:** the fill decision belongs in `MarketClient::order_status()`, because that
is the method `order_status_poller_task` already polls every 2s. On each poll the simulated
executor reads the **live Polymarket outcome price from the shared `OutcomeBookCache`** (the
same data `decision_center` trades on) and decides whether the resting order has crossed —
no new task, no engine change. `SimMarketClient` is handed an `Arc<RwLock<OutcomeBookCache>>`
at construction.

---

## Phase 1 — Dry-Run Mode (primary deliverable)

### 1. Execution mode config
- Add `ExecutionMode { Live, DryRun }` enum in `pm-core` (`types.rs` or a new `config.rs`).
- Read from env (`EXECUTION_MODE=dry-run`, default `live`) in `main.rs`. CLI flags can come later.
- In `main.rs`, branch the adapter construction only — task wiring stays identical, and
  **re-enable the currently-commented-out `executor_task`** (wired with `client`):
  ```text
  let client: Arc<dyn MarketClient> = match mode {
      Live   => Arc::new(ClobMarketClient::new(...)),
      // SimMarketClient reads live crossing from the shared OutcomeBookCache (PR #9):
      DryRun => Arc::new(SimMarketClient::new(book_cache.clone(), SimConfig::from_env())),
  };
  let store = SqliteStore::open(db_path_for(mode));   // separate file in dry-run
  let bankroll0 = match mode { DryRun => cfg.virtual_bankroll, Live => client.balance().await? };
  ```
  `book_cache` is the `Arc<RwLock<OutcomeBookCache>>` already constructed in `main.rs` and
  fed by `market_data_task`.

### 2. `SimMarketClient` — new adapter (`adapters/src/sim_market_client.rs`)
Implements `MarketClient`. Internal state behind a `Mutex`/`RwLock`:
- `resting: HashMap<String, SimOrder>` where `SimOrder { order_id, position_id, token_id,
  side, limit_price, shares, submitted_at }`.
- A **virtual balance** seeded from `SimConfig.virtual_bankroll`.
- `book_cache: Arc<RwLock<OutcomeBookCache>>` — the **real live** Polymarket outcome
  buy/sell prices (PR #9), shared with `decision_center` and fed by `market_data_task`.

Method behavior:
- `place_order(intent, token_id)` → mint synthetic id (`sim-{counter}`), insert into
  `resting`, reserve `limit_price * shares` from virtual balance, return id. No network.
- `order_status(order_id, position_id)` → **the fill model** (see §3). Returns
  `OrderUpdate::Submitted` while resting, `Filled { avg_price, size_matched }` once crossed.
- `cancel_order(id)` → drop from `resting`, release reservation.
- `quote(token_id, side)` → read `book_cache.price(token_id, side)` (still on the trait,
  though `decision_center` no longer calls it post-PR #9).
- `balance()` → return the virtual balance (never touches the Safe wallet).
- `redeem(position)` → synthetic `RedeemReceipt { payout, transaction_id: Some("sim-tx-..") }`;
  payout = winning ? `shares * 1 USDC` : `0`. (Win/loss already decided by `settlement_task`.)
- `redemption_status(tx)` → `Confirmed` (optionally after a latency tick).
- `heartbeat()` → `Ok(())` no-op.

**Reuse:** emits the existing `OrderUpdate` enum (`domain.rs`), so `bankroll_task`,
`persistence_task`, and the `PositionState` machine (`state.rs`) work unchanged.

### 3. Fill model v1 — quote-crossing + latency
On each `order_status` poll for a resting order:
1. **Latency gate:** require `now - submitted_at >= SimConfig.fill_latency_ms` (default e.g.
   250–500ms) — models submit→ack→queue delay and removes the "instant fill" advantage.
2. **Cross check** against the live outcome price from `OutcomeBookCache` for the token
   (`book_cache.price(token_id, side)` — `Buy → buy_price`/ask, `Sell → sell_price`/bid):
   - BUY fills when `buy_price (ask) <= limit_price`; SELL fills when
     `sell_price (bid) >= limit_price`. If the cache has no price yet → stay resting.
3. On fill: `avg_price = limit_price` (conservative taker), `size_matched = shares` (no
   partials in v1), apply **maker/taker fees + tick-size rounding identical to live**
   (mirror whatever `ClobMarketClient::place_order` does — confirm exact fee/tick rules in
   `adapters/src/clob_market_client.rs`).
4. Otherwise stay `Submitted` (still resting) — naturally models orders that never fill
   before the round's `TradingCutoff`.

`SimConfig` (env-driven): `virtual_bankroll`, `fill_latency_ms`, `taker_fee_bps`,
`always_fill` (debug escape hatch), `dryrun_db_path`.

### 4. Running without a funded wallet
Dry-run should run **without real secrets / a funded Safe**. Post-PR #9 the marketable
price already arrives over the **public `PolymarketMarketFeed` websocket** (no signing), so
the fill model needs no credentials — it just reads `OutcomeBookCache`. Confirm
`PolymarketMarketFeed::connect(ids)` is unauthenticated. The only methods that would
otherwise need secrets — `place_order`/`cancel_order`/`redeem`/`balance` — are all
overridden by `SimMarketClient`, so no wallet/key is required in dry-run.

### 5. Persistence isolation
Dry-run writes to a **separate SQLite file** (e.g. `pm-bot.dryrun.sqlite`) via
`db_path_for(mode)`, so the audit trail is preserved but never mixed with live PnL.
`MockStore` remains the choice for unit tests.

### 6. Recorder (decorators) — builds the Phase-2 dataset for free
Because dry-run already runs the live feeds, capture them now. The **primary** stream to
record is the new `MarketDataFeed` (the real Polymarket outcome prices the fill model and
backtest both depend on):
- `RecordingMarketDataFeed { inner: Box<dyn MarketDataFeed>, sink }` → passes
  `next_update()` through and appends each `OutcomeBook` to the sink. Wrap it inside the
  `connect` closure in `main.rs`:
  `|ids| Box::new(RecordingMarketDataFeed::new(PolymarketMarketFeed::connect(ids), sink))`.
- Also `RecordingPriceFeed` (wraps `PriceFeed::next_tick` → `Tick`) and `RecordingCatalog`
  (wraps `MarketCatalog::resolve` → market metadata + strike/resolve times).
- Sink: a new `MarketDataRecorder` port + SQLite tables (`outcome_books`, `ticks`,
  `markets`) — or JSONL/parquet keyed by session id. Reuse `Timestamp` (ms) as the
  ordering key.
- Gate with `RECORD_SESSION=1` so it can run in both live and dry-run.
- Pure decorator pattern — transparent to `market_data_task` / downstream tasks.

### 7. (Stretch) Shadow logging
Since dry-run sees real quotes, log every predicted fill (price/latency). Later this lets
you compare predicted vs. actual fills if you ever run live alongside. Note only; defer.

---

## Phase 2 — Historical Simulation (DESIGN ONLY — do not implement now)

> **Not to be built in this pass.** Forward-looking design only, captured so Phase 1 is
> built with it in mind. Do **not** write any History-Simulation code yet.

The large lift, gated on the **time abstraction**.

### A. `Clock` trait (the core refactor)
```text
trait Clock: Send + Sync {
    fn now_ms(&self) -> Timestamp;
    async fn sleep(&self, d: Duration);
    async fn sleep_until(&self, t: Timestamp);
}
```
- `SystemClock` — wraps `SystemTime::now` + `tokio::time` (today's behavior).
- `SimClock` — atomic virtual `now`, advanced by the replay driver; `sleep*` registers a
  waker that fires when virtual time passes the target.
- **Call sites to migrate:** `types.rs` (`Timestamp::now_ms` — delegate to an injected clock
  rather than `SystemTime` directly), `tasks/market_rotation.rs` (`now_ms` + several
  `tokio::time::sleep`), `tasks/price_feed.rs` (retry sleep), `apps/pm-bot/src/main.rs`
  (startup window sleep). Audit-only `now_ms` in `executor.rs`/`settlement.rs` can move too
  for determinism.

### B. Replay adapters
`ReplayMarketDataFeed` / `ReplayPriceFeed` / `ReplayCatalog` read the **recorded dataset**
(from Phase 1 §6) and emit events in timestamp order, each advancing `SimClock` to the
event time. `ReplayMarketDataFeed` slots straight into the `connect` closure, so
`market_data_task` populates the same `OutcomeBookCache` from history with zero change.

### C. Reuse the same executor
`SimMarketClient` is reused **unchanged** — it already reads crossing from
`OutcomeBookCache`, which in Phase 2 is fed by `ReplayMarketDataFeed` instead of the live
websocket. Settlement also works unchanged: it derives the resolution price from cached
ticks + strike, both supplied by the replay feeds/catalog.

### D. Backtest harness
A `pm-explorer` (or new `pm-backtest`) subcommand: load a recorded session, run the full
task graph under `SimClock` + replay adapters + `SimMarketClient`, then report
fills/PnL/win-rate from the dry-run/sim `Store`.

---

## Files to create / modify (Phase 1)

**Create**
- `adapters/src/sim_market_client.rs` — `SimMarketClient` + fill model, reads `OutcomeBookCache`.
- `adapters/src/recording_feeds.rs` — `RecordingMarketDataFeed` / `RecordingPriceFeed` /
  `RecordingCatalog` decorators + `MarketDataRecorder` impl.
- `libs/pm-core/src/config.rs` (or extend `types.rs`) — `ExecutionMode`, `SimConfig`.

**Modify**
- `libs/pm-core/src/ports.rs` — add `MarketDataRecorder` port.
- `apps/pm-bot/src/main.rs` — mode branch for adapter construction; **re-enable
  `executor_task`** (commented out on main) wired with `client`; wrap the `connect` closure
  with `RecordingMarketDataFeed` when recording; seed virtual bankroll; `db_path_for(mode)`.
- `adapters/src/lib.rs` — export new adapters.

**Reused as-is (no change):** `market_data_task` + `OutcomeBookCache`, all other tasks in
`libs/pm-core/src/tasks/*`, `decision_center` (already cache-based),
`Strategy`/`SizingModel`/`EntryPolicy`, `PositionState`/`RoundSlotState`/`BankrollState`,
`OrderUpdate` events, `MockStore`.

---

## Verification

- **Unit tests** (`sim_market_client.rs`): order rests until the cached outcome price
  crosses; no fill before `fill_latency_ms`; BUY/SELL cross conditions (drive a stub
  `OutcomeBookCache`); cancel releases reservation; fee/tick rounding matches a live golden
  case. `RecordingMarketDataFeed` passes `OutcomeBook`s through and writes exactly one row
  per update.
- **End-to-end dry-run smoke:** `EXECUTION_MODE=dry-run RECORD_SESSION=1 cargo run -p pm-bot`
  for a few 5m rounds. Assert: (1) **zero** real CLOB order/redeem calls, (2) the executor
  actually runs (re-enabled) and positions land in `pm-bot.dryrun.sqlite` with sane
  `avg_price`/PnL once the live `OutcomeBookCache` price crosses the limit, (3)
  `BankrollState` reserves on place_order and settles on win/loss, (4) recorder tables fill
  with `outcome_books`/`ticks`/`markets`.
- **Regression:** `cargo test` (workspace) + `cargo build` green; existing live path
  unchanged when `EXECUTION_MODE` unset.

## Out of scope (v1) / known trade-offs
- No partial fills, no queue-position modeling (no Polymarket CLOB depth data). No simulated
  network drops/disconnects/rate-limits. No oracle-latency modeling for settlement. These
  are revisited after v1 once the recorder gives real data to calibrate against.

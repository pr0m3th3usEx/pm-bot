//! Shared start-up wiring reused by the `pm-bot` supervisor and the
//! `pm-recover-settlements` maintenance binary.

use std::str::FromStr;
use std::sync::Arc;

use pm_core::config::{ExecutionMode, SimConfig};
use pm_core::ports::{MarketCatalog, MarketClient, Store};
use pm_core::state::OutcomeBookCache;
use pm_core::types::Usdc;
use tokio::sync::RwLock;
use tracing::info;

/// SQLite path for the given execution mode.
pub fn db_path_for(mode: ExecutionMode, sim_cfg: &SimConfig) -> String {
    match mode {
        ExecutionMode::Live => "pm-bot.db".to_owned(),
        ExecutionMode::DryRun => sim_cfg.dryrun_db_path.clone(),
    }
}

/// Open the SQLite store for the given mode. Panics on failure (start-up invariant).
pub fn open_store(mode: ExecutionMode, sim_cfg: &SimConfig) -> Arc<dyn Store> {
    let db_path = db_path_for(mode, sim_cfg);
    Arc::new(
        adapters::sqlite_store::SqliteStore::open(&db_path).expect("failed to open SQLite store"),
    )
}

/// Build the Gamma market catalog.
pub fn build_catalog() -> Arc<dyn MarketCatalog> {
    Arc::new(adapters::gamma_market_catalog::GammaMarketCatalog::new())
}

/// Build the mode-appropriate market client and report the starting bankroll.
///
/// - `DryRun` → `SimMarketClient` seeded with the virtual bankroll.
/// - `Live`   → authenticated `ClobMarketClient` (reads secrets from env), balance fetched on-chain.
pub async fn build_client(
    mode: ExecutionMode,
    sim_cfg: &SimConfig,
    book_cache: Arc<RwLock<OutcomeBookCache>>,
) -> anyhow::Result<(Arc<dyn MarketClient>, Usdc)> {
    match mode {
        ExecutionMode::DryRun => {
            info!(
                virtual_bankroll = %sim_cfg.virtual_bankroll.0,
                fill_latency_ms = sim_cfg.fill_latency_ms,
                taker_fee_bps = sim_cfg.taker_fee_bps,
                always_fill = sim_cfg.always_fill,
                "[dry-run] building SimMarketClient"
            );
            let bankroll = sim_cfg.virtual_bankroll.clone();
            let sim_client = Arc::new(adapters::sim_market_client::SimMarketClient::new(
                book_cache,
                sim_cfg.clone(),
            ));
            Ok((sim_client, bankroll))
        }
        ExecutionMode::Live => {
            let private_key =
                std::env::var("POLYGON_PRIVATE_KEY").expect("POLYGON_PRIVATE_KEY must be set");
            let relayer_api_key =
                std::env::var("RELAYER_API_KEY").expect("RELAYER_API_KEY must be set");
            let rpc_url = std::env::var("POLYGON_RPC_URL").expect("POLYGON_RPC_URL must be set");

            use adapters::clob_market_client::{ClobMarketClient, CLOB_API_URL};
            use polymarket_client_sdk_v2::auth::{LocalSigner, Signer as PmSigner};
            use polymarket_client_sdk_v2::clob::types::SignatureType;
            use polymarket_client_sdk_v2::clob::{Client as ClobClient, Config};
            use polymarket_client_sdk_v2::{derive_safe_wallet, POLYGON};

            let signer = LocalSigner::from_str(&private_key)
                .expect("error with local signer")
                .with_chain_id(Some(POLYGON));

            let clob_client = ClobClient::new(CLOB_API_URL, Config::default())
                .expect("error build clob client")
                .authentication_builder(&signer)
                .signature_type(SignatureType::GnosisSafe)
                .authenticate()
                .await
                .expect("error authenticating clob client");

            let safe_address = derive_safe_wallet(clob_client.address(), POLYGON)
                .expect("error deriving safe wallet address");

            let live_client: Arc<dyn MarketClient> = Arc::new(ClobMarketClient::new(
                clob_client,
                signer,
                safe_address,
                relayer_api_key,
                rpc_url,
            ));

            let starting = live_client
                .balance()
                .await
                .expect("failed to fetch starting balance");
            info!(balance = %starting.0, "starting USDC balance");
            Ok((live_client, starting))
        }
    }
}

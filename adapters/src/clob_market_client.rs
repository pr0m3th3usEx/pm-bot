use alloy::{
    primitives::{hex as alloy_hex, Address, B256, U256},
    sol,
    sol_types::{eip712_domain, SolCall, SolStruct},
};
use async_trait::async_trait;
use pm_core::{
    domain::{Intent, OrderUpdate, PositionRecord},
    error::{CoreError, Result},
    ports::MarketClient,
    types::{Price, Shares, Side, TokenId, Usdc},
};
use polymarket_client_sdk_v2::{
    auth::{state::Authenticated, Kind, Signer as PmSigner},
    clob::{
        types::{request::PriceRequest, OrderStatusType},
        Client as ClobClient,
    },
};
use serde::Deserialize;

pub const CLOB_API_URL: &str = "https://clob.polymarket.com";
const RELAYER_URL: &str = "https://relayer-v2.polymarket.com";
const COLLATERAL_ADAPTER: &str = "0xAdA100Db00Ca00073811820692005400218FcE1f";
const COLLATERAL_TOKEN: &str = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB";
const POLYGON_CHAIN_ID: u64 = 137;

sol! {
    function redeemPositions(
        address collateralToken,
        bytes32 parentCollectionId,
        bytes32 conditionId,
        uint256[] indexSets
    );

    #[derive(Debug)]
    struct SafeTx {
        address to;
        uint256 value;
        bytes   data;
        uint8   operation;
        uint256 safeTxGas;
        uint256 baseGas;
        uint256 gasPrice;
        address gasToken;
        address refundReceiver;
        uint256 nonce;
    }
}

#[derive(Deserialize)]
struct RelayerNonceResponse {
    nonce: String,
}

#[derive(Deserialize)]
struct RelayerSubmitResponse {
    state: String,
    #[serde(rename = "transactionID")]
    transaction_id: Option<String>,
}

pub struct ClobMarketClient<K, Sgn>
where
    K: Kind + Send + Sync,
    Sgn: PmSigner + Send + Sync,
{
    client: ClobClient<Authenticated<K>>,
    signer: Sgn,
    safe_address: Address,
    http: reqwest::Client,
    relayer_api_key: String,
}

impl<K, Sgn> ClobMarketClient<K, Sgn>
where
    K: Kind + Send + Sync,
    Sgn: PmSigner + Send + Sync,
{
    pub fn new(
        client: ClobClient<Authenticated<K>>,
        signer: Sgn,
        safe_address: Address,
        relayer_api_key: String,
    ) -> Self {
        Self {
            client,
            signer,
            safe_address,
            http: reqwest::Client::new(),
            relayer_api_key,
        }
    }
}

#[async_trait]
impl<K, Sgn> MarketClient for ClobMarketClient<K, Sgn>
where
    K: Kind + Send + Sync,
    Sgn: PmSigner + Send + Sync,
{
    async fn quote(&self, token_id: &TokenId, side: Side) -> Result<Price> {
        let request = PriceRequest::builder()
            .token_id(token_id.0)
            .side(side.into())
            .build();

        let response = self
            .client
            .price(&request)
            .await
            .map_err(|e| CoreError::Adapter(format!("CLOB price request failed: {e}")))?;

        Ok(Price(response.price))
    }

    async fn place_order(&self, intent: &Intent, token_id: &TokenId) -> Result<String> {
        let order = self
            .client
            .limit_order()
            .price(intent.limit_price.0)
            .side(intent.side.into())
            .size(intent.shares.0)
            .token_id(token_id.0)
            .build()
            .await
            .map_err(|e| CoreError::Adapter(format!("CLOB order build failed: {e}")))?;

        let signed_order = self
            .client
            .sign(&self.signer, order)
            .await
            .map_err(|e| CoreError::Adapter(format!("CLOB order sign failed: {e}")))?;

        let response = self
            .client
            .post_order(signed_order)
            .await
            .map_err(|e| CoreError::Adapter(format!("CLOB post_order failed: {e}")))?;

        Ok(response.order_id)
    }

    async fn cancel_order(&self, order_id: &str) -> Result<()> {
        self.client
            .cancel_order(order_id)
            .await
            .map_err(|e| CoreError::Adapter(format!("CLOB cancel_order failed: {e}")))?;

        Ok(())
    }

    async fn order_status(&self, order_id: &str, position_id: i64) -> Result<OrderUpdate> {
        let order = self
            .client
            .order(order_id)
            .await
            .map_err(|e| CoreError::Adapter(format!("CLOB order fetch failed: {e}")))?;
        match order.status {
            OrderStatusType::Live | OrderStatusType::Delayed => Ok(OrderUpdate::Submitted {
                order_id: order_id.to_owned(),
                position_id,
            }),
            OrderStatusType::Matched => Ok(OrderUpdate::Filled {
                order_id: order_id.to_owned(),
                avg_price: Price(order.price),
                size_matched: Shares(order.size_matched),
                position_id,
            }),
            OrderStatusType::Canceled => Ok(OrderUpdate::Cancelled {
                order_id: order_id.to_owned(),
                position_id,
            }),
            OrderStatusType::Unmatched => Ok(OrderUpdate::Rejected {
                order_id: order_id.to_owned(),
                reason: None,
                position_id,
            }),
            _ => Err(CoreError::Adapter(format!(
                "CLOB order {order_id}: unhandled status {:?}",
                order.status
            ))),
        }
    }

    async fn redeem(&self, position: &PositionRecord) -> Result<Usdc> {
        let adapter: Address = COLLATERAL_ADAPTER
            .parse()
            .map_err(|e| CoreError::Adapter(format!("bad adapter address: {e}")))?;
        let collateral: Address = COLLATERAL_TOKEN
            .parse()
            .map_err(|e| CoreError::Adapter(format!("bad collateral token address: {e}")))?;

        // 1. Encode redeemPositions calldata.
        let calldata = redeemPositionsCall {
            collateralToken: collateral,
            parentCollectionId: B256::ZERO,
            conditionId: position.condition_id,
            indexSets: vec![U256::from(1u64), U256::from(2u64)],
        }
        .abi_encode();

        let signer_address = PmSigner::address(&self.signer);

        // 2. GET nonce from relayer.
        let nonce_resp: RelayerNonceResponse = self
            .http
            .get(format!("{RELAYER_URL}/v1/account/transactions/params"))
            .query(&[
                ("address", signer_address.to_checksum(None)),
                ("type", "SAFE".to_owned()),
            ])
            .send()
            .await
            .map_err(|e| CoreError::Adapter(format!("relayer nonce request failed: {e}")))?
            .json()
            .await
            .map_err(|e| CoreError::Adapter(format!("relayer nonce parse failed: {e}")))?;

        let nonce_val: U256 = nonce_resp
            .nonce
            .parse()
            .map_err(|e| CoreError::Adapter(format!("bad nonce '{}': {e}", nonce_resp.nonce)))?;

        // 3. Build EIP-712 SafeTx hash.
        let domain = eip712_domain! {
            chain_id: POLYGON_CHAIN_ID,
            verifying_contract: self.safe_address,
        };

        let safe_tx = SafeTx {
            to: adapter,
            value: U256::ZERO,
            data: calldata.clone().into(),
            operation: 0,
            safeTxGas: U256::ZERO,
            baseGas: U256::ZERO,
            gasPrice: U256::ZERO,
            gasToken: Address::ZERO,
            refundReceiver: Address::ZERO,
            nonce: nonce_val,
        };

        let signing_hash: B256 = safe_tx.eip712_signing_hash(&domain);

        tracing::debug!(
            signing_hash = %signing_hash,
            signer = %signer_address,
            safe = %self.safe_address,
            nonce = %nonce_resp.nonce,
            "computed EIP-712 SafeTx signing hash"
        );

        // 4. Sign the hash via personal_sign (EIP-191).
        // The relayer/Safe verify SAFE transactions through the `eth_sign` branch:
        // they recover from keccak256("\x19Ethereum Signed Message:\n32" || safeTxHash).
        // So we must sign the *prefixed* hash, not the raw hash. `sign_message`
        // applies eip191_hash_message (the personal_sign prefix) before signing.
        // This mirrors the TS SDK, which signs via viem `signMessage({ raw })`.
        let sig = PmSigner::sign_message(&self.signer, signing_hash.as_slice())
            .await
            .map_err(|e| CoreError::Adapter(format!("signing failed: {e}")))?;

        // 5. Pack signature — Safe eth_sign convention adds 4 to v (27->31, 28->32),
        // which tells checkSignatures to use the personal_sign recovery branch.
        let (r, s) = (sig.r(), sig.s());
        let v_raw = if sig.v() { 28u8 } else { 27u8 };
        let packed_v = v_raw + 4;
        let packed_sig = format!(
            "0x{}{}{:02x}",
            alloy_hex::encode(r.to_be_bytes::<32>()),
            alloy_hex::encode(s.to_be_bytes::<32>()),
            packed_v
        );

        // Self-verify: recovering from the EIP-191-prefixed hash must yield our signer.
        match sig.recover_address_from_msg(signing_hash.as_slice()) {
            Ok(recovered) => {
                tracing::debug!(
                    recovered = %recovered,
                    expected  = %signer_address,
                    matches   = %(recovered == signer_address),
                    signature = %packed_sig,
                    "signature self-check (personal_sign)"
                );
                if recovered != signer_address {
                    tracing::error!(
                        recovered = %recovered,
                        expected  = %signer_address,
                        "BUG: recovered address does not match signer — EIP-712 hash is wrong"
                    );
                }
            }
            Err(e) => tracing::error!(error = %e, "signature recovery failed"),
        }

        // 6. POST to relayer.
        let body = serde_json::json!({
            "type":        "SAFE",
            "from":        format!("{signer_address:#x}"),
            "to":          COLLATERAL_ADAPTER,
            "data":        format!("0x{}", alloy_hex::encode(&calldata)),
            "nonce":       nonce_resp.nonce,
            "proxyWallet": format!("{:#x}", self.safe_address),
            "signature":   packed_sig,
            "signatureParams": {
                "baseGas":       "0",
                "gasPrice":      "0",
                "gasToken":      "0x0000000000000000000000000000000000000000",
                "operation":     "0",
                "refundReceiver":"0x0000000000000000000000000000000000000000",
                "safeTxnGas":    "0"
            }
        });

        let submit_raw = self
            .http
            .post(format!("{RELAYER_URL}/submit"))
            .header("RELAYER_API_KEY", &self.relayer_api_key)
            .header("RELAYER_API_KEY_ADDRESS", format!("{signer_address:#x}"))
            .json(&body)
            .send()
            .await
            .map_err(|e| CoreError::Adapter(format!("relayer submit request failed: {e}")))?;

        if !submit_raw.status().is_success() {
            let status = submit_raw.status();
            let body_text = submit_raw.text().await.unwrap_or_default();
            tracing::error!(
                status = %status,
                body = %body_text,
                request_body = %serde_json::to_string(&body).unwrap_or_default(),
                "relayer submit rejected"
            );
            return Err(CoreError::Adapter(format!(
                "relayer submit rejected: {status} — {body_text}"
            )));
        }

        let resp: RelayerSubmitResponse = submit_raw
            .json()
            .await
            .map_err(|e| CoreError::Adapter(format!("relayer submit response parse failed: {e}")))?;

        tracing::info!(
            state = %resp.state,
            transaction_id = ?resp.transaction_id,
            "redeemPositions submitted to relayer"
        );

        // 7. Return gross payout: 1 USDC per winning share.
        Ok(Usdc(position.shares.0))
    }

    async fn heartbeat(&self) -> Result<()> {
        self.client
            .post_heartbeat(None)
            .await
            .map_err(|e| CoreError::Adapter(format!("CLOB heartbeat failed: {e}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::ClobMarketClient;
    use pm_core::{
        domain::OrderUpdate,
        ports::MarketClient,
        types::{Side, TokenId},
    };
    use polymarket_client_sdk_v2::{
        auth::{LocalSigner, Signer as _},
        clob::{types::SignatureType, Client as ClobClient, Config},
        derive_safe_wallet,
        types::U256,
        POLYGON,
    };
    use std::str::FromStr;

    async fn build_client() -> Option<Box<dyn MarketClient>> {
        let Ok(raw_key) = std::env::var("POLYGON_PRIVATE_KEY") else {
            println!("[SKIP] POLYGON_PRIVATE_KEY not set");
            return None;
        };
        let Ok(relayer_api_key) = std::env::var("RELAYER_API_KEY") else {
            println!("[SKIP] RELAYER_API_KEY not set");
            return None;
        };
        let signer = LocalSigner::from_str(&raw_key)
            .expect("invalid POLYGON_PRIVATE_KEY")
            .with_chain_id(Some(POLYGON));
        let clob = ClobClient::new("https://clob.polymarket.com", Config::default())
            .expect("failed to build ClobClient")
            .authentication_builder(&signer)
            .signature_type(SignatureType::GnosisSafe)
            .authenticate()
            .await
            .expect("CLOB authentication failed");
        let safe = derive_safe_wallet(clob.address(), POLYGON)
            .expect("failed to derive safe wallet address");
        Some(Box::new(ClobMarketClient::new(clob, signer, safe, relayer_api_key)))
    }

    #[tokio::test]
    async fn heartbeat_succeeds() {
        let Some(client) = build_client().await else {
            return;
        };
        client.heartbeat().await.expect("heartbeat failed");
    }

    #[tokio::test]
    async fn quote_buy_side_returns_valid_price() {
        let Some(client) = build_client().await else {
            return;
        };
        let Ok(raw_id) = std::env::var("POLYMARKET_TOKEN_ID") else {
            println!("[SKIP] POLYMARKET_TOKEN_ID not set");
            return;
        };
        let token_id = TokenId(U256::from_str(&raw_id).expect("invalid POLYMARKET_TOKEN_ID"));
        let price = client
            .quote(&token_id, Side::Buy)
            .await
            .expect("quote(Buy) failed");
        assert!(
            price.0 > rust_decimal::Decimal::ZERO,
            "price must be > 0, got {:?}",
            price
        );
        assert!(
            price.0 <= rust_decimal::Decimal::ONE,
            "price must be <= 1, got {:?}",
            price
        );
    }

    #[tokio::test]
    async fn quote_sell_side_returns_valid_price() {
        let Some(client) = build_client().await else {
            return;
        };
        let Ok(raw_id) = std::env::var("POLYMARKET_TOKEN_ID") else {
            println!("[SKIP] POLYMARKET_TOKEN_ID not set");
            return;
        };
        let token_id = TokenId(U256::from_str(&raw_id).expect("invalid POLYMARKET_TOKEN_ID"));
        let price = client
            .quote(&token_id, Side::Sell)
            .await
            .expect("quote(Sell) failed");
        assert!(price.0 > rust_decimal::Decimal::ZERO);
        assert!(price.0 <= rust_decimal::Decimal::ONE);
    }

    #[tokio::test]
    async fn order_status_returns_known_update() {
        let Some(client) = build_client().await else {
            return;
        };
        let Ok(order_id) = std::env::var("POLYMARKET_ORDER_ID") else {
            println!("[SKIP] POLYMARKET_ORDER_ID not set");
            return;
        };
        let update = client
            .order_status(&order_id, 0)
            .await
            .expect("order_status failed");
        let (embedded_order_id, embedded_pos_id) = match &update {
            OrderUpdate::Submitted {
                order_id,
                position_id,
            } => (order_id, position_id),
            OrderUpdate::Filled {
                order_id,
                position_id,
                ..
            } => (order_id, position_id),
            OrderUpdate::Rejected {
                order_id,
                position_id,
                ..
            } => (order_id, position_id),
            OrderUpdate::Cancelled {
                order_id,
                position_id,
            } => (order_id, position_id),
        };
        assert_eq!(embedded_order_id, &order_id);
        assert_eq!(*embedded_pos_id, 0i64);
    }
}

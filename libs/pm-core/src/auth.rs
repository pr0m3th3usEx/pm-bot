use std::str::FromStr;
use polymarket_client_sdk_v2::POLYGON;
use polymarket_client_sdk_v2::auth::{ApiKey, LocalSigner, Signer};
use polymarket_client_sdk_v2::clob::{Client, Config};
use polymarket_client_sdk_v2::types::Address;

pub async fn authenticate_user(private_key: &str) -> Result<(ApiKey, Address), anyhow::Error> {
    if private_key.is_empty() {
        return Err(anyhow::anyhow!("Private key cannot be empty"));
    }

    let signer = LocalSigner::from_str(&private_key)?.with_chain_id(Some(POLYGON));

    // Creates new credentials or derives existing ones,
    // then initializes the authenticated client — all in one step
    let client = Client::new("https://clob.polymarket.com", Config::default())?
        .authentication_builder(&signer)
        .authenticate()
        .await?;

    let credentials = client.credentials();

    Ok((credentials.key(), client.address()))
}

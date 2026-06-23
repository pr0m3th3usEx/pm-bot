use polymarket_client_sdk_v2::gamma::{Client as GammaClient, types::request::{MarketBySlugRequest, MarketsRequest, PublicProfileRequest, PublicProfileRequestBuilder}};

// Generate the next 5m candle timestamp based on the current time
fn generate_next_5m_candle_timestamp() -> u64 {
    let now = std::time::SystemTime::now();
    let duration_since_epoch = now.duration_since(std::time::UNIX_EPOCH).unwrap();
    let current_timestamp = duration_since_epoch.as_secs();

    // Calculate the next 5-minute interval
    let next_5m_interval = ((current_timestamp / 300) + 1) * 300;
    next_5m_interval
}


#[tokio::main]
async fn main() {
    let next_5m_candle_timestamp = generate_next_5m_candle_timestamp();
    println!("Next 5-minute candle timestamp: {}", next_5m_candle_timestamp);
    println!("Previous 5-minute candle timestamp: {}", next_5m_candle_timestamp - 300);

    // // Retrieve the private key from environment variables
    // let private_key = std::env::var("POLYGON_PRIVATE_KEY")
    //     .expect("POLYGON_PRIVATE_KEY must be set in the .env file");

    // // Authenticate the user and obtain the API key
    // let (api_key, address) = match pm_core::auth::authenticate_user(&private_key).await {
    //     Ok((api_key, address)) => (api_key, address),
    //     Err(e) => {
    //         eprintln!("Authentication failed: {}", e);
    //         return;
    //     }
    // };

    // // Get public profile data
    // let gamma_client = GammaClient::new("https://gamma-api.polymarket.com")
    //     .expect("Failed to create Gamma client");

    // let profile_request = PublicProfileRequest::builder()
    //     .address(address)
    //     .build();

    // let profile = gamma_client.public_profile(&profile_request).await
    // .expect("error loading profile");

    // println!("Public Profile: {:?}", profile);

    // // Get BTC 5 minute price data
    // let btc_price_request = MarketBySlugRequest::builder()
    //     .slug(format!("btc-updown-5m-{}", "1781423100"))
    //     .build();

    // let btc_price = gamma_client.market_by_slug(&btc_price_request).await
    //     .expect("error loading BTC price data");

    // println!("BTC 5 Minute Price Data: {:#?}", btc_price);
}

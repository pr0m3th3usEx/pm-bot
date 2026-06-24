use polymarket_client_sdk_v2::auth::{LocalSigner, Signer};
use polymarket_client_sdk_v2::clob::order_builder::OrderBuilder;
use polymarket_client_sdk_v2::clob::types::request::PriceRequest;
use polymarket_client_sdk_v2::clob::types::OrderStatusType;
use polymarket_client_sdk_v2::clob::types::Side::{self, Buy};
use polymarket_client_sdk_v2::clob::types::SignatureType;
use polymarket_client_sdk_v2::clob::{Client as ClobClient, Config};
use polymarket_client_sdk_v2::gamma::types::request::EventBySlugRequest;
use polymarket_client_sdk_v2::gamma::{
    types::request::{MarketBySlugRequest, PublicProfileRequest, PublicProfileRequestBuilder},
    Client as GammaClient,
};
use polymarket_client_sdk_v2::types::{dec, Decimal};
use polymarket_client_sdk_v2::{derive_proxy_wallet, derive_safe_wallet, POLYGON};
use std::str::FromStr;

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
    println!(
        "Next 5-minute candle timestamp: {}",
        next_5m_candle_timestamp
    );
    println!(
        "Current 5-minute candle timestamp: {}",
        next_5m_candle_timestamp - 300
    );

    let current_5m_candle_timestamp = next_5m_candle_timestamp - 300;

    // Retrieve the private key from environment variables
    let private_key = std::env::var("POLYGON_PRIVATE_KEY")
        .expect("POLYGON_PRIVATE_KEY must be set in the .env file");

    // Authenticate the user and obtain the API key
    let signer = LocalSigner::from_str(&private_key)
        .expect("error with local signer")
        .with_chain_id(Some(POLYGON));

    // Creates new credentials or derives existing ones,
    // then initializes the authenticated client — all in one step
    let clob_client = ClobClient::new("https://clob.polymarket.com", Config::default())
        .expect("error build clob client")
        .authentication_builder(&signer)
        .signature_type(SignatureType::GnosisSafe)
        .authenticate()
        .await
        .expect("error authenticating clob client");

    let address = clob_client.address();

    // Print the authenticated user's safe address
    println!(
        "Authenticated user's proxy address: {:?}",
        derive_safe_wallet(address, POLYGON)
    );

    // Get public profile data
    let gamma_client = GammaClient::new("https://gamma-api.polymarket.com")
        .expect("Failed to create Gamma client");

    let profile_request = PublicProfileRequest::builder().address(address).build();

    let profile = gamma_client
        .public_profile(&profile_request)
        .await
        .expect("error loading profile");

    println!("Public Profile: {:?}", profile);

    // Try to trade a position "Up" on the BTC 5 minute market

    // Step 1: Get BTC 5 minute price data
    let btc_price_request = MarketBySlugRequest::builder()
        .slug(format!("btc-updown-5m-{}", current_5m_candle_timestamp))
        .build();

    let btc_price = gamma_client
        .market_by_slug(&btc_price_request)
        .await
        .expect("error loading BTC price data");

    // println!("BTC 5 Minute Price Data: {:#?}", btc_price);

    // Get price to beat in event
    let req = EventBySlugRequest::builder()
        .slug(format!("btc-updown-5m-{}", current_5m_candle_timestamp))
        .build();
    let res = gamma_client
        .event_by_slug(&req)
        .await
        .expect("error loading event data");

    // let Some(events) = btc_price.events else {
    //     panic!("Gamma API response missing events for market slug btc-updown-5m-{}", current_5m_candle_timestamp);
    // };

    // if events.len() != 1 {
    //     panic!("Gamma API response has unexpected number of events for market slug btc-updown-5m-{}: {}", current_5m_candle_timestamp, events.len());
    // }

    // // Step 2: Get the market ID, condition ID, question ID, outcome token ID for the "Up" position
    // let market_id = btc_price.id.clone();
    // let condition_id = btc_price.condition_id.expect("condition id missing");
    // let question_id = btc_price.question_id.expect("question id missing");

    // // Get yes position based on outcome names

    // let clobTokenIds = btc_price.clob_token_ids.expect("clobTokenIds missing");

    // let up_outcome_token_id = clobTokenIds.get(
    //     btc_price
    //         .outcomes
    //         .expect("outcomes missing")
    //         .iter()
    //         .enumerate()
    //         .find(|(_, outcome)| outcome.as_str() == "Up")
    //         .map(|(index, _)| index)
    //         .expect("Up outcome not found"),
    // )
    // .expect("Up outcome token ID missing");

    // println!(
    //     "Market ID: {}\nQuestion ID: {}\n\"Up\" Outcome Token ID: {:?}",
    //     market_id, question_id, up_outcome_token_id
    // );

    // // Step 3: Get price data for the "Up" position
    // let up_request_price_request = PriceRequest::builder()
    //     .token_id(up_outcome_token_id.clone())
    //     .side(Side::Buy)
    //     .build();

    // let up_price_data = clob_client
    //     .price(&up_request_price_request)
    //     .await
    //     .expect("error loading price data for Up position");

    // println!(
    //     "Price Data for \"Up\" Position: {:#?}",
    //     up_price_data
    // );

    // // Step 4: Place an order on the "Up" position at market price
    // let order = clob_client
    //     .limit_order()
    //     .price(up_price_data.price)
    //     .side(Side::Buy)
    //     .size(Decimal::new(5, 0))
    //     .token_id(up_outcome_token_id.clone())
    //     .build()
    //     .await
    //     .expect("error building order");

    // let signed_order = clob_client
    //     .sign(&signer, order)
    //     .await
    //     .expect("error signing order");

    // let order_response = clob_client
    //     .post_order(signed_order)
    //     .await
    //     .expect("error placing order");

    // println!("Order placed — ID: {}, status: {}", order_response.order_id, order_response.status);

    // // Step 5: Poll until the order reaches a terminal state
    // let order_id = &order_response.order_id;
    // loop {
    //     tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    //     let open_order = clob_client
    //         .order(order_id)
    //         .await
    //         .expect("error fetching order status");

    //     println!("Order status: {:?}", open_order.status);

    //     match open_order.status {
    //         OrderStatusType::Matched => {
    //             println!(
    //                 "Order matched — size_matched: {}, associate_trades: {:?}",
    //                 open_order.size_matched, open_order.associate_trades
    //             );
    //             break;
    //         }
    //         OrderStatusType::Canceled | OrderStatusType::Unmatched => {
    //             println!("Order ended without fill: {}", open_order.status);
    //             break;
    //         }
    //         // Live / Delayed — keep polling
    //         _ => {}
    //     }
    // }
}

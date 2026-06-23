
#[tokio::main]
async fn main() {
    // Retrieve the private key from environment variables
    let private_key = std::env::var("POLYGON_PRIVATE_KEY")
        .expect("POLYGON_PRIVATE_KEY must be set in the .env file");

    // Authenticate the user and obtain the API key
    match pm_core::auth::authenticate_user(&private_key).await {
        Ok(api_key) => println!("Authenticated successfully! API Key: {}", api_key),
        Err(e) => eprintln!("Authentication failed: {}", e),
    }
}

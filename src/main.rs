#[tokio::main]
async fn main() {
    let path = std::env::var("AIR_SECRET_FILE").expect("AIR_SECRET_FILE is required");
    let secret = serde_json::from_slice(&std::fs::read(path).expect("read server identity"))
        .expect("invalid server identity");
    air_server::Server::start(secret).await;
}

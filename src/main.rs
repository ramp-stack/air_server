#[tokio::main]
async fn main() {
    let path = std::env::var_os("AIR_STORAGE").unwrap_or_else(|| "STORAGE.db".into());
    let secret = air_server::Server::secret(std::path::Path::new(&path));
    air_server::Server::start(secret).await;
}

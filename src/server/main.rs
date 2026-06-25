use sync_rs::{init_tracing, transport::ServerFileProcessor};
use tokio::net::TcpListener;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    info!("Sync-RS Server");

    let addr = "0.0.0.0:6868";
    let listender = TcpListener::bind(addr).await?;
    let server = ServerFileProcessor::new();

    info!("creating upload folder on server");
    server.create_folder().await?;

    info!(
        "server working on addr: {}, ready to receive connection",
        addr
    );
    loop {
        let (stream, remote) = listender.accept().await?;
        let mut s_c = server.clone();

        info!("handle file stream for: {}", remote);
        match s_c.handle_file_stream(stream).await {
            Ok(_) => {}
            Err(e) => {
                warn!("failed handle file stream: {}", e);
            }
        }
    }
}

use sync_rs::{config::CHUNK_SIZE, init_tracing, transport::ClientFileProcessor};
use tokio::net::TcpStream;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<() >{
    init_tracing();

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
         anyhow::bail!("client need a file path");
    }

    info!("Sync-RS Client");
    info!("client will upload: {}", args[1]);
    let server_addr = "0.0.0.0:6868";
    let stream = TcpStream::connect(server_addr).await?;

    let mut client = ClientFileProcessor::new(&args[1]);
    let chunks = client.chunk_and_hash_file(CHUNK_SIZE).await?;

    info!("file has been chunked into {} chunks", chunks.len());
    info!("starting upload...");

    client.send_chunks(stream, chunks).await?;

    Ok(())
}
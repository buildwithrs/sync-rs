use sync_rs::errors::SyncClientError;
use sync_rs::protocol::{new_framed_reader, new_framed_writer};
use tokio::net::TcpStream;
use tokio_retry2::strategy::{ExponentialBackoff, MaxInterval, jitter};
use tokio_retry2::{Retry, RetryError};
use tracing::info;

use sync_rs::{config::CHUNK_SIZE, init_tracing, transport::ClientFileProcessor};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        anyhow::bail!("client need a file path");
    }

    info!("Sync-RS Client");
    info!("client will upload: {}", args[1]);
    let server_addr = "0.0.0.0:6868";

    let retry_strategy = ExponentialBackoff::from_millis(10)
        .factor(1) // multiplication factor applied to delay
        .max_delay_millis(100) // set max delay between retries to 500ms
        .max_interval(1000) // set max interval to 1 second for all retries
        .take(3) // limit to 3 retries
        .map(jitter);

    let stream = Retry::spawn(retry_strategy, || action(server_addr))
        .await
        .map_err(|_| SyncClientError::ConnectServerFailed)?;

    let mut client = ClientFileProcessor::new(&args[1]);
    let chunks = client.chunk_and_hash_file(CHUNK_SIZE).await?;

    info!("file has been chunked into {} chunks", chunks.len());
    info!("starting upload...");

    let (r, w) = stream.into_split();
    let mut fr = new_framed_reader(r);
    let mut fw = new_framed_writer(w);

    let init_resp = client.send_upload_init(&mut fr, &mut fw).await?;
    info!("received init resp: {:?}", init_resp);

    let _ = client.send_chunks(&mut fr, &mut fw, chunks).await?;
    info!("done send the chunks");

    let done_ack = client.send_upload_done(&mut fr, &mut fw).await?;
    info!("received upload done ack: {:?}", done_ack);

    Ok(())
}

async fn action(server_addr: &str) -> Result<TcpStream, RetryError<()>> {
    match TcpStream::connect(server_addr).await {
        Ok(s) => Ok(s),
        Err(e) => {
            eprintln!("failed connect to: {}, err: {}", server_addr, e);
            Err(RetryError::Transient {
                err: (),
                retry_after: None,
            })
        }
    }
}

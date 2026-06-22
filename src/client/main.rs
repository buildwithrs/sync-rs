use sync_rs::init_tracing;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<() >{
    init_tracing();

    info!("Sync-RS Client");
    Ok(())
}
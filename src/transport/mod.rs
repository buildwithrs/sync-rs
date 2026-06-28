pub mod client;
pub mod server;

use std::path::PathBuf;

use blake3::Hash;
use memmap2::Mmap;
use tokio::fs::File;

use crate::errors::SyncError;

pub use client::ClientFileProcessor;
pub use server::ServerFileProcessor;

pub async fn file_hash(path: &PathBuf) -> Result<Hash, SyncError> {
    let file = File::open(path).await?;
    let mmap = unsafe { Mmap::map(&file)? };
    Ok(blake3::hash(&mmap))
}

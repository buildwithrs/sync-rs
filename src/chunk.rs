
use bytes::Bytes;
use chunkrs::{Chunk, ChunkConfig, Chunker};
use tokio::{
    fs::{self},
};

use crate::errors::SyncError;

/// split the file into chunks
pub async fn split_file(file_path: &str) -> Result<Vec<Chunk>, SyncError> {
    let mut chunker = Chunker::new(ChunkConfig::default());

    let content = fs::read(file_path).await?;
    let bs = Bytes::from(content);

    let mut all_chunks= Vec::new();
    let (chunks, _leftover) = chunker.push(bs);
    all_chunks.extend_from_slice(chunks.as_slice());

    if let Some(final_chunk) = chunker.finish() {
        println!("Final chunk: {} bytes", final_chunk.len());
        all_chunks.push(final_chunk);
    }

    Ok(chunks)
}

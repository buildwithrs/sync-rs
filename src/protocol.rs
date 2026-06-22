use async_trait::async_trait;
use chunkrs::Chunk;

use crate::errors::SyncError;

/// send chunked file data stream in client side
#[async_trait]
pub trait SyncSendStream {
    async fn send(&mut self, chunks: Vec<Chunk>) -> Result<(), SyncError>;
}


/// send chunked file data stream in server side
#[async_trait]
pub trait SyncRecvStream {
    async fn recv(&mut self) -> Result<(), SyncError>;
}
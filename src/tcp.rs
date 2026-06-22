use async_trait::async_trait;
use chunkrs::Chunk;

use crate::{errors::SyncError, protocol::SyncSendStream};

pub struct TCPClient {}

#[async_trait]
impl SyncSendStream for TCPClient {
    async fn send(&mut self, chunks: Vec<Chunk>) -> Result<(), SyncError> {
        Ok(())
    }
}


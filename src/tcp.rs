use async_trait::async_trait;
use chunkrs::Chunk;
use tokio::net::TcpStream;

use crate::{errors::SyncError, protocol::SyncSendStream};

pub struct TCPClient {
    pub stream: TcpStream,
}

impl TCPClient {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
    }
}

#[async_trait]
impl SyncSendStream for TCPClient {
    async fn send(&mut self, chunks: Vec<Chunk>) -> Result<(), SyncError> {
        Ok(())
    }
}


use bao_tree::io::EncodeError;
use chunkrs::ChunkError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SyncError {
    #[error("file size exceed limit: {0}")]
    FileSizeTooLarge(usize),

     #[error("io error: {0}")]
    StdIOError(String),

    #[error("failed to send data: {0}")]
    IOError(#[from] tokio::io::Error),

    #[error("chunk error: {0}")]
    ChunkError(#[from] ChunkError),

    #[error("chunk data is broken: {0}")]
    BadChunkData(String),

    #[error("encode error: {0}")]
    BaoTreeEncodeError(#[from] EncodeError),

    #[error("uuid error: {0}")]
    UUidError(#[from] uuid::Error)
}
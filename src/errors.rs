use thiserror::Error;

#[derive(Debug, Error)]
pub enum SyncError {
    #[error("file size exceed limit: {0}")]
    FileSizeTooLarge(usize),

     #[error("io error: {0}")]
    StdIOError(String),

    #[error("failed to send data: {0}")]
    IOError(#[from] tokio::io::Error),

    #[error("chunk data is broken: {0}")]
    BadChunkData(String)
}
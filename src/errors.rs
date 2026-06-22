use thiserror::Error;
use tokio::io;

#[derive(Debug, Error)]
pub enum SyncError {
    #[error("file size exceed limit: {0}")]
    FileSizeTooLarge(usize),

    #[error("failed to send data: {0}")]
    IOError(#[from] io::Error)
}
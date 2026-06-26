use thiserror::Error;

use crate::protocol::ErrMsg;

#[derive(Debug, Error)]
pub enum SyncClientError {
    #[error("connect server fail")]
    ConnectServerFailed,
}

#[derive(Debug, Error)]
pub enum SyncError {
    #[error("file size exceed limit: {0}")]
    FileSizeTooLarge(usize),

    #[error("io error: {0}")]
    StdIOError(String),

    #[error("failed to send data: {0}")]
    IOError(#[from] tokio::io::Error),

    #[error("chunk data is broken: {0}")]
    BadChunkData(String),

    #[error("no chunks")]
    NoChunks,

    #[error("duplicate file: {0}")]
    DuplicateFile(String),

    #[error("file upload not init: {0}")]
    FileUploadNotInit(String),
}

pub const IO_ERRCODE: u16 = 1001;
pub const FILESIZE_EXCEED_ERRCODE: u16 = 1002;
pub const BADCHUNK_ERRCODE: u16 = 1003;
pub const DUPLICATE_FILE_ERRCODE: u16 = 1004;
pub const UPLOAD_NOT_INIT_CODE: u16 = 1005;
pub const NO_CHUNKS_CODE: u16 = 1006;

impl From<SyncError> for ErrMsg {
    fn from(err: SyncError) -> Self {
        match err {
            SyncError::IOError(e) => ErrMsg::new(IO_ERRCODE, &e.to_string()),
            SyncError::StdIOError(e) => ErrMsg::new(IO_ERRCODE, &e.to_string()),
            SyncError::FileSizeTooLarge(e) => ErrMsg::new(FILESIZE_EXCEED_ERRCODE, &e.to_string()),
            SyncError::BadChunkData(e) => ErrMsg::new(BADCHUNK_ERRCODE, &e.to_string()),
            SyncError::DuplicateFile(e) => ErrMsg::new(DUPLICATE_FILE_ERRCODE, &e.to_string()),
            SyncError::FileUploadNotInit(e) => ErrMsg::new(UPLOAD_NOT_INIT_CODE, &e.to_string()),
            SyncError::NoChunks => ErrMsg::new(NO_CHUNKS_CODE, "no file content chunks"),
        }
    }
}

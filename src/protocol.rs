use std::path::PathBuf;

use async_trait::async_trait;
use blake3::{Hash, hash, hazmat::{ChainingValue, HasherExt, merge_subtrees_non_root, merge_subtrees_root}};
use bytes::{BufMut, Bytes, BytesMut};
use chunkrs::{Chunk, ChunkHash};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use uuid::Uuid;

use crate::{chunk::SyncChunk, errors::SyncError};

/// send chunked file data stream in client side
#[async_trait]
pub trait SyncSendStream {
    async fn send(self, chunks: Vec<Chunk>) -> Result<(), SyncError>;
}

/// send chunked file data stream in server side
#[async_trait]
pub trait SyncRecvStream {
    async fn recv(&self) -> Result<(), SyncError>;
}

#[derive(Debug, Clone)]
pub enum FileUploadState {
    Init,
    Uploading,
    Done
}

#[derive(Debug, Clone)]
pub struct FileMeta {
    pub file_id: Uuid,
    pub file_path: PathBuf,
    pub total_size: usize,
    pub hash: Option<blake3::Hash>,
}

impl FileMeta {
    pub fn new(path: &str) -> Self {
        Self {
            file_id: uuid::Uuid::new_v4(),
            file_path: PathBuf::from(path),
            total_size: 0,
            hash: None,
        }
    }

    pub fn new1(file_id: Uuid, path: PathBuf, size: usize, hash: blake3::Hash) -> Self {
        Self {
            file_id,
            file_path: path,
            total_size: size,
            hash: Some(hash),
        }
    }
}

/*

### Message Types

| Tag | Name | Fields |
|----:|------|--------|
| 0x01 | `UploadInit` | `file_id: [u8;16]`, `file_size: u64`, `sha256: [u8;32]`, `filename: String` |
| 0x02 | `UploadInitAck` | `file_id: [u8;16]`, `resume_offset: u64` |
| 0x03 | `Chunk` | `file_id: [u8;16]`, `offset: u64`, `data: Bytes` |
| 0x04 | `ChunkAck` | `file_id: [u8;16]`, `offset: u64` |
| 0x05 | `UploadComplete` | `file_id: [u8;16]` |
| 0x06 | `UploadCompleteAck` | `file_id: [u8;16]`, `ok: bool`, `msg: String` |
| 0xFF | `Error` | `code: u8`, `msg: String` |
*/

pub const UPLOAD_INIT_TAG: u8 = 0x01;
pub const CHUNK_TAG: u8 = 0x03;
pub const UPLOAD_DONE_TAG: u8 = 0x05;
pub const ERR_TAG: u8 = 0xFF;

#[derive(Debug)]
pub struct UploadInitEvent {
    pub file_id: Uuid,
    pub size: u64,
    pub hash: blake3::Hash,
    pub name: String,
}

#[derive(Debug)]
pub struct ChunkEvent {
    pub file_id: Uuid,
    pub offset: u64,
    pub data: Bytes,
}

#[derive(Debug)]
pub struct ErrMsg {
    pub code: u8,
    pub msg: String,
}

#[derive(Debug)]
pub struct UploadDoneEvent {
    pub file_id: Uuid,
}

pub fn new_framed_writer(
    stream: OwnedWriteHalf,
) -> FramedWrite<OwnedWriteHalf, LengthDelimitedCodec> {
    FramedWrite::new(stream, LengthDelimitedCodec::default())
}

pub fn new_framed_reader(
    stream: OwnedReadHalf,
) -> FramedRead<OwnedReadHalf, LengthDelimitedCodec> {
    FramedRead::new(stream, LengthDelimitedCodec::default())
}

/// tag | file_id
pub fn encode_upload_done(d: UploadDoneEvent) -> BytesMut {
    let mut encode_bs = BytesMut::with_capacity(100);
    encode_bs.put_u8(UPLOAD_DONE_TAG); // 1 byte
    encode_bs.put_slice(&d.file_id.into_bytes()); // variable length
    encode_bs
}

/// tag(1) | file_id(16)
pub fn decode_upload_done(bs: &mut BytesMut) -> Result<UploadDoneEvent, SyncError> {
    if bs.len() < 16 {
        return Err(SyncError::BadChunkData(
            "chunk length must >= 16".to_string(),
        ));
    }

    let file_id = String::from_utf8_lossy(&bs[..16]);
    Ok(UploadDoneEvent { file_id: uuid::Uuid::parse_str(&file_id)? })
}


/// err_tag | code(1) | msg
pub fn encode_error(err_msg: ErrMsg) -> BytesMut {
    let mut encode_bs = BytesMut::with_capacity(100);
    encode_bs.put_u8(ERR_TAG); // 1 byte

    encode_bs.put_u8(err_msg.code); // 1 byte
    encode_bs.put_slice(&err_msg.msg.into_bytes()); // variable length
    encode_bs
}

/// code(1) | msg
pub fn decode_error(bs: &mut BytesMut) -> Result<ErrMsg, SyncError> {
   if bs.len() <= 1 {
        return Err(SyncError::BadChunkData(
            "chunk length must > 1".to_string(),
        ));
    }

    let code = u8::from_be_bytes(bs[..1].try_into().unwrap());
    let msg = String::from_utf8_lossy(&bs[1..]);

    Ok(ErrMsg { code, msg: msg.to_string() })
}

pub fn encode_chunk(chunk: Chunk) -> BytesMut {
    let offset = chunk.offset.unwrap_or(0);

    let mut encode_bs = BytesMut::with_capacity(100);
    encode_bs.put_u64(offset);

    let hash = chunk.hash.unwrap_or(ChunkHash::new([0u8; 32]));
    encode_bs.put_slice(hash.as_bytes());

    encode_bs.put_slice(&chunk.data);
    encode_bs
}

/// Upload Init Event
/// size(8) | field_id(16) | hash(32) | name
pub fn encode_upload_init(upload_init: UploadInitEvent) -> BytesMut {
    let size = upload_init.size;

    let mut encode_bs = BytesMut::with_capacity(100);

    encode_bs.put_u8(UPLOAD_INIT_TAG);

    encode_bs.put_u64(size); // 8 bytes
    encode_bs.put_slice(&upload_init.file_id.into_bytes()); // 16
    encode_bs.put_slice(upload_init.hash.as_bytes()); // 32 
    encode_bs.put_slice(&upload_init.name.into_bytes()); // variable length
    encode_bs
}

/// Chunk Event
/// offset(8) | field_id(uuid: 16) | data
pub fn encode_chunk_event(chunk: ChunkEvent) -> BytesMut {
    let offset = chunk.offset;

    let mut encode_bs = BytesMut::with_capacity(100);
    encode_bs.put_u8(CHUNK_TAG);

    encode_bs.put_u64(offset);
    encode_bs.put_slice(&chunk.file_id.into_bytes());
    encode_bs.put_slice(&chunk.data);
    encode_bs
}

pub fn decode_chunk(bs: Bytes) -> Result<Chunk, SyncError> {
    if bs.len() < 40 {
        return Err(SyncError::BadChunkData(
            "chunk length must >= 40".to_string(),
        ));
    }

    let offset = u64::from_be_bytes(bs[..8].try_into().unwrap());
    let hash = ChunkHash::from_slice(&bs[8..40]);

    let data = Bytes::from(bs.slice(40..));
    Ok(Chunk::with_offset(data, offset).set_hash(hash.unwrap()))
}

pub fn decode_chunk_event(bs: &mut BytesMut) -> Result<ChunkEvent, SyncError> {
    if bs.len() < 24 {
        return Err(SyncError::BadChunkData(
            "chunk length must >= 24".to_string(),
        ));
    }

    let offset = u64::from_be_bytes(bs[..8].try_into().unwrap());
    let file_id = String::from_utf8_lossy(&bs[8..24]);
    // let _ = bs.split_to(24);
    let data = Bytes::copy_from_slice(&bs[24..]);

    Ok(ChunkEvent {
        data,
        offset,
        file_id: uuid::Uuid::parse_str(&file_id)?,
    })
}

/// size(8) | field_id(16) | hash(32) | name
pub fn decode_upload_init(bs: &mut BytesMut) -> Result<UploadInitEvent, SyncError> {
    if bs.len() <= 56 {
        return Err(SyncError::BadChunkData(
            "chunk length must > 56".to_string(),
        ));
    }

    let file_size = u64::from_be_bytes(bs[..8].try_into().unwrap());
    let file_id = String::from_utf8_lossy(&bs[8..24]);
    // let _ = bs.split_to(24);
    let hash_bs = &bs[24..56];
    let mut hash_arr = [0u8; 32];
    hash_arr.copy_from_slice(hash_bs);

    let file_name = String::from_utf8_lossy(&bs[56..]);

    Ok(UploadInitEvent {
        size: file_size,
        hash: Hash::from_bytes(hash_arr),
        file_id: uuid::Uuid::parse_str(&file_id)?,
        name: file_name.to_string(),
    })
}

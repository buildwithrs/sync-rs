use async_trait::async_trait;
use blake3::hazmat::{ChainingValue, HasherExt, merge_subtrees_non_root, merge_subtrees_root};
use bytes::{BufMut, Bytes, BytesMut};
use chunkrs::{Chunk, ChunkHash};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio_util::codec::{FramedWrite, LengthDelimitedCodec};
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

pub fn new_framed_writer(
    stream: OwnedWriteHalf,
) -> FramedWrite<OwnedWriteHalf, LengthDelimitedCodec> {
    FramedWrite::new(stream, LengthDelimitedCodec::default())
}

pub fn new_framed_reader(
    stream: OwnedReadHalf,
) -> FramedWrite<OwnedReadHalf, LengthDelimitedCodec> {
    FramedWrite::new(stream, LengthDelimitedCodec::default())
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
/// size | field_id | hash | name
pub fn encode_upload_init(upload_init: UploadInitEvent) -> BytesMut {
    let size = upload_init.size;

    let mut encode_bs = BytesMut::with_capacity(100);

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

pub fn decode_sync_chunk(bs: Bytes) -> Result<SyncChunk, SyncError> {
    if bs.len() < 24 {
        return Err(SyncError::BadChunkData(
            "chunk length must >= 24".to_string(),
        ));
    }

    let offset = u64::from_be_bytes(bs[..8].try_into().unwrap());
    let file_id = String::from_utf8_lossy(&bs[8..24]);
    let data = Bytes::from(bs.slice(24..));

    Ok(SyncChunk {
        file_id: uuid::Uuid::parse_str(&file_id)?,
        bytes: data,
        offset: offset as usize,
    })
}

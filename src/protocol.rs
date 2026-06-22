use std::array::TryFromSliceError;

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use chunkrs::{Chunk, ChunkHash};

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

pub fn encode_chunk(chunk: Chunk) -> BytesMut {
    let offset = chunk.offset.unwrap_or(0);

    let mut encode_bs = BytesMut::with_capacity(100);
    encode_bs.put_u64(offset);

    let hash = chunk.hash.unwrap_or(ChunkHash::new([0u8; 32]));
    encode_bs.put_slice(hash.as_bytes());

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

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

    let offset_bs = &bs[0..8];
    let hash: &[u8] = &bs[8..40];
    let mut ck_hash = [0u8; 32];
    ck_hash.copy_from_slice(hash);

    let d = &bs[40..];
    let data_vec = d.to_vec();
    let data = Bytes::from(data_vec);

    let offset_arr = offset_bs
        .try_into()
        .map_err(|e: TryFromSliceError| SyncError::StdIOError(e.to_string()))?;
    let chunk =
        Chunk::with_offset(data, u64::from_be_bytes(offset_arr)).set_hash(ChunkHash::new(ck_hash));
    Ok(chunk)
}

use std::path::PathBuf;

use async_trait::async_trait;
use blake3::Hash;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use chunkrs::{Chunk, ChunkHash};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::info;
use uuid::Uuid;

use crate::errors::SyncError;

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
    Done,
}

#[derive(Debug, Clone)]
pub struct FileMeta {
    pub file_id: Uuid,
    pub file_path: PathBuf,
    pub file_name: String,
    pub total_size: usize,
    pub hash: Option<blake3::Hash>,
}

impl FileMeta {
    pub fn new(path: &str) -> Self {
        let p = PathBuf::from(path);
        let p1 = p.clone();
        
        Self {
            file_id: uuid::Uuid::new_v4(),
            file_path: p, 
            file_name: Self::path_2_name(p1),
            total_size: 0,
            hash: None,
        }
    }

    pub fn new1(file_id: Uuid, path: PathBuf, size: usize, hash: blake3::Hash) -> Self {
        let p1 = path.clone();
        Self {
            file_id,
            file_path: path,
            file_name: Self::path_2_name(p1),
            total_size: size,
            hash: Some(hash),
        }
    }

    fn path_2_name(path: PathBuf) -> String {
        path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown").to_string()
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

#[derive(Debug, Clone, PartialEq)]
pub struct UploadInitEvent {
    pub file_id: Uuid,
    pub size: u64,
    pub hash: blake3::Hash,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChunkEvent {
    pub file_id: Uuid,
    pub offset: u64,
    pub data: Bytes,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ErrMsg {
    pub code: u16,
    pub msg: String,
}

impl ErrMsg {
    pub fn new(code: u16, msg: &str) -> Self {
        Self { code, msg: msg.to_string() }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct UploadDoneEvent {
    pub file_id: Uuid,
}

pub fn new_framed_writer(
    stream: OwnedWriteHalf,
) -> FramedWrite<OwnedWriteHalf, LengthDelimitedCodec> {
    FramedWrite::new(stream, LengthDelimitedCodec::default())
}

pub fn new_framed_reader(stream: OwnedReadHalf) -> FramedRead<OwnedReadHalf, LengthDelimitedCodec> {
    FramedRead::new(stream, LengthDelimitedCodec::default())
}

/// tag | file_id
pub fn encode_upload_done(d: UploadDoneEvent) -> BytesMut {
    let mut encode_bs = BytesMut::with_capacity(17);
    encode_bs.put_u8(UPLOAD_DONE_TAG); // 1 byte
    encode_bs.put_slice(&d.file_id.into_bytes()); // 16 bytes
    encode_bs
}

/// tag(1) | file_id(16)
pub fn decode_upload_done(bs: &mut BytesMut) -> Result<UploadDoneEvent, SyncError> {
    if bs.len() < 16 {
        return Err(SyncError::BadChunkData(
            "chunk length must >= 16".to_string(),
        ));
    }

    let file_id = bytes_to_uid(bs);
    Ok(UploadDoneEvent {
        file_id,
    })
}

/// err_tag | code(2) | msg
pub fn encode_error(err_msg: ErrMsg) -> BytesMut {
    let mut encode_bs = BytesMut::with_capacity(100);
    encode_bs.put_u8(ERR_TAG); // 1 byte

    encode_bs.put_u16(err_msg.code); // 2 byte
    encode_bs.put_slice(&err_msg.msg.into_bytes()); // variable length
    encode_bs
}

/// code(1) | msg
pub fn decode_error(bs: &mut BytesMut) -> Result<ErrMsg, SyncError> {
    if bs.len() <= 1 {
        return Err(SyncError::BadChunkData("chunk length must > 1".to_string()));
    }

    let code = bs.get_u16();
    let msg = String::from_utf8_lossy(&bs[..]);

    Ok(ErrMsg {
        code,
        msg: msg.to_string(),
    })
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

    let offset = bs.get_u64();
    let file_id = bytes_to_uid(bs);
    // let _ = bs.split_to(24);
    let data = Bytes::copy_from_slice(&bs[16..]);

    Ok(ChunkEvent {
        data,
        offset,
        file_id,
    })
}

/// Upload Init Event
/// size(8) | field_id(16) | hash(32) | name
pub fn encode_upload_init(upload_init: UploadInitEvent) -> BytesMut {
    let mut encode_bs = BytesMut::with_capacity(100);
    encode_bs.put_u8(UPLOAD_INIT_TAG); // 1 byte

    encode_bs.put_u64(upload_init.size); // 8 bytes
    let uid_bs = &upload_init.file_id.into_bytes();
    println!("writing uid bytes: {:?} into buffer", uid_bs);

    encode_bs.put_slice(uid_bs); // 16
    encode_bs.put_slice(upload_init.hash.as_bytes()); // 32 
    encode_bs.put_slice(&upload_init.name.into_bytes()); // variable length
    encode_bs
}

/// size(8) | field_id(16) | hash(32) | name
pub fn decode_upload_init(bs: &mut BytesMut) -> Result<UploadInitEvent, SyncError> {
    if bs.len() <= 56 {
        return Err(SyncError::BadChunkData(
            "chunk length must > 56".to_string(),
        ));
    }

    let file_size = bs.get_u64();
    let file_id = bytes_to_uid(bs);

    let hash_bs = &bs[16..48];
    let mut hash_arr = [0u8; 32];
    hash_arr.copy_from_slice(hash_bs);

    let file_name = String::from_utf8_lossy(&bs[48..]);

    Ok(UploadInitEvent {
        size: file_size,
        hash: Hash::from_bytes(hash_arr),
        file_id: file_id,
        name: file_name.to_string(),
    })
}

fn bytes_to_uid(bs: &mut BytesMut) -> Uuid {
    let mut uid_arr = [0u8; 16];
    uid_arr.copy_from_slice(&bs[..16]);
    println!("uid bytes: {:?}", uid_arr);

    Uuid::from_bytes(uid_arr)
}

#[cfg(test)]
mod tests {
    use bytes::{Buf, Bytes};

use crate::protocol::{
        ChunkEvent, ErrMsg, UploadDoneEvent, UploadInitEvent, CHUNK_TAG, ERR_TAG, UPLOAD_DONE_TAG,
        decode_chunk_event, decode_error, decode_upload_done, decode_upload_init, encode_chunk_event,
        encode_error, encode_upload_done, encode_upload_init,
    };

    #[test]
    fn test_encode_decode_upload_init() {
        let arr = [0u8; 32];
        let init_ev = UploadInitEvent {
            file_id: uuid::Uuid::new_v4(),
            size: 100,
            hash: blake3::Hash::from_bytes(arr),
            name: "test.txt".to_string(),
        };

        println!("encode {:?}", init_ev);
        let mut bs = encode_upload_init(init_ev.clone());

        let tag = bs.get_u8();
        println!("tag: {}", tag);

        let decode_ev = decode_upload_init(&mut bs);
        println!("decode_ev: {:?}", decode_ev);
        assert!(decode_ev.is_ok());

        assert_eq!(init_ev, decode_ev.unwrap());
    }

    #[test]
    fn test_encode_decode_chunk_event() {
        let ev = ChunkEvent {
            file_id: uuid::Uuid::new_v4(),
            offset: 1024,
            data: Bytes::from_static(b"hello world chunk payload"),
        };

        let mut bs = encode_chunk_event(ev.clone());
        let tag = bs.get_u8();
        assert_eq!(tag, CHUNK_TAG);

        let decode_ev = decode_chunk_event(&mut bs).unwrap();
        assert_eq!(decode_ev, ev);
    }

    #[test]
    fn test_encode_decode_err_msg() {
        let ev = ErrMsg {
            code: 42,
            msg: "something went wrong".to_string(),
        };

        let mut bs = encode_error(ev.clone());
        let tag = bs.get_u8();
        assert_eq!(tag, ERR_TAG);

        let decode_ev = decode_error(&mut bs).unwrap();
        assert_eq!(decode_ev, ev);
    }

    #[test]
    fn test_encode_decode_upload_done() {
        let ev = UploadDoneEvent {
            file_id: uuid::Uuid::new_v4(),
        };

        let mut bs = encode_upload_done(ev.clone());
        let tag = bs.get_u8();
        assert_eq!(tag, UPLOAD_DONE_TAG);

        let decode_ev = decode_upload_done(&mut bs).unwrap();
        assert_eq!(decode_ev, ev);
    }
}
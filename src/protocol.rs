use std::path::PathBuf;

use blake3::Hash;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use uuid::Uuid;

use crate::errors::SyncError;

pub type SRFramedRead = FramedRead<OwnedReadHalf, LengthDelimitedCodec>;
pub type SRFramedWrite = FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>;

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
        path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown")
            .to_string()
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
| 0x05 | `UploadDone` | `file_id: [u8;16]` |
| 0x06 | `UploadDoneAck` | `file_id: [u8;16]`, `ok: bool`, `msg: String` |
| 0xFF | `Error` | `code: u8`, `msg: String` |
*/

pub const UPLOAD_INIT_TAG: u8 = 0x01;
pub const UPLOAD_INIT_ACK_TAG: u8 = 0x02;
pub const CHUNK_TAG: u8 = 0x03;
pub const CHUNK_ACK_TAG: u8 = 0x04;
pub const UPLOAD_DONE_TAG: u8 = 0x05;
pub const UPLOAD_DONE_ACK_TAG: u8 = 0x06;
pub const ERR_TAG: u8 = 0xFF;

#[derive(Debug, Clone, PartialEq)]
pub struct UploadInitEvent {
    pub file_id: Uuid,
    pub size: u64,
    pub hash: blake3::Hash,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UploadInitACK {
    pub file_id: Uuid,
    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChunkEvent {
    pub file_id: Uuid,
    pub offset: u64,
    pub data: Bytes,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChunkACK {
    pub file_id: Uuid,
    pub offset: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ErrMsg {
    pub code: u16,
    pub msg: String,
}

impl ErrMsg {
    pub fn new(code: u16, msg: &str) -> Self {
        Self {
            code,
            msg: msg.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct UploadDoneEvent {
    pub file_id: Uuid,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UploadDoneACK {
    pub file_id: Uuid,
    pub ok: bool,
    pub msg: String,
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
    Ok(UploadDoneEvent { file_id })
}

/// err_tag | code(2) | msg
pub fn encode_error(err_msg: ErrMsg) -> Bytes {
    let mut encode_bs = BytesMut::with_capacity(100);
    encode_bs.put_u8(ERR_TAG); // 1 byte

    encode_bs.put_u16(err_msg.code); // 2 byte
    encode_bs.put_slice(&err_msg.msg.into_bytes()); // variable length
    encode_bs.freeze()
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
/// tag(1) | size(8) | field_id(16) | hash(32) | name
pub fn encode_upload_init(upload_init: UploadInitEvent) -> Bytes {
    let mut encode_bs = BytesMut::with_capacity(100);
    encode_bs.put_u8(UPLOAD_INIT_TAG); // 1 byte

    encode_bs.put_u64(upload_init.size); // 8 bytes
    encode_bs.put_slice(&upload_init.file_id.into_bytes()); // 16
    encode_bs.put_slice(upload_init.hash.as_bytes()); // 32 
    encode_bs.put_slice(&upload_init.name.into_bytes()); // variable length
    encode_bs.freeze()
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

/// Upload Init ACK
/// tag(1) | offset(8) | file_id(16)
pub fn encode_upload_init_ack(ack: UploadInitACK) -> Bytes {
    let mut encode_bs = BytesMut::with_capacity(25);
    encode_bs.put_u8(UPLOAD_INIT_ACK_TAG);
    encode_bs.put_u64(ack.offset);
    encode_bs.put_slice(&ack.file_id.into_bytes());
    encode_bs.freeze()
}

/// offset(8) | file_id(16)
pub fn decode_upload_init_ack(bs: &mut BytesMut) -> Result<UploadInitACK, SyncError> {
    if bs.len() < 24 {
        return Err(SyncError::BadChunkData(
            "upload init ack chunk length must >= 24".to_string(),
        ));
    }

    let offset = bs.get_u64();
    Ok(UploadInitACK {
        offset,
        file_id: bytes_to_uid(bs),
    })
}

/// Chunk ACK
/// tag(1) | offset(8) | file_id(16)
pub fn encode_chunk_ack(ack: ChunkACK) -> Bytes {
    let mut encode_bs = BytesMut::with_capacity(25);
    encode_bs.put_u8(CHUNK_ACK_TAG);
    encode_bs.put_u64(ack.offset);
    encode_bs.put_slice(&ack.file_id.into_bytes());
    encode_bs.freeze()
}

/// offset(8) | file_id(16)
pub fn decode_chunk_ack(bs: &mut BytesMut) -> Result<ChunkACK, SyncError> {
    if bs.len() < 24 {
        return Err(SyncError::BadChunkData(
            "chunk ack length must >= 24".to_string(),
        ));
    }

    let offset = bs.get_u64();
    Ok(ChunkACK {
        file_id: bytes_to_uid(bs),
        offset,
    })
}

/// Upload Done ACK
/// tag(1) | file_id(16) | ok(1) | msg
pub fn encode_upload_done_ack(ack: UploadDoneACK) -> Bytes {
    let mut encode_bs = BytesMut::with_capacity(100);
    encode_bs.put_u8(UPLOAD_DONE_ACK_TAG);
    encode_bs.put_u8(if ack.ok { 1 } else { 0 });
    encode_bs.put_slice(&ack.file_id.into_bytes());
    encode_bs.put_slice(&ack.msg.into_bytes());
    encode_bs.freeze()
}

/// file_id(16) | ok(1) | msg
pub fn decode_upload_done_ack(bs: &mut BytesMut) -> Result<UploadDoneACK, SyncError> {
    if bs.len() < 17 {
        return Err(SyncError::BadChunkData(
            "upload done ack length must >= 17".to_string(),
        ));
    }

    let ok = bs.get_u8() != 0;
    let file_id = bytes_to_uid(bs);
    let msg = String::from_utf8_lossy(&bs[16..]).to_string();
    Ok(UploadDoneACK {
        file_id,
        ok,
        msg,
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
    use bytes::{Buf, Bytes, BytesMut};

    use crate::protocol::{
        CHUNK_ACK_TAG, CHUNK_TAG, ChunkACK, ChunkEvent, ERR_TAG, ErrMsg, UPLOAD_DONE_ACK_TAG,
        UPLOAD_DONE_TAG, UPLOAD_INIT_ACK_TAG, UploadDoneACK, UploadDoneEvent, UploadInitACK,
        UploadInitEvent, decode_chunk_ack, decode_chunk_event, decode_error,
        decode_upload_done, decode_upload_done_ack, decode_upload_init, decode_upload_init_ack,
        encode_chunk_ack, encode_chunk_event, encode_error, encode_upload_done,
        encode_upload_done_ack, encode_upload_init, encode_upload_init_ack,
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

        let mut bs_mut = BytesMut::from(bs);
        let decode_ev = decode_upload_init(&mut bs_mut);
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

        let mut bs = BytesMut::from(encode_error(ev.clone()));
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

    #[test]
    fn test_encode_decode_upload_init_ack() {
        let ev = UploadInitACK {
            file_id: uuid::Uuid::new_v4(),
            offset: 4096,
        };

        let mut bs = BytesMut::from(encode_upload_init_ack(ev.clone()));
        let tag = bs.get_u8();
        assert_eq!(tag, UPLOAD_INIT_ACK_TAG);

        let decode_ev = decode_upload_init_ack(&mut bs).unwrap();
        assert_eq!(decode_ev, ev);
    }

    #[test]
    fn test_encode_decode_chunk_ack() {
        let ev = ChunkACK {
            file_id: uuid::Uuid::new_v4(),
            offset: 8192,
        };

        let mut bs = BytesMut::from(encode_chunk_ack(ev.clone()));
        let tag = bs.get_u8();
        assert_eq!(tag, CHUNK_ACK_TAG);

        let decode_ev = decode_chunk_ack(&mut bs).unwrap();
        assert_eq!(decode_ev, ev);
    }

    #[test]
    fn test_encode_decode_upload_done_ack() {
        let ev = UploadDoneACK {
            file_id: uuid::Uuid::new_v4(),
            ok: true,
            msg: "finalized ok".to_string(),
        };

        let mut bs = BytesMut::from(encode_upload_done_ack(ev.clone()));
        let tag = bs.get_u8();
        assert_eq!(tag, UPLOAD_DONE_ACK_TAG);

        let decode_ev = decode_upload_done_ack(&mut bs).unwrap();
        assert_eq!(decode_ev, ev);
    }

    #[test]
    fn test_encode_decode_upload_done_ack_failure() {
        let ev = UploadDoneACK {
            file_id: uuid::Uuid::new_v4(),
            ok: false,
            msg: "hash mismatch".to_string(),
        };

        let mut bs = BytesMut::from(encode_upload_done_ack(ev.clone()));
        let tag = bs.get_u8();
        assert_eq!(tag, UPLOAD_DONE_ACK_TAG);

        let decode_ev = decode_upload_done_ack(&mut bs).unwrap();
        assert_eq!(decode_ev, ev);
    }
}

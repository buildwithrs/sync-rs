use std::collections::HashSet;

use bytes::{Buf, Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use tokio::{
    fs::File,
    io::{AsyncReadExt, BufReader},
};
use tracing::{info, warn};

use crate::{
    errors::SyncError,
    protocol::{
        CHUNK_ACK_TAG, ChunkEvent, FileMeta, SRFramedRead, SRFramedWrite, UPLOAD_DONE_ACK_TAG,
        UPLOAD_INIT_ACK_TAG, UploadDoneACK, UploadDoneEvent, UploadInitACK, UploadInitEvent,
        decode_chunk_ack, decode_upload_done_ack, decode_upload_init_ack, encode_chunk_event,
        encode_upload_done, encode_upload_init,
    },
};

#[derive(Debug)]
pub struct OngoingState {
    pub upload_offsets: HashSet<u64>,
}

impl OngoingState {
    pub fn new() -> Self {
        Self { upload_offsets: HashSet::new() }
    }

    pub fn insert_upload_offsets(&mut self, offset: u64) {
        self.upload_offsets.insert(offset);
    }

    pub fn remove_acked_offset(&mut self, offset: u64) {
        self.upload_offsets.remove(&offset);
    }
}

#[derive(Debug)]
pub struct ClientFileProcessor {
    pub meta: FileMeta,
    pub ongoing: OngoingState,
}

impl ClientFileProcessor {
    pub fn new(path: &str) -> Self {
        Self {
            meta: FileMeta::new(path),
            ongoing: OngoingState { upload_offsets: HashSet::new() }
        }
    }

    pub async fn chunk_and_hash_file(
        &mut self,
        chunk_size: usize,
    ) -> Result<Vec<ChunkEvent>, SyncError> {
        let f_meta = self.meta.clone();
        let file = File::open(&f_meta.file_path).await?;

        let meta = file.metadata().await?;
        self.meta.total_size = meta.len() as usize;

        let mut reader = BufReader::with_capacity(chunk_size, file);
        let mut buf = BytesMut::zeroed(chunk_size);
        let mut chunk_index = 0;
        let mut offset: usize = 0;
        let mut chunk_events = Vec::new();

        let mut hasher = blake3::Hasher::new();

        loop {
            let n = reader.read(&mut buf).await?;
            if n == 0 {
                break;
            }

            println!("Chunk {}: {} bytes", chunk_index, n,);

            hasher.update(&buf[..n]);

            let ck = ChunkEvent {
                file_id: f_meta.file_id.clone(),
                data: Bytes::copy_from_slice(&buf[..n]),
                offset: offset as u64,
            };

            self.ongoing.insert_upload_offsets(ck.offset);

            chunk_events.push(ck);
            offset += n;
            chunk_index += 1;
        }

        self.meta.hash = Some(hasher.finalize());
        Ok(chunk_events)
    }

    pub async fn send_upload_init(
        &self,
        reader: &mut SRFramedRead,
        writer: &mut SRFramedWrite,
    ) -> Result<UploadInitACK, SyncError> {
        let f_meta = &self.meta;
        let upload_event = UploadInitEvent {
            file_id: f_meta.file_id,
            size: f_meta.total_size as u64,
            hash: f_meta.hash.unwrap(),
            name: f_meta.file_name.clone(),
        };

        info!("send upload init...");
        if let Err(e) = writer.send(encode_upload_init(upload_event)).await {
            warn!("send upload init failed: {}", e);
            return Err(SyncError::from(e));
        }

        if let Some(val) = reader.next().await {
            match val {
                Ok(mut bs) => {
                    let tag = bs.get_u8();
                    if tag != UPLOAD_INIT_ACK_TAG {
                        return Err(SyncError::BadChunkData(
                            "expect upload init ack".to_string(),
                        ));
                    }
                    return Ok(decode_upload_init_ack(&mut bs)?);
                }
                Err(e) => return Err(SyncError::StdIOError(e.to_string())),
            }
        }

        return Err(SyncError::ServerNoResp);
    }

    pub async fn send_upload_done(
        &self,
        reader: &mut SRFramedRead,
        writer: &mut SRFramedWrite,
    ) -> Result<UploadDoneACK, SyncError> {
        let f_meta = &self.meta;

        let done_event = UploadDoneEvent {
            file_id: f_meta.file_id,
        };

        info!("send upload done event...");
        if let Err(e) = writer
            .send(Bytes::from(encode_upload_done(done_event)))
            .await
        {
            warn!("failed to write done_event: {}", e);
            return Err(SyncError::from(e));
        }

        if let Some(val) = reader.next().await {
            match val {
                Ok(mut bs) => {
                    let tag = bs.get_u8();
                    if tag != UPLOAD_DONE_ACK_TAG {
                        return Err(SyncError::BadChunkData(
                            "expect upload done ack".to_string(),
                        ));
                    }
                    return Ok(decode_upload_done_ack(&mut bs)?);
                }
                Err(e) => return Err(SyncError::StdIOError(e.to_string())),
            }
        }

        return Err(SyncError::ServerNoResp);
    }

    pub async fn send_chunks(
        &mut self,
        reader: &mut SRFramedRead,
        writer: &mut SRFramedWrite,
        chunks: Vec<ChunkEvent>,
    ) -> Result<(), SyncError> {
        if chunks.len() == 0 {
            return Err(SyncError::NoChunks);
        }

        for chunk in chunks {
            self.ongoing.insert_upload_offsets(chunk.offset);

            if let Err(e) = writer.send(encode_chunk_event(chunk).into()).await {
                warn!("failed to write chunk to TCP Stream: {}", e);
                return Err(SyncError::from(e));
            }

            match reader.next().await {
                Some(val) => match val {
                    Ok(mut bs) => {
                        let tag = bs.get_u8();
                        if tag != CHUNK_ACK_TAG {
                            return Err(SyncError::BadChunkData("expect chunk ack".to_string()));
                        }

                        let ck_ack = decode_chunk_ack(&mut bs)?;
                        info!("received chunk ack: {:?}", ck_ack);
                        // assert!(ck_ack.offset == offset);
                        self.ongoing.remove_acked_offset(ck_ack.offset);
                    }
                    Err(e) => return Err(SyncError::StdIOError(e.to_string())),
                },
                None => return Err(SyncError::ServerNoResp),
            }
        }

        Ok(())
    }
}

use std::{
    collections::HashMap,
    io::{ErrorKind, SeekFrom},
    path::{Path, PathBuf},
    sync::Arc,
};

use bytes::{Buf, Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader},
    net::{TcpStream, tcp::OwnedWriteHalf},
    sync::{RwLock, mpsc},
    task::JoinSet,
};
use tokio_util::codec::{FramedWrite, LengthDelimitedCodec};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    errors::SyncError,
    protocol::{
        CHUNK_TAG, ChunkEvent, FileMeta, FileUploadState, UPLOAD_DONE_TAG, UPLOAD_INIT_TAG,
        UploadDoneEvent, UploadInitEvent, decode_chunk_event, decode_upload_done,
        decode_upload_init, encode_chunk_event, encode_upload_done, encode_upload_init,
        new_framed_reader, new_framed_writer,
    },
};

const MAX_SEND_TASK: usize = 8;
const SERVER_FOLDER: &'static str = "/tmp/uploads";

pub type DataWriter = FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>;

#[derive(Debug)]
pub enum StreamEvent {
    UploadInit(UploadInitEvent),
    Chunk(ChunkEvent),
    Unknown,
}

#[derive(Debug)]
pub struct ClientFileProcessor {
    pub meta: FileMeta,
}

impl ClientFileProcessor {
    pub fn new(path: &str) -> Self {
        Self {
            meta: FileMeta::new(path),
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

            chunk_events.push(ck);
            offset += n;
            chunk_index += 1;
        }

        self.meta.hash = Some(hasher.finalize());
        Ok(chunk_events)
    }

    pub async fn send_chunks(
        &self,
        stream: TcpStream,
        chunks: Vec<ChunkEvent>,
    ) -> Result<(), SyncError> {
        if chunks.len() == 0 {
            return Err(SyncError::NoChunks);
        }

        let (_reader, writer) = stream.into_split();
        let mut framed_writer = new_framed_writer(writer);

        let (tx, mut rx) = mpsc::channel::<Bytes>(32);

        let f_meta = &self.meta;
        let upload_event = UploadInitEvent {
            file_id: f_meta.file_id,
            size: f_meta.total_size as u64,
            hash: f_meta.hash.unwrap(),
            name: f_meta.file_name.clone(),
        };

        let done_event = UploadDoneEvent {
            file_id: f_meta.file_id,
        };

        // Writer task: owns `rx` and the framed writer.
        // Drains every chunk, then writes the done event.
        let writer_handle = tokio::spawn(async move {
            let upload_bs = encode_upload_init(upload_event);
            info!("send upload init...");

            if let Err(e) = framed_writer.send(Bytes::from(upload_bs)).await {
                warn!("send upload init failed: {}", e);
                return Err(SyncError::from(e));
            }

            info!("send file chunks to server...");
            while let Some(data) = rx.recv().await {
                if let Err(e) = framed_writer.send(data).await {
                    warn!("failed to write chunk to TCP Stream: {}", e);
                    return Err(SyncError::from(e));
                }
            }

            info!("send upload done event...");
            if let Err(e) = framed_writer
                .send(Bytes::from(encode_upload_done(done_event)))
                .await
            {
                warn!("failed to write done_event: {}", e);
                return Err(SyncError::from(e));
            }

            Ok::<(), SyncError>(())
        });

        let mut join_set = JoinSet::new();
        for chunk in chunks {
            if join_set.len() >= MAX_SEND_TASK {
                join_set.join_next().await;
            }

            let tx_c = tx.clone();
            join_set.spawn(async move {
                tx_c.send(encode_chunk_event(chunk).into())
                    .await
                    .expect("failed to send item to write channel")
            });
        }

        // Drop the original sender so the receiver can finish once every clone is gone.
        drop(tx);

        while let Some(r) = join_set.join_next().await {
            r.expect("sending chunk task failed");
        }

        writer_handle.await.expect("writer task panicked")?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ServerFileProcessor {
    pub file_dict: Arc<RwLock<HashMap<Uuid, FileMeta>>>,
    pub file_state: Arc<RwLock<HashMap<Uuid, FileUploadState>>>,
}

impl ServerFileProcessor {
    pub fn new() -> Self {
        Self {
            file_dict: Arc::new(RwLock::new(HashMap::with_capacity(100))),
            file_state: Arc::new(RwLock::new(HashMap::with_capacity(100))),
        }
    }

    pub async fn create_folder(&self) -> Result<(), SyncError> {
        match tokio::fs::create_dir(SERVER_FOLDER).await {
            Ok(_) => Ok(()),
            Err(e) => {
                if e.kind() == ErrorKind::AlreadyExists {
                    return Ok(());
                }

                return Err(SyncError::IOError(e));
            }
        }
    }

    /// concurrently recv chunks from stream,
    /// and verify the chunk is okay,
    /// then write the chunk at the position: chunk.offset
    pub async fn handle_file_stream(&mut self, stream: TcpStream) -> Result<(), SyncError> {
        let (reader, _writer) = stream.into_split();
        let mut framed_reader = new_framed_reader(reader);
        // let mut framed_writer = new_framed_writer(writer);

        while let Some(payload) = framed_reader.next().await {
            match payload {
                Ok(mut data) => match self.handle_stream(&mut data).await {
                    Ok(_) => {}
                    Err(e) => {
                        warn!("failed handle stream: {}", e);
                    }
                },
                Err(e) => {
                    warn!("failed to read payload: {}", e);
                }
            }
        }

        Ok(())
    }

    async fn handle_stream(&mut self, data: &mut BytesMut) -> Result<(), SyncError> {
        let tag = data.get_u8();
        match tag {
            UPLOAD_INIT_TAG => {
                let f_id = self.handle_upload_init(data).await?;
                info!("received upload init for: {}", f_id);

                let mut f_s = self.file_state.write().await;
                f_s.insert(f_id, FileUploadState::Init);
            }

            CHUNK_TAG => {
                info!("received file chunk");

                let f_id = self.handle_chunk(data).await?;
                info!("received chunk for: {}", f_id);

                let mut f_s = self.file_state.write().await;
                f_s.insert(f_id, FileUploadState::Uploading);
            }

            UPLOAD_DONE_TAG => {
                let ev = decode_upload_done(data)?;
                info!("received upload done for: {}", ev.file_id);

                let mut f_s = self.file_state.write().await;
                f_s.insert(ev.file_id, FileUploadState::Done);
            }
            _ => {}
        }
        Ok(())
    }

    async fn handle_upload_init(&mut self, data: &mut BytesMut) -> Result<Uuid, SyncError> {
        let upload_init = decode_upload_init(data)?;

        info!("received upload init: {:?}", upload_init);

        let f_id = upload_init.file_id;
        info!("init file: {}", f_id);

        let mut w = self.file_dict.write().await;
        let meta = w.get(&f_id);
        match meta {
            Some(mt) => return Err(SyncError::DuplicateFile(mt.file_id.to_string())),
            None => {
                let idp = PathBuf::from(format!("{}_{}", f_id.to_string(), upload_init.name));
                let f_p = PathBuf::from(SERVER_FOLDER).join(idp);
                w.insert(
                    f_id,
                    FileMeta::new1(
                        upload_init.file_id,
                        f_p.clone(),
                        upload_init.size as usize,
                        upload_init.hash,
                    ),
                );

                info!("create file at: {:?}", f_p);
                create_server_file(f_p, upload_init.size).await?;
            }
        };

        Ok(f_id)
    }

    async fn handle_chunk(&mut self, data: &mut BytesMut) -> Result<Uuid, SyncError> {
        let chunk = decode_chunk_event(data)?;

        let fd = self.file_dict.read().await;
        let meta = fd.get(&chunk.file_id);
        match meta {
            Some(mt) => {
                info!("file name: {}", mt.file_name);

                let f_p = PathBuf::from(SERVER_FOLDER).join(mt.file_name.clone());
                info!("writing data into: {:?}", f_p);

                // `append(true)` is intentionally NOT set: O_APPEND makes the
                // kernel move the file position to the end of the file before
                // every write(2), which silently overrides the `seek` below.
                // The client sends each chunk with the offset it belongs at, so
                // we seek + write and rely on `create_server_file` having
                // pre-allocated the file to the final size.
                let mut f = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(f_p)
                    .await?;

                f.seek(SeekFrom::Start(chunk.offset)).await?;

                info!("writing data at: {}", chunk.offset);
                f.write_all(&chunk.data).await?;
                f.flush().await?;
            }
            None => return Err(SyncError::FileUploadNotInit(chunk.file_id.to_string())),
        }

        Ok(chunk.file_id)
    }
}

/// Create the destination file (if missing) and pre-allocate it to `size` bytes.
///
/// Note: `append(true)` is intentionally NOT set. `std::fs::OpenOptions` rejects
/// `append(true)` combined with `truncate(true)` — its `get_creation_mode`
/// validation returns "creating or truncating a file requires write or append
/// access" for that combination, even when `write(true)` is also set. The error
/// message is misleading; the real rule is "no append + truncate".
async fn create_server_file(fp: PathBuf, size: u64) -> Result<(), SyncError> {
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(fp)
        .await?;

    f.set_len(size).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::tcp::{SERVER_FOLDER, create_server_file};

    #[test]
    fn test_uuid_v4() {
        let uid = uuid::Uuid::new_v4();
        println!("{}: {}", uid.to_string(), uid.into_bytes().len());
    }

    #[tokio::test]
    async fn test_create_folder() {
        let p = PathBuf::from(SERVER_FOLDER).join("README.md");
        let res = create_server_file(p, 1000).await;

        println!("create file res: {:?}", res);
        assert!(res.is_ok());
    }
}

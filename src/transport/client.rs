use bytes::{Bytes, BytesMut};
use futures::SinkExt;
use tokio::{
    fs::File,
    io::{AsyncReadExt, BufReader},
    net::TcpStream,
    sync::mpsc,
    task::JoinSet,
};
use tracing::{info, warn};

use crate::{
    config::MAX_SEND_TASK,
    errors::SyncError,
    protocol::{
        ChunkEvent, FileMeta, UploadDoneEvent, UploadInitEvent, encode_chunk_event,
        encode_upload_done, encode_upload_init, new_framed_writer,
    },
};

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
            info!("send upload init...");
            if let Err(e) = framed_writer.send(encode_upload_init(upload_event)).await {
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

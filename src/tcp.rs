use std::path::PathBuf;

use bytes::{Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use tokio::{
    fs::File,
    io::{AsyncReadExt, BufReader},
    net::TcpStream,
    sync::mpsc,
    task::JoinSet,
};
use tokio_util::codec::FramedRead;
use tracing::warn;
use uuid::Uuid;

use crate::{
    errors::SyncError, protocol::{
        ChunkEvent, FileMeta, UploadInitEvent, encode_chunk_event, encode_upload_init, new_framed_reader, new_framed_writer,
    },
};

const MAX_SEND_TASK: usize = 8;
const SERVER_FOLDER: &'static str = "/tmp/uploads/";

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

    async fn send_chunks(
        &self,
        stream: TcpStream,
        chunks: Vec<ChunkEvent>,
    ) -> Result<(), SyncError> {
        let f_meta = &self.meta;

        let (_reader, writer) = stream.into_split();
        let mut framed_writer = new_framed_writer(writer);

        let (tx, mut rx) = mpsc::channel::<Bytes>(32);

        let name = f_meta
            .file_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown");
        let upload_event = UploadInitEvent {
            file_id: f_meta.file_id,
            size: f_meta.total_size as u64,
            hash: f_meta.hash.unwrap(),
            name: name.to_string(),
        };

        tokio::spawn(async move {
            let upload_bs = encode_upload_init(upload_event);

            let _ = framed_writer.send(Bytes::from(upload_bs)).await;

            while let Some(data) = rx.recv().await {
                if let Err(e) = framed_writer.send(data).await {
                    warn!("failed to write chunk to TCP Stream: {}", e);
                    break;
                }
            }
        });

        let mut join_set = JoinSet::new();
        for chunk in chunks {
            if join_set.len() >= MAX_SEND_TASK {
                join_set.join_next().await;
            }

            let tx_c = tx.clone();
            join_set.spawn(async move {
                let bs = encode_chunk_event(chunk);
                tx_c.send(bs.into())
                    .await
                    .expect("failed to send item to write channel")
            });
        }

        while let Some(r) = join_set.join_next().await {
            r.expect("sending chunk task failed");
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct ServerFileProcessor {
    pub meta: FileMeta,
}

impl ServerFileProcessor {
    /// concurrently recv chunks from stream,
    /// and verify the chunk is okay,
    /// then write the chunk at the position: chunk.offset
    async fn serve(&mut self, stream: TcpStream) -> Result<(), SyncError> {

        let (reader, writer) = stream.into_split();
        let mut framed_reader = new_framed_reader(reader);

        while let Some(payload) = framed_reader.next().await {
            match payload {
                Ok(data) => {
                    match self.handle_stream(data).await {
                        Ok(_) => {},
                        Err(e) => {
                            eprintln!("failed handle stream: {}", e);
                        }

                    }
                }
                Err(e) => {
                    eprintln!("failed to read payload: {}", e);
                }
            }
        }

        Ok(())
    }


    async fn handle_stream(&mut self, data: BytesMut) -> Result<(), SyncError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_uuid_v4() {
        let uid = uuid::Uuid::new_v4();
        println!("{}: {}", uid.to_string(), uid.into_bytes().len());
    }
}

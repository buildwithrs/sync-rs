use std::path::PathBuf;

use bytes::{Bytes, BytesMut};
use chunkrs::Chunk;
use futures::SinkExt;
use tokio::{
    fs::File, io::{AsyncReadExt, AsyncWriteExt, BufReader}, net::TcpStream, sync::mpsc, task::JoinSet,
};
use tracing::warn;
use uuid::Uuid;

use crate::{
    chunk::SyncChunk, errors::SyncError, protocol::{ChunkEvent, encode_chunk, encode_chunk_event, new_framed_writer},
};

const MAX_SEND_TASK: usize = 8;

#[derive(Debug)]
pub struct ClientFileProcessor {
    pub file_id: Uuid,
    pub file_path: PathBuf,
    pub total_size: usize,
    pub hash: Option<blake3::Hash>,
}

impl ClientFileProcessor {
    pub fn new(path: &str) -> Self {
        Self {
            file_id: uuid::Uuid::new_v4(),
            file_path: PathBuf::from(path),
            total_size: 0,
            hash: None,
        }
    }

    pub async fn process_file_chunks(
        &mut self,
        chunk_size: usize,
    ) -> Result<Vec<ChunkEvent>, SyncError> {
        let file = File::open(&self.file_path).await?;

        let meta = file.metadata().await?;
        self.total_size = meta.len() as usize;

        let mut reader = BufReader::with_capacity(chunk_size, file);
        let mut buf = BytesMut::zeroed(chunk_size);
        let mut chunk_index = 0;
        let mut offset: usize = 0;
        let mut chunk_events = Vec::new();

        loop {
            let n = reader.read(&mut buf).await?;
            if n == 0 {
                break;
            }

            println!(
                "Chunk {}: {} bytes",
                chunk_index,
                n,
            );

            let ck = ChunkEvent {
                file_id: self.file_id.clone(),
                data: Bytes::copy_from_slice(&buf[..n]),
                offset: offset as u64,
            };

            chunk_events.push(ck);
            offset += n;
            chunk_index += 1;
        }

        Ok(chunk_events)
    }
}

async fn send(stream: TcpStream, chunks: Vec<Chunk>) -> Result<(), SyncError> {
    let (_reader, writer) = stream.into_split();
    let mut framed_writer = new_framed_writer(writer);

    let (tx, mut rx) = mpsc::channel::<Bytes>(32);

    tokio::spawn(async move {
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
            let bs = encode_chunk(chunk);
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

/// concurrently recv chunks from stream,
/// and verify the chunk is okay,
/// then write the chunk at the position: chunk.offset
async fn recv(stream: TcpStream) -> Result<(), SyncError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_uuid_v4() {
        let uid = uuid::Uuid::new_v4();
        println!("{}: {}", uid.to_string(), uid.into_bytes().len());
    }
}
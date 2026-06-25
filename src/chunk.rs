use bytes::{Bytes, BytesMut};
use chunkrs::{Chunk, ChunkConfig, Chunker};
use tokio::fs;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc::{self, Receiver};
use tokio::{fs::File, io::BufReader};
use uuid::Uuid;

use crate::errors::SyncError;

#[derive(Debug)]
pub struct SyncChunk {
    pub file_id: Uuid,
    pub bytes: Bytes,
    pub offset: usize,
}

/// split the file into chunks
pub async fn split_file(file_path: &str) -> Result<Vec<Chunk>, SyncError> {
    let mut chunker = Chunker::new(ChunkConfig::new(16 * 1024, 64 * 1024, 128 * 1024)?);

    let f = File::open(file_path).await?;

    let mut buffer_reader = BufReader::new(f);

    let mut all_chunks = Vec::new();
    let mut remain = Bytes::new();

    loop {
        let mut buf = BytesMut::with_capacity(64 * 1024);
        let n = buffer_reader.read_buf(&mut buf).await?;
        if n == 0 && remain.len() == 0 {
            break;
        }

        if remain.len() > 0 {
            buf.extend_from_slice(&remain);
        }

        let (chunks, leftover) = chunker.push(Bytes::from(buf));
        all_chunks.extend_from_slice(chunks.as_slice());

        remain = leftover;
    }

    if let Some(final_chunk) = chunker.finish() {
        println!("Final chunk: {} bytes", final_chunk.len());
        all_chunks.push(final_chunk);
    }

    Ok(all_chunks)
}

async fn split_file1(file_path: &str, ck_size: usize) -> Result<Receiver<SyncChunk>, SyncError> {
    let (tx, rx) = mpsc::channel::<SyncChunk>(1000);

    let mut offset = 0;
    let f = fs::File::open(file_path).await?;
    let mut buf_reader = BufReader::with_capacity(ck_size, f);
    loop {
        let mut buf = Vec::with_capacity(ck_size);
        let n = AsyncReadExt::take(&mut buf_reader, ck_size as u64)
            .read_to_end(&mut buf)
            .await?;
        if n == 0 {
            break;
        }

        let _ = tx
            .send(SyncChunk {
                file_id: Uuid::new_v4(),
                bytes: Bytes::from(buf),
                offset,
            })
            .await;
        offset += n;
    }
    Ok(rx)
}

#[cfg(test)]
mod tests {
    use crate::chunk::{split_file, split_file1};
    use crate::config::CHUNK_SIZE_T;

    #[tokio::test]
    async fn test_chunks() {
        let f = "README.md";
        match split_file(f).await {
            Ok(chunks) => {
                println!("length of chunk: {}", chunks.len());
                println!(
                    "length of chunk: {:?}, {:?}",
                    chunks[0].hash, chunks[0].offset
                );
                assert!(chunks.len() > 0)
            }
            Err(e) => {
                eprintln!("split file error: {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_split_file1() {
        let f = "README.md";
        match split_file1(f, CHUNK_SIZE_T).await {
            Ok(mut chunk_recv) => {
                let mut chunks = Vec::new();
                match chunk_recv.recv().await {
                    Some(chunk) => {
                        println!("received chunk: {:?}", chunk);

                        chunks.push(chunk);
                    }
                    None => {}
                }

                assert_eq!(chunks.len(), 1);
                assert_eq!(chunks[0].bytes.as_ref(), b"# sync-rs\n");
                assert_eq!(chunks[0].offset, 0);
            }
            Err(e) => {
                eprintln!("split file error: {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_split_file11() {
        let f = "LICENSE";
        match split_file1(f, CHUNK_SIZE_T).await {
            Ok(mut chunk_recv) => {
                let mut chunks = Vec::new();
                while let Some(ck) = chunk_recv.recv().await {
                    println!("received chunk: {:?}", ck);
                    chunks.push(ck);
                }

                assert!(chunks.len() > 1);
            }
            Err(e) => {
                eprintln!("split file error: {}", e);
            }
        }
    }
}

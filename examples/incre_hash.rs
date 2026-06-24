use std::collections::HashMap;

struct IncrementalHasher {
    chunk_hashes: HashMap<u64, blake3::Hash>, // chunk_index → hash
    chunk_size: u64,
}

impl IncrementalHasher {
    fn new(chunk_size: u64) -> Self {
        Self { chunk_hashes: HashMap::new(), chunk_size }
    }

    fn update_chunk(&mut self, index: u64, data: &[u8]) {
        let hash = blake3::hash(data);
        self.chunk_hashes.insert(index, hash);
    }

    fn final_hash(&self, total_chunks: u64) -> blake3::Hash {
        let mut hasher = blake3::Hasher::new();
        for i in 0..total_chunks {
            if let Some(h) = self.chunk_hashes.get(&i) {
                hasher.update(h.as_bytes());
            }
        }
        hasher.finalize()
    }
}

fn main() {
    println!("increment hash");
}
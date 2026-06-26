# sync-rs Architecture

Three ASCII diagrams documenting the runtime data flow in this project.
All call sites are grounded in `src/` — `src/client/main.rs`,
`src/server/main.rs`, `src/transport/client.rs`,
`src/transport/server.rs`, plus the protocol codecs in
`src/protocol.rs` and the constants in `src/config.rs`.

---

## 1. Client-side data flow

Entry point: `src/client/main.rs`. The driver is a single `TcpStream` that
is split once, then fed by a small fan-out pipeline.

```
                              src/client/main.rs
+--------------------------------------------------------------------+
|  args[1] = file path                                               |
|       |                                                            |
|       v                                                            |
|  Retry::spawn(ExpBackoff, take=3, jitter)  <-- 0.0.0.0:6868         |
|       |                          |                                 |
|       | TcpStream::connect       | transient err -> backoff        |
|       v                          v                                 |
|  +------------------+   +-------------------------+                |
|  |  Ok(stream)      |   |  RetryError::Transient  |                |
|  +------------------+   +-------------------------+                |
|       |                                                            |
|       v                                                            |
|  ClientFileProcessor::new(path)                                    |
|   - builds FileMeta (uuid_v4, file_path, file_name)                |
|       |                                                            |
|       v                                                            |
|  ClientFileProcessor::chunk_and_hash_file(CHUNK_SIZE = 64 KiB)     |
|   - File::open  -->  meta.total_size                               |
|   - BufReader::with_capacity(64 KiB, file)                         |
|   - loop: read 64 KiB -> blake3::Hasher::update                    |
|       |                                                            |
|       v   Vec<ChunkEvent> { file_id, offset, data }                |
|       |   (offset monotonically increases by `n` per chunk)        |
|       |                                                            |
|       v                                                            |
|  ClientFileProcessor::send_chunks(stream, chunks)                  |
|       |                                                            |
|       |  stream.into_split()                                       |
|       +-------------------+----------------------------------------+
|       |                   |                                        |
|       v                   v                                        |
|    (_reader)        OwnedWriteHalf                                  |
|                          |                                         |
|                          v  new_framed_writer()                    |
|                  FramedWrite<LengthDelimitedCodec>                 |
|                          ^                                         |
|                          | framed.send(bytes)                      |
|                          |                                         |
|              +-----------+-----------+                             |
|              |     writer task       |  tokio::spawn               |
|              |  (owns framed_writer) |                             |
|              |  send UploadInit      |                             |
|              |  loop rx.recv()       |                             |
|              |  send UploadDone      |                             |
|              +-----------------------+                             |
|                          ^                                         |
|                          | Bytes (encoded frame)                   |
|                          |                                         |
|                    mpsc::channel<Bytes>(32)                        |
|                          ^                                         |
|                          | tx.send(encode_chunk_event(c).into())   |
|                          |                                         |
|              +-----------+-----------+                             |
|              |    JoinSet (cap=8)    |  MAX_SEND_TASK = 8          |
|              |                       |                             |
|              |  task 1: encode + tx  |                             |
|              |  task 2: encode + tx  |                             |
|              |  ...                  |                             |
|              |  task N: encode + tx  |  joins oldest at len >= 8   |
|              +-----------------------+                             |
|                          ^                                         |
|                          | chunks.into_iter()                      |
|                          |                                         |
|                  Vec<ChunkEvent>                                   |
|                                                                    |
|       drop(original tx)  -->  rx returns None  -->  writer flush   |
|       join_set.join_next() drained                                |
|       writer_handle.await  -->  UploadInit + chunks + UploadDone   |
|                                   sent on the wire                 |
+--------------------------------------------------------------------+
```

Notable details from the source:

- Retry strategy is `ExponentialBackoff::from_millis(10).factor(1).max_delay_millis(100).max_interval(1000).take(3).map(jitter)` (client/main.rs:22).
- `chunk_and_hash_file` reads until EOF and updates the rolling `blake3::Hasher`; the final digest is stored on `self.meta.hash` (transport/client.rs:49-72).
- `send_chunks` caps concurrent encoder/sender tasks at `MAX_SEND_TASK = 8` via a `JoinSet`; when the set is full the loop awaits the next completion before spawning the next chunk (transport/client.rs:132-143).
- The mpsc channel is dropped *after* every producer task joins; that is what lets the writer task drain and emit `UploadDone` (transport/client.rs:146-152).
- The reader half of the split stream is intentionally discarded — the client never reads server responses in the current implementation.

---

## 2. Server-side data flow

Entry point: `src/server/main.rs`. The server is a single accept loop;
every accepted socket runs `handle_file_stream` to completion.

```
                              src/server/main.rs
+--------------------------------------------------------------------+
|  TcpListener::bind("0.0.0.0:6868")                                 |
|       |                                                            |
|       v                                                            |
|  ServerFileProcessor::new()                                        |
|   - file_dict : Arc<RwLock<HashMap<Uuid, FileMeta>>>               |
|   - file_state: Arc<RwLock<HashMap<Uuid, FileUploadState>>>         |
|       |                                                            |
|       v                                                            |
|  create_folder()  -->  mkdir /tmp/uploads  (ignore AlreadyExists)  |
|       |                                                            |
|       v                                                            |
|  +-------------------------------+                                 |
|  | loop: listener.accept().await |  one server instance, many conns|
|  +-------------------------------+                                 |
|       |                                                            |
|       | (stream, remote)                                           |
|       v                                                            |
|  ServerFileProcessor::handle_file_stream(stream)                   |
|       |                                                            |
|       |  stream.into_split()                                       |
|       +-------------------+--------------------+                   |
|       |                   |                    |                   |
|       v                   v                    |                   |
|   OwnedReadHalf      OwnedWriteHalf             |                   |
|       |                   |                    |                   |
|       v new_framed_reader v new_framed_writer  |                   |
|   FramedRead<Length-   FramedWrite<Length-     |                   |
|     DelimitedCodec>      DelimitedCodec>       |                   |
|       ^                   ^                    |                   |
|       |                   | encode_error(e)    |                   |
|       |                   | on Err             |                   |
|       |       +-----------+                    |                   |
|       |       |                                |                   |
|       +-------+                                |                   |
|               |                                |                   |
|       while let Some(payload) =                |                   |
|               framed_reader.next().await       |                   |
|               |                                |                   |
|               v (Ok / Err)                     |                   |
|       +-------+--------------+                 |                   |
|       |                      |                 |                   |
|       | Ok(data)             | Err(e)          |                   |
|       v                      v warn!           |                   |
|   handle_stream(&mut data)   (continue loop)   |                   |
|       |                      ^                 |                   |
|       | data.get_u8() -> tag |                 |                   |
|       +----+----+----+-------+                 |                   |
|            |    |    |                         |                   |
|   0x01     | 0x03| 0x05| ...                   |                   |
| UploadInit |Chunk|Done |                       |                   |
|            v    v    v                         |                   |
|       +----+--+-+--+-+                          |                   |
|       | handle_  |  |  |                        |                   |
|       | upload_  |  |  | decode_upload_done()   |                   |
|       | init     |  |  |   file_state[uuid]     |                   |
|       |          |  |  |   = Done               |                   |
|       v          v  v                          |                   |
|   decode_upload_init                             |                  |
|       |                                         |                  |
|       v                                         |                  |
|   file_dict.write().await                       |                  |
|       |                                         |                  |
|       v                                         |                  |
|   path = /tmp/uploads/{uuid}_{name}             |                  |
|   FileMeta::new1(file_id, path, size, hash)     |                  |
|       |                                         |                  |
|       v                                         |                  |
|   create_server_file(path, size)                |                  |
|     - OpenOptions read+write+create             |                  |
|     - set_len(size)  (pre-allocate)             |                  |
|       |                                         |                  |
|       v                                         |                  |
|   file_state[uuid] = Init                       |                  |
|                                                                    |
|   handle_chunk(data):                                              |
|     decode_chunk_event  -> { file_id, offset, data }               |
|       |                                                           |
|       v                                                           |
|   file_dict.read().await.get(uuid)                                |
|       |                                                           |
|       v                                                           |
|   OpenOptions write+create   (NO append -- explicit seek below)   |
|   f.seek(SeekFrom::Start(offset))                                 |
|   f.write_all(&chunk.data)                                       |
|   f.flush()                                                       |
|       |                                                           |
|       v                                                           |
|   file_state[uuid] = Uploading                                    |
|                                                                    |
|   on any Err in handle_stream:                                    |
|       framed_writer.send(encode_error(SyncError -> ErrMsg))        |
|                                                                    |
|   loop ends when framed_reader.next() returns None                |
|   (client closed the connection after UploadDone)                 |
+--------------------------------------------------------------------+
```

Notable details from the source:

- The server uses two `Arc<RwLock<HashMap<Uuid, …>>>` registries so concurrent connections could share state; today a single shared instance services every accepted socket (server.rs:29-40).
- `handle_file_stream` writes back errors on the write half of the same split stream (server.rs:69).
- `handle_chunk` deliberately does *not* set `append(true)` — the comment in the source explains that O_APPEND would override the per-chunk `seek` and corrupt the file (server.rs:159-175).
- `create_server_file` pre-allocates `set_len(size)` so chunks can `seek`+`write` into a fully-sized hole and the offsets stay valid even if chunks arrive out of order (server.rs:191-200).
- The server has no per-connection state beyond the read-side dispatch loop; `file_state` records lifecycle (`Init` / `Uploading` / `Done`) but is not currently consulted for control flow.

---

## 3. Whole architecture (client <-> server)

End-to-end view of one upload. Wire format is framed by
`LengthDelimitedCodec` (length-prefixed frames) and each frame starts
with a 1-byte tag from `src/protocol.rs`.

```
       CLIENT  (src/client/main.rs)                                  SERVER  (src/server/main.rs)
       ----------------------                                         -------------------------

  argv[1]                                                       TcpListener::bind("0.0.0.0:6868")
     |                                                                  |
     v                                                                  v
  Retry::spawn(ExpBackoff take=3)                       ServerFileProcessor::new
  TcpStream::connect("0.0.0.0:6868")                          file_dict / file_state
     |  <-------- TCP three-way handshake --------------------------->  |
     |                                                                  v
     |                                                            create_folder()
     |                                                            /tmp/uploads
     |                                                                  |
     v                                                                  |
  ClientFileProcessor::new(path)                                         |
  FileMeta { uuid, file_path, file_name, total_size, hash }              |
     |                                                                  |
     v                                                                  |
  chunk_and_hash_file(64 KiB)                                            |
     |  read loop, blake3::Hasher::update                               |
     v                                                                  |
  Vec<ChunkEvent { file_id, offset, data }>                              |
     |                                                                  |
     v                                                                  |
  send_chunks(stream, chunks)                                            v
     |                                                          accept() -> stream
     |  stream.into_split()                                          |
     +------------------+-----------------------------+-----------------+
                        |                             |
                        v                             v
                OwnedWriteHalf                  OwnedReadHalf
                        |                             |
                        v                             v
                FramedWrite<LDC>                 FramedRead<LDC>
                        |                             ^
                        |                             | next().await
                        |                             |
   writer task:                frames arrive on the wire as
   send UploadInit  ------->   [len][tag|size|file_id|hash|name]   --->  handle_stream
   loop rx.recv()       <---   [len][tag|offset|file_id|data]      --->  handle_stream
   send UploadDone     ------>[len][tag|file_id]                    --->  handle_stream
                                                                              |
                                                          +-------------------+------------------+
                                                          |                   |                  |
                                                       0x01               0x03               0x05
                                                  UploadInit tag         Chunk tag         UploadDone tag
                                                          |                   |                  |
                                                          v                   v                  v
                                                  decode_upload_init  decode_chunk_event  decode_upload_done
                                                          |                   |                  |
                                                          v                   v                  v
                                                  file_dict.write      file_dict.read     file_state.write
                                                  Insert FileMeta      OpenOptions        Insert Done
                                                  (uuid -> path,       write+create
                                                   size, hash)         seek(offset)
                                                          |            write_all(data)
                                                          v            flush
                                                  create_server_file
                                                  set_len(size)
                                                  /tmp/uploads/
                                                  {uuid}_{name}
                                                          |                   |
                                                          +-------+-----------+
                                                                  |
                                                                  v
                                                          /tmp/uploads/{uuid}_{name}
                                                          (sparse file, written at
                                                           explicit offsets)

   on error: client sends   [len][tag=0xFF|code(2)|msg]   --->  framed_reader
                                                            no client handler reads it
                                                            today, but the wire path exists

   client closes the TCP write side after UploadDone
   server's framed_reader.next() returns None -> handle_file_stream returns Ok(())
```

### Wire format (src/protocol.rs)

```
+--------------+-----------+----------------+----------------+-----------+
| tag (1 byte) | size (u64)| file_id (16 B) | hash (32 B)    | name      |   UploadInit   (tag = 0x01)
+--------------+-----------+----------------+----------------+-----------+

+--------------+----------+----------------+----------+
| tag (1 byte) | off (u64)| file_id (16 B) | data ... |            Chunk        (tag = 0x03)
+--------------+----------+----------------+----------+

+--------------+----------------+
| tag (1 byte) | file_id (16 B) |                                  UploadDone    (tag = 0x05)
+--------------+----------------+

+--------------+----------+--------+
| tag (1 byte) | code(u16)| msg   |                                     Error        (tag = 0xFF)
+--------------+----------+--------+
```

Each frame is length-prefixed by `LengthDelimitedCodec::default()`; the
codec's length bytes are the outer `len` field in the diagram above and
the tag/contents bytes follow inside.

### Concurrency summary

```
CLIENT (per upload)                          SERVER (per accepted conn)
---------------------------                  ----------------------------
- 1 writer task (frames wire)                - 1 read loop (frames wire)
- up to MAX_SEND_TASK = 8 senders via        - 1 dispatcher (tag -> handler)
  JoinSet, each -> mpsc(32) -> writer        - file_dict / file_state behind
- 1 reader half (unused)                       Arc<RwLock<HashMap>>
- hash task is implicit in the read loop       (server itself is single-threaded
  in chunk_and_hash_file (sequential)          async; multiple accepted conns
                                               share the registries)
```

### Lifecycle of a single `file_id` (uuid_v4 minted on the client)

```
client.ClientFileProcessor::new()                 (uuid minted here)
        |
        v
client.chunk_and_hash_file()  --> server.handle_upload_init()
        |                            file_dict[uuid] = FileMeta
        |                            file_state[uuid] = Init
        |                            /tmp/uploads/{uuid}_{name} created+truncated
        v
client.send_chunks()  --> server.handle_chunk()    (repeated, one per ChunkEvent)
        |                            f.seek(offset); f.write_all(data)
        |                            file_state[uuid] = Uploading
        v
client writer task     --> server.handle_upload_done()
                             file_state[uuid] = Done
                             framed_reader.next() then returns None
                             handle_file_stream returns Ok(())
                             server.accept() loop ready for the next client
```

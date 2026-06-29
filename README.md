# sync-rs

A small, async, tokio-based file sync tool written in Rust. It ships two
binaries — `client` and `server` — that transfer a file from the client to the
server over a length-delimited TCP stream, hashing the file with BLAKE3 along
the way and verifying the result on completion.

## Features

- TCP transport built on `tokio` + `tokio-util`'s `LengthDelimitedCodec`.
- Fixed-size chunk upload (64 KiB by default) with per-chunk acknowledgements.
- End-to-end integrity check: the client streams a BLAKE3 hash alongside the
  `UploadInit` event, and the server re-hashes the assembled file in
  `UploadDone` and rejects mismatches.
- Resumable upload scaffolding: `UploadInitACK` carries an `offset` the server
  can use to indicate where to resume from.
- Connect-with-retry on the client side, backed by `tokio-retry2` with
  exponential backoff + jitter.
- Structured logging via `tracing` / `tracing-subscriber` (configurable
  through the `RUST_LOG` env var).

## Project layout

```
.
├── Cargo.toml
├── src/
│   ├── lib.rs                  # library entry; re-exports + tracing init
│   ├── main.rs                 # placeholder binary
│   ├── client/main.rs          # `client` binary
│   ├── server/main.rs          # `server` binary
│   ├── config.rs               # tunables (chunk size, server folder, ...)
│   ├── errors.rs               # SyncError / SyncClientError + wire codes
│   ├── protocol.rs             # wire-format events and encode/decode fns
│   └── transport/
│       ├── mod.rs              # shared helpers (file_hash)
│       ├── client.rs           # ClientFileProcessor
│       └── server.rs           # ServerFileProcessor
├── examples/incre_hash.rs      # incremental blake3 hashing example
├── docs/                       # design notes
└── tests/                      # integration tests (currently empty)
```

## Architecture

```
                ┌──────────────────┐   TCP (length-delimited)   ┌──────────────────┐
                │  client binary   │  ───────────────────────▶   │  server binary   │
                │  (tokio main)    │  ◀───────────────────────    │  (tokio main)    │
                └──────────────────┘                             └──────────────────┘
                        │                                                │
                        ▼                                                ▼
               ClientFileProcessor                              ServerFileProcessor
               (transport/client.rs)                            (transport/server.rs)
                        │                                                │
                        └──────────────►  protocol.rs  ◄────────────────┘
                                  (encode/decode events)
```

Both binaries depend on the same `sync-rs` library, which exposes the
protocol, the error type, the transport processors, and the configuration
constants. The library also exposes `init_tracing()` to set up the
`tracing-subscriber` stack.

## Module reference

| Module | Responsibility |
|---|---|
| `lib.rs` | Public module surface and `init_tracing()` helper. |
| `config.rs` | Constants: `CHUNK_SIZE` (64 KiB), `CHUNK_SIZE_T` (test value), `MAX_SEND_TASK` (8), `SERVER_FOLDER` (`/tmp/uploads`). |
| `errors.rs` | `SyncError` and `SyncClientError` enums (using `thiserror`) and a `From<SyncError> for ErrMsg` impl that turns internal errors into wire messages with stable error codes (`IO_ERRCODE`, `FILESIZE_EXCEED_ERRCODE`, ...). |
| `protocol.rs` | Wire types (`UploadInitEvent`, `UploadInitACK`, `ChunkEvent`, `ChunkACK`, `UploadDoneEvent`, `UploadDoneACK`, `ErrMsg`), tag bytes, and `encode_*` / `decode_*` functions. Also provides `new_framed_reader` / `new_framed_writer` helpers and the `FileMeta` / `FileUploadState` value types. |
| `transport::mod` | Shared helpers, including `file_hash()` (memory-mapped BLAKE3). |
| `transport::client` | `ClientFileProcessor`: chunks a file, hashes it, drives the three-phase send (init → chunks → done). |
| `transport::server` | `ServerFileProcessor`: per-connection state (`FileDict`, `FileState`), accepts `UploadInit`/`Chunk`/`UploadDone` events, writes chunks to disk, verifies the hash on completion. |

### Protocol overview

Each frame on the wire is a length-delimited `Bytes` payload whose first byte
is a tag identifying the message kind. Tags live in `protocol.rs`:

| Tag | Name | Payload layout |
|----:|------|----------------|
| `0x01` | `UploadInit` | `size(u64) \| file_id([u8;16]) \| hash([u8;32]) \| name(bytes)` |
| `0x02` | `UploadInitACK` | `offset(u64) \| file_id([u8;16])` |
| `0x03` | `Chunk` | `offset(u64) \| file_id([u8;16]) \| data` |
| `0x04` | `ChunkACK` | `offset(u64) \| file_id([u8;16])` |
| `0x05` | `UploadDone` | `file_id([u8;16])` |
| `0x06` | `UploadDoneACK` | `ok(u8) \| file_id([u8;16]) \| msg(bytes)` |
| `0xFF` | `Error` | `code(u16) \| msg(bytes)` |

The happy-path flow is:

1. Client sends `UploadInit` (size, file id, blake3 hash, file name).
2. Server registers the file, pre-allocates it to the declared size, replies
   with `UploadInitACK` (resume offset — currently `0`).
3. Client streams `Chunk` events; each one is acknowledged with a
   `ChunkACK` carrying the offset just written.
4. Client sends `UploadDone`.
5. Server re-hashes the file on disk, compares it with the hash from
   `UploadInit`, and replies with `UploadDoneACK` (`ok=true` on match,
   `ok=false` on mismatch).

## Dependencies

Direct dependencies declared in `Cargo.toml`:

| Crate | Version | Why |
|---|---|---|
| `anyhow` | 1.0 | Ergonomic error handling in binaries. |
| `blake3` | 1.8 | Content hashing for integrity checks. |
| `bytes` | 1.12 | Zero-copy byte buffers used by the protocol codec. |
| `futures` | 0.3 | `SinkExt` / `StreamExt` on the framed stream. |
| `memmap2` | 0.9 | Memory-mapped file reads when computing server-side hashes. |
| `thiserror` | 2.0 | Derive `Error` for `SyncError` / `SyncClientError`. |
| `tokio` | 1.52 (features = `full`) | Async runtime, TCP, file I/O. |
| `tokio-retry2` | 0.9 (feature = `jitter`) | Connect-with-retry on the client. |
| `tokio-util` | 0.7 (features = `codec`, `rt`) | `LengthDelimitedCodec`. |
| `tracing` | 0.1 | Structured logging facade. |
| `tracing-subscriber` | 0.3 (features = `env-filter`, `json`, `fmt`) | Subscriber used by `init_tracing()`. |
| `uuid` | 1.23 (feature = `v4`) | Per-upload file identifiers. |

The two binaries (`client` and `server`) are declared via `[[bin]]` blocks in
`Cargo.toml`, each pointing at its own `main.rs`.

## Build

```bash
# Build both binaries
cargo build --release

# Run the unit tests (protocol encode/decode, transport helpers)
cargo test
```

## Usage

### Server

The server listens on `0.0.0.0:6868`, creates the upload folder
(`/tmp/uploads` by default — see `config::SERVER_FOLDER`) if it does not
already exist, and accepts one connection at a time on the accept loop.
Each connection is driven to completion by `ServerFileProcessor::handle_file_stream`.

```bash
cargo run --bin server
```

Server log output (level controlled via `RUST_LOG`, e.g. `RUST_LOG=debug`):

```
INFO Sync-RS Server
INFO creating upload folder on server
INFO server working on addr: 0.0.0.0:6868, ready to receive connection
INFO handle file stream for: 127.0.0.1:54321
INFO received upload init for: <uuid>
INFO received file chunk
INFO received chunk for: <uuid>
...
INFO received upload done for: <uuid>
INFO upload success for: <your-file>
```

### Client

The client takes a single positional argument: the path of the file to
upload. It connects to the server at `0.0.0.0:6868` with up to 3 retries
(exponential backoff + jitter), chunks the file into 64 KiB pieces, hashes
it with BLAKE3, and runs the three-phase upload.

```bash
cargo run --bin client -- /path/to/local/file.txt
```

The same address is currently hard-coded in `src/client/main.rs` (`let
server_addr = "0.0.0.0:6868"`); change it there to point at a remote host.

Example run:

```
INFO Sync-RS Client
INFO client will upload: /path/to/local/file.txt
INFO file has been chunked into 16 chunks
INFO starting upload...
INFO send upload init...
INFO received init resp: UploadInitACK { file_id: ..., offset: 0 }
INFO received chunk ack: ChunkACK { ... }
...
INFO done send the chunks
INFO send upload done event...
INFO received upload done ack: UploadDoneACK { ok: true, msg: "upload success for: file.txt", ... }
```

The uploaded file lands at `/tmp/uploads/<file_id>_<name>` on the server.

## Examples

`examples/incre_hash.rs` shows how to compute a BLAKE3 hash over a file
incrementally. Run it with:

```bash
cargo run --example incre_hash
```

## Design notes

`docs/` contains longer-form write-ups that informed the current design:

- `arch.md` — overall architecture.
- `correct-create-file.md` — why the server's `OpenOptions` deliberately
  does **not** set `append(true)`.
- `correctly-send-chunks.md` — the three-phase send used by the client.
- `decode_chunk_performance.md` — performance notes on chunk decoding.
- `issue1_async_trait.md` — note on the `async fn in trait` situation.
- `parse-uuid-correctly.md` — UUID parsing during protocol decoding.

## License

MIT — see `LICENSE`.

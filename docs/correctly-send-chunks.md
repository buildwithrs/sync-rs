# Correctly sending file chunks over TCP

File: `src/tcp.rs`
Function: `ClientFileProcessor::send_chunks` (lines 97–175)

This document explains the bugs in the original "oneshot + select!" implementation
of `send_chunks`, why they made the function incorrect, and how the rewritten
version fixes them. The rewritten code is the one currently on `main`.

---

## Stated intent

1. Send an `UploadInit` event.
2. Send all file chunks, possibly in parallel, with bounded concurrency.
3. After every chunk has been written to the TCP stream, send the `UploadDone`
   event to signal completion to the server.
4. Only then return success to the caller.

In other words: **"UploadDone must arrive strictly after every chunk has been
flushed, and `Ok(())` must mean everything was flushed."**

---

## Original (buggy) implementation — what it tried to do

The original code used a `tokio::sync::oneshot` channel as a "done" signal
between the chunk-producing task and the chunk-writing task:

```rust
let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

tokio::spawn(async move {
    framed_writer.send(Bytes::from(upload_bs)).await.ok();

    tokio::select! {
        _ = shutdown_rx => {
            framed_writer.send(Bytes::from(encode_upload_done(done_event))).await.ok();
            return;
        }
        _ = async {
            while let Some(data) = rx.recv().await {
                if let Err(e) = framed_writer.send(data).await {
                    warn!("...");
                    break;
                }
            }
        } => {}
    };
});

// spawn chunk senders via JoinSet ...
while let Some(r) = join_set.join_next().await { r.expect("..."); }
let _ = shutdown_tx.send(());
Ok(())
```

The author probably reasoned: "after the JoinSet drains, signal `shutdown_tx`,
the writer task will see `shutdown_rx` and write `done_event`."

This is wrong in five ways.

---

## 🔴 Bug 1 — Buffered chunks are silently dropped when `shutdown_rx` fires

The `select!` arm that drains `rx` owns the receiver:

```rust
_ = async {
    while let Some(data) = rx.recv().await { ... }
} => {}
```

When the other arm (`shutdown_rx`) wins the `select!`, this future is **dropped
on the floor**, taking `rx` with it. Dropping `rx` closes the mpsc channel and
discards whatever is still in the 32-slot buffer. Those chunks never reach the
TCP stream, and the server never sees them — yet the client moves on and writes
`done_event` after them.

### Impact

Data loss. The server may receive `[UploadInit, Chunk×K, UploadDone]` with K
*less than* the actual chunk count, and there is no error to surface.

### Why the buffer is non-empty in practice

`tx_c.send(...).await` only returns once the item is in the buffer (or has been
pulled by `rx`). The `JoinSet` waits for that. But the writer task pulling from
`rx` then has to call `framed_writer.send(data).await`, which is also async and
may be slower than the producers. So at any moment several chunks can be sitting
in the buffer waiting to be written to TCP.

---

## 🔴 Bug 2 — `rx` can never naturally complete during `send_chunks`

```rust
let (tx, mut rx) = mpsc::channel::<Bytes>(32);
// ...
let tx_c = tx.clone();      // spawned tasks own tx_c, drop on exit
// ... for loop / JoinSet wait ...
let _ = shutdown_tx.send(());
Ok(())                       // tx is dropped HERE, on return
```

The original `tx` is held in the `send_chunks` stack frame until the function
returns. While it is alive, `rx.recv()` can never return `None` — only the
chunk-sender `tx_c` clones get dropped when their tasks end, but the channel
needs *all* senders gone to close.

This means the chunk-writer arm of the `select!` is **perpetually pending**
during the entire `send_chunks` call. The "race" between `shutdown_rx` and the
chunk-writer arm is therefore one-sided: `shutdown_rx` always wins, and Bug 1
fires every time.

---

## 🔴 Bug 3 — `send_chunks` returns before the writer is done

```rust
let _ = shutdown_tx.send(());
Ok(())
```

The function signals "all chunks sent" and returns immediately, but the spawned
writer task is still running. If the caller drops the `TcpStream`, ends the
process, or otherwise lets the writer task get cancelled before it finishes
flushing `done_event`, the server never receives the done signal — yet the
client reports `Ok(())`.

### Impact

A successful return value is a lie: it does not mean "server has been notified
of completion."

---

## 🟡 Bug 4 — Silent failure on a chunk-write error

Inside the chunk-writer arm:

```rust
while let Some(data) = rx.recv().await {
    if let Err(e) = framed_writer.send(data).await {
        warn!("...");
        break;     // <-- exits the inner loop
    }
}                // <-- implicit return (), no done_event
```

If the TCP write fails, `break` exits the loop, the async block completes, the
`select!` resolves via the chunk-writer arm, the spawn task returns, and **no
`done_event` is ever sent**. The caller of `send_chunks` has no way to learn
about the error (the writer's `JoinHandle` is dropped on the floor along with
the `Result` it would have reported).

---

## 🟡 Bug 5 — Dead code after the `select!`

Lines 151–156 of the original were commented-out code in the *post-`select!`
position* — they could never execute. They were a leftover from the previous
straight-line implementation and made the file harder to read.

---

## ✅ Solution — let `rx` returning `None` *be* the signal

The writer task already knows when it is done: when the producer side of the
channel has been fully drained and closed. We just have to make sure that
actually happens before we return, and propagate the writer's `Result`.

The fix has three parts:

### 1. Move all the work into the writer task, in strict order

```rust
let writer_handle = tokio::spawn(async move {
    // 1. UploadInit
    framed_writer.send(Bytes::from(encode_upload_init(upload_event))).await?;

    // 2. Every chunk
    while let Some(data) = rx.recv().await {
        framed_writer.send(data).await?;
    }

    // 3. UploadDone — rx returned None => every sender dropped, buffer empty
    framed_writer.send(Bytes::from(encode_upload_done(done_event))).await?;

    Ok::<(), SyncError>(())
});
```

Strict ordering: `UploadInit` → all chunks → `UploadDone`. No race because
there is only one writer.

### 2. Explicitly `drop(tx)` after spawning the producers

```rust
let mut join_set = JoinSet::new();
for chunk in chunks {
    if join_set.len() >= MAX_SEND_TASK {
        join_set.join_next().await;
    }
    let tx_c = tx.clone();
    join_set.spawn(async move {
        tx_c.send(encode_chunk_event(chunk).into())
            .await
            .expect("writer task dropped the receiver");
    });
}

drop(tx);                    // <-- the original sender goes away here
```

After this `drop`, the only senders left are the per-task `tx_c` clones. When
they all complete, the channel is fully closed and `rx` will return `None` —
*that* is when the writer knows it is done.

### 3. Await the writer before returning

```rust
while let Some(r) = join_set.join_next().await {
    r.expect("chunk sender task failed");
}

writer_handle.await.expect("writer task panicked")?;
Ok(())
```

By the time `writer_handle.await` returns `Ok(())`:
- the writer has flushed `UploadInit`,
- the writer has drained `rx` to `None`,
- the writer has flushed `UploadDone`,
- the TCP write to `done_event` returned success.

So `Ok(())` from `send_chunks` finally means what it says.

---

## Backpressure, preserved

Removing the `oneshot` did not change the backpressure story:

- The mpsc channel is bounded at 32.
- Up to `MAX_SEND_TASK = 8` chunk senders run concurrently.
- A sender blocks on `tx_c.send(...).await` when the buffer is full.
- The writer pulls from `rx` and writes to TCP; if TCP is slow, `rx.recv()`
  parks, the buffer fills, the senders park, and `JoinSet` stops accepting new
  spawns (because `join_set.len() >= MAX_SEND_TASK` is reached and we await
  `join_next()` before spawning another).

So slow TCP still propagates pressure all the way back to chunk production.

---

## Error propagation

| Failure                             | Surfaced via                                       |
|-------------------------------------|----------------------------------------------------|
| Writer fails to write `UploadInit`  | `writer_handle.await?` returns `Err(...)`          |
| Writer fails to write a chunk       | `writer_handle.await?` returns `Err(...)`          |
| Writer fails to write `UploadDone`  | `writer_handle.await?` returns `Err(...)`          |
| Writer panics                       | `writer_handle.await.expect(...)` panics           |
| A chunk sender fails (rx dropped)   | `join_next().expect(...)` panics (writer panic or early return) |

In the last case the panic is loud but does not carry the writer's original
error. If we want to preserve it, the cleanest follow-up is to `await` the
writer *before* draining `JoinSet`, and only drain it afterwards — but for a
sync tool a panic on this rare path is acceptable today.

---

## Summary

| # | Severity | Original bug                                              | Fix                                             |
|---|----------|-----------------------------------------------------------|-------------------------------------------------|
| 1 | 🔴 Major | `select!` drop discards buffered chunks                   | Writer drains `rx` to `None`; no `select!`      |
| 2 | 🔴 Major | `tx` held until return, so `rx` never closes              | Explicit `drop(tx)` after spawning producers    |
| 3 | 🔴 Major | `send_chunks` returns before writer is done               | `await writer_handle` before returning          |
| 4 | 🟡 Minor | Chunk-write error skips `done_event` silently            | Writer `?`-propagates every TCP error           |
| 5 | 🟡 Minor | Dead code after `select!`                                | Removed entirely                                |

The key insight: **you don't need an explicit "done" signal here.** The mpsc
receiver returning `None` already *is* "every chunk has been queued by
producers and drained by the consumer." Wiring the `done_event` write to that
event (and `await`ing the writer) gives you the ordering you wanted for free,
with no race window for lost chunks.

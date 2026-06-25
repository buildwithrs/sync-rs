# File creation fails: "creating or truncating a file requires write or append access"

The server's `create_server_file` in `src/tcp.rs` was failing at `open()` with
`tokio::io::Error` of kind `InvalidInput`:

```
2026-06-25T03:50:15.984921Z  WARN sync_rs::tcp: failed handle stream: \
    failed to send data: creating or truncating a file requires write or append access
```

The misleading part: `write(true)` *was* set. The error message is correct but
points at the wrong rule.

## The cause: `append(true)` + `truncate(true)` is an invalid combination

`std::fs::OpenOptions` runs **two** validations before opening the file, both in
`library/std/src/sys/fs/unix.rs`. The first one (the one the error message is
written for) checks that at least one of read/write/append is set. The second one
— `get_creation_mode` — has this arm:

```rust
match (self.write, self.append) {
    (true, false) => {}                              // OK
    (false, false) => { /* error if create/truncate */ }
    (_, true) => {                                   // append is set
        if self.truncate && !self.create_new {
            return Err(io::Error::new(
                InvalidInput,
                "creating or truncating a file requires write or append access",
            ));
        }
    }
}
```

The std lib refuses to open a file in append mode that also gets truncated — on
POSIX, `O_APPEND` ("writes go to the end") and `O_TRUNC` ("file starts at length 0")
are contradictory, so the lib bails out before the syscall. The error string
mentions "write or append access" because that's the same string used by the
*other* validation, which makes it look like write is missing when it isn't.

The original code at `src/tcp.rs:316` had every flag set:

```rust
let f = opts
    .read(true)
    .write(true)
    .append(true)
    .create(true)
    .truncate(true)
    .open(fp).await?;
```

— but `append + truncate` is enough to trigger the rejection, no matter what
else is set.

## Reproducer

Six chain variants, same destination dir, all using `tokio::fs::OpenOptions`:

| chain                                       | result |
|---------------------------------------------|--------|
| `read+write+append+create+truncate`         | **fail** |
| `read+write+create+truncate`                | ok     |
| `write+append+create+truncate`              | **fail** |
| `write+create+truncate`                     | ok     |
| `append+create+truncate`                    | **fail** |
| `create+truncate+write+append+read` (reversed) | **fail** |

The pattern: anything with `append(true)` *and* `truncate(true)` (without
`create_new(true)`) fails. The order of method calls on `OpenOptions` does not
matter.

## The fix

Drop `append(true)`. The current `create_server_file` (src/tcp.rs:316):

```rust
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
```

`truncate(true)` was also removed in the same pass. The function is only called
once per upload during `handle_upload_init`, so truncation isn't required — for a
fresh upload the file doesn't exist yet, and for a resumed upload the file's
existing size was already set by the previous `set_len` call.

`handle_chunk` at `src/tcp.rs:293` is a different code path and is left as
`read + append` (no `create`, no `truncate`) — that combination is fine, the
append-vs-truncate rule doesn't apply there.

## Lesson

When `OpenOptions::open` returns "creating or truncating a file requires write or
append access", the first thing to check is whether `append` and `truncate` are
both set. The error message comes from `get_creation_mode`, not
`get_access_mode`, so the missing access mode it complains about is *append
itself being used with truncate*, not any of the access flags being unset.

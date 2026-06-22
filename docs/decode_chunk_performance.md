# `decode_chunk` Performance Analysis

File: `src/protocol.rs`
Function: `decode_chunk(bs: Bytes) -> Result<Chunk, SyncError>` (lines 34–56)

This document describes the performance issues found in `decode_chunk`.
**No fixes are applied** — it only explains what the issue is and how it would be fixed.

---

## 🔴 Main Issue — Unnecessary allocation + memcpy of chunk data (lines 46–48)

```rust
let d = &bs[40..];
let data_vec = d.to_vec();          // <-- allocates new Vec<u8> + copies all bytes
let data = Bytes::from(data_vec);   // <-- then re-wraps it in Bytes
```

### What is happening

The input `bs: Bytes` is already a reference-counted buffer that supports cheap,
zero-copy slicing. But the current code:

1. Borrows a slice `&bs[40..]` — this is zero-copy, just bumps a refcount and
   adjusts a pointer.
2. Calls `to_vec()` on the `&[u8]` — **allocates a brand-new heap buffer of
   `len - 40` bytes and memcpys every byte of the payload into it.**
3. Wraps that `Vec<u8>` in `Bytes` via `Bytes::from(Vec<u8>)` — another thin
   wrapper, no copy at this step, but the allocation + memcpy have already
   happened.

### Impact

For every chunk received over the wire, the decode path pays:

- **One extra heap allocation** sized to the chunk payload.
- **One full memcpy of the entire chunk payload**.

This is exactly the cost that the `bytes::Bytes` API was designed to avoid.
For large or high-frequency chunks this is the dominant cost of `decode_chunk`.

### How to fix it

Use `Bytes::slice`, which is O(1) (refcount bump + pointer/length adjustment,
no copy, no allocation):

```rust
let data = bs.slice(40..);          // zero-copy
```

Then pass `data` straight into `Chunk::with_offset(...)`. The data portion of
`decode_chunk` becomes zero-copy.

---

## 🟡 Minor Issue 1 — 32-byte hash copy (lines 43–44)

```rust
let mut ck_hash = [0u8; 32];
ck_hash.copy_from_slice(hash);
```

### What is happening

The 32 bytes of the chunk hash are copied from `&bs[8..40]` into a stack array
`ck_hash: [u8; 32]` before being passed to `ChunkHash::new`.

### Impact

This is a single 32-byte memcpy on the stack. It is unavoidable in the current
shape because `ChunkHash::new([u8; 32])` requires an owned fixed-size array.

In absolute terms this is tiny (32 bytes) and is dwarfed by the main issue
above, so it is not worth restructuring on its own.

### How to fix it (optional)

Expose a constructor on `ChunkHash` that takes a borrowed slice, e.g.:

```rust
impl ChunkHash {
    pub fn from_slice(s: &[u8]) -> Result<Self, ...> { ... }
    // or
    // impl TryFrom<&[u8]> for ChunkHash
}
```

Then `ck_hash` could be skipped and the hash built directly from `&bs[8..40]`,
removing the stack-buffer + copy. Only worth doing if `chunkrs` is in scope.

---

## 🟡 Minor Issue 2 — Redundant `try_into` + error mapping (lines 41, 50–52)

```rust
let offset_bs = &bs[0..8];
...
let offset_arr = offset_bs
    .try_into()
    .map_err(|e: TryFromSliceError| SyncError::StdIOError(e.to_string()))?;
let chunk =
    Chunk::with_offset(data, u64::from_be_bytes(offset_arr)).set_hash(ChunkHash::new(ck_hash));
```

### What is happening

The function has already validated `bs.len() >= 40` at the top, so `offset_bs`
is guaranteed to be exactly 8 bytes. The `try_into` therefore cannot fail, and
the `Result` + `map_err` branch is dead complexity.

### Impact

Not a real performance hotspot — it just adds an `enum` discriminant and a
branch on the error path that is never taken. It is also a small readability
issue: an infallible operation is dressed up as fallible.

### How to fix it

Skip the `Result` plumbing and call `try_into().unwrap()` directly, or use a
pattern that makes the infallibility obvious:

```rust
let offset = u64::from_be_bytes(bs[..8].try_into().unwrap());
```

Pure code-cleanliness win, not a meaningful perf change.

---

## Summary

| # | Location | Severity | Cost                                                  | Fix                                |
|---|----------|----------|-------------------------------------------------------|------------------------------------|
| 1 | `decode_chunk` lines 46–48 | 🔴 Major | 1 heap alloc + 1 full-payload memcpy per chunk        | `bs.slice(40..)` instead of `to_vec()` + `Bytes::from` |
| 2 | `decode_chunk` lines 43–44 | 🟡 Minor | 32-byte stack copy                                    | `ChunkHash::from_slice` if exposed |
| 3 | `decode_chunk` lines 50–52 | 🟡 Minor | Dead `Result` branch (length already validated above) | Inline `try_into().unwrap()`       |

The one real performance bug is **#1**: every decoded chunk currently costs an
extra heap allocation and an extra full-payload memcpy that the `Bytes` API
was specifically designed to avoid. Replacing it with `bs.slice(40..)` makes
the data portion of `decode_chunk` zero-copy.
# UUID parse failure: raw bytes vs. hex string

The decode functions in `src/protocol.rs` were trying to parse the `file_id` as a UUID
string, but the encoder writes it as **raw 16 bytes**. `Uuid::parse_str` then failed on
the first non-hex byte and the unit test panicked with
`UUidError(Error(ParseChar { character: '\t', index: 1 }))`.

## The mismatch

The spec at the top of `src/protocol.rs` defines `file_id: [u8;16]`, and the encoders
follow it:

```rust
// encode_upload_done / encode_chunk_event / encode_upload_init
encode_bs.put_slice(&d.file_id.into_bytes());   // 16 raw bytes, big-endian
```

But three decoders were treating those 16 bytes as a UTF-8 string and handing the
result to `Uuid::parse_str`:

```rust
let file_id = String::from_utf8_lossy(&bs[..16]);
file_id: uuid::Uuid::parse_str(&file_id)?,   // expects "xxxxxxxx-xxxx-..."
```

`Uuid::parse_str` only accepts the canonical hex-with-dashes form like
`39092f6c-59f2-4d88-a049-a270de166bc0`. The 16 raw bytes produced by `into_bytes()`
are arbitrary values — for a UUID whose big-endian byte form starts with `0x39, 0x09`,
the second byte decodes to `'\t'` (tab), which is not a valid hex digit, so the parser
fails at index 1.

The same bug was in all three decoders:

- `decode_upload_done` (src/protocol.rs:141)
- `decode_chunk_event` (src/protocol.rs:222)
- `decode_upload_init` (src/protocol.rs:258)

## The fix

Read the 16 bytes as a fixed array and call `Uuid::from_bytes`:

```rust
let mut file_id_arr = [0u8; 16];
file_id_arr.copy_from_slice(&bs[..16]);
file_id: uuid::Uuid::from_bytes(file_id_arr),
```

To avoid the same mistake being reintroduced in a fourth place, the shared version
was extracted into a helper at `src/protocol.rs:282`:

```rust
fn bytes_to_uid(bs: &mut BytesMut) -> Uuid {
    let mut uid_arr = [0u8; 16];
    uid_arr.copy_from_slice(&bs[..16]);
    Uuid::from_bytes(uid_arr)
}
```

All three decoders now call `bytes_to_uid(bs)`.

## Why the round-trip unit tests catch this

A round-trip test (encode → consume tag → decode → `assert_eq!`) is a load-bearing
check here: it forces encoder and decoder to agree on the wire format. As long as
encode uses `into_bytes()` and decode uses `from_bytes`, the original `file_id` comes
back unchanged. If either side flips the convention (e.g. someone "fixes" the encoder
to write a hex string to "match" the parser), the round-trip test fails and the
divergence is caught immediately.

The new tests in `src/protocol.rs::tests` cover all four event types:
`test_encode_decode_upload_init`, `test_encode_decode_chunk_event`,
`test_encode_decode_err_msg`, `test_encode_decode_upload_done`.

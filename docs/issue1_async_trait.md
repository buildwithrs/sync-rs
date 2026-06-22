# error[E0195]: lifetime parameters or bounds on method `send` do not match the trait declaration

The `impl SyncSendStream for TCPClient` block in `src/tcp.rs` is missing the
  `#[async_trait]` attribute that the trait has. This causes the lifetime mismatch because

The lifetime error was in src/tcp.rs — the impl SyncSendStream for TCPClient block was
  missing the #[async_trait] attribute that the trait uses.

  Why this caused a lifetime error: The `#[async_trait]` macro rewrites async fn into a regular fn
  returning a `Pin<Box<dyn Future + Send + 'async_trait>>`. The trait declaration therefore carries an
  implicit lifetime bound on the method that the impl must match — without the attribute on the impl,
  the lifetimes diverged and the compiler reported E0195 ("lifetimes in impl do not match this method
  in trait").

  Fix applied (src/tcp.rs:6, :9): added use async_trait::async_trait; and #[async_trait] on the impl
  block. src/protocol.rs was already correct — no change needed there.
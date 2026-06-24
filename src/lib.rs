pub mod chunk; // chunk the file into multiple parts
pub mod protocol; // how to send the data through TCP/UDP connnection.
pub mod tcp;  // the implement of TCP data transfer
pub mod errors; // all the app errors
pub mod config;


use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

pub fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,debug"));
    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer().with_target(true))
        .init();
}
#![recursion_limit="512"]
#![feature(try_trait)]
/*
pub use futures;
pub use tokio;
pub use sp_dto;
pub use proto::{ServerConfig, ClientMsg, ProcessStream, ProcessEventRaw, ProcessRpcRaw, ProcessEvent, ProcessRpc, StreamStartup, Startup, MagicBall, ProcessError, RestreamMsg};
#[cfg(not(feature = "fs"))]
pub use broker::server::{start, start_future};
#[cfg(feature = "fs")]
pub use fs::server::{start, start_future};
#[cfg(not(feature = "fs"))]
pub use broker::client::{stream_mode, full_message_raw_mode, full_message_mode};
#[cfg(feature = "fs")]
pub use fs::client::magic_ball;

#[cfg(not(feature = "fs"))]
mod broker {
    pub mod server;
    pub mod client;
}
#[cfg(feature = "fs")]
mod fs {
    pub mod server;
    pub mod client;
}
*/
mod proto;

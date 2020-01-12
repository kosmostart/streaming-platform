#![recursion_limit="1024"]
#![feature(try_trait)]
pub use futures;
pub use tokio;
pub use sp_dto;
pub use proto::{get_stream_id, ServerConfig, ClientMsg, StreamLayout, StreamCompletion, ProcessStream, ProcessEventRaw, ProcessRpcRaw, ProcessEvent, ProcessRpc, StreamStartup, Startup, MagicBall, ProcessError, RestreamMsg};
pub use server::{start, start_future};
pub use client::{stream_mode, full_message_raw_mode, full_message_mode};

mod proto;
pub mod server;
pub mod client;

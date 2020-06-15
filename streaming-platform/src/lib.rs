#![recursion_limit="1024"]
#![feature(try_trait)]
pub use futures;
pub use tokio;
pub use sp_dto;
pub use proto::{LEN_BUF_SIZE, LENS_BUF_SIZE, DATA_BUF_SIZE, ServerConfig, ClientMsg, StreamLayout, StreamCompletion, ProcessStream, ProcessEvent, ProcessRpc, StreamStartup, Startup, MagicBall, ProcessError, RestreamMsg, StreamUnit};
pub use server::{start, start_future};
pub use client::{stream_mode, full_message_mode};

mod proto;
pub mod server;
pub mod client;
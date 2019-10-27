#![recursion_limit="512"]
#![feature(try_trait)]
pub use tokio;
pub use sp_dto;
pub use proto::{ClientMsg, Mode};
#[cfg(not(feature = "fs"))]
pub use broker::server::{start, start_future};
#[cfg(feature = "fs")]
pub use fs::server::{start, start_future};
#[cfg(not(feature = "fs"))]
pub use broker::client::{magic_ball, connect_future};
#[cfg(feature = "fs")]
pub use fs::client::{magic_ball, connect_future};

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
mod proto;

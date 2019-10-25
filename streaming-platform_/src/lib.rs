mod proto;
mod simple {
    pub mod server;
    pub mod client;
}
//mod tls;
mod error;

pub use crate::simple::{server::{start, start_with_link}, client::{connect, connect2}};
pub use crate::proto::{MagicBall, MagicBall2, ClientKind};
//pub use crate::tls::{start_tls, connect_tls};
pub use crate::error::Error;

#[derive(Clone)]
pub struct AuthData {

}

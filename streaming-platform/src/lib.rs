mod proto;
mod simple {
    pub mod server;
    pub mod client;
}
//mod tls;
mod error;

pub use crate::simple::{server::start, client::connect, client::connect2};
//pub use crate::tls::{start_tls, connect_tls};
pub use crate::error::Error;

#[derive(Clone)]
pub struct AuthData {

}

#[derive(Clone)]
pub struct Config {

}

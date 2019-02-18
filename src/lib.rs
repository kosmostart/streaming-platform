mod proto;
mod simple;
//mod tls;
mod error;

pub use crate::simple::{start, connect, connect2};
//pub use crate::tls::{start_tls, connect_tls};
pub use crate::error::Error;

#[derive(Clone)]
pub struct AuthData {

}

#[derive(Clone)]
pub struct Config {

}

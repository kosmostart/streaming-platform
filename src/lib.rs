mod proto;
mod simple;
//mod tls;
mod error;

pub use crossbeam;
pub use crate::proto::{Sender2};
pub use crate::simple::{start, connect};
//pub use crate::tls::{start_tls, connect_tls};
pub use error::Error;

#[derive(Clone)]
pub struct AuthData {

}

#[derive(Clone)]
pub struct Config {

}

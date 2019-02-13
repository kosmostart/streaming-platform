mod proto;

//pub use auth_token::AuthData;

pub use crossbeam;
pub use ws::Message;
pub use crate::proto::Route;
pub use crate::proto::{start, start_tls, connect, connect_tls};

#[derive(Clone)]
pub struct AuthData {

}

#[derive(Clone)]
pub struct Config {

}

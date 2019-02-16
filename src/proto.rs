use serde::{Serialize, Deserialize};
use ws::{Message, Sender};

pub enum ServerMsg {
    AddClient(String, Sender),
    RemoveClient(String),
    SendMsg(String, Message)
}

pub enum Route {
    ToSender(String),
    ToClient(String, String),
    Broadcast(String),
    Disconnect
}

pub struct Sender2 {
    sender: Sender
}
pub struct Message2<T> where T: Serialize, for<'de> T: serde::Deserialize<'de> {
    pub payload: T
}
impl Sender2 {
    //fn send(msg: )
}

use bytes::BufMut;
use serde_derive::{Serialize, Deserialize};
use ws::{Message, Sender};
use crate::error::Error;

pub enum ClientKind {
    App,
    Service
}
pub enum ServerMsg {
    AddClient(String, Sender),
    RemoveClient(String),
    SendMsg(String, Vec<u8>)
}

#[derive(Clone)]
pub struct MagicBall {
    sender: Sender
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MsgMeta {
    pub addr: String
}

impl MagicBall {
    pub fn new(sender: Sender) -> MagicBall {
        MagicBall {
            sender
        }
    }
    pub fn send<T>(&self, addr: String, payload: T) -> Result<(), Error> where T: serde::Serialize, for<'de> T: serde::Deserialize<'de> {
        
        let msg_meta = MsgMeta {
            addr
        };

        let mut msg_meta = serde_json::to_vec(&msg_meta)?;
        let mut payload = serde_json::to_vec(&payload)?;

        let mut buf = vec![];

        buf.put_u32_be(msg_meta.len() as u32);

        buf.append(&mut msg_meta);
        buf.append(&mut payload);

        self.sender.send(Message::Binary(buf));
        
        Ok(())
    }
}

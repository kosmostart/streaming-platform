use std::{marker::PhantomData, fmt::Debug};
use log::*;
use bytes::{Buf, BufMut};
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
pub struct MagicBall<T, R> where T: serde::Serialize, for<'de> T: serde::Deserialize<'de>, R: serde::Serialize, for<'de> R: serde::Deserialize<'de> {
    phantom_data_for_T: PhantomData<T>,
    phantom_data_for_R: PhantomData<R>,
    sender: Sender,
    rx: crossbeam::channel::Receiver<Vec<u8>>
}

#[derive(Clone)]
pub struct MagicBall2 {    
    sender: Sender,
    rx: crossbeam::channel::Receiver<Vec<u8>>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MsgMeta {
    pub addr: String
}

impl<T, R> MagicBall<T, R> where T: Debug, T: serde::Serialize, for<'de> T: serde::Deserialize<'de>, R: Debug, R: serde::Serialize, for<'de> R: serde::Deserialize<'de> {
    pub fn new(sender: Sender, rx: crossbeam::channel::Receiver<Vec<u8>>) -> MagicBall<T, R> {
        MagicBall {
            phantom_data_for_T: PhantomData,
            phantom_data_for_R: PhantomData,
            sender,
            rx
        }
    }
    pub fn send(&self, addr: String, payload: T) -> Result<(), Error> {
        
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
    pub fn recv(&self) -> Result<(String, R), Error> {
        let data = self.rx.recv()?;

        let len = {
            let mut buf = std::io::Cursor::new(&data);
            buf.get_u32_be() as usize
        };
        
        let msg_meta = serde_json::from_slice::<MsgMeta>(&data[4..len + 4])?;
        let payload = serde_json::from_slice::<R>(&data[len + 4..])?;

        info!("Received message, {:#?} {:#?}", msg_meta, payload);

        Ok((msg_meta.addr, payload))
    }
}

impl MagicBall2 {
    pub fn new(sender: Sender, rx: crossbeam::channel::Receiver<Vec<u8>>) -> MagicBall2 {
        MagicBall2 {            
            sender,
            rx
        }
    }
    pub fn send(&self, addr: String, mut payload: Vec<u8>) -> Result<(), Error> {
        
        let msg_meta = MsgMeta {
            addr
        };

        let mut msg_meta = serde_json::to_vec(&msg_meta)?;        

        let mut buf = vec![];

        buf.put_u32_be(msg_meta.len() as u32);

        buf.append(&mut msg_meta);
        buf.append(&mut payload);

        self.sender.send(Message::Binary(buf));
        
        Ok(())
    }
    pub fn recv(&self) -> Result<(String, Vec<u8>), Error> {
        let data = self.rx.recv()?;

        let len = {
            let mut buf = std::io::Cursor::new(&data);
            buf.get_u32_be() as usize
        };
        
        let msg_meta = serde_json::from_slice::<MsgMeta>(&data[4..len + 4])?;
        let payload = &data[len + 4..];

        info!("Received message, {:#?}", msg_meta);

        Ok((msg_meta.addr, payload.to_vec()))
    }
}


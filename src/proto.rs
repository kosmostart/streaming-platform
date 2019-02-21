use std::{marker::PhantomData, fmt::Debug};
use log::*;
use bytes::{Buf, BufMut};
use serde_derive::{Serialize, Deserialize};
use ws::{Message, Sender};
use uuid::Uuid;
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

pub enum ClientMsg {
    AddRpc(Uuid, crossbeam::channel::Sender<(MsgMeta, usize, Vec<u8>)>),
    RemoveRpc(Uuid),
    RpcDataRequest(Uuid),
    RpcDataResponse(Uuid, crossbeam::channel::Sender<(MsgMeta, usize, Vec<u8>)>)
}

#[derive(Clone)]
pub struct MagicBall<T, R> where T: serde::Serialize, for<'de> T: serde::Deserialize<'de>, R: serde::Serialize, for<'de> R: serde::Deserialize<'de> {
    phantom_data_for_T: PhantomData<T>,
    phantom_data_for_R: PhantomData<R>,
    addr: String,
    sender: Sender,
    rx: crossbeam::channel::Receiver<(MsgMeta, usize, Vec<u8>)>,
    rpc_request_rx: crossbeam::channel::Receiver<(MsgMeta, usize, Vec<u8>)>,
    rpc_tx: crossbeam::channel::Sender<ClientMsg>
}

#[derive(Clone)]
pub struct MagicBall2 {
    addr: String,
    sender: Sender,
    rx: crossbeam::channel::Receiver<(MsgMeta, usize, Vec<u8>)>,
    rpc_request_rx: crossbeam::channel::Receiver<(MsgMeta, usize, Vec<u8>)>,
    rpc_tx: crossbeam::channel::Sender<ClientMsg>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MsgMeta {
    pub tx: String,
    pub rx: String,
    pub kind: MsgKind,
    pub correlation_id: Option<Uuid>
}

#[derive(Debug, Serialize, Deserialize)]
pub enum MsgKind {
    Event,
    RpcRequest,
    RpcResponse
}

impl<T, R> MagicBall<T, R> where T: Debug, T: serde::Serialize, for<'de> T: serde::Deserialize<'de>, R: Debug, R: serde::Serialize, for<'de> R: serde::Deserialize<'de> {
    pub fn new(addr: String, sender: Sender, rx: crossbeam::channel::Receiver<(MsgMeta, usize, Vec<u8>)>, rpc_request_rx: crossbeam::channel::Receiver<(MsgMeta, usize, Vec<u8>)>, rpc_tx: crossbeam::channel::Sender<ClientMsg>) -> MagicBall<T, R> {
        MagicBall {
            phantom_data_for_T: PhantomData,
            phantom_data_for_R: PhantomData,
            addr,
            sender,
            rx,
            rpc_request_rx,
            rpc_tx
        }
    }
    pub fn send_event(&self, addr: String, payload: T) -> Result<(), Error> {        
        let msg_meta = MsgMeta {
            tx: self.addr.clone(),
            rx: addr,
            kind: MsgKind::Event,
            correlation_id: None
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
    pub fn reply_to_rpc(&self, addr: String, correlation_id: Option<Uuid>, payload: T) -> Result<(), Error> {        
        if correlation_id.is_none() {
            return  Err(Error::EmptyCorrelationIdPassed);
        }

        let msg_meta = MsgMeta {
            tx: self.addr.clone(),
            rx: addr,
            kind: MsgKind::RpcResponse,
            correlation_id            
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
    pub fn recv_event(&self) -> Result<(MsgMeta, R), Error> {
        let (msg_meta, len, data) = self.rx.recv()?;            

        let payload = serde_json::from_slice::<R>(&data[len + 4..])?;        

        Ok((msg_meta, payload))
    }
    pub fn recv_rpc_request(&self) -> Result<(MsgMeta, R), Error> {
        let (msg_meta, len, data) = self.rpc_request_rx.recv()?;            

        let payload = serde_json::from_slice::<R>(&data[len + 4..])?;        

        Ok((msg_meta, payload))
    }
    pub fn rpc(&self, addr: String, mut payload: Vec<u8>) -> Result<(MsgMeta, R), Error> {
        let correlation_id = Uuid::new_v4();

        let msg_meta = MsgMeta {
            tx: self.addr.clone(),
            rx: addr,
            kind: MsgKind::RpcRequest,
            correlation_id: Some(correlation_id)
        };

        let mut msg_meta = serde_json::to_vec(&msg_meta)?;        

        let mut buf = vec![];

        buf.put_u32_be(msg_meta.len() as u32);

        buf.append(&mut msg_meta);
        buf.append(&mut payload);

        let (rpc_tx, rpc_rx) = crossbeam::channel::unbounded();
        
        self.rpc_tx.send(ClientMsg::AddRpc(correlation_id, rpc_tx));
        
        self.sender.send(Message::Binary(buf));

        let res = match self.rx.recv() {
            Ok((msg_meta, len, data)) => {
                let payload = &data[len + 4..];
                let payload = serde_json::from_slice::<R>(&data[len + 4..])?;
                Ok((msg_meta, payload))
            }
            Err(err) => Err(err)?
        };

        self.rpc_tx.send(ClientMsg::RemoveRpc(correlation_id));

        res
    }
}

impl MagicBall2 {
    pub fn new(addr: String, sender: Sender, rx: crossbeam::channel::Receiver<(MsgMeta, usize, Vec<u8>)>, rpc_request_rx: crossbeam::channel::Receiver<(MsgMeta, usize, Vec<u8>)>, rpc_tx: crossbeam::channel::Sender<ClientMsg>) -> MagicBall2 {
        MagicBall2 {
            addr,
            sender,
            rx,
            rpc_request_rx,
            rpc_tx
        }
    }
    pub fn send_event(&self, addr: String, mut payload: Vec<u8>) -> Result<(), Error> {        
        let msg_meta = MsgMeta {
            tx: self.addr.clone(),
            rx: addr,
            kind: MsgKind::Event,
            correlation_id: None
        };

        let mut msg_meta = serde_json::to_vec(&msg_meta)?;        

        let mut buf = vec![];

        buf.put_u32_be(msg_meta.len() as u32);

        buf.append(&mut msg_meta);
        buf.append(&mut payload);

        self.sender.send(Message::Binary(buf));
        
        Ok(())
    }
    pub fn reply_to_rpc(&self, addr: String, correlation_id: Option<Uuid>, mut payload: Vec<u8>) -> Result<(), Error> {        
        if correlation_id.is_none() {
            return  Err(Error::EmptyCorrelationIdPassed);
        }

        let msg_meta = MsgMeta {
            tx: self.addr.clone(),
            rx: addr,
            kind: MsgKind::RpcResponse,
            correlation_id
        };

        let mut msg_meta = serde_json::to_vec(&msg_meta)?;        

        let mut buf = vec![];

        buf.put_u32_be(msg_meta.len() as u32);

        buf.append(&mut msg_meta);
        buf.append(&mut payload);

        self.sender.send(Message::Binary(buf));
        
        Ok(())
    }
    pub fn recv_event(&self) -> Result<(MsgMeta, Vec<u8>), Error> {
        let (msg_meta, len, data) = self.rx.recv()?;            
        let payload = &data[len + 4..];        

        Ok((msg_meta, payload.to_vec()))
    }
    pub fn recv_rpc_request(&self) -> Result<(MsgMeta, Vec<u8>), Error> {
        let (msg_meta, len, data) = self.rpc_request_rx.recv()?;                
        let payload = &data[len + 4..];        

        Ok((msg_meta, payload.to_vec()))
    }
    pub fn rpc(&self, addr: String, mut payload: Vec<u8>) -> Result<(MsgMeta, Vec<u8>), Error> {
        let correlation_id = Uuid::new_v4();

        let msg_meta = MsgMeta {
            tx: self.addr.clone(),
            rx: addr,
            kind: MsgKind::RpcRequest,
            correlation_id: Some(correlation_id)
        };

        let mut msg_meta = serde_json::to_vec(&msg_meta)?;        

        let mut buf = vec![];

        buf.put_u32_be(msg_meta.len() as u32);

        buf.append(&mut msg_meta);
        buf.append(&mut payload);

        let (rpc_tx, rpc_rx) = crossbeam::channel::unbounded();
        
        self.rpc_tx.send(ClientMsg::AddRpc(correlation_id, rpc_tx));
        
        self.sender.send(Message::Binary(buf));

        let res = match self.rx.recv() {
            Ok((msg_meta, len, data)) => {
                let payload = &data[len + 4..];        
                Ok((msg_meta, payload.to_vec()))
            }
            Err(err) => Err(err)?
        };

        self.rpc_tx.send(ClientMsg::RemoveRpc(correlation_id));

        res
    }
}

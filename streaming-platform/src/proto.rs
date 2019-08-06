use std::{marker::PhantomData, fmt::Debug};
use log::*;
use sp_dto::bytes::{Buf, BufMut};
use serde_derive::{Serialize, Deserialize};
use ws::{Message, Sender};
use sp_dto::uuid::Uuid;
use sp_dto::*;
use crate::error::Error;

#[derive(Debug, Clone)]
pub enum ClientKind {
    App,
    Service,
    Hub
}
pub enum ServerMsg {
    AddClient(String, Sender),
    RemoveClient(String),
    SendMsg(String, Vec<u8>)
}

#[derive(Debug)]
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
	pub fn get_addr(&self) -> String {
		self.addr.clone()
	}
    pub fn send_event(&self, addr: &str, key: &str, payload: T, source: MsgSource) -> Result<(), Error> {        
        let dto = event_dto(self.addr.clone(), addr.to_owned(), key.to_owned(), payload, source)?;

        self.sender.send(Message::Binary(dto));
        
        Ok(())
    }    
    pub fn reply_to_rpc(&self, addr: String, key: String, correlation_id: Uuid, payload: R, source: MsgSource) -> Result<(), Error> {        
        let dto = reply_to_rpc_dto(self.addr.clone(), addr, key, correlation_id, payload, source)?;

        self.sender.send(Message::Binary(dto));
        
        Ok(())
    }
    pub fn recv_event(&self) -> Result<(MsgMeta, R), Error> {
        let (msg_meta, len, data) = self.rx.recv()?;            

        let payload = serde_json::from_slice::<R>(&data[len + 4..])?;        

        Ok((msg_meta, payload))
    }
    pub fn recv_rpc_request(&self) -> Result<(MsgMeta, T), Error> {
        let (msg_meta, len, data) = self.rpc_request_rx.recv()?;            

        let payload = serde_json::from_slice::<T>(&data[len + 4..])?;        

        Ok((msg_meta, payload))
    }
    pub fn rpc(&self, addr: &str, key: &str, payload: T, source: MsgSource) -> Result<(MsgMeta, R), Error> {
		info!("rpc call, addr: {}, payload: {:#?}, source: {:#?}", addr, payload, source);
		
        let (correlation_id, dto) = rpc_dto_with_correlation_id(self.addr.clone(), addr.to_owned(), key.to_owned(), payload, source)?;
        let (rpc_tx, rpc_rx) = crossbeam::channel::unbounded();
        
        self.rpc_tx.send(ClientMsg::AddRpc(correlation_id, rpc_tx));        
        self.sender.send(Message::Binary(dto));

        let res = match rpc_rx.recv_timeout(std::time::Duration::from_secs(30)) {
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
	pub fn get_addr(&self) -> String {
		self.addr.clone()
	}
    pub fn send_event(&self, addr: &str, key: &str, mut payload: Vec<u8>, source: MsgSource) -> Result<(), Error> {                
        let dto = event_dto2(self.addr.clone(), addr.to_owned(), key.to_owned(), payload, source)?;

        self.sender.send(Message::Binary(dto));
        
        Ok(())
    }
    pub fn send_data(&self, data: Vec<u8>) -> Result<(), Error> {
        self.sender.send(Message::Binary(data));
        
        Ok(())
    }
    pub fn reply_to_rpc(&self, addr: &str, key: &str, correlation_id: Uuid, mut payload: Vec<u8>, source: MsgSource) -> Result<(), Error> {        
        let dto = reply_to_rpc_dto2(self.addr.clone(), addr.to_owned(), key.to_owned(), correlation_id, payload, source)?;        

        self.sender.send(Message::Binary(dto));
        
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
    pub fn rpc(&self, addr: &str, key: &str, mut payload: Vec<u8>, source: MsgSource) -> Result<(MsgMeta, Vec<u8>), Error> {
        let (correlation_id, dto) = rpc_dto_with_correlation_id_2(self.addr.clone(), addr.to_owned(), key.to_owned(), payload, source)?;

        let (rpc_tx, rpc_rx) = crossbeam::channel::unbounded();
        
        self.rpc_tx.send(ClientMsg::AddRpc(correlation_id, rpc_tx));
        
        self.sender.send(Message::Binary(dto));

        let res = match rpc_rx.recv_timeout(std::time::Duration::from_secs(30)) {
            Ok((msg_meta, len, data)) => {
                let payload = &data[len + 4..];        
                Ok((msg_meta, payload.to_vec()))
            }
            Err(err) => Err(err)?
        };

        self.rpc_tx.send(ClientMsg::RemoveRpc(correlation_id));

        res
    }
    pub fn proxy_event(&self, tx: String, mut data: Vec<u8>) -> Result<(), Error> {
        let (res, len) = {
            let mut buf = std::io::Cursor::new(&data);
            let len = buf.get_u32_be() as usize;

            match len > data.len() - 4 {
                true => {
                    let custom_error = std::io::Error::new(std::io::ErrorKind::Other, "oh no!");
                    return Err(Error::Io(custom_error));
                }
                false => (serde_json::from_slice::<MsgMeta>(&data[4..len + 4]), len)
            }
        };

        let mut msg_meta = res?;

        msg_meta.tx = tx;

        let mut msg_meta = serde_json::to_vec(&msg_meta)?;
                                                      
        let mut payload_with_attachments: Vec<_> = data.drain(4 + len..).collect();
        let mut buf = vec![];

        buf.put_u32_be(msg_meta.len() as u32);

        buf.append(&mut msg_meta);
        buf.append(&mut payload_with_attachments);

        self.sender.send(Message::Binary(buf));
        
        Ok(())
    }
    pub fn proxy_rpc(&self, tx: String, mut data: Vec<u8>) -> Result<(MsgMeta, Vec<u8>), Error> {

        let (res, len) = {
            let mut buf = std::io::Cursor::new(&data);
            let len = buf.get_u32_be() as usize;

            match len > data.len() - 4 {
                true => {
                    let custom_error = std::io::Error::new(std::io::ErrorKind::Other, "oh no!");
                    return Err(Error::Io(custom_error));
                }
                false => (serde_json::from_slice::<MsgMeta>(&data[4..len + 4]), len)
            }
        };

        let mut msg_meta = res?;

        let correlation_id = msg_meta.correlation_id;

        msg_meta.tx = tx;

        let mut msg_meta = serde_json::to_vec(&msg_meta)?;
                                                      
        let mut payload_with_attachments: Vec<_> = data.drain(4 + len..).collect();
        let mut buf = vec![];

        buf.put_u32_be(msg_meta.len() as u32);

        buf.append(&mut msg_meta);
        buf.append(&mut payload_with_attachments);
        

        let (rpc_tx, rpc_rx) = crossbeam::channel::unbounded();
        
        self.rpc_tx.send(ClientMsg::AddRpc(correlation_id, rpc_tx));
        
        self.sender.send(Message::Binary(buf));

        let res = match rpc_rx.recv_timeout(std::time::Duration::from_secs(30)) {
            Ok((msg_meta, len, data)) => {
                //let payload = &data[len + 4..];        
                //Ok((msg_meta, payload.to_vec()))
                Ok((msg_meta, data))
            }
            Err(err) => Err(err)?
        };

        self.rpc_tx.send(ClientMsg::RemoveRpc(correlation_id));

        res
    }
}

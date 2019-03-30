use std::fmt::Debug;
use bytes::{Buf, BufMut};
use serde_derive::{Serialize, Deserialize};
use serde_json::Error;
use uuid::Uuid;
pub use bytes;
pub use uuid;

#[derive(Debug, Serialize, Deserialize)]
pub enum MsgSource {
    Component(String, String),
    Service(String)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MsgMeta {
    pub tx: String,
    pub rx: String,
    pub kind: MsgKind,
    pub correlation_id: Option<Uuid>,    
    pub source: Option<MsgSource>,
	pub attachments: Vec<Attachment>
}

#[derive(Debug, Serialize, Deserialize)]
pub enum MsgKind {
    Event,
    RpcRequest,
    RpcResponse
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Attachment {
	pub name: String
}

pub fn send_event_dto<T>(tx: String, rx: String, payload: T) -> Result<Vec<u8>, Error> where T: Debug, T: serde::Serialize, for<'de> T: serde::Deserialize<'de> {
    let msg_meta = MsgMeta {
        tx,
        rx,
        kind: MsgKind::Event,
        correlation_id: None,
        source: None,
		attachments: vec![]
    };

    let mut msg_meta = serde_json::to_vec(&msg_meta)?;
    let mut payload = serde_json::to_vec(&payload)?;

    let mut buf = vec![];

    buf.put_u32_be(msg_meta.len() as u32);

    buf.append(&mut msg_meta);
    buf.append(&mut payload);
    
    Ok(buf)
}

pub fn reply_to_rpc_dto<T>(tx: String, rx: String, correlation_id: Option<Uuid>, payload: T) -> Result<Vec<u8>, Error> where T: Debug, T: serde::Serialize, for<'de> T: serde::Deserialize<'de> {
    let msg_meta = MsgMeta {
        tx,
        rx,
        kind: MsgKind::RpcResponse,
        correlation_id,
        source: None,
		attachments: vec![]
    };        

    let mut msg_meta = serde_json::to_vec(&msg_meta)?;
    let mut payload = serde_json::to_vec(&payload)?;

    let mut buf = vec![];

    buf.put_u32_be(msg_meta.len() as u32);

    buf.append(&mut msg_meta);
    buf.append(&mut payload);
  
    Ok(buf)
}

/*
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
    */

pub fn rpc_dto<T>(tx: String, rx: String, payload: T) -> Result<Vec<u8>, Error> where T: Debug, T: serde::Serialize, for<'de> T: serde::Deserialize<'de> {
    let correlation_id = Uuid::new_v4();

    let msg_meta = MsgMeta {
        tx,
        rx,
        kind: MsgKind::RpcRequest,
        correlation_id: Some(correlation_id),
        source: None,
		attachments: vec![]
    };
    
    let mut msg_meta = serde_json::to_vec(&msg_meta)?;
    let mut payload = serde_json::to_vec(&payload)?;

    let mut buf = vec![];

    buf.put_u32_be(msg_meta.len() as u32);

    buf.append(&mut msg_meta);
    buf.append(&mut payload);

    Ok(buf)
}

/*
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
    */

pub fn send_event_dto2(tx: String, rx: String, mut payload: Vec<u8>) -> Result<Vec<u8>, Error> {        
    let msg_meta = MsgMeta {
        tx,
        rx,
        kind: MsgKind::Event,
        correlation_id: None,
        source: None,
		attachments: vec![]
    };

    let mut msg_meta = serde_json::to_vec(&msg_meta)?;        

    let mut buf = vec![];

    buf.put_u32_be(msg_meta.len() as u32);

    buf.append(&mut msg_meta);
    buf.append(&mut payload);
    
    Ok(buf)
}

pub fn reply_to_rpc_dto2(tx: String, rx: String, correlation_id: Option<Uuid>, mut payload: Vec<u8>) -> Result<Vec<u8>, Error> {
    let msg_meta = MsgMeta {
        tx,
        rx,
        kind: MsgKind::RpcResponse,
        correlation_id,
        source: None,
		attachments: vec![]
    };

    let mut msg_meta = serde_json::to_vec(&msg_meta)?;        

    let mut buf = vec![];

    buf.put_u32_be(msg_meta.len() as u32);

    buf.append(&mut msg_meta);
    buf.append(&mut payload);
    
    Ok(buf)
}

    /*
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
    */

pub fn rpc_dto2(tx: String, rx: String, mut payload: Vec<u8>) -> Result<Vec<u8>, Error> {
    let correlation_id = Uuid::new_v4();

    let msg_meta = MsgMeta {
        tx,
        rx,
        kind: MsgKind::RpcRequest,
        correlation_id: Some(correlation_id),
        source: None,
		attachments: vec![]
    };

    let mut msg_meta = serde_json::to_vec(&msg_meta)?;    

    let mut buf = vec![];

    buf.put_u32_be(msg_meta.len() as u32);

    buf.append(&mut msg_meta);
    buf.append(&mut payload);

    Ok(buf)
}

pub fn rpc_dto_with_attachments2(tx: String, rx: String, mut payload: Vec<u8>, attachments: Vec<(String, Vec<u8>)>) -> Result<Vec<u8>, Error> {
    let correlation_id = Uuid::new_v4();
    let mut attachments_meta = vec![];
    let mut attachments_payload = vec![];

    for (attachment_name, mut attachment_payload) in attachments {
        attachments_meta.push(Attachment {
            name: attachment_name
        });
        attachments_payload.put_u32_be(attachment_payload.len() as u32);
        attachments_payload.append(&mut attachment_payload);
    }

    let msg_meta = MsgMeta {
        tx,
        rx,
        kind: MsgKind::RpcRequest,
        correlation_id: Some(correlation_id),
        source: None,
		attachments: attachments_meta
    };

    let mut msg_meta = serde_json::to_vec(&msg_meta)?;    

    let mut buf = vec![];

    buf.put_u32_be(msg_meta.len() as u32);

    buf.append(&mut msg_meta);
    buf.append(&mut payload);
    buf.append(&mut attachments_payload);

    Ok(buf)
}

pub fn rpc_dto_with_correlation_id_2(tx: String, rx: String, mut payload: Vec<u8>) -> Result<(Uuid, Vec<u8>), Error> {
    let correlation_id = Uuid::new_v4();

    let msg_meta = MsgMeta {
        tx,
        rx,
        kind: MsgKind::RpcRequest,
        correlation_id: Some(correlation_id),
        source: None,
		attachments: vec![]
    };

    let mut msg_meta = serde_json::to_vec(&msg_meta)?;        

    let mut buf = vec![];

    buf.put_u32_be(msg_meta.len() as u32);

    buf.append(&mut msg_meta);
    buf.append(&mut payload);

    Ok((correlation_id, buf))
}

pub fn get_msg_meta(data: &Vec<u8>) -> Result<MsgMeta, Error> {
    let mut buf = std::io::Cursor::new(data);
    let len = buf.get_u32_be() as usize;

    serde_json::from_slice::<MsgMeta>(&data[4..len + 4])
}

pub fn get_msg<T>(data: &Vec<u8>) -> Result<(MsgMeta, T), Error> where T: Debug, T: serde::Serialize, for<'de> T: serde::Deserialize<'de> {
    let mut buf = std::io::Cursor::new(data);
    let len = buf.get_u32_be() as usize;

    let msg_meta = serde_json::from_slice::<MsgMeta>(&data[4..len + 4])?;
    let payload = serde_json::from_slice::<T>(&data[len + 4..])?;

    Ok((msg_meta, payload))
}

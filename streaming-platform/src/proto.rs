use std::collections::HashMap;
use std::pin::Pin;
use std::error::Error;
use std::fmt::{Debug, Display};
use std::option;
use std::io::Cursor;
use std::net::SocketAddr;
use bytes::Buf;
//use tokio::io::Take;
use tokio::tokio_io::io::take::Take;
use tokio::net::tcp::split::ReadHalf;
use tokio::sync::{mpsc::{Sender, error::SendError}, oneshot};
use tokio::prelude::*;
use serde_json::{from_slice, Value};
use serde_derive::{Deserialize};
use sp_dto::{*, uuid::Uuid};

pub const LEN_BUF_SIZE: usize = 4;
pub const DATA_BUF_SIZE: usize = 1024;
pub const MPSC_SERVER_BUF_SIZE: usize = 1000;
pub const MPSC_CLIENT_BUF_SIZE: usize = 100;
pub const MPSC_RPC_BUF_SIZE: usize = 10000;

#[derive(Debug, Clone)]
pub enum ClientKind {
    App,
    Service,
    Hub
}

/// Read full message from source in to memory. Should be used carefully with large message content.
pub async fn read_full(socket_read: &mut ReadHalf<'_>) -> Result<(MsgMeta, Vec<u8>, Vec<u8>), ProcessError> {
    let mut len_buf = [0; LEN_BUF_SIZE];
    socket_read.read_exact(&mut len_buf).await?;

    let mut buf = Cursor::new(len_buf);        
    let len = buf.get_u32_be() as usize;
    println!("len {}", len);
    let mut adapter = socket_read.take(len as u64);

    let mut msg_meta = vec![];

    let n = adapter.read_to_end(&mut msg_meta).await?;
    println!("n {}", n);

    let msg_meta: MsgMeta = from_slice(&msg_meta)?;        
    let mut adapter = socket_read.take(msg_meta.payload_size as u64);

    let mut payload = vec![];
    let n = adapter.read_to_end(&mut payload).await?;

    let mut adapter = socket_read.take(msg_meta.attachments_len() as u64);

    let mut attachments = vec![];
    let n = adapter.read_to_end(&mut attachments).await?;
    
    Ok((msg_meta, payload, attachments))
}

/// The result of reading function
pub enum ReadResult {
    /// This one indicates MsgMeta struct len was read successfully
    LenFinished([u8; DATA_BUF_SIZE]),
    /// Message data stream is prepended with MsgMeta struct
    MsgMeta(MsgMeta, Vec<u8>),
    /// Payload data stream message
    PayloadData(usize, [u8; DATA_BUF_SIZE]),
    /// This one indicates payload data stream finished
    PayloadFinished,
    /// Attachment whith index data stream message
    AttachmentData(usize, usize, [u8; DATA_BUF_SIZE]),
    /// This one indicates attachment data stream by index finished
    AttachmentFinished(usize),
    /// Message stream finished, simple as that
    MessageFinished
}

#[derive(Debug)]
pub enum Step {
    Len,
    MsgMeta(u32),
    Payload,
    Attachment(usize),
    Finish
}

/// Data structure used for convenience when streaming data from source
pub struct State {
    pub step: Step,
    pub attachments: Option<Vec<u64>>    
}

impl State {
    pub fn new() -> State {
        State {            
            step: Step::Len,
            attachments: None
        }  
    }    
}

pub async fn read(state: &mut State, adapter: &mut Take<ReadHalf<'_>>) -> Result<ReadResult, ProcessError> {
    match state.step {
        Step::Len => {
            let mut len_buf = [0; DATA_BUF_SIZE];
            adapter.read(&mut len_buf).await?;

            let mut buf = Cursor::new(&len_buf[..LEN_BUF_SIZE]);
            let len = buf.get_u32_be();

            state.step = Step::MsgMeta(len);

            Ok(ReadResult::LenFinished(len_buf))
        }
        Step::MsgMeta(len) => {
            adapter.set_limit(len as u64);
            let mut buf = vec![];
            let n = adapter.read_to_end(&mut buf).await?;

            let msg_meta: MsgMeta = from_slice(&buf)?;
            adapter.set_limit(msg_meta.payload_size as u64);

            state.attachments = Some(msg_meta.attachments.iter().map(|x| x.size).collect());

            state.step = Step::Payload;

            Ok(ReadResult::MsgMeta(msg_meta, buf))
        }
        Step::Payload => {
            let mut data_buf = [0; DATA_BUF_SIZE];                       

            match adapter.read(&mut data_buf).await? {
                0 => {
                    let attachments = state.attachments.as_ref().ok_or(ProcessError::AttachmentFieldIsEmpty)?;

                    match attachments.len() {
                        0 => {
                            adapter.set_limit(LEN_BUF_SIZE as u64);
                            state.step = Step::Finish;
                        }
                        _ => {                      
                            adapter.set_limit(attachments[0] as u64);
                            state.step = Step::Attachment(0);
                        }                            
                    };

                    Ok(ReadResult::PayloadFinished)
                }
                n => Ok(ReadResult::PayloadData(n, data_buf))
            }                                
        }
        Step::Attachment(index) => {
            let mut data_buf = [0; DATA_BUF_SIZE];                       

            match adapter.read(&mut data_buf).await? {
                0 => {
                    let attachments = state.attachments.as_ref().ok_or(ProcessError::AttachmentFieldIsEmpty)?;

                    match index < attachments.len() - 1 {
                        true => {
                            let new_index = index + 1;
                            adapter.set_limit(attachments[new_index] as u64);
                            state.step = Step::Attachment(new_index);
                        }
                        false => {
                            adapter.set_limit(LEN_BUF_SIZE as u64);
                            state.step = Step::Finish;
                        }
                    };

                    Ok(ReadResult::AttachmentFinished(index))
                }
                n => Ok(ReadResult::AttachmentData(index, n, data_buf))
            }
        }
        Step::Finish => {
            state.step = Step::Len;
            Ok(ReadResult::MessageFinished)
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub dirs: Option<Vec<Dir>>
}

#[derive(Debug, Deserialize, Clone)]
pub struct Dir {
    pub access_key: String,
    pub path: String
}

pub struct Client {
    pub net_addr: SocketAddr,
    pub tx: Sender<(usize, [u8; DATA_BUF_SIZE])>
}

pub enum ServerMsg {
    AddClient(String, SocketAddr, Sender<(usize, [u8; DATA_BUF_SIZE])>),
    SendBuf(String, usize, [u8; DATA_BUF_SIZE]),
    RemoveClient(String)
}

/// Type for function called on data stream message processing
pub type ProcessStreamMsg<T> = fn(ClientMsg) -> T;
/// Type for function called on event processing with raw payload
pub type ProcessEventRaw<T> = fn(HashMap<String, String>, MagicBall, MsgMeta, Vec<u8>, Vec<u8>) -> T;
/// Type for function called on rpc processing with raw payload
pub type ProcessRpcRaw<T> = fn(HashMap<String, String>, MagicBall, MsgMeta, Vec<u8>, Vec<u8>) -> T;
/// Type for function called on event processing with json payload
pub type ProcessEvent<T> = fn(HashMap<String, String>, MagicBall, MsgMeta, Value, Vec<u8>) -> T;
/// Type for function called on rpc processing with json payload
pub type ProcessRpc<T> = fn(HashMap<String, String>, MagicBall, MsgMeta, Value, Vec<u8>) -> T;
/// Type for function called on stream client starting
pub type StreamStartup<T> = fn(HashMap<String, String>) -> T;
/// Type for function called on client starting
pub type Startup<T> = fn(HashMap<String, String>, MagicBall) -> T;

/// Messages received from client
pub enum ClientMsg {
    /// This is sent with fs future
    FileReceiveComplete(String),
    /// This is sent in Stream mode without fs future
    MsgMeta(MsgMeta),
    /// This is sent in Stream mode without fs future
    PayloadData(usize, [u8; DATA_BUF_SIZE]),
    /// This is sent in Stream mode without fs future
    PayloadFinished,
    /// This is sent in Stream mode without fs future
    AttachmentData(usize, usize, [u8; DATA_BUF_SIZE]),
    /// This is sent in Stream mode without fs future
    AttachmentFinished(usize),
    /// This is sent in Stream mode without fs future
    MessageFinished,
    /// This is sent in FullMessage mode without fs future
    Message(MsgMeta, Vec<u8>, Vec<u8>)
}

pub async fn write(data: Vec<u8>, write_tx: &mut Sender<(usize, [u8; DATA_BUF_SIZE])>) -> Result<(), ProcessError> {
    let mut source = &data[..];

    loop {
        let mut data_buf = [0; DATA_BUF_SIZE];
        let n = source.read(&mut data_buf).await?;        

        match n {
            0 => break,
            _ => write_tx.send((n, data_buf)).await?
        }
    }

    Ok(())
}

pub async fn server_write(data: Vec<u8>, rx: &str, write_tx: &mut Sender<ServerMsg>) -> Result<(), ProcessError> {
    let mut source = &data[..];

    loop {
        let mut data_buf = [0; DATA_BUF_SIZE];
        let n = source.read(&mut data_buf).await?;        

        match n {
            0 => break,
            _ => write_tx.send(ServerMsg::SendBuf(rx.to_owned(), n, data_buf)).await?
        }
    }

    Ok(())
}

// Used for RPC implementation
pub enum RpcMsg {
    AddRpc(Uuid, oneshot::Sender<(MsgMeta, Vec<u8>, Vec<u8>)>),    
    RpcDataRequest(Uuid),
    RpcDataResponse(Uuid, oneshot::Sender<(MsgMeta, Vec<u8>, Vec<u8>)>)
}

#[derive(Clone)]
pub struct MagicBall {
    addr: String,
    write_tx: Sender<(usize, [u8; DATA_BUF_SIZE])>,
    rpc_inbound_tx: Sender<RpcMsg>
}

impl MagicBall {
    pub fn new(addr: String, write_tx: Sender<(usize, [u8; DATA_BUF_SIZE])>, rpc_inbound_tx: Sender<RpcMsg>) -> MagicBall {
        MagicBall {
            addr,
            write_tx,
            rpc_inbound_tx
        }
    }
    pub fn get_addr(&self) -> String {
		self.addr.clone()
	}
    /*
    pub async fn send_event(&mut self, dto: Vec<u8>) -> Result<(), ProcessError> {
        write(dto, &mut self.write_tx).await
    }
    pub async fn send_rpc(&mut self, dto: Vec<u8>) -> Result<(), ProcessError> {
        write(dto, &mut self.write_tx).await?;

        Ok(())
    }
    */
    pub async fn send_event<T>(&mut self, addr: &str, key: &str, payload: T) -> Result<(), ProcessError> where T: serde::Serialize, for<'de> T: serde::Deserialize<'de>, T: Debug {
        let route = Route {
            source: Participator::Service(self.addr.clone()),
            spec: RouteSpec::Simple,
            points: vec![Participator::Service(self.addr.to_owned())]
        };

        let dto = event_dto(self.addr.clone(), addr.to_owned(), key.to_owned(), payload, route)?;

        write(dto, &mut self.write_tx).await?;
        
        Ok(())
    }
    pub async fn send_event_with_route<T>(&mut self, addr: &str, key: &str, payload: T, mut route: Route) -> Result<(), ProcessError> where T: serde::Serialize, for<'de> T: serde::Deserialize<'de>, T: Debug {
        //info!("send_event, route {:?}, target addr {}, key {}, payload {:?}, ", route, addr, key, payload);

        route.points.push(Participator::Service(self.get_addr()));

        let dto = event_dto(self.get_addr(), addr.to_owned(), key.to_owned(), payload, route)?;

        write(dto, &mut self.write_tx).await?;
        
        Ok(())
    }    
    pub async fn rpc<T, R>(&mut self, addr: &str, key: &str, payload: T) -> Result<(MsgMeta, R, Vec<u8>), ProcessError> where T: serde::Serialize, T: Debug, for<'de> R: serde::Deserialize<'de>, R: Debug {
        let route = Route {
            source: Participator::Service(self.addr.clone()),
            spec: RouteSpec::Simple,
            points: vec![Participator::Service(self.addr.to_owned())]
        };

		//info!("send_rpc, route {:?}, target addr {}, key {}, payload {:?}, ", route, addr, key, payload);
		
        let (correlation_id, dto) = rpc_dto_with_correlation_id(self.addr.clone(), addr.to_owned(), key.to_owned(), payload, route)?;
        let (rpc_tx, rpc_rx) = oneshot::channel();
        
        self.rpc_inbound_tx.send(RpcMsg::AddRpc(correlation_id, rpc_tx));                
        write(dto, &mut self.write_tx).await?;

        let (msg_meta, payload, attachments) = rpc_rx.await?;
        let payload: R = from_slice(&payload)?;        

        Ok((msg_meta, payload, attachments))
    }
    pub async fn rpc_with_route<T, R>(&mut self, addr: &str, key: &str, payload: T, mut route: Route) -> Result<(MsgMeta, R, Vec<u8>), ProcessError> where T: serde::Serialize, T: Debug, for<'de> R: serde::Deserialize<'de>, R: Debug {
		//info!("send_rpc, route {:?}, target addr {}, key {}, payload {:?}, ", route, addr, key, payload);

        route.points.push(Participator::Service(self.addr.to_owned()));
		
        let (correlation_id, dto) = rpc_dto_with_correlation_id(self.addr.clone(), addr.to_owned(), key.to_owned(), payload, route)?;
        let (rpc_tx, rpc_rx) = oneshot::channel();
        
        self.rpc_inbound_tx.send(RpcMsg::AddRpc(correlation_id, rpc_tx));        
        write(dto, &mut self.write_tx).await?;

        let (msg_meta, payload, attachments) = rpc_rx.await?;
        let payload: R = from_slice(&payload)?;        

        Ok((msg_meta, payload, attachments))
    }
    
    /*
    pub fn reply_to_rpc(&self, addr: String, key: String, correlation_id: Uuid, payload: R, mut route: Route) -> Result<(), Error> {
        route.points.push(Participator::Service(self.addr.to_owned()));

        let dto = reply_to_rpc_dto(self.addr.clone(), addr, key, correlation_id, payload, route)?;

        self.sender.send(Message::Binary(dto));
        
        Ok(())
    }
    */
    /*
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
    */
}

#[derive(Debug)]
pub enum ProcessError {
    StreamClosed,
    NotEnoughBytesForLen,
    IncorrectReadResult,
    AttachmentFieldIsEmpty,
    Io(std::io::Error),
    SerdeJson(serde_json::Error),
    GetFile(GetFileError),
    SendError(SendError),
    OneshotRecvError(oneshot::error::RecvError),
    NoneError
}

#[derive(Debug)]
pub enum GetFileError {
    ConfigDirsIsEmpty,
    NoAccessKeyInPayload,
    TargetDirNotFound,
    NoFilesInTargetDir,
    FileNameIsEmpty,
    AttachmentsAreEmpty
}

impl Display for ProcessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SuperError is here!")
    }
}

impl Error for ProcessError {
    fn description(&self) -> &str {
        "I'm the superhero of errors"
    }    
}

impl From<std::io::Error> for ProcessError {
	fn from(err: std::io::Error) -> ProcessError {
		ProcessError::Io(err)
	}
}

impl From<serde_json::Error> for ProcessError {
	fn from(err: serde_json::Error) -> ProcessError {
		ProcessError::SerdeJson(err)
	}
}

impl From<option::NoneError> for ProcessError {
	fn from(err: option::NoneError) -> ProcessError {
		ProcessError::NoneError
	}
}

impl From<SendError> for ProcessError {
	fn from(err: SendError) -> ProcessError {
		ProcessError::SendError(err)
	}
}

impl From<oneshot::error::RecvError> for ProcessError {
	fn from(err: oneshot::error::RecvError) -> ProcessError {
		ProcessError::OneshotRecvError(err)
	}
}


/*
#![feature(unboxed_closures, fn_traits)]
use std::future::Future;

fn main() {
    start(f);
}

async fn f(data: &[String]) {
    println!("{:?}", data);
}

async fn start<F>(prm: F) 
where
    F: for<'a> AsyncFn<'a, (&'a [String],), ()>
{
    let data = vec![];
    
    prm.call_async((&data,)).await;
}

// =======================================

trait AsyncFn<'a, Args: 'a, R> {
    type Output: Future<Output = R> + 'a;

    fn call_async(&self, args: Args) -> Self::Output;
}

impl<'a, Args: 'a, R, F: Fn<Args>> AsyncFn<'a, Args, R> for F
where
    F::Output: Future<Output = R> + 'a,
{
    type Output = F::Output;

    fn call_async(&self, args: Args) -> Self::Output {
        self.call(args)
    }
}
*/
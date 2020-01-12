use std::collections::HashMap;
use std::pin::Pin;
use std::error::Error;
use std::fmt::{Debug, Display};
use std::option;
use std::sync::atomic::{AtomicU32, Ordering};
use std::io::Cursor;
use std::net::SocketAddr;
use log::*;
use bytes::{Buf, BufMut};
use tokio::io::Take;
use tokio::net::tcp::ReadHalf;
use tokio::sync::{mpsc::{Sender, Receiver, error::SendError}, oneshot};
use tokio::prelude::*;
use serde_json::{from_slice, Value, to_vec};
use serde_derive::{Deserialize};
use sp_dto::{*, uuid::Uuid};

pub const LEN_BUF_SIZE: usize = 4;
pub const LENS_BUF_SIZE: usize = 8;
pub const DATA_BUF_SIZE: usize = 1024;
pub const MPSC_SERVER_BUF_SIZE: usize = 1000;
pub const MPSC_CLIENT_BUF_SIZE: usize = 100;
pub const MPSC_RPC_BUF_SIZE: usize = 10000;

static COUNTER: AtomicU32 = AtomicU32::new(1);

pub fn get_stream_id() -> u32 { 
    if COUNTER.load(Ordering::Relaxed) == std::u32::MAX {
        COUNTER.store(1, Ordering::Relaxed);
    }    
    COUNTER.fetch_add(1, Ordering::Relaxed) 
}

#[derive(Debug, Clone)]
pub enum ClientKind {
    App,
    Service,
    Hub
}

/*
/// Read full message from source in to memory. Should be used carefully with large message content.
pub async fn read_full(socket_read: &mut ReadHalf<'_>) -> Result<(MsgMeta, Vec<u8>, Vec<u8>), ProcessError> {
    //info!("read_full start {}", addr);
    let mut len_buf = [0; LEN_BUF_SIZE];
    socket_read.read_exact(&mut len_buf).await?;

    let mut buf = Cursor::new(len_buf);        
    let len = buf.get_u32() as usize;
    //info!("read_full len {} {}", addr, len);
    let mut adapter = socket_read.take(len as u64);

    let mut msg_meta = vec![];

    let n = adapter.read_to_end(&mut msg_meta).await?;
    //info!("read_full n {} {}", addr, n);

    let msg_meta: MsgMeta = from_slice(&msg_meta)?;        
    //info!("read_full {} {:?}", addr, msg_meta);
    let mut adapter = socket_read.take(msg_meta.payload_size as u64);

    let mut payload = vec![];
    let n = adapter.read_to_end(&mut payload).await?;

    let mut adapter = socket_read.take(msg_meta.attachments_len() as u64);

    let mut attachments = vec![];
    let n = adapter.read_to_end(&mut attachments).await?;

    //info!("read_full ok {}", addr);
    
    Ok((msg_meta, payload, attachments))
}
*/

/// The result of reading function
pub enum ReadResult {    
    /// Message data stream is prepended with MsgMeta struct
    MsgMeta(u32, MsgMeta, Vec<u8>),
    /// Payload data stream message
    PayloadData(u32, usize, [u8; DATA_BUF_SIZE]),
    /// This one indicates payload data stream finished
    PayloadFinished(u32, usize, [u8; DATA_BUF_SIZE]),
    /// Attachment whith index data stream message
    AttachmentData(u32, usize, usize, [u8; DATA_BUF_SIZE]),
    /// This one indicates attachment data stream by index finished
    AttachmentFinished(u32, usize, usize, [u8; DATA_BUF_SIZE]),
    /// Message stream finished, simple as that
    MessageFinished(u32, MessageFinishBytes)
}

/// Part of result of reading function for message finish
pub enum MessageFinishBytes {
    /// This indicates message was finished with payload
    Payload(usize, [u8; DATA_BUF_SIZE]),
    /// This indicates message was finished with attachment
    Attachment(usize, usize, [u8; DATA_BUF_SIZE])
}

#[derive(Debug)]
pub enum Step {    
    MsgMeta,
    Payload(u64, u64),
    // current attachment index
    Attachment(usize, u64, u64)    
}

// Data structure used for convenience when streaming data from source

pub struct State {
    pub stream_states: HashMap<u32, StreamState>
}

impl State {
    pub fn new() -> State {
        State {
            stream_states: HashMap::new()
        }
    }
}

pub struct StreamState {
    pub step: Step,
    pub attachments: Vec<u64>
}

impl StreamState {
    pub fn new() -> StreamState {
        StreamState {            
            step: Step::MsgMeta,
            attachments: vec![]
        }  
    }    
}

pub struct StreamLayout {
    pub id: u32,
    pub msg_meta: MsgMeta,
    pub payload: Vec<u8>,
    pub attachments_data: Vec<u8>
}

pub async fn read(state: &mut State, adapter: &mut Take<ReadHalf<'_>>) -> Result<ReadResult, ProcessError> {    
    let mut u32_buf = [0; LEN_BUF_SIZE];

    adapter.read(&mut u32_buf).await?;
    let mut buf = Cursor::new(&u32_buf[..]);
    let stream_id = buf.get_u32();

    info!("read stream_id {}", stream_id);

    adapter.read(&mut u32_buf).await?;
    let mut buf = Cursor::new(&u32_buf[..]);
    let unit_size = buf.get_u32();

    info!("read unit_size {}", unit_size);

    if !state.stream_states.contains_key(&stream_id) {
        state.stream_states.insert(stream_id, StreamState::new());
    }

    let stream_state = state.stream_states.get_mut(&stream_id).ok_or(ProcessError::StreamNotFoundInState)?;

    adapter.set_limit(unit_size as u64);

    let res = match stream_state.step {
        Step::MsgMeta => {
            let mut buf = vec![];
            let n = adapter.read_to_end(&mut buf).await?;
            info!("read step msg meta, n {}", unit_size);
            let msg_meta: MsgMeta = from_slice(&buf)?;
            for attachment in msg_meta.attachments.iter() {
                stream_state.attachments.push(attachment.size);
            }             
            stream_state.step = Step::Payload(msg_meta.payload_size, 0);
            Ok(ReadResult::MsgMeta(stream_id, msg_meta, buf))            
        }
        Step::Payload(payload_size, bytes_read) => {
            info!("step payload, payload_size {}, bytes_read {}", payload_size, bytes_read);
            let mut data_buf = [0; DATA_BUF_SIZE];
            let n = adapter.read(&mut data_buf).await?;
            let bytes_read = bytes_read + n as u64;
            info!("step payload, n {}, payload_size {}, bytes_read {}", n, payload_size, bytes_read);
            if bytes_read < payload_size {
                stream_state.step = Step::Payload(payload_size, bytes_read);
                Ok(ReadResult::PayloadData(stream_id, n, data_buf))
            } else if bytes_read == payload_size {                
                match stream_state.attachments.len() {
                    0 => {
                        let _ = state.stream_states.remove(&stream_id);
                        Ok(ReadResult::MessageFinished(stream_id, MessageFinishBytes::Payload(n, data_buf)))
                    }
                    _ => {
                        stream_state.step = Step::Attachment(0, stream_state.attachments[0], 0);
                        Ok(ReadResult::PayloadFinished(stream_id, n, data_buf))
                    }
                }               
            } else if bytes_read > payload_size {
                Err(ProcessError::BytesReadAmountExceededPayloadSize)
            } else {
                Err(ProcessError::PayloadSizeChecksFailed)
            }
        }
        Step::Attachment(index, attachment_size, bytes_read) => {
            let mut data_buf = [0; DATA_BUF_SIZE];
            let n = adapter.read(&mut data_buf).await?;
            let bytes_read = bytes_read + n as u64;
            if bytes_read < attachment_size {
                stream_state.step = Step::Attachment(index, attachment_size, bytes_read);
                Ok(ReadResult::AttachmentData(stream_id, index, n, data_buf))
            } else if bytes_read == attachment_size {                
                match stream_state.attachments.len() {
                    index => {                        
                        let _ = state.stream_states.remove(&stream_id);
                        Ok(ReadResult::MessageFinished(stream_id, MessageFinishBytes::Attachment(index, n, data_buf)))
                    }
                    _ => {
                        stream_state.step = Step::Attachment(index + 1, stream_state.attachments[index + 1], 0);
                        Ok(ReadResult::AttachmentFinished(stream_id, index, n, data_buf))
                    }
                }              
            } else if bytes_read > attachment_size {
                Err(ProcessError::BytesReadAmountExceededAttachmentSize)
            } else {
                Err(ProcessError::AttachmentSizeChecksFailed)
            }
        }        
    };

    adapter.set_limit(LENS_BUF_SIZE as u64);

    res    
}

/*
pub async fn read(state: &mut State, adapter: &mut Take<ReadHalf<'_>>) -> Result<ReadResult, ProcessError> {
    //info!("reading");
    match state.step {        
        Step::Len => {
            //info!("step len");
            let mut len_buf = [0; DATA_BUF_SIZE];
            adapter.read(&mut len_buf).await?;

            let mut buf = Cursor::new(&len_buf[..LEN_BUF_SIZE]);
            let len = buf.get_u32();

            state.step = Step::MsgMeta(len);

            //info!("step len ok");

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
*/

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
    pub tx: Sender<StreamUnit>
}

pub enum ServerMsg {
    AddClient(String, SocketAddr, Sender<StreamUnit>),
    SendUnit(String, StreamUnit),
    RemoveClient(String)
}

pub enum StreamUnit {
    Array(u32, usize, [u8; DATA_BUF_SIZE]),
    Vector(u32, Vec<u8>)
}

/// Type for function called on data stream processing
pub type ProcessStream<T> = fn(HashMap<String, String>, MagicBall, Receiver<ClientMsg>, Option<Receiver<RestreamMsg>>) -> T;
/// Type for function called on event processing with raw payload
pub type ProcessEventRaw<T> = fn(HashMap<String, String>, MagicBall, MessageRaw) -> T;
/// Type for function called on rpc processing with raw payload
pub type ProcessRpcRaw<T> = fn(HashMap<String, String>, MagicBall, MessageRaw) -> T;
/// Type for function called on event processing with json payload
pub type ProcessEvent<T, R> = fn(HashMap<String, String>, MagicBall, Message<R>) -> T;
/// Type for function called on rpc processing with json payload
pub type ProcessRpc<T, R> = fn(HashMap<String, String>, MagicBall, Message<R>) -> T;
/// Type for function called on stream client starting
pub type StreamStartup<T> = fn(HashMap<String, String>) -> T;
/// Type for function called on client starting
pub type Startup<T> = fn(HashMap<String, String>, MagicBall) -> T;

/// Messages received from client
pub enum ClientMsg {    
    /// This is sent in Stream mode without fs future
    MsgMeta(u32, MsgMeta),
    /// This is sent in Stream mode without fs future
    PayloadData(u32, usize, [u8; DATA_BUF_SIZE]),
    /// This is sent in Stream mode without fs future
    PayloadFinished(u32, usize, [u8; DATA_BUF_SIZE]),
    /// This is sent in Stream mode without fs future. First field is index, second is number of bytes read, last is data itself.
    AttachmentData(u32, usize, usize, [u8; DATA_BUF_SIZE]),
    /// This is sent in Stream mode without fs future
    AttachmentFinished(u32, usize, usize, [u8; DATA_BUF_SIZE]),
    /// This is sent in Stream mode without fs future
    MessageFinished(u32),
    /// This is sent in FullMessage mode without fs future
    Message(MsgMeta, Vec<u8>, Vec<u8>)
}

pub enum RestreamMsg {
    StartSimple,
    #[cfg(feature = "http")]
    StartHttp(hyper::body::Sender, oneshot::Sender<StreamCompletion>)
}

pub enum StreamCompletion {
    Ok,
    Err
}

pub async fn write(stream_id: u32, data: Vec<u8>, msg_meta_size: u64, payload_size: u64, attachments_sizes: Vec<u64>, write_tx: &mut Sender<StreamUnit>) -> Result<(), ProcessError> {    
    let msg_meta_offset = LEN_BUF_SIZE + msg_meta_size as usize;
    let payload_offset = msg_meta_offset + payload_size as usize;
    info!("msg_meta_offset {}", msg_meta_offset);
    info!("payload_offset {}", payload_offset);
    let mut data_buf = [0; DATA_BUF_SIZE];    

    write_tx.send(StreamUnit::Vector(stream_id, data[LEN_BUF_SIZE..msg_meta_offset].to_vec())).await?;

    let mut source = &data[msg_meta_offset..payload_offset];

    loop {        
        let n = source.read(&mut data_buf).await?;        
        match n {
            0 => break,
            _ => write_tx.send(StreamUnit::Array(stream_id, n, data_buf)).await?
        }
    }

    let mut prev = payload_offset as usize;

    for attachment_size in attachments_sizes {
        let attachment_offset = prev + attachment_size as usize;
        let mut source = &data[prev..attachment_offset];

        loop {        
            let n = source.read(&mut data_buf).await?;        
            match n {
                0 => break,
                _ => write_tx.send(StreamUnit::Array(stream_id, n, data_buf)).await?
            }
        }

        prev = attachment_offset;
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
    pub write_tx: Sender<StreamUnit>,
    rpc_inbound_tx: Sender<RpcMsg>
}


impl MagicBall {
    pub fn new(addr: String, write_tx: Sender<StreamUnit>, rpc_inbound_tx: Sender<RpcMsg>) -> MagicBall {
        MagicBall {            
            addr,
            write_tx,
            rpc_inbound_tx
        }
    }    
    pub fn get_addr(&self) -> String {
		self.addr.clone()
    }
    /// Please note, stream_id value MUST ONLY BE ACQUIRED with get_stream_id() function. 
    /// This value MUST BE UNIQUE FOR EACH MESSAGE IN TRANSFER AT CURRENT MOMENT OF TIME (that is what get_stream_id() function does).
    /// stream_id generation made explicit on purpose to point out RESPONSIBILITY OF THE USER OF THIS FUNCTION.
    /// Ofcourse, stream_id generation can be implicit - this, however, will lead to less flexible API (if for example you need to call this function multiple times to write parts of one message).
    pub async fn write_vec(&mut self, stream_id: u32, data: Vec<u8>, msg_meta_size: u64, payload_size: u64, attachments_sizes: Vec<u64>) -> Result<(), ProcessError> {
        let msg_meta_offset = LEN_BUF_SIZE + msg_meta_size as usize;
        let payload_offset = msg_meta_offset + payload_size as usize;
        let mut data_buf = [0; DATA_BUF_SIZE];

        let mut source = &data[LEN_BUF_SIZE..msg_meta_offset];    

        loop {        
            let n = source.read(&mut data_buf).await?;        
            match n {
                0 => break,
                _ => self.write_tx.send(StreamUnit::Array(stream_id, n, data_buf)).await?
            }
        }

        let mut source = &data[msg_meta_offset..payload_offset];

        loop {        
            let n = source.read(&mut data_buf).await?;        
            match n {
                0 => break,
                _ => self.write_tx.send(StreamUnit::Array(stream_id, n, data_buf)).await?
            }
        }

        let mut prev = payload_offset as usize;

        for attachment_size in attachments_sizes {
            let attachment_offset = prev + attachment_size as usize;
            let mut source = &data[prev..attachment_offset];

            loop {        
                let n = source.read(&mut data_buf).await?;        
                match n {
                    0 => break,
                    _ => self.write_tx.send(StreamUnit::Array(stream_id, n, data_buf)).await?
                }
            }

            prev = attachment_offset;
        }

        Ok(())
    }
    pub async fn send_event<T>(&mut self, addr: &str, key: &str, payload: T) -> Result<(), ProcessError> where T: serde::Serialize, for<'de> T: serde::Deserialize<'de>, T: Debug {
        let route = Route {
            source: Participator::Service(self.addr.clone()),
            spec: RouteSpec::Simple,
            points: vec![Participator::Service(self.addr.to_owned())]
        };

        let (dto, msg_meta_size, payload_size, attachments_sizes) = event_dto_with_sizes(self.addr.clone(), addr.to_owned(), key.to_owned(), payload, route)?;

        write(get_stream_id(), dto, msg_meta_size, payload_size, attachments_sizes, &mut self.write_tx).await?;
        
        Ok(())
    }
    pub async fn send_event_with_route<T>(&mut self, addr: &str, key: &str, payload: T, mut route: Route) -> Result<(), ProcessError> where T: serde::Serialize, for<'de> T: serde::Deserialize<'de>, T: Debug {
        //info!("send_event, route {:?}, target addr {}, key {}, payload {:?}, ", route, addr, key, payload);

        route.points.push(Participator::Service(self.get_addr()));

        let (dto, msg_meta_size, payload_size, attachments_sizes) = event_dto_with_sizes(self.get_addr(), addr.to_owned(), key.to_owned(), payload, route)?;

        write(get_stream_id(), dto, msg_meta_size, payload_size, attachments_sizes, &mut self.write_tx).await?;
        
        Ok(())
    }    
    pub async fn rpc<T, R>(&mut self, addr: &str, key: &str, payload: T) -> Result<Message<R>, ProcessError> where T: serde::Serialize, T: Debug, for<'de> R: serde::Deserialize<'de>, R: Debug {
        let route = Route {
            source: Participator::Service(self.addr.clone()),
            spec: RouteSpec::Simple,
            points: vec![Participator::Service(self.addr.to_owned())]
        };

		//info!("send_rpc, route {:?}, target addr {}, key {}, payload {:?}, ", route, addr, key, payload);
		
        let (correlation_id, dto, msg_meta_size, payload_size, attachments_sizes) = rpc_dto_with_correlation_id_sizes(self.addr.clone(), addr.to_owned(), key.to_owned(), payload, route)?;
        let (rpc_tx, rpc_rx) = oneshot::channel();
        
        self.rpc_inbound_tx.send(RpcMsg::AddRpc(correlation_id, rpc_tx)).await?;        
        write(get_stream_id(), dto, msg_meta_size, payload_size, attachments_sizes, &mut self.write_tx).await?;        

        let (msg_meta, payload, attachments_data) = rpc_rx.await?;
        let payload: R = from_slice(&payload)?;        

        Ok(Message {
            meta: msg_meta, 
            payload, 
            attachments_data
        })
    }
    pub async fn rpc_with_route<T, R>(&mut self, addr: &str, key: &str, payload: T, mut route: Route) -> Result<Message<R>, ProcessError> where T: serde::Serialize, T: Debug, for<'de> R: serde::Deserialize<'de>, R: Debug {
		//info!("send_rpc, route {:?}, target addr {}, key {}, payload {:?}, ", route, addr, key, payload);

        route.points.push(Participator::Service(self.addr.to_owned()));
		
        let (correlation_id, dto, msg_meta_size, payload_size, attachments_sizes) = rpc_dto_with_correlation_id_sizes(self.addr.clone(), addr.to_owned(), key.to_owned(), payload, route)?;
        let (rpc_tx, rpc_rx) = oneshot::channel();
        
        self.rpc_inbound_tx.send(RpcMsg::AddRpc(correlation_id, rpc_tx)).await?;
        write(get_stream_id(), dto, msg_meta_size, payload_size, attachments_sizes, &mut self.write_tx).await?;

        let (msg_meta, payload, attachments_data) = rpc_rx.await?;
        let payload: R = from_slice(&payload)?;        

        Ok(Message {
            meta: msg_meta, 
            payload, 
            attachments_data
        })
    }
    pub async fn proxy_event(&mut self, tx: String, mut data: Vec<u8>) -> Result<(), ProcessError> {
        let (res, len) = {
            let mut buf = Cursor::new(&data);
            let len = buf.get_u32() as usize;

            match len > data.len() - 4 {
                true => {
                    let custom_error = std::io::Error::new(std::io::ErrorKind::Other, "len incosistent with data on proxy event");
                    return Err(ProcessError::Io(custom_error));
                }
                false => (serde_json::from_slice::<MsgMeta>(&data[4..len + 4]), len)
            }
        };

        let mut msg_meta = res?;

        msg_meta.tx = tx;        
        msg_meta.route.points.push(Participator::Service(self.addr.to_owned()));

        let payload_size = msg_meta.payload_size;
        let attachments_sizes = msg_meta.attachments_sizes();

        let mut msg_meta = serde_json::to_vec(&msg_meta)?;
        let msg_meta_size = msg_meta.len() as u64;
                                                      
        let mut payload_with_attachments: Vec<_> = data.drain(4 + len..).collect();
        let mut buf = vec![];

        buf.put_u32(msg_meta.len() as u32);

        buf.append(&mut msg_meta);
        buf.append(&mut payload_with_attachments);

        write(get_stream_id(), buf, msg_meta_size, payload_size, attachments_sizes,  &mut self.write_tx).await?;
        
        Ok(())
    }    
    pub async fn proxy_rpc(&mut self, tx: String, mut data: Vec<u8>) -> Result<(MsgMeta, Vec<u8>), ProcessError> {
        let (res, len) = {
            let mut buf = std::io::Cursor::new(&data);
            let len = buf.get_u32() as usize;

            match len > data.len() - 4 {
                true => {
                    let custom_error = std::io::Error::new(std::io::ErrorKind::Other, "len incosistent with data on proxy rpc request");
                    return Err(ProcessError::Io(custom_error));
                }
                false => (serde_json::from_slice::<MsgMeta>(&data[4..len + 4]), len)
            }
        };

        let mut msg_meta = res?;

        let correlation_id = msg_meta.correlation_id;

        msg_meta.tx = tx;
        msg_meta.route.points.push(Participator::Service(self.addr.to_owned()));

        let payload_size = msg_meta.payload_size;
        let attachments_sizes = msg_meta.attachments_sizes();

        let mut msg_meta = to_vec(&msg_meta)?;
        let msg_meta_size = msg_meta.len() as u64;
                                                      
        let mut payload_with_attachments: Vec<_> = data.drain(4 + len..).collect();
        let mut buf = vec![];

        buf.put_u32(msg_meta.len() as u32);

        buf.append(&mut msg_meta);
        buf.append(&mut payload_with_attachments);

        let (rpc_tx, rpc_rx) = oneshot::channel();
                
        self.rpc_inbound_tx.send(RpcMsg::AddRpc(correlation_id, rpc_tx)).await?;
        write(get_stream_id(), buf, msg_meta_size, payload_size, attachments_sizes, &mut self.write_tx).await?;

        let (msg_meta, mut payload, mut attachments) = rpc_rx.await?;

        let mut buf = vec![];
        let mut msg_meta_buf = to_vec(&msg_meta)?;

        buf.put_u32(msg_meta_buf.len() as u32);
        buf.append(&mut msg_meta_buf);
        buf.append(&mut payload);
        buf.append(&mut attachments);
        
        Ok((msg_meta, buf))
    }    
}

#[derive(Debug)]
pub enum ProcessError {
    StreamNotFoundInState,
    StreamLayoutNotFound,
    AuthStreamLayoutIsEmpty,
    ClientAddrNotFound,
    BytesReadAmountExceededPayloadSize,
    PayloadSizeChecksFailed,
    BytesReadAmountExceededAttachmentSize,
    AttachmentSizeChecksFailed,    
    StreamClosed,
    NotEnoughBytesForLen,
    IncorrectReadResult,    
    Io(std::io::Error),
    SerdeJson(serde_json::Error),
    GetFile(GetFileError),
    SendBufError,
    SendServerMsgError,
    SendClientMsgError,
    SendRpcMsgError,
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

impl From<SendError<StreamUnit>> for ProcessError {
	fn from(err: SendError<StreamUnit>) -> ProcessError {
		ProcessError::SendBufError
	}
}

impl From<SendError<ServerMsg>> for ProcessError {
	fn from(err: SendError<ServerMsg>) -> ProcessError {
		ProcessError::SendServerMsgError
	}
}

impl From<SendError<ClientMsg>> for ProcessError {
	fn from(err: SendError<ClientMsg>) -> ProcessError {
		ProcessError::SendClientMsgError
	}
}

impl From<SendError<RpcMsg>> for ProcessError {
	fn from(err: SendError<RpcMsg>) -> ProcessError {
		ProcessError::SendRpcMsgError
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
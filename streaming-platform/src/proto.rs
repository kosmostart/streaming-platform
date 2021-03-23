use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Debug, Display};
use std::option;
use std::io::Cursor;
use std::net::SocketAddr;
use std::hash::Hasher;
use std::time::Duration;
use log::*;
use rand::random;
use tokio::net::TcpStream;
use tokio::sync::{mpsc::{UnboundedSender, UnboundedReceiver, error::{SendError, TrySendError}}, oneshot};
//use tokio::time::{timeout, error::Elapsed};
use tokio::time::timeout;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use serde_json::{from_slice, Value, to_vec};
use siphasher::sip::SipHasher24;
use sp_dto::bytes::{Buf, BytesMut, BufMut};
use sp_dto::{*, uuid::Uuid};

pub const STREAM_ID_BUF_SIZE: usize = 8;
pub const LEN_BUF_SIZE: usize = 4;
pub const LENS_BUF_SIZE: usize = 12;
pub const DATA_BUF_SIZE: usize = 1024;
//pub const MPSC_SERVER_BUF_SIZE: usize = 1000000;
//pub const MPSC_CLIENT_BUF_SIZE: usize = 1000000;
//pub const MPSC_RPC_BUF_SIZE: usize = 1000000;
pub const RPC_TIMEOUT_MS_AMOUNT: u64 = 30000;
//pub const STREAM_UNIT_READ_TIMEOUT_MS_AMOUNT: u64 = 1000;

/*
static COUNTER: AtomicU32 = AtomicU32::new(1);

pub fn get_counter_value() -> u32 {
    if COUNTER.load(Ordering::Relaxed) == std::u32::MAX {
        COUNTER.store(1, Ordering::Relaxed);
    }
    COUNTER.fetch_add(1, Ordering::Relaxed)
}
*/

pub fn get_hasher() -> SipHasher24 {    
    SipHasher24::new_with_keys(0, random::<u64>())
}

pub fn get_stream_id_onetime(addr: &str) -> u64 {
    let mut buf = BytesMut::new();
    buf.put(addr.as_bytes());
    //buf.put_u32(get_counter_value());
    buf.extend_from_slice(Uuid::new_v4().to_string().as_bytes());
    let mut hasher = get_hasher();
    hasher.write(&buf);
    hasher.finish()
}

/// The result of reading function
pub enum ReadResult {    
    /// Message data stream is prepended with MsgMeta struct
    MsgMeta(u64, MsgMeta, Vec<u8>),
    /// Payload data stream message
    PayloadData(u64, usize, [u8; DATA_BUF_SIZE]),
    /// This one indicates payload data stream finished
    PayloadFinished(u64, usize, [u8; DATA_BUF_SIZE]),
    /// Attachment whith index data stream message
    AttachmentData(u64, usize, usize, [u8; DATA_BUF_SIZE]),
    /// This one indicates attachment data stream by index finished
    AttachmentFinished(u64, usize, usize, [u8; DATA_BUF_SIZE]),
    /// Message stream finished, simple as that
    MessageFinished(u64, MessageFinishBytes),
    /// Message was aborted, through cancelation or error
    MessageAborted(Option<u64>)
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
    pub addr: String,
    pub stream_states: HashMap<u64, StreamState>
}

impl State {
    pub fn new(addr: String) -> State {
        State {
            addr,
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
    pub id: u64,
    pub msg_meta: MsgMeta,
    pub payload: Vec<u8>,
    pub attachments_data: Vec<u8>
}

pub async fn read(state: &mut State, socket_read: &mut TcpStream) -> Result<ReadResult, ProcessError> {    
    let mut u64_buf = [0; STREAM_ID_BUF_SIZE];
    let mut u32_buf = [0; LEN_BUF_SIZE];

    debug!("{} read stream_id attempt", state.addr);

    //let mut adapter = socket_read.take(LENS_BUF_SIZE as u64);
    
    //adapter.read(&mut u64_buf).await?;
    socket_read.read_exact(&mut u64_buf).await?;

    let mut buf = Cursor::new(&u64_buf[..]);
    let stream_id = buf.get_u64();
    
    if stream_id == 0 {                
        return Err(ProcessError::StreamIdIsZero);
    }

    debug!("{} read stream_id succeded, stream_id {}", state.addr, stream_id);
    debug!("{} read unit_size attempt, stream_id {}", state.addr, stream_id);

    //adapter.read(&mut u32_buf).await?;
    socket_read.read_exact(&mut u32_buf).await?;

    /*
    match timeout(Duration::from_millis(STREAM_UNIT_READ_TIMEOUT_MS_AMOUNT), adapter.read(&mut u32_buf)).await? {
        Ok(_) => {}
        Err(e) => {
            error!("read error {:#?}, read unit size attempt, stream_id {}", e, stream_id);            
            return Ok(ReadResult::MessageAborted(Some(stream_id)));
        }
    }
    */

    let mut buf = Cursor::new(&u32_buf[..]);
    let unit_size = buf.get_u32();

    debug!("{} read unit_size succeded, unit_size {}, stream_id {}", state.addr, unit_size, stream_id);

    if !state.stream_states.contains_key(&stream_id) {
        state.stream_states.insert(stream_id, StreamState::new());
    }

    let stream_state = match state.stream_states.get_mut(&stream_id) {
        Some(stream_state) => stream_state,
        None => {
            error!("read error {:#?}, stream_id {}", ProcessError::StreamNotFoundInState, stream_id);            
            return Ok(ReadResult::MessageAborted(Some(stream_id)));            
        }
    };        

    let res = match stream_state.step {
        Step::MsgMeta => {
            let mut buf = vec![];
            let mut adapter = socket_read.take(unit_size as u64);
            let _n = adapter.read_to_end(&mut buf).await?;
            let msg_meta: MsgMeta = from_slice(&buf)?;            
            for attachment in msg_meta.attachments.iter() {
                stream_state.attachments.push(attachment.size);
            }             
            stream_state.step = Step::Payload(msg_meta.payload_size, 0);
            Ok(ReadResult::MsgMeta(stream_id, msg_meta, buf))
            /*
            match timeout(Duration::from_millis(STREAM_UNIT_READ_TIMEOUT_MS_AMOUNT), adapter.read_to_end(&mut buf)).await? {
                Ok(n) => {
                    let msg_meta: MsgMeta = from_slice(&buf)?;            
                    for attachment in msg_meta.attachments.iter() {
                        stream_state.attachments.push(attachment.size);
                    }             
                    stream_state.step = Step::Payload(msg_meta.payload_size, 0);
                    Ok(ReadResult::MsgMeta(stream_id, msg_meta, buf))
                }
                Err(e) => {
                    error!("read error {:#?}, stream_id {}", e, stream_id);
                    Ok(ReadResult::MessageAborted(Some(stream_id)))
                }
            }
            */           
        }
        Step::Payload(payload_size, bytes_read) => {            
            let mut data_buf = [0; DATA_BUF_SIZE];
            //let n = adapter.read_to_end(&mut data_buf).await?;
            let n = socket_read.read_exact(&mut data_buf[..unit_size as usize]).await?;
            let bytes_read = bytes_read + n as u64;            
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
                error!("read error {:#?}, stream_id {}", ProcessError::BytesReadAmountExceededPayloadSize, stream_id);
                Ok(ReadResult::MessageAborted(Some(stream_id)))
            } else {                        
                error!("read error {:#?}, stream_id {}", ProcessError::PayloadSizeChecksFailed, stream_id);
                Ok(ReadResult::MessageAborted(Some(stream_id)))
            }
            /*
            match timeout(Duration::from_millis(STREAM_UNIT_READ_TIMEOUT_MS_AMOUNT), adapter.read(&mut data_buf)).await? {
                Ok(n) => {
                    let bytes_read = bytes_read + n as u64;            
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
                        error!("read error {:#?}, stream_id {}", ProcessError::BytesReadAmountExceededPayloadSize, stream_id);
                        Ok(ReadResult::MessageAborted(Some(stream_id)))
                    } else {                        
                        error!("read error {:#?}, stream_id {}", ProcessError::PayloadSizeChecksFailed, stream_id);
                        Ok(ReadResult::MessageAborted(Some(stream_id)))
                    }
                }
                Err(e) => {
                    error!("read error {:#?}, stream_id {}", e, stream_id);                    
                    Ok(ReadResult::MessageAborted(Some(stream_id)))
                }
            }
            */          
        }
        Step::Attachment(index, attachment_size, bytes_read) => {
            let mut data_buf = [0; DATA_BUF_SIZE];
            //let n = adapter.read(&mut data_buf).await?;
            let n = socket_read.read_exact(&mut data_buf[..unit_size as usize]).await?;
            let bytes_read = bytes_read + n as u64;
            if bytes_read < attachment_size {
                stream_state.step = Step::Attachment(index, attachment_size, bytes_read);
                Ok(ReadResult::AttachmentData(stream_id, index, n, data_buf))
            } else if bytes_read == attachment_size {                
                if stream_state.attachments.len() == index {
                    let _ = state.stream_states.remove(&stream_id);
                    Ok(ReadResult::MessageFinished(stream_id, MessageFinishBytes::Attachment(index, n, data_buf)))
                } else {
                    stream_state.step = Step::Attachment(index + 1, stream_state.attachments[index + 1], 0);
                    Ok(ReadResult::AttachmentFinished(stream_id, index, n, data_buf))                
                }              
            } else if bytes_read > attachment_size {
                error!("read error {:#?}, stream_id {}", ProcessError::BytesReadAmountExceededAttachmentSize, stream_id);
                Ok(ReadResult::MessageAborted(Some(stream_id)))
            } else {
                error!("read error {:#?}, stream_id {}", ProcessError::AttachmentSizeChecksFailed, stream_id);
                Ok(ReadResult::MessageAborted(Some(stream_id)))
            }
            /*
            match timeout(Duration::from_millis(STREAM_UNIT_READ_TIMEOUT_MS_AMOUNT), adapter.read(&mut data_buf)).await? {
                Ok(n) => {
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
                        error!("read error {:#?}, stream_id {}", ProcessError::BytesReadAmountExceededAttachmentSize, stream_id);
                        Ok(ReadResult::MessageAborted(Some(stream_id)))
                    } else {
                        error!("read error {:#?}, stream_id {}", ProcessError::AttachmentSizeChecksFailed, stream_id);
                        Ok(ReadResult::MessageAborted(Some(stream_id)))
                    }
                }
                Err(e) => {
                    error!("read error {:#?}, stream_id {}", e, stream_id);                    
                    Ok(ReadResult::MessageAborted(Some(stream_id)))
                }
            }
            */           
        }    
    };    

    debug!("{} read finished, unit_size {}, stream_id {}", state.addr, unit_size, stream_id);

    res    
}

pub struct Client {
    pub net_addr: SocketAddr,
    pub tx: UnboundedSender<StreamUnit>
}

pub enum ServerMsg {
    AddClient(String, SocketAddr, UnboundedSender<StreamUnit>),
    SendUnit(String, StreamUnit),
    RemoveClient(String)
}

pub enum StreamUnit {
    Array(u64, usize, [u8; DATA_BUF_SIZE]),
    Vector(u64, Vec<u8>),
    Empty(u64)
}

/*
pub trait DI<T> {
    fn get() -> T;
}
*/

/// Type for function called on data stream processing
pub type ProcessStream<T, D> = fn(HashMap<String, String>, MagicBall, UnboundedReceiver<ClientMsg>, Option<UnboundedReceiver<RestreamMsg>>, D) -> T;
/// Type for function called on event processing with json payload
pub type ProcessEvent<T, R, D> = fn(HashMap<String, String>, MagicBall, Message<R>, D) -> T;
/// Type for function called on rpc processing with json payload
pub type ProcessRpc<T, R, D> = fn(HashMap<String, String>, MagicBall, Message<R>, D) -> T;
/// Type for function called on stream client starting
pub type StreamStartup<T, D> = fn(HashMap<String, String>, Option<Value>, D) -> T;
/// Type for function called on client starting
pub type Startup<T, D> = fn(HashMap<String, String>, MagicBall, Option<Value>, D) -> T;

/// Messages received from client
pub enum ClientMsg {    
    /// This is sent in Stream mode without fs future
    MsgMeta(u64, MsgMeta),
    /// This is sent in Stream mode without fs future
    PayloadData(u64, usize, [u8; DATA_BUF_SIZE]),
    /// This is sent in Stream mode without fs future
    PayloadFinished(u64, usize, [u8; DATA_BUF_SIZE]),
    /// This is sent in Stream mode without fs future. First field is index, second is number of bytes read, last is data itself.
    AttachmentData(u64, usize, usize, [u8; DATA_BUF_SIZE]),
    /// This is sent in Stream mode without fs future
    AttachmentFinished(u64, usize, usize, [u8; DATA_BUF_SIZE]),
    /// This is sent in Stream mode without fs future
    MessageFinished(u64),
    /// This is sent in FullMessage mode without fs future
    Message(u64, MsgMeta, Vec<u8>, Vec<u8>),
    /// Message was aborted, through cancelation or error
    MessageAborted(Option<u64>)
}

impl ClientMsg {
    pub fn get_stream_id(&self) -> Option<u64> {
        match self {
            ClientMsg::MsgMeta(stream_id, _) |
            ClientMsg::PayloadData(stream_id, _, _) |
            ClientMsg::PayloadFinished(stream_id, _, _) |
            ClientMsg::AttachmentData(stream_id, _, _, _) |
            ClientMsg::AttachmentFinished(stream_id, _, _, _) |
            ClientMsg::MessageFinished(stream_id) |
            ClientMsg::Message(stream_id, _, _, _) => Some(*stream_id),
            ClientMsg::MessageAborted(stream_id) => *stream_id
        }
    }
}

pub enum RestreamMsg {
    StartSimple,
    #[cfg(feature = "http")]
    StartHttp(Value, hyper::body::Sender, Option<oneshot::Sender<StreamCompletion>>)
}

pub enum StreamCompletion {
    Ok,
    Err
}

pub async fn write(stream_id: u64, data: Vec<u8>, msg_meta_size: u64, payload_size: u64, attachments_sizes: Vec<u64>, write_tx: &mut UnboundedSender<StreamUnit>) -> Result<(), ProcessError> {    
    let msg_meta_offset = LEN_BUF_SIZE + msg_meta_size as usize;
    let payload_offset = msg_meta_offset + payload_size as usize;
    debug!("write stream_id {}, data len {}, msg_meta_offset {}, payload_offset {}", stream_id, data.len(), msg_meta_offset, payload_offset);    
    let mut data_buf = [0; DATA_BUF_SIZE];    

    write_tx.send(StreamUnit::Vector(stream_id, data[LEN_BUF_SIZE..msg_meta_offset].to_vec()))?;


    match payload_size {
        0 => {
            write_tx.send(StreamUnit::Empty(stream_id))?;
        }
        _ => {
            let mut source = &data[msg_meta_offset..payload_offset];

            loop {        
                let n = source.read(&mut data_buf).await?;        
                match n {
                    0 => break,
                    _ => write_tx.send(StreamUnit::Array(stream_id, n, data_buf))?
                }
            }
        }
    }    

    let mut prev = payload_offset as usize;

    for attachment_size in attachments_sizes {
        let attachment_offset = prev + attachment_size as usize;

        match attachment_size {
            0 => {
                write_tx.send(StreamUnit::Empty(stream_id))?;
            }
            _ => {
                let mut source = &data[prev..attachment_offset];

                loop {        
                    let n = source.read(&mut data_buf).await?;        
                    match n {
                        0 => break,
                        _ => write_tx.send(StreamUnit::Array(stream_id, n, data_buf))?
                    }
                }
            }
        }        

        prev = attachment_offset;
    }

    debug!("stream_id {} write succeeded", stream_id);

    Ok(())
}

pub async fn write_to_stream(stream_id: u64, data: Vec<u8>, msg_meta_size: u64, payload_size: u64, attachments_sizes: Vec<u64>, stream: &mut TcpStream) -> Result<(), ProcessError> {
    let msg_meta_offset = LEN_BUF_SIZE + msg_meta_size as usize;
    let payload_offset = msg_meta_offset + payload_size as usize;

    debug!("write stream_id {}, data len {}, msg_meta_offset {}, payload_offset {}", stream_id, data.len(), msg_meta_offset, payload_offset);    

    let mut data_buf = [0; DATA_BUF_SIZE];    

    write_stream_unit(stream, StreamUnit::Vector(stream_id, data[LEN_BUF_SIZE..msg_meta_offset].to_vec())).await?;

    match payload_size {
        0 => {
            write_stream_unit(stream, StreamUnit::Empty(stream_id)).await?;
        }
        _ => {
            let mut source = &data[msg_meta_offset..payload_offset];

            loop {        
                let n = source.read(&mut data_buf).await?;        
                match n {
                    0 => break,
                    _ => write_stream_unit(stream, StreamUnit::Array(stream_id, n, data_buf)).await?
                }
            }
        }
    }    

    let mut prev = payload_offset as usize;

    for attachment_size in attachments_sizes {
        let attachment_offset = prev + attachment_size as usize;

        match attachment_size {
            0 => {
                write_stream_unit(stream, StreamUnit::Empty(stream_id)).await?;
            }
            _ => {
                let mut source = &data[prev..attachment_offset];

                loop {        
                    let n = source.read(&mut data_buf).await?;        
                    match n {
                        0 => break,
                        _ => write_stream_unit(stream, StreamUnit::Array(stream_id, n, data_buf)).await?
                    }
                }
            }
        }        

        prev = attachment_offset;
    }

    debug!("stream_id {} write succeeded", stream_id);

    Ok(())
}

pub async fn write_loop(addr: String, mut client_rx: UnboundedReceiver<StreamUnit>, socket_write: &mut TcpStream) -> Result<(), ProcessError> {    
    loop {       
        match client_rx.recv().await {
            Some(res) => {
                let mut buf_u64 = BytesMut::new();
                let mut buf_u32 = BytesMut::new();

                match res {
                    StreamUnit::Array(stream_id, n, buf) => {
                        debug!("{} StreamUnit::Array write to socket attempt, n {}, stream_id {}", addr, n, stream_id);                        
                        buf_u64.put_u64(stream_id);
                        socket_write.write_all(&buf_u64[..]).await?;                        
                        buf_u32.put_u32(n as u32);
                        socket_write.write_all(&buf_u32[..]).await?;
                        socket_write.write_all(&buf[..n]).await?;
                        debug!("{} StreamUnit::Array write to socket succeded, stream_id {}", addr, stream_id);
                    }
                    StreamUnit::Vector(stream_id, buf) => {                        
                        debug!("{} StreamUnit::Vector write to socket attempt, len {}, stream_id {}", addr, buf.len(), stream_id);                        
                        buf_u64.put_u64(stream_id);
                        socket_write.write_all(&buf_u64[..]).await?;                        
                        buf_u32.put_u32(buf.len() as u32);
                        socket_write.write_all(&buf_u32[..]).await?;
                        socket_write.write_all(&buf).await?;
                        debug!("{} StreamUnit::Vector write to socket succeded, stream_id {}", addr, stream_id);
                    }
                    StreamUnit::Empty(stream_id) => {       
                        debug!("{} StreamUnit::Empty write to socket attempt, stream_id {}", addr, stream_id);                                         
                        buf_u64.put_u64(stream_id);
                        socket_write.write_all(&buf_u64[..]).await?;                        
                        buf_u32.put_u32(0);
                        socket_write.write_all(&buf_u32[..]).await?;
                        debug!("{} StreamUnit::Empty write to socket succeded, stream_id {}", addr, stream_id);
                    }
                }
            }
            None => return Err(ProcessError::WriteChannelDropped)
        }
    }
}

pub async fn write_stream_unit(socket_write: &mut TcpStream, stream_unit: StreamUnit) -> Result<(), ProcessError> {
    let mut buf_u64 = BytesMut::new();
    let mut buf_u32 = BytesMut::new();

    match stream_unit {
        StreamUnit::Array(stream_id, n, buf) => {
            debug!("StreamUnit::Array write to socket attempt, n {}, stream_id {}", n, stream_id);            
            buf_u64.put_u64(stream_id);
            socket_write.write_all(&buf_u64[..]).await?;            
            buf_u32.put_u32(n as u32);
            socket_write.write_all(&buf_u32[..]).await?;
            socket_write.write_all(&buf[..n]).await?;
            debug!("StreamUnit::Array write to socket succeded, stream_id {}", stream_id);
        }
        StreamUnit::Vector(stream_id, buf) => {                        
            debug!("StreamUnit::Vector write to socket attempt, len {}, stream_id {}", buf.len(), stream_id);            
            buf_u64.put_u64(stream_id);
            socket_write.write_all(&buf_u64[..]).await?;            
            buf_u32.put_u32(buf.len() as u32);
            socket_write.write_all(&buf_u32[..]).await?;
            socket_write.write_all(&buf).await?;
            debug!("StreamUnit::Vector write to socket succeded, stream_id {}", stream_id);
        }
        StreamUnit::Empty(stream_id) => {       
            debug!("StreamUnit::Empty write to socket attempt, stream_id {}", stream_id);                             
            buf_u64.put_u64(stream_id);
            socket_write.write_all(&buf_u64[..]).await?;            
            buf_u32.put_u32(0);
            socket_write.write_all(&buf_u32[..]).await?;
            debug!("StreamUnit::Empty write to socket succeded, stream_id {}", stream_id);
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
    pub addr: String,
    pub auth_token: Option<String>,
    pub auth_data: Option<Value>,
    hash_buf: BytesMut,
    addr_bytes_len: usize,
    hasher: SipHasher24,
    pub write_tx: UnboundedSender<StreamUnit>,
    rpc_inbound_tx: UnboundedSender<RpcMsg>
}


impl MagicBall {
    pub fn new(addr: String, write_tx: UnboundedSender<StreamUnit>, rpc_inbound_tx: UnboundedSender<RpcMsg>) -> MagicBall {
        let mut hash_buf = BytesMut::new();
        let addr_bytes = addr.as_bytes();
        let addr_bytes_len = addr_bytes.len();
        hash_buf.put(addr_bytes);
        let hasher = get_hasher();
        
        MagicBall {            
            addr,
            auth_token: None,
            auth_data: None,
            hash_buf,
            addr_bytes_len,
            hasher,
            write_tx,
            rpc_inbound_tx
        }
    }
    /// This function generates new stream id
    pub fn get_stream_id(&mut self) -> u64 {
        self.hash_buf.truncate(self.addr_bytes_len);
        //self.hash_buf.put_u32(get_counter_value());
        self.hash_buf.extend_from_slice(Uuid::new_v4().to_string().as_bytes());
        self.hasher.write(&self.hash_buf);
        self.hasher.finish()
    }
    /// This function should be called for single message or parts of it (not for multiple messages inside vec)
    /// stream_id value MUST BE ACQUIRED with get_stream_id() function. stream_id generation can be implicit, however this will leads to less flexible API (if for example you need stream payload or attachments data).
    /// attachments_sizes parameter should be not empty ONLY IF data parameter contains attachments themself.
    /// If you plan to write attachments data later (for example in streaming fashion), leave attachments_sizes parameter empty and only fill attachment sizes in msg_meta
    pub async fn write_vec(&mut self, stream_id: u64, data: Vec<u8>, msg_meta_size: u64, payload_size: u64, attachments_sizes: Vec<u64>) -> Result<(), ProcessError> {
        let msg_meta_offset = LEN_BUF_SIZE + msg_meta_size as usize;
        let payload_offset = msg_meta_offset + payload_size as usize;
        let mut data_buf = [0; DATA_BUF_SIZE];

        let mut source = &data[LEN_BUF_SIZE..msg_meta_offset];    

        loop {        
            let n = source.read(&mut data_buf).await?;
            match n {
                0 => break,
                _ => self.write_tx.send(StreamUnit::Array(stream_id, n, data_buf))?
            }
        }

        match payload_size {
            0 => {
                self.write_tx.send(StreamUnit::Empty(stream_id))?
            }
            _ => {            
                let mut source = &data[msg_meta_offset..payload_offset];

                loop {        
                    let n = source.read(&mut data_buf).await?;        
                    match n {
                        0 => break,
                        _ => self.write_tx.send(StreamUnit::Array(stream_id, n, data_buf))?
                    }
                }
            }
        }

        let mut prev = payload_offset as usize;

        for attachment_size in attachments_sizes {
            let attachment_offset = prev + attachment_size as usize;            

            match attachment_size {
                0 => {
                    self.write_tx.send(StreamUnit::Empty(stream_id))?
                }
                _ => {
                    let mut source = &data[prev..attachment_offset];

                    loop {        
                        let n = source.read(&mut data_buf).await?;        
                        match n {
                            0 => break,
                            _ => self.write_tx.send(StreamUnit::Array(stream_id, n, data_buf))?
                        }
                    }                    
                }
            }            

            prev = attachment_offset;
        }

        Ok(())
    }
    pub async fn send_event<T>(&mut self, key: Key, payload: T) -> Result<(), ProcessError> where T: serde::Serialize, for<'de> T: serde::Deserialize<'de>, T: Debug {
        let route = Route {
            source: Participator::Service(self.addr.clone()),
            spec: RouteSpec::Simple,
            points: vec![Participator::Service(self.addr.to_owned())]
        };

        let (dto, msg_meta_size, payload_size, attachments_sizes) = event_dto_with_sizes(self.addr.clone(), key.to_owned(), payload, route, self.auth_token.clone(), self.auth_data.clone())?;

        write(self.get_stream_id(), dto, msg_meta_size, payload_size, attachments_sizes, &mut self.write_tx).await?;
        
        Ok(())
    }
    pub async fn send_event_with_route<T>(&mut self, key: Key, payload: T, mut route: Route) -> Result<(), ProcessError> where T: serde::Serialize, for<'de> T: serde::Deserialize<'de>, T: Debug {
        //info!("send_event, route {:?}, key {}, payload {:?}, ", route, addr, key, payload);

        route.points.push(Participator::Service(self.addr.clone()));

        let (dto, msg_meta_size, payload_size, attachments_sizes) = event_dto_with_sizes(self.addr.clone(), key.to_owned(), payload, route, self.auth_token.clone(), self.auth_data.clone())?;

        write(self.get_stream_id(), dto, msg_meta_size, payload_size, attachments_sizes, &mut self.write_tx).await?;
        
        Ok(())
    }    
    pub async fn rpc<T, R>(&mut self, key: Key, payload: T) -> Result<Message<R>, ProcessError> where T: serde::Serialize, T: Debug, for<'de> R: serde::Deserialize<'de>, R: Debug {
        let route = Route {
            source: Participator::Service(self.addr.clone()),
            spec: RouteSpec::Simple,
            points: vec![Participator::Service(self.addr.to_owned())]
        };

		//info!("send_rpc, route {:?}, key {}, payload {:?}, ", route, key, payload);
		
        let (correlation_id, dto, msg_meta_size, payload_size, attachments_sizes) = rpc_dto_with_correlation_id_sizes(self.addr.clone(), key.to_owned(), payload, route, self.auth_token.clone(), self.auth_data.clone())?;
        let (rpc_tx, rpc_rx) = oneshot::channel();
        
        self.rpc_inbound_tx.send(RpcMsg::AddRpc(correlation_id, rpc_tx))?;
        write(self.get_stream_id(), dto, msg_meta_size, payload_size, attachments_sizes, &mut self.write_tx).await?;        

        let (msg_meta, payload, attachments_data) = timeout(Duration::from_millis(RPC_TIMEOUT_MS_AMOUNT), rpc_rx).await??;
        let payload: R = from_slice(&payload)?;        

        Ok(Message {
            meta: msg_meta, 
            payload, 
            attachments_data
        })
    }
    pub async fn rpc_with_route<T, R>(&mut self, key: Key, payload: T, mut route: Route) -> Result<Message<R>, ProcessError> where T: serde::Serialize, T: Debug, for<'de> R: serde::Deserialize<'de>, R: Debug {
		//info!("send_rpc, route {:?}, key {}, payload {:?}, ", route, key, payload);

        route.points.push(Participator::Service(self.addr.to_owned()));
		
        let (correlation_id, dto, msg_meta_size, payload_size, attachments_sizes) = rpc_dto_with_correlation_id_sizes(self.addr.clone(), key.to_owned(), payload, route, self.auth_token.clone(), self.auth_data.clone())?;
        let (rpc_tx, rpc_rx) = oneshot::channel();
        
        self.rpc_inbound_tx.send(RpcMsg::AddRpc(correlation_id, rpc_tx))?;
        write(self.get_stream_id(), dto, msg_meta_size, payload_size, attachments_sizes, &mut self.write_tx).await?;

        let (msg_meta, payload, attachments_data) = timeout(Duration::from_millis(RPC_TIMEOUT_MS_AMOUNT), rpc_rx).await??;
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

        write(self.get_stream_id(), buf, msg_meta_size, payload_size, attachments_sizes,  &mut self.write_tx).await?;
        
        Ok(())
    }
    pub async fn proxy_event_with_auth_data(&mut self, tx: String, auth_data: Value, mut data: Vec<u8>) -> Result<(), ProcessError> {
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
        match auth_data["domain"].as_str() {
            Some(domain) if msg_meta.key.domain == "Default" => msg_meta.key.domain = domain.to_owned(),
            _ => {}
        }
        msg_meta.auth_data = Some(auth_data);
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

        write(self.get_stream_id(), buf, msg_meta_size, payload_size, attachments_sizes,  &mut self.write_tx).await?;
        
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
                
        self.rpc_inbound_tx.send(RpcMsg::AddRpc(correlation_id, rpc_tx))?;
        debug!("proxy_rpc write attempt");
        write(self.get_stream_id(), buf, msg_meta_size, payload_size, attachments_sizes, &mut self.write_tx).await?;
        debug!("proxy_rpc write attempt succeeded");

        let (msg_meta, mut payload, mut attachments_data) = timeout(Duration::from_millis(RPC_TIMEOUT_MS_AMOUNT), rpc_rx).await??;

        let mut buf = vec![];
        let mut msg_meta_buf = to_vec(&msg_meta)?;

        buf.put_u32(msg_meta_buf.len() as u32);
        buf.append(&mut msg_meta_buf);
        buf.append(&mut payload);
        buf.append(&mut attachments_data);
        
        Ok((msg_meta, buf))
    }
    pub async fn proxy_rpc_with_auth_data(&mut self, tx: String, auth_data: Value, mut data: Vec<u8>) -> Result<(MsgMeta, Vec<u8>), ProcessError> {
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
        match auth_data["domain"].as_str() {
            Some(domain) if msg_meta.key.domain == "Default" => msg_meta.key.domain = domain.to_owned(),
            _ => {}
        }
        msg_meta.auth_data = Some(auth_data);
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
                
        self.rpc_inbound_tx.send(RpcMsg::AddRpc(correlation_id, rpc_tx))?;
        debug!("proxy_rpc_with_auth_data write attempt");
        write(self.get_stream_id(), buf, msg_meta_size, payload_size, attachments_sizes, &mut self.write_tx).await?;
        debug!("proxy_rpc_with_auth_data write attempt succeeded");

        let (msg_meta, mut payload, mut attachments_data) = timeout(Duration::from_millis(RPC_TIMEOUT_MS_AMOUNT), rpc_rx).await??;

        let mut buf = vec![];
        let mut msg_meta_buf = to_vec(&msg_meta)?;

        buf.put_u32(msg_meta_buf.len() as u32);
        buf.append(&mut msg_meta_buf);
        buf.append(&mut payload);
        buf.append(&mut attachments_data);
        
        Ok((msg_meta, buf))
    }
    pub async fn proxy_rpc_with_payload<T>(&mut self, tx: String, mut data: Vec<u8>) -> Result<(MsgMeta, T, Vec<u8>), ProcessError> where for<'de> T: serde::Deserialize<'de>, T: Debug {
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
                
        self.rpc_inbound_tx.send(RpcMsg::AddRpc(correlation_id, rpc_tx))?;
        debug!("proxy_rpc_with_payload write attempt");
        write(self.get_stream_id(), buf, msg_meta_size, payload_size, attachments_sizes, &mut self.write_tx).await?;
        debug!("proxy_rpc_with_payload write attempt succeeded");

        let (msg_meta, payload, attachments_data) = timeout(Duration::from_millis(RPC_TIMEOUT_MS_AMOUNT), rpc_rx).await??;

        let payload: T = from_slice(&payload)?;
        
        Ok((msg_meta, payload, attachments_data))
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
    StreamIdIsZero,
    NotEnoughBytesForLen,
    WriteChannelDropped,
    IncorrectReadResult,    
    Io(std::io::Error),
    SerdeJson(serde_json::Error),
    GetFile(GetFileError),    
    SendStreamUnitError,
    SendServerMsgError,
    SendClientMsgError,
    SendRpcMsgError,
    OneshotRecvError(oneshot::error::RecvError),
    Timeout,
    NoneError,
    TrySendServerMsg,
    TrySendClientMsg,
    TrySendStreamUnit,
    TrySendRpcMsg
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
	fn from(e: std::io::Error) -> ProcessError {
		ProcessError::Io(e)
	}
}

impl From<serde_json::Error> for ProcessError {
	fn from(e: serde_json::Error) -> ProcessError {
		ProcessError::SerdeJson(e)
	}
}

impl From<option::NoneError> for ProcessError {
	fn from(_: option::NoneError) -> ProcessError {
		ProcessError::NoneError
	}
}

impl From<SendError<StreamUnit>> for ProcessError {
	fn from(_: SendError<StreamUnit>) -> ProcessError {
		ProcessError::SendStreamUnitError
	}
}

impl From<SendError<ServerMsg>> for ProcessError {
	fn from(_: SendError<ServerMsg>) -> ProcessError {
		ProcessError::SendServerMsgError
	}
}

impl From<SendError<ClientMsg>> for ProcessError {
	fn from(_: SendError<ClientMsg>) -> ProcessError {
		ProcessError::SendClientMsgError
	}
}

impl From<SendError<RpcMsg>> for ProcessError {
	fn from(_: SendError<RpcMsg>) -> ProcessError {
		ProcessError::SendRpcMsgError
	}
}

impl From<oneshot::error::RecvError> for ProcessError {
	fn from(e: oneshot::error::RecvError) -> ProcessError {
		ProcessError::OneshotRecvError(e)
	}
}

impl From<tokio::time::error::Elapsed> for ProcessError {
	fn from(_: tokio::time::error::Elapsed) -> ProcessError {
		ProcessError::Timeout
	}
}

impl From<TrySendError<ServerMsg>> for ProcessError {
	fn from(_: TrySendError<ServerMsg>) -> ProcessError {
		ProcessError::TrySendServerMsg
	}
}

impl From<TrySendError<ClientMsg>> for ProcessError {
	fn from(_: TrySendError<ClientMsg>) -> ProcessError {
		ProcessError::TrySendClientMsg
	}
}

impl From<TrySendError<StreamUnit>> for ProcessError {
	fn from(_: TrySendError<StreamUnit>) -> ProcessError {
		ProcessError::TrySendStreamUnit
	}
}

impl From<TrySendError<RpcMsg>> for ProcessError {
	fn from(_: TrySendError<RpcMsg>) -> ProcessError {
		ProcessError::TrySendRpcMsg
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
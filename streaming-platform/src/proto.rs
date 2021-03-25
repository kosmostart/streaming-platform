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
use byteorder::ByteOrder;
use serde_json::{from_slice, Value, to_vec};
use siphasher::sip::SipHasher24;
use tokio::net::TcpStream;
use tokio::sync::{mpsc::{UnboundedSender, UnboundedReceiver, error::{SendError, TrySendError}}, oneshot};
//use tokio::time::{timeout, error::Elapsed};
use tokio::time::timeout;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use sp_dto::bytes::{Buf, BytesMut, BufMut};
use sp_dto::{*, uuid::Uuid};

pub const STREAM_ID_BUF_SIZE: usize = 8;
pub const LEN_BUF_SIZE: usize = 4;
pub const LENS_BUF_SIZE: usize = 12;

pub const FRAME_HEADER_SIZE: usize = 28;
pub const MAX_FRAME_PAYLOAD_SIZE: usize = 1024;

pub const MAX_FRAME_SIZE: usize = FRAME_HEADER_SIZE + MAX_FRAME_PAYLOAD_SIZE;

/*
u8 frame_type 1
u16 frame_size 2
u8 msg_type 1
u64 key_hash 8
u64 stream_id 8
u64 frame_signature 8
*/

//pub const MPSC_SERVER_BUF_SIZE: usize = 1000000;
//pub const MPSC_CLIENT_BUF_SIZE: usize = 1000000;
//pub const MPSC_RPC_BUF_SIZE: usize = 1000000;
pub const RPC_TIMEOUT_MS_AMOUNT: u64 = 30000;
//pub const STREAM_UNIT_READ_TIMEOUT_MS_AMOUNT: u64 = 1000;

pub fn get_key_hasher() -> SipHasher24 {    
    SipHasher24::new_with_keys(0, random::<u64>())
}

pub fn get_stream_id_hasher() -> SipHasher24 {    
    SipHasher24::new_with_keys(0, random::<u64>())
}

pub fn get_stream_id_onetime(addr: &str) -> u64 {
    let mut buf = BytesMut::new();
    buf.put(addr.as_bytes());
    //buf.put_u32(get_counter_value());
    buf.extend_from_slice(Uuid::new_v4().to_string().as_bytes());
    let mut hasher = get_stream_id_hasher();
    hasher.write(&buf);
    hasher.finish()
}

pub struct StreamLayout {
    pub id: u64,
    pub msg_meta: Vec<u8>,
    pub payload: Vec<u8>,
    pub attachments_data: Vec<u8>
}

#[derive(Debug, Clone)]
pub struct Frame {
    pub frame_type: u8,
    pub payload_size: u16,
    pub msg_type: u8,
    pub key_hash: u64,
    pub stream_id: u64,
    pub frame_signature: u64,
    pub payload: [u8; MAX_FRAME_PAYLOAD_SIZE]
}

pub enum FrameType {
    MsgMeta = 0,
    Payload = 1,
    Attachment = 2,
    End = 3
}

impl Frame {
    pub fn new(frame_type: u8, payload_size: u16, msg_type: u8, key_hash: u64, stream_id: u64, payload: [u8; MAX_FRAME_PAYLOAD_SIZE]) -> Frame {
        let frame_signature = 0;

        Frame {
            frame_type,
            payload_size,
            msg_type,
            key_hash,
            stream_id,
            frame_signature,
            payload
        }
    }
    pub fn get_frame_type(&self) -> Result<FrameType, ProcessError> {
        Ok(match self.frame_type {
            0 => FrameType::MsgMeta,
            1 => FrameType::Payload,
            2 => FrameType::Attachment,
            _ => return Err(ProcessError::IncorrectFrameType)
        })
    }
    pub fn get_msg_type(&self) -> Result<MsgType, ProcessError> {
        Ok(match self.msg_type {
            0 => MsgType::Event,
            1 => MsgType::RpcRequest,
            2 => MsgType::RpcResponse(RpcResult::Ok),
            3 => MsgType::RpcResponse(RpcResult::Err),
            _ => return Err(ProcessError::IncorrectMsgType)
        })
    }
}
pub struct State2 {
    bytes_read: usize,
    frame_buf: [u8; MAX_FRAME_SIZE],
    read_buf: [u8; MAX_FRAME_SIZE],
    offset: usize
}

pub enum NextAction {
    ReadFrame,
    ReadMoreBytes
}

pub enum ReadFrameResult {
    Frame(Frame),
    ReadMoreBytes
}

impl State2 {
    pub fn new() -> State2 {
        State2 {
            bytes_read: 0,
            frame_buf: [0; MAX_FRAME_SIZE],
            read_buf: [0; MAX_FRAME_SIZE],
            offset: 0
        }
    }
    pub fn clear(&mut self) {
        self.bytes_read = 0;
        self.offset = 0;
    }
    pub async fn read_from_tcp_stream(&mut self, tcp_stream: &mut TcpStream) -> Result<NextAction, ProcessError> {
        let bytes_read = tcp_stream.read(&mut self.read_buf[..]).await?;

        match bytes_read {
            0 => {
                error!("Read 0 bytes");
                Ok(NextAction::ReadMoreBytes)
            }
            _ => {
                let mut i = 0;

                while i < bytes_read {
                    self.frame_buf[self.bytes_read + i] = self.read_buf[i];

                    i = i + 1;
                }

                self.bytes_read = self.bytes_read + bytes_read;

                debug!("Bytes read from tcp stream: {}, bytes read in state: {}", bytes_read, self.bytes_read);

                match self.bytes_read >= FRAME_HEADER_SIZE {
                    true => {
                        self.offset = 0;

                        Ok(NextAction::ReadFrame)
                    }
                    false => Ok(NextAction::ReadMoreBytes)
                }
            }
        }
    }
    pub fn read_frame(&mut self) -> ReadFrameResult {
        debug!("Got frame type {}, offset {}", self.frame_buf[self.offset], self.offset);

        //debug!("{:?}", self.frame_buf);

        let payload_size_u16 = byteorder::BigEndian::read_u16(&self.frame_buf[self.offset + 1..self.offset + 3]);
        let payload_size = payload_size_u16 as usize;

        debug!("Got payload size {}", payload_size_u16);

        let frame_size = FRAME_HEADER_SIZE + payload_size;

        match self.offset + frame_size <= self.bytes_read {
            true => {
                let key_hash = byteorder::BigEndian::read_u64(&self.frame_buf[self.offset + 4..self.offset + 12]);
                let stream_id = byteorder::BigEndian::read_u64(&self.frame_buf[self.offset + 12..self.offset + 20]);
                let frame_signature = byteorder::BigEndian::read_u64(&self.frame_buf[self.offset + 20..self.offset + 28]);

                let payload_slice = &self.frame_buf[self.offset + FRAME_HEADER_SIZE..self.offset + frame_size];
                let mut payload = [0; MAX_FRAME_PAYLOAD_SIZE];

                let mut i = 0;

                while i < payload_size {
                    payload[i] = payload_slice[i];
                    i = i + 1;
                }

                let res = Frame {
                    frame_type: self.frame_buf[self.offset],
                    payload_size: payload_size_u16,
                    msg_type: self.frame_buf[self.offset + 3],
                    key_hash,
                    stream_id,
                    frame_signature,
                    payload
                };

                self.offset = self.offset + frame_size;

                ReadFrameResult::Frame(res)
            },
            false => {
                let mut i = 0;
                let frame_part_len = self.bytes_read - self.offset;

                while i < frame_part_len {
                    self.frame_buf[i] = self.frame_buf[self.offset + i];
                    i = i + 1;
                }

                self.bytes_read = frame_part_len;

                ReadFrameResult::ReadMoreBytes
            }
        }
    }
    /*
    pub async fn read_frame(&mut self, n: usize) -> Result<Option<Frame>, ProcessError> {
        let mut i = 0;

        if self.frame_type.is_none() {
            debug!("Got frame type {}", self.read_buf[0]);

            self.frame_type = Some(self.read_buf[0]);
        }

        if self.payload_size.is_none() && n >= 3 {
            let payload_size_u16 = byteorder::BigEndian::read_u16(&self.read_buf[1..3]);
            let payload_size = payload_size_u16 as usize;

            debug!("Got payload size {}", payload_size);

            if payload_size > MAX_FRAME_PAYLOAD_SIZE {
                return Err(ProcessError::FramePayloadSizeExceeded);
            }

            self.payload_size = Some(payload_size);
            self.payload_size_u16 = Some(payload_size_u16);
        }

        match self.payload_size {
            Some(payload_size) => {

                let frame_size = FRAME_HEADER_SIZE + payload_size;

                match self.offset + n >= frame_size {
                    true => {
                        debug!("Got frame, frame type {:?}, frame size {}", self.frame_type, frame_size);

                        while i < frame_size - self.offset {
                            self.frame_buf[self.offset + i] = self.read_buf[i];
                            i = i + 1;
                        }

                        let bytes_moved = i;

                        debug!("Bytes moved: {}", bytes_moved);
                        
                        let key_hash = byteorder::BigEndian::read_u64(&self.frame_buf[4..12]);
                        let stream_id = byteorder::BigEndian::read_u64(&self.frame_buf[12..20]);
                        let frame_signature = byteorder::BigEndian::read_u64(&self.frame_buf[20..28]);

                        i = 0;

                        let payload_slice = &self.frame_buf[FRAME_HEADER_SIZE..frame_size];
                        let mut payload = [0; MAX_FRAME_PAYLOAD_SIZE];

                        while i < payload_size {
                            payload[i] = payload_slice[i];
                            i = i + 1;
                        }

                        let res = Frame {
                            frame_type: self.frame_type?,
                            payload_size: payload_size as u16,
                            msg_type: self.frame_buf[3],
                            key_hash,
                            stream_id,
                            frame_signature,
                            payload
                        };

                        i = 0;
                        self.clear();
    
                        while i < n - bytes_moved {
                            self.frame_buf[self.offset + i] = self.read_buf[bytes_moved + i];
                            i = i + 1;
                        }

                        Ok(Some(res))
                    }
                    false => {
                        while i < n {
                            self.frame_buf[self.offset + i] = self.read_buf[i];
                            i = i + 1;
                        }
                        
                        self.offset = self.offset + n;

                        Ok(None)
                    }
                }
            }
            None => Ok(None)
        }
    }
    */
}

pub struct Client {
    pub net_addr: SocketAddr,
    pub tx: UnboundedSender<Frame>
}

pub enum ServerMsg {
    AddClient(String, SocketAddr, UnboundedSender<Frame>),
    RemoveClient(String),
    Send(String, Frame)
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
    /// This is sent in Stream mode
    Frame(Frame),
    /// This is sent in FullMessage mode
    Message(u64, MsgMeta, Vec<u8>, Vec<u8>)
}

impl ClientMsg {
    pub fn get_stream_id(&self) -> u64 {
        match self {
            ClientMsg::Frame(frame) => frame.stream_id, 
            ClientMsg::Message(stream_id, _, _, _) => *stream_id
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

pub fn get_key_hash(hasher: &mut SipHasher24, buf: &mut BytesMut, key: Key) -> u64 {
    debug!("Creating hash for: {:?}", key);
    buf.clear();
    buf.put((key.service + &key.action + &key.domain).as_bytes());  
    hasher.write(&buf);

    let res = hasher.finish();

    debug!("Created hash for key, hash {}", res);

    res
}

pub async fn write_frame(tcp_stream: &mut TcpStream, frame: Frame) -> Result<(), ProcessError> {
    debug!("Frame write to socket attempt, stream_id {}, frame type {}", frame.stream_id, frame.frame_type);

    let mut header = [0; FRAME_HEADER_SIZE];

    header[0] = frame.frame_type;
    byteorder::BigEndian::write_u16(&mut header[1..3], frame.payload_size);
    header[3] = frame.msg_type;
    byteorder::BigEndian::write_u64(&mut header[4..12], frame.key_hash);
    byteorder::BigEndian::write_u64(&mut header[12..20], frame.stream_id);
    byteorder::BigEndian::write_u64(&mut header[20..28], frame.frame_signature);

    tcp_stream.write_all(&header[..]).await?;

    if frame.payload_size > 0 {
        tcp_stream.write_all(&frame.payload[..frame.payload_size as usize]).await?;
    }

    Ok(())
}

pub async fn write_loop(addr: String, mut client_rx: UnboundedReceiver<Frame>, tcp_stream: &mut TcpStream) -> Result<(), ProcessError> {    
    loop {       
        match client_rx.recv().await {
            Some(frame) => {
                match write_frame(tcp_stream, frame).await {
                    Ok(()) => {}
                    Err(e) => error!("Error writing frame in write loop: {:?}", e)
                }
            }
            None => return Err(ProcessError::WriteChannelDropped)
        }
    }
}

// Use this only for single message or parts of it
pub async fn write_to_tcp_stream(tcp_stream: &mut TcpStream, msg_type: u8, key_hash: u64, stream_id: u64, data: Vec<u8>, msg_meta_size: u64, payload_size: u64, attachments_sizes: Vec<u64>) -> Result<(), ProcessError> {
    let msg_meta_offset = LEN_BUF_SIZE + msg_meta_size as usize;
    let payload_offset = msg_meta_offset + payload_size as usize;
    let mut data_buf = [0; MAX_FRAME_PAYLOAD_SIZE];

    let mut source = &data[LEN_BUF_SIZE..msg_meta_offset];

    loop {        
        let n = source.read(&mut data_buf).await?;
        match n {
            0 => break,
            _ => write_frame(tcp_stream, Frame::new(FrameType::MsgMeta as u8, n as u16, msg_type, key_hash, stream_id, data_buf)).await?
        }
    }

    match payload_size {
        0 => {
            //self.write_tx.send(Frame::Empty(stream_id))?
        }
        _ => {            
            let mut source = &data[msg_meta_offset..payload_offset];

            loop {        
                let n = source.read(&mut data_buf).await?;        
                match n {
                    0 => break,
                    _ => write_frame(tcp_stream, Frame::new(FrameType::Payload as u8, n as u16, msg_type, key_hash, stream_id, data_buf)).await?
                }
            }
        }
    }

    let mut prev = payload_offset as usize;

    for attachment_size in attachments_sizes {
        let attachment_offset = prev + attachment_size as usize;            

        match attachment_size {
            0 => {
                //self.write_tx.send(Frame::Empty(stream_id))?
            }
            _ => {
                let mut source = &data[prev..attachment_offset];

                loop {        
                    let n = source.read(&mut data_buf).await?;        
                    match n {
                        0 => break,
                        _ => write_frame(tcp_stream, Frame::new(FrameType::Attachment as u8, n as u16, msg_type, key_hash, stream_id, data_buf)).await?
                    }
                }                    
            }
        }            

        prev = attachment_offset;
    }

    write_frame(tcp_stream, Frame::new(FrameType::End as u8, 0, msg_type, key_hash, stream_id, [0; MAX_FRAME_PAYLOAD_SIZE])).await?;

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
    key_hasher: SipHasher24,
    key_hash_buf: BytesMut,
    hasher: SipHasher24,
    hash_buf: BytesMut,
    addr_bytes_len: usize,
    write_tx: UnboundedSender<Frame>,
    rpc_inbound_tx: UnboundedSender<RpcMsg>
}


impl MagicBall {
    pub fn new(addr: String, write_tx: UnboundedSender<Frame>, rpc_inbound_tx: UnboundedSender<RpcMsg>) -> MagicBall {
        let key_hasher = get_key_hasher();
        let mut key_hash_buf = BytesMut::new();

        let hasher = get_stream_id_hasher();
        let mut hash_buf = BytesMut::new();

        let addr_bytes = addr.as_bytes();
        let addr_bytes_len = addr_bytes.len();

        hash_buf.put(addr_bytes);
        
        MagicBall {            
            addr,
            auth_token: None,
            auth_data: None,
            key_hasher,
            key_hash_buf,
            hasher,
            hash_buf,
            addr_bytes_len,
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

    /// This function generates hash for key
    pub fn get_key_hash(&mut self, key: Key) -> u64 {
        get_key_hash(&mut self.key_hasher, &mut self.key_hash_buf, key)
    }
    /// This function should be called for single message or parts of it (not for multiple messages inside vec)
    /// stream_id value MUST BE ACQUIRED with get_stream_id() function. stream_id generation can be implicit, however this will leads to less flexible API (if for example you need stream payload or attachments data).
    /// attachments_sizes parameter should be not empty ONLY IF data parameter contains attachments themself.
    /// If you plan to write attachments data later (for example in streaming fashion), leave attachments_sizes parameter empty and only fill attachment sizes in msg_meta
    pub async fn write(&mut self, msg_type: u8, key_hash: u64, stream_id: u64, data: Vec<u8>, msg_meta_size: u64, payload_size: u64, attachments_sizes: Vec<u64>) -> Result<(), ProcessError> {
        let msg_meta_offset = LEN_BUF_SIZE + msg_meta_size as usize;
        let payload_offset = msg_meta_offset + payload_size as usize;
        let mut data_buf = [0; MAX_FRAME_PAYLOAD_SIZE];

        let mut source = &data[LEN_BUF_SIZE..msg_meta_offset];    

        loop {        
            let n = source.read(&mut data_buf).await?;
            match n {
                0 => break,
                _ => self.write_tx.send(Frame::new(FrameType::MsgMeta as u8, n as u16, msg_type, key_hash, stream_id, data_buf))?
            }
        }

        match payload_size {
            0 => {
                //self.write_tx.send(Frame::Empty(stream_id))?
            }
            _ => {            
                let mut source = &data[msg_meta_offset..payload_offset];

                loop {        
                    let n = source.read(&mut data_buf).await?;        
                    match n {
                        0 => break,
                        _ => self.write_tx.send(Frame::new(FrameType::Payload as u8, n as u16, msg_type, key_hash, stream_id, data_buf))?
                    }
                }
            }
        }

        let mut prev = payload_offset as usize;

        for attachment_size in attachments_sizes {
            let attachment_offset = prev + attachment_size as usize;            

            match attachment_size {
                0 => {
                    //self.write_tx.send(Frame::Empty(stream_id))?
                }
                _ => {
                    let mut source = &data[prev..attachment_offset];

                    loop {        
                        let n = source.read(&mut data_buf).await?;        
                        match n {
                            0 => break,
                            _ => self.write_tx.send(Frame::new(FrameType::Attachment as u8, n as u16, msg_type, key_hash, stream_id, data_buf))?
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

        let (dto, msg_meta_size, payload_size, attachments_sizes) = event_dto_with_sizes(self.addr.clone(), key.clone(), payload, route, self.auth_token.clone(), self.auth_data.clone())?;

        let key_hash = get_key_hash(&mut self.key_hasher, &mut self.key_hash_buf, key);
        let stream_id = self.get_stream_id();

        self.write(MsgType::Event.get_u8(), key_hash, stream_id, dto, msg_meta_size, payload_size, attachments_sizes).await?;
        
        Ok(())
    }
    pub async fn send_event_with_route<T>(&mut self, key: Key, payload: T, mut route: Route) -> Result<(), ProcessError> where T: serde::Serialize, for<'de> T: serde::Deserialize<'de>, T: Debug {
        //info!("send_event, route {:?}, key {}, payload {:?}, ", route, addr, key, payload);

        route.points.push(Participator::Service(self.addr.clone()));

        let (dto, msg_meta_size, payload_size, attachments_sizes) = event_dto_with_sizes(self.addr.clone(), key.clone(), payload, route, self.auth_token.clone(), self.auth_data.clone())?;

        let key_hash = get_key_hash(&mut self.key_hasher, &mut self.key_hash_buf, key);
        let stream_id = self.get_stream_id();

        self.write(MsgType::Event.get_u8(), key_hash, stream_id, dto, msg_meta_size, payload_size, attachments_sizes).await?;
        
        Ok(())
    }    
    pub async fn rpc<T, R>(&mut self, key: Key, payload: T) -> Result<Message<R>, ProcessError> where T: serde::Serialize, T: Debug, for<'de> R: serde::Deserialize<'de>, R: Debug {
        let route = Route {
            source: Participator::Service(self.addr.clone()),
            spec: RouteSpec::Simple,
            points: vec![Participator::Service(self.addr.to_owned())]
        };

		//info!("send_rpc, route {:?}, key {}, payload {:?}, ", route, key, payload);
		
        let (correlation_id, dto, msg_meta_size, payload_size, attachments_sizes) = rpc_dto_with_correlation_id_sizes(self.addr.clone(), key.clone(), payload, route, self.auth_token.clone(), self.auth_data.clone())?;
        let (rpc_tx, rpc_rx) = oneshot::channel();
        
        self.rpc_inbound_tx.send(RpcMsg::AddRpc(correlation_id, rpc_tx))?;

        let key_hash = get_key_hash(&mut self.key_hasher, &mut self.key_hash_buf, key);
        let stream_id = self.get_stream_id();

        self.write(MsgType::RpcRequest.get_u8(), key_hash, stream_id, dto, msg_meta_size, payload_size, attachments_sizes).await?;       

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
		
        let (correlation_id, dto, msg_meta_size, payload_size, attachments_sizes) = rpc_dto_with_correlation_id_sizes(self.addr.clone(), key.clone(), payload, route, self.auth_token.clone(), self.auth_data.clone())?;
        let (rpc_tx, rpc_rx) = oneshot::channel();
        
        self.rpc_inbound_tx.send(RpcMsg::AddRpc(correlation_id, rpc_tx))?;

        let key_hash = get_key_hash(&mut self.key_hasher, &mut self.key_hash_buf, key);
        let stream_id = self.get_stream_id();

        self.write(MsgType::RpcRequest.get_u8(), key_hash, stream_id, dto, msg_meta_size, payload_size, attachments_sizes).await?;

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

        let mut msg_meta_vec = to_vec(&msg_meta)?;
        let msg_meta_size = msg_meta_vec.len() as u64;
                                                      
        let mut payload_with_attachments: Vec<_> = data.drain(4 + len..).collect();
        let mut buf = vec![];

        buf.put_u32(msg_meta_vec.len() as u32);

        buf.append(&mut msg_meta_vec);
        buf.append(&mut payload_with_attachments);

        let key_hash = get_key_hash(&mut self.key_hasher, &mut self.key_hash_buf, msg_meta.key);
        let stream_id = self.get_stream_id();

        self.write(msg_meta.msg_type.get_u8(), key_hash, stream_id, buf, msg_meta_size, payload_size, attachments_sizes).await?;
        
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

        let mut msg_meta_vec = serde_json::to_vec(&msg_meta)?;
        let msg_meta_size = msg_meta_vec.len() as u64;
                                                      
        let mut payload_with_attachments: Vec<_> = data.drain(4 + len..).collect();
        let mut buf = vec![];

        buf.put_u32(msg_meta_vec.len() as u32);

        buf.append(&mut msg_meta_vec);
        buf.append(&mut payload_with_attachments);

        let key_hash = get_key_hash(&mut self.key_hasher, &mut self.key_hash_buf, msg_meta.key);
        let stream_id = self.get_stream_id();

        self.write(msg_meta.msg_type.get_u8(), key_hash, stream_id, buf, msg_meta_size, payload_size, attachments_sizes).await?;
        
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

        let mut msg_meta_vec = to_vec(&msg_meta)?;
        let msg_meta_size = msg_meta_vec.len() as u64;
                                                      
        let mut payload_with_attachments: Vec<_> = data.drain(4 + len..).collect();
        let mut buf = vec![];

        buf.put_u32(msg_meta_vec.len() as u32);

        buf.append(&mut msg_meta_vec);
        buf.append(&mut payload_with_attachments);

        let (rpc_tx, rpc_rx) = oneshot::channel();
                
        self.rpc_inbound_tx.send(RpcMsg::AddRpc(correlation_id, rpc_tx))?;

        debug!("proxy_rpc write attempt");

        let key_hash = get_key_hash(&mut self.key_hasher, &mut self.key_hash_buf, msg_meta.key);
        let stream_id = self.get_stream_id();

        self.write(msg_meta.msg_type.get_u8(), key_hash, stream_id, buf, msg_meta_size, payload_size, attachments_sizes).await?;

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

        let mut msg_meta_vec = to_vec(&msg_meta)?;
        let msg_meta_size = msg_meta_vec.len() as u64;
                                                      
        let mut payload_with_attachments: Vec<_> = data.drain(4 + len..).collect();
        let mut buf = vec![];

        buf.put_u32(msg_meta_vec.len() as u32);

        buf.append(&mut msg_meta_vec);
        buf.append(&mut payload_with_attachments);

        let (rpc_tx, rpc_rx) = oneshot::channel();
                
        self.rpc_inbound_tx.send(RpcMsg::AddRpc(correlation_id, rpc_tx))?;

        debug!("proxy_rpc_with_auth_data write attempt");

        let key_hash = get_key_hash(&mut self.key_hasher, &mut self.key_hash_buf, msg_meta.key);
        let stream_id = self.get_stream_id();

        self.write(msg_meta.msg_type.get_u8(), key_hash, stream_id, buf, msg_meta_size, payload_size, attachments_sizes).await?;

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

        let mut msg_meta_vec = to_vec(&msg_meta)?;
        let msg_meta_size = msg_meta_vec.len() as u64;
                                                      
        let mut payload_with_attachments: Vec<_> = data.drain(4 + len..).collect();
        let mut buf = vec![];

        buf.put_u32(msg_meta_vec.len() as u32);

        buf.append(&mut msg_meta_vec);
        buf.append(&mut payload_with_attachments);

        let (rpc_tx, rpc_rx) = oneshot::channel();
                
        self.rpc_inbound_tx.send(RpcMsg::AddRpc(correlation_id, rpc_tx))?;

        debug!("proxy_rpc_with_payload write attempt");

        let key_hash = get_key_hash(&mut self.key_hasher, &mut self.key_hash_buf, msg_meta.key);
        let stream_id = self.get_stream_id();

        self.write(msg_meta.msg_type.get_u8(), key_hash, stream_id, buf, msg_meta_size, payload_size, attachments_sizes).await?;

        debug!("proxy_rpc_with_payload write attempt succeeded");

        let (msg_meta, payload, attachments_data) = timeout(Duration::from_millis(RPC_TIMEOUT_MS_AMOUNT), rpc_rx).await??;

        let payload: T = from_slice(&payload)?;
        
        Ok((msg_meta, payload, attachments_data))
    }
}

#[derive(Debug)]
pub enum ProcessError {
    FramePayloadSizeExceeded,
    ReadLoopCompleted,
    IncorrectFrameType,
    IncorrectMsgType,
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
    SendFrameError,
    SendServerMsgError,
    SendClientMsgError,
    SendRpcMsgError,
    OneshotRecvError(oneshot::error::RecvError),
    Timeout,
    NoneError,
    TrySendServerMsg,
    TrySendClientMsg,
    TrySendFrame,
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

impl From<SendError<Frame>> for ProcessError {
	fn from(_: SendError<Frame>) -> ProcessError {
		ProcessError::SendFrameError
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

impl From<TrySendError<Frame>> for ProcessError {
	fn from(_: TrySendError<Frame>) -> ProcessError {
		ProcessError::TrySendFrame
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
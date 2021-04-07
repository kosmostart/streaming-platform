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

pub const LEN_BUF_SIZE: usize = 4;

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
    SipHasher24::new_with_keys(0, 0)
}

pub fn get_stream_id_hasher() -> SipHasher24 {    
    SipHasher24::new_with_keys(0, 0)
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
    pub payload: Option<[u8; MAX_FRAME_PAYLOAD_SIZE]>
}

pub enum FrameType {
    MsgMeta = 0,
	MsgMetaEnd = 1,
	Payload = 2,
    PayloadEnd = 3,
    Attachment = 4,
	AttachmentEnd = 5,
    End = 6
}

impl Frame {
    pub fn new(frame_type: u8, payload_size: u16, msg_type: u8, key_hash: u64, stream_id: u64, payload: Option<[u8; MAX_FRAME_PAYLOAD_SIZE]>) -> Frame {
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
			1 => FrameType::MsgMetaEnd,
            2 => FrameType::Payload,
			3 => FrameType::PayloadEnd,
            4 => FrameType::Attachment,
            5 => FrameType::AttachmentEnd,
			6 => FrameType::End,
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
pub struct State {
	frame_reading_status: FrameReadingStatus,
    bytes_read: usize,
	bytes_processed: usize,
	read_buf: [u8; MAX_FRAME_SIZE],
    frame_buf: [u8; MAX_FRAME_SIZE],
	payload_size_u16: u16,
	payload_size: usize,
	key_hash: u64,
	stream_id: u64,
	frame_signature: u64,
	frame_size: usize
}

pub enum FrameReadingStatus {
	Header,
	Payload
}
pub enum ReadFrameResult {
	NotEnoughBytesForFrame,
	NextStep,
    Frame(Frame)
}

impl State {
    pub fn new() -> State {
        State {
			frame_reading_status: FrameReadingStatus::Header,
            bytes_read: 0,
			bytes_processed: 0,
            frame_buf: [0; MAX_FRAME_SIZE],
            read_buf: [0; MAX_FRAME_SIZE],
			payload_size_u16: 0,
			payload_size: 0,
			key_hash: 0,
			stream_id: 0,
			frame_signature: 0,
			frame_size: 0
        }
    }
    pub fn clear(&mut self) {
		self.frame_reading_status = FrameReadingStatus::Header;
        self.bytes_read = 0;
        self.bytes_processed = 0;
    }
    pub async fn read_from_tcp_stream(&mut self, tcp_stream: &mut TcpStream) -> Result<(), ProcessError> {
        let bytes_read = tcp_stream.read(&mut self.read_buf[self.bytes_read..]).await?;

		debug!("Read {} bytes, self.bytes_read was {}", bytes_read, self.bytes_read);

        match bytes_read {
            0 => {
                error!("Read 0 bytes");
                Err(ProcessError::StreamClosed)
            }
            _ => {
                let mut i = 0;

				while i < bytes_read {
                    self.frame_buf[self.bytes_read + i] = self.read_buf[self.bytes_read + i];

                    i = i + 1;
                }

                self.bytes_read = self.bytes_read + bytes_read;

                debug!("Bytes read set to {}", self.bytes_read);

				Ok(())
            }
        }
    }
    pub fn read_frame(&mut self) -> ReadFrameResult {
		match self.frame_reading_status {
			FrameReadingStatus::Header => {
                debug!("FrameReadingStatus::Header");
				if self.bytes_read < self.bytes_processed + FRAME_HEADER_SIZE {
                    debug!("ReadFrameResult::NotEnoughBytesForFrame");
					return ReadFrameResult::NotEnoughBytesForFrame;
				}            
		
				self.payload_size_u16 = byteorder::BigEndian::read_u16(&self.frame_buf[self.bytes_processed + 1..self.bytes_processed + 3]);
				self.payload_size = self.payload_size_u16 as usize;
				self.key_hash = byteorder::BigEndian::read_u64(&self.frame_buf[self.bytes_processed + 4..self.bytes_processed + 12]);
				self.stream_id = byteorder::BigEndian::read_u64(&self.frame_buf[self.bytes_processed + 12..self.bytes_processed + 20]);
				self.frame_signature = byteorder::BigEndian::read_u64(&self.frame_buf[self.bytes_processed + 20..self.bytes_processed + 28]);
		
				debug!("Got payload size {}", self.payload_size_u16);
		
				self.frame_size = FRAME_HEADER_SIZE + self.payload_size;

				self.frame_reading_status = FrameReadingStatus::Payload;

				ReadFrameResult::NextStep
			}
			FrameReadingStatus::Payload => {
                debug!("FrameReadingStatus::Payload");
                debug!("Bytes read {}, bytes processed {}, frame size {}", self.bytes_read, self.bytes_processed, self.frame_size);
				if self.bytes_read < self.bytes_processed + self.frame_size {
                    debug!("ReadFrameResult::NotEnoughBytesForFrame");

                    // MAX_FRAME_SIZE const is used as buffer length here, so we compare against buffer size, not frame size actually
                    match self.bytes_processed + self.frame_size > MAX_FRAME_SIZE {
                        true => {
                            debug!("No full frame left for processing, but some bytes left");

                            let bytes_left = self.bytes_read - self.bytes_processed;

                            let mut i = 0;
            
                            while i < bytes_left {
                                self.frame_buf[i] = self.read_buf[self.bytes_processed + i];
                                i = i + 1;
                            }

                            self.bytes_read = bytes_left;
                            self.bytes_processed = 0;

                            debug!("Buffer process complete, bytes left: {}, bytes read set to {}", bytes_left, bytes_left);
                        }
                        false => {}
                    }
					return ReadFrameResult::NotEnoughBytesForFrame;
				}

                let mut i = 0;

                let res = match self.payload_size_u16 {
                    0 => Frame {
                        frame_type: self.frame_buf[self.bytes_processed],
                        payload_size: self.payload_size_u16,
                        msg_type: self.frame_buf[self.bytes_processed + 3],
                        key_hash: self.key_hash,
                        stream_id: self.stream_id,
                        frame_signature: self.frame_signature,
                        payload: None
                    },
                    _ => {
                        let payload_slice = &self.frame_buf[self.bytes_processed + FRAME_HEADER_SIZE..self.bytes_processed + self.frame_size];
                        let mut payload = [0; MAX_FRAME_PAYLOAD_SIZE];        
                
                        while i < self.payload_size {
                            payload[i] = payload_slice[i];
                            i = i + 1;
                        }				
                
                        Frame {
                            frame_type: self.frame_buf[self.bytes_processed],
                            payload_size: self.payload_size_u16,
                            msg_type: self.frame_buf[self.bytes_processed + 3],
                            key_hash: self.key_hash,
                            stream_id: self.stream_id,
                            frame_signature: self.frame_signature,
                            payload: Some(payload)
                        }
                    }
                };
	

				self.bytes_processed = self.bytes_processed + self.frame_size;
				self.frame_reading_status = FrameReadingStatus::Header;

				match self.bytes_processed < self.bytes_read {
					true => {
						match self.bytes_processed + FRAME_HEADER_SIZE <= self.bytes_read {
							true => {
								debug!("At least one frame header left for processing, bytes processed {}", self.bytes_processed);
							}
							false => {
								debug!("No frame headers left for processing, but some bytes left");

								let bytes_left = self.bytes_read - self.bytes_processed;

								i = 0;
				
								while i < bytes_left {
									self.frame_buf[i] = self.read_buf[self.bytes_processed + i];
									i = i + 1;
								}

								self.bytes_read = bytes_left;
								self.bytes_processed = 0;

                                debug!("Buffer process complete, bytes left: {}, bytes read set to {}", bytes_left, bytes_left);
							}
						}												
					}
					false => {
						debug!("No frame headers left for processing, no bytes left");
						self.bytes_read = 0;
						self.bytes_processed = 0;
					}
				}				
				
				ReadFrameResult::Frame(res)
			}
		}
    }
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
pub type ProcessStream<T, D> = fn(HashMap<String, String>, MagicBall, UnboundedReceiver<ClientMsg>, Option<UnboundedSender<RestreamMsg>>, Option<UnboundedReceiver<RestreamMsg>>, D) -> T;
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
    #[cfg(feature = "http")]
    AddRestream(Uuid, hyper::body::Sender, Option<oneshot::Sender<StreamCompletion>>),
    #[cfg(feature = "http")]
    GetRestreams(oneshot::Sender<Vec<(Uuid, hyper::body::Sender, Option<oneshot::Sender<StreamCompletion>>)>>)
}

pub enum StreamCompletion {
    Ok,
    Err
}

pub fn get_key_hash(key: Key) -> u64 {	
	let mut hasher = get_key_hasher();

    hasher.write(&(key.service + &key.action + &key.domain).as_bytes());

    let res = hasher.finish();

    debug!("Created hash for key, hash {}", res);

    res
}

pub async fn write_frame(tcp_stream: &mut TcpStream, frame: Frame) -> Result<(), ProcessError> {
    debug!("Frame write to socket attempt, stream_id {}, frame type {}, payload size {}", frame.stream_id, frame.frame_type, frame.payload_size);

    let mut header = [0; FRAME_HEADER_SIZE];

    header[0] = frame.frame_type;
    byteorder::BigEndian::write_u16(&mut header[1..3], frame.payload_size);
    header[3] = frame.msg_type;
    byteorder::BigEndian::write_u64(&mut header[4..12], frame.key_hash);
    byteorder::BigEndian::write_u64(&mut header[12..20], frame.stream_id);
    byteorder::BigEndian::write_u64(&mut header[20..28], frame.frame_signature);

    tcp_stream.write_all(&header[..]).await?;

    match frame.payload {
        Some(payload) => {
            tcp_stream.write_all(&payload[..frame.payload_size as usize]).await?;
        }
        None => {}
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
pub async fn write_to_tcp_stream(tcp_stream: &mut TcpStream, msg_type: u8, key_hash: u64, stream_id: u64, data: Vec<u8>, msg_meta_size: u64, payload_size: u64, attachments_sizes: Vec<u64>, send_end_frame: bool) -> Result<(), ProcessError> {
    let msg_meta_offset = LEN_BUF_SIZE + msg_meta_size as usize;
    let payload_offset = msg_meta_offset + payload_size as usize;
    let mut data_buf = [0; MAX_FRAME_PAYLOAD_SIZE];

    let mut source = &data[LEN_BUF_SIZE..msg_meta_offset];

	let mut bytes_send = 0;

    loop {        
        let n = source.read(&mut data_buf).await?;
        match n {
            0 => break,
            _ => {
				bytes_send = bytes_send + n as u64;

				let frame_type = match bytes_send == msg_meta_size {
					true => FrameType::MsgMetaEnd,
					false => FrameType::MsgMeta
				};

				write_frame(tcp_stream, Frame::new(FrameType::MsgMeta as u8, n as u16, msg_type, key_hash, stream_id, Some(data_buf))).await?;
			}
        }
    }

	bytes_send = 0;

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
                    _ => {
						bytes_send = bytes_send + n as u64;

						let frame_type = match bytes_send == payload_size {
							true => FrameType::PayloadEnd,
							false => FrameType::Payload
						};

						write_frame(tcp_stream, Frame::new(FrameType::Payload as u8, n as u16, msg_type, key_hash, stream_id, Some(data_buf))).await?;
					}
                }
            }
        }
    }

    let mut prev = payload_offset as usize;

    for attachment_size in attachments_sizes {
        let attachment_offset = prev + attachment_size as usize;

		bytes_send = 0;

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
                        _ => {
							bytes_send = bytes_send + n as u64;

							let frame_type = match bytes_send == attachment_size {
								true => FrameType::AttachmentEnd,
								false => FrameType::Attachment
							};

							write_frame(tcp_stream, Frame::new(FrameType::Attachment as u8, n as u16, msg_type, key_hash, stream_id, Some(data_buf))).await?;
						}
                    }
                }                    
            }
        }            

        prev = attachment_offset;
    }

    if send_end_frame {
		write_frame(tcp_stream, Frame::new(FrameType::End as u8, 0, msg_type, key_hash, stream_id, None)).await?;
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
	frame_type: u8,
	msg_type: u8,
	key_hash: u64,
	stream_id: u64,
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
            hash_buf,
            addr_bytes_len,
			frame_type: 0,
			msg_type: 0,
			key_hash: 0,
			stream_id: 0,
            write_tx,
            rpc_inbound_tx
        }
    }
    /// This function generates new stream id
    pub fn get_stream_id(&mut self) -> u64 {
        self.hash_buf.truncate(self.addr_bytes_len);
        //self.hash_buf.put_u32(get_counter_value());
        self.hash_buf.extend_from_slice(Uuid::new_v4().to_string().as_bytes());
		let mut hasher = get_stream_id_hasher();
        hasher.write(&self.hash_buf);
        hasher.finish()
    }

    /// This function should be called for single message write.
    /// stream_id value MUST BE ACQUIRED with get_stream_id() function. stream_id generation can be implicit, however this will leads to less flexible API (if for example you need stream payload or attachments data).
    /// If you plan to write attachments data later (for example in streaming fashion), leave attachments_sizes parameter empty and only fill attachment sizes in msg_meta
    pub async fn write_full_message(&mut self, msg_type: u8, key_hash: u64, stream_id: u64, data: Vec<u8>, msg_meta_size: u64, payload_size: u64, attachments_sizes: Vec<u64>, send_end_frame: bool) -> Result<(), ProcessError> {
        let msg_meta_offset = LEN_BUF_SIZE + msg_meta_size as usize;
        let payload_offset = msg_meta_offset + payload_size as usize;
        let mut data_buf = [0; MAX_FRAME_PAYLOAD_SIZE];

        let mut source = &data[LEN_BUF_SIZE..msg_meta_offset];

		let mut bytes_send = 0;

        loop {        
            let n = source.read(&mut data_buf).await?;
            match n {
                0 => break,
                _ => {
					bytes_send = bytes_send + n as u64;

					let frame_type = match bytes_send == msg_meta_size {
						true => FrameType::MsgMetaEnd,
						false => FrameType::MsgMeta
					};

					self.write_tx.send(Frame::new(frame_type as u8, n as u16, msg_type, key_hash, stream_id, Some(data_buf)))?;					
				}
            }
        }

		bytes_send = 0;

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
                        _ => {
							bytes_send = bytes_send + n as u64;

							let frame_type = match bytes_send == payload_size {
								true => FrameType::PayloadEnd,
								false => FrameType::Payload
							};

							self.write_tx.send(Frame::new(frame_type as u8, n as u16, msg_type, key_hash, stream_id, Some(data_buf)))?;
						}
                    }
                }
            }
        }

        let mut prev = payload_offset as usize;

        for attachment_size in attachments_sizes {
            let attachment_offset = prev + attachment_size as usize;

			bytes_send = 0;

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
                            _ => {
								bytes_send = bytes_send + n as u64;

								let frame_type = match bytes_send == attachment_size {
									true => FrameType::AttachmentEnd,
									false => FrameType::Attachment
								};
								
								self.write_tx.send(Frame::new(frame_type as u8, n as u16, msg_type, key_hash, stream_id, Some(data_buf)))?
							}
                        }
                    }                    
                }
            }            

            prev = attachment_offset;
        }

		if send_end_frame {
			self.write_tx.send(Frame::new(FrameType::End as u8, 0, msg_type, key_hash, stream_id, None))?;
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

        self.key_hash = get_key_hash(key);
        self.stream_id = self.get_stream_id();

        self.write_full_message(MsgType::Event.get_u8(), self.key_hash, self.stream_id, dto, msg_meta_size, payload_size, attachments_sizes, true).await?;
        
        Ok(())
    }
    pub async fn send_event_with_route<T>(&mut self, key: Key, payload: T, mut route: Route) -> Result<(), ProcessError> where T: serde::Serialize, for<'de> T: serde::Deserialize<'de>, T: Debug {
        //info!("send_event, route {:?}, key {}, payload {:?}, ", route, addr, key, payload);

        route.points.push(Participator::Service(self.addr.clone()));

        let (dto, msg_meta_size, payload_size, attachments_sizes) = event_dto_with_sizes(self.addr.clone(), key.clone(), payload, route, self.auth_token.clone(), self.auth_data.clone())?;

        self.key_hash = get_key_hash(key);
        self.stream_id = self.get_stream_id();

        self.write_full_message(MsgType::Event.get_u8(), self.key_hash, self.stream_id, dto, msg_meta_size, payload_size, attachments_sizes, true).await?;
        
        Ok(())
    }
	pub async fn start_event_stream<T>(&mut self, key: Key, payload: T) -> Result<(), ProcessError> where T: serde::Serialize, for<'de> T: serde::Deserialize<'de>, T: Debug {
        let route = Route {
            source: Participator::Service(self.addr.clone()),
            spec: RouteSpec::Simple,
            points: vec![Participator::Service(self.addr.to_owned())]
        };

        let (dto, msg_meta_size, payload_size, attachments_sizes) = event_dto_with_sizes(self.addr.clone(), key.clone(), payload, route, self.auth_token.clone(), self.auth_data.clone())?;

		self.frame_type = FrameType::Attachment as u8;
		self.msg_type = MsgType::Event.get_u8();
        self.key_hash = get_key_hash(key);
        self.stream_id = self.get_stream_id();

        self.write_full_message(self.msg_type, self.key_hash, self.stream_id, dto, msg_meta_size, payload_size, attachments_sizes, false).await?;
        
        Ok(())
    }
    pub async fn start_rpc_stream<T>(&mut self, key: Key, payload: T) -> Result<Uuid, ProcessError> where T: serde::Serialize, for<'de> T: serde::Deserialize<'de>, T: Debug {
        let route = Route {
            source: Participator::Service(self.addr.clone()),
            spec: RouteSpec::Simple,
            points: vec![Participator::Service(self.addr.clone())]
        };

        let (correlation_id, dto, msg_meta_size, payload_size, attachments_sizes) = rpc_dto_with_correlation_id_sizes(self.addr.clone(), key.clone(), payload, route, self.auth_token.clone(), self.auth_data.clone())?;

		self.frame_type = FrameType::Attachment as u8;
		self.msg_type = MsgType::RpcRequest.get_u8();
        self.key_hash = get_key_hash(key);
        self.stream_id = self.get_stream_id();

        self.write_full_message(self.msg_type, self.key_hash, self.stream_id, dto, msg_meta_size, payload_size, attachments_sizes, false).await?;
        
        Ok(correlation_id)
    }
    pub async fn start_rpc_stream_response<T>(&mut self, mut msg_meta: MsgMeta, payload: T) -> Result<(), ProcessError> where T: serde::Serialize, for<'de> T: serde::Deserialize<'de>, T: Debug {
        msg_meta.route.points.push(Participator::Service(self.addr.clone()));

        let rpc_result = RpcResult::Ok;

        let (dto, msg_meta_size, payload_size, attachments_sizes) = rpc_response_dto_sizes(self.addr.clone(), msg_meta.key.clone(), msg_meta.correlation_id, payload, vec![], vec![], rpc_result.clone(), msg_meta.route, self.auth_token.clone(), self.auth_data.clone())?;

		self.frame_type = FrameType::Attachment as u8;
		self.msg_type = MsgType::RpcResponse(rpc_result).get_u8();
        self.key_hash = get_key_hash(msg_meta.key);
        self.stream_id = self.get_stream_id();

        self.write_full_message(self.msg_type, self.key_hash, self.stream_id, dto, msg_meta_size, payload_size, attachments_sizes, false).await?;
        
        Ok(())
    }
    pub async fn start_rpc_stream_response_custom_res<T>(&mut self, mut msg_meta: MsgMeta, payload: T, rpc_result: RpcResult) -> Result<(), ProcessError> where T: serde::Serialize, for<'de> T: serde::Deserialize<'de>, T: Debug {
        msg_meta.route.points.push(Participator::Service(self.addr.clone()));

        let (dto, msg_meta_size, payload_size, attachments_sizes) = rpc_response_dto_sizes(self.addr.clone(), msg_meta.key.clone(), msg_meta.correlation_id, payload, vec![], vec![], rpc_result.clone(), msg_meta.route, self.auth_token.clone(), self.auth_data.clone())?;

		self.frame_type = FrameType::Attachment as u8;
		self.msg_type = MsgType::RpcResponse(rpc_result).get_u8();
        self.key_hash = get_key_hash(msg_meta.key);
        self.stream_id = self.get_stream_id();

        self.write_full_message(self.msg_type, self.key_hash, self.stream_id, dto, msg_meta_size, payload_size, attachments_sizes, false).await?;
        
        Ok(())
    }
    pub async fn send_rpc_response<T>(&mut self, mut msg_meta: MsgMeta, payload: T) -> Result<(), ProcessError> where T: serde::Serialize, for<'de> T: serde::Deserialize<'de>, T: Debug {
        msg_meta.route.points.push(Participator::Service(self.addr.clone()));

        let rpc_result = RpcResult::Ok;

        let (dto, msg_meta_size, payload_size, attachments_sizes) = rpc_response_dto_sizes(self.addr.clone(), msg_meta.key.clone(), msg_meta.correlation_id, payload, vec![], vec![], rpc_result.clone(), msg_meta.route, self.auth_token.clone(), self.auth_data.clone())?;

		self.frame_type = FrameType::Attachment as u8;
		self.msg_type = MsgType::RpcResponse(rpc_result).get_u8();
        self.key_hash = get_key_hash(msg_meta.key);
        self.stream_id = self.get_stream_id();

        self.write_full_message(self.msg_type, self.key_hash, self.stream_id, dto, msg_meta_size, payload_size, attachments_sizes, true).await?;
        
        Ok(())
    }
    pub async fn send_rpc_response_with_attachments<T>(&mut self, mut msg_meta: MsgMeta, payload: T, attachments: Vec<(String, u64)>, attachments_data: Vec<u8>) -> Result<(), ProcessError> where T: serde::Serialize, for<'de> T: serde::Deserialize<'de>, T: Debug {
        msg_meta.route.points.push(Participator::Service(self.addr.clone()));

        let rpc_result = RpcResult::Ok;

        let (dto, msg_meta_size, payload_size, attachments_sizes) = rpc_response_dto_sizes(self.addr.clone(), msg_meta.key.clone(), msg_meta.correlation_id, payload, attachments, attachments_data, rpc_result.clone(), msg_meta.route, self.auth_token.clone(), self.auth_data.clone())?;

		self.frame_type = FrameType::Attachment as u8;
		self.msg_type = MsgType::RpcResponse(rpc_result).get_u8();
        self.key_hash = get_key_hash(msg_meta.key);
        self.stream_id = self.get_stream_id();

        self.write_full_message(self.msg_type, self.key_hash, self.stream_id, dto, msg_meta_size, payload_size, attachments_sizes, true).await?;
        
        Ok(())
    }
	pub fn send_frame(&mut self, payload: &[u8], payload_size: usize) -> Result<(), ProcessError> {
        if payload_size == 0 {
            return Err(ProcessError::ZeroSizedPayloadNotAllowed);
        }	

		let mut buf = [0; MAX_FRAME_PAYLOAD_SIZE];

		let mut i = 0;

		while i < payload_size {
			buf[i] = payload[i];
			i = i + 1;
		}

        self.write_tx.send(Frame::new(self.frame_type, payload_size as u16, self.msg_type, self.key_hash, self.stream_id, Some(buf)))?;

		Ok(())
	}
    pub fn complete_msg_meta(&mut self) -> Result<(), ProcessError> {
        self.write_tx.send(Frame::new(FrameType::MsgMetaEnd as u8, 0, self.msg_type, self.key_hash, self.stream_id, None))?;
		Ok(())
	}
    pub fn complete_payload(&mut self) -> Result<(), ProcessError> {
        self.write_tx.send(Frame::new(FrameType::PayloadEnd as u8, 0, self.msg_type, self.key_hash, self.stream_id, None))?;
		Ok(())
	}
    pub fn complete_attachment(&mut self) -> Result<(), ProcessError> {
        self.write_tx.send(Frame::new(FrameType::AttachmentEnd as u8, 0, self.msg_type, self.key_hash, self.stream_id, None))?;
		Ok(())
	}
    /// This function will be waiting for rpc response, please note (in async function).
    pub async fn complete_rpc_stream<T>(&mut self, correlation_id: Uuid) -> Result<Message<T>, ProcessError> where for<'de> T: serde::Deserialize<'de>, T: Debug {
        let (rpc_tx, rpc_rx) = oneshot::channel();
        
        self.rpc_inbound_tx.send(RpcMsg::AddRpc(correlation_id, rpc_tx))?;

        self.write_tx.send(Frame::new(FrameType::End as u8, 0, self.msg_type, self.key_hash, self.stream_id, None))?;

        let (msg_meta, payload, attachments_data) = timeout(Duration::from_millis(RPC_TIMEOUT_MS_AMOUNT), rpc_rx).await??;
        let payload: T = from_slice(&payload)?;

        Ok(Message {
            meta: msg_meta, 
            payload, 
            attachments_data
        })
	}
	pub fn complete_stream(&mut self) -> Result<(), ProcessError> {
        self.write_tx.send(Frame::new(FrameType::End as u8, 0, self.msg_type, self.key_hash, self.stream_id, None))?;
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

        self.key_hash = get_key_hash(key);
        self.stream_id = self.get_stream_id();

        self.write_full_message(MsgType::RpcRequest.get_u8(), self.key_hash, self.stream_id, dto, msg_meta_size, payload_size, attachments_sizes, true).await?;       

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

        self.key_hash = get_key_hash(key);
        self.stream_id = self.get_stream_id();

        self.write_full_message(MsgType::RpcRequest.get_u8(), self.key_hash, self.stream_id, dto, msg_meta_size, payload_size, attachments_sizes, true).await?;

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

        self.key_hash = get_key_hash(msg_meta.key);
        self.stream_id = self.get_stream_id();

        self.write_full_message(msg_meta.msg_type.get_u8(), self.key_hash, self.stream_id, buf, msg_meta_size, payload_size, attachments_sizes, true).await?;
        
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

        self.key_hash = get_key_hash(msg_meta.key);
        self.stream_id = self.get_stream_id();

        self.write_full_message(msg_meta.msg_type.get_u8(), self.key_hash, self.stream_id, buf, msg_meta_size, payload_size, attachments_sizes, true).await?;
        
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

        self.key_hash = get_key_hash(msg_meta.key);
        self.stream_id = self.get_stream_id();

        self.write_full_message(msg_meta.msg_type.get_u8(), self.key_hash, self.stream_id, buf, msg_meta_size, payload_size, attachments_sizes, true).await?;

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

        self.key_hash = get_key_hash(msg_meta.key);
        self.stream_id = self.get_stream_id();

        self.write_full_message(msg_meta.msg_type.get_u8(), self.key_hash, self.stream_id, buf, msg_meta_size, payload_size, attachments_sizes, true).await?;

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

        self.key_hash = get_key_hash(msg_meta.key);
        self.stream_id = self.get_stream_id();

        self.write_full_message(msg_meta.msg_type.get_u8(), self.key_hash, self.stream_id, buf, msg_meta_size, payload_size, attachments_sizes, true).await?;

        debug!("proxy_rpc_with_payload write attempt succeeded");

        let (msg_meta, payload, attachments_data) = timeout(Duration::from_millis(RPC_TIMEOUT_MS_AMOUNT), rpc_rx).await??;

        let payload: T = from_slice(&payload)?;
        
        Ok((msg_meta, payload, attachments_data))
    }
}

#[derive(Debug)]
pub enum ProcessError {	
	Io(std::io::Error),
    SerdeJson(serde_json::Error),
    FramePayloadSizeExceeded,
    ZeroSizedPayloadNotAllowed,
    ReadLoopCompleted,
    IncorrectFrameType,
    IncorrectMsgType,
    StreamLayoutNotFound,
    StreamClosed,    
    WriteChannelDropped,        
    SendFrameError,
    SendServerMsgError,
    SendRpcMsgError,
    OneshotRecvError(oneshot::error::RecvError),
    Timeout,
    Custom(String)
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
use std::option;
use std::io::Cursor;
use std::net::SocketAddr;
use bytes::Buf;
use tokio::io::Take;
use tokio::net::tcp::split::ReadHalf;
use tokio::sync::mpsc::{Sender, error::SendError};
use tokio::prelude::*;
use serde_json::from_slice;
use serde_derive::Deserialize;
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
    LenFinished,
    /// Message data stream is prepended with MsgMeta struct
    MsgMeta(MsgMeta),
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
    pub len_buf: [u8; LEN_BUF_SIZE],    
    pub acc: Vec<u8>,
    pub attachments: Option<Vec<u64>>,
    pub step: Step
}

impl State {
    pub fn new() -> State {
        State {            
            len_buf: [0; LEN_BUF_SIZE],
            acc: vec![],
            attachments: None,
            step: Step::Len
        }  
    }    
}

pub async fn read(state: &mut State, adapter: &mut Take<ReadHalf<'_>>) -> Result<ReadResult, ProcessError> {
    match state.step {
        Step::Len => {                
            adapter.read_exact(&mut state.len_buf).await?;                

            let mut buf = Cursor::new(&state.len_buf);
            let len = buf.get_u32_be();

            state.step = Step::MsgMeta(len);

            Ok(ReadResult::LenFinished)
        }
        Step::MsgMeta(len) => {
            adapter.set_limit(len as u64);
            state.acc.clear();
            let n = adapter.read_to_end(&mut state.acc).await?;            

            let msg_meta: MsgMeta = from_slice(&state.acc)?;
            adapter.set_limit(msg_meta.payload_size as u64);

            state.attachments = Some(msg_meta.attachments.iter().map(|x| x.size).collect());

            state.step = Step::Payload;

            Ok(ReadResult::MsgMeta(msg_meta))
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
pub struct Config {
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
    SendBuf(String, (usize, [u8; DATA_BUF_SIZE])),
    RemoveClient(String)
}

pub enum Mode {
    Stream(fn(ClientMsg)),
    FullMessage
}

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

// Used for RPC implementation
pub enum RpcMsg {
    AddRpc(Uuid, Sender<(MsgMeta, Vec<u8>, Vec<u8>)>),
    RemoveRpc(Uuid),
    RpcDataRequest(Uuid),
    RpcDataResponse(Uuid, Sender<(MsgMeta, Vec<u8>, Vec<u8>)>)
}

pub struct MagicBall {    
    write_tx: Sender<(usize, [u8; DATA_BUF_SIZE])>,
    rpc_tx:: Sender<(usize, [u8; DATA_BUF_SIZE])>,
}

impl MagicBall {
    pub async fn send_event(&mut self, dto: Vec<u8>) -> Result<(), ProcessError> {
        write(dto, &mut self.write_tx).await
    }
    pub async fn send_rpc(&mut self, dto: Vec<u8>) -> Result<(), ProcessError> {
        write(dto, &mut self.write_tx).await?;

        Ok(())
    }
}

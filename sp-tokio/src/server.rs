use std::fmt;
use std::error::Error;
use std::collections::HashMap;
use std::io::prelude::*;
use std::io::{Cursor, BufReader};
use std::fs::{self, DirEntry};
use std::path::Path;
use std::net::SocketAddr;
use bytes::Buf;
use futures::future::{Fuse, FusedFuture, FutureExt};
use futures::stream::StreamExt;
use futures::{select, pin_mut};
use tokio::runtime::Runtime;
use tokio::io::Take;
use tokio::net::{TcpListener, TcpStream, tcp::split::{ReadHalf, WriteHalf}};
use tokio::sync::mpsc::{self, Sender, Receiver};
use tokio::prelude::*;
use tokio::fs::File;
use serde_json::{Value, from_slice};
use serde_derive::Deserialize;
use sp_dto::*;

#[derive(Debug, Deserialize, Clone)]
struct Config {
    host: String,
    dirs: Option<Vec<Dir>>
}

#[derive(Debug, Deserialize, Clone)]
struct Dir {
    access_key: String,
    path: String
}

const LEN_BUF_SIZE: usize = 4;
const DATA_BUF_SIZE: usize = 1024;
const MPSC_SERVER_BUF_SIZE: usize = 1000;
const MPSC_CLIENT_BUF_SIZE: usize = 100;

struct Client {
    net_addr: SocketAddr,
    tx: Sender<[u8; DATA_BUF_SIZE]>
}

enum ServerMsg {
    AddClient(String, SocketAddr, Sender<[u8; DATA_BUF_SIZE]>),
    SendBuf(String, [u8; DATA_BUF_SIZE]),
    RemoveClient(String)
}

enum BufRoute {
    Broker([u8; DATA_BUF_SIZE]),
    Socket([u8; DATA_BUF_SIZE])
}

#[derive(Debug)]
enum ProcessError {
    StreamClosed,
    NotEnoughBytesForLen,
    IncorrectReadResult,
    AttachmentFieldIsEmpty,
    Io(std::io::Error),
    SerdeJson(serde_json::Error),
    GetFile(GetFileError)
}

#[derive(Debug)]
enum GetFileError {
    ConfigDirsIsEmpty,
    NoAccessKeyInPayload,
    TargetDirNotFound,
    NoFilesInTargetDir
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

/// Read full message from source in to memory. Should be used carefully with large message content.
async fn read_full(socket_read: &mut ReadHalf<'_>) -> Result<(MsgMeta, Vec<u8>, Vec<u8>), ProcessError> {
    let mut len_buf = [0; LEN_BUF_SIZE];
    socket_read.read_exact(&mut len_buf).await?;

    let mut buf = Cursor::new(len_buf);        
    let len = buf.get_u32_be() as usize;
    let mut adapter = socket_read.take(len as u64);

    let mut msg_meta = vec![];

    let n = adapter.read_to_end(&mut msg_meta).await?;        

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
enum ReadResult {
    /// This one indicates MsgMeta struct len was read successfully
    LenFinished,
    /// Message data stream is prepended with MsgMeta struct
    MsgMeta(MsgMeta),
    /// Payload data stream message
    PayloadData([u8; DATA_BUF_SIZE]),
    /// This one indicates payload data stream finished
    PayloadFinished,
    /// Attachment whith index data stream message
    AttachmentData(usize, [u8; DATA_BUF_SIZE]),
    /// This one indicates attachment data stream by index finished
    AttachmentFinished(usize)    
}

#[derive(Debug)]
enum Step {
    Len,
    MsgMeta(u32),
    Payload,
    Attachment(usize)
}

/// Data structure used for convenience when streaming data from source
struct State {
    pub len_buf: [u8; LEN_BUF_SIZE],    
    pub acc: Vec<u8>,
    pub attachments: Option<Vec<u32>>,
    pub step: Step
}

impl State {
    fn new() -> State {
        State {            
            len_buf: [0; LEN_BUF_SIZE],            
            acc: vec![],
            attachments: None,
            step: Step::Len
        }  
    }    
}

async fn read(state: &mut State, adapter: &mut Take<ReadHalf<'_>>) -> Result<ReadResult, ProcessError> {
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
            let n = adapter.read(&mut data_buf).await?;

            match n {
                0 => {
                    let attachments = state.attachments.as_ref().ok_or(ProcessError::AttachmentFieldIsEmpty)?;

                    match attachments.len() {
                        0 => {
                            adapter.set_limit(LEN_BUF_SIZE as u64);
                            state.step = Step::Len;
                        }
                        _ => {                      
                            adapter.set_limit(attachments[0] as u64);
                            state.step = Step::Attachment(0);
                        }                            
                    };

                    Ok(ReadResult::PayloadFinished)
                }
                _ => Ok(ReadResult::PayloadData(data_buf))
            }                                
        }
        Step::Attachment(index) => {
            let mut data_buf = [0; DATA_BUF_SIZE];           
            let n = adapter.read(&mut data_buf).await?;

            match n {
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
                            state.step = Step::Len;
                        }
                    };

                    Ok(ReadResult::AttachmentFinished(index))
                }
                _ => Ok(ReadResult::AttachmentData(index, data_buf))
            }
        }
    }
}

pub fn start() {
    let rt = Runtime::new().expect("failed to create runtime"); 
    
    rt.block_on(start_future());
}

async fn start_future() -> Result<(), Box<dyn Error>> {
    let config_path = std::env::args().nth(1)
        .expect("path to config file not passed as argument");

    let file = std::fs::File::open(config_path)
        .expect("failed to open config");

    let mut buf_reader = BufReader::new(file);

    let mut config_string = String::new();

    buf_reader.read_to_string(&mut config_string)
        .expect("failed to read config");

    let config: Config = toml::from_str(&config_string)
        .expect("failed to deserialize config");	

    let mut listener = TcpListener::bind(config.host.clone()).await?;

    let (mut server_tx, mut server_rx) = mpsc::channel(MPSC_SERVER_BUF_SIZE);

    tokio::spawn(async move {        
        let mut clients = HashMap::new();

        loop {
            let msg = server_rx.recv().await.expect("clients msg receive failed");

            match msg {
                ServerMsg::AddClient(addr, net_addr, tx) => {
                    let client = Client { 
                        net_addr,
                        tx 
                    };

                    clients.insert(addr, client);
                }
                ServerMsg::SendBuf(addr, buf) => {
                    let client = clients.get(&addr);
                }
                ServerMsg::RemoveClient(addr) => {
                    clients.remove(&addr);
                }
            }     
        }
    });    

    println!("ok");

    loop {        
        let (mut stream, client_net_addr) = listener.accept().await?;
        let config = config.clone();
        let server_tx = server_tx.clone();

        println!("connected");  

        tokio::spawn(async move {
            let res = process(stream, client_net_addr, server_tx, &config).await;

            println!("{:?}", res);
        });        
    }
}

async fn process(mut stream: TcpStream, client_net_addr: SocketAddr, mut server_tx: Sender<ServerMsg>, config: &Config) -> Result<(), ProcessError> {
    let (mut socket_read, mut socket_write) = stream.split();

    let (msg_meta, payload, attachments) = read_full(&mut socket_read).await?;
    let payload: Value = from_slice(&payload)?;
    
    process_msg(&msg_meta, payload, &mut socket_write, config).await?;

    //println!("auth {:?}", msg_meta);
    //println!("auth {:?}", payload);    
    
    let (mut client_tx, mut client_rx) = mpsc::channel(MPSC_CLIENT_BUF_SIZE);

    server_tx.send(ServerMsg::AddClient(msg_meta.tx.clone(), client_net_addr, client_tx)).await.unwrap();    

    let mut adapter = socket_read.take(LEN_BUF_SIZE as u64);
    let mut state = State::new();

    let mut msg_meta = None;

    loop {
        let f1 = read(&mut state, &mut adapter).fuse();
        let f2 = client_rx.recv().fuse();

        pin_mut!(f1, f2);

        let res = select! {
            res = f1 => {                
                let res = res?;                

                match res {
                    ReadResult::LenFinished => {
                        println!("len ok");
                    }
                    ReadResult::MsgMeta(new_msg_meta) => {
                        println!("{:?}", new_msg_meta);
                        msg_meta = Some(new_msg_meta);
                    }
                    ReadResult::PayloadData(buf) => {
                        println!("payload data");
                    }
                    ReadResult::PayloadFinished => {
                        println!("payload ok");
                    }
                    ReadResult::AttachmentData(index, buf) => {
                        println!("attachment data");
                    }
                    ReadResult::AttachmentFinished(index) => {
                        println!("attachment ok");
                    }
                };                            
            }
            res = f2 => {

            }
        };
    }

    /*
    println!("{:?}", payload);

    server_tx.send(ServerMsg::AddClient(msg_meta.tx.clone(), client_net_addr, client_tx))?;             

    loop {
        let route = select! {
            recv(state.rx) -> msg => BufRoute::Broker(msg?),
            recv(client_rx) -> msg => BufRoute::Socket(msg?)
        };

        match route {
            BufRoute::Broker(buf) => {
                server_tx.send(ServerMsg::SendBuf(msg_meta.tx.clone(), buf))?;
            }
            BufRoute::Socket(buf) => {
                socket_write.write_all(&buf).await?;
            }
        }
    }
    */
    
    /*
    match state.read_msg(&mut socket_read).await {
        Ok(_) => {
            loop {
                if state.read_msg(&mut socket_read).await.is_err() {
                    break;
                }

                // remove client because we have stopped reading from socket
                //server_tx.send(ServerMsg::RemoveClient(client_addr));
            }
        }
        Err(err) => {

        }
    }
    */ 

    Ok(())
}

async fn process_msg(msg_meta: &MsgMeta, payload: Value, socket_write: &mut WriteHalf<'_>, config: &Config) -> Result<(), ProcessError> {
    match msg_meta.key.as_ref() {
        "Hub.GetFile" => {    
            let dirs = config.dirs.as_ref().ok_or(ProcessError::GetFile(GetFileError::ConfigDirsIsEmpty))?;            
            let access_key = payload["access_key"].as_str().ok_or(ProcessError::GetFile(GetFileError::NoAccessKeyInPayload))?;
            let target_dir = dirs.iter().find(|x| x.access_key == access_key).ok_or(ProcessError::GetFile(GetFileError::TargetDirNotFound))?;
            let path = fs::read_dir(&target_dir.path)?.nth(0).ok_or(ProcessError::GetFile(GetFileError::NoFilesInTargetDir))??.path();            

            if path.is_file() {                                        
                let mut file_buf = [0; 1024];
                let mut file = File::open(&path).await?;

                loop {                    
                    match file.read(&mut file_buf).await? {
                        0 => break,
                        _ => socket_write.write_all(&file_buf).await?
                    }
                }                
            }
        }
        _ => {}
    }    

    Ok(())
}

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
use tokio::net::{TcpListener, TcpStream, tcp::split::ReadHalf};
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
    Io(std::io::Error),
    SerdeJson(serde_json::Error)    
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

enum Step<'a> {
    Len,
    MsgMeta(u32),
    Payload(MsgMeta, Take<&'a mut ReadHalf<'a>>),
    Attachment(MsgMeta, usize, Take<&'a mut ReadHalf<'a>>)
}

/// Data structure used for streaming data from source
struct State<'a> {
    pub len_buf: [u8; LEN_BUF_SIZE],    
    pub acc: Vec<u8>,
    pub step: Step<'a>
}

impl<'a> State<'a> {
    fn new() -> State<'a> {
        State {            
            len_buf: [0; LEN_BUF_SIZE],            
            acc: vec![],
            step: Step::Len
        }  
    }    
    async fn read(mut self, socket_read: &'a mut ReadHalf<'a>) -> Result<ReadResult, ProcessError> {
        println!("read called");

        match self.step {
            Step::Len => {                
                socket_read.read_exact(&mut self.len_buf).await?;                

                let mut buf = Cursor::new(&self.len_buf);
                let len = buf.get_u32_be();

                self.step = Step::MsgMeta(len);

                Ok(ReadResult::LenFinished)
            }
            Step::MsgMeta(len) => {
                println!("hi3 {}", len);

                let mut adapter = socket_read.take(len as u64);
                self.acc.clear();
                let n = adapter.read_to_end(&mut self.acc).await?;

                println!("len {}, n {}, acc len {}", len, n, self.acc.len());

                let msg_meta: MsgMeta = from_slice(&self.acc)?;
                let adapter = socket_read.take(msg_meta.payload_size as u64);

                self.step = Step::Payload(msg_meta.clone(), adapter);

                Ok(ReadResult::MsgMeta(msg_meta))
            }
            Step::Payload(msg_meta, mut adapter) => {
                let mut data_buf = [0; DATA_BUF_SIZE];           
                let n = adapter.read(&mut data_buf).await?;

                match n {
                    0 => {
                        match msg_meta.attachments.len() {
                            0 => self.step = Step::Len,
                            _ => {                                
                                
                                let adapter = socket_read.take(msg_meta.attachments[0].size as u64);
                                self.step = Step::Attachment(msg_meta, 0, adapter);
                            }                            
                        }

                        Ok(ReadResult::PayloadFinished)
                    }
                    _ => Ok(ReadResult::PayloadData(data_buf))
                }                                
            }
            Step::Attachment(msg_meta, index, mut adapter) => {
                let mut data_buf = [0; DATA_BUF_SIZE];           
                let n = adapter.read(&mut data_buf).await?;

                match n {
                    0 => {
                        match index < msg_meta.attachments.len() - 1 {
                            true => {
                                let new_index = index + 1;
                                let adapter = socket_read.take(msg_meta.attachments[new_index].size as u64);
                                self.step = Step::Attachment(msg_meta, new_index, adapter);                                
                            }
                            false => {
                                self.step = Step::Len;
                            }
                        }

                        Ok(ReadResult::AttachmentFinished(index))
                    }
                    _ => Ok(ReadResult::AttachmentData(index, data_buf))
                }                
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

    let mut config = String::new();

    buf_reader.read_to_string(&mut config)
        .expect("failed to read config");

    let config: Config = toml::from_str(&config)
        .expect("failed to deserialize config");	

    let mut listener = TcpListener::bind(&config.host).await?;

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
        let server_tx = server_tx.clone();

        println!("connected");  

        tokio::spawn(async move {
            let res = process(stream, client_net_addr, server_tx).await;

            println!("{:?}", res);
        });        
    }
}

async fn process(mut stream: TcpStream, client_net_addr: SocketAddr, mut server_tx: Sender<ServerMsg>) -> Result<(), ProcessError> {
    let (mut socket_read, mut socket_write) = stream.split();    

    let (msg_meta, payload, attachments) = read_full(&mut socket_read).await?;
    let payload: Value = from_slice(&payload)?;

    println!("auth {:?}", msg_meta);
    println!("auth {:?}", payload);    
    
    let (mut client_tx, mut client_rx) = mpsc::channel(MPSC_CLIENT_BUF_SIZE);

    server_tx.send(ServerMsg::AddClient(msg_meta.tx.clone(), client_net_addr, client_tx)).await.unwrap();    

    let mut state = State::new();

    let f1 = state.read(&mut socket_read).fuse();
    let f2 = client_rx.recv().fuse();

    pin_mut!(f1, f2);

    loop {
        println!("loop");        

        let res = select! {
            res = f1 => {
                match res? {
                    ReadResult::LenFinished => {
                        println!("len ok");
                    }
                    ReadResult::MsgMeta(msg_meta) => {
                        println!("{:?}", msg_meta);
                    }
                    ReadResult::PayloadData(buf) => {
                        println!("some data");
                    }
                    ReadResult::PayloadFinished => {
                        println!("payload ok");
                    }
                    ReadResult::AttachmentData(index, buf) => {

                    }
                    ReadResult::AttachmentFinished(index) => {

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

/*
async fn process(acc: &mut Vec<u8>, socket_write: &mut WriteHalf<'_>, config: &Config) -> Result<(), Box<dyn Error>> {
    let msg_meta = get_msg_meta(&acc)?;

    println!("{:?}", msg_meta);

    match msg_meta.key.as_ref() {
        "Hub.GetFile" => {
            let dirs = config.dirs.as_ref().ok_or("file requested, but config dirs are empty")?;

            let payload: Value = get_payload(&msg_meta, acc)?;

            let access_key = payload["access_key"].as_str().ok_or("no access_key in payload")?;

            let target_dir = dirs.iter().find(|x| x.access_key == access_key).ok_or("no target directory found for access key")?;

            let path = fs::read_dir(&target_dir.path)?.nth(0).ok_or("no files in target dir")??.path();            

            if path.is_file() {                                        
                let mut file_buf = [0; 1024];

                let mut file = File::open(&path).await?;

                loop {
                    let n = file.read(&mut file_buf).await?;

                    println!("file read n {}", n);

                    socket_write.write_all(&file_buf).await?;
                }                
            }
        }
        _ => {

        }
    }

    acc.clear();    

    Ok(())
}
*/

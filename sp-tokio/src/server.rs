use std::fmt;
use std::error::Error;
use std::collections::HashMap;
use std::io::prelude::*;
use std::io::BufReader;
use std::fs::{self, DirEntry};
use std::path::Path;
use std::net::SocketAddr;
use bytes::Buf;
use tokio::runtime::Runtime;
use tokio::net::{TcpListener, TcpStream, tcp::split::ReadHalf};
use tokio::prelude::*;
use tokio::fs::File;
use serde_json::Value;
use serde_derive::Deserialize;
use crossbeam::channel::{unbounded, select, Sender, Receiver};
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

struct Client {
    net_addr: SocketAddr,
    tx: Sender<[u8; DATA_BUF_SIZE]>
}

enum Operation {
    Len,
    Data
}

const LEN_BUF_SIZE: usize = 4;
const DATA_BUF_SIZE: usize = 1024;

enum ServerMsg {
    AddClient(String, SocketAddr, Sender<[u8; DATA_BUF_SIZE]>),
    SendBuf(String, [u8; DATA_BUF_SIZE]),
    RemoveClient(String)
}

enum BufRoute {
    Broker([u8; DATA_BUF_SIZE]),
    Socket([u8; DATA_BUF_SIZE])
}

struct State {
    pub operation: Operation,
    pub len: usize,
    pub bytes_read: usize,
    pub len_buf: [u8; LEN_BUF_SIZE],
    pub data_buf: [u8; DATA_BUF_SIZE], 
    pub acc: Vec<u8>,
    pub next: Vec<u8>,
    pub tx: Sender<[u8; DATA_BUF_SIZE]>,
    pub rx: Receiver<[u8; DATA_BUF_SIZE]>
}

#[derive(Debug)]
enum ProcessError {
    StreamClosed,
    NotEnoughBytesForLen,
    IncorrectReadResult,
    Io(std::io::Error),
    SerdeJson(serde_json::Error),
    CrossbeamChannelRecv(crossbeam::channel::RecvError),
    CrossbeamChannelSend(crossbeam::channel::SendError<ServerMsg>)
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

impl From<crossbeam::channel::RecvError> for ProcessError {
	fn from(err: crossbeam::channel::RecvError) -> ProcessError {
		ProcessError::CrossbeamChannelRecv(err)
	}
}

impl From<crossbeam::channel::SendError<ServerMsg>> for ProcessError {
	fn from(err: crossbeam::channel::SendError<ServerMsg>) -> ProcessError {
		ProcessError::CrossbeamChannelSend(err)
	}
}

/*
impl fmt::Display for StateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SuperErrorSideKick is here!")
    }
}

impl Error for StateError {
    fn description(&self) -> &str {
        match self {
            StateError::StreamClosed => "stream was closed",
            StateError::NotEnoughBytesForLen => "4 bytes needed on start for message"
        }
    }
}
*/

/// This enum describes how do we want to operate when reading data from socket
#[derive(PartialEq)]
enum ReadConfig {
    /// Read one message and return it
    ReadOne,
    /// Continuosly stream incoming data
    Stream
}

/// The result of reading function
enum ReadResult {
    /// A message, in case we configured state to read one message with ReadConfig::ReadOne
    Msg(MsgMeta),
    /// Reading process stop, in case we configured state for stream with ReadConfig::Stream
    Stop
}

impl State {
    fn new() -> State {
        let (tx, rx) = unbounded();

        State {
            operation: Operation::Len,
            len: 0,
            bytes_read: 0,
            len_buf: [0; LEN_BUF_SIZE],
            data_buf: [0; DATA_BUF_SIZE],
            acc: vec![],
            next: vec![],
            tx,
            rx
        }  
    }
    fn switch_to_len(&mut self) {
        self.operation = Operation::Len;
        self.len = 0;
        self.bytes_read = 0;
    }
    fn switch_to_data(&mut self, len: usize) {
        self.operation = Operation::Data;
        self.len = len;
        self.bytes_read = 0;
    }
    fn reset(&mut self, len: usize) {
        self.len = len;
        self.bytes_read = 0;
    }
    fn increment(&mut self, delta: usize) {
        self.bytes_read = self.bytes_read + delta;
    }
    async fn read_msg(&mut self, config: ReadConfig, socket_read: &mut ReadHalf<'_>) -> Result<ReadResult, ProcessError> {
        loop {
            match self.operation {
                Operation::Len => {
                    println!("len");

                    if self.acc.len() > 0 {
                        self.acc.clear();
                    }

                    if self.next.len() > 0 {
                        self.acc.extend_from_slice(&self.next);
                        self.next.clear();
                    }

                    let n = socket_read.read_exact(&mut self.len_buf).await?;

                    println!("Operation::Len, server n is {}", n);

                    match n {
                        0 => return Err(ProcessError::StreamClosed),
                        LEN_BUF_SIZE => { 
                            self.acc.extend_from_slice(&self.len_buf);

                            let mut cursor = std::io::Cursor::new(self.len_buf);
                            let len = cursor.get_u32_be();

                            self.switch_to_data(len as usize);
                        }
                        _ => return Err(ProcessError::NotEnoughBytesForLen)
                    }               
                }
                Operation::Data => {
                    let n = socket_read.read(&mut self.data_buf).await?;

                    let mut msg_meta = None;

                    println!("Operation::Data, server n is {}", n);

                    match n {
                        0 => return Err(ProcessError::StreamClosed),
                        _ => {
                            self.increment(n);

                            println!("bytes_read {}, len {}", self.bytes_read, self.len);

                            if self.bytes_read == self.len {
                                self.switch_to_len();

                                self.acc.extend_from_slice(&self.data_buf[..n]);

                                println!("bytes_read == len");
                                println!("acc len {}", self.acc.len());

                                match msg_meta {
                                    Some(msg_meta) => {
                                        match config {
                                            ReadConfig::ReadOne => {
                                                return Ok(ReadResult::Msg(msg_meta));
                                            }
                                            ReadConfig::Stream => {
                                                
                                            }
                                        }
                                    }
                                    None => {
                                        println!("1");

                                        let current = get_msg_meta(&self.acc)?;

                                        println!("{:?}",current);

                                        self.reset(current.content_len() as usize);

                                        msg_meta = Some(current);
                                    }
                                }
                            } else

                            if self.bytes_read > self.len {
                                let offset = self.bytes_read - self.len;
                                self.switch_to_len();

                                self.acc.extend_from_slice(&self.data_buf[..offset]);
                                
                                match msg_meta {
                                    Some(msg_meta) => {
                                        self.next.extend_from_slice(&self.data_buf[offset..n]);

                                        match config {
                                            ReadConfig::ReadOne => {
                                                return Ok(ReadResult::Msg(msg_meta));
                                            }
                                            ReadConfig::Stream => {

                                            }
                                        }
                                    }
                                    None => {
                                        println!("2");

                                        let current = get_msg_meta(&self.acc)?;

                                        println!("{:?}",current);

                                        self.reset(current.content_len() as usize);

                                        msg_meta = Some(current);
                                    }
                                }

                                println!("bytes_read > len");                                    
                            } else 

                            if self.bytes_read < self.len {                                    
                                self.acc.extend_from_slice(&self.data_buf[..n]);

                                println!("bytes_read < len");
                            }                                                         
                        }
                    }
                }
            }
        }

        Ok(ReadResult::Stop)
    }    
}

/*
fn send_msg(acc: &Vec<u8>, server_tx: i32) -> Result<(), Box<dyn Error>> {
    let msg_meta = get_msg_meta(acc)?;

    server_tx.send(ServerMsg::AddClient(msg_meta.rx, addr, client_tx));

    Ok(())
}
*/

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

    let (server_tx, server_rx) = crossbeam::channel::unbounded();
    
    let clients = std::thread::Builder::new()
        .name("clients".to_owned())
        .spawn(move || {    
            let mut clients = HashMap::new();

            loop {
                let msg = server_rx.recv().expect("clients msg receive failed");

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
        })
        .expect("failed to start clients thread");    

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

async fn process(mut stream: TcpStream, client_net_addr: SocketAddr, server_tx: Sender<ServerMsg>) -> Result<(), ProcessError> {
    let (mut socket_read, mut socket_write) = stream.split();
    let (client_tx, client_rx) = unbounded();

    let mut state = State::new();

    let msg_meta = match state.read_msg(ReadConfig::ReadOne, &mut socket_read).await? {
        ReadResult::Msg(msg_meta) => msg_meta,
        _ => return Err(ProcessError::IncorrectReadResult)
    };

    let payload: Value = get_payload(&msg_meta, &state.acc)?;

    println!("{:?}", msg_meta);
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

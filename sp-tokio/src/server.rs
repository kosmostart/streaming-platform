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
use crossbeam::channel::Sender;
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
    tx: Sender<i32>
}

enum Operation {
    Len,
    Data
}

struct State {
    pub operation: Operation,
    pub len: usize,
    pub bytes_read: usize,
    pub len_buf: [u8; 4],
    pub data_buf: [u8; 1024], 
    pub acc: Vec<u8>
}

#[derive(Debug)]
enum StateError {
    StreamClosed,
    NotEnoughBytesForLen
}

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

impl State {
    fn new() -> State {
        State {
            operation: Operation::Len,
            len: 0,
            bytes_read: 0,
            len_buf: [0; 4],
            data_buf: [0; 1024],
            acc: vec![]
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
    fn increment(&mut self, delta: usize) {
        self.bytes_read = self.bytes_read + delta;
    }
    async fn read_msg(&mut self, socket_read: &mut ReadHalf<'_>) -> Result<(), Box<dyn Error>> {
        loop {
            match self.operation {
                Operation::Len => {
                    println!("len");

                    let n = socket_read.read_exact(&mut self.len_buf).await?;

                    println!("server n is {}", n);

                    match n {
                        0 => {
                            // socket closed ?
                            return Err(Box::new(StateError::StreamClosed));
                        }
                        4 => {
                            //acc.extend_from_slice(&len_buf);

                            let mut cursor = std::io::Cursor::new(self.len_buf);
                            let len = cursor.get_u32_be();

                            self.switch_to_data(len as usize);
                        }
                        _ => {
                            return Err(Box::new(StateError::NotEnoughBytesForLen));
                        }
                    }               
                }
                Operation::Data => {
                    let n = socket_read.read(&mut self.data_buf).await?;

                    println!("server n is {}", n);

                    match n {
                        0 => {
                            return Err(Box::new(StateError::StreamClosed));
                        }                            
                        _ => {
                            self.increment(n);

                            println!("bytes_read {}", self.bytes_read);

                            if self.bytes_read == self.len {
                                self.switch_to_len();

                                self.acc.extend_from_slice(&mut self.data_buf[..n]);

                                println!("bytes_read == len");
                                println!("acc len {}", self.acc.len());

                                //process(&mut acc, &mut socket_write, &config).await;
                            } else

                            if self.bytes_read > self.len {
                                let offset = self.bytes_read - self.len;
                                self.switch_to_len();

                                self.acc.extend_from_slice(&mut self.data_buf[..offset]);

                                //process(&mut acc, &mut socket_write, &config).await;

                                self.acc.extend_from_slice(&mut self.data_buf[..n]);
                                println!("bytes_read > len");                                    
                            } else 

                            if self.bytes_read < self.len {                                    
                                self.acc.extend_from_slice(&mut self.data_buf[..n]);

                                println!("bytes_read < len");
                            }                                                         
                        }
                    }
                }
            }
        }

        Ok(())
    }    
}

/*
fn send_msg(acc: &Vec<u8>, server_tx: i32) -> Result<(), Box<dyn Error>> {
    let msg_meta = get_msg_meta(acc)?;

    server_tx.send(ServerMsg::AddClient(msg_meta.rx, addr, client_tx));

    Ok(())
}
*/

enum ServerMsg {
    AddClient(String, SocketAddr, Sender<i32>),
    SendBuf(String),
    RemoveClient(String)
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
                    ServerMsg::SendBuf(addr) => {
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

        println!("connected");

        let (client_tx, client_rx) = crossbeam::channel::unbounded();

        use std::sync::Arc;
        use tokio::sync::Mutex;

        let mut stream = Arc::new(Mutex::new(stream));

        //tx.send(ClientMsg::AddClient(stream));

        //let config = config.clone();
        //let tx = tx.clone();
        
        /*
        let (mut socket_read, mut socket_write) = clients.iter_mut().nth(0).unwrap().split();
        
        tokio::spawn(async {
            //socket_write.write_all(&[]).await;
        });
        */

        let mut stream2 = stream.clone();
        let mut stream3 = stream.clone();
        
        //let (mut socket_read, mut socket_write) = stream.split();
        
        tokio::spawn(async move {
            let mut q = stream3.lock().await;
            let (mut socket_read, mut socket_write) = q.split();

            println!("1 stream ok");

            loop {                
                match client_rx.recv() {
                    Ok(msg) => {

                    }
                    Err(err) => {
                        break;
                    }
                }                
            }
        });

        tokio::spawn(async move {
            let mut q = stream2.lock().await;
            let (mut socket_read, mut socket_write) = q.split();

            println!("2 stream ok");            
            
            let mut state = State::new();

            loop {                               
                if state.read_msg(&mut socket_read).await.is_err() {
                    break;
                }

                // remove client because we have stopped reading from socket
                server_tx.send(ServerMsg::RemoveClient(client_addr));
            }
        });
        
    }
}

/*
//let (tx, rx) = crossbeam::channel::unbounded();

//loop {
//    let q: Result<i32, _> = rx.recv();
//}
*/

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

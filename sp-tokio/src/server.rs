use std::error::Error;
use std::collections::HashMap;
use std::io::prelude::*;
use std::io::BufReader;
use std::fs::{self, DirEntry};
use std::path::Path;
use bytes::Buf;
use tokio::runtime::Runtime;
use tokio::net::{TcpListener, TcpStream, tcp::split::WriteHalf};
use tokio::prelude::*;
use tokio::fs::File;
use serde_json::Value;
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

struct State {
    pub operation: Operation,
    pub len: usize,
    pub bytes_read: usize
}

impl State {
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
}

pub enum Operation {
    Len,
    Data
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
    //let mut clients = HashMap::new();

    println!("ok");

    loop {
        let (mut stream, addr) = listener.accept().await?;
        let config = config.clone();

        tokio::spawn(async move {
            let (mut socket_read, mut socket_write) = stream.split();

            let mut len_buf = [0; 4];
            let mut data_buf = [0; 1024];

            let mut state = State {
                operation: Operation::Len,
                len: 0,
                bytes_read: 0
            };            
            
            let mut acc = vec![];            

            loop {
                println!("server loop");

                match state.operation {

                    Operation::Len => {
                        println!("len");

                        let n = match socket_read.read_exact(&mut len_buf).await {
                            Ok(n) => n,                    
                            Err(e) => {
                                println!("failed to read from socket; err = {:?}", e);
                                return;
                            }
                        };

                        println!("server n is {}", n);

                        match n {
                            0 => {
                                // socket closed ?
                                println!("returning, perhaps socket was closed");
                                return;
                            }
                            4 => {
                                //acc.extend_from_slice(&len_buf);

                                let mut cursor = std::io::Cursor::new(len_buf);
                                let len = cursor.get_u32_be();

                                state.switch_to_data(len as usize);
                            }
                            _ => {
                                println!("4 bytes needed on start, please reconnect with proper connection");
                                return;
                            }
                        }

                        /*
                        // Write the data back                    
                        if let Err(e) = socket_write.write_all(&buf[0..n]).await {
                            println!("failed to write to socket; err = {:?}", e);
                            return;
                        }

                        println!("server write ok");
                        }
                        */                    
                    }
                    Operation::Data => {
                        let n = match socket_read.read(&mut data_buf).await {
                            Ok(n) => n,                    
                            Err(e) => {
                                println!("failed to read from socket; err = {:?}", e);
                                return;
                            }
                        };

                        println!("server n is {}", n);

                        match n {
                            0 => {
                                // socket closed ?
                                println!("returning, perhaps socket was closed");
                                return;
                            }                            
                            _ => {
                                state.increment(n);

                                println!("bytes_read {}", state.bytes_read);

                                if state.bytes_read == state.len {
                                    state.switch_to_len();

                                    acc.extend_from_slice(&data_buf[..n]);

                                    println!("bytes_read == len");
                                    println!("acc len {}", acc.len());

                                    process(&mut acc, &mut socket_write, &config).await;
                                } else

                                if state.bytes_read > state.len {
                                    let offset = state.bytes_read - state.len;
                                    state.switch_to_len();

                                    acc.extend_from_slice(&data_buf[..offset]);

                                    process(&mut acc, &mut socket_write, &config).await;

                                    acc.extend_from_slice(&data_buf[..n]);
                                    println!("bytes_read > len");                                    
                                } else 

                                if state.bytes_read < state.len {                                    
                                    acc.extend_from_slice(&data_buf[..n]);

                                    println!("bytes_read < len");
                                }                                                         
                            }
                        }
                    }
                }  
            }
        });
    }
}

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

async fn dog() -> Result<(), serde_json::Error> {
    let data = vec![];

    let msg_meta = get_msg_meta(&data)?;
    let payload: Value = get_payload(&msg_meta, &data)?;

    Ok(())
}

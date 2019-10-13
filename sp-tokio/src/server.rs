use std::error::Error;
use std::collections::HashMap;
use bytes::Buf;
use tokio::runtime::Runtime;
use tokio::net::{TcpListener, TcpStream, tcp::split::WriteHalf};
use tokio::prelude::*;
use tokio::fs::File;
use serde_json::Value;
use serde_derive::Deserialize;
use sp_dto::*;

#[derive(Debug, Deserialize)]
struct Config {
    host: String    
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
    let mut listener = TcpListener::bind("127.0.0.1:12346").await?;
    //let mut clients = HashMap::new();

    println!("ok!");

    loop {
        let (mut stream, addr) = listener.accept().await?;        

        tokio::spawn(async move {
            let (mut socket_read, mut socket_write) = stream.split();
            
            let mut len_buf = [0; 4];
            let mut data_buf = [0; 1024];

            let mut state = State {
                operation: Operation::Len,
                len: 0,
                bytes_read: 0
            };

            println!("state initialized");
            
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

                                    process(&mut acc, &mut socket_write).await;
                                } else

                                if state.bytes_read > state.len {
                                    let offset = state.bytes_read - state.len;
                                    state.switch_to_len();

                                    acc.extend_from_slice(&data_buf[..offset]);

                                    process(&mut acc, &mut socket_write).await;

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

async fn process(acc: &mut Vec<u8>, socket_write: &mut WriteHalf<'_>) {
    let msg_meta = get_msg_meta(&acc).unwrap();

    println!("{:?}", msg_meta);

    match msg_meta.key.as_ref() {
        "Hub.GetFile" => {

            let payload: Value = get_payload(&msg_meta, acc).unwrap();

            let access_key = payload["access_key"].as_str().unwrap();
            
            let file_name = "Cargo.toml";

            let mut contents = [0; 1024];                                    

            match File::open(file_name).await {
                Ok(mut file) => {
                    loop {
                        match file.read(&mut contents).await {
                            Ok(n) => {
                                println!("file read n {}", n);

                                if n < 1024 {
                                    return;
                                }
                            }
                            Err(err) => {
                                println!("failed to read from file {} {:?}", file_name, err);
                                return;
                            }
                        }

                        if let Err(err) = socket_write.write_all(&contents).await {
                            println!("failed to write to socket {:?}", err);
                            return;
                        }

                        println!("server write ok");
                    }                                                                                        
                }
                Err(err) => {
                    println!("error opening file {} {:?}", file_name, err);    
                }
            }            
        }
        _ => {

        }
    }

    acc.clear();
}

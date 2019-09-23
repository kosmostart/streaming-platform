use std::error::Error;
use tokio::runtime::Runtime;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio_io::split::split;

pub enum State {
    Len,
    Data {
        len: usize,
        bytes_read: usize
    }
}

pub fn q() {
    let rt = Runtime::new().unwrap();
    
     rt.block_on(start());
}
pub async fn start() -> Result<(), Box<dyn Error>> {
    let mut listener = TcpListener::bind("127.0.0.1:12346").await?;

    println!("ok!");

    loop {
        let (mut stream, _) = listener.accept().await?;
        let (mut socket_read, mut socket_write) = split(stream);

        tokio::spawn(async move {
            let mut len_buf = [0; 4];
            let mut data_buf = [0; 1024];
            let mut state = State::Len;
            let mut acc = vec![];

            loop {
                match state {
                    State::Len => {
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
                            4 => state = State::Data { len: n, bytes_read: 0 },
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
                    State::Data { len, bytes_read } => {
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
                                let bytes_amount = bytes_read + n;

                                if bytes_amount == len {
                                    let state = State::Len;
                                    acc.extend_from_slice(&data_buf);
                                } else 

                                if bytes_amount > len {
                                    let state = State::Len;
                                    let offset = bytes_amount - len;                                    
                                } else 

                                if bytes_amount < len {
                                    let state = State::Data { len, bytes_read: bytes_amount };
                                    acc.extend_from_slice(&data_buf);
                                }                                                         
                            }
                        }
                    }
                }
            }
        });
    }
}

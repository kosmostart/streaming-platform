use std::error::Error;
use bytes::Buf;
use tokio::runtime::Runtime;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio_io::split::split;
use sp_dto::*;

pub enum State {
    Len,
    Data {
        len: usize,
        bytes_read: usize
    }
}

pub fn start() {
    let rt = Runtime::new().unwrap();
    
    rt.block_on(start_future());
}

async fn start_future() -> Result<(), Box<dyn Error>> {
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
            let mut next = vec![];

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
                            4 => {
                                //acc.extend_from_slice(&len_buf);

                                let mut cursor = std::io::Cursor::new(len_buf);
                                let len = cursor.get_u32_be();

                                state = State::Data { len: len as usize, bytes_read: 0 };
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

                                    acc.extend_from_slice(&data_buf[..n]);

                                    println!("bytes_amount == len");

                                    println!("len {}", acc.len());

                                    let q = get_msg_meta(&acc).unwrap();

                                    println!("{:?}", q);

                                    use tokio::fs::File;
                                    use tokio::prelude::*;

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
                                } else 

                                if bytes_amount > len {
                                    let state = State::Len;
                                    let offset = bytes_amount - len;

                                    acc.extend_from_slice(&data_buf[..offset]);
                                    next.extend_from_slice(&data_buf[offset..]);

                                    println!("bytes_amount > len");                                    
                                } else 

                                if bytes_amount < len {
                                    let state = State::Data { len, bytes_read: bytes_amount };

                                    acc.extend_from_slice(&data_buf[..n]);

                                    println!("bytes_amount < len");
                                }                                                         
                            }
                        }
                    }
                }
            }
        });
    }
}

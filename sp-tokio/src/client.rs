/*
use std::io::prelude::*;
use std::net::TcpStream;
use bytes::BufMut;
use serde_json::json;
use sp_dto::*;

pub fn connect() -> std::io::Result<()> {
    let mut stream = TcpStream::connect("127.0.0.1:12346")?;

    let mut data_buf = [0; 1024];

    let route = Route {
        source: Participator::Service("qwe".to_owned()),
        spec: RouteSpec::Simple,
        points: vec![Participator::Service("qwe".to_owned())]
    };  

    let rpc_dto = rpc_dto("asd".to_owned(), "qwe".to_owned(), "key".to_owned(), json!({
    }), route).unwrap();

    let mut buf = vec![];

    let len = rpc_dto.len();

    println!("len {}", len);

    buf.put_u32_be(len as u32);

    stream.write(&buf)?;
    stream.write(&rpc_dto)?;

    loop {
        stream.read(&mut data_buf)?;
    }    

    Ok(())
} 
*/

use std::error::Error;
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio::runtime::Runtime;
use tokio_io::split::{split, WriteHalf};
use bytes::BufMut;
use serde_json::json;
use sp_dto::*;

pub fn connect() {
    let rt = Runtime::new().expect("failed to create runtime"); 
    
    rt.block_on(connect_future());
}

pub async fn connect_future() {
    let mut stream = TcpStream::connect("127.0.0.1:12346").await.unwrap();
    let (mut socket_read, mut socket_write) = split(stream);

    //let mut data_buf = [0; 1024];

    let route = Route {
        source: Participator::Service("qwe".to_owned()),
        spec: RouteSpec::Simple,
        points: vec![Participator::Service("qwe".to_owned())]
    };  

    let rpc_dto = rpc_dto("asd".to_owned(), "qwe".to_owned(), "key".to_owned(), json!({
    }), route).unwrap();

    let mut buf = vec![];

    let len = rpc_dto.len();

    println!("len {}", len);

    buf.put_u32_be(len as u32);

    if let Err(err) = socket_write.write_all(&buf).await {
        println!("failed to write to socket {:?}", err);
        return;
    }

    if let Err(err) = socket_write.write_all(&rpc_dto).await {
        println!("failed to write to socket {:?}", err);
        return;
    }

    let mut len_buf = [0; 4];

    loop {
        let n = match socket_read.read_exact(&mut len_buf).await {
            Ok(n) => n,                    
            Err(e) => {
                println!("failed to read from socket; err = {:?}", e);
                return;
            }
        };
    }

    //Ok(())
}

/*
//let (tx, rx) = crossbeam::channel::unbounded();

//loop {
//    let q: Result<i32, _> = rx.recv();
//}
*/
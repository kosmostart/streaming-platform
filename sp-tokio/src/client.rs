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


/*
//let (tx, rx) = crossbeam::channel::unbounded();

//loop {
//    let q: Result<i32, _> = rx.recv();
//}
*/
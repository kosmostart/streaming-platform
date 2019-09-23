use std::error::Error;
use bytes::BufMut;
use tokio::runtime::Runtime;
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio_io::split::split;
use serde_json::json;
use sp_dto::*;

pub fn connect() {
    let rt = Runtime::new().unwrap();
    
    rt.block_on(connect_future());
}

async fn connect_future() -> Result<(), Box<dyn Error>> {
    let mut stream = TcpStream::connect("127.0.0.1:12346").await?;
    let (mut socket_read, mut socket_write) = split(stream);

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

    socket_write.write_all(&buf).await?;
    socket_write.write_all(&rpc_dto).await?;

    loop {
        let n = socket_read.read(&mut data_buf).await?;
        println!("client n is {:?}", n);
    }    
}

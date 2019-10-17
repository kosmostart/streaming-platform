use std::error::Error;
use bytes::BufMut;
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio::runtime::Runtime;
use serde_json::json;
use sp_dto::*;

pub fn connect(addr: &str) {
    let rt = Runtime::new().expect("failed to create runtime"); 
    
    rt.block_on(connect_future(addr));
}

pub async fn connect_future(addr: &str) {
    let mut stream = TcpStream::connect("127.0.0.1:60000").await.unwrap();
    let (mut socket_read, mut socket_write) = stream.split();

    //let mut data_buf = [0; 1024];

    let route = Route {
        source: Participator::Service(addr.to_owned()),
        spec: RouteSpec::Simple,
        points: vec![Participator::Service(addr.to_owned())]
    };  

    let rpc_dto = rpc_dto(addr.to_owned(), addr.to_owned(), "key".to_owned(), json!({
    }), route).unwrap();

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

        println!("client n {}", n);
    }

    //Ok(())
}

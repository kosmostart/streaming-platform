use tokio::runtime::Runtime;
use tokio::net::TcpStream;
use tokio::prelude::*;
use std::error::Error;
use tokio_io::split::split;

pub fn q() {
    let rt = Runtime::new().unwrap();
    
     rt.block_on(connect());
}

async fn connect() -> Result<(), Box<dyn Error>> {    
    let mut stream = TcpStream::connect("127.0.0.1:12346").await?;
    let (mut socket_read, mut socket_write) = split(stream);

    let mut buf = [0; 1024];

    socket_write.write_all(b"hello world!").await?;

    loop {
        let n = socket_read.read(&mut buf).await?;
        println!("client n is {:?}", n);
    }    
}

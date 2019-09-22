use std::error::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio_io::split::split;

#[tokio::main]
pub async fn connect() -> Result<(), Box<dyn Error>> {
    // Connect to a peer 
    let mut socket = TcpStream::connect("127.0.0.1:12346").await.unwrap();
    let (mut socket_read, mut socket_write) = split(socket); //socket.split();
    
    tokio::spawn(async move {
        let mut buf = [0; 1024];
        // In a loop, read data from the socket and write the data back.
        println!("client loop");        
        socket_write.write_all(b"hello world!").await.unwrap();        
 
        loop {
            println!("client read");
            // Write some data.            
            let n = match socket_read.read(&mut buf).await {
                // socket closed
                Ok(n) if n == 0 => return,
                Ok(n) => n,
                Err(e) => {
                    println!("failed to read from socket; err = {:?}", e);
                    return;
                }
            };
            println!("client n is {}", n);
        }        
    });

    //if let Err(e) = socket_write.write_all(b"hello").await {
    //    println!("failed to write to socket; err = {:?}", e);
    //}

    Ok(())
}

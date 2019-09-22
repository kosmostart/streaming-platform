use std::error::Error;
use tokio::runtime::Runtime;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio_io::split::split;

pub fn q() {
    let rt = Runtime::new().unwrap();
    
     rt.block_on(start());
}
pub async fn start() -> Result<(), Box<dyn Error>> {
    let mut listener = TcpListener::bind("127.0.0.1:12346").await?;

    println!("ok!");

    loop {
        let (mut socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            let mut buf = [0; 1024];

            // In a loop, read data from the socket and write the data back.
            loop {
                let n = match socket.read(&mut buf).await {
                    // socket closed
                    Ok(n) if n == 0 => return,
                    Ok(n) => n,
                    Err(e) => {
                        println!("failed to read from socket; err = {:?}", e);
                        return;
                    }
                };

                println!("server n is {}", n);

                // Write the data back
                if let Err(e) = socket.write_all(&buf[0..n]).await {
                    println!("failed to write to socket; err = {:?}", e);
                    return;
                }

                println!("server write ok");
            }
        });
    }    
}

use std::error::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio_io::split::split;
/*
#[tokio::main]
async fn start() -> Result<(), Box<dyn Error>> {
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
*/
#[tokio::main]
async fn connect() -> Result<(), Box<dyn Error>> {
    // Connect to a peer 
      
    
    tokio::spawn(async move {
        let mut buf = [0; 1024];
        // In a loop, read data from the socket and write the data back.
        println!("client loop");        
        //socket_write.write_all(b"hello world!").await.unwrap();
        // 
        
        //let mut socket = TcpStream::connect("127.0.0.1:12346").await.unwrap();
        //let (mut socket_read, mut socket_write) = split(socket); //socket.split();
 /*
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
        */
    });

    //if let Err(e) = socket_write.write_all(b"hello").await {
    //    println!("failed to write to socket; err = {:?}", e);
    //}

    Ok(())
}

#[test]
fn test_all() {
    /*
    let server = std::thread::Builder::new()
        .name("server".to_owned())
        .spawn(move || {
            let res = start();

            println!("{:#?}", res);
        })
        .unwrap();

    use std::{thread, time};

    thread::sleep(time::Duration::from_millis(5000));
    */

    let res = connect();
            
    println!("{:#?}", res);
}

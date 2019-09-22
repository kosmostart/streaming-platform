pub mod server;
pub mod client;

/*
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
*/
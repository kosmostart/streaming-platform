use sp_tokio::client::connect;
use sp_pack_core::unpack;

fn main() {
    let host = "127.0.0.1:60000";
    
    connect(host);
    /*
    let path = std::env::args().nth(1)
        .expect("path to file not passed as argument");

    unpack(&path);
    */
}

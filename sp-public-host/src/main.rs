use std::net::SocketAddr;
use warp::Filter;

pub fn main() {
    let routes = warp::fs::file("/delivery/spup");

	let addr = "0.0.0.0:80".parse::<SocketAddr>().unwrap();
    //let addr = "0.0.0.0:12345".parse::<SocketAddr>().unwrap();
    warp::serve(routes).run(addr);
}

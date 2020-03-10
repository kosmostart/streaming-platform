use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::net::SocketAddr;
use serde_derive::Deserialize;
use futures::stream::TryStreamExt;
use futures::future::TryFutureExt;
use bytes::buf::BufMut;
use tokio::runtime::Runtime;
use warp::{Filter, fs};
use warp::{multipart, http::{Response}};

#[derive(Debug, Deserialize)]
struct Config {
    host: String,
    cert_path: String,
    key_path: String,
    dir: String
}

pub fn main() {
    let config_path = std::env::args().nth(1)
        .expect("path to config file not passed as argument");
    let file = File::open(config_path)
        .expect("failed to open config");
    let mut buf_reader = BufReader::new(file);
    let mut config = String::new();
    buf_reader.read_to_string(&mut config)
        .expect("failed to read config");
    let config: Config = toml::from_str(&config)
        .expect("failed to deserialize config");
    let host = config.host.parse::<SocketAddr>().unwrap();
    let routes = fs::dir(config.dir);
    
    /*
    let routes = 
        warp::path("upload").and(
        multipart::form()).and_then(|form: multipart::FormData| {
        async {
            // Collect the fields into (name, value): (String, Vec<u8>)
            let part: Result<Vec<(String, Vec<u8>)>, warp::Rejection> = form
                .and_then(|part| {
                    let name = part.name().to_string();
                    let value = part.stream().try_fold(Vec::new(), |mut vec, data| {
                        vec.put(data);
                        async move { Ok(vec) }
                    });
                    value.map_ok(move |vec| (name, vec))
                })
                .try_collect()
                .await
                .map_err(|e| {
                    panic!("multipart error: {:?}", e);
                });

                for (name, data) in part.unwrap() {
                    let mut file = File::create(name).unwrap();
                    file.write_all(&data).unwrap();
                }

                let res = Response::builder()                
                .body("ok").unwrap();

                Ok::<Response<&str>, warp::Rejection>(res)
        }
    });
    */

    let mut rt = Runtime::new().expect("failed to create runtime");
    rt.block_on(warp::serve(routes)
        .tls()
        .cert_path(config.cert_path)
        .key_path(config.key_path)
        .run(host)
    );   
}

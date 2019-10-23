use std::thread;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::io::BufReader;
use std::io::prelude::*;
use serde_derive::Deserialize;
use serde_json::json;
use sp_tokio::{tokio, tokio::runtime::Runtime, mpsc, sp_dto::*, client::{connect, write, ClientMsg}, proto::MPSC_CLIENT_BUF_SIZE};
use sp_pack_core::unpack;

#[derive(Debug, Deserialize)]
struct Config {
    host: String,
    addr: String,
    access_key: String,
    path: String
}

fn main() {
    let config_path = std::env::args().nth(1)
        .expect("path to config file not passed as argument");

    let file = std::fs::File::open(config_path)
        .expect("failed to open config");

    let mut buf_reader = BufReader::new(file);

    let mut config = String::new();

    buf_reader.read_to_string(&mut config)
        .expect("failed to read config");

    let config: Config = toml::from_str(&config)
        .expect("failed to deserialize config");

    let rt = Runtime::new().expect("failed to create runtime");

    let (mut read_tx, mut read_rx) = mpsc::channel(MPSC_CLIENT_BUF_SIZE);
    let (mut write_tx, mut write_rx) = mpsc::channel(MPSC_CLIENT_BUF_SIZE);

    let addr = config.addr.clone();
    let access_key = config.access_key.clone();

    let mut write_tx = write_tx.clone();

    rt.spawn(async move {        
        let target = "SvcHub";

        let route = Route {
            source: Participator::Service(addr.clone()),
            spec: RouteSpec::Simple,
            points: vec![Participator::Service(addr.clone())]
        };  

        let dto = rpc_dto(addr.clone(), target.to_owned(), "Auth".to_owned(), json!({
            "access_key": access_key
        }), route).unwrap();

        let res = write(dto, &mut write_tx).await;
        println!("{:?}", res);

        let route = Route {
            source: Participator::Service(addr.clone()),
            spec: RouteSpec::Simple,
            points: vec![Participator::Service(addr.clone())]
        };  

        let dto = rpc_dto(addr.to_owned(), target.to_owned(), "Hub.GetFile".to_owned(), json!({        
        }), route).unwrap();

        let res = write(dto, &mut write_tx).await;
        println!("{:?}", res);
    });    

    let path = config.path.clone();

    rt.spawn(async move {
        match read_rx.recv().await {
            Some(msg) => {
                match msg {
                    ClientMsg::FileReceiveComplete(name) => unpack(&(path + "/" + &name))
                }
            }
            None => {

            }
        }
    });

    rt.block_on(connect(&config.host, &config.addr, &config.access_key, read_tx, write_rx));    
}

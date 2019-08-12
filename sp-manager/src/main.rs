use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;
use std::net::SocketAddr;
use serde_derive::Deserialize;
use warp::Filter;

#[derive(Debug, Deserialize)]
struct Hub {
    file_name: Option<String>,
    config: Option<toml::Value>
}

#[derive(Debug, Deserialize)]
struct Service {
    file_name: Option<String>,
    config: Option<toml::Value>
}

/// This is what we're going to decode into. Each field is optional, meaning
/// that it doesn't have to be present in TOML.
#[derive(Debug, Deserialize)]
struct Config {
    hub_path: Option<String>,
    service_path: Option<String>,
    hubs: Option<Vec<Hub>>,
    services: Option<Vec<Service>>
}

fn main() {

    let config_path = std::env::args().nth(1)
        .expect("config location not passed as argument");

    let file = File::open(config_path)
        .expect("failed to open config");
    //let file = File::open("ddl/".to_owned() + &service.path() + "/" + &domain.path() + ".sql")?;
    let mut buf_reader = BufReader::new(file);

    let mut config = String::new();

    buf_reader.read_to_string(&mut config)
        .expect("failed to read config");

    let config: Config = toml::from_str(&config).unwrap();
    println!("{:#?}", config);

    let hub_path = config.hub_path
        .expect("hub path is empty");

    for hub in config.hubs.unwrap() {

        match hub.file_name {
            Some(file_name) => {                
                match hub.config {
                    Some(config) => {
                        std::process::Command::new(hub_path.clone() + "/" + &file_name)
                            .arg(toml::to_string(&config)
                                .expect("serialization to TOML string failed, check hub config")
                            )
                            .spawn()
                            .expect(&format!("{} command failed to start", file_name));
                    }
                    None => {
                        std::process::Command::new(hub_path.clone() + "/" + &file_name)
                            .spawn()
                            .expect(&format!("{} command failed to start", file_name));
                    }
                }
            }
            None => {
                println!("hub with empty file name, please note");
            }
        }

    }

    match config.services {
        Some(services) => {
            let service_path = config.service_path.clone()
                .expect("service path is empty, but services are present");

            for service in services {

                match service.file_name {
                    Some(file_name) => {
                        match service.config {
                            Some(config) => {
                                std::process::Command::new(service_path.clone() + "/" + &file_name)
                                    .arg(toml::to_string(&config)
                                        .expect("serialization to TOML string failed, check service config")
                                    )
                                    .spawn()
                                    .expect(&format!("{} command failed to start", file_name));
                            }
                            None => {
                                std::process::Command::new(service_path.clone() + "/" + &file_name)                                    
                                    .spawn()
                                    .expect(&format!("{} command failed to start", file_name));
                            }
                        }
                    }
                    None => {
                        println!("service with empty file name, please note");
                    }
                }
            }

        }
        None => {
            println!("no services are configured to run");
        }
    }

    let routes = warp::path("hello")
        .and(warp::path::param())
        .and(warp::header("user-agent"))
        .map(|param: String, agent: String| {
            format!("Hello {}, whose agent is {}", param, agent)
        });
	
    let addr = "0.0.0.0:49999".parse::<SocketAddr>().unwrap();
    warp::serve(routes).run(addr);
}

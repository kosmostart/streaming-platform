use std::net::SocketAddr;
use serde_derive::Deserialize;
use warp::Filter;

#[derive(Debug, Deserialize)]
struct Hub {
    file_name: Option<String>
}

#[derive(Debug, Deserialize)]
struct Service {
    file_name: Option<String>
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
    let toml_str = r#"
        hub_path = "C:\\src\\hubs\\target\\debug"
        service_path = "C:\\src\\services\\target\\debug"

        #hub_path = "test"
        #service_path = "test"

        [server]

        ip = "127.0.0.1"
        port = 80

        [[hubs]]

        file_name = "service-hub.exe"

        [[hubs]]

        file_name = "app-hub.exe"
        
    "#;

    let config: Config = toml::from_str(toml_str).unwrap();
    println!("{:#?}", config);


    std::process::Command::new(config.hub_path.unwrap() + r#"/service-hub.exe"#)
        //.arg("&")
        .spawn()
        .expect("ls command failed to start");


    let routes = warp::path("hello")
    .and(warp::path::param())
    .and(warp::header("user-agent"))
    .map(|param: String, agent: String| {
        format!("Hello {}, whose agent is {}", param, agent)
    });
	
    let addr = "0.0.0.0:49999".parse::<SocketAddr>().unwrap();
    warp::serve(routes).run(addr);
        
}

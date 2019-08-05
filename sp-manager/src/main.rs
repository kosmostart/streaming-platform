use serde_derive::Deserialize;

#[derive(Debug, Deserialize)]
struct PeerConfig {
    ip: Option<String>,
    port: Option<u64>,
}

/// This is what we're going to decode into. Each field is optional, meaning
/// that it doesn't have to be present in TOML.
#[derive(Debug, Deserialize)]
struct Config {
    peers: Option<Vec<PeerConfig>>
}

fn main() {
    let toml_str = r#"
        global_string = "test"
        global_integer = 5

        [server]

        ip = "127.0.0.1"
        port = 80

        [[peers]]

        ip = "127.0.0.1"
        port = 8080

        [[peers]]

        ip = "127.0.0.1"
    "#;

    let cfg: Config = toml::from_str(toml_str).unwrap();
    println!("{:#?}", cfg);

    std::process::Command::new("./service-hub")
        //.arg("&")
        .spawn()
        .expect("ls command failed to start");
}

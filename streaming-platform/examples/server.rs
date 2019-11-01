use std::io::{Cursor, BufReader, Read};
use streaming_platform::{start, Config};

fn main() {
    let config_path = std::env::args().nth(1)
        .expect("path to config file not passed as argument");

    let file = std::fs::File::open(config_path)
        .expect("failed to open config");

    let mut buf_reader = BufReader::new(file);

    let mut config_string = String::new();

    buf_reader.read_to_string(&mut config_string)
        .expect("failed to read config");

    let config: Config = toml::from_str(&config_string)
        .expect("failed to deserialize config");

    start(config);
}

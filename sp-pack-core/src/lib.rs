use std::env;
use std::fs::{File, remove_file};
use std::io::{self, Result};
use std::path::{Path, PathBuf};
use std::io::BufReader;
use std::io::prelude::*;
use rand::{Rng, thread_rng};
use chrono::Utc;
use serde_derive::Deserialize;
use lz4::{Decoder, EncoderBuilder};

#[derive(Debug, Deserialize)]
struct Config {
    result_file_tag: String,
    dirs: Option<Vec<TargetDir>>,
    files: Vec<TargetFile>
}

#[derive(Debug, Deserialize)]
struct TargetDir {
    arch_name: String,
    path: String
}

#[derive(Debug, Deserialize)]
struct TargetFile {
    path: String
}

pub fn pack() {    
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

    println!("{:#?}", config);

    let from = config.result_file_tag.clone() + ".tmp";

    let charset: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let len: usize = 5;
    let mut rng = thread_rng();

    let to: String = (0..len)
        .map(|_| {
            let idx = rng.gen_range(0, charset.len());
            charset[idx] as char
        })
        .collect();

    let to = config.result_file_tag + "." + &to;

    {
        let ar_file = File::create(&from)
            .expect("failed to open archive");

        let mut a = tar::Builder::new(ar_file);

        match config.dirs {
            Some(dirs) => {
                for target_dir in dirs {
                    a.append_dir_all(target_dir.arch_name, target_dir.path)
                        .expect("failed to append target dir to archive");
                }
            }
            None => {}
        }

        for target_file in config.files {
            let path = Path::new(&target_file.path);
            let mut f = File::open(path)
                .expect("failed to open target file");

            a.append_file(path.file_name().expect("failed to get file_name"), &mut f)
                .expect("failed to append target file to archive");
        }
    }    

    compress(&from, &to)
        .expect("compression failed");

    remove_file(&from)
        .expect("failed to remove temporary file");
}

pub fn unpack(save_path: String, file_name: String) {
    let from = save_path.clone() + "/" + &file_name;
    let to = save_path.clone() + "/" + &file_name + ".tmp";
    let dt = Utc::now().format("%Y-%m-%d-%H-%M-%S").to_string();
    let target = save_path.clone() + "/deploy-" + &file_name + "-" + &dt;

    decompress(&from, &to).unwrap();

    {
        let mut ar = tar::Archive::new(File::open(&to).unwrap());

        ar.unpack(&target).unwrap();
    }

    remove_file(&to)
        .expect("failed to remove temporary file");

    println!("unpack {} {} ok", save_path, file_name);
}

fn compress(from: &str, to: &str) -> Result<()> {    
    let mut input_file = File::open(from)?;
    let output_file = File::create(to)?;
    let mut encoder = EncoderBuilder::new()
        .level(4)
        .build(output_file)?;

    io::copy(&mut input_file, &mut encoder)?;

    let (_output, result) = encoder.finish();

    result
}

fn decompress(from: &str, to: &str) -> Result<()> {    
    let input_file = File::open(from)?;
    let mut decoder = Decoder::new(input_file)?;
    let mut output_file = File::create(to)?;

    io::copy(&mut decoder, &mut output_file)?;

    Ok(())
}

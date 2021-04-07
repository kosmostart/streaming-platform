use std::collections::HashMap;
use std::fs::{File, remove_file};
use std::io::{self, Error};
use std::path::Path;
use std::io::BufReader;
use std::io::prelude::*;
use rand::{Rng, thread_rng};
use chrono::Utc;
use serde_derive::{Serialize, Deserialize};
use lz4::{Decoder, EncoderBuilder};

#[derive(Debug, Deserialize)]
pub struct DeployUnitConfig {
    pub result_file_tag: String,
    pub dirs: Option<Vec<TargetDir>>,
    pub files: Option<Vec<TargetFile>>
}

#[derive(Debug, Deserialize)]
pub struct TargetDir {
    pub arch_name: String,
    pub path: String
}

#[derive(Debug, Deserialize)]
pub struct TargetFile {
    pub path: String
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RunConfig {
    pub run_units: Vec<RunUnit>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RunUnit {
    pub path: String,
    pub config: Option<HashMap<String, String>>
}

pub fn pack(config: DeployUnitConfig) -> Result<String, Error> {
    println!("{:#?}", config);

    let from = config.result_file_tag.clone() + ".tmp";

    let charset: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let len: usize = 5;
    let mut rng = thread_rng();

    let to: String = (0..len)
        .map(|_| {
            let idx = rng.gen_range(0..charset.len());
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

        match config.files {
            Some(files) => {
                for target_file in files {
                    let path = Path::new(&target_file.path);
                    let mut f = File::open(path)
                        .expect("failed to open target file");
        
                    a.append_file(path.file_name().expect("failed to get file_name"), &mut f)
                        .expect("failed to append target file to archive");
                }
            }
            None => {}
        }
    }    

    compress(&from, &to)
        .expect("compression failed");

    remove_file(&from)
        .expect("failed to remove temporary file");

    Ok(to)
}

pub fn unpack(save_path: String, file_name: String) -> Result<(), Error> {
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

    println!("unpack {} ok, path is {}", file_name, save_path);

    Ok(())
}

fn compress(from: &str, to: &str) -> Result<(), Error> {    
    let mut input_file = File::open(from)?;
    let output_file = File::create(to)?;
    let mut encoder = EncoderBuilder::new()
        .level(4)
        .build(output_file)?;

    io::copy(&mut input_file, &mut encoder)?;

    let (_output, res) = encoder.finish();

    Ok(res?)
}

fn decompress(from: &str, to: &str) -> Result<(), Error> {    
    let input_file = File::open(from)?;
    let mut decoder = Decoder::new(input_file)?;
    let mut output_file = File::create(to)?;

    io::copy(&mut decoder, &mut output_file)?;

    Ok(())
}
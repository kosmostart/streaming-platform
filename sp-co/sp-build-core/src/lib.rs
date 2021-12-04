use std::fs::{
	File, remove_file
};
use std::io::{
	self, Error
};
use std::path::Path;
use log::*;
use rand::{Rng, thread_rng};
use time::{
	OffsetDateTime, format_description
};
use serde_json::Value;
use serde_derive::{Serialize, Deserialize};
use lz4_flex::frame::{
    FrameEncoder, FrameDecoder
};

#[derive(Debug, Serialize, Deserialize)]
pub struct DeployConfig {
    pub build_configs: Vec<BuildConfig>,
    pub deploy_unit_config: DeployUnitConfig,
    pub run_config: Option<RunConfig>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BuildConfig {
    pub build_name: String,
    pub build_cmd: String,
    pub args: Option<Vec<String>>,
    pub pull_config: Option<PullConfig>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PullConfig {
    pub repository_path: String,
    pub remote_name: String,
    pub remote_branch: String
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeployUnitConfig {
    pub result_file_tag: String,
    pub dirs: Option<Vec<TargetDir>>,
    pub files: Option<Vec<TargetFile>>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TargetDir {
    pub arch_name: String,
    pub path: String
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TargetFile {
    pub path: String
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RunConfig {
    pub run_units: Vec<RunUnit>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RunUnit {
    pub name: String,
	pub path: Option<String>,
    pub config: Option<Value>
}

pub fn pack(config: DeployUnitConfig) -> Result<String, Error> {
	info!("Packing with config:");
    info!("{:#?}", config);

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

pub fn unpack(save_path: String, file_name: String) -> Result<String, Error> {
    let from = save_path.clone() + "/" + &file_name;
    let to = save_path.clone() + "/" + &file_name + ".tmp";
	let format = format_description::parse("[year]-[month]-[day]-[hour]-[minute]-[second]").expect("Failed to create datetime format");
    let dt = OffsetDateTime::now_utc().format(&format).expect("Failed to format datetime");
    let target_path = save_path.clone() + "/deploy-" + &file_name + "-" + &dt;

    decompress(&from, &to).unwrap();

    {
        let mut ar = tar::Archive::new(File::open(&to).unwrap());

        ar.unpack(&target_path).unwrap();
    }

    remove_file(&to)
        .expect("failed to remove temporary file");

    info!("Unpack ok, target path is {}, file name is {}", target_path, file_name);

    Ok(target_path)
}

fn compress(from: &str, to: &str) -> Result<(), Error> {    
    let mut input_file = File::open(from)?;
    let output_file = File::create(to)?;    

    io::copy(&mut input_file, &mut FrameEncoder::new(output_file))?;    

    Ok(())
}

fn decompress(from: &str, to: &str) -> Result<(), Error> {    
    let input_file = File::open(from)?;    
    let mut output_file = File::create(to)?;

    io::copy(&mut FrameDecoder::new(input_file), &mut output_file)?;

    Ok(())
}

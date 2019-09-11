use std::env;
use std::fs::File;
use std::io::{self, Result};
use std::path::{Path, PathBuf};
use lz4::{Decoder, EncoderBuilder};

fn main() {
    println!("LZ4 version: {}", lz4::version());

    let path = Path::new("");

    if let Some("lz4") = path.extension().and_then(|e| e.to_str()) {
        decompress(&path, &path).unwrap();
    } else {
        compress(&path, &path.with_extension("lz4")).unwrap();
    }
}

fn compress(source: &Path, destination: &Path) -> Result<()> {
    println!("Compressing: {} -> {}", source.display(), destination.display());

    let mut input_file = File::open(source)?;
    let output_file = File::create(destination)?;
    let mut encoder = EncoderBuilder::new()
        .level(4)
        .build(output_file)?;
    io::copy(&mut input_file, &mut encoder)?;
    let (_output, result) = encoder.finish();
    result
}

fn decompress(source: &Path, destination: &Path) -> Result<()> {
    println!("Decompressing: {} -> {}", source.display(), destination.display());

    let input_file = File::open(source)?;
    let mut decoder = Decoder::new(input_file)?;
    let mut output_file = File::create(destination)?;
    io::copy(&mut decoder, &mut output_file)?;

    Ok(())
}

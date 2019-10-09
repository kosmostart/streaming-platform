use sp_pack_core::unpack;

fn main() {
    let path = std::env::args().nth(1)
        .expect("path to file not passed as argument");

    unpack(&path);
}

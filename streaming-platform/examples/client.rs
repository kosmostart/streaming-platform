use streaming_platform::{magic_ball, Mode};

fn main() {
    let host = "127.0.0.1:60000";
    let addr = "SuperService";
    let access_key = "";
    let mode = Mode::FullMessage;

    magic_ball(host, addr, access_key, mode);
}

use std::collections::HashMap;
use streaming_platform::{magic_ball, Mode, MagicBall, sp_dto::MsgMeta};

fn main() {
    let host = "127.0.0.1:60000";
    let addr = "SuperService";
    let access_key = "";
    let config = HashMap::new();
    let mode = Mode::FullMessage(process_event, process_rpc_request);

    magic_ball(host, addr, access_key, mode, config);
}

fn process_event(config: &HashMap<String, String>, mb: &mut MagicBall, msg_meta: &MsgMeta, payload: Vec<u8>, attachments: Vec<u8>) {

}
fn process_rpc_request(config: &HashMap<String, String>, mb: &mut MagicBall, msg_meta: &MsgMeta, payload: Vec<u8>, attachments: Vec<u8>) -> (Vec<u8>, Vec<(String, u64)>, Vec<u8>) {
    let payload = vec![];
    let attachments_meta = vec![];
    let attachments_data = vec![];

    (payload, attachments_meta, attachments_data)
}

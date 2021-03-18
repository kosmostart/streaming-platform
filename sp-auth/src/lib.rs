use base64::decode;
use serde_json::{Value, to_vec, from_slice};
use sha3::Sha3_256;
use hmac::{Hmac, Mac, crypto_mac::{NewMac, MacError}};

// Create alias for HMAC-SHA3 256, it's SHA3 friends, umad?
type HmacSha = Hmac<Sha3_256>;

pub fn create_auth_token(auth_token_key: &[u8], payload: &Value) -> Result<Vec<u8>, serde_json::Error> {
    // Create HMAC-SHA3 256 instance which implements `Mac` trait
    let mut mac = HmacSha::new_varkey(auth_token_key).expect("HMAC can take key of any size");
    mac.update(&to_vec(payload)?);
    
    let result = mac.finalize();
    // To get underlying array use `code` method, but be carefull, since
    // incorrect use of the code value may permit timing attacks which defeat
    // the security provided by the `MacResult`
    let code_bytes = result.into_bytes();

    Ok(code_bytes.to_vec())
}

pub fn verify_auth_token(auth_token_key: &[u8], auth_token: &str) -> Result<Value, Error> {
    let split: Vec<&str> = auth_token.split(".").collect();

    if split.len() != 2 {
        return Err(Error::IncorrectSplitLen);
    }

    let hash = decode(split[0])?;
    let payload = decode(split[1])?;

    //To verify the message:
    let mut mac = HmacSha::new_varkey(auth_token_key).expect("HMAC can take key of any size");

    mac.update(&payload);

    // `verify` will return `Ok(())` if code is correct, `Err(MacError)` otherwise
    mac.verify(&hash)?;

    Ok(from_slice(&payload)?)
}

#[derive(Debug)]
pub enum Error {
    SerdeJson(serde_json::Error),
    Mac(MacError),
    Decode(base64::DecodeError),
    IncorrectSplitLen
}

impl From<serde_json::Error> for Error {
	fn from(e: serde_json::Error) -> Error {
		Error::SerdeJson(e)
	}
}

impl From<MacError> for Error {
	fn from(e: MacError) -> Error {
		Error::Mac(e)
	}
}

impl From<base64::DecodeError> for Error {
	fn from(e: base64::DecodeError) -> Error {
		Error::Decode(e)
	}
}

#[test]
fn check_auth_token_length() {
    let res = create_auth_token(b"asdqwe", &json!({
        "user_id": 123321123
    })).expect("Failed to create auth token");

    println!("{}", res.len());
}
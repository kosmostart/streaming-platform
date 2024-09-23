use std::collections::HashMap;
use serde_json::json;
use rkyv::{access, ser::{allocator::ArenaHandle, sharing::Share}, util::AlignedVec, Archive, Deserialize, Portable, Serialize};

pub const POSITION_LEN: usize = 8;

#[derive(Archive, Debug, Deserialize, Serialize)]
// We have a recursive type, which requires some special handling
//
// First the compiler will return an error:
//
// > error[E0275]: overflow evaluating the requirement `HashMap<String,
// > JsonValue>: Archive`
//
// This is because the implementation of Archive for Json value requires that
// JsonValue: Archive, which is recursive!
// We can fix this by adding #[omit_bounds] on the recursive fields. This will
// prevent the derive from automatically adding a `HashMap<String, JsonValue>:
// Archive` bound on the generated impl.
//
// Next, the compiler will return these errors:
//
// > error[E0277]: the trait bound `__S: ScratchSpace` is not satisfied
// > error[E0277]: the trait bound `__S: Serializer` is not satisfied
//
// This is because those bounds are required by HashMap and Vec, but we removed
// the default generated bounds to prevent a recursive impl.
// We can fix this by manually specifying the bounds required by HashMap and Vec
// in an attribute, and then everything will compile:
#[rkyv(serialize_bounds(
    __S: rkyv::ser::Writer + rkyv::ser::Allocator,
    __S::Error: rkyv::rancor::Source,
))]
#[rkyv(deserialize_bounds(__D::Error: rkyv::rancor::Source))]
// We need to manually add the appropriate non-recursive bounds to our
// `CheckBytes` derive. In our case, we need to bound
// `__C: rkyv::validation::ArchiveContext`. This will make sure that our `Vec`
// and `HashMap` have the `ArchiveContext` trait implemented on the validator.
// This is a necessary requirement for containers to check their bytes.
//
// With those two changes, our recursive type can be validated with `access`!
#[rkyv(bytecheck(
    bounds(
        __C: rkyv::validation::ArchiveContext,
    )
))]
pub enum Value {
    Null,
    Bool(bool),
    Number(Number),
    String(String),
    Array(#[rkyv(omit_bounds)] Vec<Value>),
    Object(#[rkyv(omit_bounds)] HashMap<String, Value>)
}

#[derive(Debug, Archive, Deserialize, Serialize)]
pub enum Number {
    PosInt(u64),
    NegInt(i64),
    Float(f64),
}

pub fn deserialize_value(buf: &[u8]) -> Result<Value, Error> {
    let archived = unsafe { rkyv::access_unchecked::<ArchivedValue>(buf) };

    match rkyv::deserialize::<Value, rkyv::rancor::Error>(archived) {
        Ok(res) => Ok(res),
        Err(e) => Err(Error::Rkyv(e))
    }
}

pub fn convert_value(input: serde_json::Value) -> Value {
    match input {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(value) => Value::Bool(value),
        serde_json::Value::Number(value) => {
            if let Some(res) = value.as_u64() {
                return Value::Number(Number::PosInt(res));
            }

            if let Some(res) = value.as_i64() {
                return Value::Number(Number::NegInt(res));
            }

            if let Some(res) = value.as_f64() {
                return Value::Number(Number::Float(res));
            }

            Value::Null
        }
        serde_json::Value::String(value) => Value::String(value),
        serde_json::Value::Array(value) => {
            let mut res = vec![];

            for item in value {
                res.push(convert_value(item));
            }

            Value::Array(res)
        },
        serde_json::Value::Object(value) => {
            let mut res = HashMap::new();

            for (key, item_value) in value {
                res.insert(key, convert_value(item_value));
            }

            Value::Object(res)
        }
    }
}

pub fn convert_value2(input: Value) -> serde_json::Value {
    match input {
        Value::Null => serde_json::Value::Null,
        Value::Bool(value) => serde_json::Value::Bool(value),
        Value::Number(value) => {
            match value {
                Number::PosInt(item) => json!(item),
                Number::NegInt(item) => json!(item),
                Number::Float(item) => json!(item)
            }
        }
        Value::String(value) => serde_json::Value::String(value),
        Value::Array(value) => {
            let mut res = vec![];

            for item in value {
                res.push(convert_value2(item));
            }

            serde_json::Value::Array(res)
        },
        Value::Object(value) => {
            let mut res = serde_json::Map::new();

            for (key, item_value) in value {
                res.insert(key, convert_value2(item_value));
            }

            serde_json::Value::Object(res)
        }
    }
}

#[derive(Debug)]
pub enum Error {
	None,	
	Custom(String),    
    Rkyv(rkyv::rancor::Error)
}

impl Error {
	pub fn custom(e: &str) -> Error {
		Error::Custom(e.to_owned())
	}
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {		
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        "I'm the superhero of errors"
    }    
}

impl From<String> for Error {
	fn from(e: String) -> Error {
		Error::Custom(e)
	}
}

impl From<rkyv::rancor::Error> for Error {
	fn from(e: rkyv::rancor::Error) -> Error {
		Error::Rkyv(e)
	}
}

#[test]
fn full_test() -> Result<(), Error> {
    use log::*;
    use serde_json::json;

    let _ = env_logger::try_init();

    let value = convert_value(json!({
        "id": 123,
        "name": "Dog",
        "payload": {
            "asd": "qwe",
            "qwe": 1,
            "dog": vec![
                json!({
                    "hi": "dde8de8"
                }),
                json!({
                    "hi2": "asda"
                })
            ]
        }
    }));

    info!("{:#?}", value);

    let buf = rkyv::to_bytes::<rkyv::rancor::Error>(&value)?;
    let deserialized: Value = deserialize_value(&buf)?;
    info!("{:#?}", deserialized);

    Ok(())
}

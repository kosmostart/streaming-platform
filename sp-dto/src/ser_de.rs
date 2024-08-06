use std::collections::HashMap;
use serde_json::json;
use rkyv::{
	AlignedVec, Archive, Deserialize, Fallible, Infallible, Serialize, archived_root,
	ser::{
		serializers::{AllocSerializer, CompositeSerializerError, AllocScratchError, SharedSerializeMapError}, Serializer
	}
};

pub const POSITION_LEN: usize = 8;

#[derive(Debug, Archive)]
pub enum Value {
    Null,
    Bool(bool),
    Number(Number),
    String(String),
    Array(#[omit_bounds] Vec<Value>),
    Object(#[omit_bounds] HashMap<String, Value>)
}

impl Serialize<AllocSerializer<1024>> for Value {
    fn serialize(&self, serializer: &mut AllocSerializer<1024>) -> Result<Self::Resolver, CompositeSerializerError<std::convert::Infallible, AllocScratchError, SharedSerializeMapError>> {
        Ok(match self {
            Value::Null => ValueResolver::Null,
            Value::Bool(b) => ValueResolver::Bool(b.serialize(serializer)?),
            Value::Number(n) => ValueResolver::Number(n.serialize(serializer)?),
            Value::String(s) => ValueResolver::String(s.serialize(serializer)?),
            Value::Array(a) => ValueResolver::Array(a.serialize(serializer)?),
            Value::Object(m) => ValueResolver::Object(m.serialize(serializer)?)
        })
    }
}

impl<D: Fallible + ?Sized> Deserialize<Value, D> for ArchivedValue {
    fn deserialize(&self, deserializer: &mut D) -> Result<Value, D::Error> {
        Ok(match self {
            ArchivedValue::Null => Value::Null,
            ArchivedValue::Bool(b) => Value::Bool(b.deserialize(deserializer)?),
            ArchivedValue::Number(n) => Value::Number(n.deserialize(deserializer)?),
            ArchivedValue::String(s) => Value::String(s.deserialize(deserializer)?),
            ArchivedValue::Array(a) => Value::Array(a.deserialize(deserializer)?),
            ArchivedValue::Object(m) => Value::Object(m.deserialize(deserializer)?)
        })
    }
}

#[derive(Debug, Archive, Deserialize, Serialize)]
pub enum Number {
    PosInt(u64),
    NegInt(i64),
    Float(f64),
}

pub fn serialize<T>(value: &T) -> Result<AlignedVec, Error> where 
    T: rkyv::Serialize<rkyv::ser::serializers::CompositeSerializer<rkyv::ser::serializers::AlignedSerializer<rkyv::AlignedVec>, 
    rkyv::ser::serializers::FallbackScratch<rkyv::ser::serializers::HeapScratch<1024>, 
    rkyv::ser::serializers::AllocScratch>, rkyv::ser::serializers::SharedSerializeMap>>
{
    let mut serializer = AllocSerializer::<1024>::default();
    let _ = serializer.serialize_value(value)?;
    let res = serializer.into_serializer().into_inner();

    Ok(res)
}

pub fn deserialize(buf: &[u8]) -> Result<Value, Error> {
    let archived = unsafe { archived_root::<Value>(buf) };

    match archived.deserialize(&mut Infallible) {
        Ok(res) => Ok(res),
        Err(e) => Err(Error::Deserialize)
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
    Serialize(CompositeSerializerError<std::convert::Infallible, AllocScratchError, SharedSerializeMapError>),
    Deserialize
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

impl From<CompositeSerializerError<std::convert::Infallible, AllocScratchError, SharedSerializeMapError>> for Error {
	fn from(e: CompositeSerializerError<std::convert::Infallible, AllocScratchError, SharedSerializeMapError>) -> Error {
		Error::Serialize(e)
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

    let buf = serialize(&value)?;
    let deserialized: Value = deserialize(&buf)?;
    info!("{:#?}", deserialized);

    Ok(())
}

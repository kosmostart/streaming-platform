use std::io::Cursor;
use std::fmt::Debug;
use std::collections::HashMap;
use bytes::{Buf, BufMut};
use serde_derive::{Serialize, Deserialize};
use serde_json::{Value, Error};
use uuid::Uuid;
pub use bytes;
pub use uuid;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct CmpSpec {
    pub addr: String,
    pub tx: String        
}

impl CmpSpec {
    // These methods create spec for child components, note this: tx: self.addr.clone(),
    pub fn new_addr(&self, addr: &str) -> CmpSpec {
        CmpSpec {
            addr: addr.to_owned(),
            tx: self.addr.clone()            
        }
    }
    pub fn add_to_addr(&self, delta: &str) -> CmpSpec {
        CmpSpec {
            addr: self.addr.clone() + "." + delta,
            tx: self.addr.clone()            
        }
    }
}

impl Default for CmpSpec {
    fn default() -> Self {
        CmpSpec {
            addr: String::new(),
            tx: String::new()
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Participator {
    /// Service process addr running in background somewhere
    Service(String),
    /// Command line application
    Cli(String),
    /// UI component, consists of addr, app_addr and client addr
    Component(String, Option<String>, Option<String>)
}

/// At the moment used in case when it is needed to overwrite rpc response receiver
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum RouteSpec {
    /// No rpc reponse receiver overwrite will happen
    Simple,
    /// Rpc reponse receiver overwrite will happen
    Client(Participator)
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Route {
    pub source: Participator,
    pub spec: RouteSpec,
    pub points: Vec<Participator>
}

impl Route {
    pub fn new_cli(addr: &str) -> Route {
        Route {
            source: Participator::Cli(addr.to_owned()),
            spec: RouteSpec::Simple,
            points: vec![Participator::Cli(addr.to_owned())]
        }
    }
    pub fn new_cli_with_service_client(addr: &str, service_addr: &str) -> Route {
        Route {
            source: Participator::Cli(addr.to_owned()),
            spec: RouteSpec::Client(Participator::Service(service_addr.to_owned())),
            points: vec![Participator::Cli(addr.to_owned())]
        }
    }
    pub fn get_source_addr(&self) -> &String {
        match &self.source {
            Participator::Service(addr) => addr,
            Participator::Cli(addr) => addr,
            Participator::Component(addr, _, _) => addr
        }
    }
	pub fn get_last_point_addr(&self) -> Option<&String> {
        match &self.points.last() {
			Some(point) => Some(match point {
					Participator::Service(addr) => addr,
					Participator::Cli(addr) => addr,
					Participator::Component(addr, _, _) => addr
				}),
			None => None
		}
    }
}

/// The message itself
#[derive(Debug, Deserialize, Clone)]
pub struct Message<T> {
    pub meta: MsgMeta,
    pub payload: T,
    pub attachments_data: Option<Vec<u8>>
}

/// The message itself with payload as raw bytes
#[derive(Debug, Deserialize, Clone)]
pub struct MessageRaw {
    pub meta: MsgMeta,
    pub payload: Vec<u8>,
    pub attachments_data: Option<Vec<u8>>
}

/// Enum used for returning from processing rpc functions in full message mode
#[derive(Debug, Serialize, Clone)]
pub enum Response<T> {
    Simple(T),
    Full(T, Vec<(String, u64)>, Vec<u8>)
}


/// Helper function for creating responses.
pub fn resp<T>(payload: T) -> Result<Response<T>, Box<dyn std::error::Error>> {
    Ok(Response::Simple(payload))
}

pub fn resp_full<T>(payload: T, attachments: Vec<(String, u64)>, attachments_data: Vec<u8>) -> Result<Response<T>, Box<dyn std::error::Error>> {
    Ok(Response::Full(payload, attachments, attachments_data))
}

pub fn resp_raw(payload: Vec<u8>) -> Result<ResponseRaw, Box<dyn std::error::Error>> {
    Ok(ResponseRaw::Simple(payload))
}

pub fn resp_raw_full(payload: Vec<u8>, attachments: Vec<(String, u64)>, attachments_data: Vec<u8>) -> Result<ResponseRaw, Box<dyn std::error::Error>> {
    Ok(ResponseRaw::Full(payload, attachments, attachments_data))
}

/// Enum used for returning from processing rpc functions in raw mode
#[derive(Debug, Serialize, Clone)]
pub enum ResponseRaw {
    Simple(Vec<u8>),
    Full(Vec<u8>, Vec<(String, u64)>, Vec<u8>)
}

/// Message routing key. Used for delivering message to subscribers.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct Key {    
    pub action: String,
    pub service: String,
    pub domain: String,
	pub source: Option<String>,
    pub tags: Option<Vec<String>>
}

/// Parameters are as follows: event_subscribes, rpc_subscribes, rpc_response_subscribes
#[derive(Debug, Serialize, Deserialize, Clone)] 
pub struct SubscribeByAddr {
    pub addr: String,
    pub keys: Vec<Key>
}

#[derive(Debug, Serialize, Deserialize, Clone)] 
pub struct SubscribeByKey {
    pub key: Key,
    pub addrs: Vec<String>
}

pub fn new_subscribes_by_addr(data: Vec<(&str, Vec<Key>)>) -> Vec<SubscribeByAddr> {
    data.into_iter().map(|(addr, keys)| SubscribeByAddr { addr: addr.to_owned(), keys }).collect()
}    
pub fn new_subscribes_by_key(data: Vec<(Key, Vec<&str>)>) -> Vec<SubscribeByKey> {
    data.into_iter().map(|(key, addrs)| SubscribeByKey { key, addrs: addrs.into_iter().map(|addr| addr.to_owned()).collect() }).collect()
}

pub fn subscribes_vec_to_keys(subscribes: Vec<SubscribeByAddr>) -> Vec<SubscribeByKey> {
    let mut res: Vec<SubscribeByKey> = vec![];

    for subscribe in subscribes {
        for key in subscribe.keys {
            match res.iter_mut().find(|sub_by_key| sub_by_key.key == key) {
                Some(sub_by_key) => {
                    if !sub_by_key.addrs.iter().any(|addr| addr == &subscribe.addr) {
                        sub_by_key.addrs.push(subscribe.addr.clone());
                    }
                }
                None => {
                    res.push(SubscribeByKey { 
                        key: key.clone(), 
                        addrs: vec![
                            subscribe.addr.clone()
                        ]
                    });
                }
            }           
        }
    }                

    res
}

pub fn subscribes_map_to_keys(subscribes: HashMap<String, Vec<Key>>) -> Vec<SubscribeByKey> {
    let mut res: Vec<SubscribeByKey> = vec![];

    for (sub_addr, sub_keys) in subscribes {
        for key in sub_keys {
            match res.iter_mut().find(|sub_by_key| sub_by_key.key == key) {
                Some(sub_by_key) => {
                    if !sub_by_key.addrs.iter().any(|addr| addr == &sub_addr) {
                        sub_by_key.addrs.push(sub_addr.clone());
                    }
                }
                None => {
                    res.push(SubscribeByKey { 
                        key: key.clone(), 
                        addrs: vec![
                            sub_addr.clone()
                        ]
                    });
                }
            }           
        }
    }                

    res
}

pub fn subscribes_vec_to_map(subscribes: Vec<SubscribeByAddr>) -> HashMap<String, Vec<Key>> {
    let mut res = HashMap::new();

    for sub in subscribes {
        res.insert(sub.addr, sub.keys);
    }

    res
}

impl Key {
    pub fn new(action: &str, service: &str, domain: &str) -> Key {
        Key {
            action: action.to_owned(),
            service: service.to_owned(),
            domain: domain.to_owned(),
			source: None,
            tags: None
        }
    }
	pub fn new_with_source(action: &str, service: &str, domain: &str, source: &str) -> Key {
        Key {
            action: action.to_owned(),
            service: service.to_owned(),
            domain: domain.to_owned(),
			source: Some(source.to_owned()),
            tags: None
        }
    }
    pub fn new_with_tags(action: &str, service: &str, domain: &str, tags: Vec<&str>) -> Key {
        Key {
            action: action.to_owned(),
            service: service.to_owned(),
            domain: domain.to_owned(),
			source: None,
            tags: {
				let mut res = vec![];

				for source in tags {
					res.push(source.to_owned())
				}

				Some(res)
			}
        }
    }
    pub fn simple(action: &str) -> Key {
        Key {
            action: action.to_owned(),
            service: "".to_owned(),
            domain: "".to_owned(),
			source: None,
            tags: None
        }
    }
	pub fn simple_with_source(action: &str, source: &str) -> Key {
        Key {
            action: action.to_owned(),
            service: "".to_owned(),
            domain: "".to_owned(),
			source: Some(source.to_owned()),
            tags: None
        }
    }
	pub fn simple_with_tags(action: &str, tags: Vec<&str>) -> Key {
        Key {
            action: action.to_owned(),
            service: "".to_owned(),
            domain: "".to_owned(),
			source: None,
            tags: {
				let mut res = vec![];

				for source in tags {
					res.push(source.to_owned())
				}

				Some(res)
			}
        }
    }
}

/// Message meta data. Message passing protocol is build around this structure.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MsgMeta {
    /// Addr of message sender
    pub tx: String,    
    /// Logic key for message processing
    pub key: Key,
    /// Defines what msg_type of message it is
    pub msg_type: MsgType,
    /// Correlation id is needed for rpc and for message chains
    pub correlation_id: Uuid,
    /// Logical message route, receiver are responsible for moving message on.
    pub route: Route,
    /// Size of payload, used for deserialization. Also useful for monitoring.
    pub payload_size: u64,
    /// Authorization token.
    pub auth_token: Option<String>,
    /// Authorization data.
    pub auth_data: Option<Value>,
    /// Attachments to message
	pub attachments: Vec<AttachmentMeta>
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum MsgType {
    Event,
    RpcRequest,
    RpcResponse(RpcResult),
    ServerRpcRequest,
    ServerRpcResponse(RpcResult)
}

impl MsgType {
    pub fn get_u8(&self) -> u8 {
        match self {
            MsgType::Event => 0,
            MsgType::RpcRequest => 1,
            MsgType::RpcResponse(rpc_result) => {
                match rpc_result {
                    RpcResult::Ok => 2,
                    RpcResult::Err => 3
                }
            },
            MsgType::ServerRpcRequest => 4,
            MsgType::ServerRpcResponse(rpc_result) => {
                match rpc_result {
                    RpcResult::Ok => 5,
                    RpcResult::Err => 6
                }
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum RpcResult {
    Ok,
    Err
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AttachmentMeta {
	pub name: String,
    pub size: u64
}

impl MsgMeta {
    /// Payload plus attachments len.
    pub fn content_len(&self) -> u64 {
        let mut len = self.payload_size;
        for attachment in &self.attachments {
            len = len + attachment.size;
        }
        len
    }
    /// Attachments len.
    pub fn attachments_len(&self) -> u64 {
        let mut len = 0;
        for attachment in &self.attachments {
            len = len + attachment.size;
        }
        len
    }
    /// Attachments sizes.
    pub fn attachments_sizes(&self) -> Vec<u64> {
        let mut res = vec![];
        for attachment in &self.attachments {
            res.push(attachment.size);
        }
        res        
    }
    /// Short display of message meta data
    pub fn display(&self) -> String {
        format!("{}, {:#?} {:?}", self.tx, self.key, self.msg_type)
    }
    /// Get key part, index is zero based, . is used as a separator.
    pub fn action_part(&self, index: usize) -> Result<&str, String> {
        let split: Vec<&str> = self.key.action.split(".").collect();

        if index >= split.len() {
            return Err("index equals or superior to parts length".to_owned());
        }

        return Ok(split[index])
    }
    /// Compares key part with passed value, index is zero based, . is used as a separator.
    pub fn match_action_part(&self, index: usize, value: &str) -> Result<bool, String> {
        let split: Vec<&str> = self.key.action.split(".").collect();

        if index >= split.len() {
            return Err("index equals or superior to parts length".to_owned());
        }

        return Ok(split[index] == value)
    }
    /// Get tx part, index is zero based, . is used as a separator.
    pub fn tx_part(&self, index: usize) -> Result<&str, String> {
        let split: Vec<&str> = self.tx.split(".").collect();

        if index >= split.len() {
            return Err("index equals or superior to parts length".to_owned());
        }

        return Ok(split[index])
    }
    /// Compares tx part with passed value, index is zero based, . is used as a separator.
    pub fn match_tx_part(&self, index: usize, value: &str) -> Result<bool, String> {
        let split: Vec<&str> = self.tx.split(".").collect();

        if index >= split.len() {
            return Err("index equals or superior to parts length".to_owned());
        }

        return Ok(split[index] == value)
    }    
    pub fn source_cmp_addr(&self) -> Option<&str> {
        match &self.route.source {
            Participator::Component(addr, _, _) => Some(&addr),
            _ => None
        }
    }
    /// Get source cmp part, index is zero based, . is used as a separator.
    pub fn source_cmp_part(&self, index: usize) -> Result<&str, String> {
        let addr = self.source_cmp_addr().ok_or("Not a cmp source".to_owned())?;
        let split: Vec<&str> = addr.split(".").collect();

        if index >= split.len() {
            return Err("index equals or superior to parts length".to_owned());
        }

        return Ok(split[index])
    }
    /// Compares source cmp part with passed value, index is zero based, . is used as a separator.
    pub fn match_source_cmp_part(&self, index: usize, value: &str) -> Result<bool, String> {
        let addr = self.source_cmp_addr().ok_or("Not a cmp source".to_owned())?;
        let split: Vec<&str> = addr.split(".").collect();

        if index >= split.len() {
            return Err("index equals or superior to parts length".to_owned());
        }

        return Ok(split[index] == value)
    }
    /// Get source cmp part before last one, . is used as a separator.
    pub fn source_cmp_part_before_last(&self) -> Result<&str, String> {
        let addr = self.source_cmp_addr().ok_or("Not a cmp source".to_owned())?;
        let split: Vec<&str> = addr.split(".").collect();

        if split.len() < 2 {
            return Err("parts length is less than 2".to_owned());
        }

        return Ok(split[split.len() - 2])
    }
    pub fn source_svc_addr(&self) -> Option<String> {
        match &self.route.source {
            Participator::Service(addr) => Some(addr.clone()),
            _ => None
        }
    }
    pub fn client_cmp_addr(&self) -> Option<String> {
        match &self.route.spec {
            RouteSpec::Client(participator) => {
                match participator {
                    Participator::Component(addr, _, _) => Some(addr.clone()),
                    _ => None
                }
            }
            _ => None
        }
    }
    pub fn client_svc_addr(&self) -> Option<String> {
        match &self.route.spec {
            RouteSpec::Client(participator) => {
                match participator {
                    Participator::Service(addr) => Some(addr.clone()),
                    _ => None
                }
            }
            _ => None
        }
    }
}

pub fn event_dto<T>(tx: String, key: Key, payload: T, route: Route, auth_token: Option<String>, auth_data: Option<Value>) -> Result<Vec<u8>, Error> where T: Debug, T: serde::Serialize {
    let mut payload = serde_json::to_vec(&payload)?;
    let correlation_id = Uuid::new_v4();

    let msg_meta = MsgMeta {
        tx,        
        key,
        msg_type: MsgType::Event,
        correlation_id,
        route,
        payload_size: payload.len() as u64,
        auth_token,
        auth_data,
		attachments: vec![]
    };

    let mut msg_meta = serde_json::to_vec(&msg_meta)?;    

    let mut buf = vec![];

    buf.put_u32(msg_meta.len() as u32);

    buf.append(&mut msg_meta);
    buf.append(&mut payload);
    
    Ok(buf)
}

pub fn event_dto_with_sizes<T>(tx: String, key: Key, payload: T, route: Route, auth_token: Option<String>, auth_data: Option<Value>) -> Result<(Uuid, Vec<u8>, u64, u64, Vec<u64>), Error> where T: Debug, T: serde::Serialize {
    let mut payload = serde_json::to_vec(&payload)?;
    let correlation_id = Uuid::new_v4();
    let msg_meta = MsgMeta {
        tx,        
        key,
        msg_type: MsgType::Event,
        correlation_id,
        route,
        payload_size: payload.len() as u64,
        auth_token,
        auth_data,
		attachments: vec![]
    };
    let payload_size = msg_meta.payload_size;
    let attachments_sizes = msg_meta.attachments_sizes();
    let mut msg_meta = serde_json::to_vec(&msg_meta)?;    
    let msg_meta_size = msg_meta.len() as u64;
    let mut buf = vec![];
    buf.put_u32(msg_meta.len() as u32);
    buf.append(&mut msg_meta);
    buf.append(&mut payload);    
    Ok((correlation_id, buf, msg_meta_size, payload_size, attachments_sizes))
}

pub fn rpc_response_dto<T>(tx: String, key: Key, correlation_id: Uuid, payload: T, result: RpcResult, route: Route, auth_token: Option<String>, auth_data: Option<Value>) -> Result<Vec<u8>, Error> where T: Debug, T: serde::Serialize {
    let mut payload = serde_json::to_vec(&payload)?;

    let msg_meta = MsgMeta {
        tx,        
        key,
        msg_type: MsgType::RpcResponse(result),
        correlation_id,
        route,
        payload_size: payload.len() as u64,
        auth_token,
        auth_data,
		attachments: vec![]
    };        

    let mut msg_meta = serde_json::to_vec(&msg_meta)?;    

    let mut buf = vec![];

    buf.put_u32(msg_meta.len() as u32);

    buf.append(&mut msg_meta);
    buf.append(&mut payload);
  
    Ok(buf)
}

pub fn rpc_dto<T>(tx: String, key: Key, payload: T, route: Route, auth_token: Option<String>, auth_data: Option<Value>) -> Result<(Uuid, Vec<u8>), Error> where T: Debug, T: serde::Serialize {
    let mut payload = serde_json::to_vec(&payload)?;
    let correlation_id = Uuid::new_v4();

    let msg_meta = MsgMeta {
        tx,        
        key,
        msg_type: MsgType::RpcRequest,
        correlation_id,
        route,
        payload_size: payload.len() as u64,
        auth_token,
        auth_data,
		attachments: vec![]
    };
    
    let mut msg_meta = serde_json::to_vec(&msg_meta)?;

    let mut buf = vec![];

    buf.put_u32(msg_meta.len() as u32);

    buf.append(&mut msg_meta);
    buf.append(&mut payload);

    Ok((correlation_id, buf))
}

pub fn rpc_dto_no_payload(msg_meta: &MsgMeta) -> Result<(u32, Vec<u8>), Error> {        
    let mut msg_meta = serde_json::to_vec(msg_meta)?;
	let len = msg_meta.len() as u32;

    let mut buf = vec![];	

    buf.put_u32(len);

    buf.append(&mut msg_meta);    

    Ok((len, buf))
}

pub fn rpc_dto_with_sizes<T>(tx: String, key: Key, payload: T, route: Route, auth_token: Option<String>, auth_data: Option<Value>) -> Result<(Uuid, Vec<u8>, u64, u64, Vec<u64>), Error> where T: Debug, T: serde::Serialize {
    let mut payload = serde_json::to_vec(&payload)?;
    let correlation_id = Uuid::new_v4();
    let msg_meta = MsgMeta {
        tx,        
        key,
        msg_type: MsgType::RpcRequest,
        correlation_id,
        route,
        payload_size: payload.len() as u64,
        auth_token,
        auth_data,
		attachments: vec![]
    };
    let payload_size = msg_meta.payload_size;
    let attachments_sizes = msg_meta.attachments_sizes();
    let mut msg_meta = serde_json::to_vec(&msg_meta)?;
    let msg_meta_size = msg_meta.len() as u64;
    let mut buf = vec![];
    buf.put_u32(msg_meta.len() as u32);
    buf.append(&mut msg_meta);
    buf.append(&mut payload);
    Ok((correlation_id, buf, msg_meta_size, payload_size, attachments_sizes))
}

pub fn rpc_dto_with_attachments<T>(tx: String, key: Key, payload: T, attachments: Vec<(String, Vec<u8>)>, route: Route, auth_token: Option<String>, auth_data: Option<Value>) -> Result<(Uuid, Vec<u8>), Error> where T: Debug, T: serde::Serialize {
    let mut payload = serde_json::to_vec(&payload)?;
    let correlation_id = Uuid::new_v4();
    let mut attachments_meta = vec![];
    let mut attachments_payload = vec![];

    for (attachment_name, mut attachment_payload) in attachments {
        attachments_meta.push(AttachmentMeta {
            name: attachment_name,
            size: attachment_payload.len() as u64
        });        
        attachments_payload.append(&mut attachment_payload);
    }

    let msg_meta = MsgMeta {
        tx,        
        key,
        msg_type: MsgType::RpcRequest,
        correlation_id,
        route,
        payload_size: payload.len() as u64,
        auth_token,
        auth_data,
		attachments: attachments_meta
    };

    let mut msg_meta = serde_json::to_vec(&msg_meta)?;    

    let mut buf = vec![];

    buf.put_u32(msg_meta.len() as u32);

    buf.append(&mut msg_meta);
    buf.append(&mut payload);
    buf.append(&mut attachments_payload);

    Ok((correlation_id, buf))
}

pub fn event_dto2(tx: String, key: Key, mut payload: Vec<u8>, route: Route, auth_token: Option<String>, auth_data: Option<Value>) -> Result<(Uuid, Vec<u8>), Error> {        
    let correlation_id = Uuid::new_v4();
    
    let msg_meta = MsgMeta {
        tx,        
        key,
        msg_type: MsgType::Event,
        correlation_id,
        route,
        payload_size: payload.len() as u64,
        auth_token,
        auth_data,
		attachments: vec![]
    };

    let mut msg_meta = serde_json::to_vec(&msg_meta)?;        

    let mut buf = vec![];

    buf.put_u32(msg_meta.len() as u32);

    buf.append(&mut msg_meta);
    buf.append(&mut payload);
    
    Ok((correlation_id, buf))
}

pub fn rpc_response_dto_sizes<T>(tx: String, key: Key, correlation_id: Uuid, payload: T, attachments: Vec<(String, u64)>, mut attachments_data: Vec<u8>, result: RpcResult, route: Route, auth_token: Option<String>, auth_data: Option<Value>) -> Result<(Vec<u8>, u64, u64, Vec<u64>), Error> where T: Debug, T: serde::Serialize {
    let mut payload = serde_json::to_vec(&payload)?;
    let mut attachments_meta = vec![];
    for (attachment_name,attachment_size) in attachments {
        attachments_meta.push(AttachmentMeta {
            name: attachment_name,
            size: attachment_size
        });                
    }
    let msg_meta = MsgMeta {
        tx,        
        key,
        msg_type: MsgType::RpcResponse(result),
        correlation_id,
        route,
        payload_size: payload.len() as u64,
        auth_token,
        auth_data,
		attachments: attachments_meta
    };
    let payload_size = msg_meta.payload_size;
    let attachments_sizes = msg_meta.attachments_sizes();
    let mut msg_meta = serde_json::to_vec(&msg_meta)?;
    let msg_meta_size = msg_meta.len() as u64;     
    let mut buf = vec![];
    buf.put_u32(msg_meta.len() as u32);
    buf.append(&mut msg_meta);
    buf.append(&mut payload);
    buf.append(&mut attachments_data);    
    Ok((buf, msg_meta_size, payload_size, attachments_sizes))
}

pub fn rpc_response_dto2_sizes(tx: String, key: Key, correlation_id: Uuid, mut payload: Vec<u8>, attachments: Vec<(String, u64)>, mut attachments_data: Vec<u8>, result: RpcResult, route: Route, auth_token: Option<String>, auth_data: Option<Value>) -> Result<(Vec<u8>, u64, u64, Vec<u64>), Error> {
    let mut attachments_meta = vec![];
    for (attachment_name,attachment_size) in attachments {
        attachments_meta.push(AttachmentMeta {
            name: attachment_name,
            size: attachment_size
        });                
    }
    let msg_meta = MsgMeta {
        tx,        
        key,
        msg_type: MsgType::RpcResponse(result),
        correlation_id,
        route,
        payload_size: payload.len() as u64,
        auth_token,
        auth_data,
		attachments: attachments_meta
    };
    let payload_size = msg_meta.payload_size;
    let attachments_sizes = msg_meta.attachments_sizes();
    let mut msg_meta = serde_json::to_vec(&msg_meta)?;
    let msg_meta_size = msg_meta.len() as u64;     
    let mut buf = vec![];
    buf.put_u32(msg_meta.len() as u32);
    buf.append(&mut msg_meta);
    buf.append(&mut payload);
    buf.append(&mut attachments_data);    
    Ok((buf, msg_meta_size, payload_size, attachments_sizes))
}

pub fn rpc_dto2(tx: String, key: Key, mut payload: Vec<u8>, route: Route, auth_token: Option<String>, auth_data: Option<Value>) -> Result<(Uuid, Vec<u8>), Error> {
    let correlation_id = Uuid::new_v4();

    let msg_meta = MsgMeta {
        tx,        
        key,
        msg_type: MsgType::RpcRequest,
        correlation_id,
        route,
        payload_size: payload.len() as u64,
        auth_token,
        auth_data,
		attachments: vec![]
    };

    let mut msg_meta = serde_json::to_vec(&msg_meta)?;

    let mut buf = vec![];

    buf.put_u32(msg_meta.len() as u32);

    buf.append(&mut msg_meta);
    buf.append(&mut payload);

    Ok((correlation_id, buf))
}

pub fn rpc_dto_with_attachments2(tx: String, key: Key, mut payload: Vec<u8>, attachments: Vec<(String, Vec<u8>)>, route: Route, auth_token: Option<String>, auth_data: Option<Value>) -> Result<(Uuid, Vec<u8>), Error> {
    let correlation_id = Uuid::new_v4();
    let mut attachments_meta = vec![];
    let mut attachments_payload = vec![];

    for (attachment_name, mut attachment_payload) in attachments {
        attachments_meta.push(AttachmentMeta {
            name: attachment_name,
            size: attachment_payload.len() as u64
        });        
        attachments_payload.append(&mut attachment_payload);
    }

    let msg_meta = MsgMeta {
        tx,        
        key,
        msg_type: MsgType::RpcRequest,
        correlation_id,
        route,
        payload_size: payload.len() as u64,
        auth_token,
        auth_data,
		attachments: attachments_meta
    };

    let mut msg_meta = serde_json::to_vec(&msg_meta)?;    

    let mut buf = vec![];

    buf.put_u32(msg_meta.len() as u32);

    buf.append(&mut msg_meta);
    buf.append(&mut payload);
    buf.append(&mut attachments_payload);

    Ok((correlation_id, buf))
}

pub fn raw_dto(msg_meta: MsgMeta, mut payload_data: Vec<u8>, mut attachments_data: Option<Vec<u8>>) -> Result<Vec<u8>, Error> {
	let mut msg_meta = serde_json::to_vec(&msg_meta)?;    

    let mut buf = vec![];

    buf.put_u32(msg_meta.len() as u32);

    buf.append(&mut msg_meta);
    buf.append(&mut payload_data);

	match attachments_data {
		Some(ref mut attachments_data) => {
			buf.append(attachments_data);
		}
		None => {}
	}    

	Ok(buf)
}

pub fn get_msg_len(data: &[u8]) -> u32 {
    let mut buf = Cursor::new(data);
    buf.get_u32()
}

pub fn get_msg_meta(data: &[u8]) -> Result<MsgMeta, Error> {
    let mut buf = Cursor::new(data);
    let len = buf.get_u32() as usize;

    serde_json::from_slice::<MsgMeta>(&data[4..len + 4])
}

pub fn get_msg_meta_with_len(data: &[u8], len: usize) -> Result<MsgMeta, Error> {    
    serde_json::from_slice::<MsgMeta>(&data[4..len + 4])
}

pub fn get_msg<T>(data: &[u8]) -> Result<Message<T>, Error> where T: Debug, T: serde::Serialize, for<'de> T: serde::Deserialize<'de> {
    let mut buf = Cursor::new(data);    
    let len = buf.get_u32();
    let msg_meta_offset = (len + 4) as usize;

    let msg_meta = serde_json::from_slice::<MsgMeta>(&data[4..msg_meta_offset as usize])?;

    let payload_offset = msg_meta_offset + msg_meta.payload_size as usize;
    let payload = serde_json::from_slice::<T>(&data[msg_meta_offset..payload_offset])?;

    Ok(match len > payload_offset as u32 {
        true => Message {
            meta: msg_meta,
            payload,
            attachments_data: None
        },
        false => Message {
            meta: msg_meta,
            payload,
            attachments_data: Some(data[payload_offset ..].to_owned())
        }
    })
}

pub fn get_msg_meta_and_payload<T>(data: &[u8]) -> Result<(MsgMeta, T), Error> where T: Debug, T: serde::Serialize, for<'de> T: serde::Deserialize<'de> {
    let mut buf = Cursor::new(data);    
    let len = buf.get_u32();
    let msg_meta_offset = (len + 4) as usize;

    let msg_meta = serde_json::from_slice::<MsgMeta>(&data[4..msg_meta_offset as usize])?;

    let payload_offset = msg_meta_offset + msg_meta.payload_size as usize;

    let payload = serde_json::from_slice::<T>(&data[msg_meta_offset..payload_offset])?;    

    Ok((msg_meta, payload))
}

pub fn get_payload<T>(msg_meta: &MsgMeta, data: &[u8]) -> Result<T, Error> where T: Debug, T: serde::Serialize, for<'de> T: serde::Deserialize<'de> {
    let mut buf = Cursor::new(data);    
    let len = buf.get_u32();
    let msg_meta_offset = (len + 4) as usize;    

    let payload_offset = msg_meta_offset + msg_meta.payload_size as usize;

    let payload = serde_json::from_slice::<T>(&data[msg_meta_offset..payload_offset])?;    

    Ok(payload)
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum QueryPrm {
	EqualsI64(i64),
	EqualsU64(u64),
	LessThanU64(u64),
    GreaterThanU64(u64),
	EqualsString(String),
	IdList(Vec<u64>)
}

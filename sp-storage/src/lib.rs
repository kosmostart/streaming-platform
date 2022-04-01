use std::convert::TryInto;
use std::collections::HashMap;
use std::ops::Index;
use log::*;
use time::OffsetDateTime;
use serde_json::{
	json, Value
};
use sled::{
	Db, Tree, transaction::TransactionalTree
};
use sp_dto::QueryPrm;
use ser_de::{
	serialize, deserialize, convert_value, convert_value2
};
use error::Error;
pub use rkyv;
pub use sled;

pub mod error;
pub mod ser_de;

fn dog(barry: &[u8]) -> Result<[u8; 8], Error> {
    Ok(barry.try_into()?)
}

#[derive(Clone)]
pub struct Dc {
    storage_path: String,
    path: String,
    user_id: u64,
    pub id: Option<u64>,
    pub payload: Option<Value>,
    db: Db,
    tree: Tree
}

pub struct TxDc<'a> {    
    path: String,
    user_id: u64,    
    db: &'a Db,
    tree: &'a TransactionalTree
}

#[derive(Clone)]
pub struct Sc {
    pub name: String,
    pub service: String,
    pub domain: String,    
    pub files_path: Option<String>,
    pub region_id: u64,
    pub scope_id: u64,
    pub service_id: u64,
    pub user_id: u64,
    pub tokens: HashMap<String, Dc>
}

impl Index<&str> for Sc {
    type Output = Dc;

    fn index(&self, key: &str) -> &Dc {
        self.tokens.get(key).expect("Token not found")
    }
}

impl Sc {
    pub fn new(user_id: u64, region_id: u64, scope_id: u64, service_id: u64, files_path: Option<String>, name: &str, service: &str, domain: &str, tokens: HashMap<String, Dc>) -> Result<Sc, Error> {

        Ok(Sc {
            name: name.to_owned(),
            service: service.to_owned(),
            domain: domain.to_owned(),            
            files_path: files_path.map(|a| a + "/scope-" + &scope_id.to_string()),
            region_id,
            scope_id,
            service_id,
            user_id,
            tokens
        })
    }    
}

impl Dc {
    pub fn new(path: String, tree_name: &str, user_id: u64, storage_path: &str, id: Option<u64>, payload: Option<Value>) -> Result<Dc, Error> {
        let storage_path = storage_path.to_owned();
        
        let db = sled::open(&path)?;
        let tree = db.open_tree(&tree_name)?;

        Ok(Dc {
            storage_path,
            path,
            user_id,
            id,
            payload,
            db,
            tree
        })
    }
    
    pub fn tx(&self, transaction_body: fn(TxDc) -> Result<(), Error>) -> Result<(), Error> {
        let res = self.tree.transaction(|tx_tree| {
            let tx_dc = TxDc {
                path: self.path.clone(),
                user_id: self.user_id,                
                db: &self.db,
                tree: tx_tree
            };

            match transaction_body(tx_dc) {
                Ok(()) => Ok(()),
                Err(e) => Err(sled::transaction::ConflictableTransactionError::Abort(format!("{:#?}",e)))
            }
        });

        match res {
            Ok(()) => Ok(()),
            Err(e) => Err(Error::SledTransactionDc(e))
        }        
    }    

    pub fn create(&self, mut payload: Value) -> Result<u64, Error> {
        let id = self.db.generate_id()?;

        payload["id"] = json!(id);
        payload["created_at"] = json!(OffsetDateTime::now_utc());
        payload["author"] = json!(self.user_id);
        payload["deactivated_at"] = json!(None as Option<OffsetDateTime>);
        payload["updated_at"] = json!(None as Option<OffsetDateTime>);

        let _ = self.tree.insert(id.to_be_bytes(), &serialize(&convert_value(payload))?[..])?;
    
        Ok(id)
    }

    pub fn create_with_payload_return(&self, mut payload: Value) -> Result<(u64, Value), Error> {
        let id = self.db.generate_id()?;

        payload["id"] = json!(id);
        payload["created_at"] = json!(OffsetDateTime::now_utc());
        payload["author"] = json!(self.user_id);
        payload["deactivated_at"] = json!(None as Option<OffsetDateTime>);
        payload["updated_at"] = json!(None as Option<OffsetDateTime>);

        let converted_payload = convert_value(payload);

        let _ = self.tree.insert(id.to_be_bytes(), &serialize(&converted_payload)?[..])?;

        let res = convert_value2(converted_payload);
    
        Ok((id, res))
    }

    pub fn get_all(&self) -> Result<Vec<(u64, Value)>, Error> {
        let mut res = vec![];

        for pair in self.tree.iter() {
            let (id, bytes) = pair?;

            let payload = deserialize(&bytes)?;

            res.push((u64::from_be_bytes(dog(&id)?), convert_value2(payload)));
        }

        Ok(res)
    }

    pub fn get(&self, id: u64) -> Result<Option<Value>, Error> {
        Ok(match self.tree.get(id.to_be_bytes())? {
            Some(bytes) => {
                let payload = deserialize(&bytes)?;                
                Some(convert_value2(payload))
            }
            None => None
        })
    }

    pub fn update(&self, id: u64, mut payload: Value) -> Result<u64, Error> {
        let id_bytes = id.to_be_bytes();        

        match self.tree.contains_key(id_bytes)? {
            true => {                    
                payload["updated_at"] = json!(OffsetDateTime::now_utc());                    
                self.tree.insert(id_bytes.to_vec(), &serialize(&convert_value(payload))?[..])?;
            }
            false => {
                info!("Entity with id {} not found, path {}", id, self.path);
            }
        }
    
        Ok(id)
    }

    pub fn find(&self, condition: impl Fn(&Value) -> bool) -> Result<Option<(u64, Value)>, Error> {
        for pair in self.tree.iter() {
            let (id, bytes) = pair?;

            let payload = deserialize(&bytes)?;
            let payload = convert_value2(payload);

            if condition(&payload) {
                return Ok(Some((u64::from_be_bytes(dog(&id)?), payload)));
            }
        }

        Ok(None)
    }

    pub fn filter(&self, condition: impl Fn(&Value) -> bool) -> Result<Vec<(u64, Value)>, Error> {
        let mut res = vec![];

        for pair in self.tree.iter() {
            let (id, bytes) = pair?;

            let payload = deserialize(&bytes)?;
            let payload = convert_value2(payload);

            if condition(&payload) {
                res.push((u64::from_be_bytes(dog(&id)?), payload));
            }
        }

        Ok(res)
    }

    pub fn count(&self, condition: impl Fn(&Value) -> bool) -> Result<u64, Error> {
        let mut res = 0;

        for pair in self.tree.iter() {
            let (_id, bytes) = pair?;

            let payload = deserialize(&bytes)?;
            let payload = convert_value2(payload);

            if condition(&payload) {
                res = res + 1;
            }
        }

        Ok(res)
    }

    pub fn find_prm(&self, prm: Vec<(String, QueryPrm)>) -> Result<Option<(u64, Value)>, Error> {
        for pair in self.tree.iter() {
            let (id, bytes) = pair?;            

            let payload = deserialize(&bytes)?;
            let payload = convert_value2(payload);

            let mut check_sum = 0;

            for (key, value) in &prm {
                match value {
                    QueryPrm::EqualsI64(value) => {
                        match payload[key].as_i64() {
                            Some(field_value) => {
                                if field_value == *value {
                                    check_sum = check_sum + 1;
                                }
                            }
                            None => {}
                        }
                    }
                    QueryPrm::EqualsU64(value) => {
                        match payload[key].as_u64() {
                            Some(field_value) => {
                                if field_value == *value {
                                    check_sum = check_sum + 1;
                                }
                            }
                            None => {}
                        }
                    }
                    QueryPrm::LessThanU64(value) => {
                        match payload[key].as_u64() {
                            Some(field_value) => {
                                if field_value < *value {
                                    check_sum = check_sum + 1;
                                }
                            }
                            None => {}
                        }
                    }
                    QueryPrm::GreaterThanU64(value) => {
                        match payload[key].as_u64() {
                            Some(field_value) => {
                                if field_value > *value {
                                    check_sum = check_sum + 1;
                                }                         
                            }
                            None => {}
                        }
                    }
                    QueryPrm::EqualsString(value) => {
                        match payload[key].as_str() {
                            Some(field_value) => {
                                if field_value == value {
                                    check_sum = check_sum + 1;
                                }
                            }
                            None => {}
                        }
                    }
                    _ => {}
                }
            }

            if check_sum == prm.len() {
                return Ok(Some((u64::from_be_bytes(dog(&id)?), payload)));
            }
        }

        Ok(None)
    }        

    pub fn filter_prm(&self, prm: Vec<(String, QueryPrm)>) -> Result<Vec<(u64, Value)>, Error> {
        let mut res = vec![];

        for pair in self.tree.iter() {
            let (id, bytes) = pair?;

            let payload = deserialize(&bytes)?;
            let payload = convert_value2(payload);

            let mut check_sum = 0;

            for (key, value) in &prm {
                match value {
                    QueryPrm::EqualsI64(value) => {
                        match payload[key].as_i64() {
                            Some(field_value) => {
                                if field_value == *value {
                                    check_sum = check_sum + 1;
                                }
                            }
                            None => {}
                        }
                    }
                    QueryPrm::EqualsU64(value) => {
                        match payload[key].as_u64() {
                            Some(field_value) => {
                                if field_value == *value {
                                    check_sum = check_sum + 1;
                                }
                            }
                            None => {}
                        }
                    }
                    QueryPrm::LessThanU64(value) => {
                        match payload[key].as_u64() {
                            Some(field_value) => {
                                if field_value < *value {
                                    check_sum = check_sum + 1;
                                }                            
                            }
                            None => {}
                        }
                    }
                    QueryPrm::GreaterThanU64(value) => {
                        match payload[key].as_u64() {
                            Some(field_value) => {
                                if field_value > *value {
                                    check_sum = check_sum + 1;
                                }                            
                            }
                            None => {}
                        }
                    }
                    QueryPrm::EqualsString(value) => {
                        match payload[key].as_str() {
                            Some(field_value) => {
                                if field_value == value {
                                    check_sum = check_sum + 1;
                                }
                            }
                            None => {}
                        }
                    }
                    _ => {}
                }
            }

            if check_sum == prm.len() {
                res.push((u64::from_be_bytes(dog(&id)?), payload));
            }
        }

        Ok(res)
    }    

    pub fn count_prm(&self, prm: Vec<(String, QueryPrm)>) -> Result<u64, Error> {
        let mut res = 0;

        for pair in self.tree.iter() {
            let (_id, bytes) = pair?;

            let payload = deserialize(&bytes)?;
            let payload = convert_value2(payload);

            let mut check_sum = 0;

            for (key, value) in &prm {
                match value {
                    QueryPrm::EqualsI64(value) => {
                        match payload[key].as_i64() {
                            Some(field_value) => {
                                if field_value == *value {
                                    check_sum = check_sum + 1;
                                }
                            }
                            None => {}
                        }
                    }
                    QueryPrm::EqualsU64(value) => {
                        match payload[key].as_u64() {
                            Some(field_value) => {
                                if field_value == *value {
                                    check_sum = check_sum + 1;
                                }
                            }
                            None => {}
                        }
                    }
                    QueryPrm::LessThanU64(value) => {
                        match payload[key].as_u64() {
                            Some(field_value) => {
                                if field_value < *value {
                                    check_sum = check_sum + 1;
                                }
                            }
                            None => {}
                        }
                    }
                    QueryPrm::GreaterThanU64(value) => {
                        match payload[key].as_u64() {
                            Some(field_value) => {
                                if field_value > *value {
                                    check_sum = check_sum + 1;
                                }                         
                            }
                            None => {}
                        }
                    }
                    QueryPrm::EqualsString(value) => {
                        match payload[key].as_str() {
                            Some(field_value) => {
                                if field_value == value {
                                    check_sum = check_sum + 1;
                                }
                            }
                            None => {}
                        }
                    }
                    _ => {}
                }
            }

            if check_sum == prm.len() {
                res = res + 1;
            }
        }

        Ok(res)
    }
}

impl TxDc<'_> {
    pub fn create(&self, mut payload: Value) -> Result<u64, Error> {
        let id = self.db.generate_id()?;

        payload["id"] = json!(id);
        payload["created_at"] = json!(OffsetDateTime::now_utc());
        payload["author"] = json!(self.user_id);
        payload["deactivated_at"] = json!(None as Option<OffsetDateTime>);
        payload["updated_at"] = json!(None as Option<OffsetDateTime>);

        let _ = self.tree.insert(id.to_be_bytes().to_vec(), &serialize(&convert_value(payload))?[..])?;
    
        Ok(id)
    }

    pub fn create_with_payload_return(&self, mut payload: Value) -> Result<(u64, Value), Error> {
        let id = self.db.generate_id()?;

        payload["id"] = json!(id);
        payload["created_at"] = json!(OffsetDateTime::now_utc());
        payload["author"] = json!(self.user_id);
        payload["deactivated_at"] = json!(None as Option<OffsetDateTime>);
        payload["updated_at"] = json!(None as Option<OffsetDateTime>);

        let converted_payload = convert_value(payload);

        let _ = self.tree.insert(id.to_be_bytes().to_vec(), &serialize(&converted_payload)?[..])?;

        let res = convert_value2(converted_payload);
    
        Ok((id, res))
    }

    pub fn get(&self, id: u64) -> Result<Option<Value>, Error> {
        Ok(match self.tree.get(id.to_be_bytes())? {
            Some(bytes) => {
                let payload = deserialize(&bytes)?;
                let payload = convert_value2(payload);
                Some(payload)
            }
            None => None
        })
    }    

    pub fn update(&self, id: u64, mut payload: Value) -> Result<u64, Error> {
        let id_bytes = id.to_be_bytes();        

        match self.tree.get(id_bytes)? {
            Some(_) => {
                payload["updated_at"] = json!(OffsetDateTime::now_utc());

                self.tree.insert(id_bytes.to_vec(), &serialize(&convert_value(payload))?[..])?;
            }
            None => {
                info!("Entity with id {} not found, path {}", id, self.path);
            }
        }
    
        Ok(id)
    }
}

use std::convert::TryInto;
use std::collections::HashMap;
use std::ops::Index;
use std::fs::{read_dir, remove_dir_all};
use log::*;
use chrono::{NaiveDateTime, Utc};
use serde_json::{json, Value};
use sled::{Db, Tree, transaction::TransactionalTree};
use crate::sd::{serialize, deserialize, convert_value, convert_value2};
use crate::error::Error;

fn dog(barry: &[u8]) -> Result<[u8; 8], Error> {
    Ok(barry.try_into()?)
}

#[derive(Clone)]
pub struct Dc {
    root_path: String,
    user_id: u64,
    db: Db,
    tree: Tree
}

pub struct TxDc<'a> {
    user_id: u64,
    db: &'a Db,
    tree: &'a TransactionalTree
}

impl Dc {
    pub fn new(user_id: u64, root_path: &str) -> Result<Dc, Error> {
        let root_path = root_path.to_owned();
        let tree_name = "sp-co-cfg";
        
        let db = sled::open(&root_path)?;
        let tree = db.open_tree(&tree_name)?;

        Ok(Dc {
            root_path,
            user_id,
            db,
            tree
        })
    }
    
    pub fn tx(&self, transaction_body: fn(TxDc) -> Result<(), Error>) -> Result<(), Error> {
        let res = self.tree.transaction(|tx_tree| {
            let tx_dc = TxDc {
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
        payload["created_at"] = json!(Utc::now().naive_utc());
        payload["author_id"] = json!(self.user_id);
        payload["deactivated_at"] = json!(None as Option<NaiveDateTime>);
        payload["updated_at"] = json!(None as Option<NaiveDateTime>);

        let _ = self.tree.insert(id.to_be_bytes(), &serialize(&convert_value(payload))?[..])?;
    
        Ok(id)
    }

    pub fn create_with_payload_return(&self, mut payload: Value) -> Result<(u64, Value), Error> {
        let id = self.db.generate_id()?;

        payload["id"] = json!(id);
        payload["created_at"] = json!(Utc::now().naive_utc());
        payload["author_id"] = json!(self.user_id);
        payload["deactivated_at"] = json!(None as Option<NaiveDateTime>);
        payload["updated_at"] = json!(None as Option<NaiveDateTime>);

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
                payload["updated_at"] = json!(Utc::now().naive_utc());                    
                self.tree.insert(id_bytes.to_vec(), &serialize(&convert_value(payload))?[..])?;
            }
            false => {
                info!("Entity with id {} not found, root path {}", id, self.root_path);
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
}

impl TxDc<'_> {
    pub fn create(&self, mut payload: Value) -> Result<u64, Error> {
        let id = self.db.generate_id()?;

        payload["id"] = json!(id);
        payload["created_at"] = json!(Utc::now().naive_utc());
        payload["author_id"] = json!(self.user_id);
        payload["deactivated_at"] = json!(None as Option<NaiveDateTime>);
        payload["updated_at"] = json!(None as Option<NaiveDateTime>);

        let _ = self.tree.insert(id.to_be_bytes().to_vec(), &serialize(&convert_value(payload))?[..])?;
    
        Ok(id)
    }

    pub fn create_with_payload_return(&self, mut payload: Value) -> Result<(u64, Value), Error> {
        let id = self.db.generate_id()?;

        payload["id"] = json!(id);
        payload["created_at"] = json!(Utc::now().naive_utc());
        payload["author_id"] = json!(self.user_id);
        payload["deactivated_at"] = json!(None as Option<NaiveDateTime>);
        payload["updated_at"] = json!(None as Option<NaiveDateTime>);

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
                payload["updated_at"] = json!(Utc::now().naive_utc());

                self.tree.insert(id_bytes.to_vec(), &serialize(&convert_value(payload))?[..])?;
            }
            None => {
                info!("Entity with id {} not found", id);
            }
        }
    
        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use log::*;
    use serde_json::{json, Value};
    use crate::storage::*;
    use crate::error::Error;
    
    #[test]
    fn full_test() -> Result<(), Error> {
        let _ = env_logger::try_init();

        let user_id = 1; 
        let root_path = "d:/src/test-storage";

        let dc = Dc::new(user_id, root_path)?;

        dc.create(json!({
            "hello": "hello"
        }))?;

        let res = dc.get_all()?;

        info!("{:#?}", res);
        
        Ok(())
    }
}
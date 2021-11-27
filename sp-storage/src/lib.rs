use std::convert::TryInto;
use std::collections::HashMap;
use std::ops::Index;
use std::fs::{read_dir, remove_dir_all};
use log::*;
use time::OffsetDateTime;
use serde_json::{
	json, Value
};
use sled::{
	Db, Tree, transaction::TransactionalTree
};
use sp_dto::{
	QueryPrm, Relation
};
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

#[derive(Debug, Clone)]
pub enum Location {
    Regions,
    Scopes,
    Services { region_id: u64, scope_id: u64 },    
    Tokens { region_id: u64, scope_id: u64, service_id: u64 },
    Token { region_id: u64, scope_id: u64, service_id: u64, token_id: u64 }
}

impl Location {
    fn get_scope_id(&self) -> Option<u64> {
        match self {
            Location::Services { region_id: _, scope_id} |
            Location::Tokens { region_id: _, scope_id, service_id: _ } |
            Location::Token { region_id: _, scope_id, service_id: _, token_id: _ } => Some(*scope_id),
            _ => None
        }
    }
}

#[derive(Clone)]
pub struct Dc {
    storage_path: String,
    location: Location,
    user_id: u64,
    pub id: u64,
    pub payload: Value,
    db: Db,
    tree: Tree
}

pub struct TxDc<'a> {    
    location: Location,
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
    pub fn new(location: Location, user_id: u64, storage_path: &str, id: Option<u64>, payload: Option<Value>) -> Result<Dc, Error> {

        let storage_path = storage_path.to_owned();

        let (path, tree_name) = match location {
            Location::Regions => (
                storage_path.clone() + "/regions",
                "regions".to_owned()
            ),
            Location::Scopes => (
                storage_path.clone() + "/scopes",
                "scopes".to_owned()
            ),
            Location::Services { region_id: _, scope_id } => (
                storage_path.clone() + "/scope-" + &scope_id.to_string() + "/services",
                "services".to_owned()
            ),            
            Location::Tokens { region_id: _, scope_id, service_id } => (
                storage_path.clone() + "/scope-" + &scope_id.to_string() + "/svc-" + &service_id.to_string() + "-tokens",
                "tokens".to_owned()                        
            ),
            Location::Token { region_id: _, scope_id, service_id, token_id } => (
                storage_path.clone() + "/scope-" + &scope_id.to_string() + "/svc-" + &service_id.to_string() + "-token-" + &token_id.to_string(),
                "token".to_owned()
            )
        };
        
        let db = sled::open(path)?;
        let tree = db.open_tree(&tree_name)?;

        Ok(Dc {
            storage_path,
            location,
            user_id,
            id: id.unwrap_or(0),
            payload: payload.unwrap_or(json!({})),
            db,
            tree
        })
    }
    
    pub fn tx(&self, transaction_body: fn(TxDc) -> Result<(), Error>) -> Result<(), Error> {
        let res = self.tree.transaction(|tx_tree| {
            let tx_dc = TxDc {
                location: self.location.clone(),
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
        payload["author_id"] = json!(self.user_id);
        payload["deactivated_at"] = json!(None as Option<OffsetDateTime>);
        payload["updated_at"] = json!(None as Option<OffsetDateTime>);

        let _ = self.tree.insert(id.to_be_bytes(), &serialize(&convert_value(payload))?[..])?;
    
        Ok(id)
    }

    pub fn create_with_payload_return(&self, mut payload: Value) -> Result<(u64, Value), Error> {
        let id = self.db.generate_id()?;

        payload["id"] = json!(id);
        payload["created_at"] = json!(OffsetDateTime::now_utc());
        payload["author_id"] = json!(self.user_id);
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
                info!("Entity with id {} not found, location {:?}", id, self.location);
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
                    QueryPrm::I64(value) => {
                        match payload[key].as_i64() {
                            Some(field_value) => {
                                if field_value == *value {
                                    check_sum = check_sum + 1;
                                }
                            }
                            None => {}
                        }
                    }
                    QueryPrm::U64(value) => {
                        match payload[key].as_u64() {
                            Some(field_value) => {
                                if field_value == *value {
                                    check_sum = check_sum + 1;
                                }
                            }
                            None => {}
                        }
                    }
                    QueryPrm::U64Relation(relation, value) => {
                        match payload[key].as_u64() {
                            Some(field_value) => {
                                match relation {
                                    Relation::Less => {
                                        if field_value < *value {
                                            check_sum = check_sum + 1;
                                        }
                                    }
                                    Relation::Greater => {
                                        if field_value > *value {
                                            check_sum = check_sum + 1;
                                        }
                                    }
                                }                                
                            }
                            None => {}
                        }
                    }
                    QueryPrm::String(value) => {
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
                    QueryPrm::I64(value) => {
                        match payload[key].as_i64() {
                            Some(field_value) => {
                                if field_value == *value {
                                    check_sum = check_sum + 1;
                                }
                            }
                            None => {}
                        }
                    }
                    QueryPrm::U64(value) => {
                        match payload[key].as_u64() {
                            Some(field_value) => {
                                if field_value == *value {
                                    check_sum = check_sum + 1;
                                }
                            }
                            None => {}
                        }
                    }
                    QueryPrm::U64Relation(relation, value) => {
                        match payload[key].as_u64() {
                            Some(field_value) => {
                                match relation {
                                    Relation::Less => {
                                        if field_value < *value {
                                            check_sum = check_sum + 1;
                                        }
                                    }
                                    Relation::Greater => {
                                        if field_value > *value {
                                            check_sum = check_sum + 1;
                                        }
                                    }
                                }                                
                            }
                            None => {}
                        }
                    }
                    QueryPrm::String(value) => {
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
                    QueryPrm::I64(value) => {
                        match payload[key].as_i64() {
                            Some(field_value) => {
                                if field_value == *value {
                                    check_sum = check_sum + 1;
                                }
                            }
                            None => {}
                        }
                    }
                    QueryPrm::U64(value) => {
                        match payload[key].as_u64() {
                            Some(field_value) => {
                                if field_value == *value {
                                    check_sum = check_sum + 1;
                                }
                            }
                            None => {}
                        }
                    }
                    QueryPrm::U64Relation(relation, value) => {
                        match payload[key].as_u64() {
                            Some(field_value) => {
                                match relation {
                                    Relation::Less => {
                                        if field_value < *value {
                                            check_sum = check_sum + 1;
                                        }
                                    }
                                    Relation::Greater => {
                                        if field_value > *value {
                                            check_sum = check_sum + 1;
                                        }
                                    }
                                }                                
                            }
                            None => {}
                        }
                    }
                    QueryPrm::String(value) => {
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
        payload["author_id"] = json!(self.user_id);
        payload["deactivated_at"] = json!(None as Option<OffsetDateTime>);
        payload["updated_at"] = json!(None as Option<OffsetDateTime>);

        let _ = self.tree.insert(id.to_be_bytes().to_vec(), &serialize(&convert_value(payload))?[..])?;
    
        Ok(id)
    }

    pub fn create_with_payload_return(&self, mut payload: Value) -> Result<(u64, Value), Error> {
        let id = self.db.generate_id()?;

        payload["id"] = json!(id);
        payload["created_at"] = json!(OffsetDateTime::now_utc());
        payload["author_id"] = json!(self.user_id);
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
                info!("Entity with id {} not found, location {:?}", id, self.location);
            }
        }
    
        Ok(id)
    }
}

pub fn create_scope(storage_path: &str) -> Result<(u64, u64), Error> {
    let user_id = 1;
    
    let region_dc = Dc::new(Location::Regions, user_id, storage_path, None, None)?;

    let (region_id, mut region_payload) = match region_dc.find(|x| x["count"].as_u64().is_some() && x["count"].as_u64().unwrap() < 3)? {
        Some((id, payload)) => (id, payload),
        None => region_dc.create_with_payload_return(json!({ "count": 0u64 }))?,
    };

    info!("Region id {}", region_id);

    region_payload["count"] = json!(match region_payload["count"].as_u64() {
        Some(current_count) => current_count + 1,
        None => 1
    });    

    region_dc.update(region_id, region_payload)?;
        
    
    let dc = Dc::new(Location::Scopes, user_id, storage_path, None, None)?;
    
    let scope_id = dc.create(json!({
        "region_id": region_id
    }))?;

    info!("Scope id {}", scope_id);

    let count = dc.count(|x| x["region_id"].as_u64() == Some(region_id))?;

    info!("Count is {}", count);

    Ok((region_id, scope_id))
}

pub fn create_service(service_dc: &Dc, name: &str, service: &str, domain: &str) -> Result<u64, Error> {
    Ok(service_dc.create(json!({
        "name": name,
		"service": service,
		"domain": domain
    }))?)
}

pub fn delete_service(service_dc: &Dc, name: &str, storage_path: &str) -> Result<(), Error> {
    let (service_id, _payload) = service_dc.find(|a| a["name"].as_str() == Some(name))?.ok_or(Error::None)?;

    let pattern = "svc-".to_owned() + &service_id.to_string();
    let pattern_len = pattern.len();

    let scope_path = storage_path.to_owned() + "/scope-" + &service_dc.location.get_scope_id().ok_or(Error::None)?.to_string();

    info!("{}", scope_path);
    info!("{}", pattern);
    
    for entry in read_dir(scope_path)? {
        let target = entry?.path();        
        if target.is_dir() {
            let target_str = target.file_name().ok_or(Error::None)?;
            
            if target_str.len() >= pattern_len && target_str.to_str().ok_or(Error::None)?[..pattern_len] == pattern {
                info!("Removing {}, path {:?}", target_str.to_str().ok_or(Error::None)?, target);

                remove_dir_all(target)?;

                info!("Ok");
            }            
        }
    }

    info!("Removing service from db");

    service_dc.tree.remove(service_id.to_be_bytes())?.ok_or(Error::None)?;

    info!("Ok");

    Ok(())
}

#[cfg(test)]
mod tests {
    use log::*;
    use serde_json::json;
    use crate::{
        Location, Sc, Dc, error::Error, create_service, create_scope
    };
    
    //#[test]
    fn full_test() -> Result<(), Error> {
        let _ = env_logger::try_init();

        let storage_path = "c:/src/dog";
        let user_id = 1;

        let (region_id, scope_id) = create_scope(storage_path)?;

        let storage_path = "c:/src/storage";    

        let service_dc = Dc::new(Location::Services { region_id, scope_id }, user_id, storage_path, None, None)?;

        let service_id = create_service(&service_dc, "Dog")?;

        {
            let dc = Dc::new(Location::Tokens { region_id, scope_id, service_id }, user_id, storage_path, None, None)?;

            dc.create(json!({
                "name": "DogToken"
            }))?;
        }    

		let sc = Sc::new(user_id, region_id, scope_id, service_id, storage_path, "Dog", "Dog", "Dog")?;

        sc["DogToken"].create(json!({
            "name": "hello"
        }))?;

        let res = sc["DogToken"].get_all()?;

        info!("{:#?}", res);
        
        Ok(())
    }

    //#[test]
    fn test_service_delete() -> Result<(), Error> {
        let _ = env_logger::try_init();

        let storage_path = "d:/src/dog";
        let user_id = 1;

        let (region_id, scope_id) = create_scope(storage_path)?;

        let storage_path = "d:/src/storage";    

        let service_dc = Dc::new(Location::Services { region_id, scope_id }, user_id, storage_path, None, None)?;

        let service_id = create_service(&service_dc, "Dog")?;

        {
            let dc = Dc::new(Location::Tokens { region_id, scope_id, service_id }, user_id, storage_path, None, None)?;

            dc.create(json!({
                "name": "DogToken"
            }))?;
        }    

        let sc = Sc::new(user_id, region_id, scope_id, service_id, storage_path, "Dog", "Dog", "Dog")?;

        sc["DogToken"].create(json!({
            "name": "hello"
        }))?;

        delete_service(&service_dc, "Dog", storage_path)?;
        
        Ok(())
    }    
}

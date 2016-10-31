use std::sync::Arc;

use experiment::*;

extern crate bson;

extern crate mongo_driver;
use self::mongo_driver::client::{Client as MongoClient, ClientPool, Uri};
use self::mongo_driver::Result as MongoResult;

extern crate chrono;

pub struct Client {
    client_pool: Arc<ClientPool>,
    dbname: String,
}

pub struct ClientFactory {
    client_pool: Arc<ClientPool>,
    dbname: String,
}

impl ClientFactory {
    pub fn new(uri: String, dbname: String) -> ClientFactory {
        ClientFactory {
            client_pool: Arc::new(ClientPool::new(Uri::new(uri).unwrap(),
                                                  None)),
            dbname: dbname,
        }
    }
}

impl TestClientFactory<Client> for ClientFactory {
    fn create(&self) -> Client {
        Client::new(self.client_pool.clone(), self.dbname.clone())
    }
}

impl Client {
    fn new(client_pool: Arc<ClientPool>, dbname: String) -> Client {
        Client {
            client_pool: client_pool,
            dbname: dbname,
        }
    }

    fn to_bson(&self, tag: &Tag) -> bson::Document {
        let mut doc = bson::Document::new();
        doc.insert("id".to_string(), tag.id);
        doc.insert("v".to_string(), tag.value);
        doc.insert("t".to_string(),
                   bson::Bson::TimeStamp(1000 * tag.timestamp));
        doc
    }
}

impl TestClient for Client {
    fn insert(&mut self, batch: &[Tag]) -> Result<(), String> {
        let client = self.client_pool.as_ref().pop();
        let collection = client.get_collection(self.dbname.as_str(), "tags");
        let bulk_operation = collection.create_bulk_operation(None);
        for tag in batch {
            let doc = self.to_bson(tag);
            bulk_operation.insert(&doc).unwrap();
        }
        match bulk_operation.execute() {
            Ok(_) => Ok(()),
            Error => Err("Error".to_string()),
        }
    }

    fn query(&mut self, id: u64, start: i64, end: i64) -> Vec<Tag> {
        let mut tags = vec![];
        let start_enc = bson::Bson::TimeStamp(start);
        let end_enc = bson::Bson::TimeStamp(end);
        let query = doc!{
            "id" => id,
            "t" => {"$gt" => start_enc,
                    "$lt" => end_enc
            }
        };
        let client = self.client_pool.as_ref().pop();
        let collection = client.get_collection(self.dbname.as_str(), "tags");

        let cursor = collection.find(&query, None).unwrap();

        for iter in cursor.into_iter() {
            let document = iter.unwrap();
            // tags.push(Tag{
            //     id: document.get_i64("id").unwrap() as u64,
            //     timestamp: document.get_time_stamp("t").unwrap(),
            //     value: document.get_f64("v").unwrap(),
            // });
        }
        tags
    }
}

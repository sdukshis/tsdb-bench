use std::io::Read;

extern crate hyper;
use self::hyper::client::{Client as hyperClient, Request, Response};
use self::hyper::client::pool::Config;
use self::hyper::Url;
use self::hyper::header::{ContentType, Connection, Headers};
use self::hyper::mime::{Mime, TopLevel, SubLevel};


use experiment::*;

pub struct Client {
    base_url: String,
    dbname: String,
    client: hyperClient,
    buffer: String,
}

pub struct ClientFactory {
    base_url: String,
    dbname: String,
}

impl ClientFactory {
    pub fn new(url: String, dbname: String) -> ClientFactory {
        ClientFactory {
            base_url: url,
            dbname: dbname,
        }
    }
}

impl TestClientFactory<Client> for ClientFactory {
    fn create(&self) -> Client {
        Client::new(self.base_url.clone(), self.dbname.clone())
    }
}

impl Client {
    pub fn new(url: String, dbname: String) -> Client {
        Client {
            base_url: url,
            dbname: dbname,
            client: hyperClient::new(),
            buffer: String::new(),
        }
    }

    fn to_plain(&mut self, batch: &[Tag]) {
        self.buffer.clear();
        for tag in batch {
            self.buffer.push_str(format!("tags,id={} value={} {}\n",
                                         tag.id,
                                         tag.value,
                                         1000000 * tag.timestamp)
                                     .as_str());
        }
    }
}


impl TestClient for Client {
    fn insert(&mut self, batch: &[Tag]) -> Result<(), String> {
        let put_url = self.base_url.clone() +
                      format!("/write?db={}", self.dbname).as_str();
        self.to_plain(batch);
        // debug!("body: {:?}", self.buffer);
        let request = self.client
                          .post(Url::parse(put_url.as_str()).unwrap())
                          .body(self.buffer.as_str());
        let mut result = request.send();
        match result {
            Ok(mut response) => {
                let mut s = String::new();
                response.read_to_string(&mut s).unwrap();
                debug!("Response: {:?}", response);
                if response.status.is_success() {
                    return Ok(());
                } else {
                    return Err(s);
                }
            }
            Error => {
                return Err("HTTP request error".to_string());
            }
        }

    }

    fn query(&mut self, id: u64, start: i64, end: i64) -> Vec<Tag> {
        vec![]
    }
}

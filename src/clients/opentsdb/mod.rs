use std::collections::BTreeMap;
use std::io::Read;

extern crate rustc_serialize;
use self::rustc_serialize::json::{self, Json, ToJson};

extern crate hyper;
use self::hyper::client::{Client as hyperClient, Request, Response};
use self::hyper::client::pool::Config;
use self::hyper::Url;
use self::hyper::header::{ContentType, Connection, Headers};
use self::hyper::mime::{Mime, TopLevel, SubLevel};


use experiment::*;

pub struct Client {
    base_url: String,
    client: hyperClient,
}

impl Client {
    pub fn new(url: String) -> Client {
        Client {
            base_url: url,
            client: hyperClient::new(),
        }
    }
}

pub struct ClientFactory {
    base_url: String,
}

impl ClientFactory {
    pub fn new(url: String) -> ClientFactory {
        ClientFactory { base_url: url }
    }
}

impl TestClientFactory<Client> for ClientFactory {
    fn create(&self) -> Client {
        Client::new(self.base_url.clone())
    }
}

impl ToJson for Tag {
    fn to_json(&self) -> Json {
        let mut dict = BTreeMap::new();
        dict.insert("metric".to_string(), "tag".to_json());
        dict.insert("timestamp".to_string(), self.timestamp.to_json());
        dict.insert("value".to_string(), self.value.to_json());
        let mut tags = BTreeMap::new();
        tags.insert("id".to_string(), self.id.to_json());
        dict.insert("tags".to_string(), tags.to_json());
        Json::Object(dict)
    }
}

impl TestClient for Client {
    fn insert(&mut self, batch: &[Tag]) -> Result<(), String> {
        let encoded = batch.to_json();

        let put_url = self.base_url.clone() + "/api/put";
        let body = encoded.to_string();
        let mut headers = Headers::new();
        // headers.set(Connection::keep_alive());
        headers.set(ContentType(Mime(TopLevel::Application,
                                     SubLevel::Json,
                                     vec![])));
        let request = self.client
                          .post(Url::parse(put_url.as_str()).unwrap())
                          .headers(headers)
                          .body(body.as_str());
        let mut result = request.send();
        match result {
            Ok(mut response) => {
                let mut s = String::new();
                response.read_to_string(&mut s).unwrap();
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

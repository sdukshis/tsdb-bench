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
    client: hyperClient,
    buffer: String,
}

pub struct ClientFactory {
    base_url: String,
}

impl ClientFactory {
    pub fn new(url: String) -> ClientFactory {
        ClientFactory {
            base_url: url,
        }
    }
}

impl TestClientFactory<Client> for ClientFactory {
    fn create(&self) -> Client {
        Client::new(self.base_url.clone())
    }
}

impl Client {
    pub fn new(url: String) -> Client {
        Client {
            base_url: url,
            client: hyperClient::new(),
            buffer: String::new(),
        }
    }

    fn to_plain(&mut self, batch: &[Tag]) {
        self.buffer.clear();
        // self.buffer.push_str("INSERT INTO tags(d, id, ts, value) VALUES");
        self.buffer.push_str("INSERT INTO tags(id, ts, value) FORMAT TabSeparated\n");
        for tag in batch {
            // self.buffer.push_str(format!("(toDate('2016-10-12'), {}, {}, {}),",
            self.buffer.push_str(format!("{}\t{}\t{}\n",
                                         tag.id,
                                         tag.timestamp as f64 / 1e3,
                                         tag.value)
                                     .as_str());
        }
        self.buffer.pop();
    }
}


impl TestClient for Client {
    fn insert(&mut self, batch: &[Tag]) -> Result<(), String> {
        let put_url = self.base_url.clone();
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

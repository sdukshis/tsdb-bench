use experiment::*;

extern crate postgres;

use self::postgres::{Connection, SslMode};
use self::postgres::error::Error;

pub struct Client {
    connection: Connection,
    buffer: String,
}

pub struct ClientFactory {
    host: String,
    port: i32,
    dbname: String,
    user: String,
    passwd: String,
}

impl ClientFactory {
    pub fn new(host: String,
               port: i32,
               dbname: String,
               user: String,
               passwd: String)
               -> ClientFactory {
        ClientFactory {
            host: host,
            port: port,
            dbname: dbname,
            user: user,
            passwd: passwd,
        }
    }
}


impl TestClientFactory<Client> for ClientFactory {
    fn create(&self) -> Client {
        let uri = format!("postgres://{}:{}@{}:{}/{}",
                          self.user,
                          self.passwd,
                          self.host,
                          self.port,
                          self.dbname);
        Client {
            connection: Connection::connect(uri.as_str(), SslMode::None)
                            .unwrap(),
            buffer: String::new(),
        }
    }
}


impl TestClient for Client {
    fn insert(&mut self, batch: &[Tag]) -> Result<(), String> {
        self.buffer.clear();
        self.buffer.push_str("INSERT INTO tags(id, ts, value) VALUES");
        for tag in batch {
            self.buffer.push_str(format!("({}, to_timestamp({}), {}),",
                                         tag.id,
                                         tag.timestamp as f64 / 1e3,
                                         tag.value)
                                     .as_str());
        }
        self.buffer.pop();
        self.buffer.push(';');
        match self.connection.batch_execute(self.buffer.as_str()) {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("{}", e)),
        }
    }

    fn query(&mut self, id: u64, start: i64, end: i64) -> Vec<Tag> {
        vec![]
    }
}

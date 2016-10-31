#[macro_use]
extern crate log;
extern crate env_logger;

#[macro_use]
extern crate bson;


use std::time::{Instant, Duration};
use std::env;
use std::f64;

mod experiment;
use experiment::*;

mod clients;
use clients::*;

fn main() {
    env_logger::init().unwrap();
    let usage_info = "Usage: dbms-bench tag_id start end total_time";

    let tag_id = env::args()
                     .nth(1)
                     .expect(usage_info)
                     .parse::<u64>()
                     .expect(usage_info);
    let start = env::args()
                    .nth(2)
                    .expect(usage_info)
                    .parse::<i64>()
                    .expect(usage_info);
    let end = env::args()
                  .nth(3)
                  .expect(usage_info)
                  .parse::<i64>()
                  .expect(usage_info);
    let total_queries = env::args()
                            .nth(4)
                            .expect(usage_info)
                            .parse::<usize>()
                            .expect(usage_info);


    let client_factory = mongodb::ClientFactory::new("mongodb://localhost:\
                                                      27017/"
                                                         .to_string(),
                                                     "tags".to_string());
    let mut client = client_factory.create();

    println!("ttime");
    for query_num in 0..total_queries {
        let query_start = Instant::now();
        let result = client.query(tag_id, start, end);
        let duration = Instant::now() - query_start;
        let dur_ms = 1000 * duration.as_secs() +
                     duration.subsec_nanos() as u64 / 1000000;
        println!("{}", dur_ms);
    }
}

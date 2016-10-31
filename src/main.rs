#[macro_use]
extern crate log;
extern crate env_logger;

#[macro_use]
extern crate bson;

use std::env;
use std::f64;

mod experiment;
use experiment::*;

mod clients;
use clients::*;

fn main() {
    env_logger::init().unwrap();
    let usage_info = "Usage: dbms-bench nb_tags batch_size nb_threads \
                      total_time";

    let nb_tags = env::args()
                      .nth(1)
                      .expect(usage_info)
                      .parse::<u64>()
                      .expect(usage_info);
    let batch_size = env::args()
                         .nth(2)
                         .expect(usage_info)
                         .parse::<u64>()
                         .expect(usage_info);
    let nb_threads = env::args()
                         .nth(3)
                         .expect(usage_info)
                         .parse::<u64>()
                         .expect(usage_info);
    let total_time = env::args()
                         .nth(4)
                         .expect(usage_info)
                         .parse::<usize>()
                         .expect(usage_info);
    let lambda = env::args()
                     .nth(5)
                     .expect(usage_info)
                     .parse::<f64>()
                     .expect(usage_info);


    // let client_factory = influxdb::ClientFactory::new("http://localhost:8086".to_string(),
                                                      // "tags".to_string());
    let client_factory = mongodb::ClientFactory::new("mongodb://localhost:27017/".to_string(),
                                                     "tags".to_string());
    // let client_factory = opentsdb::ClientFactory::new("http://localhost:4242/".to_string());
    // let client_factory = clickhouse::ClientFactory::new("http://localhost:8123/".to_string());
    // let client_factory = postgres::ClientFactory::new("localhost".to_string(),
    //                                                   5432,
    //                                                   "tags".to_string(),
    //                                                   "root".to_string(),
    //                                                   "secret".to_string());

    let mut experiment = Experiment::new(client_factory,
                                         nb_tags,
                                         batch_size,
                                         nb_threads,
                                         lambda,
                                         StopCriteria::TotalTime(total_time));
    experiment.run();
}

use std::env;
use std::f64;

extern crate time;
use time::{now_utc, Duration};

extern crate rand;
use rand::random;

use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;

fn main() {
    let usage_info = "Usage: dbms-bench nb_tags batch_size nb_threads total_time";

    let nb_tags = env::args().nth(1).expect(usage_info).parse::<u64>().expect(usage_info);
    let total_tags = env::args().nth(2).expect(usage_info).parse::<usize>().expect(usage_info);

    let mut curr_tag = 0;
    let mut now = now_utc();
    let omega = 2.0 / 3600.0 * f64::consts::PI;

    let mut files = BTreeMap::new();
    for tag_id in 0..nb_tags {
        files.insert(tag_id,
                     File::create(format!("tags.{}.txt", tag_id))
                         .unwrap());
    }
    for _ in 0..total_tags {
        let tm = now.to_timespec();
        let timestamp = tm.sec as i64 * 1000000000 + tm.nsec as i64;
        let value = f64::sin(omega * tm.sec as f64) + 0.01 * random::<f64>();
        files.get_mut(&curr_tag)
             .unwrap()
             .write_fmt(format_args!("tags,id={} value={} {}\n", curr_tag, value, timestamp));
        curr_tag += 1;
        if curr_tag >= nb_tags {
            curr_tag = 0;
            now = now + Duration::seconds(1);
        }
    }
}

use std::vec::Vec;
use std::time::{Instant, Duration};
use std::thread::{self, sleep};
use std::f64;
use std::sync::{Arc, Mutex};
use std::marker::PhantomData;

extern crate time;
pub use self::time::Tm;
use self::time::now_utc;

extern crate rand;
use self::rand::random;

#[derive(Debug)]
pub struct Tag {
    pub id: u64,
    pub timestamp: i64,
    pub value: f64,
}



pub trait TestClient {
    fn insert(&mut self, batch: &[Tag]) -> Result<(), String>;

    fn query(&mut self, id: u64, start: i64, end: i64) -> Vec<Tag>;
}

pub trait TestClientFactory<T: TestClient> {
    fn create(&self) -> T;
}

#[derive(Clone, Copy)]
pub enum StopCriteria {
    Infinite,
    TotalTags(usize),
    TotalTime(usize),
}

pub struct Experiment<T: TestClient, F: TestClientFactory<T>> {
    client_factory: F,
    nb_tags: u64,
    batch_size: u64,
    nb_threads: u64,
    lambda: f64,
    stop_criteria: StopCriteria,
    total_tags_sent: Arc<Mutex<usize>>,
    phantom: PhantomData<T>,
}

struct SingleClient<T: TestClient> {
    client: T,
    nb_tags: u64,
    batch_size: u64,
    lambda: f64,
    stop_criteria: StopCriteria,
    current_tag: u64,
    last_tag_gen_time: Instant,
    total_tags_sent: Arc<Mutex<usize>>,
}

impl<T: TestClient> SingleClient<T> {
    fn new(client: T,
           nb_tags: u64,
           batch_size: u64,
           lambda: f64,
           stop_criteria: StopCriteria,
           total_tags_sent: Arc<Mutex<usize>>)
           -> SingleClient<T> {
        SingleClient {
            client: client,
            nb_tags: nb_tags,
            batch_size: batch_size,
            lambda: lambda,
            stop_criteria: stop_criteria,
            current_tag: 0,
            last_tag_gen_time: Instant::now(),
            total_tags_sent: total_tags_sent,
        }
    }

    fn run(&mut self) {
        match self.stop_criteria {
            StopCriteria::Infinite => self.run_infinite(),
            StopCriteria::TotalTags(total_tags) => {
                self.run_with_total_tags(total_tags)
            }
            StopCriteria::TotalTime(total_time) => {
                self.run_with_total_time(total_time)
            }
        }
    }

    fn run_infinite(&mut self) {
        loop {
            let tags = self.generate_tags();
            match self.client.insert(&tags) {
                Ok(_) => {
                    let mut counter = self.total_tags_sent.lock().unwrap();
                    *counter += tags.len();
                }
                Err(error) => {
                    error!("{}", error);
                    sleep(Duration::new(1, 0));
                }
            }
        }
    }

    fn run_with_total_tags(&mut self, total_tags: usize) {
        while *self.total_tags_sent.lock().unwrap() < total_tags {
            let tags = self.generate_tags();
            match self.client.insert(&tags) {
                Ok(_) => {
                    let mut counter = self.total_tags_sent.lock().unwrap();
                    *counter += tags.len();
                }
                Err(error) => {
                    error!("{}", error);
                    sleep(Duration::new(1, 0));
                }
            }
        }
    }

    fn run_with_total_time(&mut self, total_time: usize) {
        let start = Instant::now();

        while (Instant::now() - start) <= Duration::new(total_time as u64, 0) {
            let tags = self.generate_tags();
            match self.client.insert(&tags) {
                Ok(_) => {
                    let mut counter = self.total_tags_sent.lock().unwrap();
                    *counter += tags.len();
                }
                Err(error) => {
                    error!("{}", error);
                    sleep(Duration::new(1, 0));
                }
            }
        }
    }


    fn generate_tags(&mut self) -> Vec<Tag> {
        let mut tags = Vec::new();
        for _ in 0..self.batch_size {
            tags.push(self.generate_next_tag());
        }
        tags
    }

    fn generate_next_tag(&mut self) -> Tag {
        let now = now_utc().to_timespec();
        if self.current_tag >= self.nb_tags {
            self.current_tag = 0;
            if self.lambda > 0.0 {
                let now = Instant::now();
                let delay = 1.0 / (self.lambda as f64);
                let delta_t = now - self.last_tag_gen_time;
                let delta_t_sec = delta_t.as_secs() as f64 +
                                  delta_t.subsec_nanos() as f64 / 1e9;
                if delta_t_sec < delay {
                    sleep(Duration::new(0,
                                        (1e9 * (delay - delta_t_sec)) as u32));
                }
                self.last_tag_gen_time = Instant::now();
            }
        } else {
            self.current_tag += 1;
        }
        let omega = 2.0 / 3600.0 * f64::consts::PI;
        Tag {
            id: self.current_tag,
            timestamp: now.sec * 1000 + now.nsec as i64 / 1000000,
            value: f64::sin(omega * now.sec as f64) + 0.01 * random::<f64>(),
        }
    }
}

impl<T: TestClient, F: TestClientFactory<T>> Experiment<T, F> {
    pub fn new(client_factory: F,
               nb_tags: u64,
               batch_size: u64,
               nb_threads: u64,
               lambda: f64,
               stop_criteria: StopCriteria)
               -> Experiment<T, F> {
        Experiment {
            client_factory: client_factory,
            nb_tags: nb_tags,
            batch_size: batch_size,
            nb_threads: nb_threads,
            lambda: lambda,
            stop_criteria: stop_criteria,
            total_tags_sent: Arc::new(Mutex::new(0)),
            phantom: PhantomData,
        }
    }

    pub fn run(&mut self)
        where T: Send + 'static
    {
        let running = Arc::new(Mutex::new(true));
        let counter = self.total_tags_sent.clone();

        let reporter_flag = running.clone();
        let mut reporter = thread::spawn(move || {
            while *reporter_flag.lock().unwrap() {
                let last_report_tags = *counter.lock().unwrap();
                sleep(Duration::new(1, 0));
                let now = now_utc().to_timespec();
                println!("{},{}",
                         now.sec,
                         *counter.lock().unwrap() - last_report_tags);
            }
        });
        let mut childs = vec![];

        for _ in 0..self.nb_threads {
            let mut client = SingleClient::new(self.client_factory.create(),
                                               self.nb_tags,
                                               self.batch_size,
                                               self.lambda,
                                               self.stop_criteria.clone(),
                                               self.total_tags_sent.clone());
            childs.push(thread::spawn(move || {
                client.run();
            }));
        }

        for child in childs {
            child.join();
        }
        *running.lock().unwrap() = false;
        reporter.join();
    }
}

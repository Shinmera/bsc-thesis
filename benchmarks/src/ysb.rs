use config::Config;
use endpoint::{self, Source, Drain, ToData, FromData, EventSource};
use operators::{Window, Reduce};
use rand::{self, Rng, SeedableRng};
use serde_json;
use std::collections::HashMap;
use std::fs::File;
use std::fs;
use std::io::{Result, Write};
use std::thread::{self, JoinHandle};
use std::sync::RwLock;
use test::{Test, TestImpl, Benchmark};
use timely::dataflow::operators::{Map, Filter};
use timely::dataflow::scopes::{Root, Child};
use timely::dataflow::{Stream};
use timely::progress::timestamp::{RootTimestamp, Timestamp};
use timely_communication::allocator::Generic;
use uuid::Uuid;

#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Abomonation)]
struct Event {
    user_id: String,
    page_id: String,
    ad_id: String,
    ad_type: String,
    /// One of "view", "click", or "purchase", where "view" is the relevant type.
    event_type: String,
    /// Apparently timestamps in miliseconds
    event_time: usize,
    ip_address: String,
}

impl ToData<usize, Event> for String{
    fn to_data(self) -> Result<(usize, Event)> {
        serde_json::from_str(&self)
            .map(|e: Event| (e.event_time / 1000, e))
            .map_err(|e| e.into())
    }
}

impl FromData<usize> for Event{
    fn from_data(&self, _: &usize) -> String {
        serde_json::to_string(self).unwrap()
    }
}

impl<T: Timestamp> FromData<T> for (String, usize) {
    fn from_data(&self, t: &T) -> String {
        format!("{:?} {:?}", t, self)
    }
}

struct Query {
    campaign_map: RwLock<HashMap<String, String>>
}

impl Query {
    fn new() -> Self {
        Query {
            campaign_map: RwLock::new(HashMap::new())
        }
    }
}

impl TestImpl for Query {
    type D = Event;
    type DO = (String, usize);
    type T = usize;

    fn name(&self) -> &str { "Yahoo Streaming Benchmark" }

    fn create_endpoints(&self, config: &Config, _index: usize, _workers: usize) -> Result<(Source<Self::T, Self::D>, Drain<Self::T, Self::DO>)> {
        let gen = YSBGenerator::new(config);
        let mut target = self.campaign_map.write().unwrap();
        for (k, v) in &gen.map { target.insert(k.clone(), v.clone()); }
        Ok((Source::from_config(config, Source::new(Box::new(gen)))?,
            Drain::from_config(config)?))
    }

    fn construct_dataflow<'scope>(&self, config: &Config, stream: &Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>) -> Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO> {
        let window_size = config.get_as_or("window-size", 10);
        let table = self.campaign_map.read().unwrap().clone();
        stream
            .filter(|x: &Event| x.event_type == "view")
            .map(|x| (x.ad_id, x.event_time))
            .map(move |(ad_id, _)|
                 match table.get(&ad_id){
                     Some(id) => id.clone(),
                     None => String::from("UNKNOWN AD")
                 })
            .tumbling_window(move |t| RootTimestamp::new(((t.inner/window_size)+1)*window_size))
            .reduce_by(|campaign_id| campaign_id.clone(), 0, |_, count| count+1)
    }
}

#[derive(Clone)]
struct YSBGenerator {
    map: HashMap<String, String>,
    time: f64,
    timestep: f64,
    max_time: f64,
}

impl YSBGenerator {
    fn new(config: &Config) -> Self {
        let index = config.get_as_or("worker-index", 0);
        let threads = config.get_as_or("threads", 1);
        let campaigns = config.get_as_or("campaigns", 100);
        let ads = config.get_as_or("ads", 10);
        let seconds = config.get_as_or("seconds", 60);
        let events_per_second = config.get_as_or("events-per-second", 100_000);
        let timestep = (1000 * threads) as f64 / events_per_second as f64;
        
        // Generate campaigns map
        let mut map = HashMap::new();
        for _ in 0..campaigns {
            let campaign_id = format!("{}", Uuid::new_v4());
            for _ in 0..ads {
                let ad_id = format!("{}", Uuid::new_v4());
                map.insert(ad_id, campaign_id.clone());
            }
        }

        YSBGenerator{
            map: map,
            time: 1.0+(index*1000/threads) as f64,
            timestep: timestep,
            max_time: (seconds * 1000) as f64,
        }
    }
}

impl EventSource<usize, Event> for YSBGenerator {
    fn next(&mut self) -> Result<(usize, Vec<Event>)> {
        const AD_TYPES: [&str; 5] = ["banner", "modal", "sponsored-search", "mail", "mobile"];
        const EVENT_TYPES: [&str; 3] = ["view", "click", "purchase"];
        let mut rng = rand::StdRng::from_seed(&[0xDEAD, 0xBEEF, 0xFEED]); // Predictable RNG clutch
        let mut data = Vec::with_capacity((1000.0 / self.timestep) as usize);
        let epoch = self.time as usize / 1000;
        
        while self.time < ((epoch+1)*1000) as f64
            && self.time < self.max_time as f64 {
            data.push(Event {
                user_id: format!("{}", Uuid::new_v4()),
                page_id: format!("{}", Uuid::new_v4()),
                ad_id: self.map.keys().nth(rng.gen_range(0, self.map.len())).unwrap().clone(),
                ad_type: String::from(*rng.choose(&AD_TYPES).unwrap()),
                event_type: String::from(*rng.choose(&EVENT_TYPES).unwrap()),
                event_time: self.time as usize,
                ip_address: String::from("0.0.0.0"),
            });
            self.time += self.timestep;
        }

        if data.len() == 0 {
            endpoint::out_of_data()
        } else {
            Ok((epoch, data))
        }
    }
}

pub struct YSB {}

impl YSB {
    pub fn new() -> Self { YSB{} }
}

impl Benchmark for YSB {

    fn name(&self) -> &str { "Yahoo Streaming Benchmark" }

    fn generate_data(&self, config: &Config) -> Result<()> {
        let data_dir = format!("{}/ysb", config.get_or("data-dir", "data"));
        let partitions = config.get_as_or("threads", 1);
        let campaigns = config.get_as_or("campaigns", 100);
        let ads = config.get_as_or("ads", 10);
        let seconds = config.get_as_or("seconds", 60);
        let events_per_second = config.get_as_or("events-per-second", 100_000);
        fs::create_dir_all(&data_dir)?;

        println!("Generating {} events/s for {}s over {} partitions for {} campaigns with {} ads each.",
                 events_per_second, seconds, partitions, campaigns, ads);
        
        let generator = YSBGenerator::new(config);
        let campaign_file = File::create(format!("{}/campaigns.json", &data_dir))?;
        serde_json::to_writer(campaign_file, &generator.map)?;
        
        // Generate events
        let mut threads: Vec<JoinHandle<Result<()>>> = Vec::new();
        for p in 0..partitions {
            let mut generator = generator.clone();
            let mut file = File::create(format!("{}/events-{}.json", &data_dir, p))?;;
            threads.push(thread::spawn(move || {
                loop{
                    let (_, d) = generator.next()?;
                    for e in d {
                        serde_json::to_writer(&file, &e)?;
                        file.write(b"\n")?;
                    }
                }
            }));
        }
        for t in threads.drain(..){
            endpoint::accept_out_of_data(t.join().unwrap())?;
        }
        Ok(())
    }

    fn tests(&self) -> Vec<Box<Test>> {
        vec![Box::new(Query::new())]
    }
}

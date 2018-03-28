use abomonation::Abomonation;
use config::Config;
use endpoint::{Source, Drain, ToData, FromData};
use operators::{Window, Reduce};
use rand::{self, Rng};
use serde_json;
use std::collections::HashMap;
use std::fs::File;
use std::fs;
use std::io::{self, Result, Write};
use std::ops::Deref;
use std::sync::RwLock;
use test::{Test, TestImpl};
use timely::dataflow::operators::{Map, Filter};
use timely::dataflow::scopes::{Root, Child};
use timely::dataflow::{Stream};
use timely::progress::nested::product::Product;
use timely::progress::timestamp::{Timestamp, RootTimestamp};
use timely_communication::allocator::Generic;
use uuid::Uuid;

#[derive(Eq, PartialEq, Clone, Serialize, Deserialize)]
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

unsafe_abomonate!(Event : user_id, page_id, ad_id, ad_type, event_type, event_time, ip_address);

impl ToData<Product<RootTimestamp, usize>, Event> for String{
    fn to_data(self) -> Option<(Product<RootTimestamp, usize>, Event)> {
        serde_json::from_str(&self).ok()
            .map(|event: Event| (RootTimestamp::new(event.event_time / 1000), event))
    }
}

impl<T: Timestamp> FromData<T> for (String, usize) {
    fn from_data(&self, t: &T) -> String {
        format!("{:?} {:?}", t, self)
    }
}

struct YSB {
    campaign_map: RwLock<HashMap<String, String>>
}

impl YSB {
    fn new() -> Self {
        YSB{
            campaign_map: RwLock::new(HashMap::new())
        }
    }

    fn generate_event(&self, time: usize) -> Event {
        let mut rng = rand::thread_rng();
        let map = self.campaign_map.read().unwrap();
        const AD_TYPES: [&str; 5] = ["banner", "modal", "sponsored-search", "mail", "mobile"];
        const EVENT_TYPES: [&str; 3] = ["view", "click", "purchase"];
        Event {
            user_id: format!("{}", Uuid::new_v4()),
            page_id: format!("{}", Uuid::new_v4()),
            ad_id: map.keys().nth(rng.gen_range(0, map.len())).unwrap().clone(),
            ad_type: String::from(*rng.choose(&AD_TYPES).unwrap()),
            event_type: String::from(*rng.choose(&EVENT_TYPES).unwrap()),
            event_time: time,
            ip_address: String::from("0.0.0.0"),
        }
    }
}

impl TestImpl for YSB {
    type D = Event;
    type DO = (String, usize);
    type T = usize;

    fn name(&self) -> &str { "Yahoo Streaming Benchmark" }

    fn generate_data(&self, config: &Config) -> Result<()> {
        let data_dir = format!("{}/ysb", config.get_or("data-dir", "data"));
        let partitions = config.get_as_or("partitions", 10);
        let campaigns = config.get_as_or("campaigns", 100);
        let ads = config.get_as_or("ads", 10);
        let seconds = config.get_as_or("seconds", 60);
        let events_per_second = config.get_as_or("events-per-second", 100_000);
        fs::create_dir_all(&data_dir)?;

        println!("Generating {} events/s for {}s over {} partitions for {} campaigns with {} ads each.",
                 events_per_second, seconds, partitions, campaigns, ads);
        
        // Generate campaigns map
        {
            let mut map = self.campaign_map.write().unwrap();
            for _ in 0..campaigns {
                let campaign_id = format!("{}", Uuid::new_v4());
                for _ in 0..ads {
                    let ad_id = format!("{}", Uuid::new_v4());
                    map.insert(ad_id, campaign_id.clone());
                }
            }
        }
        let campaign_file = File::create(format!("{}/campaigns.json", &data_dir))?;
        let map = self.campaign_map.read().unwrap();
        serde_json::to_writer(campaign_file, map.deref())?;
        
        // Generate events
        let mut rng = rand::thread_rng();
        let mut event_files = Vec::new();
        for p in 0..partitions {
            event_files.push(File::create(format!("{}/events-{}.json", &data_dir, p))?);
        }
        let timestep = 1000.0 / events_per_second as f64;
        let mut time: f64 = 1.0;
        for _ in 0..(seconds*events_per_second) {
            let mut file = rng.choose(&event_files).unwrap();
            serde_json::to_writer(file, &self.generate_event(time as usize))?;
            file.write(b"\n")?;
            time += timestep;
        }
        Ok(())
    }

    fn create_endpoints(&self, config: &Config, index: usize, _workers: usize) -> Result<(Vec<Source<Product<RootTimestamp, Self::T>, Self::D>>, Drain<Product<RootTimestamp, Self::T>, Self::DO>)>{
        let mut config = config.clone();
        let data_dir = format!("{}/ysb", config.get_or("data-dir", "data"));
        config.insert("input-file", format!("{}/events-{}.json", &data_dir, index));
        let campaign_file = File::open(format!("{}/campaigns.json", &data_dir))?;
        let mut map: HashMap<String, String> = serde_json::from_reader(campaign_file)?;
        let mut target = self.campaign_map.write().unwrap();
        for (k, v) in map.drain(){ target.insert(k, v); }
        let int: Result<_> = config.clone().into();
        let out: Result<_> = config.clone().into();
        Ok((vec!(int?), out?))
    }

    fn construct_dataflow<'scope>(&self, _c: &Config, stream: &Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>) -> Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO> {
        let table = self.campaign_map.read().unwrap().clone();;
        stream
            .filter(|x: &Event| x.event_type == "view")
            .map(|x| (x.ad_id, x.event_time))
            .map(move |(ad_id, _)|
                 match table.get(&ad_id){
                     Some(id) => id.clone(),
                     None => String::from("UNKNOWN AD")
                 })
            .epoch_window(10, 10)
            .reduce_by(|campaign_id| campaign_id.clone(), 0, |_, count| count+1)
    }
}

pub fn ysb() -> Vec<Box<Test>>{
    vec![Box::new(YSB::new())]
}

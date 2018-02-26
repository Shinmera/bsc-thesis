extern crate serde_json;
extern crate rand;
extern crate uuid;
use std::io::{Result, Error, ErrorKind, Lines, BufRead, BufReader, Write};
use std::collections::HashMap;
use std::fs::File;
use std::fs;
use std::sync::RwLock;
use std::ops::Deref;
use abomonation::Abomonation;
use timely::dataflow::operators::{Input, Map, Filter};
use timely::dataflow::operators::input::Handle;
use timely::dataflow::scopes::{Root, Child};
use timely::dataflow::{Stream};
use timely_communication::allocator::Generic;
use operators::{EpochWindow, Reduce};
use test::Test;
use test::TestImpl;
use config::Config;
use rand::Rng;
use uuid::Uuid;

#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
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

struct YSB {
    campaign_map: RwLock<HashMap<String, String>>,
    data_dir: String,
    partitions: usize,
    campaigns: usize,
    ads: usize,
    seconds: usize,
    events_per_second: usize,
}

impl YSB {
    fn new(config: &Config) -> Self {
        YSB{
            campaign_map: RwLock::new(HashMap::new()),
            data_dir: format!("{}/ysb",config.get_or("data-dir", "data")),
            partitions: config.get_as_or("partitions", 10), 
            campaigns: config.get_as_or("campaigns", 100),
            ads: config.get_as_or("ads", 10),
            seconds: config.get_as_or("seconds", 60),
            events_per_second: config.get_as_or("events-per-second", 100_000),
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
    type G = Lines<BufReader<File>>;

    fn name(&self) -> &str { "Yahoo Streaming Benchmark" }

    fn generate_data(&self) -> Result<()> {
        fs::create_dir_all(&self.data_dir)?;

        println!("Generating {} events/s for {}s over {} partitions for {} campaigns with {} ads each.",
                 self.events_per_second, self.seconds, self.partitions, self.campaigns, self.ads);
        
        // Generate campaigns map
        {
            let mut map = self.campaign_map.write().unwrap();
            for _ in 0..self.campaigns {
                let campaign_id = format!("{}", Uuid::new_v4());
                for _ in 0..self.ads {
                    let ad_id = format!("{}", Uuid::new_v4());
                    map.insert(ad_id, campaign_id.clone());
                }
            }
        }
        let campaign_file = File::create(format!("{}/campaigns.json", &self.data_dir))?;
        let map = self.campaign_map.read().unwrap();
        serde_json::to_writer(campaign_file, map.deref())?;
        
        // Generate events
        let mut rng = rand::thread_rng();
        let mut event_files = Vec::new();
        for p in 0..self.partitions {
            event_files.push(File::create(format!("{}/events-{}.json", &self.data_dir, p))?);
        }
        let timestep = 1000.0 / self.events_per_second as f64;
        let mut time: f64 = 1.0;
        for _ in 0..(self.seconds*self.events_per_second) {
            let mut file = rng.choose(&event_files).unwrap();
            serde_json::to_writer(file, &self.generate_event(time as usize))?;
            file.write(b"\n")?;
            time += timestep;
        }
        Ok(())
    }

    fn initial_epoch(&self) -> Self::T { 0 }

    fn prepare(&self, index: usize) -> Result<Self::G> {
        let campaign_file = File::open(format!("{}/campaigns.json", &self.data_dir))?;
        let event_file = File::open(format!("{}/events-{}.json", &self.data_dir, index))?;
        let mut map: HashMap<String, String> = serde_json::from_reader(campaign_file)?;
        let mut target = self.campaign_map.write().unwrap();
        for (k, v) in map.drain(){ target.insert(k, v); }
        Ok(BufReader::new(event_file).lines())
    }

    fn epoch_data(&self, stream: &mut Self::G, epoch: &Self::T) -> Result<Vec<Vec<Self::D>>> {
        let mut data = Vec::new();
        for line in stream {
            let event: Event = serde_json::from_str(&line.unwrap())?;
            // We create second epochs to match up with what they do in YSB.
            if event.event_time / 1000 > *epoch {
                data.push(event);
                return Ok(vec![data]);
            }
            data.push(event);
        }
        if data.is_empty(){
            return Err(Error::new(ErrorKind::Other, "Out of data"));
        } else {
            return Ok(vec![data]);
        }
    }

    fn finish(&self, stream: &mut Self::G) {
        drop(stream);
    }

    fn construct_dataflow<'scope>(&self, scope: &mut Child<'scope, Root<Generic>, Self::T>) -> (Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO>, Vec<Handle<Self::T, Self::D>>) {
        let (input, stream) = scope.new_input();
        let table = self.campaign_map.read().unwrap().clone();
        let stream = stream
            // Filter to view event_type events.
            .filter(|x: &Event| x.event_type == "view")
            // Transform/Project to ad_id and event_time.
            .map(|x| (x.ad_id, x.event_time))
            // Join the ad_id into the campaign_id through a table lookup.
            .map(move |(ad_id, _)|
                 match table.get(&ad_id){
                     Some(id) => id.clone(),
                     None => String::from("UNKNOWN AD")
                 })
            // Aggregate to 10s windows based on 1s epochs.
            .epoch_window(10, 10)
            // Count each campaign in the window and return as tuples of id + count.
            .reduce_by(|campaign_id| campaign_id.clone(), 0, |_, count| count+1);
        (stream, vec![input])
    }
}

pub fn ysb(args: &Config) -> Vec<Box<Test>>{
    vec![Box::new(YSB::new(args))]
}

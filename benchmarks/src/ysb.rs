extern crate serde_json;
extern crate rand;
extern crate uuid;
use std::io::{Result, Error, ErrorKind, Lines, BufRead, BufReader, Write};
use std::collections::HashMap;
use std::fs::File;
use std::fs;
use std::sync::RwLock;
use abomonation::Abomonation;
use timely::dataflow::operators::{Input, Map, Filter};
use timely::dataflow::operators::input::Handle;
use timely::dataflow::scopes::{Root, Child};
use timely::dataflow::{Stream};
use timely_communication::allocator::Generic;
use operators::{EpochWindow};
use test::Test;
use test::TestImpl;
use config::Config;
use rand::Rng;
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

struct YSB {
    campaign_map: RwLock<HashMap<String, String>>,
    data_dir: String,
    campaign_count: usize,
    ad_count: usize,
    event_count: usize,
}

impl TestImpl for YSB {
    type D = Event;
    type DO = Vec<(String, usize)>;
    type T = usize;
    type G = Lines<BufReader<File>>;

    fn new(config: &Config) -> Self {
        YSB{
            campaign_map: RwLock::new(HashMap::new()),
            data_dir: config.get_or("data-dir", "data/ysb/"),
            campaign_count: config.get_or_as("campaigns", 10),
            ad_count: config.get_or_as("ads", 10),
            event_count: config.get_or_as("events", 10)
        }
    }

    fn name(&self) -> &str { "Yahoo Streaming Benchmark" }

    fn generate_data(&self) -> Result<()> {
        fs::create_dir_all(&self.data_dir)?;
        let mut rng = rand::thread_rng();
        // Generate campaigns map
        let campaigns = File::create(format!("{}/campaigns.json", &self.data_dir))?;
        let mut map = HashMap::new();
        for _ in 0..self.campaign_count {
            let campaign_id = format!("{}", Uuid::new_v4());
            for _ in 0..self.ad_count {
                let ad_id = format!("{}", Uuid::new_v4());
                map.insert(ad_id, campaign_id.clone());
            }
        }
        serde_json::to_writer(campaigns, &map)?;
        // Generate events
        let mut events = File::create(format!("{}/events.json", &self.data_dir))?;
        let ad_types = vec!["banner", "modal", "sponsored-search", "mail", "mobile"];
        let event_types = vec!["view", "click", "purchase"];
        let mut time = 1000000;
        for _ in 0..self.event_count {
            // We step randomly between event times, with a max of 10 seconds.
            time = time + rng.gen_range(0, 10000);
            let event = Event {
                user_id: format!("{}", Uuid::new_v4()),
                page_id: format!("{}", Uuid::new_v4()),
                ad_id: map.keys().nth(rng.gen_range(0, map.len())).unwrap().clone(),
                ad_type: String::from(*rng.choose(&ad_types).unwrap()),
                event_type: String::from(*rng.choose(&event_types).unwrap()),
                event_time: time,
                ip_address: String::from("0.0.0.0"),
            };
            serde_json::to_writer(&events, &event)?;
            events.write(b"\n")?;
        }
        Ok(())
    }

    fn initial_epoch(&self) -> Self::T { 0 }

    fn prepare(&self, index: usize) -> Result<Self::G> {
        let campaigns = File::open(format!("{}/campaigns.json", &self.data_dir))?;
        let events = File::open(format!("{}/events-{}.json", &self.data_dir, index))?;
        let mut map: HashMap<String, String> = serde_json::from_reader(campaigns)?;
        let mut target = self.campaign_map.write().unwrap();
        for (k, v) in map.drain(){ target.insert(k, v); }
        Ok(BufReader::new(events).lines())
    }

    fn epoch_data(&self, stream: &mut Self::G, epoch: &Self::T) -> Result<Vec<Self::D>> {
        let mut data = Vec::new();
        for line in stream {
            let event: Event = serde_json::from_str(&line.unwrap())?;
            // We create second epochs to match up with what they do in YSB.
            if event.event_time / 1000 > *epoch {
                data.push(event);
                return Ok(data);
            }
            data.push(event);
        }
        if data.is_empty(){
            return Err(Error::new(ErrorKind::Other, "Out of data"));
        } else {
            return Ok(data);
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
            .map(|x| {
                let mut counts = HashMap::new();
                for campaign_id in x {
                    let count = counts.get(&campaign_id).unwrap_or(&0)+1;
                    counts.insert(campaign_id, count);
                }
                // Not sure why I need to do this, Rust complains if I return directly.
                let data = counts.drain().collect::<Vec<_>>();
                data
            });
        (stream, vec![input])
    }
}

pub fn ysb(args: &Config) -> Vec<Box<Test>>{
    vec![Box::new(YSB::new(args))]
}

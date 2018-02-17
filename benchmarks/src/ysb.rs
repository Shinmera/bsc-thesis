extern crate serde_json;
use std::collections::HashMap;
use std::fs::File;
use abomonation::Abomonation;
use timely::dataflow::operators::{Input, Map, Filter};
use timely::dataflow::operators::input::Handle;
use timely::dataflow::scopes::{Root, Child};
use timely::dataflow::{Stream};
use timely_communication::allocator::Generic;
use operators::{EpochWindow};
use test::Test;
use test::TestImpl;
use std::io::{Result, Error, ErrorKind, Lines, BufRead, BufReader};
use getopts::Options;

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
    campaign_map: HashMap<String, String>,
    campaign_file: String,
    event_file: String,
    event_stream: Option<Lines<BufReader<File>>>
}

impl TestImpl for YSB {
    type D = Event;
    type DO = Vec<(String, usize)>;
    type T = usize;

    fn new(args: &[String]) -> Self {
        let mut opts = Options::new();
        let mut dir = String::from("data/");
        opts.optopt("d", "data", "Specify the data directory.", "DIR");
        if let Ok(matches) = opts.parse(args){
            dir = matches.opt_str("d").unwrap_or(dir);
        }
        YSB{
            campaign_map: HashMap::new(),
            campaign_file: format!("{}/ysb-campaigns.json", dir),
            event_file: format!("{}/ysb-events.json", dir),
            event_stream: None
        }
    }

    fn name(&self) -> &str { "Yahoo Streaming Benchmark" }

    fn generate_data(&self) -> Result<()> {
        // FIXME
        Ok(())
    }

    fn initial_epoch(&self) -> Self::T { 0 }

    fn prepare(&mut self, index: usize) -> Result<bool> {
        if index != 0 { return Ok(false); }
        
        let campaigns = File::open(&self.campaign_file)?;
        let events = File::open(&self.event_file)?;
        let mut map: HashMap<String, String> = serde_json::from_reader(campaigns)?;
        for (k, v) in map.drain(){ self.campaign_map.insert(k, v); }
        self.event_stream = Some(BufReader::new(events).lines());
        Ok(true)
    }

    fn epoch_data(&mut self, epoch: &Self::T) -> Result<Vec<Self::D>> {
        let mut data = Vec::new();
        let events = self.event_stream.as_mut().unwrap();
        for line in events {
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

    fn finish(&mut self) {
        self.event_stream = None;
    }

    fn construct_dataflow<'scope>(&self, scope: &mut Child<'scope, Root<Generic>, Self::T>) -> (Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO>, Vec<Handle<Self::T, Self::D>>) {
        let (input, stream) = scope.new_input();
        let table = self.campaign_map.clone();
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

pub fn ysb(args: &[String]) -> Vec<Box<Test>>{
    vec![Box::new(YSB::new(args))]
}

use std::collections::HashMap;
use abomonation::Abomonation;
use timely::dataflow::operators::{Input, Map, Filter};
use timely::dataflow::operators::input::Handle;
use timely::dataflow::scopes::{Root, Child};
use timely::dataflow::{Stream};
use timely_communication::allocator::Generic;
use operators::{EpochWindow};
use test::Test;
use test::TestImpl;

#[derive(Eq, PartialEq, Clone)]
struct RawData {
    user_id: String,
    page_id: String,
    ad_id: String,
    ad_type: String,
    event_type: String,
    event_time: usize,
    ip_address: String,
}

unsafe_abomonate!(RawData : user_id, page_id, ad_id, ad_type, event_type, event_time, ip_address);

struct YSB {
    campaign_map: HashMap<String, String>
}

impl TestImpl for YSB {
    type D = RawData;
    type DO = Vec<(String, usize)>;
    type T = usize;

    fn name(&self) -> &str { "Yahoo Streaming Benchmark" }

    fn initial_epoch(&self) -> Self::T { 0 }

    fn prepare_data(&self, _index: usize) -> Result<bool, String> {
        // FIXME: fill campaign_map
        Ok(false)
    }

    fn construct_dataflow<'scope>(&self, scope: &mut Child<'scope, Root<Generic>, Self::T>) -> (Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO>, Vec<Handle<Self::T, Self::D>>) {
        let (input, stream) = scope.new_input();
        let stream = stream
            // Filter to view event_type events.
            .filter(|x: &RawData| x.event_type == "view")
            // Transform/Project to ad_id and event_time.
            .map(|x| (x.ad_id, x.event_time))
            // Join the ad_id into the campaign_id through a table lookup.
            .map(|(ad_id, _)|
                 match self.campaign_map.get(&ad_id){
                     Some(id) => id.clone(),
                     None => String::from("UNKNOWN AD")
                 })
            // Aggregate to a window based on timed epochs.
            .epoch_window(10, 10)
            // Count each campaign in the window and return as tuples of id + count.
            .map(|x| {
                let counts = HashMap::new();
                for campaign_id in x {
                    counts.insert(campaign_id, counts.get(&campaign_id).unwrap_or(&0)+1);
                }
                counts.drain().collect::<Vec<_>>()
            });
        (stream, vec![input])
    }
}

pub fn ysb() -> Vec<Box<Test>>{
    vec![Box::new(YSB{campaign_map: HashMap::new()})]
}

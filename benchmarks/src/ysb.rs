use std::collections::HashMap;
use abomonation::Abomonation;
use timely::dataflow::operators::{Input, Map, Filter};
use timely::dataflow::operators::input::Handle;
use timely::dataflow::scopes::{Root, Child};
use timely::dataflow::{Stream};
use timely_communication::allocator::Generic;
use operators::{EpochWindow, Join};
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

#[derive(Eq, PartialEq, Clone)]
struct Campaign {
    ad_id: String,
    campaign_id: String,
}

unsafe_abomonate!(Campaign : ad_id, campaign_id);

struct YSB {}
impl TestImpl for YSB {
    type D = RawData;
    type DO = Vec<(String, usize)>;
    type T = usize;

    fn name(&self) -> &str { "Yahoo Streaming Benchmark" }

    fn initial_epoch(&self) -> Self::T { 0 }

    fn construct_dataflow<'scope>(&self, scope: &mut Child<'scope, Root<Generic>, Self::T>) -> (Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO>, Vec<Handle<Self::T, Self::D>>) {
        let (input, stream) = scope.new_input();
        let (table_input, table_stream) = scope.new_input();
        let stream = stream
            .filter(|x: &RawData| x.event_type == "view")
            .map(|x| (x.ad_id, x.event_time))
            .join(&table_stream, |&(ad_id, _)| ad_id, |y: &Campaign| y.ad_id, |x, y| y.campaign_id)
            .epoch_window(10, 10)
            .map(|x| {
                let counts = HashMap::new();
                for campaign_id in x {
                    counts.insert(campaign_id, counts.get(&campaign_id).unwrap_or(&0)+1);
                }
                counts.drain().collect::<Vec<_>>()
            });
        (stream, vec![input, table_input])
    }
}

pub fn ysb() -> Vec<Box<Test>>{
    vec![Box::new(YSB{})]
}

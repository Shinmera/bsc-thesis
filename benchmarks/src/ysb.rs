use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use timely::dataflow::operators::{Input, Exchange, Map};
use timely::dataflow::operators::input::Handle;
use timely::dataflow::scopes::{Root, Child};
use timely::dataflow::{Stream};
use timely_communication::allocator::Generic;
use operators::RollingCount;
use operators::EpochWindow;
use Test::Test;
use Test::TestImpl;

struct RawData {
    user_id: String,
    page_id: String,
    ad_id: String,
    ad_type: String,
    event_type: String,
    event_time: Timestamp,
    ip_address: String,
}

struct Campaign {
    ad_id: String,
    campaign_id: String,
}

struct YSB {}
impl TestImpl for YSB {
    type D = RawData;
    type DO = (String, usize);
    type T = usize;

    fn name(&self) -> &str { "Yahoo Streaming Benchmark" }

    fn initial_epoch(&self) -> Self::T { 0 }

    fn construct_dataflow<'scope>(&self, scope: &mut Child<'scope, Root<Generic>, Self::T>) -> (Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO>, Vec<Handle<Self::T, Self::D>>) {
        let (input, stream) = scope.new_input();
        let (table_input, table_stream) = scope.new_input();
        let stream = stream
            .filter(|x| x.event_type == "view")
            .map(|x| (x.ad_id, x.event_time))
            .join(table_stream, |(ad_id, _)| ad_id, |y| y.ad_id, |x, y| y.campaign_id)
            .epoch_window(10, 10)
            .flat_map(|x| x)
            .rolling_count(|x| (x.campaign_id, 1))
        (stream, vec![input, table_input])
    }
}

pub fn ysb() -> Vec<Box<Test>>{
    vec![Box::new(YSB{})];
}

use serde_json;
use abomonation::Abomonation;
use config::Config;
use endpoint::{self, Source, Drain, ToData, FromData, EventSource};
use operators::{Window, Reduce, Join, FilterMap, Session, Partition};
use rand::{Rng, StdRng, SeedableRng};
use std::char::from_u32;
use std::cmp::{max, min};
use std::collections::HashMap;
use std::f64::consts::PI;
use std::fs::File;
use std::fs;
use std::time::Instant;
use std::thread::{self, JoinHandle};
use std::io::{Result, Write};
use test::{Test, TestImpl, Benchmark};
use timely::dataflow::Stream;
use timely::dataflow::operators::{Filter, Map, Binary};
use timely::dataflow::scopes::{Root, Child};
use timely::progress::timestamp::{Timestamp, RootTimestamp};
use timely_communication::allocator::Generic;
use timely::dataflow::channels::pact::Exchange;

type Id = usize;
type Date = usize;

const MIN_STRING_LENGTH: usize = 3;
const NUM_CATEGORIES: usize = 5;
const AUCTION_ID_LEAD: usize = 10;
const HOT_SELLER_RATIO: usize = 100;
const HOT_AUCTION_RATIO: usize = 100;
const HOT_BIDDER_RATIO: usize = 100;
// WARNING: You might think that you can just change these three proportion values
//          linearly as you please as long as their relation does not change without
//          the results changing, but this is /not/ the case. For instance, changing
//          these values to 2, 6, 92 to make it a denominator of 100 actually changes
//          the results even though the proportions are the same relatively.
const PERSON_PROPORTION: usize = 1;
const AUCTION_PROPORTION: usize = 3;
const BID_PROPORTION: usize = 46;
const PROPORTION_DENOMINATOR: usize = PERSON_PROPORTION + AUCTION_PROPORTION + BID_PROPORTION;
const FIRST_AUCTION_ID: usize = 1000;
const FIRST_PERSON_ID: usize = 1000;
const FIRST_CATEGORY_ID: usize = 10;
const PERSON_ID_LEAD: usize = 10;
const SINE_APPROX_STEPS: usize = 10;
const BASE_TIME: usize = 1436918400_000; // 2015-07-15T00:00:00.000Z
const US_STATES: [&str; 6] = ["AZ","CA","ID","OR","WA","WY"];
const US_CITIES: [&str; 10] = ["Phoenix", "Los Angeles", "San Francisco", "Boise", "Portland", "Bend", "Redmond", "Seattle", "Kent", "Cheyenne"];
const FIRST_NAMES: [&str; 11] = ["Peter", "Paul", "Luke", "John", "Saul", "Vicky", "Kate", "Julie", "Sarah", "Deiter", "Walter"];
const LAST_NAMES: [&str; 9] = ["Shultz", "Abrams", "Spencer", "White", "Bartels", "Walton", "Smith", "Jones", "Noris"];

trait NEXMarkRng {
    fn gen_string(&mut self, usize) -> String;
    fn gen_price(&mut self) -> usize;
}

impl NEXMarkRng for StdRng {
    fn gen_string(&mut self, max: usize) -> String {
        let len = self.gen_range(MIN_STRING_LENGTH, max);
        String::from((0..len).map(|_|{
            if self.gen_range(0, 13) == 0 { String::from(" ") }
            else { from_u32('a' as u32+self.gen_range(0, 26)).unwrap().to_string() }
        }).collect::<Vec<String>>().join("").trim())
    }

    fn gen_price(&mut self) -> usize {
        (10.0_f32.powf(self.gen::<f32>() * 6.0) * 100.0).round() as usize
    }
}

#[derive(PartialEq)]
enum RateShape {
    Square,
    Sine,
}

#[derive(Clone)]
struct NEXMarkConfig {
    active_people: usize,
    in_flight_auctions: usize,
    out_of_order_group_size: usize,
    hot_seller_ratio: usize,
    hot_auction_ratio: usize,
    hot_bidder_ratio: usize,
    first_event_id: usize,
    first_event_number: usize,
    base_time: usize,
    step_length: usize,
    events_per_epoch: usize,
    epoch_period: f32,
    inter_event_delays: Vec<f32>,
}

impl NEXMarkConfig {
    fn new(config: &Config) -> Self{
        let rate_shape = if config.get_or("rate-shape", "sine") == "sine"{ RateShape::Sine }else{ RateShape::Square };
        // Calculate inter event delays array.
        let mut inter_event_delays = Vec::new();
        let first_rate = config.get_as_or("first-event-rate", config.get_as_or("events-per-second", 10_000));
        let next_rate = config.get_as_or("next-event-rate", first_rate);
        let us_per_unit = config.get_as_or("us-per-unit", 1_000_000); // Rate is in μs
        let generators = config.get_as_or("threads", 1) as f32;
        let rate_to_period = |r| (us_per_unit) as f32 / r as f32;
        if first_rate == next_rate {
            inter_event_delays.push(rate_to_period(first_rate) * generators);
        } else {
            match rate_shape {
                RateShape::Square => {
                    inter_event_delays.push(rate_to_period(first_rate) * generators);
                    inter_event_delays.push(rate_to_period(next_rate) * generators);
                },
                RateShape::Sine => {
                    let mid = (first_rate + next_rate) as f64 / 2.0;
                    let amp = (first_rate - next_rate) as f64 / 2.0;
                    for i in 0..SINE_APPROX_STEPS {
                        let r = (2.0 * PI * i as f64) / SINE_APPROX_STEPS as f64;
                        let rate = mid + amp * r.cos();
                        inter_event_delays.push(rate_to_period(rate.round() as usize) * generators);
                    }
                }
            }
        }
        // Calculate events per epoch and epoch period.
        let n = if rate_shape == RateShape::Square { 2 } else { SINE_APPROX_STEPS };
        let step_length = (config.get_as_or("rate-period", 600) + n - 1) / n;
        let mut events_per_epoch = 0;
        let mut epoch_period = 0.0;
        if inter_event_delays.len() > 1 {
            for inter_event_delay in &inter_event_delays {
                let num_events_for_this_cycle = (step_length * 1_000_000) as f32 / inter_event_delay;
                events_per_epoch += num_events_for_this_cycle.round() as usize;
                epoch_period += (num_events_for_this_cycle * inter_event_delay) / 1000.0;
            }
        }
        NEXMarkConfig {
            active_people: config.get_as_or("active-people", 1000),
            in_flight_auctions: config.get_as_or("in-flight-auctions", 100),
            out_of_order_group_size: config.get_as_or("out-of-order-group-size", 1),
            hot_seller_ratio: config.get_as_or("hot-seller-ratio", 4),
            hot_auction_ratio: config.get_as_or("hot-auction-ratio", 2),
            hot_bidder_ratio: config.get_as_or("hot-bidder-ratio", 4),
            first_event_id: config.get_as_or("first-event-id", 0),
            first_event_number: config.get_as_or("first-event-number", 0),
            base_time: config.get_as_or("base-time", BASE_TIME),
            step_length: step_length,
            events_per_epoch: events_per_epoch,
            epoch_period: epoch_period,
            inter_event_delays: inter_event_delays,
        }
    }

    fn event_timestamp(&self, event_number: usize) -> usize {
        if self.inter_event_delays.len() == 1 {
            return self.base_time + ((event_number as f32 * self.inter_event_delays[0]) / 1000.0).round() as usize;
        }

        let epoch = event_number / self.events_per_epoch;
        let mut event_i = event_number % self.events_per_epoch;
        let mut offset_in_epoch = 0.0;
        for inter_event_delay in &self.inter_event_delays {
            let num_events_for_this_cycle = (self.step_length * 1_000_000) as f32 / inter_event_delay;
            if self.out_of_order_group_size < num_events_for_this_cycle.round() as usize {
                let offset_in_cycle = event_i as f32 * inter_event_delay;
                return self.base_time + (epoch as f32 * self.epoch_period + offset_in_epoch + offset_in_cycle / 1000.0).round() as usize;
            }
            event_i -= num_events_for_this_cycle.round() as usize;
            offset_in_epoch += (num_events_for_this_cycle * inter_event_delay) / 1000.0;
        }
        return 0
    }

    fn next_adjusted_event(&self, events_so_far: usize) -> usize {
        let n = self.out_of_order_group_size;
        let event_number = self.first_event_number + events_so_far;
        (event_number / n) * n + (event_number * 953) % n
    }
}

#[derive(Serialize, Deserialize, Abomonation, Debug)]
struct EventCarrier {
    time: Date,
    event: Event,
}

#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug, Abomonation)]
#[serde(tag = "type")]
enum Event {
    Person(Person),
    Auction(Auction),
    Bid(Bid),
}

impl Event {
    fn new(events_so_far: usize, nex: &mut NEXMarkConfig) -> Self {
        let rem = nex.next_adjusted_event(events_so_far) % PROPORTION_DENOMINATOR;
        let timestamp = nex.event_timestamp(nex.next_adjusted_event(events_so_far));
        let id = nex.first_event_id + nex.next_adjusted_event(events_so_far);
        let mut rng = StdRng::from_seed(&[id]);

        if rem < PERSON_PROPORTION {
            Event::Person(Person::new(id, timestamp, &mut rng))
        } else if rem < PERSON_PROPORTION + AUCTION_PROPORTION {
            Event::Auction(Auction::new(events_so_far, id, timestamp, &mut rng, nex))
        } else {
            Event::Bid(Bid::new(id, timestamp, &mut rng, nex))
        }
    }
}

impl ToData<usize, Event> for String{
    fn to_data(self) -> Result<(usize, Event)> {
        serde_json::from_str(&self)
            .map(|c: EventCarrier| (c.time, c.event))
            .map_err(|e| e.into())
    }
}

impl FromData<usize> for Event {
    fn from_data(&self, t: &usize) -> String {
        serde_json::to_string(&EventCarrier{ time: t.clone(), event: self.clone()}).unwrap()
    }
}

#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug, Abomonation)]
struct Person{
    id: Id,
    name: String,
    email_address: String,
    credit_card: String,
    city: String,
    state: String,
    date_time: Date
}

impl Person {
    fn from(event: Event) -> Option<Person> {
        match event {
            Event::Person(p) => Some(p),
            _ => None
        }
    }
    
    fn new(id: usize, time: Date, rng: &mut StdRng) -> Self {
        Person {
            id: Self::last_id(id) + FIRST_PERSON_ID,
            name: format!("{} {}", *rng.choose(&FIRST_NAMES).unwrap(), *rng.choose(&LAST_NAMES).unwrap()),
            email_address: format!("{}@{}.com", rng.gen_string(7), rng.gen_string(5)),
            credit_card: (0..4).map(|_| format!("{:04}", rng.gen_range(0, 10000))).collect::<Vec<String>>().join(" "),
            city: String::from(*rng.choose(&US_CITIES).unwrap()),
            state: String::from(*rng.choose(&US_STATES).unwrap()),
            date_time: time,
        }
    }

    fn next_id(id: usize, rng: &mut StdRng, nex: &NEXMarkConfig) -> Id {
        let people = Self::last_id(id) + 1;
        let active = min(people, nex.active_people);
        people - active + rng.gen_range(0, active + PERSON_ID_LEAD)
    }

    fn last_id(id: usize) -> Id {
        let epoch = id / PROPORTION_DENOMINATOR;
        let mut offset = id % PROPORTION_DENOMINATOR;
        if PERSON_PROPORTION <= offset { offset = PERSON_PROPORTION - 1; }
        epoch * PERSON_PROPORTION + offset
    }
}

#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
struct Auction{
    id: Id,
    item_name: String,
    description: String,
    initial_bid: usize,
    reserve: usize,
    date_time: Date,
    expires: usize,
    seller: Id,
    category: Id,
}
unsafe_abomonate!(Auction : id, item_name, description, initial_bid, reserve, date_time, expires, seller, category);

impl Auction {
    fn from(event: Event) -> Option<Auction> {
        match event {
            Event::Auction(p) => Some(p),
            _ => None
        }
    }
    
    fn new(events_so_far: usize, id: usize, time: Date, rng: &mut StdRng, nex: &NEXMarkConfig) -> Self {
        let initial_bid = rng.gen_price();
        let seller = if rng.gen_range(0, nex.hot_seller_ratio) > 0 {
            (Person::last_id(id) / HOT_SELLER_RATIO) * HOT_SELLER_RATIO
        } else {
            Person::next_id(id, rng, nex)
        };
        Auction {
            id: Self::last_id(id) + FIRST_AUCTION_ID,
            item_name: rng.gen_string(20),
            description: rng.gen_string(100),
            initial_bid: initial_bid,
            reserve: initial_bid + rng.gen_price(),
            date_time: time,
            expires: time + Self::next_length(events_so_far, rng, time, nex),
            seller: seller + FIRST_PERSON_ID,
            category: FIRST_CATEGORY_ID + rng.gen_range(0, NUM_CATEGORIES),
        }
    }

    fn next_id(id: usize, rng: &mut StdRng, nex: &NEXMarkConfig) -> Id {
        let max_auction = Self::last_id(id);
        let min_auction = if max_auction < nex.in_flight_auctions { 0 } else { max_auction - nex.in_flight_auctions };
        min_auction + rng.gen_range(0, max_auction - min_auction + 1 + AUCTION_ID_LEAD)
    }

    fn last_id(id: usize) -> Id {
        let mut epoch = id / PROPORTION_DENOMINATOR;
        let mut offset = id % PROPORTION_DENOMINATOR;
        if offset < PERSON_PROPORTION {
            epoch -= 1;
            offset = AUCTION_PROPORTION - 1;
        } else if PERSON_PROPORTION + AUCTION_PROPORTION <= offset {
            offset = AUCTION_PROPORTION - 1;
        } else {
            offset -= PERSON_PROPORTION;
        }
        epoch * AUCTION_PROPORTION + offset
    }

    fn next_length(events_so_far: usize, rng: &mut StdRng, time: Date, nex: &NEXMarkConfig) -> Date {
        let current_event = nex.next_adjusted_event(events_so_far);
        let events_for_auctions = (nex.in_flight_auctions * PROPORTION_DENOMINATOR) / AUCTION_PROPORTION;
        let future_auction = nex.event_timestamp(current_event+events_for_auctions);
        
        let horizon = future_auction - time;
        1 + rng.gen_range(0, max(horizon * 2, 1))
    }
}

#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
struct Bid{
    auction: Id,
    bidder: Id,
    price: usize,
    date_time: Date,
}
unsafe_abomonate!(Bid : auction, bidder, price, date_time);

impl Bid {
    fn from(event: Event) -> Option<Bid> {
        match event {
            Event::Bid(p) => Some(p),
            _ => None
        }
    }
    
    fn new(id: usize, time: Date, rng: &mut StdRng, nex: &NEXMarkConfig) -> Self {
        let auction = if 0 < rng.gen_range(0, nex.hot_auction_ratio){
            (Auction::last_id(id) / HOT_AUCTION_RATIO) * HOT_AUCTION_RATIO
        } else {
            Auction::next_id(id, rng, nex)
        };
        let bidder = if 0 < rng.gen_range(0, nex.hot_bidder_ratio) {
            (Person::last_id(id) / HOT_BIDDER_RATIO) * HOT_BIDDER_RATIO + 1
        } else {
            Person::next_id(id, rng, nex)
        };
        Bid {
            auction: auction + FIRST_AUCTION_ID,
            bidder: bidder + FIRST_PERSON_ID,
            price: rng.gen_price(),
            date_time: time,
        }
    }
}

struct Query0 {}

impl Query0 {
    fn new() -> Self {
        Query0 {}
    }
}

impl TestImpl for Query0 {
    type T = Date;
    type D = Event;
    type DO = Event;

    fn name(&self) -> &str { "NEXMark Query 0" }
    
    fn create_endpoints(&self, config: &Config, _index: usize, _workers: usize) -> Result<(Source<Self::T, Self::D>, Drain<Self::T, Self::DO>)> {
        Ok((Source::from_config(config, Source::new(Box::new(NEXMarkGenerator::new(config))))?,
            Drain::from_config(config)?))
    }

    fn construct_dataflow<'scope>(&self, _c: &Config, stream: &Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>) -> Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO> {
        stream.map(|e| e)
    }
}

struct Query1 {}

impl Query1 {
    fn new() -> Self {
        Query1 {}
    }
}

impl TestImpl for Query1 {
    type T = Date;
    type D = Event;
    type DO = (Id, Id, usize, Date);

    fn name(&self) -> &str { "NEXMark Query 1" }
    
    fn create_endpoints(&self, config: &Config, _index: usize, _workers: usize) -> Result<(Source<Self::T, Self::D>, Drain<Self::T, Self::DO>)> {
        Ok((Source::from_config(config, Source::new(Box::new(NEXMarkGenerator::new(config))))?,
            Drain::from_config(config)?))
    }

    fn construct_dataflow<'scope>(&self, _c: &Config, stream: &Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>) -> Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO> {
        stream
            .filter_map(|e| Bid::from(e))
            .map(|b| (b.auction, b.bidder, (b.price*89)/100, b.date_time))
    }
}

impl FromData<usize> for (Id, Id, usize, Date) {
    fn from_data(&self, t: &usize) -> String {
        format!("{} {:?}", t, self)
    }
}

struct Query2 {}

impl Query2 {
    fn new() -> Self {
        Query2 { }
    }
}

impl TestImpl for Query2 {
    type T = Date;
    type D = Event;
    type DO = (Id, usize);

    fn name(&self) -> &str { "NEXMark Query 2" }
    
    fn create_endpoints(&self, config: &Config, _index: usize, _workers: usize) -> Result<(Source<Self::T, Self::D>, Drain<Self::T, Self::DO>)> {
        Ok((Source::from_config(config, Source::new(Box::new(NEXMarkGenerator::new(config))))?,
            Drain::from_config(config)?))
    }

    fn construct_dataflow<'scope>(&self, config: &Config, stream: &Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>) -> Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO> {
        let auction_skip = config.get_as_or("auction-skip", 123);
        stream
            .filter_map(|e| Bid::from(e))
            .filter(move |b| b.auction % auction_skip == 0)
            .map(|b| (b.auction, b.price))
    }
}

impl FromData<usize> for (Id, usize) {
    fn from_data(&self, t: &usize) -> String {
        format!("{} {:?}", t, self)
    }
}

struct Query3 {}

impl Query3 {
    fn new() -> Self {
        Query3 {}
    }
}

impl TestImpl for Query3 {
    type T = Date;
    type D = Event;
    type DO = (String, String, String, Id);

    fn name(&self) -> &str { "NEXMark Query 3" }
    
    fn create_endpoints(&self, config: &Config, _index: usize, _workers: usize) -> Result<(Source<Self::T, Self::D>, Drain<Self::T, Self::DO>)> {
        Ok((Source::from_config(config, Source::new(Box::new(NEXMarkGenerator::new(config))))?,
            Drain::from_config(config)?))
    }

    fn construct_dataflow<'scope>(&self, _c: &Config, stream: &Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>) -> Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO> {
        let auctions = stream
            .filter_map(|e| Auction::from(e))
            .filter(|a| a.category == 10);
        
        let persons = stream
            .filter_map(|e| Person::from(e))
            .filter(|p| p.state=="OR" || p.state=="ID" || p.state=="CA");
        
        persons.left_join(&auctions, |p| p.id, |a| a.seller,
                          |p, a| (p.name, p.city, p.state, a.id))
    }
}

impl FromData<usize> for (String, String, String, Id) {
    fn from_data(&self, t: &usize) -> String {
        format!("{} {:?}", t, self)
    }
}

struct Query4 {}

impl Query4 {
    fn new() -> Self {
        Query4 { }
    }
}

impl TestImpl for Query4 {
    type T = Date;
    type D = Event;
    type DO = (usize, f32);

    fn name(&self) -> &str { "NEXMark Query 4" }
    
    fn create_endpoints(&self, config: &Config, _index: usize, _workers: usize) -> Result<(Source<Self::T, Self::D>, Drain<Self::T, Self::DO>)> {
        Ok((Source::from_config(config, Source::new(Box::new(NEXMarkGenerator::new(config))))?,
            Drain::from_config(config)?))
    }

    fn construct_dataflow<'scope>(&self, _c: &Config, stream: &Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>) -> Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO> {
        hot_bids(stream)
            .average_by(|&(ref a, _)| a.category, |(_, p)| p)
    }
}

struct Query5 {}

impl Query5 {
    fn new() -> Self {
        Query5 {}
    }
}

impl TestImpl for Query5 {
    type T = Date;
    type D = Event;
    type DO = (Id, usize);

    fn name(&self) -> &str { "NEXMark Query 5" }
    
    fn create_endpoints(&self, config: &Config, _index: usize, _workers: usize) -> Result<(Source<Self::T, Self::D>, Drain<Self::T, Self::DO>)> {
        Ok((Source::from_config(config, Source::new(Box::new(NEXMarkGenerator::new(config))))?,
            Drain::from_config(config)?))
    }

    fn construct_dataflow<'scope>(&self, config: &Config, stream: &Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>) -> Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO> {
        let window_size = config.get_as_or("window-size", 10) as usize;
        let window_slide = config.get_as_or("window-slide", 5) as usize;
        
        let bids = stream
            .filter_map(|e| Bid::from(e))
            .epoch_window(window_size, window_slide)
            .reduce_by(|b| b.auction, 0, |_, c| c+1);
        
        let max = bids.reduce_to(0, |(_, p), c| max(p, c));
        
        max.epoch_join(&bids, |_| 0, |_| 0, |m, (a, c)| (a, c, m))
            .filter(|&(_, c, m)| c == m)
            .map(|(a, c, _)| (a, c))
    }
}

struct Query6 {}

impl Query6 {
    fn new() -> Self {
        Query6 {}
    }
}

impl TestImpl for Query6 {
    type T = Date;
    type D = Event;
    type DO = (Id, f32);

    fn name(&self) -> &str { "NEXMark Query 6" }
    
    fn create_endpoints(&self, config: &Config, _index: usize, _workers: usize) -> Result<(Source<Self::T, Self::D>, Drain<Self::T, Self::DO>)> {
        Ok((Source::from_config(config, Source::new(Box::new(NEXMarkGenerator::new(config))))?,
            Drain::from_config(config)?))
    }

    fn construct_dataflow<'scope>(&self, _c: &Config, stream: &Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>) -> Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO> {
        hot_bids(stream)
            .partition(10, |&(ref a, _)| a.seller)
            .map(|p| (p[0].1, p.iter().map(|p| p.1 as f32).sum::<f32>() / p.len() as f32))
    }
}

impl FromData<usize> for (Id, f32) {
    fn from_data(&self, t: &usize) -> String {
        format!("{} {:?}", t, self)
    }
}

struct Query7 {}

impl Query7 {
    fn new() -> Self {
        Query7 {}
    }
}

impl TestImpl for Query7 {
    type T = Date;
    type D = Event;
    type DO = (Id, usize, Id);

    fn name(&self) -> &str { "NEXMark Query 7" }
    
    fn create_endpoints(&self, config: &Config, _index: usize, _workers: usize) -> Result<(Source<Self::T, Self::D>, Drain<Self::T, Self::DO>)> {
        Ok((Source::from_config(config, Source::new(Box::new(NEXMarkGenerator::new(config))))?,
            Drain::from_config(config)?))
    }

    fn construct_dataflow<'scope>(&self, config: &Config, stream: &Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>) -> Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO> {
        let window_size = config.get_as_or("window-size", 10) as usize;
        
        stream
            .filter_map(|e| Bid::from(e))
            .tumbling_window(window_size)
            .reduce(|_| 0, (0, 0, 0), |b, (a, p, bi)| {
                if p < b.price { (b.auction, b.price, b.bidder) }
                else { (a, p, bi) }
            }, |_, d, _| d)
    }
}

impl FromData<usize> for (Id, usize, Id) {
    fn from_data(&self, t: &usize) -> String {
        format!("{} {:?}", t, self)
    }
}

struct Query8 {}

impl Query8 {
    fn new() -> Self {
        Query8 {}
    }
}

impl TestImpl for Query8 {
    type T = Date;
    type D = Event;
    type DO = (Id, String, usize);

    fn name(&self) -> &str { "NEXMark Query 8" }
    
    fn create_endpoints(&self, config: &Config, _index: usize, _workers: usize) -> Result<(Source<Self::T, Self::D>, Drain<Self::T, Self::DO>)> {
        Ok((Source::from_config(config, Source::new(Box::new(NEXMarkGenerator::new(config))))?,
            Drain::from_config(config)?))
    }

    fn construct_dataflow<'scope>(&self, config: &Config, stream: &Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>) -> Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO> {
        let window_size = config.get_as_or("window-size", 10) as usize;
        
        let auctions = stream
            .filter_map(|e| Auction::from(e))
            .tumbling_window(window_size);
        
        let persons = stream
            .filter_map(|e| Person::from(e))
            .tumbling_window(window_size);
        
        persons.epoch_join(&auctions, |p| p.id, |a| a.seller,
                           |p, a| (p.id, p.name, a.reserve))
    }
}

impl FromData<usize> for (Id, String, usize) {
    fn from_data(&self, t: &usize) -> String {
        format!("{} {:?}", t, self)
    }
}

struct Query9 {}

impl Query9 {
    fn new() -> Self {
        Query9 {}
    }
}

fn hot_bids<'scope>(stream: &Stream<Child<'scope, Root<Generic>, Date>, Event>) -> Stream<Child<'scope, Root<Generic>, Date>, (Auction, usize)> {
    // FIXME: If bids arrive after the expiry of their auction
    //        they will be retained forever. I'm actually not
    //        sure how to remedy this beyond keeping /some/ data
    //        indefinitely, like each auction's expiry time.
    let bids = stream.filter_map(|e| Bid::from(e));
    let auctions = stream.filter_map(|e| Auction::from(e));
    
    let mut auction_map = HashMap::new();
    let mut bid_map: HashMap<Id, Vec<Bid>> = HashMap::new();
    let auction_ex = Exchange::new(|a: &Auction| a.id as u64);
    let bid_ex = Exchange::new(|b: &Bid| b.auction as u64);
    
    auctions.binary_notify(&bids, auction_ex, bid_ex, "HotBids", Vec::new(), move |input1, input2, output, notificator|{
        input1.for_each(|time, data|{
            data.drain(..).for_each(|a|{
                let future = RootTimestamp::new(a.expires - BASE_TIME);
                let auctions = auction_map.entry(future).or_insert_with(Vec::new);
                auctions.push(a);
                notificator.notify_at(time.delayed(&future));
            });
        });

        input2.for_each(|_, data|{
            data.drain(..).for_each(|b|{
                bid_map.entry(b.auction).or_insert_with(Vec::new).push(b);
            });
        });

        notificator.for_each(|cap, _, _|{
            if let Some(mut auctions) = auction_map.remove(cap.time()) {
                auctions.drain(..).for_each(|a|{
                    if let Some(mut bids) = bid_map.remove(&a.id) {
                        bids.drain(..)
                            .filter(|b| a.reserve <= b.price && b.date_time < a.expires)
                            .map(|b| b.price)
                            .max()
                            .map(|price| output.session(&cap).give((a, price)));
                    }
                });
            }
        });
    })
}

impl TestImpl for Query9 {
    type T = Date;
    type D = Event;
    type DO = (Auction, usize);

    fn name(&self) -> &str { "NEXMark Query 9" }
    
    fn create_endpoints(&self, config: &Config, _index: usize, _workers: usize) -> Result<(Source<Self::T, Self::D>, Drain<Self::T, Self::DO>)> {
        Ok((Source::from_config(config, Source::new(Box::new(NEXMarkGenerator::new(config))))?,
            Drain::from_config(config)?))
    }

    fn construct_dataflow<'scope>(&self, _c: &Config, stream: &Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>) -> Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO> {
        hot_bids(stream)
    }
}

impl<T: Timestamp> FromData<T> for (Auction, usize) {
    fn from_data(&self, t: &T) -> String {
        format!("{:?} {:?}", t, self)
    }
}

struct Query11 {}

impl Query11 {
    fn new() -> Self { Query11{} }
}

impl TestImpl for Query11 {
    type T = Date;
    type D = Event;
    type DO = (Id, usize);

    fn name(&self) -> &str { "NEXMark Query 11" }
    
    fn create_endpoints(&self, config: &Config, _index: usize, _workers: usize) -> Result<(Source<Self::T, Self::D>, Drain<Self::T, Self::DO>)> {
        Ok((Source::from_config(config, Source::new(Box::new(NEXMarkGenerator::new(config))))?,
            Drain::from_config(config)?))
    }

    fn construct_dataflow<'scope>(&self, _c: &Config, stream: &Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>) -> Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO> {
        stream
            .filter_map(|e| Bid::from(e))
            .session(10, |b| (b.bidder, b.date_time / 1000))
            .map(|(b, d)| (b, d.len()))
    }
}

struct Query12 {}

impl Query12 {
    fn new() -> Self { Query12{} }
}

impl TestImpl for Query12 {
    type T = Date;
    type D = Event;
    type DO = (Id, usize);

    fn name(&self) -> &str { "NEXMark Query 12" }
    
    fn create_endpoints(&self, config: &Config, _index: usize, _workers: usize) -> Result<(Source<Self::T, Self::D>, Drain<Self::T, Self::DO>)> {
        Ok((Source::from_config(config, Source::new(Box::new(NEXMarkGenerator::new(config))))?,
            Drain::from_config(config)?))
    }

    fn construct_dataflow<'scope>(&self, _c: &Config, stream: &Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>) -> Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO> {
        let start = Instant::now();
        stream
            .filter_map(|e| Bid::from(e))
            .session(10, move |b| {
                let d = Instant::now().duration_since(start);
                (b.bidder, d.as_secs() as usize)
            })
            .map(|(b, d)| (b, d.len()))
    }
}

// FIXME: Merge this with NEXMarkConfig
#[derive(Clone)]
pub struct NEXMarkGenerator {
    config: NEXMarkConfig,
    events: usize,
    seconds: usize
}

impl NEXMarkGenerator {
    fn new(config: &Config) -> Self {
        NEXMarkGenerator {
            config: NEXMarkConfig::new(config),
            events: 0,
            seconds: config.get_as_or("seconds", 60)
        }
    }
}

impl EventSource<usize, Event> for NEXMarkGenerator {
    fn next(&mut self) -> Result<(usize, Vec<Event>)> {
        let mut data = Vec::with_capacity((1000.0 / self.config.inter_event_delays[0]) as usize);
        let epoch = (self.config.event_timestamp(self.events + self.config.first_event_id) - self.config.base_time) / 1000;
        
        loop {
            let time = self.config.event_timestamp(self.events + self.config.first_event_id);
            let next_epoch = (time - self.config.base_time) / 1000;
            let event = Event::new(self.events, &mut self.config);

            if next_epoch < self.seconds && next_epoch == epoch {
                self.events += 1;
                data.push(event);
            } else {
                break;
            }
        }

        if data.len() == 0 {
            endpoint::out_of_data()
        } else {
            Ok((epoch, data))
        }
    }
}

pub struct NEXMark {}

impl NEXMark {
    pub fn new() -> Self { NEXMark {} }
}

impl Benchmark for NEXMark {

    fn name(&self) -> &str { "NEXMark" }

    fn generate_data(&self, config: &Config) -> Result<()> {
        let data_dir = format!("{}/nexmark",config.get_or("data-dir", "data"));
        fs::create_dir_all(&data_dir)?;
        let seconds = config.get_as_or("seconds", 60);
        let partitions = config.get_as_or("threads", 1);

        println!("Generating events for {}s over {} partitions.", seconds, partitions);

        let generator =  NEXMarkGenerator::new(config);

        let mut threads: Vec<JoinHandle<Result<()>>> = Vec::new();
        for p in 0..partitions {
            let mut file = File::create(format!("{}/events-{}.json", &data_dir, p))?;
            let mut generator = generator.clone();
            threads.push(thread::spawn(move || {
                loop{
                    let (t, d) = generator.next()?;
                    for e in d {
                        serde_json::to_writer(&file, &EventCarrier{ time: t, event: e })?;
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

    fn tests(&self) -> Vec<Box<Test>>{
        vec![Box::new(Query0::new()),
             Box::new(Query1::new()),
             Box::new(Query2::new()),
             Box::new(Query3::new()),
             Box::new(Query4::new()),
             Box::new(Query5::new()),
             Box::new(Query6::new()),
             Box::new(Query7::new()),
             Box::new(Query8::new()),
             Box::new(Query9::new()),
             Box::new(Query11::new()),
             //Box::new(Query12::new())
        ]
    }

}

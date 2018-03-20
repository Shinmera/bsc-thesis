extern crate serde_json;
use abomonation::Abomonation;
use timely::dataflow::operators::{Input, Filter, Map};
use timely::dataflow::scopes::{Root, Child};
use timely::dataflow::Stream;
use timely::Data;
use timely_communication::allocator::Generic;
use test::{Test, TestImpl, InputHandle};
use operators::{Window, Reduce, Join};
use config::Config;
use rand::{Rng, StdRng, SeedableRng};
use std::char::from_u32;
use std::cmp::{max, min};
use std::f64::consts::PI;
use std::fs::File;
use std::fs;
use std::fmt::Debug;
use std::io::{Result, Error, ErrorKind, Lines, BufRead, BufReader, Write};

type Id = usize;
type Date = usize;

const MIN_STRING_LENGTH: usize = 3;
const NUM_CATEGORIES: usize = 5;
const AUCTION_ID_LEAD: usize = 10;
const HOT_SELLER_RATIO: usize = 100;
const HOT_AUCTION_RATIO: usize = 100;
const HOT_BIDDER_RATIO: usize = 100;
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

struct NEXMark {
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
    epoch_period: usize,
    inter_event_delays: Vec<usize>,
    auction_skip: usize,
}

impl NEXMark {
    fn new(config: &Config) -> Self{
        let rate_shape = if config.get_or("rate-shape", "sine") == "sine"{ RateShape::Sine }else{ RateShape::Square };
        // Calculate inter event delays array.
        let mut inter_event_delays = Vec::new();
        let first_rate = config.get_as_or("first-event-rate", 10000);
        let next_rate = config.get_as_or("next-event-rate", 10000);
        let rate = config.get_as_or("rate", 1_000_000); // Rate is in μs
        let generators = config.get_as_or("event-generators", 100);
        let rate_to_period = |r| (rate + r / 2) / r;
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
        let mut epoch_period = 0;
        if inter_event_delays.len() > 1 {
            for inter_event_delay in &inter_event_delays {
                let num_events_for_this_cycle = (step_length * 1_000_000) / inter_event_delay;
                events_per_epoch += num_events_for_this_cycle;
                epoch_period += (num_events_for_this_cycle * inter_event_delay) / 1000;
            }
        }
        NEXMark {
            active_people: config.get_as_or("active-people", 1000),
            in_flight_auctions: config.get_as_or("in-flight-auctions", 100),
            out_of_order_group_size: config.get_as_or("out-of-order-group-size", 11),
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
            auction_skip: config.get_as_or("auction-skip", 123),
        }
    }

    fn event_timestamp(&self, event_number: usize) -> usize {
        if self.inter_event_delays.len() == 1 {
            return self.base_time + (event_number * self.inter_event_delays[0]) / 1000;
        }

        let epoch = event_number / self.events_per_epoch;
        let mut event_i = event_number % self.events_per_epoch;
        let mut offset_in_epoch = 0;
        for inter_event_delay in &self.inter_event_delays {
            let num_events_for_this_cycle = (self.step_length * 1_000_000) / inter_event_delay;
            if self.out_of_order_group_size < num_events_for_this_cycle {
                let offset_in_cycle = event_i * inter_event_delay;
                return self.base_time + epoch * self.epoch_period + offset_in_epoch + offset_in_cycle / 1000;
            }
            event_i -= num_events_for_this_cycle;
            offset_in_epoch += (num_events_for_this_cycle * inter_event_delay) / 1000;
        }
        return 0
    }

    fn next_adjusted_event(&self, events_so_far: usize) -> usize {
        let n = self.out_of_order_group_size;
        let event_number = self.first_event_number + events_so_far;
        (event_number / n) * n + (event_number * 953) % n
    }
}

#[derive(Serialize, Deserialize)]
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
    fn new(events_so_far: usize, nex: &mut NEXMark) -> Self {
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

impl Into<Option<Person>> for Event {
    fn into(self) -> Option<Person> {
        match self {
            Event::Person(p) => Some(p),
            _ => None
        }
    }
}

impl Into<Option<Auction>> for Event {
    fn into(self) -> Option<Auction> {
        match self {
            Event::Auction(p) => Some(p),
            _ => None
        }
    }
}

impl Into<Option<Bid>> for Event {
    fn into(self) -> Option<Bid> {
        match self {
            Event::Bid(p) => Some(p),
            _ => None
        }
    }
}

#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
struct Person{
    id: Id,
    name: String,
    email_address: String,
    credit_card: String,
    city: String,
    state: String,
    date_time: Date
}
unsafe_abomonate!(Person : id, name, email_address, credit_card, city, state, date_time);

impl Person {
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

    fn next_id(id: usize, rng: &mut StdRng, nex: &NEXMark) -> Id {
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
    fn new(events_so_far: usize, id: usize, time: Date, rng: &mut StdRng, nex: &NEXMark) -> Self {
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

    fn next_id(id: usize, rng: &mut StdRng, nex: &NEXMark) -> Id {
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

    fn next_length(events_so_far: usize, rng: &mut StdRng, time: Date, nex: &NEXMark) -> Date {
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
    fn new(id: usize, time: Date, rng: &mut StdRng, nex: &NEXMark) -> Self {
        let auction = if rng.gen_range(0, nex.hot_auction_ratio) > 0 {
            (Auction::last_id(id) / HOT_AUCTION_RATIO) * HOT_AUCTION_RATIO
        } else {
            Auction::next_id(id, rng, nex)
        };
        let bidder = if rng.gen_range(0, nex.hot_bidder_ratio) > 0 {
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

trait NEXMarkQuery {
    type DO: Data+Debug;

    fn name(&self) -> &str;
    fn construct_dataflow<'scope>(&self, stream: &mut Child<'scope, Root<Generic>, usize>) -> Stream<Child<'scope, Root<Generic>, usize>, Self::DO>;
}

impl<I, DO: Data+Debug> TestImpl for I where I: NEXMarkQuery<DO=DO> {
    type D = Event;
    type DO = I::DO;
    type T = usize;
    type G = Lines<BufReader<File>>;

    fn name(&self) -> &str { I::name(self) }

    fn initial_epoch(&self) -> Self::T { 0 }

    fn generate_data(&self) -> Result<()> {
        fs::create_dir_all(&self.data_dir)?;
        let mut nex = NEXMark::new(&self.config);
        let seconds = self.config.get_as_or("seconds", 60);
        let wall_base = 0;
        let mut file = File::create(format!("{}/events.json", &self.data_dir))?;
        
        for events_so_far in 0.. {
            let time = nex.event_timestamp(nex.first_event_id + events_so_far);
            let wall = wall_base + (time - nex.base_time);
            let event = Event::new(events_so_far, &mut nex);
            let carrier = EventCarrier{ time: wall, event: event };
            serde_json::to_writer(&file, &carrier)?;
            file.write(b"\n")?;
            
            if seconds < (wall / 1000) { break; }
        }
        Ok(())
    }

    fn prepare(&self, _index: usize, _workers: usize) -> Result<Self::G> {
        let event_file = File::open(format!("{}/events.json", &self.data_dir))?;
        Ok(BufReader::new(event_file).lines())
    }

    fn epoch_data(&self, stream: &mut Self::G, epoch: &Self::T) -> Result<Vec<Self::D>> {
        let mut data = Vec::new();
        for line in stream {
            let carrier: EventCarrier = serde_json::from_str(&line.unwrap())?;
            if carrier.time / 1000 > *epoch {
                data.push(carrier.event);
                return Ok(data);
            }
            data.push(carrier.event);
        }
        if data.is_empty(){
            return Err(Error::new(ErrorKind::Other, "Out of data"));
        } else {
            return Ok(data);
        }
    }

    fn construct_dataflow<'scope>(&self, scope: &mut Child<'scope, Root<Generic>, Self::T>) -> (Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO>, Box<InputHandle<Self::T, Self::D>>) {
        let (input, mut stream) = scope.new_input();
        (I::construct_dataflow(self, &mut stream), Box::new(input))
    }
}

struct Query0 {
    data_dir: String,
    config: Config,
}

impl Query0 {
    fn new(config: &Config) -> Self {
        Query0 {
            data_dir: format!("{}/nexmark",config.get_or("data-dir", "data")),
            config: config.clone()
        }
    }
}

impl NEXMarkQuery for Query0 {
    type DO = Event;

    fn name(&self) -> &str { "NEXMark Query 0" }

    fn construct_dataflow<'scope>(&self, stream: &mut Child<'scope, Root<Generic>, usize>) -> Stream<Child<'scope, Root<Generic>, usize>, Self::DO> {
        *stream
    }
}

struct Query1 {
    data_dir: String,
    config: Config,
}

impl Query1 {
    fn new(config: &Config) -> Self {
        Query0 {
            data_dir: format!("{}/nexmark",config.get_or("data-dir", "data")),
            config: config.clone()
        }
    }
}

impl NEXMarkQuery for Query1 {
    type DO = (Id, Id, usize, Date);

    fn name(&self) -> &str { "NEXMark Query 1" }

    fn construct_dataflow<'scope>(&self, stream: &mut Child<'scope, Root<Generic>, usize>) -> Stream<Child<'scope, Root<Generic>, usize>, Self::DO> {
        stream
            .filter(|e| e.into::<Option<Bid>>().is_some())
            .map(|e| e.into::<Option<Bid>>().unwrap())
            .map(|b| (b.auction, b.bidder, (b.price * 89) / 100, b.date_time))
    }
}

struct Query2 {
    data_dir: String,
    config: Config,
}

impl Query2 {
    fn new(config: &Config) -> Self {
        Query0 {
            data_dir: format!("{}/nexmark",config.get_or("data-dir", "data")),
            config: config.clone()
        }
    }
}

impl NEXMarkQuery for Query2 {
    type DO = (Id, usize);

    fn name(&self) -> &str { "NEXMark Query 2" }

    fn construct_dataflow<'scope>(&self, stream: &mut Child<'scope, Root<Generic>, usize>) -> Stream<Child<'scope, Root<Generic>, usize>, Self::DO> {
        stream
            .filter(|e| e.into::<Option<Bid>>().is_some())
            .map(|e| e.into::<Option<Bid>>().unwrap())
            .filter(|b| b.auction % self.config.auction_skip == 0)
            .map(|b| (b.auction, b.price))
    }
}

struct Query3 {
    data_dir: String,
    config: Config,
}

impl Query3 {
    fn new(config: &Config) -> Self {
        Query0 {
            data_dir: format!("{}/nexmark",config.get_or("data-dir", "data")),
            config: config.clone()
        }
    }
}

impl NEXMarkQuery for Query3 {
    type DO = (String, String, String, Id);

    fn name(&self) -> &str { "NEXMark Query 3" }

    fn construct_dataflow<'scope>(&self, stream: &mut Child<'scope, Root<Generic>, usize>) -> Stream<Child<'scope, Root<Generic>, usize>, Self::DO> {
        // FIXME: unbounded windows?
        let auctions = stream
            .filter(|e| e.into::<Option<Auction>>().is_some())
            .map(|e| e.into::<Option<Auction>>().unwrap())
            .filter(|a| a.category == 10);
        let persons = stream
            .filter(|e| e.into::<Option<Person>>().is_some())
            .map(|e| e.into::<Option<Person>>().unwrap())
            .filter(|p| p.state == "OR" || p.state == "ID" || p.state == "CA");
        auctions.join(persons, |a| a.id, |p| p.id, |a, p| (p.name, p.city, p.state, a.id))
    }
}

struct Query4 {
    data_dir: String,
    config: Config,
}

impl Query4 {
    fn new(config: &Config) -> Self {
        Query0 {
            data_dir: format!("{}/nexmark",config.get_or("data-dir", "data")),
            config: config.clone()
        }
    }
}

impl NEXMarkQuery for Query4 {
    type DO = (String, String, String, Id);

    fn name(&self) -> &str { "NEXMark Query 4" }

    fn construct_dataflow<'scope>(&self, stream: &mut Child<'scope, Root<Generic>, usize>) -> Stream<Child<'scope, Root<Generic>, usize>, Self::DO> {
        // FIXME: unbounded windows?
        let auctions = stream
            .filter(|e| e.into::<Option<Auction>>().is_some())
            .map(|e| e.into::<Option<Auction>>().unwrap())
            .filter(|a| a.expires <= CURRENT_TIME);
        let bids = stream
            .filter(|e| e.into::<Option<Bid>>().is_some())
            .map(|e| e.into::<Option<Bid>>().unwrap());
        auctions.join(bids, |a| a.id, |b| b.auction, |a, b| (b.date_time, a.expires, a.id, a.category, b.price))
            .filter(|(t, e, _, _, _)| t < e)
            .reduce_by(|(_, _, id, cat, _)| (id, cat), (0, 0), |(_, _, _, cat, price), (_, p)| (cat, max(p, price)))
            .reduce_by(|_| 0, 0, |(_, price), p| p + price/NUM_CATEGORIES) 
    }
}

struct Query5 {
    data_dir: String,
    config: Config,
}

impl Query5 {
    fn new(config: &Config) -> Self {
        Query0 {
            data_dir: format!("{}/nexmark",config.get_or("data-dir", "data")),
            config: config.clone()
        }
    }
}

impl NEXMarkQuery for Query5 {
    type DO = (String, String, String, Id);

    fn name(&self) -> &str { "NEXMark Query 5" }

    fn construct_dataflow<'scope>(&self, stream: &mut Child<'scope, Root<Generic>, usize>) -> Stream<Child<'scope, Root<Generic>, usize>, Self::DO> {
        let bids = stream
            .filter(|e| e.into::<Option<Bid>>().is_some())
            .map(|e| e.into::<Option<Bid>>().unwrap())
            .epoch_window(60*60, 60);
        let count = bids.reduce_by(|_| 0, 0, |_, c| c+1);
        bids.reduce_by(|b| b.auction, (0, 0), |b, (_, c)| (b.auction, c+1))
            .join(count, |_| 0, |_| 0, |t, (a, c)| (t, a, c))
            .filter(|(t, _, c)| c >= t)
            .map(|(_, a, _)| a)
    }
}

pub fn nexmark(args: &Config) -> Vec<Box<Test>>{
    vec![Box::new(Query0::new(args))]
}

use abomonation::Abomonation;
use timely::dataflow::operators::Input;
use timely::dataflow::scopes::{Root, Child};
use timely::dataflow::Stream;
use timely_communication::allocator::Generic;
use test::{Test, TestImpl, InputHandle};
use config::Config;

type Id = usize;
type Date = usize;

#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug, Abomonation)]
enum Event{
    Person(Person),
    Auction(Auction),
    Bid(Bid),
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

#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
struct Bid{
    auction: Id,
    bidder: Id,
    price: usize,
    date_time: Date,
}

unsafe_abomonate!(Person : id, name, email_address, credit_card, city, state, date_time);
unsafe_abomonate!(Auction : id, item_name, description, initial_bid, reserve, date_time, expires, seller, category);
unsafe_abomonate!(Bid : auction, bidder, price, date_time);

struct Query0 {}

impl TestImpl for Query0 {
    type D = Event;
    type DO = Event;
    type T = usize;
    type G = ();

    fn name(&self) -> &str { "NEXMark Query 0" }

    fn initial_epoch(&self) -> Self::T { 0 }

    fn construct_dataflow<'scope>(&self, scope: &mut Child<'scope, Root<Generic>, Self::T>) -> (Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO>, Box<InputHandle<Self::T, Self::D>>) {
        let (input, stream) = scope.new_input();
        (stream, Box::new(input))
    }
}

pub fn nexmark(args: &Config) -> Vec<Box<Test>>{
    vec![Box::new(Query0 {})]
}

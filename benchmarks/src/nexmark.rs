use abomonation::Abomonation;
use test::Test;
use test::TestImpl;
use config::Config;

type Id = usize;
type Date = usize;

#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
struct Person{
    id: Id,
    name: String,
    city: String,
    state: String,
}

#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
struct Auction{
    id: Id,
    item: Id,
    seller: Id,
    category: Id,
    initial: usize,
    reserve: usize,
    expires: Date,
}

#[derive(Eq, PartialEq, Clone, Serialize, Deserialize, Debug)]
struct Bid{
    auction: Id,
    bidder: Id,
    price: usize,
    datetime: Date,
}

unsafe_abomonate!(Person : id, name, city, state);
unsafe_abomonate!(Auction : id, item, seller, category, initial, reserve, expires);
unsafe_abomonate!(Bid : auction, bidder, price, datetime);

pub fn nexmark(args: &Config) -> Vec<Box<Test>>{
    vec![]
}

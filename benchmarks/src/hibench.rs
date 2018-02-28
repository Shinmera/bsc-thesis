extern crate rdkafka;
extern crate kafkaesque;

use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use timely::dataflow::operators::{Operator, Input, Map, Exchange};
use timely::dataflow::operators::aggregation::Aggregate;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::input::Handle;
use timely::dataflow::scopes::{Root, Child};
use timely::dataflow::{Stream};
use timely_communication::allocator::Generic;
use operators::RollingCount;
use operators::EpochWindow;
use test::Test;
use test::TestImpl;
use std::cmp;
use std::str::FromStr;
use config::Config;
use self::kafkaesque::EventProducer;
use self::rdkafka::config::ClientConfig;
use timely::dataflow::operators::Capture;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use timely::dataflow::operators::Unary;

// Hasher used for data shuffling
fn hasher(x: &String) -> u64 {
    let mut s = DefaultHasher::new();
    x.hash(&mut s);
    s.finish()
}

// Function used for extracting the IP address from HiBench records with the following text format:
// timestamp  ip, session id, something, something, browser, ...
// 0    227.209.164.46,nbizrgdziebsaecsecujfjcqtvnpcnxxwiopmddorcxnlijdizgoi,1991-06-10,0.115967035,Mozilla/5.0 (iPhone; U; CPU like Mac OS X)AppleWebKit/420.1 (KHTML like Gecko) Version/3.0 Mobile/4A93Safari/419.3,YEM,YEM-AR,snowdrops,1
// 0    35.143.225.164,nbizrgdziebsaecsecujfjcqtvnpcnxxwiopmddorcxnlijdizgoi,1996-05-31,0.8792629,Mozilla/5.0 (Windows; U; Windows NT 5.2) AppleWebKit/525.13 (KHTML like Gecko) Chrome/0.2.149.27 Safari/525.13,PRT,PRT-PT,fraternally,8
// 0    34.57.45.175,nbizrgdziebtsaecsecujfjcqtvnpcnxxwiopmddorcxnlijdizgoi,2001-06-29,0.14202267,Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1),DOM,DOM-ES,Gaborone's,7
fn get_ip(record: &String) -> String 
{
    // TODO (john): Change this into a regex 
    let end = record.find(',').expect("HiBench: Cannot parse IP.");
    let ip = &record[0..end];
    let start = ip.rfind(char::is_whitespace).expect("HiBench: Cannot parse IP.");
    record[start+1..end].to_string()
}

struct Identity {}

impl Identity {
    fn new(_config: &Config) -> Self {
        Identity{}
    }
}

impl TestImpl for Identity {
    type D = (String,String);
    type DO = (String,String);
    type T = usize;
    type G = ();
    
    fn name(&self) -> &str { "HiBench Identity" }

    fn initial_epoch(&self) -> Self::T { 0 }

    fn construct_dataflow<'scope>(&self, scope: &mut Child<'scope, Root<Generic>, Self::T>) -> (Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO>, Vec<Handle<Self::T, Self::D>>) {
        let (input, stream) = scope.new_input();
        let topic = "1".to_string();
        let count = 1;
        let brokers = "localhost:9092";

        // Create Kafka stuff.
        let mut producer_config = ClientConfig::new();
        producer_config
            .set("produce.offset.report", "true")
            .set("bootstrap.servers", &brokers);

        let producer = EventProducer::new(producer_config, topic);
        // TODO (john): For each tuple in the input stream, the sinc operator must report a tuple of the form (ts,system_time) to Kafka
        stream.map(|(ts,_):(String,_)| 
            (u64::from_str(&ts).expect("Identity: Cannot parse event timestamp."), SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards").as_secs()))
            .capture_into(producer);

        (stream, vec![input])
    }
}

struct Repartition {}

impl Repartition {
    fn new(_config: &Config) -> Self {
        Repartition{}
    }
}

impl TestImpl for Repartition {
    type D = String;
    type DO = String;
    type T = usize;
    type G = ();
    
    fn name(&self) -> &str { "HiBench Repartition" }

    fn initial_epoch(&self) -> Self::T { 0 }
    
    fn construct_dataflow<'scope>(&self, scope: &mut Child<'scope, Root<Generic>, Self::T>) -> (Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO>, Vec<Handle<Self::T, Self::D>>) {
        let (input, stream) = scope.new_input();
        let peers = scope.peers() as u64;   // Total number of workers executing the dataflow
        // Simulate a RoundRobin shuffling
        let stream = stream.unary_stream(Pipeline, "RoundRobin", move |input, output| {
                let mut counter = 0u64;
                input.for_each(|time, data| {
                    for record in data.drain(..) {
                        let r = (counter, record);
                        counter += 1;
                        if counter == peers { counter = 0; }
                        output.session(&time).give(r);
                    }
                });
            })
            // Exchange on worker id (worker ids are in [0,peers)
            .exchange(|&(worker_id,_)| worker_id)
            .map(|(_,record)| record);
        // TODO (john): For each tuple in the input stream, the sinc operator must report a tuple of the form (ts,system_time) to Kafka
        stream.sink(Pipeline,"Sink",|_| ());
        (stream, vec![input])
    }
}

struct Wordcount {}

impl Wordcount {
    fn new(_config: &Config) -> Self {
        Wordcount{}
    }
}

impl TestImpl for Wordcount {
    type D = (String,String);
    type DO = (String, String, usize);
    type T = usize;
    type G = ();
    
    fn name(&self) -> &str { "HiBench Wordcount" }

    fn initial_epoch(&self) -> Self::T { 0 }

    fn construct_dataflow<'scope>(&self, scope: &mut Child<'scope, Root<Generic>, Self::T>) -> (Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO>, Vec<Handle<Self::T, Self::D>>) {
        let (input, stream) = scope.new_input();
        let stream = stream
            .map(|(ts,b)| (get_ip(&b),ts))
            .exchange(|&(ref ip,_)| hasher(&ip))
            .rolling_count(|&(ref ip,ref ts):&(String,String)| (ip.clone(),ts.clone()));
        // TODO (john): For each tuple in the output stream, the sinc operator must report a tuple of the form (ts,system_time) to Kafka
        stream.sink(Pipeline,"Sink",|_| ());
        (stream, vec![input])
    }
}

struct Fixwindow {}

impl Fixwindow {
    fn new(_config: &Config) -> Self {
        Fixwindow{}
    }
}

impl TestImpl for Fixwindow {
    type D = (String,String);
    type DO = (String,u64,u32);
    type T = usize;
    type G = ();
    
    fn name(&self) -> &str { "HiBench Fixwindow" }

    fn initial_epoch(&self) -> Self::T { 0 }

    fn construct_dataflow<'scope>(&self, scope: &mut Child<'scope, Root<Generic>, Self::T>) -> (Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO>, Vec<Handle<Self::T, Self::D>>) {
        let (input, stream) = scope.new_input();
        let stream = stream
            .map(|(ts,b):Self::D| (get_ip(&b),u64::from_str(&ts).expect("FixWindow: Cannot parse event timestamp.")))
            // TODO (john): Check if timestamps in the input stream correspond to seconds
            // A tumbling window of 10 epochs
            .epoch_window(10, 10)
            // Group by ip and report the minimum observed timestamp and the total number of records per group
            .aggregate::<_,(u64,u32),_,_,_>(
               |_ip, ts, agg| 
                { 
                    agg.0 = cmp::min(agg.0,ts); 
                    agg.1 += 1;
                },
               |ip, agg| (ip, agg.0,agg.1),
               |ip| hasher(ip)
            );
        // TODO (john): For each tuple in the output stream, the sinc operator must report agg.1 tuples of the form (agg.0,system_time) to Kafka
        stream.sink(Pipeline,"Sink",|_| ());
        (stream, vec![input])
    }
}

pub fn hibench(args: &Config) -> Vec<Box<Test>>{
    vec![Box::new(Identity::new(args)),
         Box::new(Repartition::new(args)),
         Box::new(Wordcount::new(args)),
         Box::new(Fixwindow::new(args))]
}

#[cfg(test)]
mod tests {
    use ::hibench::get_ip;

    #[test]
    fn test_ip_parser() {
        let record = "0    227.209.164.46,nbizrgdziebsaecsecujfjcqtvnpcnxxwiopmddorcxnlijdizgoi,1991-06-10,0.115967035,Mozilla/5.0 (iPhone; U; CPU like Mac OS X)AppleWebKit/420.1 (KHTML like Gecko) Version/3.0 Mobile/4A93Safari/419.3,YEM,YEM-AR,snowdrops,1".to_string();
        assert_eq!(get_ip(&record),"227.209.164.46");
    }
}



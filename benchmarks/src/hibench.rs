use config::Config;
use operators::{Window, RollingCount, Reduce};
use rand::{self, Rng};
use std::cmp::min;
use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::{Result, Write};
use std::str::FromStr;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use std::thread::{self, JoinHandle};
use test::{Test, TestImpl, Benchmark};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Unary;
use timely::dataflow::operators::{Map, Exchange};
use timely::dataflow::scopes::{Root, Child};
use timely::dataflow::{Stream};
use timely::progress::nested::product::Product;
use timely::progress::timestamp::{Timestamp, RootTimestamp};
use timely_communication::allocator::Generic;
use endpoint::{Drain, Source, FromData, ToData};

// Hasher used for data shuffling
fn hasher(x: &String) -> u64 {
    let mut s = DefaultHasher::new();
    x.hash(&mut s);
    s.finish()
}

impl ToData<Product<RootTimestamp, usize>, (String, String)> for String{
    fn to_data(self) -> Option<(Product<RootTimestamp, usize>, (String, String))> {
        if let Some(t) = self.get(0..4){
            let t = t.trim_left();
            if let Some(d) = self.get(5..) {
                if let Ok(tt) = usize::from_str(t) {
                    return Some((RootTimestamp::new(tt), (String::from(t), String::from(d))));
                }
            }
        }
        None
    }
}

// Function used for extracting the IP address from HiBench records with the following text format:
// timestamp  ip, session id, something, something, browser, ...
// 0    227.209.164.46,nbizrgdziebsaecsecujfjcqtvnpcnxxwiopmddorcxnlijdizgoi,1991-06-10,0.115967035,Mozilla/5.0 (iPhone; U; CPU like Mac OS X)AppleWebKit/420.1 (KHTML like Gecko) Version/3.0 Mobile/4A93Safari/419.3,YEM,YEM-AR,snowdrops,1
// 0    35.143.225.164,nbizrgdziebsaecsecujfjcqtvnpcnxxwiopmddorcxnlijdizgoi,1996-05-31,0.8792629,Mozilla/5.0 (Windows; U; Windows NT 5.2) AppleWebKit/525.13 (KHTML like Gecko) Chrome/0.2.149.27 Safari/525.13,PRT,PRT-PT,fraternally,8
// 0    34.57.45.175,nbizrgdziebtsaecsecujfjcqtvnpcnxxwiopmddorcxnlijdizgoi,2001-06-29,0.14202267,Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1),DOM,DOM-ES,Gaborone's,7
fn get_ip(record: &String) -> String {
    // TODO (john): Change this into a regex 
    let end = record.find(',').expect("HiBench: Cannot parse IP.");
    let ip = &record[0..end];
    String::from(ip)
}

fn random_ip() -> String {
    let mut rng = rand::thread_rng();
    format!("{}.{}.{}.{}", rng.gen_range(0, 255), rng.gen_range(0, 255), rng.gen_range(0, 255), rng.gen_range(0, 255))
}

fn random_date() -> String {
    let mut rng = rand::thread_rng();
    format!("{}-{}-{}", rng.gen_range(1990, 2010), rng.gen_range(1, 12), rng.gen_range(0, 31))
}

struct Identity {
}

impl Identity {
    fn new() -> Self {
        Identity{}
    }
}

impl TestImpl for Identity {
    type D = (String,String);
    type DO = (u64,u64);
    type T = usize;
    
    fn name(&self) -> &str { "HiBench Identity" }

    fn create_endpoints(&self, config: &Config, index: usize, _workers: usize) -> Result<(Vec<Source<Product<RootTimestamp, Self::T>, Self::D>>, Drain<Product<RootTimestamp, Self::T>, Self::DO>)> {
        let mut config = config.clone();
        let data_dir = format!("{}/hibench", config.get_or("data-dir", "data"));
        config.insert("input-file", format!("{}/events-{}.csv", &data_dir, index));
        let int: Result<_> = config.clone().into();
        let out: Result<_> = config.clone().into();
        Ok((vec!(int?), out?))
    }

    fn construct_dataflow<'scope>(&self, _c: &Config, stream: &Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>) -> Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO> {
        stream.map(|(ts, _)|
            (u64::from_str(&ts).unwrap(),
             SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()))
    }
}

impl<T: Timestamp> FromData<T> for (u64,u64) {
    fn from_data(&self, t: &T) -> String {
        format!("{:?} {:?}", t, self)
    }
}

struct Repartition {}

impl Repartition {
    fn new() -> Self {
        Repartition{}
    }
}

impl TestImpl for Repartition {
    type D = (String,String);
    type DO = (String,String);
    type T = usize;
    
    fn name(&self) -> &str { "HiBench Repartition" }

    fn create_endpoints(&self, config: &Config, index: usize, _workers: usize) -> Result<(Vec<Source<Product<RootTimestamp, Self::T>, Self::D>>, Drain<Product<RootTimestamp, Self::T>, Self::DO>)> {
        let mut config = config.clone();
        let data_dir = format!("{}/hibench", config.get_or("data-dir", "data"));
        config.insert("input-file", format!("{}/events-{}.csv", &data_dir, index));
        let int: Result<_> = config.clone().into();
        let out: Result<_> = config.clone().into();
        Ok((vec!(int?), out?))
    }
    
    fn construct_dataflow<'scope>(&self, config: &Config, stream: &Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>) -> Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO> {
        let peers = config.get_as_or("workers", 1) as u64;
        // Simulate a RoundRobin shuffling
        stream.unary_stream(Pipeline, "RoundRobin", move |input, output| {
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
            .exchange(|&(worker_id, _)| worker_id)
            .map(|(_, record)| record)
    }
}

impl<T: Timestamp> FromData<T> for (String, String) {
    fn from_data(&self, t: &T) -> String {
        format!("{:?} {:?}", t, self)
    }
}

struct Wordcount {}

impl Wordcount {
    fn new() -> Self {
        Wordcount{}
    }
}

impl TestImpl for Wordcount {
    type D = (String,String);
    type DO = (String, String, usize);
    type T = usize;
    
    fn name(&self) -> &str { "HiBench Wordcount" }

    fn create_endpoints(&self, config: &Config, index: usize, _workers: usize) -> Result<(Vec<Source<Product<RootTimestamp, Self::T>, Self::D>>, Drain<Product<RootTimestamp, Self::T>, Self::DO>)> {
        let mut config = config.clone();
        let data_dir = format!("{}/hibench", config.get_or("data-dir", "data"));
        config.insert("input-file", format!("{}/events-{}.csv", &data_dir, index));
        let int: Result<_> = config.clone().into();
        let out: Result<_> = config.clone().into();
        Ok((vec!(int?), out?))
    }

    fn construct_dataflow<'scope>(&self, _c: &Config, stream: &Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>) -> Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO> {
        stream
            .map(|(ts,b)| (get_ip(&b), ts))
            .exchange(|&(ref ip,_)| hasher(&ip))
            .rolling_count(|&(ref ip, _)| ip.clone(), |(ip, ts), c| (ip, ts, c))
    }
}

impl<T: Timestamp> FromData<T> for (String, String, usize) {
    fn from_data(&self, t: &T) -> String {
        format!("{:?} {:?}", t, self)
    }
}

struct Fixwindow {}

impl Fixwindow {
    fn new() -> Self {
        Fixwindow{}
    }
}

impl TestImpl for Fixwindow {
    type D = (String,String);
    type DO = (String,(u64,u32));
    type T = usize;
    
    fn name(&self) -> &str { "HiBench Fixwindow" }

    fn create_endpoints(&self, config: &Config, index: usize, _workers: usize) -> Result<(Vec<Source<Product<RootTimestamp, Self::T>, Self::D>>, Drain<Product<RootTimestamp, Self::T>, Self::DO>)> {
        let mut config = config.clone();
        let data_dir = format!("{}/hibench", config.get_or("data-dir", "data"));
        config.insert("input-file", format!("{}/events-{}.csv", &data_dir, index));
        let int: Result<_> = config.clone().into();
        let out: Result<_> = config.clone().into();
        Ok((vec!(int?), out?))
    }

    fn construct_dataflow<'scope>(&self, _c: &Config, stream: &Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>) -> Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO> {
        // TODO (john): Check if timestamps in the input stream correspond to seconds
        stream
            .map(|(ts, b)| (get_ip(&b), u64::from_str(&ts).unwrap()))
            .epoch_window(10, 10)
            .reduce_by(|&(ref ip, _)| ip.clone(),
                       (0, 0), |(_, t), (m, c)| (min(m, t), c+1))
    }
}

impl<T: Timestamp> FromData<T> for (String, (u64, u32)) {
    fn from_data(&self, t: &T) -> String {
        format!("{:?} {:?}", t, self)
    }
}

pub struct HiBench {}

impl HiBench {
    pub fn new() -> Self { HiBench{} }
}

impl Benchmark for HiBench {
    fn name(&self) -> &str { "HiBench" }

    fn generate_data(&self, config: &Config) -> Result<()> {
        let data_dir = format!("{}/hibench", config.get_or("data-dir", "data"));
        let partitions = config.get_as_or("partitions", 10);
        let seconds = config.get_as_or("seconds", 60);
        let events_per_second = config.get_as_or("events-per-second", 100_000);
        let ips = config.get_as_or("ips", 100);
        fs::create_dir_all(&data_dir)?;

        println!("Generating {} events/s for {}s over {} partitions for {} ips.",
                 events_per_second, seconds, partitions, ips);

        let ips: Vec<_> = (0..ips).map(|_| random_ip()).collect();
        let mut threads: Vec<JoinHandle<Result<()>>> = Vec::new();
        for p in 0..partitions {
            let mut file = File::create(format!("{}/events-{}.csv", &data_dir, p))?;
            let ips = ips.clone();
            threads.push(thread::spawn(move || {
                let mut rng = rand::thread_rng();
                for t in 0..seconds {
                    for _ in 0..(events_per_second/partitions) {
                        let ip = rng.choose(&ips).unwrap().clone();
                        let session: String = rng.gen_ascii_chars().take(54).collect();
                        let date = random_date();
                        let float = 0.0;
                        let agent = "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)";
                        let s = "DOM";
                        let subs = "DOM-ES";
                        let word = "snow";
                        let int = 6;
                        writeln!(&mut file, "{:4} {},{},{},{:.8},{},{},{},{},{}",
                                 t, ip, session, date, float, agent, s, subs, word, int)?;
                    }
                }
                Ok(())
            }));
        }
        for t in threads.drain(..){
            t.join().unwrap()?;
        }
        Ok(())
    }

    fn tests(&self) -> Vec<Box<Test>> {
        vec![Box::new(Identity::new()),
             Box::new(Repartition::new()),
             Box::new(Wordcount::new()),
             Box::new(Fixwindow::new())]
    }
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



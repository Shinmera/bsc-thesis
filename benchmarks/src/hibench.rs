use config::Config;
use operators::{Window, RollingCount};
use rand::{self, Rng};
use std::cmp;
use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::{Result, Write};
use std::str::FromStr;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use test::{Test, TestImpl};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Unary;
use timely::dataflow::operators::aggregation::Aggregate;
use timely::dataflow::operators::{Map, Exchange};
use timely::dataflow::scopes::{Root, Child};
use timely::dataflow::{Stream};
use timely_communication::allocator::Generic;

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
fn get_ip(record: &String) -> String {
    // TODO (john): Change this into a regex 
    let end = record.find(',').expect("HiBench: Cannot parse IP.");
    let ip = &record[0..end];
    let start = ip.rfind(char::is_whitespace).expect("HiBench: Cannot parse IP.");
    record[start+1..end].to_string()
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
    data_dir: String,
    seconds: usize,
    events_per_second: usize,
    ips: usize,
}

impl Identity {
    fn new(config: &Config) -> Self {
        Identity{
            data_dir: format!("{}/hibench", config.get_or("data-dir", "data")),
            seconds: config.get_as_or("seconds", 60),
            events_per_second: config.get_as_or("events-per-second", 100_000),
            ips: config.get_as_or("ips", 100),
        }
    }
}

impl TestImpl for Identity {
    type D = (String,String);
    type DO = (u64,u64);
    type T = usize;
    
    fn name(&self) -> &str { "HiBench Identity" }

    fn generate_data(&self) -> Result<()> {
        fs::create_dir_all(&self.data_dir)?;

        println!("Generating {} events/s for {}s for {} ips.",
                 self.events_per_second, self.seconds, self.ips);

        let mut rng = rand::thread_rng();
        let mut file = File::create(format!("{}/data.csv", &self.data_dir))?;
        let ips: Vec<_> = (0..self.ips).map(|_| random_ip()).collect();
        for t in 0..self.seconds {
            for _ in 0..self.events_per_second {
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
    }

    fn construct_dataflow<'scope>(&self, stream: &Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>) -> Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO> {
        stream.map(|(ts,_):(String,_)| 
            (u64::from_str(&ts).expect("Identity: Cannot parse event timestamp."), SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards").as_secs()))
    }
}

struct Repartition {
    peers: usize
}

impl Repartition {
    fn new(config: &Config) -> Self {
        Repartition{peers: config.get_as_or("workers", 1)}
    }
}

impl TestImpl for Repartition {
    type D = String;
    type DO = String;
    type T = usize;
    
    fn name(&self) -> &str { "HiBench Repartition" }
    
    fn construct_dataflow<'scope>(&self, stream: &Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>) -> Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO> {
        let peers = self.peers as u64;
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
            .exchange(|&(worker_id,_)| worker_id)
            .map(|(_,record)| record)
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
    
    fn name(&self) -> &str { "HiBench Wordcount" }

    fn construct_dataflow<'scope>(&self, stream: &Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>) -> Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO> {
        stream
            .map(|(ts,b)| (get_ip(&b),ts))
            .exchange(|&(ref ip,_)| hasher(&ip))
            .rolling_count(|&(ref ip,ref ts):&(String,String)| (ip.clone(),ts.clone()))
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
    
    fn name(&self) -> &str { "HiBench Fixwindow" }

    fn construct_dataflow<'scope>(&self, stream: &Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>) -> Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO> {
        stream
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
            )
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



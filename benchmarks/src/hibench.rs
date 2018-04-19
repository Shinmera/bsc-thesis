use config::Config;
use operators::{Window, RollingCount, Reduce};
use rand::{self, Rng};
use std::cmp::min;
use std::fs::File;
use std::fs;
use std::io::{Result, Write, Error, ErrorKind};
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
use timely::progress::timestamp::{RootTimestamp, Timestamp};
use timely_communication::allocator::Generic;
use endpoint::{self, Drain, Source, FromData, ToData, EventSource};

#[derive(Eq, PartialEq, Clone, Abomonation)]
struct Event {
    time: usize,
    data: String,
}

impl Event {
    fn ip(&self) -> String {
        let end = self.data.find(',').expect("HiBench: Cannot parse IP.");
        let ip = &self.data[0..end];
        String::from(ip)
    }
}

impl ToData<usize, Event> for String{
    fn to_data(self) -> Result<(usize, Event)> {
        if let Some(t) = self.get(0..4){
            let t = t.trim_left();
            if let Some(d) = self.get(5..) {
                match usize::from_str(t) {
                    Ok(tt) => return Ok((tt, Event{ time: tt, data: String::from(d)})),
                    Err(e) => return Err(Error::new(ErrorKind::Other, ::std::error::Error::description(&e)))
                }
            }
        }
        Err(Error::new(ErrorKind::Other, "Failed to parse."))
    }
}

impl FromData<usize> for Event{
    fn from_data(&self, _: &usize) -> String {
        format!("{:4} {}", self.time, self.data)
    }
}

// Function used for extracting the IP address from HiBench records with the following text format:
// timestamp  ip, session id, something, something, browser, ...
// 0    227.209.164.46,nbizrgdziebsaecsecujfjcqtvnpcnxxwiopmddorcxnlijdizgoi,1991-06-10,0.115967035,Mozilla/5.0 (iPhone; U; CPU like Mac OS X)AppleWebKit/420.1 (KHTML like Gecko) Version/3.0 Mobile/4A93Safari/419.3,YEM,YEM-AR,snowdrops,1
// 0    35.143.225.164,nbizrgdziebsaecsecujfjcqtvnpcnxxwiopmddorcxnlijdizgoi,1996-05-31,0.8792629,Mozilla/5.0 (Windows; U; Windows NT 5.2) AppleWebKit/525.13 (KHTML like Gecko) Chrome/0.2.149.27 Safari/525.13,PRT,PRT-PT,fraternally,8
// 0    34.57.45.175,nbizrgdziebtsaecsecujfjcqtvnpcnxxwiopmddorcxnlijdizgoi,2001-06-29,0.14202267,Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1),DOM,DOM-ES,Gaborone's,7

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
    type D = Event;
    type DO = (usize, u64);
    type T = usize;
    
    fn name(&self) -> &str { "HiBench Identity" }

    fn create_endpoints(&self, config: &Config, _index: usize, _workers: usize) -> Result<(Source<Self::T, Self::D>, Drain<Self::T, Self::DO>)> {
        Ok((Source::from_config(config, Source::new(Box::new(HiBenchGenerator::new(config))))?,
            Drain::from_config(config)?))
    }

    fn construct_dataflow<'scope>(&self, _c: &Config, stream: &Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>) -> Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO> {
        stream.map(|e|
                   (e.time,
                    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()))
    }
}

impl<T: Timestamp> FromData<T> for (usize, u64) {
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
    type D = Event;
    type DO = Event;
    type T = usize;
    
    fn name(&self) -> &str { "HiBench Repartition" }
    
    fn create_endpoints(&self, config: &Config, _index: usize, _workers: usize) -> Result<(Source<Self::T, Self::D>, Drain<Self::T, Self::DO>)> {
        Ok((Source::from_config(config, Source::new(Box::new(HiBenchGenerator::new(config))))?,
            Drain::from_config(config)?))
    }
    
    fn construct_dataflow<'scope>(&self, config: &Config, stream: &Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>) -> Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO> {
        let peers = config.get_as_or("threads", 1) as u64;
        // Simulate a RoundRobin shuffling
        stream.unary_stream(Pipeline, "RoundRobin", move |input, output| {
            let mut counter = 0u64;
            input.for_each(|time, data| {
                output.session(&time).give_iterator(data.drain(..).map(|r| {
                    counter = (counter + 1) % peers;
                    (counter, r)
                }));
            })})
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
    type D = Event;
    type DO = (String, usize, usize);
    type T = usize;
    
    fn name(&self) -> &str { "HiBench Wordcount" }

    fn create_endpoints(&self, config: &Config, _index: usize, _workers: usize) -> Result<(Source<Self::T, Self::D>, Drain<Self::T, Self::DO>)> {
        Ok((Source::from_config(config, Source::new(Box::new(HiBenchGenerator::new(config))))?, 
            Drain::from_config(config)?))
    }

    fn construct_dataflow<'scope>(&self, _c: &Config, stream: &Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>) -> Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO> {
        stream
            .map(|e| (e.ip(), e.time))
            .rolling_count(|&(ref ip, _)| ip.clone(), |(ip, ts), c| (ip, ts, c))
    }
}

impl<T: Timestamp> FromData<T> for (String, usize, usize) {
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
    type D = Event;
    type DO = (String,(usize, u32));
    type T = usize;
    
    fn name(&self) -> &str { "HiBench Fixwindow" }

    fn create_endpoints(&self, config: &Config, _index: usize, _workers: usize) -> Result<(Source<Self::T, Self::D>, Drain<Self::T, Self::DO>)> {
        Ok((Source::from_config(config, Source::new(Box::new(HiBenchGenerator::new(config))))?, 
            Drain::from_config(config)?))
    }

    fn construct_dataflow<'scope>(&self, config: &Config, stream: &Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>) -> Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO> {
        // TODO (john): Check if timestamps in the input stream correspond to seconds
        let window_size = config.get_as_or("window-size", 10);
        stream
            .map(|e| (e.ip(), e.time))
            .tumbling_window(move |t| RootTimestamp::new(((t.inner/window_size)+1)*window_size))
            .reduce_by(|&(ref ip, _)| ip.clone(),
                       (0, 0), |(_, t), (m, c)| (min(m, t), c+1))
    }
}

impl<T: Timestamp> FromData<T> for (String, (usize, u32)) {
    fn from_data(&self, t: &T) -> String {
        format!("{:?} {:?}", t, self)
    }
}

#[derive(Clone)]
pub struct HiBenchGenerator {
    ips: Vec<String>,
    count: usize,
    epoch: usize,
    max: usize,
}

impl HiBenchGenerator {
    fn new(config: &Config) -> Self {
        let partitions = config.get_as_or("threads", 10);
        let seconds = config.get_as_or("seconds", 60);
        let events_per_second = config.get_as_or("events-per-second", 100_000);
        let ips = config.get_as_or("ips", 100);
        
        HiBenchGenerator {
            ips: (0..ips).map(|_| random_ip()).collect(),
            count: events_per_second/partitions,
            epoch: 0,
            max: seconds,
        }
    }
}

impl EventSource<usize, Event> for HiBenchGenerator {
    fn next(&mut self) -> Result<(usize, Vec<Event>)> {
        if self.epoch < self.max {
            let mut rng = rand::thread_rng();
            let mut data = Vec::with_capacity(self.count);
            
            for _ in 0..self.count {
                let ip = rng.choose(&self.ips).unwrap().clone();
                let session: String = rng.gen_ascii_chars().take(54).collect();
                let date = random_date();
                let float = 0.0;
                let agent = "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)";
                let s = "DOM";
                let subs = "DOM-ES";
                let word = "snow";
                let int = 6;
                data.push(Event{
                    time: self.epoch,
                    data: format!("{},{},{},{:.8},{},{},{},{},{}",
                                  ip, session, date, float, agent, s, subs, word, int)
                });
            }
            
            self.epoch += 1;
            Ok((self.epoch-1, data))
        } else {
            endpoint::out_of_data()
        }
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
        let partitions = config.get_as_or("threads", 10);
        let seconds = config.get_as_or("seconds", 60);
        let events_per_second = config.get_as_or("events-per-second", 100_000);
        let ips = config.get_as_or("ips", 100);
        fs::create_dir_all(&data_dir)?;

        println!("Generating {} events/s for {}s over {} partitions for {} ips.",
                 events_per_second, seconds, partitions, ips);

        let generator = HiBenchGenerator::new(config);
        let mut threads: Vec<JoinHandle<Result<()>>> = Vec::new();
        for p in 0..partitions {
            let mut file = File::create(format!("{}/events-{}.csv", &data_dir, p))?;
            let mut generator = generator.clone();
            threads.push(thread::spawn(move || {
                loop{
                    let (_, d) = generator.next()?;
                    for e in d {
                        writeln!(&mut file, "{:4} {}", e.time, e.data)?;
                    }
                }
            }));
        }
        for t in threads.drain(..){
            endpoint::accept_out_of_data(t.join().unwrap())?;
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

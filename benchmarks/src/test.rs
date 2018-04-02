use config::Config;
use statistics::Statistics;
use endpoint::{Source, Drain};
use std::collections::{HashMap};
use std::io::{BufRead, Result, Error, ErrorKind};
use std::sync::{Mutex,Arc};
use std::time::Instant;
use timely::dataflow::operators::capture::{Capture, Replay};
use timely::dataflow::operators::Inspect;
use timely::dataflow::scopes::{Child, Root};
use timely::dataflow::{Stream, Scope};
use timely::progress::Timestamp;
use timely::progress::timestamp::RootTimestamp;
use timely::progress::nested::product::Product;
use timely::{Data, Configuration};
use timely;
use timely_communication::allocator::Generic;

/// This presents the public interface for a test collection.
pub trait Benchmark {
    /// The name of this test suite / benchmark.
    fn name(&self) -> &str;
    
    /// This function is used to generate a workload or data set
    /// for use during testing. It will write it out to files, which
    /// can then be used to feed the tests.
    fn generate_data(&self, config: &Config) -> Result<()>;

    /// Returns a fresh vector of test instances for this benchmark.
    fn tests(&self) -> Vec<Box<Test>>;
}

/// This presents the public interface for a test.
pub trait Test : Sync+Send {
    /// The name of the test as a human readable string.
    fn name(&self) -> &str;
    
    /// This function handles the actual running of the test.
    fn run(&self, config: &Config, worker: &mut Root<Generic>) -> Result<Statistics>;
}

pub trait Constructor<G: Scope, D: Data, D2: Data> {
    fn construct_dataflow<C>(&self, constructor: C) -> Stream<G, D2>
    where C: Fn(&Stream<G, D>)->Stream<G, D2>;
}

impl<G: Scope, D: Data, D2: Data> Constructor<G, D, D2> for Stream<G, D> {
    fn construct_dataflow<C>(&self, constructor: C) -> Stream<G, D2>
    where C: Fn(&Stream<G, D>)->Stream<G, D2>{
        constructor(self)
    }
}

pub trait TestImpl : Sync+Send {
    type D: Data;
    type DO: Data;
    type T: Timestamp;

    fn name(&self) -> &str;

    fn create_endpoints(&self, _config: &Config, _index: usize, _workers: usize) -> Result<(Vec<Source<Product<RootTimestamp, Self::T>, Self::D>>, Drain<Product<RootTimestamp, Self::T>, Self::DO>)> {
        Ok((vec!(().into()), ().into()))
    }

    fn construct_dataflow<'scope>(&self, _config: &Config, stream: &Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>) -> Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO>;

    fn run(&self, config: &Config, worker: &mut Root<Generic>) -> Result<Statistics>{
        // Construct the full flow.
        let starts = Arc::new(Mutex::new(HashMap::new()));
        let ends = Arc::new(Mutex::new(HashMap::new()));
        let (ins, out) = self.create_endpoints(config, worker.index(), worker.peers())?;
        worker.dataflow(|scope| {
            let ends = ends.clone();
            let starts = starts.clone();
            // FIXME!! We need a better way to measure when an epoch is completely done.
            ins.replay_into(scope)
                .inspect_batch(move |t, _|{
                    let mut starts = starts.lock().unwrap();
                    starts.entry(t.inner.clone()).or_insert(Instant::now());})
                .construct_dataflow(|s| self.construct_dataflow(config, s))
                .inspect_batch(move |t, _|{
                    let mut ends = ends.lock().unwrap();
                    ends.entry(t.inner.clone()).or_insert(Instant::now());})
                .capture_into(out);
        });
        // Step until we're done.
        while worker.step() {}
        // Collect statistics.
        let starts = starts.lock().unwrap();
        let ends = ends.lock().unwrap();
        let durations: Vec<_> = ends.iter().filter_map(|(t, i)|starts.get(t).map(|s|(s, i))).collect();
        return Ok(Statistics::from(durations));
    }
}

impl<I, T: Timestamp, D: Data, DO: Data> Test for I where I: TestImpl<T=T,D=D,DO=DO> {
    fn name(&self) -> &str { I::name(self) }
    fn run(&self, config: &Config, worker: &mut Root<Generic>) -> Result<Statistics>{ I::run(self, config, worker) }
}

/// This function extracts the timely_communication
/// configuration object from the supplied Config.
fn timely_configuration(config: &Config) -> Configuration {
    let threads = config.get_as_or("threads", 1);
    let process = config.get_as_or("process", 0);
    let processes = config.get_as_or("processes", 1);
    let report = config.get_or("report", "true") == "true";

    assert!(process < processes);

    if processes > 1 {
        let mut addresses = Vec::new();
        if let Some(hosts) = config.get("hostfile") {
            let reader = ::std::io::BufReader::new(::std::fs::File::open(hosts.clone()).unwrap());
            for x in reader.lines().take(processes) {
                addresses.push(x.unwrap());
            }
            if addresses.len() < processes {
                panic!("could only read {} addresses from {}, but -n: {}", addresses.len(), hosts, processes);
            }
        }
        else {
            for index in 0..processes {
                addresses.push(format!("localhost:{}", 2101 + index));
            }
        }

        assert!(processes == addresses.len());
        Configuration::Cluster(threads, process, addresses, report)
    }
    else {
        if threads > 1 { Configuration::Process(threads) }
        else { Configuration::Thread }
    }
}

/// Wraps the timely parts to execute a test from a configuration.
pub fn run_test(test: Box<Test>, config: &Config) -> Result<Statistics> {
    let config = config.clone();
    let configuration = timely_configuration(&config);
    timely::execute(configuration, move |worker| {
        test.run(&config, worker)
    }).and_then(|x| x.join().pop().unwrap())
        .map_err(|x| Error::new(ErrorKind::Other, x))
        .and_then(|x| x)
}

use timely::dataflow::{Stream};
use timely::dataflow::operators::{Inspect, Probe, Input};
use timely::dataflow::scopes::{Child, Root};
use timely::progress::Timestamp;
use timely::dataflow::operators::input::Handle;
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::{Data, Configuration};
use timely_communication::allocator::Generic;
use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;
use timely;
use config::Config;
use statistics::{duration_fsecs, Statistics};
use std::ops::Add;
use std::io::{BufRead, Result, Error, ErrorKind};
use std::fmt::Debug;
use std::error::Error as StdError;
use std::time::Instant;
use std::collections::{HashMap};
use std::sync::{Mutex,Arc};

/// Simple trait for incrementable objects.
/// This is used to automatically advance the timestamp.
pub trait Inc: Copy {
    fn next(&mut self) -> Self;
}

impl Inc for () { fn next(&mut self) -> Self {()} }
impl Inc for u8  { fn next(&mut self) -> Self {self.add(1)} }
impl Inc for u16 { fn next(&mut self) -> Self {self.add(1)} }
impl Inc for u32 { fn next(&mut self) -> Self {self.add(1)} }
impl Inc for u64 { fn next(&mut self) -> Self {self.add(1)} }
impl Inc for usize { fn next(&mut self) -> Self {self.add(1)} }
impl Inc for i8  { fn next(&mut self) -> Self {self.add(1)} }
impl Inc for i16 { fn next(&mut self) -> Self {self.add(1)} }
impl Inc for i32 { fn next(&mut self) -> Self {self.add(1)} }
impl Inc for i64 { fn next(&mut self) -> Self {self.add(1)} }
impl Inc for isize { fn next(&mut self) -> Self {self.add(1)} }
impl Inc for f32 { fn next(&mut self) -> Self {self.add(1.0)} }
impl Inc for f64 { fn next(&mut self) -> Self {self.add(1.0)} }

/// This trait exposes the implementation details of a benchmark.
///
/// The idea is that it encapsulates common functionality between
/// the tests and avoids duplicating code. It also presents a
/// simple framework for implementing a benchmark.
pub trait TestImpl : Sync+Send{
    type D: Data+Debug;
    type DO: Data+Debug;
    type T: Timestamp+Inc+Debug;
    type G;

    /// The name of the test as a human readable string.
    fn name(&self) -> &str;

    /// This function is used to generate a workload or data set
    /// for use during testing. It will write it out to a configured
    /// file, which is then read back when the test is actually run.
    fn generate_data(&self) -> Result<()>{
        Ok(())
    }

    /// This function is called at the beginning of a test run and
    /// is responsible for opening input streams and preparing data.
    /// The index is the current worker's index, which is used to
    /// decide whether this should feed the dataflow at all.
    ///
    /// The Ok result should contain data that the worker can use
    /// with epoch_data to generate the next round of inputs.
    fn prepare(&self, _index: usize) -> Result<Self::G> {
        Err(Error::new(ErrorKind::Other, "No data"))
    }

    /// Generates a single run of data for an epoch. If there is no
    /// more data available (and the test is thus over), the function
    /// should return
    fn epoch_data(&self, _data: &mut Self::G, _epoch: &Self::T) -> Result<Vec<Vec<Self::D>>> {
        println!("Warning: {} does not implement a data generator.", self.name());
        Err(Error::new(ErrorKind::Other, "Out of data"))
    }

    /// Used to close open file handles and otherwise tear down stuff
    /// that was opened up in prepare().
    fn finish(&self, _data: &mut Self::G) {
    }

    /// This function is responsible for constructing the data flow
    /// used for the computation, returning the last node of the
    /// graph and a vector of input handles.
    ///
    /// The input argument should be the object received by the
    /// function passed to worker.dataflow()
    fn construct_dataflow<'scope>(&self, &mut Child<'scope, Root<Generic>, Self::T>) -> (Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO>, Vec<Handle<Self::T, Self::D>>);

    /// This function will return the starting epoch timestamp.
    fn initial_epoch(&self) -> Self::T;

    /// A shorthand function to test whether the frontier has reached
    /// all the inputs yet.
    fn frontier_behind(&self, probe: &ProbeHandle<Product<RootTimestamp, Self::T>>, inputs: &Vec<Handle<Self::T, Self::D>>) -> bool{
        for input in inputs {
            if probe.less_than(input.time()) {
                return true;
            }
        }
        return false;
    }

    /// This function handles the actual running of the test.
    ///
    /// This function is the primary raison d'être of this
    /// framework, as its behaviour is the same across each
    /// possible test we might to run; you open some streams,
    /// construct the data flow, loop to feed data and advance
    /// the workers as needed.
    fn run(&self, worker: &mut Root<Generic>) -> Result<Statistics>{
        let mut starts = HashMap::new();
        let ends = Arc::new(Mutex::new(HashMap::new()));
        let mut feeder_data = self.prepare(worker.index())?;
        let (probe, mut inputs) = worker.dataflow(|scope|{
            let ends = ends.clone();
            let (stream, inputs) = self.construct_dataflow(scope);
            (stream.inspect_batch(move |t, _|{
                let mut ends = ends.lock().unwrap();
                ends.insert(t.inner, Instant::now());
            }).probe(), inputs)
        });
        
        let mut epoch = self.initial_epoch();
        let start = Instant::now();
        loop {
            starts.insert(epoch, Instant::now());
            match self.epoch_data(&mut feeder_data, &epoch){
                Ok(mut data) => {
                    data.drain(..).zip(&mut inputs).for_each(|(mut d, i)| i.send_batch(&mut d));
                },
                // FIXME: Intercept SIGINT and gracefully end the test run as if it had run out of data.
                Err(error) => {
                    let end = Instant::now();
                    for input in inputs { input.close(); }
                    self.finish(&mut feeder_data);
                    // If we are simply out of data it means the run has finished successfully.
                    if error.kind() == ErrorKind::Other && error.description() == "Out of data" {
                        let ends = ends.lock().unwrap();
                        let durations: Vec<_> = ends.iter().map(|(t, i)|(starts.get(t).unwrap(), i)).collect();
                        let mut stats = Statistics::from(durations);
                        // Typically we expect the total to be the length of the run, but since
                        // epoch timings overlap, the default total calculated would be way too
                        // high. We adjust it manually here.
                        stats.total = duration_fsecs(&end.duration_since(start));
                        return Ok(stats);
                    }
                    return Err(error);
                }
            }
            epoch = epoch.next();
            for i in 0..inputs.len() {
                inputs[i].advance_to(epoch);
            }
            while self.frontier_behind(&probe, &inputs) {
                worker.step();
            }
        }
    }
}

/// This is an interface for tests that do not require a lot of
/// functionality. It takes over timestamp stepping, limits
/// to a single input, and only allows simplistic data generation.
pub trait SimpleTest : Sync+Send {
    type D: Data+Debug;
    type DO: Data+Debug;

    fn name(&self) -> &str {
        "Simple Test"
    }

    fn rounds(&self) -> usize {
        10
    }

    fn generate(&self, epoch: &usize) -> Vec<Self::D>;

    fn construct<'scope>(&self, stream: &mut Stream<Child<'scope, Root<Generic>, usize>, Self::D>) -> Stream<Child<'scope, Root<Generic>, usize>, Self::DO>;
}

impl<I, D: Data+Debug, DO: Data+Debug> TestImpl for I where I: SimpleTest<D=D,DO=DO> {
    type D = I::D;
    type DO = I::DO;
    type T = usize;
    type G = usize;

    fn name(&self) -> &str { I::name(self) }

    fn prepare(&self, _index: usize) -> Result<Self::G> {
        Ok(I::rounds(self))
    }

    fn epoch_data(&self, counter: &mut Self::G, epoch: &Self::T) -> Result<Vec<Vec<Self::D>>> {
        if 0 < *counter {
            *counter -= 1;
            Ok(vec![I::generate(self, epoch)])
        } else {
            Err(Error::new(ErrorKind::Other, "Out of data"))
        }
    }

    fn construct_dataflow<'scope>(&self, scope: &mut Child<'scope, Root<Generic>, Self::T>) -> (Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO>, Vec<Handle<Self::T, Self::D>>) {
        let (input, mut stream) = scope.new_input();
        (I::construct(self, &mut stream), vec![input])
    }

    fn initial_epoch(&self) -> Self::T { 0 }
}

/// This presents the public interface for a test.
///
/// The reason this exists is so that you can talk about a test
/// without having to know the precise type of the test, or the
/// TestImpl's trait types.
pub trait Test : Sync+Send {
    fn name(&self) -> &str;
    fn generate_data(&self) -> Result<()>;
    fn run(&self, worker: &mut Root<Generic>) -> Result<Statistics>;
}

impl<I, T: Timestamp+Inc+Debug, D: Data+Debug, DO: Data+Debug> Test for I where I: TestImpl<T=T,D=D,DO=DO> {
    fn name(&self) -> &str { I::name(self) }
    fn generate_data(&self) -> Result<()>{ I::generate_data(self) }
    fn run(&self, worker: &mut Root<Generic>) -> Result<Statistics>{ I::run(self, worker) }
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
pub fn run_test(config: &Config, test: Box<Test>) -> Result<Statistics> {
    let configuration = timely_configuration(config);
    timely::execute(configuration, move |worker| {
        test.run(worker)
    }).and_then(|x| x.join().pop().unwrap())
        .map_err(|x| Error::new(ErrorKind::Other, x))
        .and_then(|x| x)
}

pub fn generate_test(test: Box<Test>) -> Result<()> {
    test.generate_data()
}

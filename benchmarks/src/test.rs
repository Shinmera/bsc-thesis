use timely::dataflow::{Stream};
use timely::dataflow::operators::{Inspect, Probe, Input};
use timely::dataflow::scopes::{Child, Root};
use timely::progress::Timestamp;
use timely::progress::timestamp::RootTimestamp;
use timely::progress::nested::product::Product;
use timely::dataflow::operators::input::Handle;
use timely::{Data, Configuration};
use timely_communication::allocator::Generic;
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
use std::cmp::max;
use std::ptr::drop_in_place;

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

/// This presents the public interface for a test.
pub trait Test : Sync+Send {
    /// The name of the test as a human readable string.
    fn name(&self) -> &str;
    
    /// This function is used to generate a workload or data set
    /// for use during testing. It will write it out to a configured
    /// file, which is then read back when the test is actually run.
    fn generate_data(&self) -> Result<()>;
    
    /// This function handles the actual running of the test.
    fn run(&self, worker: &mut Root<Generic>) -> Result<Statistics>;
}

pub trait InputHandle<T, D> {
    fn send(&mut self, Vec<D>);
    fn advance_to(&mut self, T);
    fn max_time(&self) -> &Product<RootTimestamp, T>;
}

impl<T: Timestamp, D: Data> InputHandle<T, D> for Handle<T, D> {
    fn send(&mut self, mut data: Vec<D>) {
        self.send_batch(&mut data);
    }

    fn advance_to(&mut self, t: T) {
        Handle::advance_to(self, t);
    }

    fn max_time(&self) -> &Product<RootTimestamp, T> {
        self.time()
    }
}

impl<T: Timestamp, D1: Data, D2: Data> InputHandle<T, (D1, D2)> for (Handle<T, D1>, Handle<T, D2>) {
    fn send(&mut self, mut data: Vec<(D1, D2)>) {
        let &mut (ref mut a, ref mut b) = self;
        data.drain(..).for_each(|(d1, d2)|{
            a.send(d1);
            b.send(d2);
        });
    }

    fn advance_to(&mut self, t: T) {
        let &mut (ref mut a, ref mut b) = self;
        Handle::advance_to(a, t.clone());
        Handle::advance_to(b, t);
    }

    fn max_time(&self) -> &Product<RootTimestamp, T> {
        let &(ref a, ref b) = self;
        max(a.time(), b.time())
    }
}

pub trait TestImpl : Sync+Send {
    type D: Data+Debug;
    type DO: Data+Debug;
    type T: Timestamp+Inc+Debug;
    type G;

    fn name(&self) -> &str;

    fn generate_data(&self) -> Result<()>{
        Ok(())
    }

    fn prepare(&self, _index: usize, _workers: usize) -> Result<Self::G> {
        Err(Error::new(ErrorKind::Other, "No data"))
    }

    fn initial_epoch(&self) -> Self::T;

    fn epoch_data(&self, _data: &mut Self::G, _epoch: &Self::T) -> Result<Vec<Self::D>> {
        println!("Warning: {} does not implement a data generator.", self.name());
        Err(Error::new(ErrorKind::Other, "Out of data"))
    }
    
    fn finish(&self, _data: &mut Self::G) {
    }

    fn construct_dataflow<'scope>(&self, &mut Child<'scope, Root<Generic>, Self::T>) -> (Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO>, Box<InputHandle<Self::T, Self::D>>);

    fn run(&self, worker: &mut Root<Generic>) -> Result<Statistics>{
        let mut starts = HashMap::new();
        let ends = Arc::new(Mutex::new(HashMap::new()));
        let mut feeder_data = self.prepare(worker.index(), worker.peers())?;
        let (probe, mut input) = worker.dataflow(|scope|{
            let ends = ends.clone();
            let (stream, input) = self.construct_dataflow(scope);
            (stream.inspect_batch(move |t, _|{
                let mut ends = ends.lock().unwrap();
                ends.insert(t.inner, Instant::now());
            }).probe(), input)
        });
        
        let mut epoch = self.initial_epoch();
        let start = Instant::now();
        loop {
            starts.insert(epoch, Instant::now());
            match self.epoch_data(&mut feeder_data, &epoch){
                Ok(data) => {
                    input.send(data);
                },
                // FIXME: Intercept SIGINT and gracefully end the test run as if it had run out of data.
                Err(error) => {
                    let end = Instant::now();
                    // We cannot call close on the input as it would have to allocate the trait,
                    // which is impossible. We instead need to drop in place, which should have
                    // the same effect as closing.
                    unsafe { drop_in_place(Box::into_raw(input)); }
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
            input.advance_to(epoch);
            while probe.less_than(input.max_time()) {
                worker.step();
            }
        }
    }
}

impl<I, T: Timestamp+Inc+Debug, D: Data+Debug, DO: Data+Debug> Test for I where I: TestImpl<T=T,D=D,DO=DO> {
    fn name(&self) -> &str { I::name(self) }
    fn generate_data(&self) -> Result<()>{ I::generate_data(self) }
    fn run(&self, worker: &mut Root<Generic>) -> Result<Statistics>{ I::run(self, worker) }
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

    fn prepare(&self, _index: usize, _workers: usize) -> Result<Self::G> {
        Ok(I::rounds(self))
    }

    fn epoch_data(&self, counter: &mut Self::G, epoch: &Self::T) -> Result<Vec<Self::D>> {
        if 0 < *counter {
            *counter -= 1;
            Ok(I::generate(self, epoch))
        } else {
            Err(Error::new(ErrorKind::Other, "Out of data"))
        }
    }

    fn construct_dataflow<'scope>(&self, scope: &mut Child<'scope, Root<Generic>, Self::T>) -> (Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO>, Box<InputHandle<Self::T, Self::D>>) {
        let (input, mut stream) = scope.new_input();
        (I::construct(self, &mut stream), Box::new(input))
    }

    fn initial_epoch(&self) -> Self::T { 0 }
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

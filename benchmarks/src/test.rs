use config::Config;
use statistics::Statistics;
use endpoint::{Source, Drain, EventSource, EventDrain};
use std::collections::{HashMap};
use operators::Timer;
use std::io::{BufRead, Result, Error, ErrorKind};
use std::sync::{Mutex,Arc};
use std::ops::DerefMut;
use timely::dataflow::scopes::{Child, Root};
use timely::dataflow::operators::probe::Handle;
use timely::dataflow::operators::{Probe, Unary};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::{Stream, Scope, InputHandle};
use timely::progress::Timestamp;
use timely::{Data, Configuration};
use timely;
use timely_communication::allocator::Generic;

/// This presents the public interface for a test collection.
pub trait Benchmark {
    /// The name of this test suite / benchmark.
    ///
    /// This name is used to identify the benchmark when
    /// selecting benchmarks to use from the command line.
    fn name(&self) -> &str;
    
    /// This function is used to generate a workload or data set
    /// for use during testing. It will write it out to files, which
    /// can then be used to feed the tests.
    fn generate_data(&self, config: &Config) -> Result<()>;

    /// Returns a fresh vector of test instances for this benchmark.
    ///
    /// You should be able to call run_test on an instance contained
    /// in this vector in order to execute its dataflow and collect
    /// timing data.
    fn tests(&self) -> Vec<Box<Test>>;
}

/// This presents the public interface for a test.
///
/// This trait should be used wherever generic kinds of tests are
/// handed around. It avoids the trait bounds of local type fields
/// that are required in the TestImpl trait, so that an instance can
/// be easily passed around without knowledge of internal data types.
///
/// Note that since this is still an unsized trait, you will need to
/// use Boxes in order to pass an instance around.
pub trait Test : Sync+Send {
    /// The name of the test as a human readable string.
    ///
    /// This name is used to identify the test when selecting
    /// tests to run from the command line.
    fn name(&self) -> &str;
    
    /// This function handles the actual running of the test.
    ///
    /// A test run should construct a complete dataflow on the worker,
    /// including data source and drain, execute it to completion,
    /// and finally return a Statistics instance that contains computed
    /// timing data for the run.
    fn run(&self, config: &Config, worker: &mut Root<Generic>) -> Result<Statistics>;
}

/// A shorthand trait to allow appending to the dataflow through a closure.
/// This just exists to make the dataflow construction in the TestImpl::run
/// function a bit nicer to read.
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

    /// This function is responsible for creating appropriate dataflow endpoints for the dataflow graph.
    ///
    /// The endpoints are used to feed and consume the data at either ends of the "actual" dataflow.
    /// By default this method returns a NullInput and NullOutput, to allow you to first focus on the
    /// dataflow construction of your test. Ideally the default would construct appropriate endpoints
    /// based on the config object, however this leads to additional type constraints that are not
    /// possible to resolve without limiting the general scope of tests.
    ///
    /// A typical implementation of this method for configurable endpoints will look something like this:
    ///
    ///     fn create_endpoints(&self, config: &Config, index: usize, _workers: usize) -> Result<(Vec<Source<Product<RootTimestamp, Self::T>, Self::D>>, Drain<Product<RootTimestamp, Self::T>, Self::DO>)> {
    ///         let int: Result<_> = config.clone().into();
    ///         let out: Result<_> = config.clone().into();
    ///         Ok((vec!(int?), out?))
    ///     }
    ///
    /// You might want to adapt the default input-file in the config object to match what is generated
    /// by this test's benchmark data generation. Typically that will require something like this:
    ///
    ///     let mut config = config.clone();
    ///     let data_dir = format!("{}/nexmark", config.get_or("data-dir", "data"));
    ///     config.insert("input-file", format!("{}/events-{}.json", &data_dir, index));
    ///
    /// You may also use this function in order to perform preparation work for the test run, such as
    /// loading additional cache files and tables in that are not directly available in source, and
    /// are not fed into the dataflow as time based events.
    fn create_endpoints(&self, _config: &Config, _index: usize, _workers: usize) -> Result<(Source<Self::T, Self::D>, Drain<Self::T, Self::DO>)> {
        // I tried automatically creating endpoints according to the config, but that does not
        // seem to work out in any feasible manner. So, in order to make the imeplementation
        // of tests a bit less annoying we default to null endpoints, which should let you focus
        // on writing the dataflow at first -- the most important part.
        Ok((().into(), ().into()))
    }

    /// This is the centrepiece of the test implementation and constructs the core dataflow.
    ///
    /// You should manipulate the stream in whatever way necessary to produce the desired dataflow
    /// that performs the computation for this test. Note that you should not include any kind of
    /// operator that interacts with the outside world as you cannot report failures nicely from this
    /// function. The create_endpoints method should be used for that sort of purpose instead.
    fn construct_dataflow<'scope>(&self, _config: &Config, stream: &Stream<Child<'scope, Root<Generic>, Self::T>, Self::D>) -> Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO>;

    /// This function executes the dataflow on the given worker to completion.
    ///
    /// On successful completion, it should return a statistics object that describes the time taken
    /// to process epochs during the test run.
    fn run(&self, config: &Config, worker: &mut Root<Generic>) -> Result<Statistics>{
        // Construct the full flow.
        let starts = Arc::new(Mutex::new(HashMap::new()));
        let ends = Arc::new(Mutex::new(HashMap::new()));
        let (mut ins, mut out) = self.create_endpoints(config, worker.index(), worker.peers())?;
        let mut input = InputHandle::new();
        let probe = worker.dataflow(|scope| {
            let ends = ends.clone();
            let starts = starts.clone();
            let mut probe = Handle::new();
            input.to_stream(scope)
                .time_first(starts)
                .construct_dataflow(|s| self.construct_dataflow(config, s))
                .time_last(ends)
                .probe_with(&mut probe)
                .unary_stream::<(), _, _>(Pipeline, "Output", move |input, output|{
                    while let Some((time, data)) = input.next() {
                        out.next(time.time().inner.clone(), data.deref_mut().clone());
                        output.session(&time);
                    }
                });
            probe
        });
        // Step until we're done.
        while let Ok((t, mut d)) = ins.next() {
            input.advance_to(t);
            input.send_batch(&mut d);
            worker.step_while(|| probe.less_than(input.time()));
        }
        // Collect statistics.
        let starts = starts.lock().unwrap();
        let ends = ends.lock().unwrap();
        let durations: Vec<_> = ends.iter().filter_map(|(t, i)|starts.get(t).map(|s|(s, i))).collect();
        return Ok(Statistics::from(durations));
    }
}

/// Default implementation to allow you to use a TestImpl as a Test directly.
///
/// This simply delegates the method calls.
impl<I, T: Timestamp, D: Data, DO: Data> Test for I where I: TestImpl<T=T,D=D,DO=DO> {
    fn name(&self) -> &str { I::name(self) }
    fn run(&self, config: &Config, worker: &mut Root<Generic>) -> Result<Statistics>{ I::run(self, config, worker) }
}

/// Creates a timely_communication object from the supplied Config.
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
        } else {
            for index in 0..processes {
                addresses.push(format!("localhost:{}", 2101 + index));
            }
        }

        assert!(processes == addresses.len());
        Configuration::Cluster(threads, process, addresses, report)
    } else {
        if threads > 1 { Configuration::Process(threads) }
        else { Configuration::Thread }
    }
}

/// Wraps the timely parts to execute a test from a configuration.
///
/// This is just a thin wrapper around timely::execute with some wrangling
/// of the result in order to turn it into what we expect for a test run.
pub fn run_test(test: Box<Test>, config: &Config) -> Result<Statistics> {
    let config = config.clone();
    let configuration = timely_configuration(&config);
    timely::execute(configuration, move |worker| {
        test.run(&config, worker)
    }).and_then(|x| x.join().pop().unwrap())
        .map_err(|x| Error::new(ErrorKind::Other, x))
        .and_then(|x| x)
}

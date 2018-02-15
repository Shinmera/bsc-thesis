use timely::dataflow::{Stream};
use timely::dataflow::operators::{Probe};
use timely::dataflow::scopes::{Child, Root};
use timely::progress::Timestamp;
use timely::dataflow::operators::input::Handle;
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::Data;
use timely_communication::allocator::Generic;
use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;
use std::ops::Add;

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
    type D: Data;
    type DO: Data;
    type T: Timestamp+Inc;

    /// The name of the test as a human readable string.
    fn name(&self) -> &str;
    
    /// This function is responsible for constructing the data flow
    /// used for the computation, returning the last node of the
    /// graph and a vector of input handles.
    ///
    /// The input argument should be the object received by the
    /// function passed to worker.dataflow()
    fn construct_dataflow<'scope>(&self, &mut Child<'scope, Root<Generic>, Self::T>) -> (Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO>, Vec<Handle<Self::T, Self::D>>);

    /// This function is called at the beginning of a test run and
    /// is responsible for opening input streams and preparing data.
    /// The index is the current worker's index, which is used to
    /// decide whether this should feed the dataflow at all.
    ///
    /// If this worker should not feed the dataflow, the result
    /// should be Ok(false), otherwise Ok(true). Err(..) can be used
    /// to report on unexpected failures in preparing or parsing the
    /// data.
    fn prepare_data(&self, _index: usize) -> Result<bool, &str> {
        Ok(false)
    }

    /// Generates a single run of data for an epoch. If there is no
    /// more data available (and the test is thus over), the function
    /// should return None.
    fn generate_data(&self) -> Option<Vec<Self::D>> {
        println!("Warning: {} does not implement a data generator.", self.name());
        None
    }

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

    /// This function will return the starting epoch timestamp.
    fn initial_epoch(&self) -> Self::T;

    /// This function handles the actual running of the test.
    ///
    /// This function is the primary raison d'être of this
    /// framework, as its behaviour is the same across each
    /// possible test we might to run; you open some streams,
    /// construct the data flow, loop to feed data and advance
    /// the workers as needed.
    fn run(&self, worker: &mut Root<Generic>) -> Result<(), &str>{
        let provides_input = self.prepare_data(worker.index())?;
        let (probe, mut inputs) = worker.dataflow(|scope|{
            let (stream, inputs) = self.construct_dataflow(scope);
            (stream.probe(), inputs)
        });
        let mut epoch = self.initial_epoch();
        
        loop {
            if provides_input {
                if let Some(mut data) = self.generate_data() {
                    let mut i = inputs.len()-1;
                    while let Some(input) = data.pop() {
                        inputs[i].send(input);
                        i = i-1;
                    }
                } else {
                    break;
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
        for input in inputs { input.close(); }
        Ok(())
    }
}

/// This presents the public interface for a test.
///
/// The reason this exists is so that you can talk about a test
/// without having to know the precise type of the test, or the
/// TestImpl's trait types.
pub trait Test : Sync+Send{
    fn name(&self) -> &str;
    fn run(&self, worker: &mut Root<Generic>) -> Result<(), &str>;
}

impl<I, T: Timestamp+Inc, D: Data, DO: Data> Test for I where I: TestImpl<T=T,D=D,DO=DO> {
    fn name(&self) -> &str { I::name(self) }
    fn run(&self, worker: &mut Root<Generic>) -> Result<(), &str>{ I::run(self, worker) }
}

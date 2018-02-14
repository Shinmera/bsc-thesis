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

pub trait TestImpl : Sync+Send{
    type D: Data;
    type DO: Data;
    type T: Timestamp+Inc;
    
    fn name(&self) -> &str;
    
    fn construct_dataflow<'scope>(&self, &mut Child<'scope, Root<Generic>, Self::T>) -> (Stream<Child<'scope, Root<Generic>, Self::T>, Self::DO>, Vec<Handle<Self::T, Self::D>>);

    fn prepare_data(&self, _index: usize) -> Result<bool, &str> {
        Ok(false)
    }

    fn generate_data(&self) -> Option<Vec<Self::D>> {
        println!("Warning: {} does not implement a data generator.", self.name());
        None
    }

    fn frontier_behind(&self, probe: &ProbeHandle<Product<RootTimestamp, Self::T>>, inputs: &Vec<Handle<Self::T, Self::D>>) -> bool{
        for input in inputs {
            if probe.less_than(input.time()) {
                return true;
            }
        }
        return false;
    }

    fn initial_epoch(&self) -> Self::T;

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

pub trait Test : Sync+Send{
    fn name(&self) -> &str;
    fn run(&self, worker: &mut Root<Generic>) -> Result<(), &str>;
}

impl<I, T: Timestamp+Inc, D: Data, DO: Data> Test for I where I: TestImpl<T=T,D=D,DO=DO> {
    fn name(&self) -> &str { I::name(self) }
    fn run(&self, worker: &mut Root<Generic>) -> Result<(), &str>{ I::run(self, worker) }
}

use timely::dataflow::{Stream};
use timely::dataflow::scopes::child::Child;
use timely::dataflow::operators::{Probe};
use timely::dataflow::scopes::{Scope, Root};
use timely::progress::Timestamp;
use timely::dataflow::operators::input::Handle;
use timely::Data;
use timely_communication::{Allocate};

pub trait TestImpl<A: Allocate> : Test<A>{
    // I have no idea if how I'm using G is right, nor what
    // concrete type I should use for it in an implementation.
    type G: Scope;
    type D: Data;
    type T: Timestamp;
    
    fn name(&self) -> str;
    
    fn construct_dataflow(&self, &mut Child<Self::G, Self::T>) -> (Stream<Self::G, Self::D>, [Box<Handle<Self::T, Self::D>>]);

    fn prepare_data(&self, index: usize) -> Result<bool, String> {
        Ok(false)
    }

    fn generate_data(&self) -> Option<Vec<Self::D>> {
        println!("Warning: {} does not implement a data generator.", self.name());
        None
    }

    fn frontier_behind(probe: Probe<Self::G, Self::D>, inputs: [Box<Handle<Self::T, Self::D>>]) {
        for input in inputs {
            if probe.less_than(input.time()) {
                return true;
            }
        }
        return false;
    }

    fn run(&self, worker: &mut Root<A>) -> Result<(), String>{
        let provides_input = self.prepare_data(worker.index())?;
        let (inputs, stream) = worker.dataflow(self.construct_dataflow);
        let probe = stream.probe();
        let mut epoch = 0;
        
        loop {
            if provides_input {
                if let Some(data) = self.generate_data() {
                    for i in 0..inputs.len() {
                        inputs[i].send(data[i]);
                    }
                } else {
                    break;
                }
            }
            epoch = epoch + 1;
            for i in 0..inputs.len() {
                inputs[i].advance_to(epoch);
            }
            while self.frontier_behind(probe, inputs) {
                worker.step();
            }
        }
        inputs.to_iter.map(|i|i.close());
        Ok(())
    }
}

pub trait Test<A: Allocate>{
    fn name(&self) -> str;
    fn run(&self, worker: &mut Root<A>) -> Result<(), String>;
}

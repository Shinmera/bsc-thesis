use timely::dataflow::{Stream};
use timely::dataflow::scopes::child::Child;
use timely::dataflow::operators::{Input, Probe};
use timely::dataflow::scopes::{Scope, Root};
use timely::progress::Timestamp;
use timely::Data;
use timely_communication::{Allocate, WorkerGuards};

pub trait Test<G: Scope, D: Data, A: Allocate, T: Timestamp>{
    fn name(&self) -> str;
    
    fn construct_dataflow(&self, &mut Child<G, T>) -> ([Input<A, T>], Stream<G, D>);

    fn generate_data(&self) -> Option<[D]> {
        println!("Warning: {} does not implement a data generator.", self.name());
        None
    }

    fn frontier_behind(probe: Probe<G, D>, inputs: [Input<A, T>]) {
        for input in inputs {
            if probe.less_than(input.time()) {
                return true;
            }
        }
        return false;
    }

    fn run(&self, worker: &mut Root<A>) -> Result<WorkerGuards<Send+'static>, String> {
        let (inputs, stream) = worker.dataflow(self.construct_dataflow);
        let probe = stream.probe();
        let mut epoch = 0;
        while let Some(data) = self.generate_data() {
            epoch = epoch + 1;
            for i in 0..inputs.len() {
                inputs[i].send(data[i]);
                inputs[i].advance_to(epoch);
            }
            while self.frontier_behind(probe, inputs) {
                worker.step();
            }
        }
        for input in inputs {
            input.close();
        }
    }
}

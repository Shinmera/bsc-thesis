extern crate timely;
mod operators;
mod common;
mod hibench;

use common::Test;
use hibench::hibench;
use timely::dataflow::operators::{Input, Probe};
use timely::dataflow::scopes::Root;
use timely::Data;

fn frontier_behind(probe: Probe, inputs: [Input]) {
    for input in inputs {
        if probe.less_than(input.time()) {
            return true;
        }
    }
    return false;
}

fn run_dataflow(worker: &mut Root, data_generator: Fn()->Option<[Data]>, probe: Probe, inputs: [Input]) {
    let mut epoch = 0;
    while let Some(data) = data_generator() {
        epoch = epoch + 1;
        for i in 0..inputs.len() {
            inputs[i].send(data[i]);
            inputs[i].advance_to(epoch);
        }
        while frontier_behind(probe, inputs) {
            worker.step();
        }
    }
}

fn run_tests(tests: [Test]) {
    for test in tests {
        println!("Running test {}", test.name());
        timely::execute_from_args(std::env::args(), move |worker| {
            let (inputs, stream) = worker.dataflow(test.construct_dataflow);
            let probe = stream.probe();
            run_dataflow(worker, probe, test.generate_data, inputs);
            for input in inputs {
                input.close();
            }
        }).unwrap();
    }
}

fn main() {
    run_tests(hibench());
}

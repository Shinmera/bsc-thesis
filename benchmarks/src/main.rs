extern crate timely;
extern crate timely_communication;
mod operators;
mod test;
mod hibench;

use test::Test;
use hibench::hibench;
use timely::dataflow::operators::{Input, Probe};
use timely::dataflow::scopes::Root;
use timely::Data;

fn run_tests(tests: [Test]) {
    for test in tests {
        println!("Running test {}", test.name());
        timely::execute_from_args(std::env::args(), move |worker| {
            test.run(worker);
        }).unwrap();
    }
}

fn main() {
    run_tests(hibench());
}

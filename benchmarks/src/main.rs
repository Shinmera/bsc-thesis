#[macro_use]
extern crate abomonation;
extern crate timely;
extern crate timely_communication;
extern crate serde_json;
mod operators;
mod test;
mod hibench;
mod ysb;

use test::Test;
use hibench::hibench;
use ysb::ysb;

fn run_test(test: Box<Test>) {
    println!("Running test {}", test.name());
    timely::execute_from_args(std::env::args(), move |worker| {
        if let Err(e) = test.run(worker) {
            println!("Failed: {}", e);
        }
    }).unwrap();
}

fn main() {
    for test in hibench() { run_test(test); }
    for test in ysb() { run_test(test); }
}

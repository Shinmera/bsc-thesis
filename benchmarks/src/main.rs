extern crate timely;
extern crate timely_communication;
mod operators;
mod test;
mod hibench;

use test::Test;
use hibench::hibench;

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
}

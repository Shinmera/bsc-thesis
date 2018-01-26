extern crate timely;
extern crate timely_communication;
mod operators;
mod test;
mod hibench;

use test::Test;
use hibench::hibench;
use timely_communication::allocator::generic::Generic;

fn run_test(test: &Test<Generic>) {
    println!("Running test {}", test.name());
    timely::execute_from_args(std::env::args(), move |worker| {
        if let Err(e) = test.run(worker) {
            println!("Failed: {}", e);
        }
    }).unwrap();
}

fn main() {
    hibench().to_iter().map(run_test);
}

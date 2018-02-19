#[macro_use]
extern crate abomonation;
extern crate timely;
extern crate timely_communication;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate getopts;
extern crate rand;
extern crate uuid;
mod operators;
mod test;
mod hibench;
mod ysb;

use std::ops::DerefMut;
use test::Test;
use hibench::hibench;
use ysb::ysb;

fn run_test(test: Box<Test>) {
    println!("Running test {}", test.name());
    timely::execute_from_args(std::env::args(), move |worker| {
        let test = test.deref_mut();
        if let Err(e) = test.run(worker) {
            println!("Failed: {}", e);
        }
    }).unwrap();
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() == 1 {
        println!("Please choose a mode (test or generate).");
    }else if args[1] == "test" {
        for test in hibench(&args[2..]) { run_test(test); }
        for test in ysb(&args[2..]) { run_test(test); }
    }else if args[1] == "generate" {
        for test in hibench(&args[2..]) { test.generate_data().unwrap(); }
        for test in ysb(&args[2..]) { test.generate_data().unwrap(); }
    }
}

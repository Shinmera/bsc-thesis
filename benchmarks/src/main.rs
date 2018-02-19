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
mod config;
mod test;
mod hibench;
mod ysb;

use test::Test;
use config::Config;
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
    let config = Config::from(std::env::args()).unwrap();
    let mode = config.get("1").unwrap();
    let mut tests = Vec::new();
    tests.append(&mut hibench(&config));
    tests.append(&mut ysb(&config));
    if mode == "test" {
        for test in tests { run_test(test); }
    }else if mode == "generate" {
        for test in tests { test.generate_data().unwrap(); }
    }
}

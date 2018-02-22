#[macro_use]
extern crate abomonation;
extern crate timely;
extern crate timely_communication;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate rand;
extern crate uuid;
mod operators;
mod config;
mod test;
mod hibench;
mod ysb;
mod integrity;

use std::io::Result;
use test::{run_test, generate_test};
use config::Config;
use hibench::hibench;
use ysb::ysb;

fn report<A>(result: Result<A>) {
    if let Err(e) = result {
        println!("Failed: {}", e);
    } else {
        println!("Successfully completed.");
    }
}

fn main() {
    let config = Config::from(std::env::args()).unwrap();
    let mode = config.get_or("1", "help");
    let mut tests = Vec::new();
    tests.append(&mut hibench(&config));
    tests.append(&mut ysb(&config));
    if mode == "test" {
        for test in tests {
            println!("> Running test {}", test.name());
            report(run_test(&config, test));
        }
    }else if mode == "generate" {
        for test in tests {
            println!("> Generating test {}", test.name());
            report(generate_test(test));
        }
    }else if mode == "help" {
        println!("Timely Benchmarks v0.1

Usage: MODE [MODE-OPTIONS]

The following modes and respective options are available:

test               Runs the benchmarks.
  --threads NUM      Number of workers per process.
  --process IDX      The identity of this process.
  --processes NUM    Number of processes.
  --hostfile FILE    Process address host file.
  --report BOOL      Whether to report connection progress.

generate           Generates benchmark workloads.
  --data-dir DIR     The directory for the benchmark data files.
  --partitions NUM   The number of dataset partitions to create.
  --campaigns NUM    (YSB) How many campaign IDs to generate.
  --ads NUM          (YSB) How many ads per campaign to generate.
  --events NUM       (YSB) How many events in total to generate.

help               Shows this documentation.
");
    }
}

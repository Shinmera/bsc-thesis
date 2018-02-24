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
mod statistics;
mod test;
mod hibench;
mod ysb;
mod integrity;

use std::io::Result;
use std::fmt::Display;
use test::{run_test, generate_test};
use config::Config;
use hibench::hibench;
use ysb::ysb;

trait Reportable { fn report(&self); }

impl<T: Display> Reportable for Result<T> {
    fn report(&self) {
        match self {
            &Ok(ref e) => println!("Successfully completed:\n{}", e),
            &Err(ref e) => println!("Failed: {}", e),
        }
    }
}

//// Rust can't do this it seems.
// impl<T> Reportable for Result<T> {
//     fn report(&self) {
//         match self {
//             &Ok(ref e) => println!("Successfully completed"),
//             &Err(ref e) => println!("Failed: {}", e),
//         }
//     }
// }

fn main() {
    let config = Config::from(std::env::args()).unwrap();
    let mode = config.get_or("1", "help");
    let mut tests = Vec::new();
    tests.append(&mut hibench(&config));
    tests.append(&mut ysb(&config));
    if mode == "test" {
        for test in tests {
            println!("> Running test {}", test.name());
            run_test(&config, test).report();
        }
    }else if mode == "generate" {
        for test in tests {
            println!("> Generating test {}", test.name());
            generate_test(test).unwrap();
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

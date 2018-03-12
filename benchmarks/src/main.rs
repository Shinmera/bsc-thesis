#[macro_use]
extern crate abomonation;
#[macro_use]
extern crate abomonation_derive;
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
mod nexmark;
mod integrity;

use std::io::Result;
use std::fmt::Display;
use test::{run_test, generate_test};
use config::Config;
use hibench::hibench;
use ysb::ysb;
use nexmark::nexmark;

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
    let mut tests = Vec::new();
    tests.append(&mut hibench(&config));
    tests.append(&mut ysb(&config));
    tests.append(&mut nexmark(&config));
    
    let to_run = config.get("tests")
        .map(|s| s.split(",").map(|s|String::from(s)).collect::<Vec<_>>())
        .unwrap_or(tests.iter().map(|t|String::from(t.name())).collect::<Vec<_>>());
    tests.retain(|t| to_run.contains(&String::from(t.name())));
    
    let mode = config.get_or("1", "help");
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

test                    Run the benchmarks.
  --threads NUM           Number of workers per process.
  --process IDX           The identity of this process.
  --processes NUM         Number of processes.
  --hostfile FILE         Process address host file.
  --report BOOL           Whether to report connection progress.
  --tests STRING          A comma-separated list of test names to run.

generate                Generate benchmark workloads.
  --tests STRING           A comma-separated list of test names to run.
  --data-dir DIR           The directory for the benchmark data files.
  --partitions NUM         The number of dataset partitions to create.
  --campaigns NUM          (YSB) How many campaign IDs to generate.
  --ads NUM                (YSB) How many ads per campaign to generate.
  --seconds NUM            (YSB) How many seconds to generate events for.
  --events-per-second NUM  (YSN) How many events to produce per second.
  --rate-shape SINE/SQUARE (NEX) In what shape to generate delays between events.
  --first-event-rate NUM   (NEX) 
  --next-event-rate NUM    (NEX)
  --rate NUM               (NEX)
  --event-generators NUM   (NEX)
  --rate-period NUM        (NEX)
  --active-people NUM      (NEX)
  --in-flight-auctions NUM (NEX)
  --out-of-order-group-size NUM (NEX)
  --hot-seller-ratio NUM   (NEX)
  --hot-auction-ratio NUM  (NEX)
  --hot-bidder-ratio NUM   (NEX)
  --first-event-id NUM     (NEX)
  --first-event-number NUM (NEX)
  --base-time NUM          (NEX)

help                    Show this documentation.
");
    }
}

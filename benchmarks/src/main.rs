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
extern crate rdkafka;
extern crate kafkaesque;
extern crate num;
extern crate fnv;
mod operators;
mod config;
mod statistics;
mod endpoint;
mod test;
mod hibench;
mod ysb;
mod nexmark;

use std::io::Result;
use std::fmt::Display;
use test::{Benchmark, run_test};
use config::Config;
use hibench::HiBench;
use ysb::YSB;
use nexmark::NEXMark;

/// Shorthand trait to easily report results.
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

/// This function returns a fresh vector of all known Benchmarks.
fn benchmarks() -> Vec<Box<Benchmark>> {
    vec!(Box::new(HiBench::new()),
         Box::new(YSB::new()),
         Box::new(NEXMark::new()))
}

fn main() {
    let config = Config::from(std::env::args()).unwrap();
    let mut benchmarks = benchmarks();
    // Compute applicable benchmarks
    let to_run = config.get("benchmarks")
        .map(|s| s.split(",").map(|s|String::from(s)).collect::<Vec<_>>())
        .unwrap_or(benchmarks.iter().map(|t|String::from(t.name())).collect::<Vec<_>>());
    benchmarks.retain(|t| to_run.iter().any(|n|t.name().contains(n)));
    
    let mode = config.get_or("1", "help");
    if mode == "test" {
        // Compute applicable tests from benchmarks
        let mut tests = Vec::new();
        benchmarks.iter().for_each(|b| tests.append(&mut b.tests()));
        
        let to_run = config.get("tests")
            .map(|s| s.split(",").map(|s|String::from(s)).collect::<Vec<_>>())
            .unwrap_or(tests.iter().map(|t|String::from(t.name())).collect::<Vec<_>>());
        tests.retain(|t| to_run.iter().any(|n|t.name().contains(n)));
        
        for test in tests {
            let name = String::from(test.name());
            eprintln!("> Running test {}", name);
            match run_test(test, &config) {
                Ok(s) => println!("{:32} {}", name, String::from(&s)),
                Err(e) => eprintln!("{:32} Failed: {}", name, e)
            }
        }
    }else if mode == "generate" {
        for bench in benchmarks {
            eprintln!("> Generating benchmark {}", bench.name());
            bench.generate_data(&config).unwrap();
        }
    }else if mode == "help" {
        eprintln!("Timely Benchmarks v0.1

Usage: MODE [MODE-OPTIONS]

The following modes and respective options are available:

test                    Run the benchmarks.
  --tests STRING          A comma-separated list of test names to run.
  --threads NUM           Number of workers per process.
  --process IDX           The identity of this process.
  --processes NUM         Number of processes.
  --hostfile FILE         Process address host file.
  --report BOOL           Whether to report connection progress.

generate                Generate benchmark workloads.
  --benchmarks STRING      A comma-separated list of benchmark names to generate.
  --data-dir DIR           The directory for the benchmark data files.
  --threads NUM            The number of dataset partitions to create.
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

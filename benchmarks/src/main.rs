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
    let report = config.get_or("report", "summary");
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
        tests.retain(|t| to_run.iter().any(|n|t.name() == n));

        if report == "summary" {
            println!("{:25} Samples    Total      Minimum    Maximum    Median     Average    Std. Dev   ", "Test");
            for test in tests {
                let name = String::from(test.name());
                eprintln!("> Running test {}", name);
                match run_test(test, &config) {
                    Ok(s) => println!("{:25}  {}", name, String::from(&s)),
                    Err(e) => eprintln!("{:25} Failed: {}", name, e)
                }
            }
        } else if report == "latencies" {
            for test in tests {
                let name = String::from(test.name());
                eprintln!("> Running test {}", name);
                match run_test(test, &config) {
                    Ok(s) => println!("{:25}  {}", name, s.data.iter().fold(String::new(), |acc, &num| acc + &num.to_string() + " ")),
                    Err(e) => eprintln!("{:25} Failed: {}", name, e)
                }
            }
        } else {
            eprintln!("Invalid report mode, should be one of summary, latencies.");
        }
    }else if mode == "generate" {
        for bench in benchmarks {
            eprintln!("> Generating benchmark {}", bench.name());
            bench.generate_data(&config).unwrap();
        }
    }else if mode == "help" {
        eprintln!("Timely Benchmarks v0.1

Usage: MODE [OPTIONS]

The following modes are available:

test                       Run the benchmarks.
generate                   Generate data to files.
help                       Show this help document.

The following options are available:

  --benchmarks STRING      A comma-separated list of benchmarks to run.
                             Default: all
  --tests STRING           A comma-separated list of test names to run.
                             Default: all
  --threads NUM            Number of workers per process.
                             Default: 10
  --process IDX            The identity of this process.
                             Default: 0
  --processes NUM          Number of processes.
                             Default: 1
  --hostfile FILE          Process address host file.
                             Default: none
  --connection-report BOOL Whether to report connection progress.
                             Default: false
  --window-size NUM        The size of the windows in epochs (usually seconds).
                             Default: test-dependent
  --window-slide NUM       The slide of the windows in epochs (usually seconds).
                             Default: test-dependent
  --input MODE             The input generation mode. Can be one of:
                             null, console, file, generated
                             Default: generated
  --output MODE            The output consuming mode. Can be one of:
                             null, console, file, meter
                             Default: null
  --report                 How to report test result data. Can be one of:
                             summary, latencies
                             Default: summary
  --log BOOL               Whether to log Timely events to a remote server.
                             Default: false
  --log-server HOST        The hostname of the server to connect to for logging.
                             Default: localhost
  --log-port PORT          The port of the server to connect to for logging.
                             Default: 2102
  --data-dir DIR           The directory for the benchmark data files.
                             Default: data/
  --threads NUM            The number of dataset partitions to create.
                             Default: 10
  --seconds NUM            How many seconds to generate events for.
                             Default: 60
  --events-per-second NUM  How many events to produce per second.
                             Default: 100000
  --ips NUM                (HIB) How many IPs to generate.
                             Default: 100
  --campaigns NUM          (YSB) How many campaign IDs to generate.
                             Default: 100
  --ads NUM                (YSB) How many ads per campaign to generate.
                             Default: 10
  --rate-shape SHAPE       (NEX) In what shape to generate delays between events. Can be one of:
                             SINE/SQUARE
                             Default: SINE
  --next-event-rate NUM    (NEX)
                             Default: events-per-second
  --us-per-unit NUM        (NEX)
                             Default: 1000000
  --rate-period NUM        (NEX)
                             Default: 600
  --active-people NUM      (NEX)
                             Default: 1000
  --in-flight-auctions NUM (NEX)
                             Default: 100
  --out-of-order-group-size NUM (NEX)
                             Default: 1
  --hot-seller-ratio NUM   (NEX)
                             Default: 4
  --hot-auction-ratio NUM  (NEX)
                             Default: 2
  --hot-bidder-ratio NUM   (NEX)
                             Default: 2
  --first-event-id NUM     (NEX)
                             Default: 0
  --first-event-number NUM (NEX)
                             Default: 0
  --base-time NUM          (NEX)
                             Default: 1436918400000 (2015-07-15T00:00:00.000Z)

");
    } else {
        eprintln!("Invalid mode, should be one of test, generate, help.");
    }
}

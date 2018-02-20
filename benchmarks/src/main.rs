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

use test::Test;
use config::Config;
use hibench::hibench;
use ysb::ysb;
use timely::Configuration;
use std::io::BufRead;
use std::io::{Result, Error, ErrorKind};

fn report<A>(result: Result<A>) {
    if let Err(e) = result {
        println!("Failed: {}", e);
    } else {
        println!("Successfully completed.");
    }
}

/// This function extracts the timely_communication
/// configuration object from the supplied Config.
fn timely_configuration(config: &Config) -> Configuration {
    let threads = config.get_as_or("threads", 1);
    let process = config.get_as_or("process", 0);
    let processes = config.get_as_or("processes", 1);
    let report = config.get_or("report", "true") == "true";

    assert!(process < processes);

    if processes > 1 {
        let mut addresses = Vec::new();
        if let Some(hosts) = config.get("hostfile") {
            let reader = ::std::io::BufReader::new(::std::fs::File::open(hosts.clone()).unwrap());
            for x in reader.lines().take(processes) {
                addresses.push(x.unwrap());
            }
            if addresses.len() < processes {
                panic!("could only read {} addresses from {}, but -n: {}", addresses.len(), hosts, processes);
            }
        }
        else {
            for index in 0..processes {
                addresses.push(format!("localhost:{}", 2101 + index));
            }
        }

        assert!(processes == addresses.len());
        Configuration::Cluster(threads, process, addresses, report)
    }
    else {
        if threads > 1 { Configuration::Process(threads) }
        else { Configuration::Thread }
    }
}

fn run_test(config: &Config, test: Box<Test>) {
    println!("> Running test {}", test.name());
    let configuration = timely_configuration(config);
    report(timely::execute_logging(configuration, Default::default(), move |worker| {
        test.run(worker)
    }).and_then(|x| x.join().pop().unwrap())
           .map_err(|x| Error::new(ErrorKind::Other, x))
           .and_then(|x| x));
}

fn generate_test(test: Box<Test>) {
    println!("> Generating test {}", test.name());
    report(test.generate_data());
}

fn main() {
    let config = Config::from(std::env::args()).unwrap();
    let mode = config.get_or("1", "help");
    let mut tests = Vec::new();
    tests.append(&mut hibench(&config));
    tests.append(&mut ysb(&config));
    if mode == "test" {
        for test in tests {
            run_test(&config, test);
        }
    }else if mode == "generate" {
        for test in tests {
            generate_test(test);
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

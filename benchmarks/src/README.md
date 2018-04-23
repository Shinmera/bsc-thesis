## Timely Benchmarks System
This system provides both a general framework for writing and running benchmarks, and a set of pre-written benchmarks for you to use.

## How To
In order to run this, you will need a recent Rust installation and Git. The program has three major modes:

* `help` for an explanation of all flags and listing of available benchmarks and tests
* `generate` in order to generate static data files
* `test` to run benchmark tests

Typically it is not necessary to generate static data files ahead of time, and instead data generation will happen on the fly as needed.

To run the full benchmark with default settings, simply running the following should be sufficient.

    cargo --release -- test

You can select individual benchmarks to run with the `--benchmarks` flag. Currently `Yahoo`, `HiBench`, and `NEXMark` are available. You can also select individual tests from the set with the `--tests` flag.

    cargo --release -- test --benchmarks "Yahoo,NEXMark"

If you would like to change the level of parallelism, you can do so with the `--threads` flag. The duration of the tests can be changed with the `--seconds` flag, and the rate with the `--events-per-second` flag.

    cargo --release -- test --threads 2 --events-per-second 1000000 --seconds 10

By default the test results will be presented in a statistical summary. If you would instead like to get the per-epoch latencies that were measured in order to process the data yourself, you can use the `--report` flag.

    cargo --release -- test --report latencies

For an explanation of all available flags, their defaults, and their possible values, please see the help document.

    cargo -- help

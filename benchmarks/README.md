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

## Batch Processing
If you need to process a series of runs with different configurations in sequence, you can use the `run.sh` wrapper script. It accepts the following options:

* `-r` for the rate. The default is `1000 10000 100000 1000000 2000000 4000000 8000000 10000000`
* `-w` for the worker count. The default is `32`
* `-s` for the number of seconds. The default is `300`
* `-b` for the benchmarks. The default is `HiBench,Yahoo,NEXMark`

In each case if you want to run multiple configurations, separate each value by a space. For instance, to run a scaling experiment the following could be used:

    ./run.sh -r 10000000 -w "1 2 4 8 16 32"

The script will write all output to files reflecting the rate and worker count in the form of `rate@workers.csv`. These data files can then be processed into the suitable files for the plots by the scripts in the `data/` directory. See the documentation there for how to proceed.

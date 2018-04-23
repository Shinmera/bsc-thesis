## About the Timely Benchmark Project
This project includes a thesis on the implementation and surveying of various benchmarks applied to streaming processing systems like Timely. As part of this it includes a Rust framework for benchmarking and an implementation of three major streaming benchmarks.

## Compiling the Thesis
All data required for the document generation should be included in this repository. You will need a recent installation of LuaTeX and Biber. An optional Makefile is included to automate the compilation procedure, so simply running `make` within this directory should generate a fresh pdf.

## Running the Benchmarks
See the [README.md](benchmarks/README.md) in the `benchmarks/` directory for more information on how to run the benchmarks.

## Compiling the Plotted Data
The thesis includes plots generated from raw latency measurements. These measurements need to be pre-processed in order to be available for the plots. See the [README.md](data/README.md) in the `data/` directory for more information on how to perform this pre-processing.

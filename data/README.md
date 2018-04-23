## Data Wrangling
This set of files presents the raw statistical data gathered from experiments. The outputs from the benchmark's `--report latencies` runs are in `*@*.csv` files, while the processed data that is used for the plots in the document are in the `latency-*.csv`, `scaling-*.csv`, and `cdf-*.csv` files.

## Processing Latency Data
The script to transform the data is `transform.lisp` and will require a Lisp and Quicklisp installation to be present locally. For a quick setup, try [Portacle](https://portacle.github.io/). Once that's set, simply run the script in this `data/` directory and it will churn the data as required.

    sbcl --load transform.lisp

If using Portacle on linux, that would be:

    path/to/portacle/portacle.run sbcl --load transform.lisp

In case of missing files, the script will present you with a debugger and a list of "restarts". You can ignore the missing script by entering `continue`, or simply abort the process by entering `exit`.

## Transforming Beam Output Data Into Timely Benchmark Input Data
For validation purposes there's a script that transforms the output file from a Beam Query 0 run into the format as digested by the Timely benchmarking system that is a part of this repository. In order to get Beam to generate these files, a run like the following is required:

    mvn exec:java -Dexec.mainClass=org.apache.beam.sdk.nexmark.Main -Pflink-runner -Dexec.args="--runner=FlinkRunner --suite=SMOKE --streaming=false --manageResources=false --monitorJobs=true --flinkMaster=local --parallelism=1 --numEventGenerators=1 --sinkType=TEXT --outputPath=."

This should be done within the `beam/sdks/java/nexmark` directory and must be preceded by a first-time `mvn compile` call. See the sparse Beam documentation for other available options. I won't document them here for the time being.

Once the `nexmark_Query0_[..].txt-[..]` files have been generated, you can transform them with the `beam-to-timely.lisp` script. It requires the same setup as the `transform.lisp` script, meaning a Lisp implementation and Quicklisp. Running the script, assuming Portacle, would look like the following:

    path/to/portacle/portacle.run sbcl --load beam-to-timely.lisp nexmark_Query0_...

It will output the data to an `output.json` file, though you can change that by passing the desired target path as the second argument to the script:

    path/to/portacle/portacle.run sbcl --load beam-to-timely.lisp nexmark_Query0_... my-output.json

Should records be malformed for whatever reason, the script should completely ignore them in case of a parse failure, or fill the output fields with `null`s in case of missing fields.

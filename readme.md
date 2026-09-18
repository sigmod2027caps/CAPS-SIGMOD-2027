CAPS: How-Much Provenance for Streaming Dataflows
-----------------

CAPS is a lightweight provenance mechanism that reports how much each source contributed to an output: how many taxi trips
from each borough are behind an hourly traffic report, for example. It asks the
same question of a whole time window, not just a single output tuple.

It does this without tracking individual tuples. Every record carries one
counter per channel, so the annotation is the size of the dataflow and not of
the data, and an index over the output timestamps answers queries over a time
range in logarithmic time.

This repository holds the implementation, the three baselines it is compared
against, and one script per table and figure of the paper.

## Before running the experiments

```bash
chmod +x run.sh install_flink.sh experiments/*/run.sh data/taxis/download.sh
```

Requirements
-----------------
  - Linux. The scripts use the GNU forms of `stat` and `sha512sum` and need
    bash 4, so they do not run on macOS as they are; see macOS below.
  - Java 8 (OpenJDK 8). Flink 1.10 does not run on newer JDKs. `JAVA_HOME`
    defaults to `/usr/lib/jvm/java-8-openjdk-amd64`, the Debian/Ubuntu path;
    on any other distribution, export it yourself.
  - Apache Maven 3.x
  - Python 3.8+ with `pip install -r requirements.txt` (needed for the figures;
    without it the experiments still write their CSVs)
  - curl, and p7zip-full for the taxi dataset
  - 48 GB of RAM, which is what `install_flink.sh` gives the task manager.
    The GeneaLog baseline retains every output tuple and its provenance graph
    in the sink, and peaks at 9.6 GB on `taxi_1`; 48 GB is headroom over that,
    so a smaller machine can lower `taskmanager.memory.process.size` in
    `code/flink-1.10.0/conf/flink-conf.yaml`, but not below about 16 GB.
  - 15 GB of free disk: 9.4 GB of it is the taxi archive and its extraction,
    which can be deleted once `data/taxis/taxis.txt` exists.

Nothing else has to be installed by hand: `./run.sh` fetches Flink and builds
the inputs itself, as described under Run. Flink 1.10 is the version GeneaLog
was developed on, which is why the comparison uses it; point `FLINK_DIR` at an
existing Flink 1.10 to use your own, and raise `FLINK_DOWNLOAD_MINUTES` above
its default of 120 if your link is slow.

macOS
-----------------
macOS ships bash 3.2, which has no associative arrays, and the BSD forms of
`stat` and `sha512sum`. There are two ways around that.

A Linux container is the simpler one, and the one these instructions were
checked with. From the repository directory, which has to be somewhere under
your home folder for the container to see it:

    colima start --cpu 4 --memory 12 --disk 60
    docker run --rm -it -v "$PWD":/caps -w /caps ubuntu:22.04 bash

and then, inside the container:

    apt update && apt install -y openjdk-8-jdk maven curl python3-venv p7zip-full
    export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64    # -amd64 on Intel
    ./install_flink.sh

A virtual machine smaller than the task manager cannot start it, so lower the
setting to fit before running an experiment:

    sed -i 's/^taskmanager.memory.process.size: .*/taskmanager.memory.process.size: 8g/' \
        code/flink-1.10.0/conf/flink-conf.yaml

12 GB of virtual machine is enough for the two Nexmark dataflows; the taxi ones
need considerably more, since GeneaLog alone peaks at 9.6 GB on `taxi_1`.

Natively is the other way, with Homebrew supplying the missing pieces:

    brew install bash coreutils p7zip
    brew install --cask temurin@8        # only if no JDK 8 is installed

    export PATH="/opt/homebrew/opt/coreutils/libexec/gnubin:$PATH"
    export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
    /opt/homebrew/bin/bash run.sh

Use `/usr/local/...` in place of `/opt/homebrew/...` on Intel Macs, and note
that `/usr/libexec/java_home` without `-v 1.8` returns the newest JDK, which
Flink 1.10 will not run on; if your 7-Zip installs as `7zz` rather than `7z`,
`data/taxis/download.sh` has to be pointed at it. The dataset scripts under
`data/` need only coreutils and p7zip, and the JUnit tests need only a JDK 8,
so those work on a Mac whichever way you go.

Either way, keep the repository out of folders synced by Dropbox, iCloud or
OneDrive: they rewrite symlinks, which corrupts `venv/` and leaves
`venv/bin/python` pointing nowhere, and they leave conflicted copies under
`target/`.

Repository layout
-----------------

    code/
      caps/             the CAPS library: HowMuch (the per-channel counters
                        that ride on every tuple), the bench sinks that build
                        the online index, and BenchSinks, which picks one from
                        the mode argument the run scripts pass
      temporal_index/   the temporal index: TemporalIndex (sorted timestamp
                        array plus a sparse B-tree over it), CapsIndex (that
                        index plus prefix sums), ArrayStore (the scan baseline),
                        GenealogIndex (the same B-tree holding GeneaLog graphs)
      <dataflow>/       one Maven project per dataflow, four in total
      inkstream/        the Green baseline library: Polynomial/Monomial (classic
                        how-provenance annotations over the contributing input
                        records) and InkMeta, which projects them per channel at
                        the sink. Package name is ink; the paper calls the
                        baseline green_polynomial
      ink/<dataflow>/   the same four dataflows annotated with Green polynomials
      genealog/         the fine-grained competitor: Ananke/GeneaLog, from
                        https://github.com/dmpalyvos/ananke, with our dataflows
                        under usecases/ and the benchmarking sinks under
                        genealog/
      noprov/           the same four dataflows with no provenance at all, which
                        is what the runtime overhead is measured against
      query_jmh/        the JMH harness for the per-output provenance query: all
                        three methods answer the same 1,000 output identifiers

    data/
      nexmark/          generate_nexmark.py builds persons.txt and auctions.txt
      taxis/            download.sh fetches the raw files, make_taxis_txt.py
                        builds taxis.txt

    experiments/
      common.sh         per-dataflow parameters and shared helpers
      plot_utils.py     regenerates the figures from the result CSVs
      <experiment>/     run.sh plus one result directory per dataflow

    install_flink.sh    downloads and configures Flink 1.10
    run.sh              sets everything up, runs every experiment, prints
                        the result CSVs

The four dataflows are `taxi_1`, `taxi_2`, `nexmark_1` and `nexmark_2`, four of
the six of Figure 9 of the paper. The two missing ones, `twitter_1` and
`twitter_2`, read the X/Twitter dataset, which cannot be redistributed for
privacy reasons, so the artifact ships only the dataflows whose input anyone can
rebuild from public data. `taxi_1` is the dataflow of the introduction figure:
two virtual sources (Manhattan and Queens pickups) times two branches (solo and
crowded rides) give four how-much channels, one per path.

Each dataflow exists four times over, once per method: `code/<dataflow>` is
CAPS, `code/ink/<dataflow>` is the Green polynomial baseline,
`code/noprov/<dataflow>` is the same computation with no provenance, and the
GeneaLog version lives inside `code/genealog`. The `how_much`, `memory`,
`query_table`, `queries` and `query_length` experiments run several methods
side by side; `expiry` and `summarize` concern the index alone and run CAPS
only.

#Datasets

Nothing under `data/` is shipped; each directory holds the scripts that build
its input, and each has a README with the details. `./run.sh` invokes these for
you, but they can also be run on their own:

    cd data/nexmark && python3 generate_nexmark.py
    cd data/taxis   && ./download.sh && python3 make_taxis_txt.py

The nexmark generator is seeded and the taxi pipeline is deterministic over a
frozen 2013 archive snapshot, so both reproduce the inputs behind the paper's
numbers.

Run
-----------------

One command, from the root of the repository, does everything:

    ./run.sh

It needs no arguments and no prior setup beyond the requirements above. In
order, it installs Flink 1.10 into `code/flink-1.10.0`, creates `venv/` and
installs `requirements.txt` into it, builds the Nexmark and taxi inputs, then
runs the eight experiments, each of which builds the four versions of every
dataflow and runs them. Every step is skipped when its output is already in
place, so an interrupted run continues where it stopped and a second run is
cheap.

Expect hours, most of it downloading: the taxi archive is 4 GB from a throttled
mirror, and the Flink tarball is allowed up to two hours on its own.

Start smaller. Nexmark is generated locally, so this exercises the whole
pipeline end to end in a few minutes with nothing to download:

    DATAFLOWS=nexmark_2 REPS=1 ./run.sh

It is the same script and the same outputs, on one dataflow instead of four and
one timed repetition instead of three. When it finishes,
`experiments/how_much/nexmark_2/times.csv` holds one row per method, which is
the sign that Java, Maven, Flink and Python are all in order. Then drop the
variables and run the whole thing.

    DATAFLOWS         which dataflows to run    default: all four
    REPS              timed repetitions, median reported    default: 3

Each experiment is also a script of its own, and any single one can be run
without the others. These are the eight, in the order `./run.sh` calls them;
run one, not the list:

    ./experiments/how_much/run.sh
    ./experiments/memory/run.sh
    ./experiments/query_table/run.sh
    ./experiments/queries/run.sh
    ./experiments/expiry/run.sh
    ./experiments/summarize/run.sh
    ./experiments/query_length/run.sh
    ./experiments/paths/run.sh

The same variables apply:

    DATAFLOWS="taxi_1 nexmark_2" REPS=1 ./experiments/queries/run.sh

`paths` runs on `taxi_1` alone, the only dataflow here with more paths than
sources; given any other it says so and stops.

Each experiment writes one result directory per dataflow and a combined CSV at
the experiment level with a leading `dataflow` column. Only the five figures the
paper has are drawn; everything the paper reports as a table stops at the CSV.
Figures can be regenerated on their own from results that already exist:

    python3 experiments/plot_utils.py experiments/queries

Results and where they are reported
-----------------

| Experiment     | CSV                                              | Figure                            | Reported as                |
| -------------- | ------------------------------------------------ | --------------------------------- | -------------------------- |
| `how_much`     | `times.csv`, `times_stats.csv`                   | `how_much_runtime_bar.pdf`        | Figure 10 (top)            |
| `how_much`     | `query_ns.csv`                                   | table only                        | Table 2, in-situ timing    |
| `how_much`     | `memory_model.csv`, `memory_model_detail.csv`    | `how_much_memory_model_bar.pdf`   | Figure 10 (bottom)         |
| `memory`       | `metadata_volume.csv`, `how_much_comparison.csv` | table only                        | Measured meta-data volume  |
| `query_table`  | `query_table.csv`                                | table only                        | Per-output query time, JMH |
| `queries`      | `query_latency.csv`                              | `queries_query_latency_bar.pdf`   | Figure 11                  |
| `queries`      | `insert_latency.csv`, `index_stats.csv`          | table only                        | Section 7.2.2              |
| `expiry`       | `expiry_stats.csv`                               | table only                        | Table 3                    |
| `expiry`       | `expiry_memory.csv`                              | table only                        | Section 7.2.3              |
| `summarize`    | `summarize.csv`                                  | table only                        | Table 4                    |
| `query_length` | `query_length_latency.csv`                       | `query_length_latency_lines.pdf`  | Figure 12                  |
| `paths`        | `paths.csv`                                      | `paths_cost_bar.pdf`              | Figure 13                  |

"Table only" means the experiment stops at the CSV; only the five figures the
paper has are drawn.

Timed runs are repeated `REPS` times and the median is what the paper reports;
`times_stats.csv` carries the repetition count, the mean and the standard
deviation beside it.

Every entry is produced for the four dataflows above, which is the `taxi_1`,
`taxi_2`, `nexmark_1` and `nexmark_2` part of each table and figure. The
`twitter_1` and `twitter_2` part is not: no script here can produce it, because
the input cannot be redistributed. Figure 13 is the `paths` experiment and comes
out for `taxi_1` alone, the only dataflow here with more paths than sources.

There are two different memory numbers and they must not be mixed in one
comparison:

  - `how_much/memory_model.csv` is the **analytical** model of Section 7.2.1 and
    the number the paper reports (Figure 10, bottom): each method is charged the
    meta-data it must retain to answer a query, so CAPS pays
    `#outputs x #channels x 8` at the sink while GeneaLog pays 32 B on every
    operator output, because it keeps the whole contribution graph.
    `how_much/memory_model_detail.csv` splits this into `sink_bytes` (retained)
    and `intermediate_bytes` / `window_state_bytes` (live).

Tests
-----------------

Every library module carries a JUnit suite, 103 tests in total, which the
experiments skip (`-DskipTests`) but which can be run on their own:

    for m in temporal_index caps inkstream genealog query_jmh; do
      (cd code/$m && mvn test)
    done

They cover the pieces a result depends on and that a run cannot check for you:
that each dataflow attaches exactly the probes its operator set declares and
attaches each one once, that the probe CSV is canonical and refuses to be
written twice, that the shared-identifier query fixture holds the same 1,000
outputs for all three methods, that a GeneaLog graph survives cloning and its
traversal answers match the counters, and that the JMH harness keeps the
annotations that make its numbers meaningful.



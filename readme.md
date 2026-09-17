# CAPS - How-Much Provenance for Streaming Dataflows

CAPS is a lightweight provenance mechanism that reports how much each source contributed to an output: how many taxi trips
from each borough are behind an hourly traffic report, for example. It asks the
same question of a whole time window, not just a single output tuple.

It does this without tracking individual tuples. Every record carries one
counter per channel, so the annotation is the size of the dataflow and not of
the data, and an index over the output timestamps answers queries over a time
range in logarithmic time.

This repository holds the implementation, the three baselines it is compared
against, and one script per table and figure of the paper.

- [Requirements](#requirements)
- [Quick start](#quick-start)
- [Running on macOS](#running-on-macos)
- [Repository layout](#repository-layout)
- [Dataflows](#dataflows)
- [Methods](#methods)
- [Datasets](#datasets)
- [Running the experiments](#running-the-experiments)
- [Results and where they are reported](#results-and-where-they-are-reported)
- [Memory](#memory)
- [Tests](#tests)
- [Offline query tool](#offline-query-tool)
- [Troubleshooting](#troubleshooting)

## Requirements

- Linux. On a Mac, see [Running on macOS](#running-on-macos).
- Java 8 (OpenJDK 8). Flink 1.10 does not run on newer JDKs, and Flink 1.10
  is the version GeneaLog was developed on, which is why the comparison uses
  it. `JAVA_HOME` defaults to `/usr/lib/jvm/java-8-openjdk-amd64`, the
  Debian/Ubuntu path; export it yourself on any other system.
- Apache Maven 3.x
- bash 4 or newer, for the associative arrays the experiment scripts use, and
  the GNU forms of `stat` and `sha512sum`
- Python 3.8+ with `pip install -r requirements.txt`, needed for the figures;
  without it the experiments still write their CSVs. Very new releases are
  best avoided: matplotlib and pandas wheels lag them, and 3.14 has none.
- curl, and p7zip for the taxi dataset
- 48 GB of RAM, which is what `install_flink.sh` gives the task manager. The
  GeneaLog baseline retains every output tuple and its provenance graph in
  the sink and peaks at 9.6 GB on `taxi_1`; 48 GB is headroom over that, so a
  smaller machine can lower `taskmanager.memory.process.size` in
  `code/flink-1.10.0/conf/flink-conf.yaml`, but not below about 16 GB.
- 15 GB of free disk. 9.4 GB of it is the taxi archive and its extraction,
  which can be deleted once `data/taxis/taxis.txt` exists.

Point `FLINK_DIR` at an existing Flink 1.10 to skip the download.

## Quick start

Nexmark is generated locally, so the smallest dataflow tests the whole setup
without the taxi download:

```bash
DATAFLOWS=nexmark_2 REPS=1 ./experiments/how_much/run.sh
```

This installs Flink, builds the four methods and runs them once. If
`experiments/how_much/nexmark_2/times.csv` has one row per method, everything
works.

The taxi dataflows need their input built first, which is the 4 GB download:

```bash
(cd data/taxis && ./download.sh && python3 make_taxis_txt.py)
DATAFLOWS="taxi_1 taxi_2" REPS=1 ./experiments/how_much/run.sh
```

Then the full set, on all four dataflows:

```bash
pip install -r requirements.txt
./run.sh
```

`run.sh` installs Flink 1.10 into `code/flink-1.10.0`, builds the two input
datasets, and runs every experiment, skipping whatever is already done. It
takes hours, most of it downloading: archive.org throttles the taxi files, and
`install_flink.sh` allows up to two hours for the Flink tarball alone (raise
`FLINK_DOWNLOAD_MINUTES` if your link is slower). Both resume, so an
interrupted run continues where it stopped.

| Variable                 | Meaning                                        | Default             |
| ------------------------ | ---------------------------------------------- | ------------------- |
| `DATAFLOWS`              | space-separated subset to run                  | all four            |
| `REPS`                   | repetitions of each timed run; median reported  | `3`                 |
| `FLINK_DIR`              | an existing Flink 1.10                         | `code/flink-1.10.0` |
| `JAVA_HOME`              | JDK 8 to build and run with                    | Debian/Ubuntu path  |
| `FLINK_DOWNLOAD_MINUTES` | patience for the Flink tarball                 | 120                 |

## Running on macOS

Everything above assumes Linux. macOS ships bash 3.2, which has no associative
arrays, and the BSD forms of `stat` and `sha512sum`, so the experiment scripts
do not run on it as they are. There are two ways around that.

The JUnit tests are the exception: they need neither of these routes, only a
JDK 8. See [Tests](#tests).

### Option 1: in a Linux container

The simpler of the two, and the one these instructions were checked with. From
the repository directory, which has to be somewhere under your home folder for
the container to see it:

```bash
colima start --cpu 4 --memory 12 --disk 60
docker run --rm -it -v "$PWD":/caps -w /caps ubuntu:22.04 bash
```

Everything from here on runs inside the container:

```bash
apt update && apt install -y openjdk-8-jdk maven curl python3-venv p7zip-full
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-arm64    # -amd64 on Intel
./install_flink.sh
```

A virtual machine smaller than the task manager cannot start it, so lower the
setting to fit before running an experiment:

```bash
sed -i 's/^taskmanager.memory.process.size: .*/taskmanager.memory.process.size: 8g/' \
    code/flink-1.10.0/conf/flink-conf.yaml
```

12 GB of virtual machine is enough for the two Nexmark dataflows. The taxi ones
need considerably more, since GeneaLog alone peaks at 9.6 GB on `taxi_1`, so
either give the virtual machine more memory or run them on a Linux machine.

### Option 2: natively, with the Homebrew tools

Homebrew supplies the missing pieces:

```bash
brew install bash coreutils p7zip
brew install --cask temurin@8        # only if no JDK 8 is installed
```

Then put the GNU tools first on the path, point `JAVA_HOME` at the JDK 8, and
run the scripts with the newer bash:

```bash
export PATH="/opt/homebrew/opt/coreutils/libexec/gnubin:$PATH"
export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)
/opt/homebrew/bin/bash run.sh
```

Use `/usr/local/...` in place of `/opt/homebrew/...` on Intel Macs.
`/usr/libexec/java_home` without `-v 1.8` returns the newest JDK, which Flink
1.10 will not run on; and if your 7-Zip installs as `7zz` rather than `7z`,
`data/taxis/download.sh` has to be pointed at it.

The dataset scripts under `data/` need only coreutils and p7zip, not the newer
bash, so the inputs can be built on a Mac even when the experiments run
elsewhere.

### Either way

Keep the repository out of folders synced by Dropbox, iCloud or OneDrive. They
rewrite symlinks, which corrupts `venv/` and leaves `venv/bin/python` pointing
nowhere, and they leave conflicted copies under `target/`.

## Repository layout

```text
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
                    the sink. The package name is ink; the paper calls the
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
  memory_model.py   the analytical meta-data model of Section 7.2.1
  <experiment>/     run.sh plus one result directory per dataflow

install_flink.sh    downloads and configures Flink 1.10
run.sh              sets everything up, runs every experiment, prints
                    the result CSVs
```

## Dataflows

| Dataflow    | Sources | Channels | Window        |
| ----------- | ------- | -------- | ------------- |
| `taxi_1`    | 1       | 4        | 10 min slide  |
| `taxi_2`    | 1       | 2        | 10 min slide  |
| `nexmark_1` | 2       | 2        | 12 h tumbling |
| `nexmark_2` | 2       | 2        | 12 h tumbling |

These are four of the six dataflows of Figure 9 of the paper. The other two,
`twitter_1` and `twitter_2`, run on a Twitter dataset that is proprietary and
cannot be shipped, so only the dataflows whose input anyone can rebuild from
public data are here.

`taxi_1` is the dataflow of the introduction figure: two virtual sources
(Manhattan and Queens pickups) times two branches (solo and crowded rides) give
four how-much channels, one per path. It is the only dataflow here with more
paths than sources, so it is the only one the `paths` experiment runs.

## Methods

Each dataflow exists four times over, one per method.

| Name in the CSVs   | Where it lives           | What it does                                                     |
| ------------------ | ------------------------ | ---------------------------------------------------------------- |
| `noprov`           | `code/noprov/<dataflow>` | the same computation with no provenance; the overhead baseline     |
| `caps`             | `code/<dataflow>`        | per-channel counters maintained at every operator                  |
| `green_polynomial` | `code/ink/<dataflow>`    | how-provenance polynomials kept to the sink and projected there     |
| `genealog`         | `code/genealog`          | fine-grained tuple-level provenance; a graph traversed per query    |

The `how_much`, `memory`, `query_table`, `queries` and `query_length`
experiments run several methods side by side; `expiry` and `summarize` concern
the index alone and run CAPS only.

## Datasets

Nothing under `data/` is shipped; each directory holds the scripts that build
its input. `./run.sh` invokes them for you, but they can also be run on their
own:

```bash
cd data/nexmark && python3 generate_nexmark.py
cd data/taxis   && ./download.sh && python3 make_taxis_txt.py
```

**Nexmark** is generated locally, with a fixed seed, and needs nothing but
Python. `generate_nexmark.py` documents its parameters and its output format in
its docstring; by default it writes 500K persons and 2M auctions.

**Taxis** is the January 2013 yellow-cab data, 14.8M rides, from the 2013 FOIL
release on archive.org. It has to be that release: it still carries the taxi
identifier (medallion), which `taxi_1` groups and joins on, and every official
TLC file has that column scrubbed, including the re-processed 2013 one. So the
official data cannot be substituted here.

`download.sh` fetches the two 7z archives and extracts them, about 4 GB down
and 9.4 GB on disk, and resumes if it is interrupted. `make_taxis_txt.py` then
joins trips to fares, filters to the NYC bounding box, maps pickups onto the
1000-region grid the paper uses, and writes `taxis.txt` capped at 10M rides. It
needs pandas, so run it from the virtualenv `run.sh` creates, or from any
Python that has it. A smaller input, for a quicker look, is a matter of one
argument:

```bash
python3 make_taxis_txt.py 1000000
```

`data/taxis/README.txt` has the column-by-column detail. Both pipelines are
deterministic, the nexmark generator through its seed and the taxi one through
a stable sort over a frozen archive snapshot, so both reproduce the inputs
behind the paper's numbers byte for byte.

## Running the experiments

In the order `run.sh` uses them:

```bash
./experiments/how_much/run.sh
./experiments/memory/run.sh
./experiments/query_table/run.sh
./experiments/queries/run.sh
./experiments/expiry/run.sh
./experiments/summarize/run.sh
./experiments/query_length/run.sh
./experiments/paths/run.sh
```

A subset of dataflows:

```bash
DATAFLOWS="taxi_1 nexmark_2" ./experiments/queries/run.sh
```

Each experiment writes one result directory per dataflow and a combined CSV at
the experiment level with a leading `dataflow` column. Only the five figures
the paper has are drawn; everything the paper reports as a table stops at the
CSV. Figures can be regenerated on their own from results that already exist:

```bash
python3 experiments/plot_utils.py experiments/queries
```

## Results and where they are reported

Our own results are already in the repository: every experiment directory holds
its CSVs, and the five figures sit in `how_much/`, `queries/`, `query_length/`
and `paths/`. Re-running an experiment overwrites them, so copy them first if
you want to compare. `experiments/memory/` and `experiments/query_table/` have
their own README with the details of those two.

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
`twitter_1` and `twitter_2` part is not, since their input cannot be shipped.
Figure 13 comes out for `taxi_1` alone.

## Memory

Two experiments report memory and they count different things, so their numbers
do not belong in the same comparison.

**What a method keeps.** `how_much/memory_model.csv` counts the meta-data that
has to stay in memory so that a query can be answered later. This is the model
of Section 7.2.1 and the number in Figure 10 (bottom). CAPS keeps its counters
at the sink, so it is charged `#outputs x #channels x 8`. GeneaLog keeps a
contribution graph, so it is charged at every operator output.
`memory_model_detail.csv` splits the total into what stays (`sink_bytes`) and
what is only alive while the job runs (`intermediate_bytes`,
`window_state_bytes`).

**What passes through.** `memory/metadata_volume.csv` counts the annotation
bytes the job produced from start to finish. Flink accumulators add up the
annotation on every operator output as it is emitted, whether it is kept
afterwards or not.

The two differ by a lot. On `taxi_1` CAPS is 150 MB of kept meta-data and
1430 MB of annotation produced. Neither one is the peak memory of the process,
and a comparison between methods has to take all of them from the same file.

## Tests

Every library module carries a JUnit suite, 103 tests in total, which the
experiments skip (`-DskipTests`) but which can be run on their own:

```bash
for m in temporal_index caps inkstream genealog query_jmh; do
  (cd code/$m && mvn test)
done
```

They check the things a run cannot check for itself:

- each dataflow attaches the probes its operators declare, each one once
- the probe CSV is written in a canonical form, and only once
- the query fixture holds the same 1,000 outputs for all three methods
- a GeneaLog graph survives cloning, and its traversal agrees with the
  counters
- the JMH harness keeps the annotations its numbers depend on

They need neither Flink nor the datasets, so they are the fastest way to check
a toolchain.

## Offline query tool

`caps.Main` loads a CAPS sink file into the index after the fact, for
inspecting provenance outside a run:

```bash
java -cp code/caps/target/classes caps.Main <sink.out> <channels> \
    query <ts> <te>
```

`delta`, `bench` and `selftest` are also accepted; `selftest` checks the
examples of Figure 5 of the paper.

## Troubleshooting

**`common.sh: line 46: nexmark_1: unbound variable`**

bash 3.2 is running the scripts; it has no associative arrays, so the
`declare -A` tables are read as indexed arrays and their keys as variables.
Install bash 4 or newer and run with it.

**`The JAVA_HOME environment variable is not defined correctly`**

`JAVA_HOME` is unset or points at a newer JDK. `/usr/libexec/java_home -V`
lists what macOS has; on Debian/Ubuntu the path is
`/usr/lib/jvm/java-8-openjdk-amd64`.

**`venv/bin/python: No such file or directory`**

The virtualenv is broken, usually because the repository sits in a synced
folder that rewrote its symlinks. `rm -rf venv`, then let `run.sh` rebuild it.

**`7z not found`**

Install p7zip. If your 7-Zip is the newer `7zz` binary, point
`data/taxis/download.sh` at it.

**`stat: illegal option -- c`**

A BSD `stat` is running a GNU invocation. Put coreutils' `gnubin` first on
`PATH`, as under [Running on macOS](#running-on-macos).

**`missing job duration in: ... rep1_job.json`**

Flink started but the job failed. The reason is in `code/flink-1.10.0/log/`;
the usual cause is a JDK newer than 8.

**Flink does not start in a container**

The task manager is configured for 48 GB and the virtual machine is smaller.
Lower `taskmanager.memory.process.size` in
`code/flink-1.10.0/conf/flink-conf.yaml`.

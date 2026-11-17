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

Before running the experiments
-----------------
Make the scripts executable, once, from the root of the repository:

    find . -name "*.sh" -exec chmod +x {} +
    chmod +x experiments/disorder/inprocess/bin/flink experiments/heap_check/heap_sampler.py experiments/verify/verify.py

Quick start
-----------------

    pip install -r requirements.txt          # or let ./run.sh create venv/ for you
    SMALL=1 REPS=1 ./run.sh                  # quick check on 10% subsets of the inputs, 3.5 to 7 hours
    ./run.sh                                 # everything, all six dataflows, REPS = 3 (the numbers of the paper, more than a day)
    PHASES=plot ./run.sh                     # after a run: redraw every figure and table from its results

`./run.sh` installs Flink 1.10 and a Python virtualenv, builds the inputs (generates Nexmark, downloads and preprocesses the
taxi trips; the tweets are included), builds the code, checks that the four methods agree, runs every experiment and writes
every figure (PDF) and table (CSV and LaTeX) of Section 7 next to the experiment that produces it. The quick check took
about 3.5 h on a 28-thread Linux machine with 128 GB of RAM and about 6.5 h (plus 1 h of setup) in Docker on an 8-core Mac
with 12 GB for the VM. A failed experiment does not stop the run: its output is kept in `logs/` and the failures are
listed at the end. `experiments/README.md`
maps every figure and table of the paper to the command that regenerates it, its runtime and its output files.

Environment
-----------------
`./run.sh` needs only Linux, bash, curl, tar and Python 3, and internet access. Java 8, Maven and 7-Zip are used if
the machine has them and otherwise downloaded into `tools/` by `bootstrap_tools.sh` (no root rights needed). With
Docker, `./docker_run.sh` runs the same `./run.sh` in a plain Ubuntu container (same options, e.g.
`SMALL=1 REPS=1 ./docker_run.sh`); the results are written into this directory. Details:

  - Linux. The scripts use the GNU forms of `stat`, `sha512sum` and `awk`; on a Mac, use a Linux container.
  - Java 8 (Flink 1.10 does not run on newer JDKs): `JAVA_HOME` if it is a JDK 8, else `/usr/lib/jvm/java-8-openjdk-*`,
    else Eclipse Temurin 8 downloaded into `tools/jdk8`.
  - Apache Maven 3.x (`mvn` on the path, else Maven 3.9.9 downloaded into `tools/maven`).
  - Python 3.8+ with `pip install -r requirements.txt` (matplotlib, pandas, numpy; the figures, the tables
    and the verification scripts). Without matplotlib the experiments still write their CSVs.
  - Flink 1.10.0, installed by `install_flink.sh` into `code/flink-1.10.0` (a checksummed download of the Apache
    release, then configured: one TaskManager with 48 GB, 8 slots, heartbeat timeout 5 min). Flink 1.10 is the
    version GeneaLog was developed on, which is why the comparison uses it. Point `FLINK_DIR` at an existing 1.10
    installation to use your own. Every timed run uses a fresh standalone cluster (REST on port 8081).
  - curl; 7-Zip for the taxi archives (`7z` on the path, else 7-Zip 25.01 downloaded into `tools/`).
  - RAM: 48 GB is what `install_flink.sh` gives the task manager. The GeneaLog temporal-query baseline keeps
    every output tuple and its provenance graph in the sink (9.6 GB on `taxi_1`), and the Green baseline keeps every output
    polynomial in its index (about 19 GB of heap on `taxi_2`), so 64 GB of RAM is comfortable for the full data. A
    smaller machine can lower `taskmanager.memory.process.size` in `code/flink-1.10.0/conf/flink-conf.yaml`
    (not below about 16 GB for the full data). The numbers of the paper were
    taken on a 28-thread Intel i7-14700K with 128 GB of RAM.
  - Disk: 15 GB free, of which 9.4 GB is the taxi archive and its extraction (can be deleted once
    `data/taxis/taxis.txt` exists); the experiments themselves write a few hundred MB of results and logs, and the
    disorder and recovery experiments a few GB of raw work directories (git-ignored).
  - One machine, one Flink job at a time. Every Flink job runs under `flock -o /tmp/caps-flink.lock` (`with_cluster` in
    `experiments/common.sh`: fresh cluster, job, cluster stopped, one lock; it refuses to time when another Flink
    cluster is running on the machine). Do not wrap a `run.sh` in `flock` yourself.

macOS
-----------------
macOS ships bash 3.2, which has no associative arrays, and the BSD forms of
`stat` and `sha512sum`, so the scripts do not run on it directly. Run them in a
Linux container with `./docker_run.sh`, which is how these instructions were
checked. From the repository directory, which has to be somewhere under
your home folder for the container to see it:

    colima start --cpu 4 --memory 12 --disk 60      # or Docker Desktop with the same resources
    SMALL=1 REPS=1 ./docker_run.sh                  # quick check; ./docker_run.sh alone runs everything

`./docker_run.sh` builds the image of `docker/Dockerfile` and runs `./run.sh` in it, so nothing has to be
installed by hand, and the results are written into this directory. 12 GB for the virtual machine is what
the quick check was run with (see Quick start); the full data needs the memory listed under Environment.

Datasets
-----------------
The Twitter/X data (`data/tweets/twitter.txt`, 5.3 MB, anonymized: see `data/tweets/README.txt`) is included in the artifact; the Nexmark and taxi inputs are
built by the scripts of their directories (the file formats are in "Preprocessing interface" below). `./run.sh` invokes them, but they can also be run on their own:

    cd data/nexmark && python3 generate_nexmark.py                          # persons.txt, auctions.txt (seed 42)
    cd data/taxis   && ./download.sh && python3 make_taxis_txt.py           # taxis.txt (~5.8 GB download)
    python3 data/make_small.py                                              # data/small/: the 10% subsets (SMALL=1)

  - **Nexmark** is generated (seeded). **Taxi**: the January 2013 NYC yellow-cab trips and fares of the archive.org
    FOIL release, a frozen snapshot; the pipeline is deterministic. Both reproduce the inputs behind the paper.
  - `sha256sum -c data/SHA256SUMS` (from the repository root) checks that the four input files are byte-identical to
    the ones behind the paper.
  - **Twitter/X**: the 100,000-tweet scrape of the paper (91 days) is **included** in the artifact as
    `data/tweets/twitter.txt` (5.3 MB, anonymized), so the paper's `twitter_1` / `twitter_2` numbers can be reproduced
    as they are. The record schema and the preprocessing from the raw scrape (which is not included) are in
    `data/tweets/README.txt` and `make_twitter_txt.py`.
  - **Synthetic scale data** for the channel-cardinality experiment: `data/synthetic_scale/generate_synthetic_scale.py`
    (seeded; made by the experiment).
  - **Subsets**: `SMALL=1` reads `data/small/` (first 10% of each stream, same file names), built by `data/make_small.py`.
    This includes the first 10,000 tweets (`data/small/tweets/twitter.txt`; 2,099,409 and 62,531 outputs for
    `twitter_1` and `twitter_2`, `SMALL_OUTPUTS` in `experiments/common.sh`). The results of a `SMALL=1` run never overwrite those of
    a full run, see below.

Build
-----------------
The experiments build what they need (`build_all` in `experiments/common.sh`: `mvn clean install` of `code/temporal_index`,
`code/tiered_index`, `code/caps`, then one `mvn clean package` per dataflow, and with the competitors `code/genealog`,
`code/inkstream`, `code/noprov/<dataflow>`, `code/ink/<dataflow>`; JMH modules on demand). To build by hand, run the same
`mvn -q clean install -DskipTests` in that order, with `JAVA_HOME` set to the JDK 8. `SKIP_BUILD=1` skips the build when the jars are up to date.

Paper item to command to output
-----------------
Every command is run from the repository root; `SMALL=1 REPS=1` in
front gives the quick check on the 10% subsets (results in `small/` or `*_small*`, never over the full ones), and
`PHASES=plot` in front redraws from the results of an earlier run without building or running anything. The
repository holds no results: every number comes from your own run. With `SMALL=1` the sweeps are shorter (Figure 11
has only m = 8); the full sweep, m = 2 to 512, needs the full run. Runtimes are in
`experiments/README.md`; each experiment has a short `README.md`.

| Paper item | Command | Output file(s) |
| ---------- | ------- | -------------- |
| Figure 8: runtime of the four methods on the six dataflows | `experiments/how_much/run.sh` | `experiments/how_much/how_much_runtime_norm.pdf` (+ `.csv`), `runtime_table.tex` |
| Table 2: meta-data memory, retained (analytical) | `experiments/how_much/run.sh` | `experiments/how_much/memory_model.csv`, `memory_model_detail.csv` |
| Cross-check (not in the paper): meta-data volume measured by probes | `experiments/memory/run.sh` | `experiments/memory/metadata_volume.csv`, `memory_table.{csv,tex}` |
| Table 2: peak live annotation state | `experiments/live_memory/run.sh` (paper: `LIVE_REPS=2`) | `experiments/live_memory/results_p1/live_memory_table.{csv,tex}` |
| Table 2: retained memory measured (peak heap above No Prov) | `experiments/heap_check/run.sh` | `experiments/heap_check/results_index/heap_check.csv`, `heap_check_bar.pdf` |
| Figure 9a: per-output query latency (JMH and in situ) | `experiments/query_table/run.sh` | `experiments/query_table/traversal.pdf`, `traversal.csv`, `query_table_summary.csv`; in-situ `experiments/how_much/query_ns.csv` |
| Figure 9b: latency against the query window length | `experiments/query_length/run.sh` | `experiments/query_length/query_length_latency_lines.pdf` (+ `.csv`) |
| Figure 9c: counters per source vs per path | `experiments/paths/run.sh` | `experiments/paths/paths_cost_bar.pdf` (+ `.csv`) |
| Figure 10: temporal how-much query latency | `experiments/queries/run.sh` | `experiments/queries/queries_howmuch_bar.pdf` (+ `.csv`) |
| Figure 11: meta-data and per-output query latency against the number of channels m (taxi) | `experiments/channel_scaling_real/run.sh` | `experiments/channel_scaling_real/results/channel_{pertaxi,perzone}_{mem,lat}.pdf`, `channel_legend.pdf` (numbers in `memory.csv`, `query.csv`) |
| Section 7.5 branch test (m real branches) | `experiments/channel_scaling/run.sh` | `experiments/channel_scaling/results/full_p1_cluster/*.pdf`, `crossover.csv` |
| Section 7.5 nested paths (512 paths recombined by a join) | `experiments/nested_paths/run.sh` | `experiments/nested_paths/results/nested.{csv,pdf}` |
| Table 3: disorder, skew, parallelism, late data | `experiments/disorder/run.sh`; delta arms: `PACED=1 experiments/disorder/run.sh` | `experiments/disorder/results_full/tables.md`, `e3_*.csv`, `e3_*.pdf`; `results_full_paced_howmuch/` |
| Section 7.7: recovery | `experiments/recovery/run.sh` | `experiments/recovery/results_full_howmuch{,_p4_D60000_d60000}/recovery_{verify,runs,summary}.csv`, `recovery_overhead.pdf` |
| Table 4: expiry (7-day horizon) | `experiments/expiry/run.sh` | `experiments/expiry/expiry_table.{csv,tex}` |
| Table 4: summarization | `experiments/summarize/run.sh` | `experiments/summarize/summarize_table.{csv,tex}` |
| Section 7.4: cost of index maintenance | `experiments/index_overhead/run.sh` | `experiments/index_overhead/index_overhead.csv`, `index_overhead_runtime_bar.pdf` |
| Section 4.3: largest per-channel count | `experiments/max_count/run.sh` | `experiments/max_count/max_count_p1.{csv,pdf}` |
| Correctness (not a paper item) | `experiments/verify/run.sh`, `experiments/verify/index_check.sh` | `experiments/verify/verify_<tag>.csv`, `index_check_<tag>.csv` |

Preprocessing interface
-----------------
Every workflow (all four methods) reads plain text files, one record per line, UTF-8, comma-separated, no header,
**sorted by timestamp, non-decreasing** (the sources emit watermarks with no out-of-orderness by default; unique
timestamps are expected for exact cross-checking of the provenance answers). Timestamps are epoch milliseconds. The
file names are fixed by `experiments/common.sh`; `SMALL=1` reads the same names under `data/small/`.

| workflow | file(s) | columns | made by |
| -------- | ------- | ------- | ------- |
| `taxi_1`, `taxi_2` | `data/taxis/taxis.txt` | `taxi_id,ts_ms,passengers,zone,payment,fare,tip` (zone 0..999, payment CRD/CSH/OTH, rides outside the borough map are skipped) | `data/taxis/download.sh`, `make_taxis_txt.py` |
| `twitter_1`, `twitter_2` | `data/tweets/twitter.txt` | `tweetId,ts_ms,hashtags(a\|b),likes,retweets,isVerified,isBlueVerified,location` (8 columns, see `data/tweets/README.txt`) | included; `make_twitter_txt.py` documents the derivation from the raw scrape |
| `nexmark_1`, `nexmark_2` | `data/nexmark/persons.txt`, `auctions.txt` | persons `personId,ts_ms,name,city,state`; auctions `auctionId,ts_ms,sellerId,category,itemName`; timestamps unique across both files | `data/nexmark/generate_nexmark.py` (seed 42) |

To plug in a new dataset for an existing workflow, write a file in the format above to the path of the table (`dataflow_inputs` in
`experiments/common.sh` maps a dataflow to its paths; `DATA_ROOT` is `data/` or `data/small/`), keep it sorted, and set `FULL_OUTPUTS` and `SAMPLE_PROB` in
`experiments/common.sh` to its output count and a probability that samples about 1,000 outputs; `experiments/verify/run.sh`
gives the output counts. A new workflow needs its source parser (the `parse` method of the `*Caps.java` class of its
dataflow, a few lines), its four variants (`code/noprov`, `code/ink`, the `genealog` use case) and an entry in `common.sh`.
`python3 data/make_small.py` builds the 10% subset. `data/tweets/stats_tweets.py` prints statistics of the tweets.

Repository layout
-----------------

    code/
      caps/             the CAPS library: HowMuch (the per-channel counters that ride on every tuple), the bench
                        sinks (TieredIndexSink and its subclasses IndexingSink, QueryLengthSink, ExpirySink,
                        SummarizeSink; ProvenanceQuerySink for the per-output query), BenchSinks (picks one from the
                        mode argument of the run scripts), and the recovery sinks (OutputLogSink, SegmentStore)
      tiered_index/     the two-tier index: Tier 1 (an augmented red-black tree holding the outputs no watermark has
                        certified), Tier 2 (the append-only CapsIndex), TwoTierIndex, ShardedIndex, the final /
                        provisional / undefined answers
      temporal_index/   TemporalIndex (sorted timestamps plus a sparse B-tree), CapsIndex (that index plus prefix
                        sums), ArrayStore (the scan baseline), GenealogIndex (the same B-tree holding GeneaLog
                        graphs), StampDerivedTime (the operator all four methods share), and the shared source path:
                        DisorderSource, Disorder, DisorderConfig (parallel readers, watermarks, controlled disorder,
                        checkpointed offsets), LiveState (peak live annotation state, instrumented model bytes)
      <dataflow>/       one Maven project per dataflow (six): the CAPS version
      noprov/<dataflow>/  the same dataflows with no provenance, what the runtime overhead is measured against
      inkstream/        the Green baseline library: Polynomial/Monomial (how-provenance annotations over the
                        contributing input records), InkMeta, and the Green index sink. Package name is ink;
                        the paper calls this baseline Green (green_polynomial in the scripts and CSVs)
      ink/<dataflow>/   the same dataflows annotated with Green polynomials
      genealog/         the fine-grained competitor, Ananke/GeneaLog (https://github.com/dmpalyvos/ananke), with our
                        dataflows under usecases/ and our benchmark sinks under genealog/
      query_jmh/        the JMH harness for the per-output provenance query: all methods answer the same 1,000
                        output identifiers
      synthetic_scale/  the synthetic dataflows of the branch test of Section 7.5 (noprov, caps, ink, genealog
                        and its JMH bench)
      real_scale/       the window dataflow on the real taxi input of Figure 11 (all methods, dense and sparse CAPS)
                        and its JMH bench

    data/               generators and inputs (see Datasets); make_small.py
    experiments/
      common.sh         per-dataflow parameters and shared helpers (with_cluster, run_caps, run_green, ...)
      common.py, plot_utils.py, tables.py, aggregate_*.py   the plot style (one colour and hatch per method), the
                        figures, the CSV and LaTeX tables, the aggregation over repetitions
      <experiment>/     run.sh plus the results (CSV, PDF, LaTeX); see experiments/README.md
    install_flink.sh    downloads and configures Flink 1.10
    run.sh              sets everything up and runs every experiment

The four methods
-----------------
The six dataflows of the paper are `taxi_1`, `taxi_2`, `nexmark_1`, `nexmark_2`, `twitter_1` and `twitter_2`
(TX1, TX2, NX1, NX2, TW1 and TW2 in the paper).
`taxi_1` is the dataflow of the introduction figure: two virtual sources (Manhattan and Queens pickups) times two
branches (solo and crowded rides) give four how-much channels, one per path. Each dataflow exists four times over:

| method (paper name)       | code                          | what it keeps per tuple / per output            |
| ------------------------- | ----------------------------- | ----------------------------------------------- |
| No Prov                   | `code/noprov/<dataflow>`      | nothing                                         |
| CAPS (ours)               | `code/<dataflow>` + `code/caps`, `code/tiered_index`, `code/temporal_index` | one counter per channel |
| Green polynomial          | `code/ink/<dataflow>` + `code/inkstream` | a provenance polynomial over the contributing source tuples |
| GeneaLog (fine-grained)   | `code/genealog` (`usecases/*/provenance`) | the contribution graph (pointers)    |

All four read through the same source and watermark path (`temporalindex.Disorder` / `DisorderSource`, parallel
readers, a periodic watermark every 1 ms by default) and stamp every window result with `StampDerivedTime`, so they
differ only in what they do with the tuples. The `how_much`, `memory`, `query_table`, `queries`, `query_length`,
`live_memory` and `channel_scaling` experiments run several methods side by side; `expiry`, `summarize`, `disorder` and
`recovery` concern the CAPS index alone.

Time semantics
-----------------
Every output tuple carries a derived timestamp, the one the temporal index is keyed by. A windowed operator (window
aggregate, window join) stamps its output with the right boundary of its window instance (`window.maxTimestamp()`, one
millisecond before the window end); this is neither the firing time nor the newest contributing input. Map, filter,
flatmap, union and multiplex copy the timestamp of their input. `StampDerivedTime` writes the timestamp that Flink
gives a window result into the tuple, so the field and the record timestamp agree. As a consequence the input of a
chained index sink is in non-decreasing timestamp order per subtask, even when the sources read out of order. The index
sinks do not reorder by a slack: every output goes into Tier 1 of a two-tier index and a Flink watermark moves the
certified prefix into the append-only Tier 2, in order. Each sink subtask owns one shard; the shards are merged after
the job (frontier = the minimum shard watermark, answer = channel-wise sum). An answer is *final* when the whole range
is below the watermark, *provisional* otherwise, and *undefined* (the share is not 0) when no output falls in the range.
The GeneaLog and Green index baselines get the same Tier 1, so all methods index the same outputs in the same order.

Run
-----------------
Everything, from a fresh checkout (the inputs are built on the way):

    ./run.sh                              # all six dataflows
    SMALL=1 REPS=1 ./run.sh               # quick check
    PHASES=plot ./run.sh                  # after a run: redraw all figures and tables from its results

One experiment (each takes its parameters from the lines at the top of its `run.sh`, or from the environment):

    ./experiments/queries/run.sh
    DATAFLOWS="taxi_1 nexmark_2" REPS=1 ./experiments/queries/run.sh
    PHASES=plot ./experiments/queries/run.sh        # no build, no cluster: aggregate, tables, figures

`EXPERIMENTS="queries expiry" ./run.sh` runs a subset through the top-level script. The commands, runtimes and outputs
of all experiments are in `experiments/README.md`.

Environment switches (all optional):

| variable             | default | meaning |
| -------------------- | ------- | ------- |
| `DATAFLOWS`          | all six | space-separated subset of the dataflows (only their inputs are built). Without it, each `run.sh` takes the list at its top, one dataflow per line (comment lines out); the top-level `run.sh` passes its own list on |
| `REPS`               | 3       | repetitions of every timed run, each on a fresh cluster; tables report the median, figures min-max |
| `SMALL`              | 0       | 1: the 10% subsets in `data/small` (and a scaled sampling probability and expiry horizon); a quick check, not numbers. The results go to separate files, see below |
| `CAPS_PARALLELISM`   | 1       | parallelism of every job of every method (readers, operators, index sinks; one index shard per sink subtask); `CAPS_SOURCE_PARALLELISM` overrides the readers |
| `TM_MEMORY`          | 48g (16g with `SMALL=1` from the top-level `run.sh`; on a machine with less than 56 GB of RAM, the RAM minus 8 GB) | size of the Flink task manager that `install_flink.sh` configures; only read when Flink is installed (an existing installation keeps its `flink-conf.yaml`: edit `taskmanager.memory.process.size` there) |
| `PHASES=plot`        | -       | redraw the figures and tables of an experiment from the results that are already there; no build, no cluster |
| `EXPERIMENTS`        | all     | (top-level `run.sh`) which experiments to run |
| `SKIP_BUILD`         | 0       | 1: the jars are up to date, do not rebuild |
| `FLINK_DIR`          | `code/flink-1.10.0` | an existing Flink 1.10 installation |
| `NUM_QUERIES`        | 1000    | temporal queries per type (how-much and versioning) and window length |
| `LOCK_FILE`          | `/tmp/caps-flink.lock` | the machine lock |

Experiment-specific switches (details at the top of each `run.sh`):

  - `disorder`: `E3_DATAFLOWS` (default: all six, as in the paper), `ARMS`
    (`D delta p skew sink tier1 idle`), `PHASES` (`late measure timing check`; `plot` redraws), `JOB_TIMEOUT_S`.
    The jobs read these variables, which default to the ordered run of the paper: `CAPS_DISORDER_MS` (delay every
    record by a fixed-seed draw from [0, D] ms; the event time stays), `CAPS_DELTA_MS` (maximum out-of-orderness of
    the watermark), `CAPS_IDLE_MS` (idleness timeout of a reader), `CAPS_WM_INTERVAL_MS` (watermark period, default
    1; the disorder and recovery experiments set 50), `CAPS_SLOW_FACTOR` (one reader runs this many times slower),
    `CAPS_ZIPF` (Zipf remapping of the dataflow's key), `CAPS_SINK_PARALLELISM` (1: one global index sink instead of
    one shard per subtask), `CAPS_TIER1` (`hybrid`, the default; `tree` or `buffer`), `CAPS_PACE_SECONDS` (read every reader's file on a schedule of this many seconds;
    the paced arms of `disorder` use it so that the watermark moves in steps commensurable with D), `CAPS_COUNT_LATE` (1: count late records; off in every
    other experiment so that the four methods run the same operators). `PACED=1` selects the paced arms (`D delta`, results in `results_*_paced_howmuch`),
    `CFG_SHARD=i/n` runs every n-th configuration (several instances in parallel, each with its own `LOCAL_CORES` and `LOCK_FILE`).
  - `recovery`: `E4_DATAFLOWS` (default: all six, as in the paper), `PHASES` (`verify timing`),
    `FORCE=1`. Checkpointing is switched on by `CAPS_CHECKPOINT_MS` inside the experiment.
  - `live_memory`: `LIVE_REPS` (repetitions per unit; the event-time numbers must be identical), `FORCE=1`. `heap_check`: `HEAP_REPS`, `HEAP_PERIOD_S`, `HEAP_PACE_S_FULL`.
  - `channel_scaling`: `MODE` (`cluster` or `local`), `M_LIST`, `SWEEP_LIST`, `PHASE_LIST` (`verify time queries`),
    `BUILD=1`, `FORCE=1`, `JMH_WARMUP`, `JMH_MEASURE`.
  - `memory`: `KEEP_PROBES=1`. `verify`: `KEEP_OUTPUTS=1`, `GENEALOG_STRATEGY`. `index_check`: `PARALLELISMS`
    (default `1 2 4`), `WITH_GENEALOG`, `WITH_GREEN`, `WITH_LEGACY`, `SWEEP_QUERIES`.

`SMALL=1` never overwrites the results of a full run. The 8 original experiments (`how_much`, `memory`, `query_table`,
`queries`, `expiry`, `summarize`, `query_length`, `paths`) write everything (per-dataflow directories, combined CSVs,
tables, figures) into the subdirectory `small/` of their directory, with the same layout; `PHASES=plot` and the top-level
summary read it too. The others keep their own: `disorder` and `recovery` use `results_small`/`work_small`,
`live_memory` `results_small_p*`, `verify` and `index_check` the `*_small*` files and directories, `channel_scaling`
`results/small_*` and `work/small_*`.

Verification
-----------------
Correctness is checked in three layers; none of them is timed.

    for m in temporal_index tiered_index caps inkstream genealog query_jmh; do (cd code/$m && mvn test); done
    ./experiments/verify/run.sh                 # SMALL=1 for the subsets; CAPS_PARALLELISM=2 for parallel jobs
    ./experiments/verify/index_check.sh         # SMALL=1; PARALLELISMS="1 2 4"

  - **Unit tests** (195 tests): each library module carries a JUnit suite that the experiments skip
    (`-DskipTests`): that each dataflow attaches exactly the probes its operator set declares, that the shared-identifier
    query fixture holds the same 1,000 outputs for all methods, that a GeneaLog graph survives cloning and its traversal
    answers match the counters, the Tier-1 structures and the two-tier index against a brute-force oracle, the live-state
    peaks against hand-computed values, the disorder and watermark logic, and that the JMH harness keeps the annotations
    that make its numbers meaningful.
  - **`experiments/verify/run.sh`** runs No Prov, CAPS, Green and GeneaLog on every dataflow with their outputs in files and
    checks that the four methods produce the same outputs (values and timestamps), that the per-output how-much counts agree
    (CAPS = Green channel by channel, CAPS = GeneaLog total), that CAPS's sink receives its input in non-decreasing derived
    time, that every output timestamp is a window boundary, and (at p > 1) that CAPS gives the outputs it gives at p = 1.
    It prints PASS or FAIL per check and writes `experiments/verify/verify_<tag>.csv`.
  - **`experiments/verify/index_check.sh`** checks the two-tier index in the Flink sinks: the answers equal the array scan, GeneaLog's
    graph traversal and Green's polynomial projection (channel by channel) and the released slack-buffer sink, are identical at
    p = 1, 2 and 4, no record is late, and the provisional answers are consistent with the final ones. Writes
    `index_check_<tag>.csv` and `index_stats_<tag>.csv`.
  - Every experiment verifies its own results as it goes: the temporal-query experiments compare the answers of Green and
    GeneaLog with CAPS query by query (`verification.csv`), `query_table` ties the JMH fixtures to the capture runs
    (`validate.py`), `expiry` compares the trimmed and the full index (`mismatches = 0`), the channel-scaling sweeps check
    every point against an oracle recomputed from the input before timing it, the recovery runs compare the answers, the
    index digest and the deduplicated output ids with the run without a failure.

Results and where they appear in the paper
-----------------
Each experiment writes one result directory per dataflow and a combined CSV at the experiment level with a leading
`dataflow` column; every figure has a CSV of the plotted numbers with the same file name; the tables the paper reports
are written by `experiments/tables.py` as `*_table.csv` and a LaTeX tabular `*_table.tex`. The table of figures,
commands and outputs is `experiments/README.md`.

There are two different memory numbers and they must not be mixed in one comparison:

  - `how_much/memory_model.csv` is the **analytical** model of Section 7.2 and the computed part of the "retained" column of Table 2: each method is charged the meta-data it must *retain* to answer a query, so CAPS pays
    `#outputs x #channels x 8` at the sink while GeneaLog pays 32 B on every operator output, because it keeps the whole
    contribution graph. `memory_model_detail.csv` splits it into `sink_bytes` (retained) and `intermediate_bytes` /
    `window_state_bytes` (live).
  - `memory/metadata_volume.csv` is the **measured** cross-check: Flink accumulator probes at every operator output that
    carries the method's annotation, summed over the run (cumulative traffic, not retained state).
  - `live_memory/` counts the **peak live** annotation bytes in the window state of all operators at once (instrumented model bytes on an event-time schedule and over wall-clock time, not heap); `heap_check/` looks at the JVM heap itself (forced GCs, all six dataflows).

None of the three is peak process memory (only `heap_check/` reads the JVM heap), and a comparison must take all methods from the same file.

Offline query tool
-----------------
`caps.Main` loads a CAPS sink file into the index after the fact, for inspecting provenance outside a run:

    java -cp code/caps/target/classes caps.Main <sink.out> <channels> \
        query <ts> <te>

`delta`, `bench` and `selftest` are also accepted; `selftest` checks the examples of Figures 4 and 5 of the paper.

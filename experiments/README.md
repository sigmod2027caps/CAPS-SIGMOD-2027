# Experiments: from figure to command

Every figure and table of Section 7 of the paper, with the command that regenerates it, its runtime and its output
files. Paths are relative to the repository root. Setup, datasets and environment switches are in `readme.md`.
`./run.sh` at the root runs all of them in the order below.

## How to read the commands

- **Full run**: the command rebuilds what it needs (Maven), runs every timed job on a fresh standalone Flink 1.10 cluster
  (one job at a time, under `flock -o /tmp/caps-flink.lock`, taken by the script itself), verifies the results, and
  writes the CSVs, tables and figures. Defaults: the six dataflows, `REPS=3` (median in tables, min-max in figures),
  parallelism 1.
- **Plot only** (`PHASES=plot <command>`): no build, no cluster, no input needed. Rebuilds the tables and the figures
  from the results of an earlier run. The repository holds no results: every number comes from your own run.
- **Quick check**: `SMALL=1 REPS=1 <command>` uses the 10% subsets (`data/small`, made by `data/make_small.py`). It
  exercises the same code path in a fraction of the time, but the numbers are not the paper's. It never overwrites the
  results of a full run: the results go to `small/`, `results_small*` or `*_small*` next to the full ones.
- Runtimes are wall-clock on the reference machine (Intel i7-14700K, 28 threads, 128 GB RAM, 48 GB TaskManager), full
  data, `REPS=3`, excluding the one-time setup (Flink download, taxi download, first Maven build).

## Paper figures and tables

| Paper item | Command (full run) | Runtime | Output files |
|---|---|---|---|
| Figure 7: the six dataflows | static drawing, no script | - | - |
| Figure 8: runtime of the four methods on the six dataflows | `experiments/how_much/run.sh` | 40 min | `experiments/how_much/how_much_runtime_norm.{pdf,csv}`, `runtime_table.tex`, `times.csv` |
| Table 2: retained meta-data (analytical model) | same run as Figure 8 | (same run) | `experiments/how_much/memory_model.csv`, `memory_model_detail.csv` |
| Cross-check (not in the paper): meta-data volume measured by probes | `experiments/memory/run.sh` | 15 min | `experiments/memory/metadata_volume.csv`, `memory_table.{csv,tex}` |
| Table 2: peak live annotation state | `experiments/live_memory/run.sh` (paper: `LIVE_REPS=2`) | 30 min | `experiments/live_memory/results_p1/live_memory_table.{csv,tex}` |
| Table 2: retained memory measured (peak JVM heap above No Prov) | `experiments/heap_check/run.sh` | about 1.5 h | `experiments/heap_check/results_index/heap_check.csv`, `heap_check_bar.pdf` |
| Figure 9a: per-output query latency (JMH and in situ) | `experiments/query_table/run.sh` (in-situ times from `how_much`) | 30 min | `experiments/query_table/traversal.{pdf,csv}`, `query_table_summary.csv`, `query_table.tex` |
| Figure 9b: latency against the query window length | `experiments/query_length/run.sh` | 3 h 10 min | `experiments/query_length/query_length_latency_lines.{pdf,csv}` |
| Figure 9c: counters per source vs per path | `experiments/paths/run.sh` (taxi_1, twitter_1) | 10 min | `experiments/paths/paths_cost_bar.{pdf,csv}` |
| Figure 10: temporal how-much query latency | `experiments/queries/run.sh` | 31 min | `experiments/queries/queries_howmuch_bar.{pdf,csv}`, `query_latency.csv`, `verification.csv` |
| Section 7.4: insertion latency, index size, cost of the online index | same run as Figure 10; `experiments/index_overhead/run.sh` | 40 min | `experiments/queries/insert_latency.csv`, `index_stats.csv`; `experiments/index_overhead/index_overhead.csv`, `index_overhead_runtime_bar.pdf` |
| Section 7.5: branch test (m real branches) | `experiments/channel_scaling/run.sh` | 2 h 50 min | `experiments/channel_scaling/results/full_p1_cluster/*.pdf`, `crossover.csv` |
| Section 7.5: nested paths (three levels of 8-way multiplexes, 512 paths, recombined by a join) | `experiments/nested_paths/run.sh` | about 30 min | `experiments/nested_paths/results/nested.{csv,pdf}` |
| Figure 11: meta-data and per-output query latency against the number of channels m (taxi) | `experiments/channel_scaling_real/run.sh` | about 10 h (m = 512 alone about 3 h per input) | `experiments/channel_scaling_real/results/channel_{pertaxi,perzone}_{mem,lat}.pdf`, `channel_legend.pdf`, `memory.csv`, `query.csv` |
| Table 3: disorder, skew, parallelism, late data | `experiments/disorder/run.sh` | several hours | `experiments/disorder/results_full/tables.md`, `e3_*.csv`, `e3_*.pdf` |
| Table 3: delta below, at and above D (paced sources) | `PACED=1 ARMS="D delta" experiments/disorder/run.sh` | several hours (can be split over parallel instances, `CFG_SHARD=i/n`) | `experiments/disorder/results_full_paced_howmuch/` |
| Section 7.7: recovery | `experiments/recovery/run.sh` (p = 1, and p = 4 with D = delta = 60 s) | about 1 h | `experiments/recovery/results_full_howmuch{,_p4_D60000_d60000}/recovery_{verify,runs,summary}.csv`, `recovery_overhead.pdf` |
| Table 4: expiry (7-day horizon) | `experiments/expiry/run.sh` | 7 min | `experiments/expiry/expiry_table.{csv,tex}`, `expiry_stats.csv`, `expiry_memory.csv` |
| Table 4: summarization | `experiments/summarize/run.sh` | 7 min | `experiments/summarize/summarize_table.{csv,tex}`, `summarize.csv` |
| Section 4.3: largest per-channel count | `experiments/max_count/run.sh` | 3 min | `experiments/max_count/max_count_p1.{csv,pdf}` |

## Where each column of Tables 2-4 comes from

| Table, column | File (after a full run) | Column |
|---|---|---|
| Names in the paper | NX1, NX2, TX1, TX2, TW1, TW2 = `nexmark_1`, `nexmark_2`, `taxi_1`, `taxi_2`, `twitter_1`, `twitter_2`; Green = `green_polynomial` / `ink`; CAPS (no index) = `array` / `caps_scan`; m = `M` | - |
| Table 2, retained, computed | `how_much/memory_model.csv` | `bytes` |
| Table 2, retained, measured | `heap_check/results_index/heap_check.csv` | `peak_above_noprov_mb` |
| Table 2, peak live, in timestamp order | `live_memory/results_p1/live_memory_table.csv` | `<method>_live_MB` |
| Table 2, peak live, actual run | `heap_check/results_index/heap_check.csv` | `live_wall_mb` |
| Table 3, wrong answers (delta = 0) | `disorder/results_full_paced_howmuch/e3_answers_delta.csv` | `pct_median` (rows `delta_ms` = 0) |
| Table 3, waiting outputs, peak and bound | `disorder/results_full/tables.md`, "Tier-1 occupancy and its bound" (`e3_occupancy.csv`, `e3_occupancy_global.csv`) | per sink / global |
| Table 3, cost per output | `disorder/results_full/e3_tier1_cost_shards.csv` | `value` |
| Table 3, final after | `disorder/results_full/tables.md`, "Delay to finality" (`e3_finality.csv`) | 99th percentile, local / global |
| Table 3, CAPS / No Prov | `disorder/results_full/e3_overhead.csv` | `ratio` |
| Table 4, expiry | `expiry/expiry_table.csv` | all columns |
| Table 4, summarization error | `summarize/summarize_table.csv` | `avg_error_pct` |

## Correctness checks (not timed, no figures)

| Check | Command | Runtime | Output files |
|---|---|---|---|
| The four methods produce the same outputs and how-much counts | `experiments/verify/run.sh` | 14 min | `experiments/verify/verify_<tag>.csv` |
| The two-tier index answers like the array scan, GeneaLog and Green, at p = 1, 2, 4 | `experiments/verify/index_check.sh` | 14 min (p = 1) | `experiments/verify/index_check_<tag>.csv`, `index_stats_<tag>.csv` |
| Unit tests of the libraries | `for m in temporal_index tiered_index caps inkstream genealog query_jmh; do (cd code/$m && mvn test); done` | 5 min | console |

## Everything at once

| Command | What it does | Runtime |
|---|---|---|
| `./run.sh` | setup, the checks, then every experiment above: the numbers of the paper | about 30 h |
| `SMALL=1 REPS=1 ./run.sh` | the quick check: 10% subsets, one repetition, shortened sweeps | 3.5 h (28 threads) to 6.5 h (8-core Mac, Docker) |
| `PHASES=plot ./run.sh` | after a run: redraw every figure and table from its results | about a minute |
| `EXPERIMENTS="queries expiry" ./run.sh` | a subset of the experiments | - |

## Conventions of the outputs

- One colour, hatch and display name per method in every plot (`experiments/common.py`); order: No Prov, GeneaLog, Green,
  CAPS; fonts embedded in the PDFs.
- Every figure has a CSV of the plotted numbers with the same stem; every table has a CSV and a LaTeX tabular.
- Medians and min-max of `REPS` repetitions; JMH rows are the score and its 99.9% confidence interval over iterations.
- Every `run.sh` lists its dataflows one per line at the top (comment lines out); the environment variable `DATAFLOWS`
  (space-separated) overrides the list, and the top-level `run.sh` passes its own list on to the experiments.

#!/usr/bin/env bash
# Per source vs per path how-much provenance (paper Figure 9c). The two
# dataflows in PATH_CHANNELS split each source into several processing paths,
# so CAPS can keep one counter per source or one per path. Each is run twice
# with the online indexing sink, once per granularity, showing that the capture
# cost does not depend on the number of counters while path-level queries
# become possible.
set -euo pipefail

# --- parameters (comment in or out) ---
DATAFLOW_LIST=( # only the two dataflows that split a source into paths take part
  taxi_1
  twitter_1
)
# DATAFLOWS="taxi_1"   # environment: a space-separated subset, overrides the list above
# REPS=3        # repetitions of every granularity, each on a fresh cluster (default 3): median and min-max
# SMALL=1       # the 10% subsets (a smoke test, not numbers); the results go to small/ and never overwrite the full ones

EXPDIR="$(cd "$(dirname "$0")" && pwd)"
if [[ -n "${DATAFLOWS:-}" ]]; then DATAFLOW_LIST=($DATAFLOWS); fi
DATAFLOWS="${DATAFLOW_LIST[*]}"
source "$(cd "$(dirname "$0")/.." && pwd)/common.sh"
RESDIR="$(results_dir "$EXPDIR")"   # SMALL=1 writes into small/ and never overwrites the full-data results
mkdir -p "$RESDIR"

# The insertion latency per record (insert_avg_ns, Figure 9c) needs the nanoTime brackets around the Tier-1 insert and split. They
# are off by default in every timed run (2 clock reads per record), so they are switched on explicitly for this experiment, as in
# experiments/queries: both granularities carry them equally (the submitted artifact timed every insert in all of its runs).
export CAPS_INSERT_TIMING=1

# Only the dataflows with more paths than sources take part.
PATHS_DATAFLOWS=""
for dataflow in $DATAFLOWS; do
  [[ -n "${PATH_CHANNELS[$dataflow]:-}" ]] && PATHS_DATAFLOWS="$PATHS_DATAFLOWS $dataflow"
done
if [[ -z $PATHS_DATAFLOWS ]]; then
  echo "Skipping: none of the selected dataflows refines sources into paths."
  echo "  this experiment runs on: ${!PATH_CHANNELS[*]}"
  exit 0
fi
DATAFLOWS="$PATHS_DATAFLOWS"

# PHASES=plot: aggregate the repetitions that are already there and redraw the figure.
if plot_only; then
  for dataflow in $DATAFLOWS; do
    "$PYTHON" "$EXPDIR/../aggregate_reps.py" --dir "$RESDIR/$dataflow" --file paths.csv --keys granularity \
      --spread runtime_s,job_duration_s,insert_avg_ns,howmuch_avg_us,versioning_avg_us,index_bytes
  done
  combine_results "$RESDIR" paths.csv
  plot_experiment "$RESDIR"
  exit 0
fi

require_flink
for dataflow in $DATAFLOWS; do require_inputs "$dataflow"; done
build_all

# runtime_s is the wall clock of `flink run` (client start included, as in the submitted paper), job_duration_s
# the duration the JobManager reports; both include the end-of-stream benchmark of the sink.
HEADER="granularity,channels,records,runtime_s,job_duration_s,insert_avg_ns,howmuch_avg_us,versioning_avg_us,index_bytes"

# One run of one granularity: a fresh cluster, the CAPS index job, the REST job JSON read while the cluster is up.
paths_unit() {
  local dataflow=$1 granularity=$2 rundir=$3 t0
  t0=$(date +%s.%N)
  CAPS_GRANULARITY="$granularity" \
    run_caps "$dataflow" queries "$rundir" "$NUM_QUERIES" "${QUERY_LEN_MS[$dataflow]}" \
    > "$rundir/flink.log" 2>&1
  PATHS_WALL=$(awk -v s="$t0" -v e="$(date +%s.%N)" 'BEGIN{printf "%.3f",e-s}')
  capture_job_json "$rundir/flink.log" "$rundir/job.json"
}

for dataflow in $DATAFLOWS; do
  echo -e "\n=== $dataflow path counters, REPS=$REPS ==="
  outdir="$RESDIR/$dataflow"
  rm -rf "$outdir"
  read -r source_channels path_channels <<<"${PATH_CHANNELS[$dataflow]}"

  for ((rep=1; rep<=REPS; rep++)); do
    repdir="$outdir/rep$rep"
    mkdir -p "$repdir"
    echo "$HEADER" > "$repdir/paths.csv"
    for granularity in source path; do
      if [[ $granularity == source ]]; then channels=$source_channels; else channels=$path_channels; fi
      rundir="$repdir/$granularity"
      mkdir -p "$rundir"
      echo "  [rep $rep/$REPS, $granularity granularity, $channels counters]"
      with_cluster paths_unit "$dataflow" "$granularity" "$rundir"
      job=$(job_duration "$rundir/job.json")
      records=$(awk -F, 'NR==2{print $1}' "$rundir/index_stats.csv")
      bytes=$(awk -F, 'NR==2{print $2}' "$rundir/index_stats.csv")
      insert=$(awk -F, '$1=="prefix"{print $3}' "$rundir/insert_latency.csv")
      howmuch=$(awk -F, '$1=="prefix" && $2=="howmuch"{print $5}' "$rundir/query_latency.csv")
      versioning=$(awk -F, '$1=="prefix" && $2=="versioning"{print $5}' "$rundir/query_latency.csv")
      echo "$granularity,$channels,$records,$PATHS_WALL,$job,$insert,$howmuch,$versioning,$bytes" >> "$repdir/paths.csv"
      echo "    runtime ${PATHS_WALL}s (job ${job}s), insert ${insert}ns, query ${howmuch}us, index ${bytes}B"
    done
  done
  "$PYTHON" "$EXPDIR/../aggregate_reps.py" --dir "$outdir" --file paths.csv --keys granularity \
    --spread runtime_s,job_duration_s,insert_avg_ns,howmuch_avg_us,versioning_avg_us,index_bytes
done

combine_results "$RESDIR" paths.csv
echo -e "\npaths.csv:"; cat "$RESDIR/paths.csv"
echo -e "\n[figures]"
plot_experiment "$RESDIR"

echo -e "\nDone. Outputs in $RESDIR/"

#!/usr/bin/env bash
# Cost of maintaining the two-tier index online: the end-to-end runtime of the CAPS job with
#   capture   provenance capture only, the per-output coin of how_much (no index)
#   tree      capture + the online two-tier index, Tier 1 = the augmented red-black tree for every record
#   hybrid    capture + the online two-tier index, Tier 1 = ordered run (sorted array with prefix sums) for in-order
#             records, red-black tree only for out-of-order ones (the default)
#   hybrid_timed  as hybrid, with the nanoTime brackets around every Tier-1 insert and split switched on
#             (CAPS_INSERT_TIMING=1; they are off in every other timed run)
# against No Prov, on the ordered input of the default configuration (parallelism 1, periodic watermark of 1 ms, one
# index shard, no provisional queries and no scan baseline, as in the timing runs of the other experiments). Every unit is
# a fresh cluster under the machine lock; REPS repetitions, interleaved. Two No Prov references: noprov (with the per-output
# sampling coin, as in how_much: the reference of the capture arm) and noprov_nocoin (no coin: the index sinks of
# queries mode do not flip it, so this is the reference of the three index arms). The sink runtime that the experiment reports is the
# job duration of the Flink REST API.
set -euo pipefail

# --- parameters (comment in or out) ---
DATAFLOW_LIST=(
  nexmark_1
  nexmark_2
  taxi_1
  taxi_2
  twitter_1
  twitter_2
)
# DATAFLOWS="nexmark_1 taxi_2"   # environment: a space-separated subset, overrides the list above
ARMS=(          # the order of the figure
  noprov
  noprov_nocoin
  capture
  tree
  hybrid_timed
  hybrid
)
# REPS=3        # repetitions of every arm (default 3): median reported, min-max in the figure
# SMALL=1       # the 10% subsets (a smoke test, not numbers); the results go to small/
# CAPS_PARALLELISM=1

EXPDIR="$(cd "$(dirname "$0")" && pwd)"
if [[ -n "${DATAFLOWS:-}" ]]; then DATAFLOW_LIST=($DATAFLOWS); fi
DATAFLOWS="${DATAFLOW_LIST[*]}"
source "$(cd "$(dirname "$0")/.." && pwd)/common.sh"
RESDIR="$(results_dir "$EXPDIR")"
mkdir -p "$RESDIR"

if plot_only; then
  combine_results "$RESDIR" reps.csv
  "$PYTHON" "$EXPDIR/plot_index_overhead.py" "$RESDIR"
  exit 0
fi

require_flink
for dataflow in $DATAFLOWS; do require_inputs "$dataflow"; done
build_all

run_arm() {
  local arm=$1 dataflow=$2 dir=$3
  case $arm in
    noprov)
      "$FLINK" run --parallelism "$CAPS_PARALLELISM" --class "${NOPROV_CLASS[$dataflow]}" \
        "$ROOT/code/noprov/$dataflow/target/${JAR[$dataflow]}" $(dataflow_inputs "$dataflow") \
        none "${SAMPLE_PROB[$dataflow]}" ;;
    noprov_nocoin)
      "$FLINK" run --parallelism "$CAPS_PARALLELISM" --class "${NOPROV_CLASS[$dataflow]}" \
        "$ROOT/code/noprov/$dataflow/target/${JAR[$dataflow]}" $(dataflow_inputs "$dataflow") \
        none ;;
    capture)
      run_caps "$dataflow" provenance "$dir" "${SAMPLE_PROB[$dataflow]}" ;;
    tree|hybrid|hybrid_timed)
      local tier1=hybrid timing=0
      [[ $arm == tree ]] && tier1=tree
      [[ $arm == hybrid_timed ]] && timing=1
      CAPS_TIER1=$tier1 CAPS_INSERT_TIMING=$timing CAPS_PROVISIONAL=0 CAPS_NO_SCAN=1 \
        run_caps "$dataflow" queries "$dir" 0 "${QUERY_LEN_MS[$dataflow]}" ;;
    *) echo "unknown arm $arm" >&2; return 1 ;;
  esac
}

timed_unit() {
  local log=$1 job_json=$2
  shift 2
  "$@" > "$log" 2>&1
  capture_job_json "$log" "$job_json"
}

for dataflow in $DATAFLOWS; do
  echo -e "\n=== $dataflow  REPS=$REPS ==="
  outdir="$RESDIR/$dataflow"
  rm -rf "$outdir"; mkdir -p "$outdir"
  echo "arm,rep,job_duration_s" > "$outdir/reps.csv"
  for ((rep=1; rep<=REPS; rep++)); do
    for arm in "${ARMS[@]}"; do
      echo "  $arm rep $rep/$REPS"
      rundir="$outdir/${arm}_rep$rep"; mkdir -p "$rundir"
      with_cluster timed_unit "$rundir/flink.log" "$rundir/job.json" run_arm "$arm" "$dataflow" "$rundir"
      duration=$(job_duration "$rundir/job.json") || { echo "no job duration: $rundir" >&2; exit 1; }
      echo "$arm,$rep,$duration" >> "$outdir/reps.csv"
      rm -f "$rundir"/*.ser "$rundir"/query_answers*.csv   # the raw outputs are not needed
    done
  done
done

combine_results "$RESDIR" reps.csv
"$PYTHON" "$EXPDIR/plot_index_overhead.py" "$RESDIR"
echo -e "\nDone. Outputs in $RESDIR/"

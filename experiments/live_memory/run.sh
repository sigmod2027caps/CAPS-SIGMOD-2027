#!/usr/bin/env bash
# Live provenance meta-data: the PEAK annotation bytes that the window
# operators hold at a time, COUNTED in the running job from each method's byte model (instrumented model bytes, NOT
# measured heap: the job evaluates "8 B per long / variable, 16 / 32 B per GeneaLog node" whenever a window pane is created
# or purged), for GeneaLog, Green and CAPS (No Prov holds none), next to the RETAINED meta-data of Table 2 (analytical, same
# model for the three methods). The peak is reported twice: on an event-time schedule (a constructed, lock-step order of the
# same events, a function of the data) and over wall-clock time of the replay (what this run actually held; run dependent).
# The JVM heap itself is looked at by experiments/heap_check.
#
# Definitions:
#   retained   what the method must keep to answer queries about the outputs (analytical, the retained column of Table 2):
#                CAPS      N * M * 8 B           N sink outputs, M channels
#                Green     sum over the sink outputs of 8 B per variable of the output's monomial
#                GeneaLog  32 B (uid, U1, U2, next) per tuple that an operator emitted (Flink's write-records of the job
#                          vertices plus the sink outputs); its graph is kept alive by its pointers
#   live peak  max over (event) time of the annotation bytes in the window state of ALL operators at once (counted at run time):
#                aggregate  one accumulator per (key, window pane)   CAPS M*8 B, GeneaLog core 16 B (two pointers),
#                           Green 8 B per variable the accumulator holds (grows with the contributors)
#                join       every buffered tuple copy, one per pane  CAPS M*8 B, Green 8 B per variable, GeneaLog core 32 B
#              Records in flight (network buffers, operator chains) are transient and not counted; stateless operators hold none.
#              GeneaLog is also reported "+ pinned": adds the tuples its aggregate chains keep alive and the provenance sets
#              its tuples carry after a network exchange (32 B per source tuple of the set).
#   GeneaLog graph  nodes reachable from the sink outputs x 32 B (the node count is measured by a traversal in the sink; the 32 B are
#                   the model's constant): what a query needs. GeneaLog's retained number in Table 2 is a model; this is the measured one
# The live peak is the maximum over event time (sources in lock step, a function of the data only: identical in repeated
# runs); the maximum over wall time of the real interleaving (peak_live_wall_bytes) depends on how far the threads run apart.
# Both are reported (live_memory_peak_bar.pdf, live_memory_peak_wall_bar.pdf).
#
# Per (dataflow, method) one unit: a fresh cluster, the job (discard sink), the REST job JSON, all under
# `flock -o /tmp/caps-flink.lock`. Each unit checks that the live total returns to 0 and that the sink saw the
# expected number of outputs (the same for the three methods).
#
#   SMALL=1 experiments/live_memory/run.sh      the 10% subsets (a smoke test)
#   experiments/live_memory/run.sh              full data
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
METHODS=(
  genealog
  green_polynomial
  caps
)
# SMALL=1                # the 10% subsets (a smoke test, not numbers)
# CAPS_PARALLELISM=1     # parallelism of every job (default 1; the numbers of the paper are p = 1)
# LIVE_REPS=1            # repetitions of every unit (default 1); with 2 or more the logical numbers must be identical
# SKIP_BUILD=1           # the jars are up to date
# FORCE=1                # run finished units again

EXPDIR="$(cd "$(dirname "$0")" && pwd)"
if [[ -n "${DATAFLOWS:-}" ]]; then DATAFLOW_LIST=($DATAFLOWS); fi
DATAFLOWS="${DATAFLOW_LIST[*]}"
source "$(cd "$(dirname "$0")/.." && pwd)/common.sh"
LIVE_REPS="${LIVE_REPS:-1}"

if ! plot_only; then
  require_flink
  for dataflow in $DATAFLOWS; do require_inputs "$dataflow"; done
  build_all --with-competitors
fi

TAG="p${CAPS_PARALLELISM}"
[[ $SMALL == 1 ]] && TAG="small_$TAG"
OUT="$EXPDIR/results_$TAG"
mkdir -p "$OUT"

# Instrument the window operators of the three methods (LiveState in temporal_index).
export CAPS_LIVE_STATE=1

# run_live <method> <dataflow> <dir>: the job, in live_memory mode; the client writes <dir>/live_state.csv.
run_live() {
  local method=$1 dataflow=$2 dir=$3
  case $method in
    caps)             run_caps "$dataflow" live_memory "$dir" ;;
    green_polynomial) run_green "$dataflow" live_memory "$dir" ;;
    genealog)         run_genealog "$dataflow" "$dir" sortedPtr --metadataVolumeDir "$dir" ;;
    *) echo "unknown method: $method" >&2; return 1 ;;
  esac
}

# unit <method> <dataflow> <dir>: the cluster is up; runs the job and keeps the REST job JSON.
unit() {
  local method=$1 dataflow=$2 dir=$3
  run_live "$method" "$dataflow" "$dir" > "$dir/flink.log" 2>&1
  capture_job_json "$dir/flink.log" "$dir/job.json"
}

# PHASES=plot: the per-run results of an earlier run are in results_<tag>/; only the summary and the figures are redone.
RUN_DATAFLOWS=$DATAFLOWS; plot_only && RUN_DATAFLOWS=""
for dataflow in $RUN_DATAFLOWS; do
  echo -e "\n=== $dataflow  small=$SMALL  parallelism=$CAPS_PARALLELISM ==="
  for method in "${METHODS[@]}"; do
    for ((rep = 1; rep <= LIVE_REPS; rep++)); do
      dir="$OUT/$dataflow/$method/rep$rep"
      # a rerun skips the units that finished (.done marker); FORCE=1 redoes them
      if [[ ${FORCE:-0} != 1 && -e "$dir/.done" ]]; then
        echo "  $method rep$rep: done"
        continue
      fi
      rm -rf "$dir"; mkdir -p "$dir"
      t0=$(date +%s)
      with_cluster unit "$method" "$dataflow" "$dir" || {
        echo "  $method FAILED, see $dir/flink.log" >&2; exit 1; }
      touch "$dir/.done"
      echo "  $method rep$rep: $(( $(date +%s) - t0 )) s"
    done
  done
done

# GeneaLog's retained bytes come from the job vertices' write-records (analytical model of how_much).
for dataflow in $DATAFLOWS; do
  for dir in "$OUT/$dataflow"/genealog/rep*; do
    [[ -s "$dir/job.json" ]] || continue
    outputs=$(awk -F, '$3=="sink_records" {print $4}' "$dir/live_state.csv")
    genealog_bytes "$dir/job.json" "$outputs" > "$dir/retained_bytes.txt"
  done
done

# Verification, CSV, table and figure.
expected_outputs() {
  local d
  for d in $DATAFLOWS; do
    if [[ $SMALL == 1 ]]; then echo "$d ${SMALL_OUTPUTS[$d]}"; else echo "$d ${FULL_OUTPUTS[$d]}"; fi
  done
}
expected_outputs > "$OUT/expected_outputs.txt"
"$PYTHON" "$EXPDIR/summarize.py" "$OUT" --reps "$LIVE_REPS"
"$PYTHON" "$EXPDIR/plot_live_memory.py" "$OUT"
echo -e "\nDone. Outputs in $OUT/"

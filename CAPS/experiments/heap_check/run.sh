#!/usr/bin/env bash
# Heap sanity check of the live-memory numbers (revision ).
#
# experiments/live_memory counts annotation bytes per window pane from a model (peak on an event-time schedule, and the
# wall-time peak of the replay), and experiments/memory counts the retained bytes analytically. This experiment looks at
# the real heap of the TaskManager JVM instead: a sampler (heap_sampler.py) forces a full GC (jcmd GC.run) every
# HEAP_PERIOD_S seconds while the job runs and records the heap that is still in use afterwards (jstat -gc). The peak of
# those samples above the idle JVM is what the job keeps alive at its largest, whatever the reason: window state, buffered
# records, the output graphs that the GeneaLog sink retains, operator objects, serializers. It is NOT the annotation alone,
# so it is compared with the computed numbers, not subtracted from them.
#
# The jobs are those of experiments/how_much (the timing configuration: sampling coin, no instrumentation, the GeneaLog sink
# retains its output graphs as in the submitted artifact), with the readers paced to HEAP_PACE_S seconds (CAPS_PACE_SECONDS) so
# that the job lasts long enough for the sampler to take many samples (a full-speed replay lasts 1-40 s and jcmd needs about
# 0.5 s per sample) and so that the heap follows the stream like a live one. Every unit is a fresh cluster, the job and the
# sampler, under `flock -o /tmp/caps-flink.lock`. The forced GCs slow the run down: this experiment says nothing about runtime.
#
#   SMALL=1 experiments/heap_check/run.sh     the 10% subsets (smoke test)
#   experiments/heap_check/run.sh             full data, p = 1
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
METHODS=(       # chronological order, ours last
  noprov
  genealog
  green_polynomial
  caps
)
# HEAP_METHODS="genealog"   # environment: a space-separated subset, overrides the list above
# HEAP_OVERHEAD=9           # the sampler waits this many times the duration of a forced GC between two samples (3: more samples, slower job)
HEAP_PERIOD_S="${HEAP_PERIOD_S:-1}"   # seconds between two forced GCs
HEAP_IDLE_S="${HEAP_IDLE_S:-4}"       # seconds of the idle JVM sampled before the job (the baseline)
HEAP_PACE_S_FULL="${HEAP_PACE_S_FULL:-60}"   # seconds in which a reader reads its whole file (full data / SMALL)
HEAP_PACE_S_SMALL="${HEAP_PACE_S_SMALL:-15}"
SINK="${HEAP_SINK:-index}"          # index: the sinks build the index of each method (the temporal-query sinks, no queries,
                                      #   CAPS without the scan baseline), so the heap at the end of the stream is the measured
                                      #   retained memory; results in results_index/ (retained memory was only computed)
# SINK=provenance                     # the capture-only sinks of experiments/how_much (no index); results in this directory
REPS="${HEAP_REPS:-3}"                # repetitions of every unit (default 3; the median is reported)
# SMALL=1       # the 10% subsets (a smoke test)

EXPDIR="$(cd "$(dirname "$0")" && pwd)"
if [[ -n "${DATAFLOWS:-}" ]]; then DATAFLOW_LIST=($DATAFLOWS); fi
if [[ -n "${HEAP_METHODS:-}" ]]; then METHODS=($HEAP_METHODS); fi
DATAFLOWS="${DATAFLOW_LIST[*]}"
source "$(cd "$(dirname "$0")/.." && pwd)/common.sh"
if [[ $SINK == index ]]; then RESDIR="$(results_dir "$EXPDIR/results_index")"; else RESDIR="$(results_dir "$EXPDIR")"; fi
mkdir -p "$RESDIR"

if plot_only; then
  "$PYTHON" "$EXPDIR/summarize_heap.py" "$RESDIR"
  exit 0
fi
require_flink
for dataflow in $DATAFLOWS; do require_inputs "$dataflow"; done
build_all --with-competitors

# run_job <method> <dataflow> <dir>: the how_much job of the method (SINK=provenance) or the job whose sink builds the
# method's index and answers no query (SINK=index)
run_job() {
  local method=$1 dataflow=$2 dir=$3
  if [[ $SINK == index ]]; then
    case $method in
      noprov)
        "$FLINK" run --parallelism "$CAPS_PARALLELISM" --class "${NOPROV_CLASS[$dataflow]}" \
          "$ROOT/code/noprov/$dataflow/target/${JAR[$dataflow]}" $(dataflow_inputs "$dataflow") none "${SAMPLE_PROB[$dataflow]}" ;;
      genealog)
        run_genealog "$dataflow" "$dir" list --indexBenchDir "$dir" --indexNumQueries 0 \
          --indexQueryLenMs "${QUERY_LEN_MS[$dataflow]}" ;;
      green_polynomial)
        run_green "$dataflow" queries "$dir" 0 "${QUERY_LEN_MS[$dataflow]}" ;;
      caps)
        CAPS_NO_SCAN=1 CAPS_PROVISIONAL=0 run_caps "$dataflow" queries "$dir" 0 "${QUERY_LEN_MS[$dataflow]}" ;;
      *) echo "unknown method: $method" >&2; return 1 ;;
    esac
    return
  fi
  case $method in
    noprov)
      "$FLINK" run --parallelism "$CAPS_PARALLELISM" --class "${NOPROV_CLASS[$dataflow]}" \
        "$ROOT/code/noprov/$dataflow/target/${JAR[$dataflow]}" $(dataflow_inputs "$dataflow") none "${SAMPLE_PROB[$dataflow]}" ;;
    genealog)
      run_genealog "$dataflow" "$dir" sortedPtr --traversalBenchDir "$dir" --traversalSampleProb "${SAMPLE_PROB[$dataflow]}" ;;
    green_polynomial)
      run_green "$dataflow" provenance "$dir" "${SAMPLE_PROB[$dataflow]}" ;;
    caps)
      run_caps "$dataflow" provenance "$dir" "${SAMPLE_PROB[$dataflow]}" ;;
    *) echo "unknown method: $method" >&2; return 1 ;;
  esac
}

# unit <method> <dataflow> <dir>: the cluster is up; the sampler starts (idle baseline), the job runs, the sampler stops
unit() {
  local method=$1 dataflow=$2 dir=$3 sampler
  HEAP_OVERHEAD="${HEAP_OVERHEAD:-9}" "$PYTHON" "$EXPDIR/heap_sampler.py" "$dir/heap_samples.csv" "$HEAP_PERIOD_S" &
  sampler=$!
  sleep "$HEAP_IDLE_S"
  local t0; t0=$(date +%s.%N)
  if [[ $SMALL == 1 ]]; then export CAPS_PACE_SECONDS=$HEAP_PACE_S_SMALL; else export CAPS_PACE_SECONDS=$HEAP_PACE_S_FULL; fi
  run_job "$method" "$dataflow" "$dir" > "$dir/flink.log" 2>&1 || { kill "$sampler" 2>/dev/null || true; return 1; }
  echo "job_start_epoch_s=$t0
job_end_epoch_s=$(date +%s.%N)" > "$dir/times.txt"
  kill "$sampler" 2>/dev/null || true
  wait "$sampler" 2>/dev/null || true
  capture_job_json "$dir/flink.log" "$dir/job.json"
}

for dataflow in $DATAFLOWS; do
  echo -e "\n=== $dataflow  small=$SMALL ==="
  for method in "${METHODS[@]}"; do
    for ((rep = 1; rep <= REPS; rep++)); do
      dir="$RESDIR/$dataflow/$method/rep$rep"
      # a rerun skips the units that finished (.done marker); FORCE=1 redoes them
      if [[ -e "$dir/.done" && ${FORCE:-0} != 1 ]]; then echo "  $method rep$rep: done"; continue; fi
      rm -rf "$dir"; mkdir -p "$dir"
      t0=$(date +%s)
      with_cluster unit "$method" "$dataflow" "$dir" || { echo "  $method FAILED, see $dir/flink.log" >&2; exit 1; }
      touch "$dir/.done"
      echo "  $method rep$rep: $(( $(date +%s) - t0 )) s, $(( $(wc -l < "$dir/heap_samples.csv") - 1 )) samples"
    done
  done
done

"$PYTHON" "$EXPDIR/summarize_heap.py" "$RESDIR"
echo -e "\nDone. Outputs in $RESDIR/"

#!/usr/bin/env bash
# One Flink job of the disorder experiment (called by run.sh under `flock -o $LOCK_FILE`): on a standalone cluster
# it starts the cluster, runs the job and stops the cluster again, all inside the one lock; with the in-process
# stand-in (inprocess/bin/flink, a directory without lib/) there is no cluster. The job's log is kept and the net
# runtime it reports (CAPS_NET_RUNTIME_MS, from JobExecutionResult).
#   run_one.sh <kind> <dataflow> <dir>
#     meas         CAPS, two-tier index, traces, NUM_QUERIES how-much queries (+ versioning ones only with CAPS_NO_VERSIONING=0) (answers, occupancy,
#                  finality, provisional and final query latency); in-process also Flink's operator metrics
#                  (records in, late records dropped, records that missed a window) through a metric reporter
#     time_caps    CAPS, two-tier index only (no scan baseline, no provisional queries, no traces, no queries)
#     time_noprov  the no-provenance dataflow, sink with the sampling coin as in experiments/how_much
#     file_caps    CAPS, every output with its counters to <dir>/caps.out (output equality, SMALL only)
# The CAPS_* configuration of the run (see code/temporal_index/.../DisorderConfig.java) comes in the environment.
set -euo pipefail
source "$(cd "$(dirname "$0")/.." && pwd)/common.sh"

kind=$1; dataflow=$2; dir=$3
mkdir -p "$dir"
len=${QUERY_LEN_MS[$dataflow]}
RUN_FLINK_DIR="${RUN_FLINK_DIR:-$FLINK_DIR}"
export REAL_FLINK="$RUN_FLINK_DIR/bin/flink"
FLINK="$(cd "$(dirname "$0")" && pwd)/flink_timeout.sh"   # JOB_TIMEOUT_S (default 900 s) per job

if [[ -d "$RUN_FLINK_DIR/lib" ]]; then # a real installation: one cluster per job, stopped again inside the lock
  FLINK_DIR="$RUN_FLINK_DIR"
  "$FLINK_DIR/bin/stop-cluster.sh" > /dev/null 2>&1 || true
  sleep 3
  "$FLINK_DIR/bin/start-cluster.sh" > "$dir/cluster.log" 2>&1
  trap '"$FLINK_DIR/bin/stop-cluster.sh" >> "$dir/cluster.log" 2>&1 || true' EXIT
  sleep 8
  metrics=""
else
  metrics="$dir/ops_metrics.csv"
fi

set +e
case $kind in
  meas)
    CAPS_TRACE=1 CAPS_TRACE_DIR="$dir" CAPS_METRICS_FILE="$metrics" \
      run_caps "$dataflow" queries "$dir" "$NUM_QUERIES" "$len" > "$dir/flink.log" 2>&1 ;;
  time_caps)
    CAPS_PROVISIONAL=0 CAPS_NO_SCAN=1 \
      run_caps "$dataflow" queries "$dir" 0 "$len" > "$dir/flink.log" 2>&1 ;;
  time_noprov)
    "$FLINK" run --parallelism "$CAPS_PARALLELISM" --class "${NOPROV_CLASS[$dataflow]}" \
      "$ROOT/code/noprov/$dataflow/target/${JAR[$dataflow]}" $(dataflow_inputs "$dataflow") \
      none "${SAMPLE_PROB[$dataflow]}" > "$dir/flink.log" 2>&1 ;;
  file_caps)
    CAPS_TRACE_DIR="$dir" run_caps_file "$dataflow" "$dir/caps.out" > "$dir/flink.log" 2>&1 ;;
  *) echo "unknown kind $kind" >&2; exit 2 ;;
esac
rc=$?
set -e
if [[ $rc == 124 ]]; then
  echo "  TIMEOUT after ${JOB_TIMEOUT_S:-900} s: $kind $dataflow $(basename "$dir")" >&2
  echo TIMEOUT > "$dir/net_runtime_ms.txt"
  exit 0
fi
[[ $rc == 0 ]] || exit $rc
runtime=$(awk '/^CAPS_NET_RUNTIME_MS/ {print $2}' "$dir/flink.log" | tail -1)
echo "${runtime:-NA}" > "$dir/net_runtime_ms.txt"

#!/usr/bin/env bash
# E4: checkpointing, failure and recovery of the CAPS index.
#
# What it shows: how the CAPS meta-data and the temporal index are integrated with Flink checkpointing and recovery,
# and what prevents duplicate contributions or an inconsistency between the sink outputs and the prefix-sum index after a failure
# and replay. The answer is in the code (DisorderSource, Disorder.Assigner, caps.TieredIndexSink, caps.SegmentStore,
# caps.OutputLogSink); this script is the evidence.
#
# Per dataflow, on the standalone cluster (one cluster per run, under `flock -o $LOCK_FILE`), with checkpointing every CKPT_MS and
# a fixed-delay restart strategy:
#   verification runs (sources paced to PACE_S seconds so that a failure lands in the middle of the job; all four write the id of
#   every output and the index digest, and build the scan baseline that the index is checked against at the end of each run):
#     plain    no checkpointing, no failure
#     ref      checkpointing, no failure                                      <- run (a)
#     inject   checkpointing, the source throws once after FAIL_FRAC of its records                       <- run (b), in-job
#     kill     checkpointing, kill -9 of the TaskManager once KILL_AFTER_CKPTS checkpoints have completed (+ KILL_DELAY_MS);
#              a replacement TaskManager is started with taskmanager.sh right after                       <- run (b), process
#   verify_recovery.py compares plain, inject and kill with ref: every benchmark answer, the Tier-2 digest and per-channel totals,
#   the sink counters, and the output ids (distinct rows equal, no id with two contents); it reports the duplicates re-emitted.
#   timing runs (not paced, REPS each, interleaved): time_plain, time_ckpt  = the runtime overhead of checkpointing
#   (index only, no scan baseline, no provisional queries, no id log, as in the timing runs of the other experiments).
# Outputs: results_<small|full>/recovery_verify.csv, recovery_runs.csv, recovery_summary.csv, recovery_*.pdf.
#
#   SMALL=1 ./run.sh                         all six dataflows on the 10% subsets
#   ./run.sh                                 all six dataflows on the full data
#   PHASES="verify" ./run.sh                 only the failure runs and their checks
set -euo pipefail

# --- parameters (comment in or out) ---
DATAFLOWS_FULL=(
  nexmark_1
  nexmark_2
  taxi_1
  taxi_2
  twitter_1
  twitter_2
)
DATAFLOWS_SMALL=(
  nexmark_1
  nexmark_2
  taxi_1
  taxi_2
  twitter_1
  twitter_2
)
KINDS=(          # verification runs, in this order
  plain
  ref
  inject
  kill
)
CONFIGS=(        # parallelism p, disorder D and watermark slack delta (ms), one results directory per configuration
  "p1_D0_d0"     # the submitted runs (results_<tag>/)
  "p4_D60000_d60000" # parallelism 4, D = delta = 60 s (results_<tag>_p4_D60000_d60000/)
)
REPS="${REPS:-3}"                       # repetitions of the timing runs
FAIL_FRAC=0.6                           # inject: the source throws after this fraction of its records
KILL_AFTER_CKPTS=3                      # kill: completed checkpoints before the kill ...
RESTART_DELAY_MS=2000                   # fixed-delay restart strategy
VERIFY_WM_INTERVAL_MS_DISORDER=1        # watermark period of the paced verification runs when D > 0: one watermark step then spans
                                        #   tens of seconds of event time, comparable to D (as in the paced correctness runs of
                                        #   experiments/disorder); at 50 ms a step spans far more than D and the disorder hardly matters
# SMALL=1                               # the 10% subsets (data/small), all six dataflows
# SKIP_BUILD=1
# FORCE=1                               # redo finished runs
PHASES="${PHASES:-verify timing}"
LOCK_FILE="${LOCK_FILE:-/tmp/caps-flink.lock}"

# Per dataflow: seconds a paced verification run lasts (PACE_S), the checkpoint interval of those runs (CKPT_MS = PACE_S / 10) and
# the interval of the unpaced timing runs (TIMING_CKPT_MS, about a tenth of their runtime).
declare -A TIMING_CKPT_MS_FULL=([nexmark_1]=250 [nexmark_2]=100 [taxi_1]=2000 [taxi_2]=600 [twitter_1]=1500 [twitter_2]=500)
declare -A TIMING_CKPT_MS_SMALL=([nexmark_1]=100 [nexmark_2]=50 [taxi_1]=500 [taxi_2]=500 [twitter_1]=500 [twitter_2]=100)
PACE_S_FULL=60
PACE_S_SMALL=20

EXPDIR="$(cd "$(dirname "$0")" && pwd)"

# One pass of this script per configuration (E4_CONFIGS="p4_D60000_d60000" ./run.sh runs only that one).
if [[ -z ${E4_CONFIG:-} ]]; then
  for cfg in ${E4_CONFIGS:-"${CONFIGS[@]}"}; do E4_CONFIG=$cfg bash "$0"; done
  exit 0
fi
IFS=_ read -r P D_MS DELTA_MS <<< "$E4_CONFIG"; P=${P#p}; D_MS=${D_MS#D}; DELTA_MS=${DELTA_MS#d}
# as in the disorder experiment (E3): p readers, p sink subtasks (one index shard each), disorder D, watermark slack delta
export CAPS_PARALLELISM=$P CAPS_SOURCE_PARALLELISM=$P CAPS_SINK_PARALLELISM=$P CAPS_DISORDER_MS=$D_MS CAPS_DELTA_MS=$DELTA_MS
[[ $D_MS != 0 ]] && export CAPS_COUNT_LATE=1   # count the records that miss a window (must stay 0 when delta >= D)
SMALL="${SMALL:-0}"
if [[ $SMALL == 1 ]]; then
  DATAFLOWS_E4=("${DATAFLOWS_SMALL[@]}"); TAG=small; PACE_S=$PACE_S_SMALL
  declare -A TIMING_CKPT_MS=(); for k in "${!TIMING_CKPT_MS_SMALL[@]}"; do TIMING_CKPT_MS[$k]=${TIMING_CKPT_MS_SMALL[$k]}; done
else
  DATAFLOWS_E4=("${DATAFLOWS_FULL[@]}"); TAG=full; PACE_S=$PACE_S_FULL
  declare -A TIMING_CKPT_MS=(); for k in "${!TIMING_CKPT_MS_FULL[@]}"; do TIMING_CKPT_MS[$k]=${TIMING_CKPT_MS_FULL[$k]}; done
fi
if [[ -n "${E4_DATAFLOWS:-}" ]]; then DATAFLOWS_E4=($E4_DATAFLOWS); fi
DATAFLOWS="${DATAFLOWS_E4[*]}"
source "$(cd "$(dirname "$0")/.." && pwd)/common.sh"

# NO_VERSIONING=1 (default): only the 1000 temporal how-much queries per run, no versioning queries (a versioning query is two
# how-much queries, Sec. 7.3); the runs go to work_<tag>_howmuch/ and results_<tag>_howmuch/ and never overwrite the earlier ones
NO_VERSIONING="${NO_VERSIONING:-1}"
export CAPS_NO_VERSIONING=$NO_VERSIONING
[[ $NO_VERSIONING == 1 ]] && TAG="${TAG}_howmuch"
[[ $E4_CONFIG != "${CONFIGS[0]}" ]] && TAG="${TAG}_$E4_CONFIG"   # the first configuration keeps the original directories
WORK="$EXPDIR/work_$TAG"
RESULTS="$EXPDIR/results_$TAG"
mkdir -p "$WORK" "$RESULTS"
CKPT_MS=$((PACE_S * 100))

if ! plot_only; then
  require_flink
  for dataflow in "${DATAFLOWS_E4[@]}"; do require_inputs "$dataflow"; done
fi

# The cluster of the experiment: the stock configuration plus a file-system state backend (the default memory backend
# limits a state to 5 MB), a long checkpoint history for the REST records, a short heartbeat (the stock timeout is 5 min; a killed
# TaskManager is detected through it, not through the closed connection), and a private pid directory (the TaskManager to kill is found there).
if ! plot_only; then
export FLINK_CONF_DIR="$WORK/conf"
export PID_DIR="$WORK/pids"
rm -rf "$FLINK_CONF_DIR"; cp -r "$FLINK_DIR/conf" "$FLINK_CONF_DIR"
cat >> "$FLINK_CONF_DIR/flink-conf.yaml" <<CONF

# --- recovery experiment (E4) ---
state.backend: filesystem
state.checkpoints.dir: file://$WORK/ckpt
web.checkpoints.history: 1000
heartbeat.interval: 1000
heartbeat.timeout: 6000
env.pid.dir: $PID_DIR
CONF
fi

if [[ ${SKIP_BUILD:-0} != 1 ]] && ! plot_only; then
  flock -o "$LOCK_FILE" bash -c "source '$ROOT/experiments/common.sh'; DATAFLOWS='$DATAFLOWS' build_all"
fi

# the watermark period of the verification and timing runs of E4 (the default of the code is 1 ms)
export CAPS_WM_INTERVAL_MS="${CAPS_WM_INTERVAL_MS:-50}"
VERIFY_WM_INTERVAL_MS=$CAPS_WM_INTERVAL_MS
(( D_MS > 0 )) && VERIFY_WM_INTERVAL_MS=$VERIFY_WM_INTERVAL_MS_DISORDER
export KILL_AFTER_CKPTS RESTART_DELAY_MS FAIL_FRAC NUM_QUERIES PACE_S VERIFY_WM_INTERVAL_MS

finished() { [[ -s "$1/run.txt" ]] && grep -q '^rc=0$' "$1/run.txt" && [[ ${FORCE:-0} != 1 ]]; }

# job <kind> <dataflow> <dir> <checkpoint interval ms>
job() {
  local kind=$1 dataflow=$2 dir=$3 ckpt=$4
  finished "$dir" && return 0
  echo "  $kind $dataflow  (checkpoint every $ckpt ms)"
  local rc attempt
  for attempt in $(seq 1 60); do # status 75: the cluster collided with another one on this machine, try again
    rc=0
    CKPT_MS=$ckpt KILL_DELAY_MS=$((ckpt / 2)) flock -o "$LOCK_FILE" bash "$EXPDIR/run_one.sh" "$kind" "$dataflow" "$dir" || rc=$?
    [[ $rc == 75 ]] || break
    echo "  (collision with another cluster, attempt $attempt; waiting)" >&2
    sleep 60
  done
  [[ $rc == 0 ]] || { echo "  FAILED: $kind $dataflow, see $dir/flink.log" >&2; return 1; }
  rm -rf "$WORK/ckpt" "$dir/segments"   # the segments are measured (recovery_stats.csv); they are not needed after the run
}

echo "dataflow,outputs" > "$WORK/outputs.csv"
echo "dataflow,ms" > "$WORK/timing_ckpt_ms.csv"
for dataflow in "${DATAFLOWS_E4[@]}"; do
  n=${FULL_OUTPUTS[$dataflow]}; [[ $SMALL == 1 ]] && n=${SMALL_OUTPUTS[$dataflow]}
  echo "$dataflow,$n" >> "$WORK/outputs.csv"
  echo "$dataflow,${TIMING_CKPT_MS[$dataflow]}" >> "$WORK/timing_ckpt_ms.csv"
done

for dataflow in "${DATAFLOWS_E4[@]}"; do
  echo -e "\n=== $dataflow  small=$SMALL  $E4_CONFIG ==="
  base="$WORK/$dataflow"
  if [[ " $PHASES " == *" verify "* ]]; then
    for kind in "${KINDS[@]}"; do job "$kind" "$dataflow" "$base/$kind" "$CKPT_MS"; done
    others=()
    for kind in "${KINDS[@]}"; do [[ $kind != ref && -s "$base/$kind/run.txt" ]] && others+=(--other "$base/$kind"); done
    if compgen -G "$base/ref/outputs_s*.log" > /dev/null && [[ ${#others[@]} -gt 0 ]]; then
      ids_only=(); [[ $dataflow == taxi_2 && $P -gt 1 ]] && ids_only=(--ids-only) # doubles summed in arrival order (see verify_recovery.py)
      "$PYTHON" "$EXPDIR/verify_recovery.py" --dataflow "$dataflow" --ref "$base/ref" "${others[@]}" "${ids_only[@]}" \
        --csv "$RESULTS/recovery_verify.csv" --tmp "$WORK" || echo "  VERIFICATION FAILED for $dataflow" >&2
      rm -f "$base"/*/outputs_s*.log   # up to a GB per run; the verdict and the counts are in recovery_verify.csv
    fi
  fi
  if [[ " $PHASES " == *" timing "* ]]; then
    for rep in $(seq 1 "$REPS"); do # interleaved: plain, checkpointing, plain, checkpointing, ...
      job time_plain "$dataflow" "$base/time_plain_$rep" 0
      job time_ckpt "$dataflow" "$base/time_ckpt_$rep" "${TIMING_CKPT_MS[$dataflow]}"
    done
  fi
done

# PHASES=plot: the raw runs (work_<tag>/) are not committed; if they are there the CSVs are computed from them again,
# otherwise the figures are redrawn from the CSVs in results_<tag>/.
if ! plot_only || [[ -n $(ls -A "$WORK"/*/ 2>/dev/null) ]]; then
  "$PYTHON" "$EXPDIR/summarize_recovery.py" --work "$WORK" --results "$RESULTS" --outputs "$WORK/outputs.csv" \
    --timing-ckpt-ms "$WORK/timing_ckpt_ms.csv"
fi
"$PYTHON" "$EXPDIR/plot_recovery.py" --results "$RESULTS"

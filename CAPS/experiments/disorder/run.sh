#!/usr/bin/env bash
# E3: out-of-order events, skew, parallel operators, late data.
#
# CAPS and the no-provenance baseline read the same replayed input through p parallel readers, each with its
# own watermark. The sources add controlled disorder D (a displacement of every record in [0, D], fixed seed),
# the watermark assigner (Flink 1.10 BoundedOutOfOrdernessTimestampExtractor, periodic) declares a maximum
# out-of-orderness delta, window operators have zero allowed lateness and a late-record side output. Two skew
# knobs: a slow reader (one partition runs `factor` times slower: inter-subtask watermark spread) and a Zipf
# remapping of the dataflow's key (skewed partitions). The sinks are the two-tier index (one shard per sink
# subtask, or one global sink fed by all subtasks).
#
# One factor at a time is varied around a default point (arms below). Per configuration:
#   late     in-process runtime (inprocess/bin/flink; Flink's operator metrics are only wired up there): one CAPS
#            run (REPS runs if delta < D or a reader goes idle, because what is late then depends on how the readers
#            progress) with the answers of 1000 how-much queries (NO_VERSIONING=1, the default; 0 adds 2 x 1000 versioning queries), compared with the ordered
#            reference run (D = 0, delta = 0, p = 1, same input and Zipf remapping) that answers the same query
#            windows; late records per operator (dropped, or missed a window), records behind the watermark
#   measure  standalone cluster, one CAPS run with traces: Tier-1 occupancy and its bound, delay to finality,
#            provisional and final query latency, the same answers
#   timing   standalone cluster, REPS runs each of the no-provenance dataflow and of CAPS (two-tier index only), same
#            p, D, delta, skew; end-to-end job duration (the net runtime Flink reports)
#   check    (SMALL only, in-process) CAPS output rows against the ordered run: how many outputs differ
# Results: results_<small|full>/*.csv and *.pdf (analyze.py, plot_disorder.py, tables.py); raw runs in work_<small|full>/.
#
# Paced mode (PACED=1, arms D and delta only): every reader follows a schedule (CAPS_PACE_SECONDS) instead of replaying at
# full speed, so that one watermark interval is a few seconds of event time, commensurable with D and delta. At full speed
# one interval (1-50 ms) covers hours of event time (the replay moves 1-10 million event ms per ms), the watermark lags
# the data by far more than D, and a record is almost never late even with delta = 0. Results go to work_<tag>_paced/ and
# results_<tag>_paced/.
#
#   SMALL=1 REPS=1 ./run.sh                          10% subsets, all six dataflows
#   PACED=1 E3_DATAFLOWS="nexmark_1 taxi_2 twitter_1" ./run.sh   paced D and delta arms, full data (in-process, PHASES=late)
#   E3_DATAFLOWS="taxi_2 twitter_1 nexmark_1" ./run.sh   full data of three dataflows
#   PHASES="timing" ./run.sh                         only the timing runs
#
# Every job holds `flock -o $LOCK_FILE` (default /tmp/caps-flink.lock) for its whole sequence: stop and start the
# standalone cluster (FLINK_DIR, default code/flink-1.10.0), run the job, stop the cluster. The cluster runs
# (measure, timing) are the ones whose processing-time numbers are reported.
set -euo pipefail

# --- parameters (comment in or out) ---
DATAFLOWS_E3=(
  nexmark_1
  nexmark_2
  taxi_1
  taxi_2
  twitter_1
  twitter_2
)
D_VALUES=(      # disorder D in ms of event time: 0, 1 s, 10 s, 60 s, 5 min
  0
  1000
  10000
  60000
  300000
)
DELTA_D_VALUES=( # the disorder D at which delta is varied (a sparse stream such as the tweets needs a large D to be hit)
  60000
  300000
)
DELTA_D_VALUES_PACED=( # the same in paced mode: the watermark moves in steps of 20-30 s of event time on nexmark_1 and taxi_2 and 200 s
  10000                # on twitter_1, so a D below the step is not seen by it; 30 min is far above every step
  60000
  300000
  1800000
)
TIMING_D_VALUES=( # the D arm times noprov and CAPS only at these D (the overhead does not depend on D: D = 0, 60 s, 5 min show it)
  0
  60000
  300000
)
DELTA_FACTORS=( # delta / D: below, equal, above (0 = no tolerance at all)
  0
  0.5
  1
  2
)
P_VALUES=(      # parallelism of the job, of the readers and of the sinks
  1
  2
  4
  8
)
SLOW_FACTORS=(  # the slow reader runs this many times slower
  1
  4
)
ZIPF_EXPONENTS=( # 0 = the file's own keys
  0
  1.0
)
SINK_MODES=(    # shards: one sink (index shard) per subtask, chained to the last operator; global: one sink for all
  shards
  global
)
TIER1_IMPLS=(   # Tier 1 of the index: augmented red-black tree, hybrid (ordered run + tree, the default), sorted buffer
  tree
  hybrid
  buffer
)
WM_INTERVALS_MS=( # watermark interval (processing time); the occupancy of Tier 1 grows with it (arm tier1)
  10
  50
  250
  1000
)
IDLE_MS=(       # idleness timeout tau of a reader, in ms (0 = off), arm idle (slow reader with long pauses)
  0
  20
)
P_DEFAULT=4
SLOW_BATCH_IDLE=16384   # records per throttle batch of the slow reader in the idle arm (long pauses)
D_DEFAULT=60000
WM_INTERVAL_DEFAULT=50
REPS="${REPS:-3}"
PACED="${PACED:-0}"                               # 1: paced sources, arms D and delta, results in work_<tag>_paced
# Paced mode: seconds in which a reader reads its whole file (full data / SMALL), and the watermark interval (ms of
# processing time). The event time of one interval is (span of the file) / PACE_S * interval: nexmark_1 2.7e6 s / 240 s =
# 11 s per ms, taxi_2 1.9e6 s / 240 s = 8 s, twitter_1 7.9e6 s / 300 s = 26 s; SMALL spans a tenth of the file. The watermark
# the window operators see moves in steps of about three intervals (timer jitter, minimum over the readers): a step of
# 20-80 s of event time. A disorder D far below the step is absorbed before the first watermark can see it.
declare -A PACE_S_FULL=([nexmark_1]=240 [nexmark_2]=240 [taxi_1]=240 [taxi_2]=240 [twitter_1]=300 [twitter_2]=300)
declare -A PACE_S_SMALL=([nexmark_1]=15 [nexmark_2]=15 [taxi_1]=15 [taxi_2]=15 [twitter_1]=20 [twitter_2]=20)
PACED_WM_INTERVAL_MS=1
TIER1_DEFAULT=hybrid                              # Tier 1 of the arms D, delta, p, skew, sink and idle: the default of the index sinks
ARM_INTERVAL=$WM_INTERVAL_DEFAULT                 # watermark interval of the arms (the tier1 arm sweeps it)
export JOB_TIMEOUT_S="${JOB_TIMEOUT_S:-420}"      # per job; a run that does not finish is recorded as TIMEOUT
[[ $PACED == 1 ]] && ARM_INTERVAL=$PACED_WM_INTERVAL_MS
[[ $PACED == 1 ]] && ZIPF_EXPONENTS=(0)           # the paced arms have no Zipf remapping
CFG_SHARD="${CFG_SHARD:-0/1}"                     # i/n: only the configurations number i (mod n): n instances of this script (own LOCAL_CORES, LOCK_FILE) share a work dir
[[ $PACED == 1 ]] && export JOB_TIMEOUT_S="${JOB_TIMEOUT_S_PACED:-900}"
if [[ $PACED == 1 ]]; then DEFAULT_ARMS="D delta"; DEFAULT_PHASES="late"; else DEFAULT_ARMS="D delta p skew sink tier1 idle"; DEFAULT_PHASES="late measure timing check"; fi
ARMS="${ARMS:-$DEFAULT_ARMS}"                     # which arms to run
PHASES="${PHASES:-$DEFAULT_PHASES}"               # late, measure, timing, check (output equality, SMALL only)
# SMALL=1                  # the 10% subsets (data/small)
# NUM_QUERIES=1000         # how-much and versioning queries per run
# SKIP_BUILD=1
# FLINK_DIR=/path/to/flink-1.10.0 # the standalone cluster of measure and timing (default: code/flink-1.10.0)
# LOCAL_CORES=8-15                # cores of the in-process runs (late, check); default 8-15, or all cores below 16
LOCK_FILE="${LOCK_FILE:-/tmp/caps-flink.lock}"
# E3 counts the records that are late or miss a window (late side outputs, MissedWindowTrigger). The other experiments
# leave this off, so that the four methods run the same operators (DisorderConfig, CAPS_COUNT_LATE).
export CAPS_COUNT_LATE=1

EXPDIR="$(cd "$(dirname "$0")" && pwd)"
if [[ -n "${E3_DATAFLOWS:-}" ]]; then DATAFLOWS_E3=($E3_DATAFLOWS); fi
if [[ -n "${E3_D_VALUES:-}" ]]; then D_VALUES=($E3_D_VALUES); fi             # e.g. E3_D_VALUES=60000 E3_WM_INTERVALS=50 ARMS="D sink tier1": the rows of the cost table only
if [[ -n "${E3_WM_INTERVALS:-}" ]]; then WM_INTERVALS_MS=($E3_WM_INTERVALS); fi
DATAFLOWS="${DATAFLOWS_E3[*]}"
source "$(cd "$(dirname "$0")/.." && pwd)/common.sh"
FLINK_DIR_INPROCESS="${FLINK_DIR_INPROCESS:-$EXPDIR/inprocess}"

# Number of keys of the Zipf remapping per dataflow: the distinct zones of the taxi file (taxi_2), taxis (taxi_1)
# and hashtags (Twitter); nexmark draws the seller of an auction from the persons of the persons file.
if [[ $SMALL == 1 ]]; then
  declare -A ZIPF_DOMAIN=([nexmark_1]=1 [nexmark_2]=1 [taxi_1]=12999 [taxi_2]=665 [twitter_1]=4062 [twitter_2]=4062)
else
  declare -A ZIPF_DOMAIN=([nexmark_1]=1 [nexmark_2]=1 [taxi_1]=13257 [taxi_2]=910 [twitter_1]=19113 [twitter_2]=19113)
fi

TAG="small"; [[ $SMALL == 1 ]] || TAG="full"
[[ $PACED == 1 ]] && TAG="${TAG}_paced"
# NO_VERSIONING=1: only the 1000 temporal how-much queries per run, no versioning queries (a versioning query is two
# how-much queries); the runs go to work_<tag>_howmuch/ and results_<tag>_howmuch/. The defaults are those of the paper:
# the paced arms (Table 3, left) with NO_VERSIONING=1, the full-speed arms (Table 3, right) with NO_VERSIONING=0.
NO_VERSIONING="${NO_VERSIONING:-$PACED}"
export CAPS_NO_VERSIONING=$NO_VERSIONING
[[ $NO_VERSIONING == 1 ]] && TAG="${TAG}_howmuch"
WORK="$EXPDIR/work_$TAG"
RESULTS="$EXPDIR/results_$TAG"
mkdir -p "$WORK" "$RESULTS"

if ! plot_only; then
  require_flink
  for dataflow in "${DATAFLOWS_E3[@]}"; do require_inputs "$dataflow"; done
fi

if [[ ${SKIP_BUILD:-0} != 1 ]] && ! plot_only; then
  echo "[build]"
  (cd "$ROOT/code/temporal_index" && mvn -q clean install -DskipTests)
  (cd "$ROOT/code/tiered_index" && mvn -q clean install -DskipTests)
  (cd "$ROOT/code/caps" && mvn -q clean install -DskipTests)
  for dataflow in "${DATAFLOWS_E3[@]}"; do
    (cd "$ROOT/code/$dataflow" && mvn -q clean package -DskipTests)
    (cd "$ROOT/code/noprov/$dataflow" && mvn -q clean package -DskipTests)
  done
fi

# --- configurations -------------------------------------------------------------------------------------------
# A configuration is p, D, delta, slow factor, Zipf exponent, sink parallelism (p or 1), Tier-1 implementation,
# watermark interval, idleness timeout, and whether its timing runs are made. Arms share configurations.
declare -A CFG_LINE ARM_OF
CFG_ORDER=()

add_cfg() { # arm p D delta slow zipf sink tier1 interval idle batch timing
  local arm=$1 p=$2 D=$3 delta=$4 slow=$5 zipf=$6 sink=$7 tier1=$8 interval=$9 idle=${10} batch=${11} timing=${12}
  local sinkp=$p; [[ $sink == global ]] && sinkp=1
  local id="p${p}_D${D}_d${delta}_s${slow}_z${zipf}_k${sink}_t${tier1}_i${interval}_u${idle}_b${batch}"
  if [[ -z ${CFG_LINE[$id]:-} ]]; then
    CFG_LINE[$id]="$p $D $delta $slow $zipf $sinkp $tier1 $interval $idle $batch $timing"
    CFG_ORDER+=("$id")
    ARM_OF[$id]="$arm"
  else
    ARM_OF[$id]="${ARM_OF[$id]} $arm"
    if [[ $timing == 1 ]]; then
      local line=(${CFG_LINE[$id]}); line[10]=1; CFG_LINE[$id]="${line[*]}"
    fi
  fi
}

delta_of() { awk -v d="$1" -v f="$2" 'BEGIN{printf "%d", d * f + 0.5}'; }

build_configs() {
  local D f p slow zipf sink tier1 interval idle timed
  for arm in $ARMS; do
    case $arm in
      D)     for D in "${D_VALUES[@]}"; do
               [[ " ${TIMING_D_VALUES[*]} " == *" $D "* ]] && timed=1 || timed=0
               add_cfg D $P_DEFAULT "$D" "$D" 1 0 shards $TIER1_DEFAULT $ARM_INTERVAL 0 1024 $timed; done ;;
      delta) local dlist=("${DELTA_D_VALUES[@]}"); [[ $PACED == 1 ]] && dlist=("${DELTA_D_VALUES_PACED[@]}")
             for D in "${dlist[@]}"; do for f in "${DELTA_FACTORS[@]}"; do
               add_cfg delta $P_DEFAULT "$D" "$(delta_of "$D" "$f")" 1 0 shards $TIER1_DEFAULT $ARM_INTERVAL 0 1024 0; done; done ;;
      p)     for p in "${P_VALUES[@]}"; do add_cfg p "$p" $D_DEFAULT $D_DEFAULT 1 0 shards $TIER1_DEFAULT $ARM_INTERVAL 0 1024 1; done ;;
      skew)  for slow in "${SLOW_FACTORS[@]}"; do for zipf in "${ZIPF_EXPONENTS[@]}"; do
               add_cfg skew $P_DEFAULT $D_DEFAULT $D_DEFAULT "$slow" "$zipf" shards $TIER1_DEFAULT $ARM_INTERVAL 0 1024 1; done; done ;;
      sink)  for sink in "${SINK_MODES[@]}"; do
               add_cfg sink $P_DEFAULT $D_DEFAULT $D_DEFAULT 1 0 "$sink" $TIER1_DEFAULT $ARM_INTERVAL 0 1024 1; done ;;
      tier1) for sink in "${SINK_MODES[@]}"; do for tier1 in "${TIER1_IMPLS[@]}"; do for interval in "${WM_INTERVALS_MS[@]}"; do
               add_cfg tier1 $P_DEFAULT $D_DEFAULT $D_DEFAULT 1 0 "$sink" "$tier1" "$interval" 0 1024 0; done; done; done ;;
      idle)  for idle in "${IDLE_MS[@]}"; do
               add_cfg idle $P_DEFAULT $D_DEFAULT $D_DEFAULT 4 0 shards $TIER1_DEFAULT $ARM_INTERVAL "$idle" $SLOW_BATCH_IDLE 0; done ;;
      *) echo "unknown arm $arm" >&2; exit 2 ;;
    esac
  done
}

# --- running --------------------------------------------------------------------------------------------------
locked() { flock -o "$LOCK_FILE" "$@"; }

# finished <dir>: the run has a runtime (or timed out: that is a result, a sorted buffer fed out of order is quadratic)
finished() { local f="$1/net_runtime_ms.txt"; [[ -s $f ]] && grep -qE '^([0-9]+|TIMEOUT)$' "$f"; }

# job <kind> <flink dir> <dataflow> <dir> <p> <D> <delta> <slow> <zipf> <sinkp> <tier1> <interval> <idle> <batch> [expected outputs]
job() {
  local kind=$1 fdir=$2 dataflow=$3 dir=$4 p=$5 D=$6 delta=$7 slow=$8 zipf=$9 sinkp=${10} tier1=${11} interval=${12} idle=${13}
  local batch=${14} expected=${15:-}
  mkdir -p "$dir"
  printf 'dataflow=%s\nkind=%s\np=%s\nD=%s\ndelta=%s\nslow=%s\nzipf=%s\nsinkp=%s\ntier1=%s\ninterval=%s\nidle=%s\nbatch=%s\nsmall=%s\nrunner=%s\n' \
    "$dataflow" "$kind" "$p" "$D" "$delta" "$slow" "$zipf" "$sinkp" "$tier1" "$interval" "$idle" "$batch" "$SMALL" \
    "$([[ -d $fdir/lib ]] && echo cluster || echo in-process)" > "$dir/config.txt"
  (
    export CAPS_PARALLELISM=$p CAPS_SOURCE_PARALLELISM=$p CAPS_DISORDER_MS=$D CAPS_DELTA_MS=$delta
    export CAPS_SLOW_FACTOR=$slow CAPS_ZIPF=$zipf CAPS_ZIPF_DOMAIN=${ZIPF_DOMAIN[$dataflow]}
    export CAPS_SINK_PARALLELISM=$sinkp CAPS_TIER1=$tier1 CAPS_WM_INTERVAL_MS=$interval CAPS_IDLE_MS=$idle
    export CAPS_SLOW_BATCH=$batch
    # paced mode: the readers follow the schedule (never the ordered references, which only provide the expected answers)
    if [[ $PACED == 1 && $dir != */ref_* ]]; then
      if [[ $SMALL == 1 ]]; then export CAPS_PACE_SECONDS=${PACE_S_SMALL[$dataflow]}; else export CAPS_PACE_SECONDS=${PACE_S_FULL[$dataflow]}; fi
    fi
    # the per-insert and per-split nanoTime brackets only in the measurement job (never in a timing run)
    [[ $kind == meas ]] && export CAPS_INSERT_TIMING=1 || export CAPS_INSERT_TIMING=0
    [[ -n $expected ]] && export EXPECTED_OUTPUTS=$expected
    export RUN_FLINK_DIR=$fdir
    locked bash "$EXPDIR/run_one.sh" "$kind" "$dataflow" "$dir"
  ) || { echo "  FAILED: $kind $dataflow $(basename "$dir"), see $dir/flink.log" >&2; echo FAIL > "$dir/net_runtime_ms.txt"; }
}

# The ordered reference of a Zipf setting: p = 1, D = 0, delta = 0, one reader, same input and remapping.
reference() { # dataflow zipf
  local dataflow=$1 zipf=$2 dir="$WORK/$1/ref_z$2/late_r1"
  [[ -s "$dir/query_answers.csv" ]] && return 0
  echo "  reference z=$zipf"
  job meas "$FLINK_DIR_INPROCESS" "$dataflow" "$dir" 1 0 0 1 "$zipf" 1 $TIER1_DEFAULT $WM_INTERVAL_DEFAULT 0 1024
}

ref_outputs() { # dataflow zipf -> indexed records of the reference (the expected number of sink outputs)
  awk -F, 'NR>1 {n += $5} END {print n}' "$WORK/$1/ref_z$2/late_r1/tiered_stats.csv"
}

run_dataflow() {
  local dataflow=$1 id line p D delta slow zipf sinkp tier1 interval idle batch timing dir exp rep m runs
  echo -e "\n=== $dataflow  small=$SMALL  configurations: ${#CFG_ORDER[@]} ==="
  for zipf in "${ZIPF_EXPONENTS[@]}"; do reference "$dataflow" "$zipf"; done
  local k=-1
  for id in "${CFG_ORDER[@]}"; do
    k=$((k + 1))
    (( k % ${CFG_SHARD#*/} == ${CFG_SHARD%/*} )) || continue
    line=(${CFG_LINE[$id]})
    p=${line[0]}; D=${line[1]}; delta=${line[2]}; slow=${line[3]}; zipf=${line[4]}; sinkp=${line[5]}
    tier1=${line[6]}; interval=${line[7]}; idle=${line[8]}; batch=${line[9]}; timing=${line[10]}
    dir="$WORK/$dataflow/$id"
    exp=$(ref_outputs "$dataflow" "$zipf")
    echo "  $id  (${ARM_OF[$id]})"
    local ranges="$WORK/$dataflow/ref_z$zipf/late_r1/query_answers.csv"
    # (the in-process runs are for the late-record counts and the repeated correctness runs; the Tier-1 arm needs neither)
    if [[ " $PHASES " == *" late "* ]] && (( timing == 1 || delta < D || idle > 0 )) || [[ $PACED == 1 && " $PHASES " == *" late "* ]]; then
      # What a late record costs depends on how the readers progress against the periodic watermark, so with
      # delta < D (or an idle reader) it is measured REPS times; otherwise nothing is late and once is enough.
      runs="late_r1"; if (( delta < D || idle > 0 )); then runs=$(seq -f 'late_r%g' 1 "$REPS" | tr '\n' ' '); fi
      for m in $runs; do
        [[ -s "$dir/$m/query_answers.csv" ]] || CAPS_QUERY_RANGES_FROM="$ranges" \
          job meas "$FLINK_DIR_INPROCESS" "$dataflow" "$dir/$m" "$p" "$D" "$delta" "$slow" "$zipf" "$sinkp" "$tier1" "$interval" "$idle" "$batch" "$exp"
      done
    fi
    if [[ " $PHASES " == *" measure "* ]] && ! finished "$dir/meas"; then
      CAPS_QUERY_RANGES_FROM="$ranges" \
        job meas "$FLINK_DIR" "$dataflow" "$dir/meas" "$p" "$D" "$delta" "$slow" "$zipf" "$sinkp" "$tier1" "$interval" "$idle" "$batch" "$exp"
    fi
    if [[ " $PHASES " == *" timing "* && $timing == 1 ]]; then
      for rep in $(seq 1 "$REPS"); do # interleaved: noprov, CAPS, noprov, CAPS, ...
        finished "$dir/time_noprov_$rep" || \
          job time_noprov "$FLINK_DIR" "$dataflow" "$dir/time_noprov_$rep" "$p" "$D" "$delta" "$slow" "$zipf" "$sinkp" "$tier1" "$interval" "$idle" "$batch" "$exp"
        finished "$dir/time_caps_$rep" || \
          job time_caps "$FLINK_DIR" "$dataflow" "$dir/time_caps_$rep" "$p" "$D" "$delta" "$slow" "$zipf" "$sinkp" "$tier1" "$interval" "$idle" "$batch" "$exp"
      done
    fi
    if [[ " $PHASES " == *" check "* && $SMALL == 1 ]] && (( timing == 1 || delta < D )); then
      if [[ ! -s "$dir/file_caps/caps.out" ]]; then
        [[ -s "$WORK/$dataflow/ref_z$zipf/file_caps/caps.out" ]] || \
          job file_caps "$FLINK_DIR_INPROCESS" "$dataflow" "$WORK/$dataflow/ref_z$zipf/file_caps" 1 0 0 1 "$zipf" 1 $TIER1_DEFAULT $WM_INTERVAL_DEFAULT 0 1024 "$exp"
        job file_caps "$FLINK_DIR_INPROCESS" "$dataflow" "$dir/file_caps" "$p" "$D" "$delta" "$slow" "$zipf" "$sinkp" "$tier1" "$interval" "$idle" "$batch" "$exp"
      fi
    fi
  done
}

build_configs
# REFS_ONLY=1: only the ordered reference runs (several instances with CFG_SHARD then do not race to make them)
if [[ ${REFS_ONLY:-0} == 1 ]]; then
  for dataflow in "${DATAFLOWS_E3[@]}"; do for zipf in "${ZIPF_EXPONENTS[@]}"; do reference "$dataflow" "$zipf"; done; done
  exit 0
fi
if ! plot_only; then for dataflow in "${DATAFLOWS_E3[@]}"; do run_dataflow "$dataflow"; done; fi

PYTHON="${PYTHON:-python3}"
# PHASES=plot: the raw runs (work_<tag>/) are not committed; if they are there the CSVs are computed from them again,
# otherwise the figures and tables are redrawn from the CSVs in results_<tag>/.
if ! plot_only || [[ -n $(ls -A "$WORK" 2>/dev/null) ]]; then
  "$PYTHON" "$EXPDIR/analyze.py" --work "$WORK" --out "$RESULTS" --small "$SMALL" --reps "$REPS"
fi
WORK_ARG=(); [[ -n $(ls -A "$WORK" 2>/dev/null) ]] && WORK_ARG=(--work "$WORK")   # the occupancy over time needs the raw runs
PACED_ARG=(); [[ $PACED == 1 ]] && PACED_ARG=(--paced)                              # the paced arms have their own figures and tables
"$PYTHON" "$EXPDIR/plot_disorder.py" --results "$RESULTS" "${WORK_ARG[@]}" "${PACED_ARG[@]}"
"$PYTHON" "$EXPDIR/tables.py" --results "$RESULTS" "${PACED_ARG[@]}" > "$RESULTS/tables.md"
echo "results: $RESULTS"

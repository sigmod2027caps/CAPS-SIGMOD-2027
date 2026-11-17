#!/usr/bin/env bash
# Channel-cardinality scalability: runtime, retained meta-data, per-output query
# latency and temporal query latency of noprov, GeneaLog, Green and CAPS against the number of
# channels M, on two synthetic dataflows with the same input and the same outputs at every M:
#
#   source sweep  one path, M channel labels (sourceId % M): M is the width of the annotation
#   path sweep    M real exclusive branches Filter(id % M == i) -> Map, then Union: the dataflow
#                 grows with M as well as the annotation (noprov shows the cost of the operators)
#
# Per (sweep, M):
#   1. verification: every method writes its outputs to a file; verify_scale.py checks them against
#      each other and against an oracle recomputed from the input (outputs, per-channel counts,
#      totals = GeneaLog traversal, window-end timestamps, order). A point that fails is not timed.
#   2. timing: REPS submissions per method on a fresh cluster, round robin, discard sink; the Flink
#      job duration (REST) is the metric, the output count is recorded.
#   3. queries: CAPS draws the shared 1,000 output ids, the three methods capture them into query
#      fixtures, and JMH times the per-output queries on the same ids (total, and the M-vector for
#      CAPS and Green); the temporal how-much query of CAPS (prefix-sum index) is timed offline on
#      the verification outputs.
# Retained meta-data is analytical: CAPS 8 B per channel per output, Green 8 B per variable of each
# output's monomial, GeneaLog 32 B per tuple an operator emitted and that stays reachable.
#
# Two engines (MODE): "cluster" (default, the one the timings are reported on) submits to the
# standalone cluster, a fresh one per run; "local" runs each job in an in-process Flink MiniCluster
# through a stand-in for `flink run` (FLINK_DIR points at it), only for checks. Verification, queries
# and meta-data do not depend on the engine; only the timings do, so PHASE_LIST=time re-runs just the
# timing next to existing verification and query results.
# Every unit (start cluster, job, stop cluster) runs under `flock -o /tmp/caps-flink.lock`, one at a
# time, so another experiment can run in between and no timed run overlaps another Flink job.
# Finished units are skipped on a rerun (FORCE=1 starts over).
#
# Usage:  SMALL=1 experiments/channel_scaling/run.sh     # 10% input, quick check
#         experiments/channel_scaling/run.sh             # full input
#         MODE=local FLINK_DIR=<dir with bin/flink> experiments/channel_scaling/run.sh   # in-process
#         MODE=cluster PHASE_LIST="time" experiments/channel_scaling/run.sh   # only re-time on the cluster
#         PHASES=plot experiments/channel_scaling/run.sh   # only the summary and the plots, no job
# Environment (common.sh): SMALL, REPS, CAPS_PARALLELISM, FLINK_DIR. Also MODE, BUILD=1 (rebuild the jars), FORCE=1,
# JMH_WARMUP / JMH_MEASURE (iterations of 1 s, default 10 and 5).
set -euo pipefail

# PHASES=plot: no job; the CSVs are computed again from the raw runs (work/, not committed) if they are there, and the
# figures are drawn from the CSVs of results/. (PHASES below is the list of phases of a run; the variable is read first.)
PLOT_ONLY=0; [[ " ${PHASES:-} " == *" plot "* ]] && PLOT_ONLY=1

# ---- parameters: comment lines in or out ----
METHODS=(
  noprov
  genealog
  green
  caps
)
SWEEPS=(
  source
  path
)
M_VALUES=(
  1
  2
  4
  8
  16
  32
  64
  128
  512
)
# Input: records / keys of the generator (60 s windows, 34 of them: about 59 records per output).
# SMALL=1 uses the 10% input (200,000 records, 100 keys). REPS and CAPS_PARALLELISM: see common.sh.
RECORDS_FULL=2000000
KEYS_FULL=1000
RECORDS_SMALL=200000
KEYS_SMALL=100
TEMPORAL_RANGE_MS=600000   # temporal query: 10 windows
MODE="${MODE:-cluster}"    # cluster | local (see above)
PHASES=(
  verify
  time
  queries
)
# ---------------------------------------------
# Quick checks without editing the lists above: M_LIST="1 4" SWEEP_LIST="source"
[[ -n ${M_LIST:-} ]] && read -ra M_VALUES <<< "$M_LIST"
[[ -n ${SWEEP_LIST:-} ]] && read -ra SWEEPS <<< "$SWEEP_LIST"
[[ -n ${PHASE_LIST:-} ]] && read -ra PHASES <<< "$PHASE_LIST"

EXPDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SELF="$EXPDIR/$(basename "${BASH_SOURCE[0]}")"
source "$EXPDIR/../common.sh"

LOCK=/tmp/caps-flink.lock   # every Flink use, in-process or standalone, holds it
SS="$ROOT/code/synthetic_scale"
JAR=synthetic-scale-1.0-REV-SNAPSHOT.jar
GL_CLASS=io.palyvos.provenance.usecases.synthetic_scale.provenance.queries.SyntheticScaleGL
BENCH_JAR="$SS/bench/target/benchmarks.jar"
INPUT_BASE="$DATA_ROOT/synthetic_scale/synthetic_scale"
INPUT="$INPUT_BASE.txt"
TAG=$([[ $SMALL == 1 ]] && echo small || echo full)_p$CAPS_PARALLELISM
WORK="$EXPDIR/work/$TAG"            # verification, fixtures, JMH: shared by the engines
RES="$EXPDIR/results/${TAG}_$MODE"  # CSVs and plots of one engine
export SMALL REPS CAPS_PARALLELISM MODE JMH_WARMUP="${JMH_WARMUP:-10}" JMH_MEASURE="${JMH_MEASURE:-5}"

# A fresh cluster per run (cluster mode); nothing to restart in-process.
fresh() { [[ $MODE == local ]] || restart_cluster; }

# gl_run <sweep> <M> <statistics dir> [extra args]: the GeneaLog job (own options, then the standard ones)
gl_run() {
  local sweep=$1 M=$2 statdir=$3; shift 3
  "$FLINK" run --parallelism "$CAPS_PARALLELISM" --class "$GL_CLASS" "$SS/genealog/target/$JAR" \
    --statisticsFolder "$statdir" --outputFile sink --inputFile "$INPUT_BASE" \
    --sourcesNumber 1 --sourceRepetitions 1 --autoFlush \
    --provenanceActivator GENEALOG --aggregateStrategy sortedPtr --sinkParallelism 1 \
    --sweep "$sweep" --channels "$M" "$@"
}

# submit <method> <sweep> <M> <kind> <dir> [manifest]
# kind: file (all outputs to <dir>/<method>.txt), discard (count into <dir>/outputs.txt),
#       ids (CAPS: write the manifest), shared (capture the manifest's ids into a fixture in <dir>)
submit() {
  local method=$1 sweep=$2 M=$3 kind=$4 dir=$5 manifest=${6:-}
  case $method:$kind in
    noprov:file)    "$FLINK" run --parallelism "$CAPS_PARALLELISM" --class SyntheticScale "$SS/noprov/target/$JAR" "$INPUT" "$dir/noprov.txt" "$sweep" "$M" ;;
    noprov:discard) "$FLINK" run --parallelism "$CAPS_PARALLELISM" --class SyntheticScale "$SS/noprov/target/$JAR" "$INPUT" unused "$sweep" "$M" discard "$dir" ;;
    caps:file)      "$FLINK" run --parallelism "$CAPS_PARALLELISM" --class SyntheticScaleCaps "$SS/caps/target/$JAR" "$INPUT" "$dir/caps.txt" "$sweep" "$M" ;;
    caps:discard)   "$FLINK" run --parallelism "$CAPS_PARALLELISM" --class SyntheticScaleCaps "$SS/caps/target/$JAR" "$INPUT" unused "$sweep" "$M" discard "$dir" ;;
    caps:ids)       "$FLINK" run --parallelism "$CAPS_PARALLELISM" --class SyntheticScaleCaps "$SS/caps/target/$JAR" "$INPUT" unused "$sweep" "$M" provenance_ids "$dir" "$manifest" 1000 ;;
    caps:shared)    "$FLINK" run --parallelism "$CAPS_PARALLELISM" --class SyntheticScaleCaps "$SS/caps/target/$JAR" "$INPUT" unused "$sweep" "$M" provenance_shared "$dir" "$manifest" ;;
    green:file)     "$FLINK" run --parallelism "$CAPS_PARALLELISM" --class SyntheticScaleGreen "$SS/ink/target/$JAR" "$INPUT" "$dir/green.txt" "$sweep" "$M" ;;
    green:discard)  "$FLINK" run --parallelism "$CAPS_PARALLELISM" --class SyntheticScaleGreen "$SS/ink/target/$JAR" "$INPUT" unused "$sweep" "$M" discard "$dir" ;;
    green:shared)   "$FLINK" run --parallelism "$CAPS_PARALLELISM" --class SyntheticScaleGreen "$SS/ink/target/$JAR" "$INPUT" unused "$sweep" "$M" provenance_shared "$dir" "$manifest" ;;
    genealog:file)  gl_run "$sweep" "$M" "$dir/gl_stats" --traversalBenchDir "$dir" --traversalDumpFile "$dir/genealog.txt" ;;
    genealog:discard) gl_run "$sweep" "$M" "$dir/gl_stats" --cardinalityDiscard "$dir" ;;
    genealog:shared) gl_run "$sweep" "$M" "$dir/gl_stats" --traversalBenchDir "$dir" --traversalQueryIds "$manifest" ;;
    *) echo "no such job: $method $kind" >&2; return 1 ;;
  esac
}

# ---- units: run under the lock, one fresh cluster each ----
unit_verify() {   # sweep M method
  local sweep=$1 M=$2 method=$3 dir="$WORK/${1}_m$2/verify"
  mkdir -p "$dir"
  fresh
  submit "$method" "$sweep" "$M" file "$dir" > "$dir/${method}_flink.log" 2>&1
}

unit_time() {     # sweep M method rep
  local sweep=$1 M=$2 method=$3 rep=$4 dir="$WORK/${1}_m$2/time_$MODE/${3}_rep$4"
  rm -rf "$dir"; mkdir -p "$dir"
  fresh
  local t0; t0=$(date +%s.%N)
  submit "$method" "$sweep" "$M" discard "$dir" > "$dir/flink.log" 2>&1
  awk -v s="$t0" -v e="$(date +%s.%N)" 'BEGIN{printf "%.3f\n", e-s}' > "$dir/wall_s.txt"
  # The job duration: the "Job Runtime: N ms" line (the job's net runtime, printed by the job itself
  # and by the cluster client). On the cluster the REST JSON is kept as well.
  local ms; ms=$(grep -oE 'Job Runtime: [0-9]+ ms' "$dir/flink.log" | tail -1 | grep -oE '[0-9]+')
  [[ -n $ms && -s "$dir/outputs.txt" ]] || { echo "incomplete run: $dir" >&2; return 1; }
  echo "$ms" > "$dir/job_ms.txt"
  [[ $MODE == local ]] || capture_job_json "$dir/flink.log" "$dir/job.json"
}

unit_fixture() {  # sweep M method   (caps runs the id pre-pass first)
  local sweep=$1 M=$2 method=$3 base="$WORK/${1}_m$2/fixture"
  local manifest="$base/query_ids.txt" dir="$base/$3"
  mkdir -p "$dir"
  fresh
  if [[ $method == caps && ! -s $manifest ]]; then
    mkdir -p "$base/ids"
    submit caps "$sweep" "$M" ids "$base/ids" "$manifest" > "$base/ids/flink.log" 2>&1
    fresh
  fi
  submit "$method" "$sweep" "$M" shared "$dir" "$manifest" > "$dir/flink.log" 2>&1
}

unit_jmh() {      # sweep M
  local sweep=$1 M=$2 base="$WORK/${1}_m$2" dir="$WORK/${1}_m$2/jmh" fx
  mkdir -p "$dir"
  [[ $MODE == local ]] || "$FLINK_DIR/bin/stop-cluster.sh" > /dev/null 2>&1 || true   # no idle JVMs during the measurements
  local method kind
  for spec in caps:total caps:vector green:total green:vector genealog:total; do
    method=${spec%%:*}; kind=${spec##*:}
    [[ -s "$dir/${method}_$kind.csv" ]] && continue
    case $method in caps) fx=caps/query_fixture_caps.ser ;; green) fx=green/query_fixture_green.ser ;;
                    genealog) fx=genealog/query_fixture_genealog.ser ;; esac
    java -jar "$BENCH_JAR" perOutput "$method" "$kind" "$base/fixture/$fx" "$dir/${method}_$kind.csv" \
      > "$dir/${method}_$kind.log" 2>&1
  done
  [[ -s "$dir/temporal.csv" ]] || java -jar "$BENCH_JAR" temporal "$base/verify/caps.txt" "$M" \
    "$TEMPORAL_RANGE_MS" "$dir/temporal.csv" > "$dir/temporal.log" 2>&1
}

# Dispatch of a unit: the script runs itself under the lock (flock -o: the cluster daemons must not
# inherit the lock, or it would stay held after the unit).
if [[ ${1:-} == --unit ]]; then
  shift; fn=$1; shift
  # start cluster -> job -> stop cluster all inside the one lock
  [[ $MODE == local ]] || trap '"$FLINK_DIR/bin/stop-cluster.sh" > /dev/null 2>&1 || true' EXIT
  "$fn" "$@"
  exit 0
fi
locked() { flock -o "$LOCK" "$BASH" "$SELF" --unit "$@"; }

# ---- main ----
plot_only || require_flink
echo "engine: $MODE ($FLINK), lock $LOCK, phases: ${PHASES[*]}"
if [[ ! -s $INPUT ]]; then
  [[ $SMALL == 1 ]] && n=$RECORDS_SMALL k=$KEYS_SMALL || { n=$RECORDS_FULL; k=$KEYS_FULL; }
  "$PYTHON" "$ROOT/data/synthetic_scale/generate_synthetic_scale.py" --records "$n" --keys "$k" --out "$INPUT"
fi
# The jars embed the caps / inkstream / ananke / temporal-index libraries of ~/.m2 at build time.
# Built once and kept (BUILD=1 rebuilds), so that a library under development cannot change a run.
for module in noprov caps ink genealog bench; do
  if ! plot_only && [[ ${BUILD:-0} == 1 || ! -s $SS/$module/target/$([[ $module == bench ]] && echo benchmarks.jar || echo $JAR) ]]; then
    echo "[build $module]"
    (cd "$SS/$module" && mvn -q clean package -DskipTests)
  fi
done
[[ ${FORCE:-0} == 1 ]] && rm -rf "$WORK" "$RES"
mkdir -p "$WORK" "$RES"
echo "input $INPUT ($(wc -l < "$INPUT") records), parallelism $CAPS_PARALLELISM, REPS=$REPS, results in $RES"

failed=0
SWEEPS_ALL=("${SWEEPS[@]}"); plot_only && SWEEPS=()
for sweep in "${SWEEPS[@]}"; do
  for M in "${M_VALUES[@]}"; do
    W="$WORK/${sweep}_m$M"
    echo -e "\n=== $sweep sweep, M=$M ==="

    # 1. verification
    if [[ " ${PHASES[*]} " != *" verify "* ]]; then :
    elif [[ ! -f $W/verify/PASS ]]; then
      rm -f "$W/verify/verify.csv"
      for method in noprov genealog green caps; do   # all four are needed for the cross-check
        [[ -s $W/verify/$method.txt ]] && continue
        echo "  verify run: $method"
        locked unit_verify "$sweep" "$M" "$method"
      done
      if "$PYTHON" "$EXPDIR/verify_scale.py" --dir "$W/verify" --input "$INPUT" --sweep "$sweep" \
           --channels "$M" --parallelism "$CAPS_PARALLELISM" --csv "$W/verify/verify.csv" \
           --stats "$W/verify/stats.json"; then
        touch "$W/verify/PASS"
      else
        echo "  VERIFICATION FAILED at $sweep M=$M: this point is not timed"
        failed=1
        continue
      fi
    else
      echo "  verification already passed"
    fi

    # 2. timing, round robin over the methods
    for ((rep=1; rep<=REPS; rep++)); do
      [[ " ${PHASES[*]} " == *" time "* ]] || break
      [[ -f $W/verify/PASS ]] || { echo "  $sweep M=$M has no passed verification: not timed" >&2; failed=1; break; }
      for method in "${METHODS[@]}"; do
        d="$W/time_$MODE/${method}_rep$rep"
        [[ -s $d/job_ms.txt && -s $d/outputs.txt ]] && continue
        echo "  time: $method rep $rep/$REPS"
        locked unit_time "$sweep" "$M" "$method" "$rep"
      done
    done

    # 3. queries
    [[ " ${PHASES[*]} " == *" queries "* && -f $W/verify/PASS ]] || continue
    for method in caps green genealog; do
      [[ " ${METHODS[*]} " == *" $method "* ]] || continue
      [[ -s $W/fixture/$method/query_fixture_$method.ser ]] && continue
      echo "  fixture: $method"
      locked unit_fixture "$sweep" "$M" "$method"
    done
    echo "  jmh"
    locked unit_jmh "$sweep" "$M"
  done
done

echo -e "\n[summary]"
if ! plot_only || [[ -n $(ls -A "$WORK" 2>/dev/null) ]]; then
  "$PYTHON" "$EXPDIR/summarize_scale.py" --work "$WORK" --out "$RES" --input "$INPUT" \
    --time-dir "time_$MODE" --sweeps "${SWEEPS_ALL[@]}" --m-values "${M_VALUES[@]}" --methods "${METHODS[@]}"
fi
"$PYTHON" "$EXPDIR/plot_scale.py" --res "$RES"
echo -e "\nDone. CSVs and plots in $RES"
exit $failed

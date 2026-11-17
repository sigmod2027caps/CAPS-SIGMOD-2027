#!/usr/bin/env bash
# Nested-path test: 3 levels of 8-way multiplexes (8 + 64 + 512
# Filters, 512 leaf paths p = id mod 512), recombined by one windowed join. We declare 8, 64 or 512 path
# channels (the channel of a contribution is that of its longest declared prefix: p mod M) and report
# runtime / No Prov, retained MB, and the per-output query time.
#
# Dataflow (identical for every M and both methods):
#   Source -> 8-way Filter -> 8-way Filter -> 8-way Filter -> Map(tag) -> Union(leaves with p mod 8 in 0..3 | 4..7)
#          -> keyBy -> 60 s tumbling window -> Agg(count, sum) -> Stamp   (left and right)
#          -> windowed Join on key in the same 60 s window (counts and sums add, counter vectors add) -> Stamp -> Sink
# Same input and window as channel_scaling. Methods: noprov (baseline, independent of M) and caps (dense long[M]).
#
# 1. verification (before any timing): verify_nested.py recomputes the join result from the input
#    (only (key, window) pairs with records on both sides) and checks noprov and caps against it. A failing M is not timed.
# 2. timing: REPS submissions per method on a fresh cluster, round robin, discard sink; Flink "Job Runtime".
#    noprov is timed once per repetition and is the denominator for every M.
# 3. per-output queries: ~1,000 sampled outputs, JMH times total / vector queries (dense CAPS), cluster stopped.
# Retained meta-data is analytical: 8 B x outputs x M. Every Flink unit runs under the machine lock.
#
# Usage:  experiments/nested_paths/run.sh
#         SMALL=1 experiments/nested_paths/run.sh               # 10% input, quick check
#         NESTED_M_LIST="8 64" experiments/nested_paths/run.sh  # a subset
#         PHASES=plot experiments/nested_paths/run.sh           # only summary and plot
set -euo pipefail

# ---- parameters: comment lines in or out ----
METHODS=(
  noprov
  caps
)
M_VALUES=(
  8
  64
  512
)
JMH_WARMUP="${JMH_WARMUP:-10}"     # iterations of 1 s
JMH_MEASURE="${JMH_MEASURE:-5}"
# ---------------------------------------------
[[ -n ${NESTED_M_LIST:-} ]] && read -ra M_VALUES <<< "$NESTED_M_LIST"   # 8, 64 and/or 512

EXPDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SELF="$EXPDIR/$(basename "${BASH_SOURCE[0]}")"
source "$EXPDIR/../common.sh"
LOCK=/tmp/caps-flink.lock
SS="$ROOT/code/synthetic_scale"
JAR="$SS/prefix/target/synthetic-prefix-1.0-REV-SNAPSHOT.jar"
BENCH="$SS/prefix_bench/target/benchmarks.jar"
INPUT="$DATA_ROOT/synthetic_scale/synthetic_scale.txt"
TAG=$([[ $SMALL == 1 ]] && echo small || echo full)_p$CAPS_PARALLELISM
WORK="$EXPDIR/work/$TAG"
RES="$EXPDIR/results"; [[ $SMALL == 1 ]] && RES="$EXPDIR/results/small"
RECORDS_SMALL=200000
KEYS_SMALL=100
export CAPS_PARALLELISM JMH_WARMUP JMH_MEASURE

declare -A CLASS=([noprov]=NestedNoprov [caps]=NestedCaps)

# submit <method> <M> <mode...>: mode = file <out> | discard <dir> | sample <dir> <period>
submit() {
  local method=$1 M=$2 mode=$3; shift 3
  case $mode in
    file)    "$FLINK" run --parallelism "$CAPS_PARALLELISM" --class "${CLASS[$method]}" "$JAR" "$INPUT" "$1" "$M" file ;;
    discard) "$FLINK" run --parallelism "$CAPS_PARALLELISM" --class "${CLASS[$method]}" "$JAR" "$INPUT" unused "$M" discard "$1" ;;
    sample)  "$FLINK" run --parallelism "$CAPS_PARALLELISM" --class "${CLASS[$method]}" "$JAR" "$INPUT" unused "$M" sample "$1" "$2" ;;
  esac
}

# ---- units: run under the lock, one fresh cluster each ----
unit_verify() {   # method M ; noprov does not depend on M: one run, M=8 is passed to the job and ignored
  local method=$1 M=$2 d
  [[ $method == noprov ]] && d="$WORK/noprov/verify" || d="$WORK/m$M/verify"
  mkdir -p "$d"; restart_cluster
  submit "$method" "$M" file "$d/$method.txt" > "$d/${method}_flink.log" 2>&1
}

unit_time() {     # method M rep
  local method=$1 M=$2 rep=$3 d
  [[ $method == noprov ]] && d="$WORK/noprov/time/rep$rep" || d="$WORK/m$M/time/caps_rep$rep"
  rm -rf "$d"; mkdir -p "$d"; restart_cluster
  submit "$method" "$M" discard "$d" > "$d/flink.log" 2>&1
  local ms; ms=$(grep -oE 'Job Runtime: [0-9]+ ms' "$d/flink.log" | tail -1 | grep -oE '[0-9]+')
  [[ -n $ms && -s "$d/outputs.txt" ]] || { echo "incomplete run: $d" >&2; return 1; }
  echo "$ms" > "$d/job_ms.txt"
}

unit_sample() {   # M period
  local M=$1 period=$2 d="$WORK/m$1/fixture"; mkdir -p "$d"; restart_cluster
  submit caps "$M" sample "$d" "$period" > "$d/flink.log" 2>&1
}

unit_jmh() {      # M
  local M=$1 d="$WORK/m$1/jmh" kind
  mkdir -p "$d"
  "$FLINK_DIR/bin/stop-cluster.sh" > /dev/null 2>&1 || true   # no idle JVMs during the measurements
  for kind in total vector; do
    [[ -s "$d/caps_$kind.csv" ]] && continue
    java -jar "$BENCH" dense "$kind" "$WORK/m$M/fixture/sample.ser" "$M" "$d/caps_$kind.csv" > "$d/caps_$kind.log" 2>&1
  done
}

if [[ ${1:-} == --unit ]]; then
  shift; fn=$1; shift
  trap '"$FLINK_DIR/bin/stop-cluster.sh" > /dev/null 2>&1 || true' EXIT
  "$fn" "$@"; exit 0
fi
locked() { flock -o "$LOCK" "$BASH" "$SELF" --unit "$@"; }

has_method() { [[ " ${METHODS[*]} " == *" $1 "* ]]; }

failed=0
if ! plot_only; then
  require_flink
  if [[ ! -s $INPUT ]]; then
    [[ $SMALL == 1 ]] && n=$RECORDS_SMALL k=$KEYS_SMALL || { n=2000000; k=1000; }
    "$PYTHON" "$ROOT/data/synthetic_scale/generate_synthetic_scale.py" --records "$n" --keys "$k" --out "$INPUT"
  fi
  # the jars embed the caps library of ~/.m2 at build time; built once and kept (BUILD=1 rebuilds)
  for module in prefix prefix_bench; do
    jar=$([[ $module == prefix ]] && echo "$JAR" || echo "$BENCH")
    if [[ ${BUILD:-0} == 1 || ! -s $jar ]]; then echo "[build $module]"; (cd "$SS/$module" && mvn -q clean package -DskipTests); fi
  done
  mkdir -p "$WORK" "$RES"
  echo "input $INPUT ($(wc -l < "$INPUT") records), parallelism $CAPS_PARALLELISM, REPS=$REPS, work $WORK"

  # 1. verification (noprov once, caps per M)
  if has_method noprov && [[ ! -s $WORK/noprov/verify/noprov.txt ]]; then
    echo "  verify run: noprov"; locked unit_verify noprov "${M_VALUES[0]}"
  fi
  passed=()
  for M in "${M_VALUES[@]}"; do
    W="$WORK/m$M"; echo -e "\n=== verify M=$M ==="
    if [[ ! -f $W/verify/PASS ]]; then
      if has_method caps && [[ ! -s $W/verify/caps.txt ]]; then
        echo "  verify run: caps"; locked unit_verify caps "$M"
      fi
      if "$PYTHON" "$EXPDIR/verify_nested.py" --dir "$W/verify" --noprov "$WORK/noprov/verify/noprov.txt" --input "$INPUT" \
           --channels "$M" --csv "$W/verify/verify.csv" --stats "$W/verify/stats.json"; then touch "$W/verify/PASS"
      else echo "  VERIFICATION FAILED at M=$M: not timed"; failed=1; continue; fi
    else echo "  verification already passed"; fi
    passed+=("$M")
  done

  # 2. timing, round robin over the methods; noprov once per repetition
  for ((rep=1; rep<=REPS; rep++)); do
    if has_method noprov && [[ ${#passed[@]} -gt 0 && ! -s $WORK/noprov/time/rep$rep/job_ms.txt ]]; then
      echo "  time: noprov rep $rep/$REPS"; locked unit_time noprov "${passed[0]}" "$rep"
    fi
    for M in "${passed[@]}"; do
      has_method caps || continue
      [[ -s $WORK/m$M/time/caps_rep$rep/job_ms.txt ]] && continue
      echo "  time: caps M=$M rep $rep/$REPS"; locked unit_time caps "$M" "$rep"
    done
  done

  # 3. per-output queries
  for M in "${passed[@]}"; do
    has_method caps || continue
    W="$WORK/m$M"
    outputs=$(wc -l < "$WORK/m$M/verify/caps.txt"); period=$(( outputs / 1000 )); (( period < 1 )) && period=1
    [[ -s $W/fixture/sample.ser ]] || { echo "  fixture M=$M (every $period-th identity hash)"; locked unit_sample "$M" "$period"; }
    echo "  jmh M=$M"; locked unit_jmh "$M"
  done
fi
echo -e "\n[summary]"
"$PYTHON" "$EXPDIR/summarize_nested.py" --work "$WORK" --out "$RES" --m-values "${M_VALUES[@]}"
"$PYTHON" "$EXPDIR/plot_nested.py" --res "$RES"
echo "Done. CSV and plot in $RES"
exit $failed

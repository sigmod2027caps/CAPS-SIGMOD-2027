#!/usr/bin/env bash
# Channel-cardinality scalability on REAL data: real input, synthetic dataflow on top.
#
#   taxi    data/taxis/taxis.txt (the input of taxi_1, 10M rides): key = taxi id, 1 h tumbling window,
#           channel = pickup zone (1,000 zones) mod M, value = fare
#   taxi_zone  the same input and window; key = pickup zone, channel = taxi id mod M (a real attribute; every
#           output combines the rides of many taxis, so many channels)
#   tweets  data/tweets/twitter.txt (the input of twitter_1, 100K tweets): one record per hashtag,
#           key = hashtag, 30 min tumbling window, channel = hash(location text) mod M ("no location" is
#           a value of its own), value = likes + retweets
#   dataflow  Source (assign channel) -> keyBy(key) -> tumbling window -> Agg(count, sum; average = sum/count)
#             -> Stamp(window end) -> Sink
# Input, window and outputs are identical across M and methods; only the width of the annotation changes with M.
#
# Methods: noprov, GeneaLog, Green (provenance polynomial), CAPS dense (long[M] per tuple), CAPS sparse
# (only the non-zero (channel, count) pairs, SparseHowMuch). GeneaLog and noprov do not depend on M
# (no channels): with MINDEP_ONCE=1 they are run (verified, timed, queried) at the first M only and the
# result is reused for the other M, which draws them flat.
#
# Per (dataset, M):
#   1. verification: every method writes its outputs (and the annotations of ~1,000 sampled outputs, the query
#      fixture); verify_real.py checks them against an oracle recomputed from the input: all methods give the
#      outputs, sparse = dense = Green per channel per output, GeneaLog traversal = sum of the channel counters.
#      A point that fails is not timed.
#   2. timing: REPS submissions per method on a fresh cluster (round robin), discard sink; metric = Flink job runtime.
#   3. per-output queries: JMH (3 s warm-up, 3 s measure) over the sampled outputs: total (a scalar), vector
#      (the M channel counters; CAPS dense, CAPS sparse, Green), pairs (CAPS sparse). GeneaLog: total only.
# Retained meta-data is analytical (summarize_real.py): dense 8 B x M per output; sparse 12 B per stored pair;
# Green 8 B per variable (contributing record); GeneaLog 32 B per operator output (source tuples + window outputs).
# Every Flink unit (fresh cluster, one job, stop) runs under the machine lock (common.sh with_cluster).
#
# Usage:  experiments/channel_scaling_real/run.sh
#         DATASET_LIST="tweets" M_LIST="1 8" experiments/channel_scaling_real/run.sh   # a subset
#         PHASES=plot experiments/channel_scaling_real/run.sh                          # only summary and plots
set -euo pipefail
PLOT_ONLY=0; [[ " ${PHASES:-} " == *" plot "* ]] && PLOT_ONLY=1

# ---- parameters: comment lines in or out ----
DATASETS=(
  taxi
  taxi_zone
#  tweets
)
SUMMARY_DATASETS=(
  taxi
  taxi_zone
)
METHODS=(
  noprov
  genealog
  green
  caps
  caps_sparse
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
)   # the values of Figure 11
REPS="${REPS:-3}"
JMH_WARMUP="${JMH_WARMUP:-3}"      # iterations of 1 s
JMH_MEASURE="${JMH_MEASURE:-3}"
MINDEP_ONCE="${MINDEP_ONCE:-1}"    # noprov and GeneaLog do not depend on M: run them at the first M only
# ---------------------------------------------
[[ -n ${M_LIST:-} ]] && read -ra M_VALUES <<< "$M_LIST"
[[ -n ${DATASET_LIST:-} ]] && read -ra DATASETS <<< "$DATASET_LIST"

EXPDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$EXPDIR/../common.sh"
RS="$ROOT/code/real_scale"
JAR="$RS/jobs/target/real-scale-1.0-REV-SNAPSHOT.jar"
BENCH="$RS/bench/target/benchmarks.jar"
WORK="$EXPDIR/work"
RES="$EXPDIR/results"
if [[ $SMALL == 1 ]]; then WORK="$EXPDIR/work_small"; RES="$EXPDIR/results_small"; fi   # never over the full results
export CAPS_PARALLELISM

declare -A INPUT=([taxi]="$DATA_ROOT/taxis/taxis.txt" [taxi_zone]="$DATA_ROOT/taxis/taxis.txt" [tweets]="$DATA_ROOT/tweets/twitter.txt")
declare -A CLASS=([noprov]=RealNoprov [genealog]=RealGL [green]=RealGreen [caps]=RealCaps [caps_sparse]=RealSparse)
mindep() { [[ $MINDEP_ONCE == 1 && ( $1 == noprov || $1 == genealog ) ]]; }

# submit <dataset> <method> <M> <output file> <mode> [args]: mode = file [sampleDir period] | discard dir
submit() {
  local ds=$1 method=$2 M=$3 out=$4 mode=$5; shift 5
  "$FLINK" run --parallelism "$CAPS_PARALLELISM" --class "realscale.${CLASS[$method]}" "$JAR" \
    "$ds" "${INPUT[$ds]}" "$out" "$M" "$mode" "$@"
}

# units (run inside with_cluster: fresh cluster, exactly one job)
unit_verify() { local ds=$1 M=$2 method=$3 period=$4 d="$WORK/$1_m$2/verify" f="$WORK/$1_m$2/fixture/$3"; mkdir -p "$d" "$f"
  if [[ $method == noprov ]]; then submit "$ds" "$method" "$M" "$d/$method.txt" file > "$d/${method}_flink.log" 2>&1
  else submit "$ds" "$method" "$M" "$d/$method.txt" file "$f" "$period" > "$d/${method}_flink.log" 2>&1; fi
  [[ $method == noprov || -s $f/sample.ser ]] || { echo "no fixture written: $d/${method}_flink.log" >&2; return 1; }; }

unit_time() { local ds=$1 M=$2 method=$3 rep=$4 d="$WORK/$1_m$2/time/${3}_rep$4"; rm -rf "$d"; mkdir -p "$d"
  submit "$ds" "$method" "$M" unused discard "$d" > "$d/flink.log" 2>&1
  local ms; ms=$(grep -oE 'Job Runtime: [0-9]+ ms' "$d/flink.log" | tail -1 | grep -oE '[0-9]+')
  [[ -n $ms && -s "$d/outputs.txt" ]] || { echo "incomplete run: $d" >&2; return 1; }
  echo "$ms" > "$d/job_ms.txt"; }

# JMH over the fixture of a method, one CSV per (method, kind); no Flink job runs meanwhile
jmh() { local ds=$1 M=$2 d="$WORK/$1_m$2/jmh" spec method kind jm
  mkdir -p "$d"
  for spec in ${JMH_SPECS:-genealog:total green:total green:vector caps:total caps:vector caps_sparse:total caps_sparse:vector caps_sparse:pairs}; do   # JMH_FORKS: forks per cell (default 1)
    method=${spec%%:*}; kind=${spec##*:}
    [[ -s "$d/${method}_$kind.csv" ]] && continue
    jm=$method; [[ $method == caps ]] && jm=dense; [[ $method == caps_sparse ]] && jm=sparse
    # -Xss1g: a GeneaLog fixture is a long linked list of source tuples (~100 per output on taxi_zone) that Java deserialization
    # walks recursively; JMH passes it on to its forks. The stack size does not change the speed of a query.
    flock -o "$LOCK_FILE" java -Xss1g -jar "$BENCH" "$jm" "$kind" "$WORK/${ds}_m$M/fixture/$method/sample.ser" "$M" "$d/${method}_$kind.csv" \
      > "$d/${method}_$kind.log" 2>&1
  done; }

failed=0
if [[ $PLOT_ONLY == 0 ]]; then
  require_flink
  if [[ ${BUILD:-0} == 1 || ! -s $JAR || ! -s $BENCH ]]; then
    # the jobs and the JMH bench embed the CAPS, Green and GeneaLog libraries: install those first
    DATAFLOWS="" build_all --with-competitors
    echo "[build real_scale]"
    mvn_module "$RS/jobs" package
    mvn_module "$RS/bench" package
  fi
  for f in "$JAR" "$BENCH"; do [[ -s $f ]] || { echo "missing $f" >&2; exit 1; }; done
  mkdir -p "$WORK" "$RES"
  first=${M_VALUES[0]}
  for ds in "${DATASETS[@]}"; do
    [[ -s ${INPUT[$ds]} ]] || { echo "missing input ${INPUT[$ds]}" >&2; exit 1; }
    [[ -s $WORK/${ds}_outputs.txt ]] || "$PYTHON" "$EXPDIR/verify_real.py" --dataset "$ds" --input "${INPUT[$ds]}" --outputs > "$WORK/${ds}_outputs.txt"
    outputs=$(cat "$WORK/${ds}_outputs.txt"); period=$(( outputs / 1000 )); (( period < 1 )) && period=1
    for M in "${M_VALUES[@]}"; do
      W="$WORK/${ds}_m$M"; echo -e "\n=== $ds, M=$M ($outputs outputs, fixture period $period) ==="
      mkdir -p "$W/verify" "$W/fixture" "$W/time"
      # 1. verification (+ fixtures)
      if [[ ! -f $W/verify/PASS ]]; then
        for method in "${METHODS[@]}"; do
          if mindep "$method" && [[ $M != "$first" ]]; then   # reuse the first M's outputs and fixture
            [[ -s $W/verify/$method.txt ]] || { ln -sfn "$WORK/${ds}_m$first/verify/$method.txt" "$W/verify/$method.txt"; ln -sfn "$WORK/${ds}_m$first/fixture/$method" "$W/fixture/$method"; }
            continue
          fi
          [[ -s $W/verify/$method.txt ]] && continue
          echo "  verify run: $method"
          # a GeneaLog fixture (~100 source tuples per output on taxi_zone) is a long linked list: serializing it recurses deeply
          # in the sink, so the (fresh) cluster of this one run gets a large thread stack (a data-capture run, not a timed one)
          if [[ $method == genealog ]]; then JAVA_TOOL_OPTIONS="-Xss256m" with_cluster unit_verify "$ds" "$M" "$method" "$period"
          else with_cluster unit_verify "$ds" "$M" "$method" "$period"; fi
        done
        if "$PYTHON" "$EXPDIR/verify_real.py" --dataset "$ds" --input "${INPUT[$ds]}" --channels "$M" --dir "$W/verify" \
             --csv "$W/verify/verify.csv" --stats "$W/verify/stats.json"; then touch "$W/verify/PASS"
        else echo "  VERIFICATION FAILED at $ds M=$M: not timed"; failed=1; continue; fi
      else echo "  verification already passed"; fi
      # 2. timing, round robin over the methods
      for ((rep=1; rep<=REPS; rep++)); do for method in "${METHODS[@]}"; do
        if mindep "$method" && [[ $M != "$first" ]]; then ln -sfn "$WORK/${ds}_m$first/time/${method}_rep$rep" "$W/time/${method}_rep$rep"; continue; fi
        [[ -s $W/time/${method}_rep$rep/job_ms.txt ]] && continue
        echo "  time: $method rep $rep/$REPS"; with_cluster unit_time "$ds" "$M" "$method" "$rep"
      done; done
      # 3. per-output queries
      echo "  jmh"; jmh "$ds" "$M"
    done
  done
fi
echo -e "\n[summary]"
# the summary and the figures cover SUMMARY_DATASETS at every M with a work directory (a run of a subset never drops the others;
# tweets are not part of the revision results)
# Without a work directory (the raw runs are not part of the repository) the figures are redrawn from the CSVs in results/.
if [[ -d $WORK ]]; then
  SUM_M=$(ls "$WORK" | sed -n 's/^.*_m\([0-9]*\)$/\1/p' | sort -un | tr '\n' ' ')
  "$PYTHON" "$EXPDIR/summarize_real.py" --work "$WORK" --out "$RES" --datasets "${SUMMARY_DATASETS[@]}" --m-values $SUM_M --methods "${METHODS[@]}"
fi
"$PYTHON" "$EXPDIR/plot_real.py" --res "$RES"
"$PYTHON" "$EXPDIR/plot_real.py" --res "$RES" --only row    # Figure 11 of the paper: channel_legend.pdf and the four channel_* panels
echo "Done. CSVs and plots in $RES"
exit $failed

#!/usr/bin/env bash
# Query latency against query window length (paper Section 7.4, Figure 9b): the same online indexes as in
# experiments/queries are queried with windows of 1, 10, 100 and 1,000 slides of the dataflow (NUM_QUERIES
# random positions per length, the same windows for every method). The array scan (CAPS without index), the
# traversal of GeneaLog's graphs and the projection of Green's polynomials grow with the number of outputs in
# the window; the prefix-sum index of CAPS answers with two B-tree descents and a subtraction regardless of the
# range. Verification (aggregate_queries.py --sweep): Green equals CAPS in every channel, GeneaLog's count equals
# the sum over the channels, for every query of every length. Repeated REPS times; median and min-max.
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
  genealog
  green
  caps        # CAPS (no index) and CAPS
)
# REPS=3        # repetitions of the whole sequence (default 3)
# NUM_QUERIES=1000
# SMALL=1       # the 10% subsets (a smoke test, not numbers); the results go to small/ and never overwrite the full ones
# CAPS_PARALLELISM=1

EXPDIR="$(cd "$(dirname "$0")" && pwd)"
if [[ -n "${DATAFLOWS:-}" ]]; then DATAFLOW_LIST=($DATAFLOWS); fi
DATAFLOWS="${DATAFLOW_LIST[*]}"
source "$(cd "$(dirname "$0")/.." && pwd)/common.sh"
RESDIR="$(results_dir "$EXPDIR")"   # SMALL=1 writes into small/ and never overwrites the full-data results
mkdir -p "$RESDIR"

has_method() { [[ " ${METHODS[*]} " == *" $1 "* ]]; }

if ! plot_only; then
  require_flink
  for dataflow in $DATAFLOWS; do require_inputs "$dataflow"; done
  build_all --with-competitors
fi

# PHASES=plot: verify and aggregate the repetitions that are already there, then redraw the figure.
if plot_only; then
  for dataflow in $DATAFLOWS; do
    "$PYTHON" "$EXPDIR/../aggregate_queries.py" --dir "$RESDIR/$dataflow" --channels "${CHANNELS[$dataflow]}" --sweep
  done
  combine_results "$RESDIR" query_length_latency.csv verification.csv
  plot_experiment "$RESDIR"
  exit 0
fi

for dataflow in $DATAFLOWS; do
  echo -e "\n=== $dataflow query latency vs window length, REPS=$REPS ==="
  outdir="$RESDIR/$dataflow"
  rm -rf "$outdir"
  lens=${QUERY_LENS_MS[$dataflow]}

  for ((rep=1; rep<=REPS; rep++)); do
    rundir="$outdir/rep$rep"
    mkdir -p "$rundir/genealog"
    echo "  --- rep $rep/$REPS ---"
    if has_method caps; then
      with_cluster run_caps "$dataflow" querylen "$rundir" "$NUM_QUERIES" "$lens" > "$rundir/flink.log" 2>&1
    fi
    if has_method green; then
      mkdir -p "$rundir/green"
      with_cluster run_green "$dataflow" querylen "$rundir" "$NUM_QUERIES" "$lens" > "$rundir/green/flink.log" 2>&1
    fi
    if has_method genealog; then
      with_cluster run_genealog "$dataflow" "$rundir/genealog" list \
        --indexBenchDir "$rundir" --indexNumQueries "$NUM_QUERIES" --indexQueryLensMs "$lens" \
        > "$rundir/genealog/flink.log" 2>&1
    fi
  done

  "$PYTHON" "$EXPDIR/../aggregate_queries.py" --dir "$outdir" --channels "${CHANNELS[$dataflow]}" --sweep
  cat "$outdir/query_length_latency.csv"
done

combine_results "$RESDIR" query_length_latency.csv verification.csv

echo -e "\nquery_length_latency.csv:"; cat "$RESDIR/query_length_latency.csv"
echo -e "\n[figures]"
plot_experiment "$RESDIR"

echo -e "\nDone. Outputs in $RESDIR/"

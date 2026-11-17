#!/usr/bin/env bash
# Temporal query latency (paper Section 7.4, Figure 10): the sink builds its index ONLINE while the dataflow
# runs and, at end of stream, answers NUM_QUERIES how-much and NUM_QUERIES versioning queries over windows of
# ten slides at random positions. Four methods, the same windows, the same seeds:
#   CAPS (no index)  every output's counters in an array in timestamp order, a query scans the range
#   CAPS             the two-tier index (Tier 1 holds what the watermark has not certified, Tier 2 is the
#                    append-only prefix-sum index; one shard per sink subtask at CAPS_PARALLELISM > 1)
#   Green            the polynomial of every output in a timestamp-ordered B-tree behind the same Tier 1; a
#                    query locates the range and projects the polynomial of each output per channel and sums
#   GeneaLog         the output tuples in the same B-tree, their provenance graphs kept in memory; a query
#                    walks the graph of every output in the range (on these dataflows a few nodes and the sizes of the
#                    provenance sets that GeneaLog's serializer built at the last keyBy: about 50 ns per output on taxi_2)
# The two CAPS variants are one job (the scan baseline sits behind the same Tier 1, so both index the same outputs).
# Verification (aggregate_queries.py): Green's answers equal CAPS's in every channel, GeneaLog's traversal count
# equals the sum over the channels, query by query. The whole sequence is repeated REPS times (fresh cluster per
# job); the CSV and the figure show the median and min-max.
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

# This experiment reports latencies, not job runtimes, and its insert latency (insert_latency.csv) needs the nanoTime
# brackets around the Tier-1 insert and split. They are off by default in every timed run (2 clock reads per record
# cost about 40 ns in the end-to-end overhead of the index), so this is the explicit, separate insert-timing phase.
export CAPS_INSERT_TIMING=1

has_method() { [[ " ${METHODS[*]} " == *" $1 "* ]]; }

if ! plot_only; then
  require_flink
  for dataflow in $DATAFLOWS; do require_inputs "$dataflow"; done
  build_all --with-competitors
fi

# PHASES=plot: verify and aggregate the repetitions that are already there, then redraw the figure.
if plot_only; then
  for dataflow in $DATAFLOWS; do
    "$PYTHON" "$EXPDIR/../aggregate_queries.py" --dir "$RESDIR/$dataflow" --channels "${CHANNELS[$dataflow]}"
  done
  combine_results "$RESDIR" query_latency.csv insert_latency.csv index_stats.csv verification.csv
  plot_experiment "$RESDIR"
  exit 0
fi

for dataflow in $DATAFLOWS; do
  echo -e "\n=== $dataflow temporal queries (online index), REPS=$REPS ==="
  outdir="$RESDIR/$dataflow"
  rm -rf "$outdir"
  len=${QUERY_LEN_MS[$dataflow]}

  for ((rep=1; rep<=REPS; rep++)); do
    rundir="$outdir/rep$rep"
    mkdir -p "$rundir/genealog"
    echo "  --- rep $rep/$REPS ---"
    if has_method caps; then
      with_cluster run_caps "$dataflow" queries "$rundir" "$NUM_QUERIES" "$len" > "$rundir/flink.log" 2>&1
    fi
    if has_method green; then
      mkdir -p "$rundir/green"
      with_cluster run_green "$dataflow" queries "$rundir" "$NUM_QUERIES" "$len" > "$rundir/green/flink.log" 2>&1
    fi
    if has_method genealog; then
      with_cluster run_genealog "$dataflow" "$rundir/genealog" list \
        --indexBenchDir "$rundir" --indexNumQueries "$NUM_QUERIES" --indexQueryLenMs "$len" \
        > "$rundir/genealog/flink.log" 2>&1
    fi
  done

  "$PYTHON" "$EXPDIR/../aggregate_queries.py" --dir "$outdir" --channels "${CHANNELS[$dataflow]}"
  cat "$outdir/query_latency.csv"
done

combine_results "$RESDIR" query_latency.csv insert_latency.csv index_stats.csv verification.csv

echo -e "\nquery_latency.csv:";  cat "$RESDIR/query_latency.csv"
echo -e "\ninsert_latency.csv:"; cat "$RESDIR/insert_latency.csv"
echo -e "\nindex_stats.csv:";    cat "$RESDIR/index_stats.csv"
echo -e "\n[figures]"
plot_experiment "$RESDIR"

echo -e "\nDone. Outputs in $RESDIR/"

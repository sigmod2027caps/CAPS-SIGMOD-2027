#!/usr/bin/env bash
# Temporal query latency (paper Section 7.2.2): the sink builds the index
# ONLINE while the dataflow runs, inserting the meta-data of every output
# through a reorder buffer that restores global timestamp order. At end of
# stream it answers NUM_QUERIES how-much and NUM_QUERIES versioning queries
# against the plain array (scan) and against the prefix-sum index, timing each
# separately and cross-checking that the answers agree.
#
# GeneaLog does not support temporal how-much queries, so it is extended in the
# most favourable way: its outputs go into the same B-tree keyed by timestamp,
# with their provenance graphs kept in memory, and a query traverses the graphs
# of every output in the window.
set -euo pipefail

EXPDIR="$(cd "$(dirname "$0")" && pwd)"
source "$(cd "$(dirname "$0")/.." && pwd)/common.sh"

require_flink
for dataflow in $DATAFLOWS; do require_inputs "$dataflow"; done
build_all --with-competitors

rm -f "$EXPDIR"/{query_latency.csv,insert_latency.csv,index_stats.csv}

for dataflow in $DATAFLOWS; do
  echo -e "\n=== $dataflow temporal queries (online index) ==="
  outdir="$EXPDIR/$dataflow"
  mkdir -p "$outdir/genealog"
  rm -f "$outdir"/{query_latency.csv,insert_latency.csv,index_stats.csv}
  rm -f "$outdir/query_latency_genealog.csv"
  rm -rf "$outdir/genealog"/*

  restart_cluster
  run_caps "$dataflow" queries "$outdir" "$NUM_QUERIES" "${QUERY_LEN_MS[$dataflow]}" \
    > "$outdir/flink.log" 2>&1

  echo "  [genealog: same queries over the contribution graphs]"
  restart_cluster
  run_genealog "$dataflow" "$outdir/genealog" list \
    --indexBenchDir "$outdir" --indexNumQueries "$NUM_QUERIES" \
    --indexQueryLenMs "${QUERY_LEN_MS[$dataflow]}" \
    > "$outdir/genealog/flink.log" 2>&1
  tail -n +2 "$outdir/query_latency_genealog.csv" >> "$outdir/query_latency.csv"
  rm -f "$outdir/query_latency_genealog.csv"

  for name in query_latency insert_latency index_stats; do
    append_combined "$dataflow" "$outdir/$name.csv" "$EXPDIR/$name.csv"
  done
  cat "$outdir/query_latency.csv"
done

echo -e "\nquery_latency.csv:";  cat "$EXPDIR/query_latency.csv"
echo -e "\ninsert_latency.csv:"; cat "$EXPDIR/insert_latency.csv"
echo -e "\nindex_stats.csv:";    cat "$EXPDIR/index_stats.csv"
echo -e "\n[figures]"
plot_experiment "$EXPDIR"

echo -e "\nDone. Outputs in $EXPDIR/"

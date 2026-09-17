#!/usr/bin/env bash
# Query latency against query window length (paper Section 7.2.2): the same
# online index is queried with windows of growing length. The array scan grows
# with the number of records in range, the prefix-sum index does not, since it
# answers with two B-tree descents and a subtraction regardless of the range.
set -euo pipefail

EXPDIR="$(cd "$(dirname "$0")" && pwd)"
source "$(cd "$(dirname "$0")/.." && pwd)/common.sh"

require_flink
for dataflow in $DATAFLOWS; do require_inputs "$dataflow"; done
build_all --with-competitors

rm -f "$EXPDIR/query_length_latency.csv"

for dataflow in $DATAFLOWS; do
  echo -e "\n=== $dataflow query latency vs window length ==="
  outdir="$EXPDIR/$dataflow"
  mkdir -p "$outdir/genealog"
  rm -f "$outdir"/{query_length_latency.csv,query_length_answers.csv}
  rm -f "$outdir"/{query_length_latency_genealog.csv,query_length_answers_genealog.csv}
  rm -rf "$outdir/genealog"/*

  restart_cluster
  run_caps "$dataflow" querylen "$outdir" "$NUM_QUERIES" "${QUERY_LENS_MS[$dataflow]}" \
    > "$outdir/flink.log" 2>&1

  echo "  [genealog: same sweep over the contribution graphs]"
  restart_cluster
  run_genealog "$dataflow" "$outdir/genealog" list \
    --indexBenchDir "$outdir" --indexNumQueries "$NUM_QUERIES" \
    --indexQueryLensMs "${QUERY_LENS_MS[$dataflow]}" \
    > "$outdir/genealog/flink.log" 2>&1
  tail -n +2 "$outdir/query_length_latency_genealog.csv" \
    >> "$outdir/query_length_latency.csv"
  rm -f "$outdir/query_length_latency_genealog.csv"

  append_combined "$dataflow" "$outdir/query_length_latency.csv" \
    "$EXPDIR/query_length_latency.csv"
  cat "$outdir/query_length_latency.csv"
done

echo -e "\nquery_length_latency.csv:"; cat "$EXPDIR/query_length_latency.csv"
echo -e "\n[figures]"
plot_experiment "$EXPDIR"

echo -e "\nDone. Outputs in $EXPDIR/"

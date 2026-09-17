#!/usr/bin/env bash
# Expiry (paper Section 7.2.3): the sink feeds two live indexes, one that never
# expires and one trimmed to a 7-day horizon. Every 6 hours of event time the
# trimmed index drops everything older than the horizon and the memory of both
# is sampled. At end of stream, in-horizon queries are answered on both indexes
# and cross-checked, showing that expiry bounds memory at no query cost.
set -euo pipefail

EXPDIR="$(cd "$(dirname "$0")" && pwd)"
source "$(cd "$(dirname "$0")/.." && pwd)/common.sh"

require_flink
for dataflow in $DATAFLOWS; do require_inputs "$dataflow"; done
build_all

rm -f "$EXPDIR"/{expiry_stats.csv,expiry_memory.csv}

for dataflow in $DATAFLOWS; do
  echo -e "\n=== $dataflow expiry (online index, 7-day horizon) ==="
  outdir="$EXPDIR/$dataflow"
  mkdir -p "$outdir"
  rm -f "$outdir"/{expiry_memory.csv,expiry_stats.csv}

  restart_cluster

  run_caps "$dataflow" expiry "$outdir" "$HORIZON_MS" "${QUERY_LEN_MS[$dataflow]}" \
    > "$outdir/flink.log" 2>&1

  for name in expiry_stats expiry_memory; do
    append_combined "$dataflow" "$outdir/$name.csv" "$EXPDIR/$name.csv"
  done
  cat "$outdir/expiry_stats.csv"
done

echo -e "\nexpiry_stats.csv:"; cat "$EXPDIR/expiry_stats.csv"

# No figure: the paper reports this experiment as Table 3.
echo -e "\nDone. Outputs in $EXPDIR/"

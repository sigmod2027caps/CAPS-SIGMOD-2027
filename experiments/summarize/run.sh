#!/usr/bin/env bash
# Summarization (paper Section 7.2.4): storage against estimation error. One
# live index per bucket width is maintained while the stream runs, and each
# time the 7-day horizon crosses into a new bucket the records behind it are
# compressed into that bucket of a second, append-only B-tree. At end of stream
# the summarized region is queried against an exact index rebuilt from the raw
# records, which gives the relative error of each width.
set -euo pipefail

EXPDIR="$(cd "$(dirname "$0")" && pwd)"
source "$(cd "$(dirname "$0")/.." && pwd)/common.sh"

require_flink
for dataflow in $DATAFLOWS; do require_inputs "$dataflow"; done
build_all

rm -f "$EXPDIR/summarize.csv"

for dataflow in $DATAFLOWS; do
  echo -e "\n=== $dataflow summarize (storage vs estimation error) ==="
  outdir="$EXPDIR/$dataflow"
  mkdir -p "$outdir"
  rm -f "$outdir/summarize.csv"

  restart_cluster

  run_caps "$dataflow" summarize "$outdir" "$HORIZON_MS" "$WIDTHS_MS" \
    > "$outdir/flink.log" 2>&1

  append_combined "$dataflow" "$outdir/summarize.csv" "$EXPDIR/summarize.csv"
  cat "$outdir/summarize.csv"
done

echo -e "\nsummarize.csv:"; cat "$EXPDIR/summarize.csv"

# No figure: the paper reports this experiment as Table 4.
echo -e "\nDone. Outputs in $EXPDIR/"

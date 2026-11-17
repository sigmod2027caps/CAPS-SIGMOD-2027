#!/usr/bin/env bash
# Summarization (paper Section 7.8, Table 4): storage against estimation error. One live index per bucket
# width is maintained while the stream runs, and each time the 7-day horizon crosses into a new bucket the
# records behind it are compressed into that bucket of a second, append-only B-tree. At end of stream the
# summarized region is queried against an exact index rebuilt from the raw records, which gives the relative
# error of each width. Repeated REPS times; the table shows the median (sizes and errors are deterministic, the
# latencies are not).
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
# REPS=3        # repetitions (default 3)
# SMALL=1       # the 10% subsets (a smoke test, not numbers); the results go to small/ and never overwrite the full ones

EXPDIR="$(cd "$(dirname "$0")" && pwd)"
if [[ -n "${DATAFLOWS:-}" ]]; then DATAFLOW_LIST=($DATAFLOWS); fi
DATAFLOWS="${DATAFLOW_LIST[*]}"
source "$(cd "$(dirname "$0")/.." && pwd)/common.sh"
RESDIR="$(results_dir "$EXPDIR")"   # SMALL=1 writes into small/ and never overwrites the full-data results
mkdir -p "$RESDIR"

if ! plot_only; then
  require_flink
  for dataflow in $DATAFLOWS; do require_inputs "$dataflow"; done
  build_all
fi

# PHASES=plot: aggregate the repetitions that are already there, then rebuild the table.
if plot_only; then
  for dataflow in $DATAFLOWS; do
    "$PYTHON" "$EXPDIR/../aggregate_reps.py" --dir "$RESDIR/$dataflow" --file summarize.csv --keys bucket_ms \
      --spread avg_summarize_us,avg_query_us
  done
  combine_results "$RESDIR" summarize.csv
  "$PYTHON" "$EXPDIR/../tables.py" summarize "$RESDIR"
  exit 0
fi

for dataflow in $DATAFLOWS; do
  echo -e "\n=== $dataflow summarize (storage vs estimation error), REPS=$REPS ==="
  outdir="$RESDIR/$dataflow"
  rm -rf "$outdir"

  for ((rep=1; rep<=REPS; rep++)); do
    rundir="$outdir/rep$rep"
    mkdir -p "$rundir"
    with_cluster run_caps "$dataflow" summarize "$rundir" "$HORIZON_MS" "$WIDTHS_MS" \
      > "$rundir/flink.log" 2>&1
    # the exact baseline (bucket 0) must have zero error
    awk -F, 'NR>1 && $1 == 0 && ($12 != 0 || $13 != 0) {bad=1} END{exit bad}' "$rundir/summarize.csv" || {
      echo "summarize: the exact baseline has an error in $rundir" >&2; exit 1; }
  done
  "$PYTHON" "$EXPDIR/../aggregate_reps.py" --dir "$outdir" --file summarize.csv --keys bucket_ms \
    --spread avg_summarize_us,avg_query_us
  cat "$outdir/summarize.csv"
done

combine_results "$RESDIR" summarize.csv
echo -e "\nsummarize.csv:"; cat "$RESDIR/summarize.csv"

# The paper reports this experiment as Table 4: a CSV and a LaTeX tabular.
echo -e "\n[table]"
"$PYTHON" "$EXPDIR/../tables.py" summarize "$RESDIR"

echo -e "\nDone. Outputs in $RESDIR/"

#!/usr/bin/env bash
# Expiry (paper Section 7.8, Table 4): the sink feeds two live indexes, one that never expires and one trimmed
# to a 7-day horizon. Every 6 hours of event time the trimmed index drops everything older than the horizon and
# the memory of both is sampled. At end of stream, in-horizon queries are answered on both indexes and
# cross-checked (the job fails on a mismatch; mismatches=0 is asserted below as well), showing that expiry
# bounds memory at no query cost. Repeated REPS times; the table shows the median.
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
    "$PYTHON" "$EXPDIR/../aggregate_reps.py" --dir "$RESDIR/$dataflow" --file expiry_stats.csv --keys horizon_ms \
      --spread avg_expiry_us,full_query_us,expiring_query_us
    "$PYTHON" "$EXPDIR/../aggregate_reps.py" --dir "$RESDIR/$dataflow" --file expiry_memory.csv --copy-first
  done
  combine_results "$RESDIR" expiry_stats.csv expiry_memory.csv
  "$PYTHON" "$EXPDIR/../tables.py" expiry "$RESDIR"
  exit 0
fi

for dataflow in $DATAFLOWS; do
  echo -e "\n=== $dataflow expiry (online index, 7-day horizon), REPS=$REPS ==="
  outdir="$RESDIR/$dataflow"
  rm -rf "$outdir"

  for ((rep=1; rep<=REPS; rep++)); do
    rundir="$outdir/rep$rep"
    mkdir -p "$rundir"
    with_cluster run_caps "$dataflow" expiry "$rundir" "$HORIZON_MS" "${QUERY_LEN_MS[$dataflow]}" \
      > "$rundir/flink.log" 2>&1
    awk -F, 'NR>1 && $7 != 0 {bad=1} END{exit bad}' "$rundir/expiry_stats.csv" || {
      echo "expiry: in-horizon answers differ between the indexes in $rundir" >&2; exit 1; }
  done
  "$PYTHON" "$EXPDIR/../aggregate_reps.py" --dir "$outdir" --file expiry_stats.csv --keys horizon_ms \
    --spread avg_expiry_us,full_query_us,expiring_query_us
  "$PYTHON" "$EXPDIR/../aggregate_reps.py" --dir "$outdir" --file expiry_memory.csv --copy-first
  cat "$outdir/expiry_stats.csv"
done

combine_results "$RESDIR" expiry_stats.csv expiry_memory.csv
echo -e "\nexpiry_stats.csv:"; cat "$RESDIR/expiry_stats.csv"

# The paper reports this experiment in Table 4: a CSV and a LaTeX tabular.
echo -e "\n[table]"
"$PYTHON" "$EXPDIR/../tables.py" expiry "$RESDIR"

echo -e "\nDone. Outputs in $RESDIR/"

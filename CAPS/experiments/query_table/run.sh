#!/usr/bin/env bash
# Provenance query latency table: JMH over the same 1,000 shared output
# identifiers for CAPS, Green-polynomial, and GeneaLog.
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
# SMALL=1       # the 10% subsets (a smoke test, not numbers); the results go to small/ and never overwrite the full ones

EXPDIR="$(cd "$(dirname "$0")" && pwd)"
if [[ -n "${DATAFLOWS:-}" ]]; then DATAFLOW_LIST=($DATAFLOWS); fi
DATAFLOWS="${DATAFLOW_LIST[*]}"
source "$(cd "$(dirname "$0")/.." && pwd)/common.sh"
RESDIR="$(results_dir "$EXPDIR")"   # SMALL=1 writes into small/ and never overwrites the full-data results
mkdir -p "$RESDIR"

REQUIRED_IDS=1000
JMH_JAR="$ROOT/code/query_jmh/target/benchmarks.jar"

if ! plot_only; then
  require_flink
  for dataflow in $DATAFLOWS; do require_inputs "$dataflow"; done
  build_all --with-competitors --with-jmh
  if [[ ! -s "$JMH_JAR" ]]; then
    echo "missing JMH jar: $JMH_JAR" >&2
    exit 1
  fi
fi

run_jmh() {
  local method=$1 outdir=$2 fixture csv
  case "$method" in
    caps) fixture="$outdir/caps/query_fixture_caps.ser" ;;
    green) fixture="$outdir/green/query_fixture_green.ser" ;;
    genealog) fixture="$outdir/genealog/query_fixture_genealog.ser" ;;
    *) echo "unknown method: $method" >&2; return 1 ;;
  esac
  if [[ ! -s "$fixture" ]]; then
    echo "missing query fixture: $fixture" >&2
    return 1
  fi
  csv="$outdir/$method/jmh_query.csv"
  rm -f "$csv"
  # JMH needs the machine to itself: under the same lock as the Flink jobs
  if ! locked java -jar "$JMH_JAR" "$method" "$fixture" "$csv"; then
    echo "JMH failed: $dataflow $method" >&2
    return 1
  fi
  if [[ ! -s "$csv" ]]; then
    echo "missing JMH output: $csv" >&2
    return 1
  fi
}

# The channel count travels with the scores: a per-output query is O(M) in the
# vector width, so a reader comparing dataflows needs the M each one was run at.
write_query_table() {
  local outdir=$1 channels=$2
  "$PYTHON" - "$outdir" "$channels" <<'PY'
import csv, sys
from pathlib import Path

outdir = Path(sys.argv[1])
channels = sys.argv[2]
methods = ("genealog", "green", "caps")
rows = []
for method in methods:
    with (outdir / method / "jmh_query.csv").open(newline="") as handle:
        row = next(csv.DictReader(handle))
    rows.append((method, channels, row["fixture_count"], row["score_ns"],
                 row["error_ns"]))
with (outdir / "query_table.csv").open("w", newline="") as handle:
    writer = csv.writer(handle)
    writer.writerow(("method", "channels", "ids", "score_ns", "error_ns"))
    writer.writerows(rows)
PY
}

# PHASES=plot: validate the captures that are there, rebuild query_table.csv from the JMH results and the table.
if plot_only; then
  for dataflow in $DATAFLOWS; do
    "$PYTHON" "$EXPDIR/validate.py" "$RESDIR/$dataflow"
    write_query_table "$RESDIR/$dataflow" "${CHANNELS[$dataflow]}"
  done
  combine_results "$RESDIR" query_table.csv
  "$PYTHON" "$EXPDIR/../tables.py" query_table "$RESDIR"
  "$PYTHON" "$EXPDIR/plot_traversal.py" "$RESDIR"    # Figure 9a
  exit 0
fi

for dataflow in $DATAFLOWS; do
  echo -e "\n=== $dataflow query table ==="
  outdir="$RESDIR/$dataflow"
  manifest="$outdir/query_ids.txt"

  rm -rf "$outdir"
  mkdir -p "$outdir/ids" "$outdir/caps" "$outdir/green" "$outdir/genealog/stats"

  # Unmeasured pre-pass: CAPS writes the shared manifest (seed 42, count 1000).
  with_cluster run_caps "$dataflow" provenance_ids "$outdir/ids" "$manifest" "$REQUIRED_IDS" \
    > "$outdir/ids/flink.log" 2>&1
  if [[ ! -s "$manifest" ]]; then
    echo "missing manifest after provenance_ids: $manifest" >&2
    exit 1
  fi

  with_cluster run_caps "$dataflow" provenance_shared "$outdir/caps" "$manifest" \
    > "$outdir/caps/flink.log" 2>&1
  run_jmh caps "$outdir"

  with_cluster run_green "$dataflow" provenance_shared "$outdir/green" "$manifest" \
    > "$outdir/green/flink.log" 2>&1
  run_jmh green "$outdir"

  with_cluster run_genealog "$dataflow" "$outdir/genealog/stats" sortedPtr \
    --traversalBenchDir "$outdir/genealog" \
    --traversalQueryIds "$manifest" \
    > "$outdir/genealog/flink.log" 2>&1
  run_jmh genealog "$outdir"

  # Becasue the ratio is meaningless otherwise, checksums tie fixture to the capture run.
  "$PYTHON" "$EXPDIR/validate.py" "$outdir"

  write_query_table "$outdir" "${CHANNELS[$dataflow]}"

  echo "  query_table.csv:"; cat "$outdir/query_table.csv"
done

combine_results "$RESDIR" query_table.csv
echo -e "\nquery_table.csv:"; cat "$RESDIR/query_table.csv"

# The paper's per-output table: JMH (the query code in a hot loop over the 1,000 captured outputs) next to the in-situ time of
# experiments/how_much (the same query, once per sampled output, in the middle of the stream: cold-path costs included).
# GeneaLog's JMH time is flat (about 10-17 ns) because its query reads the sizes of the provenance sets that its serializer
# built at the last keyBy instead of traversing the contributing set.
echo -e "\n[table]"
"$PYTHON" "$EXPDIR/../tables.py" query_table "$RESDIR"
"$PYTHON" "$EXPDIR/plot_traversal.py" "$RESDIR"    # Figure 9a: traversal.pdf, traversal.csv
echo -e "\nDone. Outputs in $RESDIR/"

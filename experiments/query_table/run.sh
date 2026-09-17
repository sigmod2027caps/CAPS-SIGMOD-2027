#!/usr/bin/env bash
# Provenance query latency table: JMH over the same 1,000 shared output
# identifiers for CAPS, Green-polynomial, and GeneaLog.
set -euo pipefail

EXPDIR="$(cd "$(dirname "$0")" && pwd)"
source "$(cd "$(dirname "$0")/.." && pwd)/common.sh"

REQUIRED_IDS=1000
JMH_JAR="$ROOT/code/query_jmh/target/benchmarks.jar"

require_flink
for dataflow in $DATAFLOWS; do require_inputs "$dataflow"; done
build_all --with-competitors --with-jmh

if [[ ! -s "$JMH_JAR" ]]; then
  echo "missing JMH jar: $JMH_JAR" >&2
  exit 1
fi

echo "dataflow,method,channels,ids,score_ns,error_ns" > "$EXPDIR/query_table.csv"

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
  if ! java -jar "$JMH_JAR" "$method" "$fixture" "$csv"; then
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
methods = ("caps", "green", "genealog")
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

for dataflow in $DATAFLOWS; do
  echo -e "\n=== $dataflow query table ==="
  outdir="$EXPDIR/$dataflow"
  manifest="$outdir/query_ids.txt"

  rm -rf "$outdir"
  mkdir -p "$outdir/ids" "$outdir/caps" "$outdir/green" "$outdir/genealog/stats"

  # Unmeasured pre-pass: CAPS writes the shared manifest (seed 42, count 1000).
  restart_cluster
  run_caps "$dataflow" provenance_ids "$outdir/ids" "$manifest" "$REQUIRED_IDS" \
    > "$outdir/ids/flink.log" 2>&1
  if [[ ! -s "$manifest" ]]; then
    echo "missing manifest after provenance_ids: $manifest" >&2
    exit 1
  fi

  restart_cluster
  run_caps "$dataflow" provenance_shared "$outdir/caps" "$manifest" \
    > "$outdir/caps/flink.log" 2>&1
  run_jmh caps "$outdir"

  restart_cluster
  run_green "$dataflow" provenance_shared "$outdir/green" "$manifest" \
    > "$outdir/green/flink.log" 2>&1
  run_jmh green "$outdir"

  restart_cluster
  run_genealog "$dataflow" "$outdir/genealog/stats" sortedPtr \
    --traversalBenchDir "$outdir/genealog" \
    --traversalQueryIds "$manifest" \
    > "$outdir/genealog/flink.log" 2>&1
  run_jmh genealog "$outdir"

  # Becasue the ratio is meaningless otherwise, checksums tie fixture to the capture run.
  "$PYTHON" "$EXPDIR/validate.py" "$outdir"

  write_query_table "$outdir" "${CHANNELS[$dataflow]}"
  append_combined "$dataflow" "$outdir/query_table.csv" "$EXPDIR/query_table.csv"

  echo "  query_table.csv:"; cat "$outdir/query_table.csv"
done

echo -e "\nquery_table.csv:"; cat "$EXPDIR/query_table.csv"
echo -e "\nDone. Outputs in $EXPDIR/"

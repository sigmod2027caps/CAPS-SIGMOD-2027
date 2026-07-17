#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
EXP="$ROOT/experiments"

EXPERIMENTS=(
  how_much
  queries
  expiry
  summarize
  query_length
)

for experiment in "${EXPERIMENTS[@]}"; do
  echo
  echo "===== Running $experiment ====="
  "$EXP/$experiment/run.sh"
done

echo
echo "========================================"
echo "Experiment results"
echo "========================================"

print_result() {
  local label=$1
  local file=$2

  echo
  echo "[$label] ${file#$ROOT/}"
  if [[ -s "$file" ]]; then
    cat "$file"
  else
    echo "ERROR: missing or empty result file" >&2
    return 1
  fi
}

print_result "How much: runtime and query latency" "$EXP/how_much/times.csv"
print_result "How much: metadata memory" "$EXP/how_much/memory_model.csv"
print_result "Temporal queries: query latency" "$EXP/queries/query_latency.csv"
print_result "Temporal queries: insertion latency" "$EXP/queries/insert_latency.csv"
print_result "Temporal queries: index statistics" "$EXP/queries/index_stats.csv"
print_result "Expiry: statistics" "$EXP/expiry/expiry_stats.csv"
print_result "Expiry: memory over time" "$EXP/expiry/expiry_memory.csv"
print_result "Summarization" "$EXP/summarize/summarize.csv"
print_result "Query latency by window length" "$EXP/query_length/query_length_latency.csv"

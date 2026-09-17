#!/usr/bin/env bash
# Runs everything: installs Flink, creates the Python virtualenv, builds the
# input datasets, then runs the eight experiments, which build CAPS and the
# competitors themselves and write both the result CSVs and the figures.
# Each step is skipped when its output is already in place, so re-running is
# cheap.
#
# Set DATAFLOWS to run a subset; only the datasets those dataflows need are
# built. For example:
#   DATAFLOWS="nexmark_1 nexmark_2" ./run.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
EXP="$ROOT/experiments"
PYTHON="${PYTHON:-python3}"

DATAFLOWS="${DATAFLOWS:-nexmark_1 nexmark_2 taxi_1 taxi_2}"
export DATAFLOWS

# Timed runs are repeated and the median is reported. Lower it to 1 for a quick
# look; the paper uses 3.
REPS="${REPS:-3}"
export REPS

needs() {
  local prefix=$1 dataflow
  for dataflow in $DATAFLOWS; do
    [[ $dataflow == $prefix* ]] && return 0
  done
  return 1
}

EXPERIMENTS=(
  how_much
  memory
  query_table
  queries
  expiry
  summarize
  query_length
  paths
)

echo "===== Setup ====="

# Set FLINK_DIR if you have a Flink 1.10 already; then nothing is downloaded.
if [[ -n "${FLINK_DIR:-}" && -x "${FLINK_DIR}/bin/flink" ]]; then
  echo "using the Flink at $FLINK_DIR"
elif [[ ! -x "$ROOT/code/flink-1.10.0/bin/flink" ]]; then
  "$ROOT/install_flink.sh"
else
  echo "Flink 1.10 already installed"
fi

if [[ ! -x "$ROOT/venv/bin/python" ]]; then
  echo "creating the Python virtualenv for the plotting scripts ..."
  "$PYTHON" -m venv "$ROOT/venv"
  "$ROOT/venv/bin/pip" install --quiet --upgrade pip
  "$ROOT/venv/bin/pip" install --quiet -r "$ROOT/requirements.txt"
else
  echo "Python virtualenv already present"
fi
PYTHON="$ROOT/venv/bin/python"
export PYTHON

if needs nexmark; then
  if [[ ! -s "$ROOT/data/nexmark/persons.txt" || ! -s "$ROOT/data/nexmark/auctions.txt" ]]; then
    echo "generating the Nexmark input ..."
    (cd "$ROOT/data/nexmark" && "$PYTHON" generate_nexmark.py)
  else
    echo "Nexmark input already present"
  fi
fi

if needs taxi; then
  if [[ ! -s "$ROOT/data/taxis/taxis.txt" ]]; then
    echo "building the NYC taxi input (downloads ~4 GB, needs 7z) ..."
    (cd "$ROOT/data/taxis" && ./download.sh && "$PYTHON" make_taxis_txt.py)
  else
    echo "NYC taxi input already present"
  fi
fi

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
print_result "How much: metadata memory (analytical model)" "$EXP/how_much/memory_model.csv"
# The measured counterpart of the model above: what the probes actually counted.
print_result "How much: metadata volume (measured by the probes)" "$EXP/memory/metadata_volume.csv"
print_result "How much: metadata volume, MB per method" "$EXP/memory/how_much_comparison.csv"
print_result "Provenance query latency, shared 1000 outputs (JMH)" "$EXP/query_table/query_table.csv"
print_result "Temporal queries: query latency" "$EXP/queries/query_latency.csv"
print_result "Temporal queries: insertion latency" "$EXP/queries/insert_latency.csv"
print_result "Temporal queries: index statistics" "$EXP/queries/index_stats.csv"
print_result "Expiry: statistics" "$EXP/expiry/expiry_stats.csv"
print_result "Expiry: memory over time" "$EXP/expiry/expiry_memory.csv"
print_result "Summarization" "$EXP/summarize/summarize.csv"
print_result "Query latency by window length" "$EXP/query_length/query_length_latency.csv"
# taxi_1 is the only dataflow here that refines sources into paths, so this one
# can legitimately have no results when DATAFLOWS does not select it.
if [[ -s "$EXP/paths/paths.csv" ]]; then
  print_result "Per source vs per path counters" "$EXP/paths/paths.csv"
fi

echo
echo "Figures (PDF) are next to the CSVs in each experiment directory."

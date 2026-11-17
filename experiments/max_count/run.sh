#!/usr/bin/env bash
# Largest per-channel count of CAPS over all sink outputs, per dataflow (instrumentation.tex, Proposition "Meta-data size":
# "C is far below 2^63-1"). Runs CAPS at parallelism 1 with every sink output written to a file, and takes the largest
# counter. The number of outputs is checked against experiments/verify. Writes max_count_<tag>.csv and max_count_<tag>.pdf.
#
#   SMALL=1 ./run.sh    the 10% subsets
#   ./run.sh            the full data
# It takes the machine lock (/tmp/caps-flink.lock) itself.
set -euo pipefail

# PHASES=plot: nothing to redraw without the raw sink files; prints the CSV of an earlier run.
if [[ " ${PHASES:-} " == *" plot "* ]]; then cat "$(dirname "$0")"/max_count_*p1.csv 2>/dev/null; exit 0; fi

if [[ -z ${CAPS_LOCK_HELD:-} ]]; then export CAPS_LOCK_HELD=1; exec flock -o "${LOCK_FILE:-/tmp/caps-flink.lock}" "$0" "$@"; fi

# --- parameters (comment in or out) ---
DATAFLOWS_DEFAULT=(
  nexmark_1
  nexmark_2
  taxi_1
  taxi_2
  twitter_1
  twitter_2
)
# SMALL=1                  # the 10% subsets (data/small)

EXPDIR="$(cd "$(dirname "$0")" && pwd)"
if [[ -z "${DATAFLOWS:-}" ]]; then DATAFLOWS="${DATAFLOWS_DEFAULT[*]}"; fi
source "$(cd "$(dirname "$0")/.." && pwd)/common.sh"

require_flink
for dataflow in $DATAFLOWS; do require_inputs "$dataflow"; done
build_all

TAG="p${CAPS_PARALLELISM}"
[[ $SMALL == 1 ]] && TAG="small_$TAG"
CSV="$EXPDIR/max_count_$TAG.csv"
RAW="$EXPDIR/work_$TAG"
mkdir -p "$RAW"

ARGS=()
for dataflow in $DATAFLOWS; do
  echo "=== $dataflow  small=$SMALL"
  workdir="$RAW/$dataflow"
  rm -rf "$workdir" && mkdir -p "$workdir"
  restart_cluster
  run_caps_file "$dataflow" "$workdir/caps.out" > "$workdir/caps.log" 2>&1 || {
    echo "  caps FAILED, see $workdir/caps.log" >&2; exit 1; }
  ARGS+=("$dataflow:${CHANNELS[$dataflow]}:$workdir/caps.out")
done

"$PYTHON" "$EXPDIR/max_count.py" --csv "$CSV" --pdf "$EXPDIR/max_count_$TAG.pdf" \
  --verify "$EXPDIR/../verify/verify_$TAG.csv" "${ARGS[@]}"
cat "$CSV"

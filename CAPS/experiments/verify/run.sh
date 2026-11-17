#!/usr/bin/env bash
# Cross-method verification: runs noprov, CAPS, Green
# polynomials and GeneaLog on the same input, with every sink output written to
# a file, and checks that
#   outputs     the four methods produced the same outputs, values and derived
#               timestamps alike,
#   howmuch     CAPS and Green agree per output on every channel, and CAPS and
#               GeneaLog (backward traversal count) on the total (above
#               parallelism 1 the GeneaLog total is only reported, see verify.py),
#   monotone    the sink of CAPS received its input in non-decreasing derived
#               time (parallelism 1: zero out-of-order arrivals),
#   window_end  every output timestamp is the right boundary of a window,
#   invariance  (parallelism above 1) CAPS gives the outputs and counts it gives
#               at parallelism 1.
# Writes verify_<tag>.csv, one row per dataflow x check, and prints PASS or FAIL.
#
#   SMALL=1 ./run.sh                    the 10% subsets, a few minutes
#   CAPS_PARALLELISM=2 SMALL=1 ./run.sh output multisets must still match; the
#                                       order may differ, so monotone is only
#                                       counted; CAPS runs once more at parallelism
#                                       1 as the reference of the invariance check
# It takes the machine lock (/tmp/caps-flink.lock) itself.
set -euo pipefail

# One Flink job at a time on this machine: the whole run holds the lock (CAPS_LOCK_HELD tells common.sh that it is held).
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
# CAPS_PARALLELISM=2       # parallelism of every job
# KEEP_OUTPUTS=1           # keep the sink files (they are large on the full data)
# GENEALOG_STRATEGY=list   # aggregate strategy of GeneaLog (default sortedPtr)

EXPDIR="$(cd "$(dirname "$0")" && pwd)"
if [[ -z "${DATAFLOWS:-}" ]]; then DATAFLOWS="${DATAFLOWS_DEFAULT[*]}"; fi
source "$(cd "$(dirname "$0")/.." && pwd)/common.sh"

require_flink
for dataflow in $DATAFLOWS; do require_inputs "$dataflow"; done
build_all --with-competitors

TAG="p${CAPS_PARALLELISM}"
[[ $SMALL == 1 ]] && TAG="small_$TAG"
CSV="$EXPDIR/verify_$TAG.csv"
# Rerunning a few dataflows replaces only their rows.
if [[ -s "$CSV" ]]; then
  awk -F, -v rerun=" $DATAFLOWS " 'NR == 1 || index(rerun, " " $1 " ") == 0' "$CSV" > "$CSV.tmp"
  mv "$CSV.tmp" "$CSV"
fi
failed=0

# run_method <method> <dataflow> <workdir>
run_method() {
  local method=$1 dataflow=$2 workdir=$3
  restart_cluster
  case $method in
    noprov)   run_noprov_file "$dataflow" "$workdir/noprov.out" ;;
    caps)     run_caps_file "$dataflow" "$workdir/caps.out" ;;
    green)    run_green_file "$dataflow" "$workdir/green.out" ;;
    genealog) mkdir -p "$workdir/genealog_stats"
              run_genealog_dump "$dataflow" "$workdir/genealog_stats" "$workdir/genealog.out" ;;
  esac > "$workdir/$method.log" 2>&1
}

for dataflow in $DATAFLOWS; do
  echo -e "\n=== $dataflow  small=$SMALL  parallelism=$CAPS_PARALLELISM ==="
  workdir="$EXPDIR/work_$TAG/$dataflow"
  rm -rf "$workdir" && mkdir -p "$workdir"

  # Above parallelism 1 the counts of CAPS are also compared with CAPS at parallelism 1.
  caps_ref=()
  if (( CAPS_PARALLELISM != 1 )); then
    restart_cluster
    CAPS_PARALLELISM=1 run_caps_file "$dataflow" "$workdir/caps_p1.out" > "$workdir/caps_p1.log" 2>&1 || {
      echo "  caps at parallelism 1 FAILED, see $workdir/caps_p1.log" >&2; exit 1; }
    caps_ref=(--caps-ref "$workdir/caps_p1.out")
  fi

  for method in noprov caps green genealog; do
    t0=$(date +%s)
    run_method "$method" "$dataflow" "$workdir" || {
      echo "  $method FAILED, see $workdir/$method.log" >&2; exit 1; }
    echo "  $method: $(( $(date +%s) - t0 )) s, $(grep -vc '^---' "$workdir/$method.out") rows"
  done

  "$PYTHON" "$EXPDIR/verify.py" --dataflow "$dataflow" --dir "$workdir" \
    --channels "${CHANNELS[$dataflow]}" --slide-ms "${OUT_SLIDE_MS[$dataflow]}" \
    --parallelism "$CAPS_PARALLELISM" --small "$SMALL" --csv "$CSV" "${caps_ref[@]}" || failed=1

  [[ -n "${KEEP_OUTPUTS:-}" ]] || rm -f "$workdir"/*.out
done

echo -e "\nverify_$TAG.csv:"; cat "$CSV"
if (( failed )); then
  echo -e "\nFAIL"
  exit 1
fi
echo -e "\nPASS"

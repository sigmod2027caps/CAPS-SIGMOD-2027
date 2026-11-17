#!/usr/bin/env bash
# Verification of the two-tier index in the Flink sinks (WP2). For every dataflow it
# runs the temporal-query benchmark (NUM_QUERIES how-much and NUM_QUERIES versioning queries) with
#   CAPS two-tier index (+ the ArrayStore scan baseline behind the same Tier 1), at p in PARALLELISMS
#   GeneaLog index (same Tier 1, then graph traversal), at p in PARALLELISMS
#   the slack-buffer sink of the released artifact (reference, p = 1 only)
# and checks (index_check.py)
#   (a) the CAPS answers are identical across parallelism (counts, defined, from/to),
#   (b) CAPS two-tier = ArrayStore scan (asserted inside the sink on every query) = GeneaLog (sum of
#       the channels against the traversal count; above p = 1 the GeneaLog total is only reported
#       for nexmark_1 and twitter_1, where its serializer collapses the graphs),
#   (b') Green index (same Tier 1, polynomials projected per channel) = CAPS in every channel, at every p,
#   (c) CAPS at p = 1 = the released slack-buffer logic on the same input,
#   (a'-b') the same for the window-length sweep (querylen mode, several lengths per dataflow),
#   (d) zero late records, and every post-job answer final; the provisional queries run during the
#       stream were all non-final and never above their final answer.
# It also tabulates the Tier-1 occupancy and the maximum of (watermark - timestamp) at arrival, which is
# negative when every arrival precedes its watermark.
#
#   SMALL=1 experiments/verify/index_check.sh
#   DATAFLOWS="nexmark_1 nexmark_2 twitter_2 taxi_2" PARALLELISMS="1" experiments/verify/index_check.sh
# Writes index_check_<tag>.csv (one row per dataflow x check) and index_stats_<tag>.csv (one row per
# dataflow x parallelism). The sink outputs are in work_index_<tag>/.
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
PARALLELISMS="${PARALLELISMS:-1 2 4}"
# SMALL=1                  # the 10% subsets (data/small)
WITH_GENEALOG="${WITH_GENEALOG:-1}"   # the GeneaLog index baseline (slow on big data)
WITH_GREEN="${WITH_GREEN:-1}"         # the Green index baseline: polynomials behind the same Tier 1
WITH_LEGACY="${WITH_LEGACY:-1}"       # the released slack-buffer sink at p = 1
SWEEP_QUERIES="${SWEEP_QUERIES:-200}" # queries per window length of the length sweep (0 = skip it); the
                                      # benchmark windows of the small nexmark subsets span the whole stream,
                                      # the shorter lengths of the sweep make the answers non-trivial

# Slack of the released artifact per dataflow (ms): twice the window length of the dataflow
# (1 h twitter_1, 2 h taxi, 12 h twitter_2, 24 h nexmark), as in the sinks before WP1.
declare -A OLD_SLACK_MS=(
  [nexmark_1]=86400000 [nexmark_2]=86400000
  [taxi_1]=7200000 [taxi_2]=7200000
  [twitter_1]=3600000 [twitter_2]=43200000
)

EXPDIR="$(cd "$(dirname "$0")" && pwd)"
if [[ -z "${DATAFLOWS:-}" ]]; then DATAFLOWS="${DATAFLOWS_DEFAULT[*]}"; fi
source "$(cd "$(dirname "$0")/.." && pwd)/common.sh"

require_flink
for dataflow in $DATAFLOWS; do require_inputs "$dataflow"; done
build_all --with-competitors

TAG="small"; [[ $SMALL == 1 ]] || TAG="full"
CSV="$EXPDIR/index_check_$TAG.csv"
STATS="$EXPDIR/index_stats_$TAG.csv"
for f in "$CSV" "$STATS"; do   # rerunning a few dataflows replaces only their rows
  if [[ -s "$f" ]]; then
    awk -F, -v rerun=" $DATAFLOWS " 'NR == 1 || index(rerun, " " $1 " ") == 0' "$f" > "$f.tmp"
    mv "$f.tmp" "$f"
  fi
done
failed=0

for dataflow in $DATAFLOWS; do
  echo -e "\n=== $dataflow  small=$SMALL  parallelism: $PARALLELISMS ==="
  workdir="$EXPDIR/work_index_$TAG/$dataflow"
  rm -rf "$workdir" && mkdir -p "$workdir"
  len=${QUERY_LEN_MS[$dataflow]}

  for p in $PARALLELISMS; do
    dir="$workdir/caps_p$p"; mkdir -p "$dir"
    restart_cluster
    t0=$(date +%s)
    CAPS_PARALLELISM=$p run_caps "$dataflow" queries "$dir" "$NUM_QUERIES" "$len" > "$dir/flink.log" 2>&1 || {
      echo "  caps p=$p FAILED, see $dir/flink.log" >&2; exit 1; }
    echo "  caps p=$p: $(( $(date +%s) - t0 )) s"
    if [[ $WITH_GENEALOG == 1 ]]; then
      dir="$workdir/genealog_p$p"; mkdir -p "$dir/stats"
      restart_cluster
      t0=$(date +%s)
      CAPS_PARALLELISM=$p run_genealog "$dataflow" "$dir/stats" list \
        --indexBenchDir "$dir" --indexNumQueries "$NUM_QUERIES" --indexQueryLenMs "$len" \
        > "$dir/flink.log" 2>&1 || { echo "  genealog p=$p FAILED, see $dir/flink.log" >&2; exit 1; }
      echo "  genealog p=$p: $(( $(date +%s) - t0 )) s"
    fi
    if [[ $WITH_GREEN == 1 ]]; then
      dir="$workdir/green_p$p"; mkdir -p "$dir"
      restart_cluster
      t0=$(date +%s)
      CAPS_PARALLELISM=$p run_green "$dataflow" queries "$dir" "$NUM_QUERIES" "$len" > "$dir/flink.log" 2>&1 || {
        echo "  green p=$p FAILED, see $dir/flink.log" >&2; exit 1; }
      echo "  green p=$p: $(( $(date +%s) - t0 )) s"
    fi
    if (( SWEEP_QUERIES > 0 )); then
      dir="$workdir/caps_len_p$p"; mkdir -p "$dir"
      restart_cluster
      CAPS_PARALLELISM=$p run_caps "$dataflow" querylen "$dir" "$SWEEP_QUERIES" "${QUERY_LENS_MS[$dataflow]}" \
        > "$dir/flink.log" 2>&1 || { echo "  caps length sweep p=$p FAILED, see $dir/flink.log" >&2; exit 1; }
      if [[ $WITH_GENEALOG == 1 ]]; then
        dir="$workdir/genealog_len_p$p"; mkdir -p "$dir/stats"
        restart_cluster
        CAPS_PARALLELISM=$p run_genealog "$dataflow" "$dir/stats" list \
          --indexBenchDir "$dir" --indexNumQueries "$SWEEP_QUERIES" --indexQueryLensMs "${QUERY_LENS_MS[$dataflow]}" \
          > "$dir/flink.log" 2>&1 || { echo "  genealog length sweep p=$p FAILED, see $dir/flink.log" >&2; exit 1; }
      fi
      if [[ $WITH_GREEN == 1 ]]; then
        dir="$workdir/green_len_p$p"; mkdir -p "$dir"
        restart_cluster
        CAPS_PARALLELISM=$p run_green "$dataflow" querylen "$dir" "$SWEEP_QUERIES" "${QUERY_LENS_MS[$dataflow]}" \
          > "$dir/flink.log" 2>&1 || { echo "  green length sweep p=$p FAILED, see $dir/flink.log" >&2; exit 1; }
      fi
      echo "  length sweep p=$p done"
    fi
  done
  if [[ $WITH_LEGACY == 1 ]]; then
    dir="$workdir/slackbuffer_p1"; mkdir -p "$dir"
    restart_cluster
    CAPS_PARALLELISM=1 run_caps "$dataflow" queries_slackbuffer "$dir" "$NUM_QUERIES" "$len" \
      "${OLD_SLACK_MS[$dataflow]}" > "$dir/flink.log" 2>&1 || {
      echo "  slack-buffer FAILED, see $dir/flink.log" >&2; exit 1; }
    echo "  slack-buffer (released logic) p=1 done"
  fi

  "$PYTHON" "$EXPDIR/index_check.py" --dataflow "$dataflow" --dir "$workdir" \
    --parallelisms "$PARALLELISMS" --channels "${CHANNELS[$dataflow]}" \
    --genealog "$WITH_GENEALOG" --green "$WITH_GREEN" --sweep "$SWEEP_QUERIES" --legacy "$WITH_LEGACY" --small "$SMALL" \
    --csv "$CSV" --stats "$STATS" || failed=1
done

echo -e "\nindex_check_$TAG.csv:"; cat "$CSV"
echo -e "\nindex_stats_$TAG.csv:"; cat "$STATS"
if (( failed )); then
  echo -e "\nFAIL"
  exit 1
fi
echo -e "\nPASS"

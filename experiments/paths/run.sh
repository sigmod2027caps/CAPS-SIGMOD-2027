#!/usr/bin/env bash
# Per source vs per path how-much provenance (paper Figure 13). The two
# dataflows in PATH_CHANNELS split each source into several processing paths,
# so CAPS can keep one counter per source or one per path. Each is run twice
# with the online indexing sink, once per granularity, showing that the capture
# cost does not depend on the number of counters while path-level queries
# become possible.
set -euo pipefail

EXPDIR="$(cd "$(dirname "$0")" && pwd)"
source "$(cd "$(dirname "$0")/.." && pwd)/common.sh"

# Only the dataflows with more paths than sources take part.
PATHS_DATAFLOWS=""
for dataflow in $DATAFLOWS; do
  [[ -n "${PATH_CHANNELS[$dataflow]:-}" ]] && PATHS_DATAFLOWS="$PATHS_DATAFLOWS $dataflow"
done
if [[ -z $PATHS_DATAFLOWS ]]; then
  echo "Skipping: none of the selected dataflows refines sources into paths."
  echo "  this experiment runs on: ${!PATH_CHANNELS[*]}"
  exit 0
fi
DATAFLOWS="$PATHS_DATAFLOWS"

require_flink
for dataflow in $DATAFLOWS; do require_inputs "$dataflow"; done
build_all

HEADER="granularity,channels,records,runtime_s,insert_avg_ns,howmuch_avg_us,versioning_avg_us,index_bytes"
rm -f "$EXPDIR/paths.csv"

for dataflow in $DATAFLOWS; do
  echo -e "\n=== $dataflow path counters ==="
  outdir="$EXPDIR/$dataflow"
  mkdir -p "$outdir"
  echo "$HEADER" > "$outdir/paths.csv"

  read -r source_channels path_channels <<<"${PATH_CHANNELS[$dataflow]}"

  for granularity in source path; do
    if [[ $granularity == source ]]; then
      channels=$source_channels
    else
      channels=$path_channels
    fi
    rundir="$outdir/$granularity"
    rm -rf "$rundir" && mkdir -p "$rundir"

    restart_cluster

    echo "  [$granularity granularity, $channels counters]"
    t0=$(date +%s.%N)
    CAPS_GRANULARITY="$granularity" \
      run_caps "$dataflow" queries "$rundir" "$NUM_QUERIES" "${QUERY_LEN_MS[$dataflow]}" \
      > "$rundir/flink.log" 2>&1
    secs=$(awk -v s="$t0" -v e="$(date +%s.%N)" 'BEGIN{printf "%.3f",e-s}')

    records=$(awk -F, 'NR==2{print $1}' "$rundir/index_stats.csv")
    bytes=$(awk -F, 'NR==2{print $2}' "$rundir/index_stats.csv")
    insert=$(awk -F, '$1=="prefix"{print $3}' "$rundir/insert_latency.csv")
    howmuch=$(awk -F, '$1=="prefix" && $2=="howmuch"{print $5}' "$rundir/query_latency.csv")
    versioning=$(awk -F, '$1=="prefix" && $2=="versioning"{print $5}' "$rundir/query_latency.csv")

    echo "$granularity,$channels,$records,$secs,$insert,$howmuch,$versioning,$bytes" \
      >> "$outdir/paths.csv"
    echo "    runtime ${secs}s, insert ${insert}ns, query ${howmuch}us, index ${bytes}B"
  done

  append_combined "$dataflow" "$outdir/paths.csv" "$EXPDIR/paths.csv"
done

echo -e "\npaths.csv:"; cat "$EXPDIR/paths.csv"
echo -e "\n[figures]"
plot_experiment "$EXPDIR"

echo -e "\nDone. Outputs in $EXPDIR/"

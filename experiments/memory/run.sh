#!/usr/bin/env bash
# Measured provenance meta-data volume: Flink accumulator probes at every
# operator. This cross-checks the analytical model the paper reports; it is
# cumulative traffic, so it is not the same quantity as the retained column of Table 2.
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
METHODS=(       # chronological order, ours last
  genealog
  green_polynomial
  caps
)
# SMALL=1       # the 10% subsets (a smoke test, not numbers); the results go to small/ and never overwrite the full ones
# KEEP_PROBES=1 # keep the probe files that are already there

EXPDIR="$(cd "$(dirname "$0")" && pwd)"
if [[ -n "${DATAFLOWS:-}" ]]; then DATAFLOW_LIST=($DATAFLOWS); fi
DATAFLOWS="${DATAFLOW_LIST[*]}"
source "$(cd "$(dirname "$0")/.." && pwd)/common.sh"
RESDIR="$(results_dir "$EXPDIR")"   # SMALL=1 writes into small/ and never overwrites the full-data results
mkdir -p "$RESDIR"

if ! plot_only; then
  require_flink
  for dataflow in $DATAFLOWS; do require_inputs "$dataflow"; done
  build_all --with-competitors
fi


# TOTAL row of the per-operator probe CSV.
probe_total() {
  local method=$1 outdir=$2
  # Separate line on purpose: inside one `local` the subscript is still empty.
  local csv="$outdir/${PROBE_CSV[$method]}"
  if [[ ! -s "$csv" ]]; then
    echo "missing probe output: $csv" >&2
    return 1
  fi
  awk -F, '$2=="TOTAL" {print $3","$4; found=1} END{if(!found) exit 1}' "$csv"
}

# PHASES=plot: rebuild metadata_volume.csv from the probe files of the runs, then the comparison and the table.
if plot_only; then
  for dataflow in $DATAFLOWS; do
    echo "method,records,bytes" > "$RESDIR/$dataflow/metadata_volume.csv"
    for method in "${METHODS[@]}"; do
      case $method in
        caps)             probe_dir="$RESDIR/$dataflow/volume_caps" ;;
        green_polynomial) probe_dir="$RESDIR/$dataflow/volume_green" ;;
        genealog)         probe_dir="$RESDIR/$dataflow/volume_genealog" ;;
      esac
      echo "$method,$(probe_total "$method" "$probe_dir")" >> "$RESDIR/$dataflow/metadata_volume.csv"
    done
  done
  combine_results "$RESDIR" metadata_volume.csv
  "$PYTHON" "$EXPDIR/build_comparison.py" "$RESDIR"
  "$PYTHON" "$EXPDIR/../tables.py" memory "$RESDIR"
  exit 0
fi

for dataflow in $DATAFLOWS; do
  echo -e "\n=== $dataflow ==="
  outdir="$RESDIR/$dataflow"
  mkdir -p "$outdir"
  echo "method,records,bytes" > "$outdir/metadata_volume.csv"

  for method in "${METHODS[@]}"; do
    case $method in
      caps)             probe_dir="$outdir/volume_caps" ;;
      green_polynomial) probe_dir="$outdir/volume_green" ;;
      genealog)         probe_dir="$outdir/volume_genealog" ;;
    esac
    [[ ${KEEP_PROBES:-0} == 1 ]] || rm -rf "$probe_dir"
    mkdir -p "$probe_dir"
    # KEEP_PROBES=1 keeps a probe file that is already there (the counts of a finished run do not change). The probes of
    # a SMALL=1 run are in small/, so they are never taken for those of a full run.
    if [[ ${KEEP_PROBES:-0} == 1 ]] && probe_total "$method" "$probe_dir" >/dev/null 2>&1; then
      echo "  $method (probe already there)"
    else
      echo "  $method"
      # A fresh cluster per probe, so the accumulators do not carry from the previous run.
      with_cluster run_probe "$method" "$dataflow" "$probe_dir" \
        > "$probe_dir/flink.log" 2>&1
    fi
    totals=$(probe_total "$method" "$probe_dir") || {
      echo "no TOTAL row for $dataflow $method, see $probe_dir/flink.log" >&2
      exit 1
    }
    IFS=, read -r records bytes <<< "$totals"
    echo "$method,$records,$bytes" >> "$outdir/metadata_volume.csv"
  done
done

combine_results "$RESDIR" metadata_volume.csv

echo -e "\nmetadata_volume.csv:"; cat "$RESDIR/metadata_volume.csv"

echo -e "\n[tables: cumulative volume (probes) and retained meta-data (analytical, experiments/how_much)]"
"$PYTHON" "$EXPDIR/build_comparison.py" "$RESDIR"
"$PYTHON" "$EXPDIR/../tables.py" memory "$RESDIR"

echo -e "\nDone. Outputs in $RESDIR/"

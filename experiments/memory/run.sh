#!/usr/bin/env bash
# Measured provenance meta-data volume: Flink accumulator probes at every
# operator. This cross-checks the analytical model the paper reports; it is
# cumulative traffic, so it is not the same quantity as Figure 10.
set -euo pipefail

EXPDIR="$(cd "$(dirname "$0")" && pwd)"
source "$(cd "$(dirname "$0")/.." && pwd)/common.sh"

METHODS=(caps green_polynomial genealog)

require_flink
for dataflow in $DATAFLOWS; do require_inputs "$dataflow"; done
build_all --with-competitors

rm -f "$EXPDIR/metadata_volume.csv"
echo "dataflow,method,records,bytes" > "$EXPDIR/metadata_volume.csv"

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

for dataflow in $DATAFLOWS; do
  echo -e "\n=== $dataflow ==="
  outdir="$EXPDIR/$dataflow"
  mkdir -p "$outdir"
  echo "method,records,bytes" > "$outdir/metadata_volume.csv"

  for method in "${METHODS[@]}"; do
    case $method in
      caps)             probe_dir="$outdir/volume_caps" ;;
      green_polynomial) probe_dir="$outdir/volume_green" ;;
      genealog)         probe_dir="$outdir/volume_genealog" ;;
    esac
    mkdir -p "$probe_dir"
    # A probe file that is already there is kept: the run is long and the counts
    # do not change. Delete the dataflow directory if you want it again.
    if probe_total "$method" "$probe_dir" >/dev/null 2>&1; then
      echo "  $method (probe already there)"
    else
      echo "  $method"
      # We restart the cluster so the accumulators do not carry from the previous run.
      restart_cluster
      run_probe "$method" "$dataflow" "$probe_dir" \
        > "$probe_dir/flink.log" 2>&1
    fi
    totals=$(probe_total "$method" "$probe_dir") || {
      echo "no TOTAL row for $dataflow $method, see $probe_dir/flink.log" >&2
      exit 1
    }
    IFS=, read -r records bytes <<< "$totals"
    echo "$method,$records,$bytes" >> "$outdir/metadata_volume.csv"
    echo "$dataflow,$method,$records,$bytes" >> "$EXPDIR/metadata_volume.csv"
  done
done

echo -e "\nmetadata_volume.csv:"; cat "$EXPDIR/metadata_volume.csv"

echo -e "\n[comparison table]"
"$PYTHON" "$EXPDIR/build_comparison.py" "$EXPDIR"

echo -e "\nDone. Outputs in $EXPDIR/"

#!/usr/bin/env bash
# Provenance capture overhead (paper Section 7.2.1): runtime, per-output
# provenance query time, and the analytical meta-data memory model, for each
# dataflow and each of the four methods. No outputs are written to file, so
# the runtime is pure processing.
#
# While the stream runs, GeneaLog, CAPS and green_polynomial flip the same
# biased coin (seed 42, per-dataflow probability, about 1,000 heads) for
# every output and, on heads, answer and time the provenance query right
# there. The coin is charged also to noprov, otherwise the overhead is not fair.
set -euo pipefail

EXPDIR="$(cd "$(dirname "$0")" && pwd)"
source "$(cd "$(dirname "$0")/.." && pwd)/common.sh"

QUERY_SEMANTICS=total_contributions

require_flink
for dataflow in $DATAFLOWS; do require_inputs "$dataflow"; done
build_all --with-competitors

rm -f "$EXPDIR"/{times.csv,times_stats.csv,memory_model.csv,query_ns.csv}

# Times a submission after a fresh cluster; returns wall seconds via stdout.
# Job duration is taken from the REST JSON, not the client wall clock becasue
# the `flink run` startup is not part of the dataflow.
timed_run() {
  local log=$1 job_json=$2
  shift 2
  restart_cluster
  local t0; t0=$(date +%s.%N)
  "$@" > "$log" 2>&1
  local wall; wall=$(awk -v s="$t0" -v e="$(date +%s.%N)" 'BEGIN{printf "%.3f",e-s}')
  capture_job_json "$log" "$job_json"
  if [[ ! -s "$job_json" ]]; then
    echo "missing job JSON: $job_json" >&2
    exit 1
  fi
  local job_secs
  job_secs=$(job_duration "$job_json") || {
    echo "missing job duration in: $job_json" >&2
    exit 1
  }
  TIMED_WALL=$wall
  TIMED_JOB=$job_secs
}

# run_noprov does not forward the sampling coin; mirror its invocation here.
run_noprov_fair() {
  local dataflow=$1
  "$FLINK" run --parallelism 1 --class "${NOPROV_CLASS[$dataflow]}" \
    "$ROOT/code/noprov/$dataflow/target/${JAR[$dataflow]}" \
    $(dataflow_inputs "$dataflow") \
    none "${SAMPLE_PROB[$dataflow]}"
}

read_query_ns() {
  local csv=$1 method=$2
  if [[ ! -s "$csv" ]]; then
    echo "missing query result: $csv" >&2
    exit 1
  fi
  awk -F, -v m="$method" 'NR>1 && $1==m {print $4; exit}' "$csv"
}

write_dataflow_results() {
  local outdir=$1 reps_csv=$2
  "$PYTHON" - "$reps_csv" "$outdir" "$QUERY_SEMANTICS" <<'PY'
import csv, statistics, sys
from pathlib import Path

reps_path, outdir, semantics = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3]
methods = ("noprov", "genealog", "caps", "green_polynomial")
query_methods = ("genealog", "caps", "green_polynomial")

def floats(values):
    out = []
    for v in values:
        s = "" if v is None else str(v).strip()
        if s:
            out.append(float(s))
    return out

def summarise(values, spec):
    vals = floats(values)
    if not vals:
        return "", "", ""
    med = statistics.median(vals)
    mean = statistics.mean(vals)
    std = statistics.stdev(vals) if len(vals) >= 2 else None
    fmt = lambda x: "" if x is None else format(x, spec)
    return fmt(med), fmt(mean), fmt(std)

by_method = {}
with reps_path.open(newline="") as handle:
    for row in csv.DictReader(handle):
        by_method.setdefault(row["method"], []).append(row)

def timer_ns(subdir, csv_name, method):
    path = outdir / subdir / csv_name
    if not path.is_file():
        return ""
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            if row.get("method") == method:
                return (row.get("avg_ns") or "").strip()
    return ""

raw_median = {}
for method in query_methods:
    med, _, _ = summarise(
        [r.get("prov_query_raw_ns") for r in by_method.get(method, [])], ".1f")
    raw_median[method] = med

timer = {
    "genealog": timer_ns("genealog_timer", "provenance_query_genealog.csv", "genealog_timer"),
    "caps": timer_ns("caps_timer", "provenance_query.csv", "caps_timer"),
    "green_polynomial": timer_ns("green_timer", "provenance_query_ink.csv", "ink_timer"),
}

stats_path = outdir / "times_stats.csv"
with stats_path.open("w", newline="") as handle:
    writer = csv.writer(handle)
    writer.writerow([
        "method", "n",
        "job_duration_median_s", "job_duration_mean_s", "job_duration_stddev_s",
        "prov_query_median_ns", "prov_query_mean_ns", "prov_query_stddev_ns",
    ])
    for method in methods:
        rows = by_method.get(method, [])
        j_med, j_mean, j_std = summarise(
            [r.get("job_duration_s") for r in rows], ".3f")
        q_med, q_mean, q_std = summarise(
            [r.get("prov_query_raw_ns") for r in rows], ".1f")
        writer.writerow([method, len(rows), j_med, j_mean, j_std, q_med, q_mean, q_std])

times_path = outdir / "times.csv"
with times_path.open("w", newline="") as handle:
    writer = csv.writer(handle)
    writer.writerow([
        "method", "runtime_s", "job_duration_s", "prov_query_ns", "query_semantics",
    ])
    for method in methods:
        rows = by_method.get(method, [])
        r_med, _, _ = summarise([r.get("runtime_s") for r in rows], ".3f")
        j_med, _, _ = summarise([r.get("job_duration_s") for r in rows], ".3f")
        q_med, _, _ = summarise([r.get("prov_query_raw_ns") for r in rows], ".1f")
        sem = semantics if method in query_methods else ""
        writer.writerow([method, r_med, j_med, q_med, sem])

query_path = outdir / "query_ns.csv"
with query_path.open("w", newline="") as handle:
    writer = csv.writer(handle)
    writer.writerow(["method", "raw_ns", "timer_ns", "calibrated_ns"])
    for method in query_methods:
        raw = raw_median.get(method, "")
        t = timer.get(method, "")
        cal = ""
        if raw and t:
            delta = float(raw) - float(t)
            if delta < 0:
                delta = 0.0
            cal = format(delta, ".1f")
        writer.writerow([method, raw, t, cal])
PY
}

for dataflow in $DATAFLOWS; do
  echo -e "\n=== $dataflow  REPS=$REPS ==="
  outdir="$EXPDIR/$dataflow"
  mkdir -p "$outdir"/{noprov,genealog,caps,green,green_verify,genealog_timer,caps_timer,green_timer}
  reps_csv="$outdir/runtime_reps.csv"
  echo "method,rep,runtime_s,job_duration_s,prov_query_raw_ns" > "$reps_csv"
  rm -f "$outdir"/{times.csv,times_stats.csv,query_ns.csv,memory_model.csv}
  rm -f "$outdir"/{job.json,provenance_query.csv,provenance_query_genealog.csv}
  rm -f "$outdir"/{provenance_query_ink.csv,metadata_ink.csv}
  rm -f "$outdir"/{outputs_caps.txt,outputs_genealog.txt,outputs_ink.txt}
  rm -f "$outdir"/selected_samples_*.csv
  rm -rf "$outdir/genealog"/* "$outdir/noprov"/* "$outdir/caps"/* "$outdir/green"/*

  # --- noprov ---
  for ((rep=1; rep<=REPS; rep++)); do
    echo "  --- noprov rep $rep/$REPS ---"
    timed_run "$outdir/noprov/rep${rep}_flink.log" "$outdir/noprov/rep${rep}_job.json" \
      run_noprov_fair "$dataflow"
    echo "noprov,$rep,$TIMED_WALL,$TIMED_JOB," >> "$reps_csv"
  done

  # --- genealog ---
  for ((rep=1; rep<=REPS; rep++)); do
    echo "  --- genealog rep $rep/$REPS ---"
    rm -rf "$outdir/genealog"/*
    rm -f "$outdir/provenance_query_genealog.csv"
    timed_run "$outdir/genealog/rep${rep}_flink.log" "$outdir/genealog/rep${rep}_job.json" \
      run_genealog "$dataflow" "$outdir/genealog" sortedPtr \
        --traversalBenchDir "$outdir" --traversalSampleProb "${SAMPLE_PROB[$dataflow]}"
    gen_ns=$(read_query_ns "$outdir/provenance_query_genealog.csv" genealog)
    echo "genealog,$rep,$TIMED_WALL,$TIMED_JOB,$gen_ns" >> "$reps_csv"
  done
  [[ -s "$outdir/outputs_genealog.txt" ]] || {
    echo "missing result file: $outdir/outputs_genealog.txt" >&2; exit 1; }
  gen_outputs=$(<"$outdir/outputs_genealog.txt")
  last_gen_job="$outdir/genealog/rep${REPS}_job.json"
  gen_bytes=$(genealog_bytes "$last_gen_job" "$gen_outputs")

  # --- caps ---
  for ((rep=1; rep<=REPS; rep++)); do
    echo "  --- caps rep $rep/$REPS ---"
    rm -f "$outdir/provenance_query.csv"
    timed_run "$outdir/caps/rep${rep}_flink.log" "$outdir/caps/rep${rep}_job.json" \
      run_caps "$dataflow" provenance "$outdir" "${SAMPLE_PROB[$dataflow]}"
    caps_ns=$(read_query_ns "$outdir/provenance_query.csv" caps)
    echo "caps,$rep,$TIMED_WALL,$TIMED_JOB,$caps_ns" >> "$reps_csv"
  done
  [[ -s "$outdir/outputs_caps.txt" ]] || {
    echo "missing result file: $outdir/outputs_caps.txt" >&2; exit 1; }
  caps_outputs=$(<"$outdir/outputs_caps.txt")
  caps_bytes=$((caps_outputs * ${CHANNELS[$dataflow]} * 8))

  # --- green_polynomial ---
  for ((rep=1; rep<=REPS; rep++)); do
    echo "  --- green_polynomial rep $rep/$REPS ---"
    rm -f "$outdir/provenance_query_ink.csv"
    timed_run "$outdir/green/rep${rep}_flink.log" "$outdir/green/rep${rep}_job.json" \
      run_green "$dataflow" provenance "$outdir" "${SAMPLE_PROB[$dataflow]}"
    green_ns=$(read_query_ns "$outdir/provenance_query_ink.csv" ink)
    echo "green_polynomial,$rep,$TIMED_WALL,$TIMED_JOB,$green_ns" >> "$reps_csv"
  done

  # Green memory model: same byte convention as how_much_ink (metadata_ink.csv).
  # A seperate verify submission; timing mode does not write that file.
  restart_cluster
  run_green "$dataflow" provenance_verify "$outdir" "${SAMPLE_PROB[$dataflow]}" \
    > "$outdir/green_verify/flink.log" 2>&1
  [[ -s "$outdir/metadata_ink.csv" ]] || {
    echo "missing result file: $outdir/metadata_ink.csv" >&2; exit 1; }
  green_bytes=$(awk -F, 'NR==2 {print $5; exit}' "$outdir/metadata_ink.csv")

  # Timer calibration runs once; bracket cost is subtracted in query_ns.csv.
  rm -rf "$outdir/genealog_timer"/* "$outdir/caps_timer"/* "$outdir/green_timer"/*
  mkdir -p "$outdir/genealog_timer" "$outdir/caps_timer" "$outdir/green_timer"
  timed_run "$outdir/genealog_timer/flink.log" "$outdir/genealog_timer/job.json" \
    run_genealog "$dataflow" "$outdir/genealog_timer" sortedPtr \
      --traversalBenchDir "$outdir/genealog_timer" \
      --traversalSampleProb "${SAMPLE_PROB[$dataflow]}" \
      --traversalCalibrate
  timed_run "$outdir/caps_timer/flink.log" "$outdir/caps_timer/job.json" \
    run_caps "$dataflow" provenance_timer "$outdir/caps_timer" "${SAMPLE_PROB[$dataflow]}"
  timed_run "$outdir/green_timer/flink.log" "$outdir/green_timer/job.json" \
    run_green "$dataflow" provenance_timer "$outdir/green_timer" "${SAMPLE_PROB[$dataflow]}"

  write_dataflow_results "$outdir" "$reps_csv"

  { echo "method,bytes"
    echo "noprov,0"
    echo "genealog,$gen_bytes"
    echo "caps,$caps_bytes"
    echo "green_polynomial,$green_bytes"; } > "$outdir/memory_model.csv"

  for name in times.csv times_stats.csv memory_model.csv query_ns.csv; do
    [[ -s "$outdir/$name" ]] || {
      echo "missing result file: $outdir/$name" >&2; exit 1; }
    append_combined "$dataflow" "$outdir/$name" "$EXPDIR/$name"
  done

  echo "  noprov:   $(awk -F, '$1=="noprov" {print $3; exit}' "$outdir/times.csv")s (job $(awk -F, '$1=="noprov" {print $4; exit}' "$outdir/times.csv")s)"
  echo "  genealog: $(awk -F, '$1=="genealog" {print $3; exit}' "$outdir/times.csv")s, query $(awk -F, '$1=="genealog" {print $5; exit}' "$outdir/times.csv")ns, meta-data ${gen_bytes}B"
  echo "  caps:     $(awk -F, '$1=="caps" {print $3; exit}' "$outdir/times.csv")s, query $(awk -F, '$1=="caps" {print $5; exit}' "$outdir/times.csv")ns, meta-data ${caps_bytes}B"
  echo "  green:    $(awk -F, '$1=="green_polynomial" {print $3; exit}' "$outdir/times.csv")s, query $(awk -F, '$1=="green_polynomial" {print $5; exit}' "$outdir/times.csv")ns, meta-data ${green_bytes}B"
done

echo -e "\ntimes.csv:";        cat "$EXPDIR/times.csv"
echo -e "\ntimes_stats.csv:";  cat "$EXPDIR/times_stats.csv"
echo -e "\nmemory_model.csv:"; cat "$EXPDIR/memory_model.csv"
echo -e "\nquery_ns.csv:";     cat "$EXPDIR/query_ns.csv"
echo -e "\n[figures]"
plot_experiment "$EXPDIR"

echo -e "\nDone. Outputs in $EXPDIR/"

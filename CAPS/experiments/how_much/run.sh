#!/usr/bin/env bash
# Provenance capture overhead (paper Section 7.2, Figure 8 and Table 2): runtime, per-output
# provenance query time, and the analytical meta-data memory model, for each
# dataflow and each of the four methods. No outputs are written to file, so
# the runtime is pure processing.
#
# While the stream runs, GeneaLog, CAPS and green_polynomial flip the same
# biased coin (seed 42, per-dataflow probability, about 1,000 heads) for
# every output and, on heads, answer and time the provenance query right
# there. The coin is charged also to noprov, otherwise the overhead is not fair.
#
# The GeneaLog sink keeps every output tuple and with it its provenance graph (GeneaLog can only answer a query about an
# output while the graph is alive), as in the submitted artifact. The garbage-collection pause time of the TaskManager
# is logged for every timed run (HotSpot GC log, common.sh CAPS_GC_LOG_DIR) and reported next to the runtime, so a runtime
# difference caused by GC is visible as such.
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
  noprov
  genealog
  green_polynomial
  caps
)
# REPS=3        # repetitions of every timed run, each on a fresh cluster (default 3): the median is reported,
                # the error bars of the figure are min-max over them
# SMALL=1       # the 10% subsets (a smoke test, not numbers); the results go to small/ and never overwrite the full ones
# CAPS_PARALLELISM=1   # parallelism of every job (default 1)

EXPDIR="$(cd "$(dirname "$0")" && pwd)"
if [[ -n "${DATAFLOWS:-}" ]]; then DATAFLOW_LIST=($DATAFLOWS); fi
DATAFLOWS="${DATAFLOW_LIST[*]}"
source "$(cd "$(dirname "$0")/.." && pwd)/common.sh"
RESDIR="$(results_dir "$EXPDIR")"   # SMALL=1 writes into small/ and never overwrites the full-data results
mkdir -p "$RESDIR"

has_method() { [[ " ${METHODS[*]} " == *" $1 "* ]]; }

QUERY_SEMANTICS=total_contributions

# PHASES=plot: the per-dataflow CSVs are the raw results; rebuild the combined CSVs, the table and the figures.
if plot_only; then
  combine_results "$RESDIR" times.csv times_stats.csv memory_model.csv memory_model_detail.csv query_ns.csv runtime_reps.csv
  "$PYTHON" "$EXPDIR/../tables.py" runtime "$RESDIR"
  plot_experiment "$RESDIR"
  exit 0
fi

if ! plot_only; then
  require_flink
  for dataflow in $DATAFLOWS; do require_inputs "$dataflow"; done
  build_all --with-competitors
fi

# One timed unit: a fresh cluster (with_cluster: under the machine lock), the job, its REST job JSON read
# while the cluster is still up. The job duration is taken from the REST JSON, not the client wall clock,
# because the `flink run` startup is not part of the dataflow. The GC log of the fresh TaskManager is summarised in
# <job json without .json>_gc.csv. Sets TIMED_WALL, TIMED_JOB and TIMED_GC_N, TIMED_GC_FULL, TIMED_GC_S, TIMED_GC_MAX_S.
timed_unit() {
  local log=$1 job_json=$2
  shift 2
  local gc_start; gc_start=$(capture_gc_count)   # pauses of the JVM start-up: not caused by the job
  local t0; t0=$(date +%s.%N)
  "$@" > "$log" 2>&1
  TIMED_WALL=$(awk -v s="$t0" -v e="$(date +%s.%N)" 'BEGIN{printf "%.3f",e-s}')
  capture_job_json "$log" "$job_json"
  capture_gc "${job_json%.json}_gc.csv" "$gc_start"
}

timed_run() {
  local log=$1 job_json=$2
  rm -rf "${job_json%.json}_gc"
  CAPS_GC_LOG_DIR="${job_json%.json}_gc" with_cluster timed_unit "$log" "$job_json" "${@:3}"
  if [[ ! -s "$job_json" ]]; then
    echo "missing job JSON: $job_json" >&2
    exit 1
  fi
  TIMED_JOB=$(job_duration "$job_json") || {
    echo "missing job duration in: $job_json" >&2
    exit 1
  }
  IFS=, read -r TIMED_GC_N TIMED_GC_FULL TIMED_GC_S TIMED_GC_MAX_S < "${job_json%.json}_gc.csv"
  rm -rf "${job_json%.json}_gc"   # the summary stays; the raw GC log of the TaskManager is not kept
}

# run_noprov does not forward the sampling coin; mirror its invocation here.
run_noprov_fair() {
  local dataflow=$1
  "$FLINK" run --parallelism "$CAPS_PARALLELISM" --class "${NOPROV_CLASS[$dataflow]}" \
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

read_query_median() { # the median of the per-sample durations of the in-situ queries (last column of the sink CSV)
  local csv=$1 method=$2
  awk -F, -v m="$method" 'NR>1 && $1==m {print $10; exit}' "$csv"
}

write_dataflow_results() {
  local outdir=$1 reps_csv=$2
  "$PYTHON" - "$reps_csv" "$outdir" "$QUERY_SEMANTICS" <<'PY'
import csv, statistics, sys
from pathlib import Path

reps_path, outdir, semantics = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3]
methods = ("noprov", "genealog", "green_polynomial", "caps")
query_methods = ("genealog", "green_polynomial", "caps")

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
        return "", "", "", "", ""
    med = statistics.median(vals)
    mean = statistics.mean(vals)
    std = statistics.stdev(vals) if len(vals) >= 2 else None
    fmt = lambda x: "" if x is None else format(x, spec)
    return fmt(med), fmt(mean), fmt(std), fmt(min(vals)), fmt(max(vals))

by_method = {}
with reps_path.open(newline="") as handle:
    for row in csv.DictReader(handle):
        by_method.setdefault(row["method"], []).append(row)

def timer_ns(subdir, csv_name, method, field="avg_ns"):
    path = outdir / subdir / csv_name
    if not path.is_file():
        return ""
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            if row.get("method") == method:
                return (row.get(field) or "").strip()
    return ""

raw_median = {}
for method in query_methods:
    med = summarise(
        [r.get("prov_query_raw_ns") for r in by_method.get(method, [])], ".1f")[0]
    raw_median[method] = med

timer_median = {
    "genealog": timer_ns("genealog_timer", "provenance_query_genealog.csv", "genealog_timer", "median_ns"),
    "caps": timer_ns("caps_timer", "provenance_query.csv", "caps_timer", "median_ns"),
    "green_polynomial": timer_ns("green_timer", "provenance_query_ink.csv", "ink_timer", "median_ns"),
}
raw_sample_median = {}
for method in query_methods:
    raw_sample_median[method] = summarise(
        [r.get("prov_query_sample_median_ns") for r in by_method.get(method, [])], ".1f")[0]

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
        "job_duration_min_s", "job_duration_max_s", "prov_query_min_ns", "prov_query_max_ns",
        "gc_median_s", "gc_min_s", "gc_max_s", "gc_pauses_median", "gc_full_pauses_median", "gc_max_pause_median_s",
        "prov_query_sample_median_ns", "prov_query_sample_median_min_ns", "prov_query_sample_median_max_ns",
    ])
    for method in methods:
        rows = by_method.get(method, [])
        j_med, j_mean, j_std, j_min, j_max = summarise(
            [r.get("job_duration_s") for r in rows], ".3f")
        q_med, q_mean, q_std, q_min, q_max = summarise(
            [r.get("prov_query_raw_ns") for r in rows], ".1f")
        g_med, _, _, g_min, g_max = summarise([r.get("gc_s") for r in rows], ".3f")
        gn_med = summarise([r.get("gc_pauses") for r in rows], ".0f")[0]
        gf_med = summarise([r.get("gc_full_pauses") for r in rows], ".0f")[0]
        gp_med = summarise([r.get("gc_max_pause_s") for r in rows], ".3f")[0]
        sm_med, _, _, sm_min, sm_max = summarise([r.get("prov_query_sample_median_ns") for r in rows], ".1f")
        writer.writerow([method, len(rows), j_med, j_mean, j_std, q_med, q_mean, q_std,
                         j_min, j_max, q_min, q_max, g_med, g_min, g_max, gn_med, gf_med, gp_med,
                         sm_med, sm_min, sm_max])

times_path = outdir / "times.csv"
with times_path.open("w", newline="") as handle:
    writer = csv.writer(handle)
    writer.writerow([
        "method", "runtime_s", "job_duration_s", "prov_query_ns", "query_semantics",
    ])
    for method in methods:
        rows = by_method.get(method, [])
        r_med = summarise([r.get("runtime_s") for r in rows], ".3f")[0]
        j_med = summarise([r.get("job_duration_s") for r in rows], ".3f")[0]
        q_med = summarise([r.get("prov_query_raw_ns") for r in rows], ".1f")[0]
        sem = semantics if method in query_methods else ""
        writer.writerow([method, r_med, j_med, q_med, sem])

query_path = outdir / "query_ns.csv"
with query_path.open("w", newline="") as handle:
    writer = csv.writer(handle)
    writer.writerow(["method", "raw_ns", "timer_ns", "calibrated_ns",
                     "raw_sample_median_ns", "timer_sample_median_ns", "calibrated_sample_median_ns"])
    for method in query_methods:
        raw = raw_median.get(method, "")
        t = timer.get(method, "")
        cal = ""
        if raw and t:
            delta = float(raw) - float(t)
            if delta < 0:
                delta = 0.0
            cal = format(delta, ".1f")
        sm, tm, cal_m = raw_sample_median.get(method, ""), timer_median.get(method, ""), ""
        if sm and tm:
            cal_m = format(max(float(sm) - float(tm), 0.0), ".1f")
        writer.writerow([method, raw, t, cal, sm, tm, cal_m])
PY
}

for dataflow in $DATAFLOWS; do
  echo -e "\n=== $dataflow  REPS=$REPS ==="
  outdir="$RESDIR/$dataflow"
  mkdir -p "$outdir"/{noprov,genealog,caps,green,green_verify,genealog_timer,caps_timer,green_timer}
  reps_csv="$outdir/runtime_reps.csv"
  echo "method,rep,runtime_s,job_duration_s,prov_query_raw_ns,gc_pauses,gc_full_pauses,gc_s,gc_max_pause_s,prov_query_sample_median_ns" > "$reps_csv"
  rm -f "$outdir"/{times.csv,times_stats.csv,query_ns.csv,memory_model.csv,memory_model_detail.csv}
  rm -f "$outdir"/{job.json,provenance_query.csv,provenance_query_genealog.csv}
  rm -f "$outdir"/{provenance_query_ink.csv,metadata_ink.csv}
  rm -f "$outdir"/{outputs_caps.txt,outputs_genealog.txt,outputs_ink.txt}
  rm -f "$outdir"/selected_samples_*.csv
  rm -rf "$outdir/genealog"/* "$outdir/noprov"/* "$outdir/caps"/* "$outdir/green"/*

  if has_method noprov; then
    # --- noprov ---
    for ((rep=1; rep<=REPS; rep++)); do
      echo "  --- noprov rep $rep/$REPS ---"
      timed_run "$outdir/noprov/rep${rep}_flink.log" "$outdir/noprov/rep${rep}_job.json" \
        run_noprov_fair "$dataflow"
      echo "noprov,$rep,$TIMED_WALL,$TIMED_JOB,,$TIMED_GC_N,$TIMED_GC_FULL,$TIMED_GC_S,$TIMED_GC_MAX_S," >> "$reps_csv"
    done
  fi

  if has_method genealog; then
    # --- genealog ---
    for ((rep=1; rep<=REPS; rep++)); do
      echo "  --- genealog rep $rep/$REPS ---"
      rm -rf "$outdir/genealog"/*
      rm -f "$outdir/provenance_query_genealog.csv"
      timed_run "$outdir/genealog/rep${rep}_flink.log" "$outdir/genealog/rep${rep}_job.json" \
        run_genealog "$dataflow" "$outdir/genealog" sortedPtr \
          --traversalBenchDir "$outdir" --traversalSampleProb "${SAMPLE_PROB[$dataflow]}"
      gen_ns=$(read_query_ns "$outdir/provenance_query_genealog.csv" genealog)
      gen_med=$(read_query_median "$outdir/provenance_query_genealog.csv" genealog)
      echo "genealog,$rep,$TIMED_WALL,$TIMED_JOB,$gen_ns,$TIMED_GC_N,$TIMED_GC_FULL,$TIMED_GC_S,$TIMED_GC_MAX_S,$gen_med" >> "$reps_csv"
    done
    [[ -s "$outdir/outputs_genealog.txt" ]] || {
      echo "missing result file: $outdir/outputs_genealog.txt" >&2; exit 1; }
    gen_outputs=$(<"$outdir/outputs_genealog.txt")
    last_gen_job="$outdir/genealog/rep${REPS}_job.json"
    gen_bytes=$(genealog_bytes "$last_gen_job" "$gen_outputs")
  fi

  if has_method caps; then
    # --- caps ---
    for ((rep=1; rep<=REPS; rep++)); do
      echo "  --- caps rep $rep/$REPS ---"
      rm -f "$outdir/provenance_query.csv"
      timed_run "$outdir/caps/rep${rep}_flink.log" "$outdir/caps/rep${rep}_job.json" \
        run_caps "$dataflow" provenance "$outdir" "${SAMPLE_PROB[$dataflow]}"
      caps_ns=$(read_query_ns "$outdir/provenance_query.csv" caps)
      caps_med=$(read_query_median "$outdir/provenance_query.csv" caps)
      echo "caps,$rep,$TIMED_WALL,$TIMED_JOB,$caps_ns,$TIMED_GC_N,$TIMED_GC_FULL,$TIMED_GC_S,$TIMED_GC_MAX_S,$caps_med" >> "$reps_csv"
    done
    [[ -s "$outdir/outputs_caps.txt" ]] || {
      echo "missing result file: $outdir/outputs_caps.txt" >&2; exit 1; }
    caps_outputs=$(<"$outdir/outputs_caps.txt")
    caps_bytes=$((caps_outputs * ${CHANNELS[$dataflow]} * 8))
  fi

  if has_method green_polynomial; then
    # --- green_polynomial ---
    for ((rep=1; rep<=REPS; rep++)); do
      echo "  --- green_polynomial rep $rep/$REPS ---"
      rm -f "$outdir/provenance_query_ink.csv"
      timed_run "$outdir/green/rep${rep}_flink.log" "$outdir/green/rep${rep}_job.json" \
        run_green "$dataflow" provenance "$outdir" "${SAMPLE_PROB[$dataflow]}"
      green_ns=$(read_query_ns "$outdir/provenance_query_ink.csv" ink)
      green_med=$(read_query_median "$outdir/provenance_query_ink.csv" ink)
      echo "green_polynomial,$rep,$TIMED_WALL,$TIMED_JOB,$green_ns,$TIMED_GC_N,$TIMED_GC_FULL,$TIMED_GC_S,$TIMED_GC_MAX_S,$green_med" >> "$reps_csv"
    done

    # Green memory model: same byte convention as how_much_ink (metadata_ink.csv).
    # A seperate verify submission; timing mode does not write that file.
    with_cluster run_green "$dataflow" provenance_verify "$outdir" "${SAMPLE_PROB[$dataflow]}" \
      > "$outdir/green_verify/flink.log" 2>&1
    [[ -s "$outdir/metadata_ink.csv" ]] || {
      echo "missing result file: $outdir/metadata_ink.csv" >&2; exit 1; }
    green_bytes=$(awk -F, 'NR==2 {print $5; exit}' "$outdir/metadata_ink.csv")
  fi

  # Timer calibration runs once; bracket cost is subtracted in query_ns.csv.
  rm -rf "$outdir/genealog_timer"/* "$outdir/caps_timer"/* "$outdir/green_timer"/*
  mkdir -p "$outdir/genealog_timer" "$outdir/caps_timer" "$outdir/green_timer"
  if has_method genealog; then
    timed_run "$outdir/genealog_timer/flink.log" "$outdir/genealog_timer/job.json" \
      run_genealog "$dataflow" "$outdir/genealog_timer" sortedPtr \
        --traversalBenchDir "$outdir/genealog_timer" \
        --traversalSampleProb "${SAMPLE_PROB[$dataflow]}" \
        --traversalCalibrate
  fi
  if has_method caps; then
    timed_run "$outdir/caps_timer/flink.log" "$outdir/caps_timer/job.json" \
      run_caps "$dataflow" provenance_timer "$outdir/caps_timer" "${SAMPLE_PROB[$dataflow]}"
  fi
  if has_method green_polynomial; then
    timed_run "$outdir/green_timer/flink.log" "$outdir/green_timer/job.json" \
      run_green "$dataflow" provenance_timer "$outdir/green_timer" "${SAMPLE_PROB[$dataflow]}"
  fi

  write_dataflow_results "$outdir" "$reps_csv"

  if has_method genealog && has_method caps && has_method green_polynomial; then
  { echo "method,bytes"
    echo "noprov,0"
    echo "genealog,$gen_bytes"
    echo "green_polynomial,$green_bytes"
    echo "caps,$caps_bytes"; } > "$outdir/memory_model.csv"

  # The same bytes split in sink / intermediate / window state, which is what
  # the memory table of the paper reports and what the figure stacks.
  "$PYTHON" "$EXPDIR/../memory_model.py" \
    --dataflow "$dataflow" --out "$outdir/memory_model_detail.csv" \
    --channels "${CHANNELS[$dataflow]}" \
    --caps-job "$outdir/caps/rep${REPS}_job.json" \
    --genealog-job "$last_gen_job" \
    --caps-outputs "$caps_outputs" --genealog-outputs "$gen_outputs" \
    --ink-meta "$outdir/metadata_ink.csv"
  fi

  for name in times.csv times_stats.csv query_ns.csv; do
    [[ -s "$outdir/$name" ]] || {
      echo "missing result file: $outdir/$name" >&2; exit 1; }
  done

  echo "  noprov:   $(awk -F, '$1=="noprov" {print $3; exit}' "$outdir/times.csv")s"
  echo "  genealog: $(awk -F, '$1=="genealog" {print $3; exit}' "$outdir/times.csv")s, query $(awk -F, '$1=="genealog" {print $4; exit}' "$outdir/times.csv")ns, meta-data ${gen_bytes:-n/a}B"
  echo "  green:    $(awk -F, '$1=="green_polynomial" {print $3; exit}' "$outdir/times.csv")s, query $(awk -F, '$1=="green_polynomial" {print $4; exit}' "$outdir/times.csv")ns, meta-data ${green_bytes:-n/a}B"
  echo "  caps:     $(awk -F, '$1=="caps" {print $3; exit}' "$outdir/times.csv")s, query $(awk -F, '$1=="caps" {print $4; exit}' "$outdir/times.csv")ns, meta-data ${caps_bytes:-n/a}B"
done

combine_results "$RESDIR" times.csv times_stats.csv memory_model.csv memory_model_detail.csv query_ns.csv runtime_reps.csv

echo -e "\ntimes.csv:";        cat "$RESDIR/times.csv"
echo -e "\ntimes_stats.csv:";  cat "$RESDIR/times_stats.csv"
[[ -s "$RESDIR/memory_model.csv" ]] && { echo -e "\nmemory_model.csv:"; cat "$RESDIR/memory_model.csv"; }
echo -e "\nmemory_model_detail.csv:"; cat "$RESDIR/memory_model_detail.csv" || true
echo -e "\nquery_ns.csv:";     cat "$RESDIR/query_ns.csv"
echo -e "\n[table and figures]"
"$PYTHON" "$EXPDIR/../tables.py" runtime "$RESDIR"
plot_experiment "$RESDIR"

echo -e "\nDone. Outputs in $RESDIR/"

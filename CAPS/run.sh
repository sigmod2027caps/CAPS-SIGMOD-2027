#!/usr/bin/env bash
# Runs everything: installs Flink, creates the Python virtualenv, builds the input datasets, verifies the four
# methods against each other, then runs every experiment of the paper. Each experiment builds
# CAPS and the competitors itself and writes its result CSVs, LaTeX tables and figures (PDF). Each step is skipped
# when its output is already in place, so re-running is cheap.
#
#   ./run.sh                          all six dataflows, REPS = 3: the numbers of the paper (hours, see readme.md)
#   SMALL=1 REPS=1 ./run.sh           quick check on the 10% subsets (about 3.5 h)
#   PHASES=plot ./run.sh              after a run: redraw every figure and table from its results
#   EXPERIMENTS="queries expiry" ./run.sh        only some of the experiments below
#   DATAFLOWS="nexmark_1 taxi_2" ./run.sh        only some dataflows (only the datasets they need are built)
#
# Environment (everything has a default): DATAFLOWS, REPS, SMALL, EXPERIMENTS, PHASES=plot, CAPS_PARALLELISM,
# FLINK_DIR, PYTHON, and the switches of the single experiments (readme.md, experiments/README.md).
# SMALL=1 never overwrites the results of a full run: the 8 original experiments write into the subdirectory small/
# of their experiment directory, the other experiments into their results_small* / *_small* files and directories.
# TM_MEMORY (default 48g; 16g with SMALL=1; less on a machine with less RAM) is the size of the Flink task manager when install_flink.sh installs
# Flink; an installation that is already there keeps its flink-conf.yaml (edit taskmanager.memory.process.size).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
EXP="$ROOT/experiments"
PYTHON="${PYTHON:-python3}"

# Dataflows. Twitter: the real tweets (100K) are part of the artifact (data/tweets/twitter.txt).
# One dataflow per line; comment out the ones you do not want. The environment variable DATAFLOWS (a space-separated
# string) overrides the list, and is what the experiments see: they take the list from the environment and use their
# own list (experiments/*/run.sh) only when they are started alone.
DATAFLOW_LIST=(
  nexmark_1
  nexmark_2
  taxi_1
  taxi_2
  twitter_1
  twitter_2
)
if [[ -n "${DATAFLOWS:-}" ]]; then DATAFLOW_LIST=($DATAFLOWS); fi
DATAFLOWS="${DATAFLOW_LIST[*]}"
export DATAFLOWS

# Timed runs are repeated and the median is reported (min-max in the figures). The paper uses 3.
REPS="${REPS:-3}"
export REPS

SMALL="${SMALL:-0}"
export SMALL
# Task manager memory for a fresh Flink installation (install_flink.sh): the quick check needs far less than the full runs.
# A machine with less RAM than that plus 8 GB for the rest gets a smaller task manager.
if [[ -z ${TM_MEMORY:-} ]]; then
  tm_gb=48; [[ $SMALL == 1 ]] && tm_gb=16
  ram_gb=$(awk '/MemTotal/ {print int($2 / 1048576)}' /proc/meminfo 2>/dev/null || echo 64)
  fit_gb=$(( ram_gb > 16 ? ram_gb - 8 : ram_gb / 2 ))
  if (( fit_gb < tm_gb )); then
    echo "this machine has $ram_gb GB of RAM: the Flink task manager gets ${fit_gb}g instead of ${tm_gb}g"
    tm_gb=$fit_gb
  fi
  export TM_MEMORY="${tm_gb}g"
fi

if [[ " ${PHASES:-} " == *" plot "* ]]; then PLOT_ONLY=1; else PLOT_ONLY=0; fi
if (( PLOT_ONLY )) && ! find "$EXP" -name '*.csv' -not -path '*/work*' | grep -q .; then
  echo "PHASES=plot redraws the figures and tables of an earlier run, but experiments/ holds no results yet." >&2
  echo "Run ./run.sh (or SMALL=1 REPS=1 ./run.sh) first." >&2
  exit 1
fi

needs() {
  local prefix=$1 dataflow
  for dataflow in $DATAFLOWS; do
    [[ $dataflow == $prefix* ]] && return 0
  done
  return 1
}

# The experiments of the paper (Section 7), in the order of the readme. See experiments/README.md.
if [[ -z "${EXPERIMENTS:-}" ]]; then
  EXPERIMENTS="verify index_check how_much memory query_table queries expiry summarize query_length paths index_overhead channel_scaling nested_paths channel_scaling_real max_count live_memory heap_check disorder disorder_paced recovery"
fi

wants() { [[ " $EXPERIMENTS " == *" $1 "* ]]; }

# pick <dataflow>...: the dataflows of DATAFLOWS that are in the given list (the quick check runs the heavy experiments
# on two dataflows).
pick() {
  local out="" dataflow
  for dataflow in "$@"; do
    [[ " $DATAFLOWS " == *" $dataflow "* ]] && out="$out $dataflow"
  done
  echo $out
}

echo "===== Setup ====="

# Python first: the plotting scripts and the generators need it.
if [[ ! -x "$ROOT/venv/bin/python" ]]; then
  echo "creating the Python virtualenv ..."
  if ! "$PYTHON" -m venv "$ROOT/venv" >/dev/null 2>&1; then
    # Debian/Ubuntu without python3-venv: a virtualenv without pip, then pip from bootstrap.pypa.io
    rm -rf "$ROOT/venv"
    "$PYTHON" -m venv --without-pip "$ROOT/venv"
    curl -fsSL https://bootstrap.pypa.io/get-pip.py | "$ROOT/venv/bin/python" - --quiet
  fi
  "$ROOT/venv/bin/python" -m pip install --quiet --upgrade pip
  "$ROOT/venv/bin/python" -m pip install --quiet -r "$ROOT/requirements.txt"
else
  echo "Python virtualenv already present"
fi
PYTHON="$ROOT/venv/bin/python"
export PYTHON

if (( ! PLOT_ONLY )); then
  # Java 8, Maven and 7-Zip: the ones of the machine, or downloaded into tools/ when missing.
  source "$ROOT/bootstrap_tools.sh"

  # Set FLINK_DIR if you have a Flink 1.10 already; then nothing is downloaded.
  if [[ -n "${FLINK_DIR:-}" && -x "${FLINK_DIR}/bin/flink" ]]; then
    echo "using the Flink at $FLINK_DIR"
  elif [[ ! -x "$ROOT/code/flink-1.10.0/bin/flink" ]]; then
    "$ROOT/install_flink.sh"
  else
    echo "Flink 1.10 already installed"
  fi

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
      echo "building the NYC taxi input (downloads ~5.8 GB, needs 7z) ..."
      (cd "$ROOT/data/taxis" && ./download.sh && "$PYTHON" make_taxis_txt.py)
    else
      echo "NYC taxi input already present"
    fi
  fi

  if needs twitter; then
    if [[ -s "$ROOT/data/tweets/twitter.txt" ]]; then
      echo "Twitter input present (data/tweets/twitter.txt)"
    else
      echo "missing data/tweets/twitter.txt (part of the artifact)" >&2
      exit 1
    fi
  fi

  if [[ $SMALL == 1 ]]; then
    echo "building the 10% subsets (data/small) ..."
    "$PYTHON" "$ROOT/data/make_small.py"
  fi
fi

# What the quick check (SMALL=1) leaves out of the heavy experiments, so that it stays short.
# Override by setting the variables yourself.
if [[ $SMALL == 1 ]]; then
  export E3_DATAFLOWS="${E3_DATAFLOWS:-$(pick taxi_2 twitter_2)}"
  export ARMS="${ARMS:-D p sink tier1}"
  export PARALLELISMS="${PARALLELISMS:-1 2}"
  export E4_DATAFLOWS="${E4_DATAFLOWS:-$(pick taxi_2 twitter_2)}"
  export M_LIST="${M_LIST:-1 8 64}"
else
  export E3_DATAFLOWS="${E3_DATAFLOWS:-$DATAFLOWS}"   # Table 3 and Section 7.7 cover all six dataflows
  export E4_DATAFLOWS="${E4_DATAFLOWS:-$DATAFLOWS}"
  export LIVE_REPS="${LIVE_REPS:-2}"
  export PARALLELISMS="${PARALLELISMS:-1}"
fi

# run_experiment <name> <command...>: the cross-method verification, then every experiment of
# the paper. The cluster is started and stopped by the experiments themselves (one Flink job at a time, under
# `flock -o /tmp/caps-flink.lock`).
# A failed experiment does not stop the run: its output is kept in logs/<name>.log, its last lines and those of the
# newest Flink log are printed, and the failures are listed at the end.
FAILED=()
run_experiment() {
  local name=$1; shift
  wants "$name" || return 0
  [[ $name == verify || $name == index_check ]] && (( PLOT_ONLY )) && return 0   # checks, no figures to redraw
  echo
  echo "===== Running $name ====="
  mkdir -p "$ROOT/logs"
  local t0=$SECONDS log="$ROOT/logs/$name.log" rc=0 flog
  touch "$log.start"
  # tee in a process substitution: a helper process left running by an experiment cannot hold the run up
  "$@" > >(tee "$log") 2>&1 || rc=$?
  if (( rc )); then
    sleep 1   # let tee write the last lines
    FAILED+=("$name")
    echo "!!!!! $name FAILED (exit $rc). Last lines of its output (all of it: logs/$name.log):"
    tail -n 30 "$log"
    flog=$(find "$ROOT/code/flink-1.10.0/log" "$EXP" -name '*.log' -newer "$log.start" 2>/dev/null | xargs -r ls -t | head -n 1 || true)
    if [[ -n $flog ]]; then
      echo "!!!!! Last lines of ${flog#$ROOT/}:"
      tail -n 30 "$flog"
    fi
  fi
  rm -f "$log.start"
  printf '[%s: %d min %d s]\n' "$name" $(((SECONDS - t0) / 60)) $(((SECONDS - t0) % 60))
}

# Correctness first. verify: the four methods produce the same outputs and how-much counts on every dataflow.
# index_check: the two-tier index answers like the array scan, like GeneaLog and Green, and like the released index,
# at the parallelisms in PARALLELISMS. Neither is timed.
run_experiment verify      "$EXP/verify/run.sh"
run_experiment index_check "$EXP/verify/index_check.sh"

run_experiment how_much     "$EXP/how_much/run.sh"
run_experiment memory       "$EXP/memory/run.sh"
run_experiment query_table  "$EXP/query_table/run.sh"
run_experiment queries      "$EXP/queries/run.sh"
run_experiment expiry       "$EXP/expiry/run.sh"
run_experiment summarize    "$EXP/summarize/run.sh"
run_experiment query_length "$EXP/query_length/run.sh"
run_experiment paths        "$EXP/paths/run.sh"
run_experiment index_overhead "$EXP/index_overhead/run.sh"   # capture only against capture + online index

# Channel scaling (Section 7.5: branch test, then Figure 11 on the real taxi input), the largest counter (Section 4.3), live memory,
# disorder and recovery. The last two take their dataflows from E3_DATAFLOWS / E4_DATAFLOWS above.
run_experiment channel_scaling "$EXP/channel_scaling/run.sh"
run_experiment nested_paths    "$EXP/nested_paths/run.sh"
if needs taxi; then run_experiment channel_scaling_real "$EXP/channel_scaling_real/run.sh"; fi   # Figure 11 (taxi input)
run_experiment max_count      "$EXP/max_count/run.sh"
run_experiment live_memory     "$EXP/live_memory/run.sh"
run_experiment heap_check      "$EXP/heap_check/run.sh"      # the JVM heap next to the counted live state
run_experiment disorder        "$EXP/disorder/run.sh"
# the paced D and delta arms (delta below, at and above D). Serial it takes 7.4 h of paced run time on the full data
# (it was run as 6 instances in parallel, see experiments/README.md); in the quick check REPS = 1 keeps it short.
run_experiment disorder_paced  env PACED=1 ARMS="D delta" PHASES="${PHASES:-late}" "$EXP/disorder/run.sh"
run_experiment recovery        "$EXP/recovery/run.sh"

echo
echo "========================================"
echo "Experiment results"
echo "========================================"

# Where the 8 original experiments write their results (the same as results_dir of experiments/common.sh).
results_dir() { if [[ $SMALL == 1 ]]; then echo "$1/small"; else echo "$1"; fi; }
result() { echo "$(results_dir "$EXP/$1")/$2"; }   # result <experiment> <file>

print_result() {
  local label=$1
  local file=$2

  echo
  echo "[$label] ${file#$ROOT/}"
  if [[ -s "$file" ]]; then
    cat "$file"
  else
    echo "ERROR: missing or empty result file" >&2
    MISSING=1
  fi
}

# The summary of the paper's experiments (the others write their tables and plots under their own directory).
if wants how_much; then
  print_result "How much: runtime and query latency" "$(result how_much times.csv)"
  print_result "How much: metadata memory (analytical model)" "$(result how_much memory_model.csv)"
fi
if wants memory; then
  # The measured counterpart of the model above: what the probes actually counted.
  print_result "How much: metadata volume (measured by the probes)" "$(result memory metadata_volume.csv)"
  print_result "How much: metadata volume, MB per method" "$(result memory how_much_comparison.csv)"
fi
wants query_table && print_result "Provenance query latency, shared 1000 outputs (JMH)" "$(result query_table query_table.csv)"
if wants queries; then
  print_result "Temporal queries: query latency" "$(result queries query_latency.csv)"
  print_result "Temporal queries: insertion latency" "$(result queries insert_latency.csv)"
  print_result "Temporal queries: index statistics" "$(result queries index_stats.csv)"
fi
if wants expiry; then
  print_result "Expiry: statistics" "$(result expiry expiry_stats.csv)"
  print_result "Expiry: memory over time" "$(result expiry expiry_memory.csv)"
fi
wants summarize && print_result "Summarization" "$(result summarize summarize.csv)"
wants query_length && print_result "Query latency by window length" "$(result query_length query_length_latency.csv)"
# taxi_1 and twitter_1 are the only dataflows that refine sources into paths, so this one can legitimately have no
# results when DATAFLOWS does not select them.
if wants paths && [[ -s "$(result paths paths.csv)" ]]; then
  print_result "Per source vs per path counters" "$(result paths paths.csv)"
fi

echo
echo "Figures (PDF), LaTeX tables and CSVs are next to each other in the directory of each experiment (in its small/ with SMALL=1);"
echo "experiments/README.md maps every figure and table of the paper to its experiment."
if (( ${#FAILED[@]} )); then
  echo
  echo "FAILED experiments: ${FAILED[*]} (their output is in logs/)"
  exit 1
fi
(( ${MISSING:-0} )) && exit 1
exit 0

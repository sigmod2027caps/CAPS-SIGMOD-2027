#!/usr/bin/env bash
# Shared configuration and helpers for the experiment scripts.
#
# Every experiment runs the same dataflows and differs only in the mode it
# passes to the job. This file holds the per-dataflow parameters and the
# handful of functions all the run.sh scripts need, so that adding a dataflow
# means editing one table.
#
# The six dataflows of the paper are all here. The two Twitter dataflows read the
# real tweets (data/tweets/twitter.txt, included in the artifact; data/tweets/README.txt).
# The run.sh of the experiments default to all six; the fallback below (all six) is for
# scripts that set nothing.
#
# Environment variables understood by every experiment:
#   DATAFLOWS         space-separated subset to run
#   REPS              repetitions of every timed run (default: 3, median reported)
#   FLINK_DIR         an existing Flink 1.10 installation (default: code/flink-1.10.0)
#   SMALL             1 reads the 10% subsets under data/small instead of data/
#                     (and scales SAMPLE_PROB to match); for quick checks. The results of a SMALL=1
#                     run never overwrite those of a full run: see results_dir below
#   CAPS_PARALLELISM  parallelism of every job of every method (default: 1)
#   PHASES=plot       only redraw figures and tables from the results of an earlier run (plot_only below)
#
# The default is parallelism one, as in the paper: the cross-method output order is then
# deterministic. The sources are parallel readers (CAPS_SOURCE_PARALLELISM overrides) and, like
# the index sinks, run at any parallelism. The index sinks (CAPS, Green and the GeneaLog index
# baseline) keep one two-tier index shard per sink subtask (watermark-driven Tier 1,
# append-only Tier 2); the last subtask to close merges the shards and runs the benchmark
# queries (experiments/verify/index_check.sh checks that the answers do not depend on the
# parallelism). The verification file sinks stay on one subtask.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

ALL_DATAFLOWS="nexmark_1 nexmark_2 taxi_1 taxi_2 twitter_1 twitter_2"
DATAFLOWS="${DATAFLOWS:-$ALL_DATAFLOWS}"

export CAPS_PARALLELISM="${CAPS_PARALLELISM:-1}"

# SMALL=1 swaps the input root for the 10% subsets (same file names).
SMALL="${SMALL:-0}"
if [[ $SMALL == 1 ]]; then
  DATA_ROOT="$ROOT/data/small"
else
  DATA_ROOT="$ROOT/data"
fi

# results_dir <experiment dir>: where an experiment of the paper (how_much, memory, query_table, queries, expiry,
# summarize, query_length, paths) writes its per-dataflow directories, combined CSVs, tables and figures: the experiment
# directory itself, or its subdirectory small/ with SMALL=1, so that a quick check never overwrites the full-data
# results. (The experiments of the revision keep results_small*, work_small* directories of their own.)
results_dir() { if [[ $SMALL == 1 ]]; then echo "$1/small"; else echo "$1"; fi; }

# Every timed run is repeated and the median is what we report, because one
# run of a JVM tells you mostly about the JVM.
REPS="${REPS:-3}"

# PHASES=plot (or PLOT_ONLY=1): regenerate the combined CSVs, the tables and the figures of an experiment from the
# result files that are already there. No build, no cluster, no input is needed. Every run.sh checks plot_only
# right after it has sourced this file.
PLOT_ONLY="${PLOT_ONLY:-0}"
[[ " ${PHASES:-} " == *" plot "* ]] && PLOT_ONLY=1
plot_only() { [[ $PLOT_ONLY == 1 ]]; }

FLINK_DIR="${FLINK_DIR:-$ROOT/code/flink-1.10.0}"
FLINK="$FLINK_DIR/bin/flink"
FLINK_REST="${FLINK_REST:-http://localhost:8081}"

# The top-level run.sh creates venv/ and installs requirements.txt into it.
if [[ -z "${PYTHON:-}" && -x "$ROOT/venv/bin/python" ]]; then
  PYTHON="$ROOT/venv/bin/python"
fi
PYTHON="${PYTHON:-python3}"

# tools/ holds the JDK 8, Maven and 7-Zip that ./run.sh downloads when the machine has none, so that an
# experiment started on its own finds them too
[[ -z ${JAVA_HOME:-} && -x $ROOT/tools/jdk8/bin/java ]] && export JAVA_HOME="$ROOT/tools/jdk8"
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-8-openjdk-amd64}"
for d in "$ROOT/tools/maven/bin" "$ROOT/tools/bin"; do [[ -d $d && ":$PATH:" != *":$d:"* ]] && PATH="$d:$PATH"; done
export PATH="$JAVA_HOME/bin:$PATH"

# Main class of the CAPS job of each dataflow.
declare -A CLASS=(
  [nexmark_1]=NexmarkMonitorNewUsersCaps
  [nexmark_2]=NexmarkLocalItemSuggestionCaps
  [taxi_1]=TaxiPassengerJoinCaps
  [taxi_2]=TaxiPaymentAvgCaps
  [twitter_1]=TwitterVerifiedJoinCaps
  [twitter_2]=TwitterLocationAvgCaps
)

# Main class of the GeneaLog job of each dataflow, inside the Ananke jar.
declare -A GENEALOG_CLASS=(
  [nexmark_1]=io.palyvos.provenance.usecases.nexmark.provenance.queries.NexmarkMonitorNewUsers
  [nexmark_2]=io.palyvos.provenance.usecases.nexmark.provenance.queries.NexmarkLocalItemSuggestion
  [taxi_1]=io.palyvos.provenance.usecases.taxi.provenance.queries.TaxiPassengerJoin
  [taxi_2]=io.palyvos.provenance.usecases.taxi.provenance.queries.TaxiPaymentAvg
  [twitter_1]=io.palyvos.provenance.usecases.twitter.provenance.queries.TwitterVerifiedJoin
  [twitter_2]=io.palyvos.provenance.usecases.twitter.provenance.queries.TwitterLocationAvg
)

# Main class of the no-provenance baseline: the same computation with no
# meta-data attached, which is what the runtime overhead is measured against.
declare -A NOPROV_CLASS=(
  [nexmark_1]=NexmarkMonitorNewUsers
  [nexmark_2]=NexmarkLocalItemSuggestion
  [taxi_1]=TaxiPassengerJoin
  [taxi_2]=TaxiPaymentAvg
  [twitter_1]=TwitterVerifiedJoin
  [twitter_2]=TwitterLocationAvg
)

# Number of input streams the GeneaLog job reads.
declare -A SOURCES=(
  [nexmark_1]=2 [nexmark_2]=2 [taxi_1]=1 [taxi_2]=1 [twitter_1]=1 [twitter_2]=1
)

# Shaded jar produced by each dataflow's pom. The caps and noprov projects of a
# dataflow share the artifact name, so one table covers both.
declare -A JAR=(
  [nexmark_1]=nexmark-1-1.0-REV-SNAPSHOT.jar
  [nexmark_2]=nexmark-2-1.0-REV-SNAPSHOT.jar
  [taxi_1]=taxi-1-1.0-REV-SNAPSHOT.jar
  [taxi_2]=taxi-2-1.0-REV-SNAPSHOT.jar
  [twitter_1]=twitter-1-1.0-REV-SNAPSHOT.jar
  [twitter_2]=twitter-2-1.0-REV-SNAPSHOT.jar
)

# How-much provenance channels: one counter per channel rides on every output.
# taxi_1 is the paper's intro figure, where the channels are the four
# (virtual source, branch) paths rather than the two sources; twitter_1 has one
# source tweet stream, multiplexed into an all-tweets path and a verified-only
# path, so its two channels are paths as well.
declare -A CHANNELS=(
  [nexmark_1]=2 [nexmark_2]=2 [taxi_1]=4 [taxi_2]=2 [twitter_1]=2 [twitter_2]=2
)

# Per-output sampling coin for the provenance query benchmark, set so that
# about 1,000 outputs are sampled over the run of each dataflow (the outputs are
# 345,793 / 46,910 / 4,693,666 / 13,463,044 / 19,900,122 / 590,888 for nexmark_1, nexmark_2,
# taxi_1, taxi_2, twitter_1 and twitter_2, as counted by experiments/verify).
declare -A SAMPLE_PROB=(
  [nexmark_1]=0.0029   [nexmark_2]=0.021
  [taxi_1]=0.00021     [taxi_2]=0.000074
  [twitter_1]=0.00005  [twitter_2]=0.0017
)

# Sink outputs of every dataflow on the 10% subsets, as counted by
# experiments/verify. SMALL=1 derives SAMPLE_PROB = 1000 / outputs from them, so
# that about 1,000 outputs are still sampled on the smaller runs.
declare -A SMALL_OUTPUTS=(
  [nexmark_1]=34451   [nexmark_2]=4637
  [taxi_1]=655304     [taxi_2]=1625426
  [twitter_1]=2099409 [twitter_2]=62531
)

# Sink outputs of every dataflow on the full data, as counted by experiments/verify. The index
# sinks use the count (CAPS_EXPECTED_OUTPUTS, per run_caps) to run a batch of provisional queries
# every tenth of the outputs; SMALL=1 uses SMALL_OUTPUTS.
declare -A FULL_OUTPUTS=(
  [nexmark_1]=345793   [nexmark_2]=46910
  [taxi_1]=4693666     [taxi_2]=13463044
  [twitter_1]=19900122 [twitter_2]=590888
)

# Temporal query window: ten slides of the dataflow's aggregation window.
declare -A QUERY_LEN_MS=(
  [nexmark_1]=432000000 [nexmark_2]=432000000   # 10 x 12 h tumbling
  [taxi_1]=6000000      [taxi_2]=6000000        # 10 x 10 min slide
  [twitter_1]=3000000   [twitter_2]=36000000    # 10 x 5 min slide, 10 x 1 h slide
)

# Window lengths swept by the query-length experiment.
declare -A QUERY_LENS_MS=(
  [nexmark_1]="43200000,216000000,432000000,2160000000"
  [nexmark_2]="43200000,216000000,432000000,2160000000"
  [taxi_1]="600000,6000000,60000000,600000000"
  [taxi_2]="600000,6000000,60000000,600000000"
  [twitter_1]="300000,3000000,30000000,300000000"
  [twitter_2]="3600000,36000000,360000000,3600000000"
)

# Slide of the last windowed operator of each dataflow. Every output is stamped
# with the right boundary of one of its windows, so its timestamp is one
# millisecond before a multiple of this slide (experiments/verify checks that).
declare -A OUT_SLIDE_MS=(
  [nexmark_1]=43200000 [nexmark_2]=43200000   # 12 h tumbling
  [taxi_1]=600000      [taxi_2]=600000        # 10 min slide
  [twitter_1]=300000   [twitter_2]=3600000    # 5 min slide, 1 h slide
)

# Dataflows in which CAPS refines each source into several paths, with the
# number of counters at source and at path granularity. Only taxi_1 and
# twitter_1 can run the paths experiment; everywhere else the two granularities
# coincide.
declare -A PATH_CHANNELS=(
  [taxi_1]="2 4"               # 2 virtual sources x 2 branches
  [twitter_1]="1 2"            # 1 tweet stream x 2 branches (all, verified)
)

HORIZON_MS=$((7 * 24 * 60 * 60 * 1000))          # expiry / summarize horizon
WIDTHS_MS="3600000,21600000,86400000,604800000"  # 1 h, 6 h, 1 d, 7 d buckets
if [[ $SMALL == 1 ]]; then
  # The subsets span about a tenth of the time (3 to 10 days instead of 22 to 91),
  # shorter than the horizon above, so both shrink by the same factor.
  HORIZON_MS=$((HORIZON_MS / 10))
  WIDTHS_MS="360000,2160000,8640000,60480000"    # 6 min, 36 min, 2.4 h, 16.8 h buckets
fi
NUM_QUERIES="${NUM_QUERIES:-1000}"

TWEETS_BASE="$DATA_ROOT/tweets/twitter"

if [[ $SMALL == 1 ]]; then
  for dataflow in "${!SMALL_OUTPUTS[@]}"; do
    SAMPLE_PROB[$dataflow]=$(awk -v n="${SMALL_OUTPUTS[$dataflow]}" \
      'BEGIN{p = 1000 / n; printf "%.6g", (p > 1 ? 1 : p)}')
  done
fi

# Input files each dataflow's main expects, in order.
dataflow_inputs() {
  case $1 in
    nexmark_*) echo "$DATA_ROOT/nexmark/persons.txt $DATA_ROOT/nexmark/auctions.txt" ;;
    taxi_*)    echo "$DATA_ROOT/taxis/taxis.txt" ;;
    twitter_*) echo "$TWEETS_BASE.txt" ;;
    *) echo "unknown dataflow: $1" >&2; return 1 ;;
  esac
}

# Fails with a pointer to the generator rather than letting Flink die on a
# missing file halfway through a long experiment.
require_inputs() {
  local dataflow=$1 missing=0 file
  for file in $(dataflow_inputs "$dataflow"); do
    if [[ ! -s "$file" ]]; then
      echo "missing input: $file" >&2
      missing=1
    fi
  done
  if (( missing )); then
    case $dataflow in
      nexmark_*) echo "  generate it with: (cd data/nexmark && python3 generate_nexmark.py)" >&2 ;;
      taxi_*)    echo "  build it with:    (cd data/taxis && ./download.sh && python3 make_taxis_txt.py)" >&2 ;;
      twitter_*) echo "  the tweets are part of the artifact (data/tweets/twitter.txt); see data/tweets/README.txt" >&2 ;;
    esac
    return 1
  fi
}

# The Ananke jar takes the input path without its .txt extension and appends
# the suffix itself; the nexmark job derives auctions.txt from persons.
dataflow_input_base() {
  case $1 in
    nexmark_*) echo "$DATA_ROOT/nexmark/persons" ;;
    taxi_*)    echo "$DATA_ROOT/taxis/taxis" ;;
    twitter_*) echo "$TWEETS_BASE" ;;
    *) echo "unknown dataflow: $1" >&2; return 1 ;;
  esac
}

# build_all [--with-competitors] [--with-jmh]   (SKIP_BUILD=1 skips it)
# The competitors are only built by the experiments that compare against them,
# since the Ananke jar is slow to shade.
build_all() {
  # SKIP_BUILD=1: the jars are up to date (one explicit build before a series of experiments)
  [[ ${SKIP_BUILD:-0} == 1 ]] && return 0
  local competitors=0 jmh=0 arg
  for arg in "$@"; do
    [[ $arg == --with-competitors ]] && competitors=1
    [[ $arg == --with-jmh ]] && jmh=1
  done
  echo "[build]"
  # every module is built with "clean": the shade plugin keeps a stale jar otherwise (a changed temporal_index class
  # would not get into the Ananke jar), and a failed build must stop the experiment, not run on the old jar
  mvn_module "$ROOT/code/temporal_index" install || return 1
  mvn_module "$ROOT/code/tiered_index" install || return 1
  mvn_module "$ROOT/code/caps" install || return 1
  local dataflow
  for dataflow in $DATAFLOWS; do
    mvn_module "$ROOT/code/$dataflow" package || return 1
  done
  if (( competitors )); then
    # ananke is installed and not only packaged, because the JMH module asks for it as a normal dependency
    mvn_module "$ROOT/code/genealog" install || return 1
    mvn_module "$ROOT/code/inkstream" install || return 1
    for dataflow in $DATAFLOWS; do
      mvn_module "$ROOT/code/noprov/$dataflow" package || return 1
      mvn_module "$ROOT/code/ink/$dataflow" package || return 1
    done
  fi
  if (( jmh )); then
    mvn_module "$ROOT/code/query_jmh" package || return 1
  fi
}

# mvn_module <dir> <install|package>: a clean build of one module, without its tests
mvn_module() {
  (cd "$1" && mvn -q clean "$2" -DskipTests)
}

restart_cluster() {
  "$FLINK_DIR/bin/stop-cluster.sh" 2>/dev/null || true
  sleep 5
  "$FLINK_DIR/bin/start-cluster.sh"
  sleep 8
}

# One Flink unit = a fresh cluster, the command, and the cluster stopped again, all inside one
# `flock -o` on LOCK_FILE: the machine is shared and only one Flink job may run at a time. The cluster
# daemons are started with the lock descriptor closed (fd 9) so that they do not inherit the lock
# (the reason for `-o` when flock runs a command). The REST API of the cluster is up while the
# command runs, which is where capture_job_json reads the job's operator counts.
#   with_cluster run_caps "$dataflow" queries ... > "$log" 2>&1
LOCK_FILE="${LOCK_FILE:-/tmp/caps-flink.lock}"
with_cluster() {
  local rc=0
  if [[ -z ${CAPS_LOCK_HELD:-} ]]; then   # CAPS_LOCK_HELD=1: the caller holds the lock (flock -o ... run.sh)
    exec 9>"$LOCK_FILE"
    flock 9
  fi
  "$FLINK_DIR/bin/stop-cluster.sh" >/dev/null 2>&1 9>&- || true
  sleep 5
  # CAPS_GC_LOG_DIR=<dir>: the TaskManager of this (fresh) cluster writes a HotSpot GC log gc-<pid>.log there. It is
  # switched on through a private copy of the conf directory, so the installed flink-conf.yaml is never touched and
  # the units of other experiments (which do not set it) keep their JVM flags. capture_gc reads the log.
  local gc_conf=""
  if [[ -n ${CAPS_GC_LOG_DIR:-} ]]; then
    mkdir -p "$CAPS_GC_LOG_DIR"
    gc_conf=$(mktemp -d)
    cp -r "$FLINK_DIR/conf/." "$gc_conf/"
    # taskmanager.sh adds -XX:+UseG1GC only when no env.java.opts.taskmanager is set: keep the collector of every other timed unit
    echo "env.java.opts.taskmanager: -XX:+UseG1GC -Xloggc:$CAPS_GC_LOG_DIR/gc-%p.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps" >> "$gc_conf/flink-conf.yaml"
    FLINK_CONF_DIR="$gc_conf" "$FLINK_DIR/bin/start-cluster.sh" >/dev/null 2>&1 9>&-
  else
    "$FLINK_DIR/bin/start-cluster.sh" >/dev/null 2>&1 9>&-
  fi
  sleep 8
  if cluster_guard; then
    "$@" 9>&- || rc=$?
    # a fresh cluster ran exactly one job: another one would have competed for the CPU, or this one ran elsewhere
    jobs_seen=$(curl -sf "$FLINK_REST/jobs/overview" | "$PYTHON" -c 'import json,sys; print(len(json.load(sys.stdin)["jobs"]))' 2>/dev/null || echo "?")
    if [[ $jobs_seen != 1 ]]; then
      echo "with_cluster: the cluster saw $jobs_seen jobs, expected exactly 1: the timing of this unit is not trustworthy" >&2
      rc=4
    fi
  else
    rc=3
  fi
  "$FLINK_DIR/bin/stop-cluster.sh" >/dev/null 2>&1 9>&- || true
  [[ -n $gc_conf ]] && rm -rf "$gc_conf"
  exec 9>&-
  return $rc
}

# capture_gc <out csv> [skip]: (inside a with_cluster unit started with CAPS_GC_LOG_DIR) the GC pauses of the TaskManager so
# far, without the first <skip> ones: pauses,full_pauses,gc_s,max_pause_s. The TaskManager is fresh and has run exactly one
# job; the caller passes the number of pauses the JVM had at start-up (capture_gc_count just before the job is submitted),
# so this is the GC pause time caused by the job.
capture_gc_count() {
  local pid log
  pid=$(pgrep -f 'taskexecutor.TaskManagerRunner' | head -1 || true)
  log="${CAPS_GC_LOG_DIR:-/nonexistent}/gc-pid$pid.log"
  [[ -s $log ]] && "$PYTHON" -I "$ROOT/experiments/gc_stats.py" "$log" | cut -d, -f1 || echo 0
}

capture_gc() {
  local out=$1 skip=${2:-0} pid log
  pid=$(pgrep -f 'taskexecutor.TaskManagerRunner' | head -1 || true)
  log="${CAPS_GC_LOG_DIR:-/nonexistent}/gc-pid$pid.log"
  if [[ -z $pid || ! -s $log ]]; then
    echo "capture_gc: no GC log for the TaskManager (pid ${pid:-none}) in ${CAPS_GC_LOG_DIR:-?}" >&2
    return 1
  fi
  "$PYTHON" -I "$ROOT/experiments/gc_stats.py" --skip "$skip" "$log" > "$out"
}

# cluster_guard: refuse to time on a shared machine unless the Flink on the REST port is ours and nothing else
# runs. Every Flink process (JobManager, TaskManager, client) must come from FLINK_DIR, the JobManager answering on
# FLINK_REST must be one of them, and it must have no job yet. (Another checkout's cluster on the same ports would
# receive our jobs, or compete with them for the CPU.)
cluster_guard() {
  local foreign
  foreign=$(pgrep -af 'StandaloneSessionClusterEntrypoint|TaskManagerRunner|CliFrontend' | grep -v -- "$FLINK_DIR" || true)
  if [[ -n $foreign ]]; then
    echo "cluster_guard: Flink processes that are not from $FLINK_DIR:" >&2
    echo "$foreign" | cut -c1-200 >&2
    return 1
  fi
  local port=${FLINK_REST##*:} pid
  pid=$(ss -ltnp 2>/dev/null | awk -v p=":$port" '$4 ~ p"$" {print $NF}' | grep -o 'pid=[0-9]*' | head -1 | cut -d= -f2 || true)
  if [[ -z $pid ]] || ! tr '\0' ' ' < "/proc/$pid/cmdline" | grep -q -- "$FLINK_DIR"; then
    echo "cluster_guard: the process on $FLINK_REST (pid ${pid:-none}) is not the JobManager of $FLINK_DIR" >&2
    return 1
  fi
  local n
  n=$(curl -sf "$FLINK_REST/jobs/overview" | "$PYTHON" -c 'import json,sys; print(len(json.load(sys.stdin)["jobs"]))' 2>/dev/null || echo "?")
  if [[ $n != 0 ]]; then
    echo "cluster_guard: $n jobs already on the cluster at $FLINK_REST" >&2
    return 1
  fi
}

# locked <command...>: a command that needs the machine to itself but no cluster (JMH).
locked() {
  if [[ -n ${CAPS_LOCK_HELD:-} ]]; then "$@"; else flock -o "$LOCK_FILE" "$@"; fi
}

require_flink() {
  if [[ ! -x "$FLINK" ]]; then
    echo "Flink 1.10 not found at $FLINK_DIR" >&2
    echo "  install it with: ./install_flink.sh   (or set FLINK_DIR)" >&2
    return 1
  fi
}

# run_caps <dataflow> <mode> [mode args...]
# Submits the CAPS job of a dataflow. The mode argument selects the bench sink:
# provenance | queries | expiry | summarize | query_length.
run_caps() {
  local dataflow=$1; shift
  local expected=${FULL_OUTPUTS[$dataflow]}
  [[ $SMALL == 1 ]] && expected=${SMALL_OUTPUTS[$dataflow]}
  expected=${EXPECTED_OUTPUTS:-$expected}   # E3: a Zipf remapping changes the number of outputs
  CAPS_EXPECTED_OUTPUTS=$expected "$FLINK" run --parallelism "$CAPS_PARALLELISM" --class "${CLASS[$dataflow]}" \
    "$ROOT/code/$dataflow/target/${JAR[$dataflow]}" \
    $(dataflow_inputs "$dataflow") \
    unused \
    "$@"
}

# run_noprov <dataflow>
# The same computation with no provenance meta-data at all.
run_noprov() {
  local dataflow=$1
  "$FLINK" run --parallelism "$CAPS_PARALLELISM" --class "${NOPROV_CLASS[$dataflow]}" \
    "$ROOT/code/noprov/$dataflow/target/${JAR[$dataflow]}" \
    $(dataflow_inputs "$dataflow") \
    none
}

# run_genealog <dataflow> <statistics dir> <aggregate strategy> [extra args...]
# Submits the GeneaLog job of a dataflow. The aggregate strategy is sortedPtr
# for the capture-overhead runs and list for the ones that build an index.
run_genealog() {
  local dataflow=$1 statdir=$2 strategy=$3; shift 3
  "$FLINK" run --parallelism "$CAPS_PARALLELISM" --class "${GENEALOG_CLASS[$dataflow]}" \
    "$ROOT/code/genealog/target/ananke-1.0-REV-SNAPSHOT.jar" \
    --statisticsFolder "$statdir" \
    --outputFile sink --inputFile "$(dataflow_input_base "$dataflow")" \
    --sourcesNumber "${SOURCES[$dataflow]}" --sourceRepetitions 1 --autoFlush \
    --provenanceActivator GENEALOG --aggregateStrategy "$strategy" \
    --sinkParallelism 1 \
    "$@"
}

# run_green <dataflow> <mode> [mode args...]
# Submits the Green baseline: the same dataflow, but every tuple carries
# a provenance polynomial of the semiring of Green, Karvounarakis and Tannen
# instead of the counters. The Java package is called ink; the method we publish
# is green_polynomial. Same command line as run_caps, so the two are swappable.
run_green() {
  local dataflow=$1; shift
  "$FLINK" run --parallelism "$CAPS_PARALLELISM" --class "${CLASS[$dataflow]}" \
    "$ROOT/code/ink/$dataflow/target/${JAR[$dataflow]}" \
    $(dataflow_inputs "$dataflow") \
    unused \
    "$@"
}

# The file-output variants of the four methods, used by experiments/verify. Each
# writes every sink output to <outfile>, one row per output:
#   noprov     the data columns
#   caps       the data columns, then one counter per channel
#   green      the same, the polynomial projected onto the channels at the sink
#   genealog   the data columns, then the number of source tuples that the
#              backward traversal of the output's provenance graph reaches
# The sinks stay on one subtask, so the rows of one file are the order in which
# the sink received the outputs.

# run_noprov_file <dataflow> <outfile>
run_noprov_file() {
  local dataflow=$1 outfile=$2
  "$FLINK" run --parallelism "$CAPS_PARALLELISM" --class "${NOPROV_CLASS[$dataflow]}" \
    "$ROOT/code/noprov/$dataflow/target/${JAR[$dataflow]}" \
    $(dataflow_inputs "$dataflow") \
    "$outfile"
}

# run_caps_file <dataflow> <outfile>
run_caps_file() {
  local dataflow=$1 outfile=$2
  "$FLINK" run --parallelism "$CAPS_PARALLELISM" --class "${CLASS[$dataflow]}" \
    "$ROOT/code/$dataflow/target/${JAR[$dataflow]}" \
    $(dataflow_inputs "$dataflow") \
    "$outfile"
}

# run_green_file <dataflow> <outfile>
run_green_file() {
  local dataflow=$1 outfile=$2
  "$FLINK" run --parallelism "$CAPS_PARALLELISM" --class "${CLASS[$dataflow]}" \
    "$ROOT/code/ink/$dataflow/target/${JAR[$dataflow]}" \
    $(dataflow_inputs "$dataflow") \
    "$outfile"
}

# run_genealog_dump <dataflow> <statistics dir> <outfile>
# Same job as run_genealog, with the sink replaced by one that traverses the
# graph of every output and writes the tuple and its count.
run_genealog_dump() {
  local dataflow=$1 statdir=$2 outfile=$3
  run_genealog "$dataflow" "$statdir" "${GENEALOG_STRATEGY:-sortedPtr}" \
    --traversalBenchDir "$statdir" --traversalDumpFile "$outfile"
}

# The three provenance methods write their meta-data volume into their own file,
# so one directory can hold all of them.
declare -A PROBE_CSV=(
  [caps]=caps_metadata_volume.csv
  [green_polynomial]=green_metadata_volume.csv
  [genealog]=genealog_metadata_volume.csv
)

# run_probe <method> <dataflow> <outdir>
# The memory_volume mode: no timing, no queries, no sink file. Every operator
# that carries provenance counts its output records and its meta-data bytes into
# a Flink accumulator, which the JobManager merges, and the job writes the
# per-operator totals on exit. This is the measured memory of the paper, as
# opposed to the analytical model, and it can be audited.
run_probe() {
  local method=$1 dataflow=$2 outdir=$3
  mkdir -p "$outdir"
  case $method in
    caps)             run_caps  "$dataflow" memory_volume "$outdir" ;;
    green_polynomial) run_green "$dataflow" memory_volume "$outdir" ;;
    genealog)         run_genealog "$dataflow" "$outdir" sortedPtr \
                        --metadataVolumeDir "$outdir" ;;
    *) echo "unknown method: $method" >&2; return 1 ;;
  esac
}

# probe_bytes <method> <outdir>
# The TOTAL row of the probe file: cumulative meta-data bytes over the run.
probe_bytes() {
  local method=$1 outdir=$2
  # Separate line on purpose: inside one `local` the subscript is still empty.
  local csv="$outdir/${PROBE_CSV[$method]}"
  if [[ ! -s "$csv" ]]; then
    echo "missing probe output: $csv" >&2
    return 1
  fi
  awk -F, '$2=="TOTAL" {print $4; found=1} END{if(!found) exit 1}' "$csv"
}

# median <numbers...>
# The REPS runs are reported by their median, which is what the paper does.
median() {
  printf '%s\n' "$@" | sort -g | awk '{v[NR]=$1} END{
    if (NR==0) exit 1;
    if (NR%2) printf "%s", v[(NR+1)/2]; else printf "%.3f", (v[NR/2]+v[NR/2+1])/2
  }'
}

# job_duration <job.json>
# Duration the JobManager reports, in seconds. The wall clock of `flink run`
# inludes one to three seconds of client JVM startup, which is not part of the
# dataflow and used to eat the whole overhead of CAPS.
job_duration() {
  local job_json=$1
  [[ -s "$job_json" ]] || return 1
  "$PYTHON" - "$job_json" <<'PY'
import json, sys
job = json.load(open(sys.argv[1]))
duration = job.get("duration")
if duration is None:
    raise SystemExit(1)
print("%.3f" % (int(duration) / 1000.0))
PY
}

# genealog_bytes <job.json> <sink output count>
# Analytical meta-data memory of GeneaLog: every tuple emitted by every
# operator carries the quadruple (uid, U1, U2, next), i.e. 4 x 8 B. The
# per-operator counts come from Flink's own job metrics. The last vertex is
# chained into the sink, so its outputs never appear in write-records and are
# added separately.
genealog_bytes() {
  local job_json=$1 sink_outputs=$2
  "$PYTHON" - "$job_json" "$sink_outputs" <<'PY'
import json, sys
job = json.load(open(sys.argv[1]))
tuples = int(sys.argv[2])
for vertex in job.get("vertices", []):
    tuples += max(vertex.get("metrics", {}).get("write-records", 0), 0)
print(tuples * 4 * 8)
PY
}

# append_combined <dataflow> <per-dataflow csv> <combined csv>
# Appends one dataflow's results to the experiment-wide CSV, prefixing a
# dataflow column. The header is copied from whichever run writes first, so
# the combined file never drifts from what the sinks actually emit.
append_combined() {
  local dataflow=$1 src=$2 dst=$3
  if [[ ! -s "$src" ]]; then
    echo "missing result file: $src" >&2
    return 1
  fi
  if [[ ! -s "$dst" ]]; then
    echo "dataflow,$(head -1 "$src")" > "$dst"
  fi
  tail -n +2 "$src" | sed "s/^/$dataflow,/" >> "$dst"
}

# combine_results <expdir> <csv name>...
# Rebuilds the experiment-wide CSVs from the per-dataflow ones (all dataflows that have the file, in
# alphabetical order), so a rerun of a few dataflows keeps the others' rows.
combine_results() {
  local expdir=$1; shift
  local name dataflow
  for name in "$@"; do
    rm -f "$expdir/$name"
    for dataflow in $ALL_DATAFLOWS; do
      if [[ -s "$expdir/$dataflow/$name" ]]; then
        append_combined "$dataflow" "$expdir/$dataflow/$name" "$expdir/$name"
      fi
    done
  done
}

# Regenerates the figures of an experiment from whatever result directories it
# has. Plotting is optional: a missing matplotlib costs you the PDFs, not the
# CSVs, so an experiment is never failed by it.
plot_experiment() {
  local expdir=$1
  if ! "$PYTHON" -c "import matplotlib" 2>/dev/null; then
    echo "  (skipping figures: no matplotlib; run ./run.sh once to create venv/)"
    return 0
  fi
  "$PYTHON" "$ROOT/experiments/plot_utils.py" "$expdir"
}

# Writes the Flink REST job JSON (per-operator output counts) of the last job
# in the given client log. Input to the analytical memory model.
capture_job_json() {
  local log=$1 out=$2 jobid
  jobid=$(grep -oE 'JobID [0-9a-f]+' "$log" | tail -1 | awk '{print $2}' || true)
  [[ -n "$jobid" ]] && curl -sf "$FLINK_REST/jobs/$jobid" -o "$out" || true
}

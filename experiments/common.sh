#!/usr/bin/env bash
# Shared configuration and helpers for the experiment scripts.
#
# Every experiment runs the same four dataflows and differs only in the mode it
# passes to the job. This file holds the per-dataflow parameters and the
# handful of functions all the run.sh scripts need, so that adding a dataflow
# means editing one table.
#
# The two Twitter dataflows of the paper are not here: the dataset cannot be
# redistributed, so we ship only the dataflows whose input anyone can rebuild
# from public data.
#
# Environment variables understood by every experiment:
#   DATAFLOWS     space-separated subset to run (default: all four)
#   REPS          repetitions of every timed run (default: 3, median reported)
#   FLINK_DIR     an existing Flink 1.10 installation (default: code/flink-1.10.0)
#
# The jobs run with parallelism one, as in the paper: the sources are
# non-parallel file readers, the cross-method output verification needs a
# deterministic output order, and the sink builds a single index that requires
# appends in global timestamp order.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

ALL_DATAFLOWS="nexmark_1 nexmark_2 taxi_1 taxi_2"
DATAFLOWS="${DATAFLOWS:-$ALL_DATAFLOWS}"

# Every timed run is repeated and the median is what we report, because one
# run of a JVM tells you mostly about the JVM.
REPS="${REPS:-3}"

FLINK_DIR="${FLINK_DIR:-$ROOT/code/flink-1.10.0}"
FLINK="$FLINK_DIR/bin/flink"
FLINK_REST="${FLINK_REST:-http://localhost:8081}"

# The top-level run.sh creates venv/ and installs requirements.txt into it.
if [[ -z "${PYTHON:-}" && -x "$ROOT/venv/bin/python" ]]; then
  PYTHON="$ROOT/venv/bin/python"
fi
PYTHON="${PYTHON:-python3}"

export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-8-openjdk-amd64}"
export PATH="$JAVA_HOME/bin:$PATH"

# Main class of the CAPS job of each dataflow.
declare -A CLASS=(
  [nexmark_1]=NexmarkMonitorNewUsersCaps
  [nexmark_2]=NexmarkLocalItemSuggestionCaps
  [taxi_1]=TaxiPassengerJoinCaps
  [taxi_2]=TaxiPaymentAvgCaps
)

# Main class of the GeneaLog job of each dataflow, inside the Ananke jar.
declare -A GENEALOG_CLASS=(
  [nexmark_1]=io.palyvos.provenance.usecases.nexmark.provenance.queries.NexmarkMonitorNewUsers
  [nexmark_2]=io.palyvos.provenance.usecases.nexmark.provenance.queries.NexmarkLocalItemSuggestion
  [taxi_1]=io.palyvos.provenance.usecases.taxi.provenance.queries.TaxiPassengerJoin
  [taxi_2]=io.palyvos.provenance.usecases.taxi.provenance.queries.TaxiPaymentAvg
)

# Main class of the no-provenance baseline: the same computation with no
# meta-data attached, which is what the runtime overhead is measured against.
declare -A NOPROV_CLASS=(
  [nexmark_1]=NexmarkMonitorNewUsers
  [nexmark_2]=NexmarkLocalItemSuggestion
  [taxi_1]=TaxiPassengerJoin
  [taxi_2]=TaxiPaymentAvg
)

# Number of input streams the GeneaLog job reads.
declare -A SOURCES=(
  [nexmark_1]=2 [nexmark_2]=2 [taxi_1]=1 [taxi_2]=1
)

# Shaded jar produced by each dataflow's pom. The caps and noprov projects of a
# dataflow share the artifact name, so one table covers both.
declare -A JAR=(
  [nexmark_1]=nexmark-1-1.0-SNAPSHOT.jar
  [nexmark_2]=nexmark-2-1.0-SNAPSHOT.jar
  [taxi_1]=taxi-1-1.0-SNAPSHOT.jar
  [taxi_2]=taxi-2-1.0-SNAPSHOT.jar
)

# How-much provenance channels: one counter per channel rides on every output.
# taxi_1 is the paper's intro figure, where the channels are the four
# (virtual source, branch) paths rather than the two sources.
declare -A CHANNELS=(
  [nexmark_1]=2 [nexmark_2]=2 [taxi_1]=4 [taxi_2]=2
)

# Per-output sampling coin for the provenance query benchmark, set so that
# about 1,000 outputs are sampled over the run of each dataflow.
declare -A SAMPLE_PROB=(
  [nexmark_1]=0.0029   [nexmark_2]=0.021
  [taxi_1]=0.00021     [taxi_2]=0.000074
)

# Temporal query window: ten slides of the dataflow's aggregation window.
declare -A QUERY_LEN_MS=(
  [nexmark_1]=432000000 [nexmark_2]=432000000   # 10 x 12 h tumbling
  [taxi_1]=6000000      [taxi_2]=6000000        # 10 x 10 min slide
)

# Window lengths swept by the query-length experiment.
declare -A QUERY_LENS_MS=(
  [nexmark_1]="43200000,216000000,432000000,2160000000"
  [nexmark_2]="43200000,216000000,432000000,2160000000"
  [taxi_1]="600000,6000000,60000000,600000000"
  [taxi_2]="600000,6000000,60000000,600000000"
)

# Dataflows in which CAPS refines each source into several paths, with the
# number of counters at source and at path granularity. Only taxi_1 can run the
# paths experiment; everywhere else the two granularities coincide.
declare -A PATH_CHANNELS=(
  [taxi_1]="2 4"               # 2 virtual sources x 2 branches
)

HORIZON_MS=$((7 * 24 * 60 * 60 * 1000))          # expiry / summarize horizon
WIDTHS_MS="3600000,21600000,86400000,604800000"  # 1 h, 6 h, 1 d, 7 d buckets
NUM_QUERIES="${NUM_QUERIES:-1000}"

# Input files each dataflow's main expects, in order.
dataflow_inputs() {
  case $1 in
    nexmark_*) echo "$ROOT/data/nexmark/persons.txt $ROOT/data/nexmark/auctions.txt" ;;
    taxi_*)    echo "$ROOT/data/taxis/taxis.txt" ;;
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
    esac
    return 1
  fi
}

# The Ananke jar takes the input path without its .txt extension and appends
# the suffix itself; the nexmark job derives auctions.txt from persons.
dataflow_input_base() {
  case $1 in
    nexmark_*) echo "$ROOT/data/nexmark/persons" ;;
    taxi_*)    echo "$ROOT/data/taxis/taxis" ;;
    *) echo "unknown dataflow: $1" >&2; return 1 ;;
  esac
}

# build_all [--with-competitors] [--with-jmh]
# The competitors are only built by the experiments that compare against them,
# since the Ananke jar is slow to shade.
build_all() {
  local competitors=0 jmh=0 arg
  for arg in "$@"; do
    [[ $arg == --with-competitors ]] && competitors=1
    [[ $arg == --with-jmh ]] && jmh=1
  done
  echo "[build]"
  (cd "$ROOT/code/temporal_index" && mvn -q install -DskipTests)
  (cd "$ROOT/code/caps" && mvn -q install -DskipTests)
  local dataflow
  for dataflow in $DATAFLOWS; do
    (cd "$ROOT/code/$dataflow" && mvn -q clean package -DskipTests)
  done
  if (( competitors )); then
    # ananke is installed and not only packaged, becasue the JMH module asks
    # for it as a normal dependency.
    (cd "$ROOT/code/genealog" && mvn -q install -DskipTests)
    (cd "$ROOT/code/inkstream" && mvn -q install -DskipTests)
    for dataflow in $DATAFLOWS; do
      (cd "$ROOT/code/noprov/$dataflow" && mvn -q clean package -DskipTests)
      (cd "$ROOT/code/ink/$dataflow" && mvn -q clean package -DskipTests)
    done
  fi
  if (( jmh )); then
    (cd "$ROOT/code/query_jmh" && mvn -q clean package -DskipTests)
  fi
}

restart_cluster() {
  "$FLINK_DIR/bin/stop-cluster.sh" 2>/dev/null || true
  sleep 5
  "$FLINK_DIR/bin/start-cluster.sh"
  sleep 8
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
  "$FLINK" run --parallelism 1 --class "${CLASS[$dataflow]}" \
    "$ROOT/code/$dataflow/target/${JAR[$dataflow]}" \
    $(dataflow_inputs "$dataflow") \
    unused \
    "$@"
}

# run_noprov <dataflow>
# The same computation with no provenance meta-data at all.
run_noprov() {
  local dataflow=$1
  "$FLINK" run --parallelism 1 --class "${NOPROV_CLASS[$dataflow]}" \
    "$ROOT/code/noprov/$dataflow/target/${JAR[$dataflow]}" \
    $(dataflow_inputs "$dataflow") \
    none
}

# run_genealog <dataflow> <statistics dir> <aggregate strategy> [extra args...]
# Submits the GeneaLog job of a dataflow. The aggregate strategy is sortedPtr
# for the capture-overhead runs and list for the ones that build an index.
run_genealog() {
  local dataflow=$1 statdir=$2 strategy=$3; shift 3
  "$FLINK" run --parallelism 1 --class "${GENEALOG_CLASS[$dataflow]}" \
    "$ROOT/code/genealog/target/ananke-1.0-SNAPSHOT.jar" \
    --statisticsFolder "$statdir" \
    --outputFile sink --inputFile "$(dataflow_input_base "$dataflow")" \
    --sourcesNumber "${SOURCES[$dataflow]}" --sourceRepetitions 1 --autoFlush \
    --provenanceActivator GENEALOG --aggregateStrategy "$strategy" \
    --sinkParallelism 1 \
    "$@"
}

# run_green <dataflow> <mode> [mode args...]
# Submits the Green baseline (R2.O3): the same dataflow, but every tuple carries
# a provenance polynomial of the semiring of Green, Karvounarakis and Tannen
# instead of the counters. The Java package is called ink; the method we publish
# is green_polynomial. Same command line as run_caps, so the two are swappable.
run_green() {
  local dataflow=$1; shift
  "$FLINK" run --parallelism 1 --class "${CLASS[$dataflow]}" \
    "$ROOT/code/ink/$dataflow/target/${JAR[$dataflow]}" \
    $(dataflow_inputs "$dataflow") \
    unused \
    "$@"
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
# opposed to the analytical model, and it is what a reviewer can audit.
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

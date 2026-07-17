#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
EXPDIR="$(cd "$(dirname "$0")" && pwd)"
DATAFLOW="nexmark_1"
FLINK_DIR="$ROOT/code/flink-1.10.0"
FLINK="$FLINK_DIR/bin/flink"
CHANNELS=2
FLINK_REST="http://localhost:8081"
SAMPLE_PROB=0.0029

export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-8-openjdk-amd64}"
export PATH="$JAVA_HOME/bin:$PATH"

mkdir -p "$EXPDIR/caps"
echo "method,seconds" > "$EXPDIR/runtime.csv"
echo "method,bytes"   > "$EXPDIR/memory_model.csv"
rm -f "$EXPDIR"/{times.csv,provenance_query.csv}
rm -f "$EXPDIR"/provenance_answers.csv
rm -f "$EXPDIR"/outputs_caps.txt

restart_cluster() {
  "$FLINK_DIR/bin/stop-cluster.sh" 2>/dev/null || true
  sleep 5
  "$FLINK_DIR/bin/start-cluster.sh"
  sleep 8
}

capture_job_json() {
  local log=$1 out=$2 jobid
  jobid=$(grep -oE 'JobID [0-9a-f]+' "$log" | tail -1 | awk '{print $2}' || true)
  [[ -n "$jobid" ]] && curl -sf "$FLINK_REST/jobs/$jobid" -o "$out" || true
}

benchmark() {
  local method=$1 out_dir=$2; shift 2

  echo -e "\n[$method]"
  restart_cluster

  local t0; t0=$(date +%s.%N)
  "$@" > "$out_dir/flink.log" 2>&1
  local secs; secs=$(awk -v s="$t0" -v e="$(date +%s.%N)" 'BEGIN{printf "%.3f",e-s}')

  capture_job_json "$out_dir/flink.log" "$out_dir/job.json"
  echo "$method,$secs" >> "$EXPDIR/runtime.csv"
  echo "  runtime: ${secs}s"
}

echo "=== $DATAFLOW ==="

echo -e "\n[build]"
(cd "$ROOT/code/temporal_index"     && mvn -q install -DskipTests)
(cd "$ROOT/code/caps"   && mvn -q install -DskipTests)
(cd "$ROOT/code/$DATAFLOW"  && mvn -q clean package -DskipTests)

rm -f "$EXPDIR/caps"/{sink.out,job.json}
benchmark caps "$EXPDIR/caps" \
  "$FLINK" run --parallelism 1 --class NexmarkMonitorNewUsersCaps \
  "$ROOT/code/$DATAFLOW/target/nexmark-1-1.0-SNAPSHOT.jar" \
  "$ROOT/data/nexmark/persons.txt" \
  "$ROOT/data/nexmark/auctions.txt" \
  unused \
  provenance "$EXPDIR" "$SAMPLE_PROB"

outputs=$(<"$EXPDIR/outputs_caps.txt")
MEM=$((outputs * CHANNELS * 8))
echo "caps,$MEM" >> "$EXPDIR/memory_model.csv"
echo "  meta-data (model): $MEM B"

avg_ns=$(awk -F, 'NR>1 && $1=="caps" {print $4; exit}' "$EXPDIR/provenance_query.csv")
seconds=$(awk -F, 'NR>1 && $1=="caps" {print $2; exit}' "$EXPDIR/runtime.csv")
{
  echo "method,runtime_s,prov_query_ns"
  echo "caps,$seconds,$avg_ns"
} > "$EXPDIR/times.csv"
rm -f "$EXPDIR"/{runtime.csv,provenance_query.csv}
echo "times.csv:"
cat "$EXPDIR/times.csv"

echo -e "\nDone. Outputs in $EXPDIR/"

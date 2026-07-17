#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
EXPDIR="$(cd "$(dirname "$0")" && pwd)"
DATAFLOW="nexmark_1"
FLINK_DIR="$ROOT/code/flink-1.10.0"
FLINK="$FLINK_DIR/bin/flink"
HORIZON_MS=$((7 * 24 * 60 * 60 * 1000))
WIDTHS_MS="3600000,21600000,86400000,604800000"

export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-8-openjdk-amd64}"
export PATH="$JAVA_HOME/bin:$PATH"

echo "=== $DATAFLOW summarize (storage vs estimation error) ==="

echo -e "\n[build]"
(cd "$ROOT/code/temporal_index"     && mvn -q install -DskipTests)
(cd "$ROOT/code/caps"   && mvn -q install -DskipTests)
(cd "$ROOT/code/$DATAFLOW" && mvn -q clean package -DskipTests)

echo -e "\n[restart cluster]"
"$FLINK_DIR/bin/stop-cluster.sh" 2>/dev/null || true
sleep 5
"$FLINK_DIR/bin/start-cluster.sh"
sleep 8

echo -e "\n[run: dataflow + summarize bench]"
rm -f "$EXPDIR/summarize.csv"
"$FLINK" run --parallelism 1 --class NexmarkMonitorNewUsersCaps \
  "$ROOT/code/$DATAFLOW/target/nexmark-1-1.0-SNAPSHOT.jar" \
  "$ROOT/data/nexmark/persons.txt" \
  "$ROOT/data/nexmark/auctions.txt" \
  unused \
  summarize "$EXPDIR" "$HORIZON_MS" "$WIDTHS_MS" > "$EXPDIR/flink.log" 2>&1

echo "summarize.csv:"
cat "$EXPDIR/summarize.csv"

echo -e "\nDone. Outputs in $EXPDIR/"

#!/bin/bash
# Tier-1 microbenchmark: writes bench_results.csv and bench_results_crossover.csv in this directory.
# Usage: ./run_bench.sh [maxN] [workloads] [out.csv]   (maxN=10000 for a quick smoke run; workloads: uniform,nearsorted,sorted)
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
cd "$(dirname "$0")"
mvn -q compile || exit 1
CAPS=$HOME/.m2/repository/temporalindex/temporal-index/1.0-REV-SNAPSHOT/temporal-index-1.0-REV-SNAPSHOT.jar
$JAVA_HOME/bin/java -Xms8g -Xmx8g -XX:+UseParallelGC -cp target/classes:$CAPS \
  tieredindex.Bench ${3:-bench_results.csv} ${1:-1000000} ${2:-uniform,nearsorted}

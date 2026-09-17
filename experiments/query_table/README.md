# Provenance query latency (shared identifiers)

JMH average query time over the same 1,000 preselected output identifiers for CAPS, Green-polynomial, and GeneaLog.

The three methods must answer the same outputs, otherwise the ratio says nothing. One CAPS `provenance_ids` pass writes the manifest; each method captures a query fixture over it. JMH is the only query timer — not a hand loop.

**Run:** `./experiments/query_table/run.sh` (needs Flink, inputs, `build_all --with-competitors --with-jmh`).

**Outputs:** `<dataflow>/query_table.csv` (`method,channels,ids,score_ns,error_ns`, where `channels` is the vector width $M$ the dataflow was run at); experiment `query_table.csv` (adds `dataflow`).

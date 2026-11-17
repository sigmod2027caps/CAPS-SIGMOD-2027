# GeneaLog baseline: origin and changes

This directory is GeneaLog as distributed with Ananke (https://github.com/dmpalyvos/ananke), licensed under the
Apache License 2.0 (`LICENSE`). Three files of `util/` (`AvgStat`, `CountStat`, `MaxStat`) carry their original
GPLv3 headers (`LICENSE.GPL-3`). The upstream README is `README.upstream.md`.

The provenance algorithm of GeneaLog (the classes of `genealog/` that the upstream code already had: tuple graph,
serializers, unfolder, file sink) is unchanged, except for timing code in one sink. Our changes:

**Modified upstream files**
- `genealog/GenealogFileSink.java`: also measures the time of the provenance traversal and writes it next to the
  memory file. The traversal itself is unchanged.
- `util/ExperimentSettings.java`: command-line options of our experiments (output files, query ids, probes).

**Added: the six dataflows of the paper** (`usecases/nexmark`, `usecases/taxi`, `usecases/twitter`): sources,
tuple classes, maps and aggregate functions, and the queries `NexmarkMonitorNewUsers` (NX1), `NexmarkLocalItemSuggestion`
(NX2), `TaxiPassengerJoin` (TX1), `TaxiPaymentAvg` (TX2), `TwitterVerifiedJoin` (TW1), `TwitterLocationAvg` (TW2).
Their tuple classes use identity equality: with field equality, two equal tuples in one window share a graph node
and window aggregation undercounts the contributing tuples.

**Added: sinks and probes for the measurements** (`genealog/`)
- `GenealogIndexingSink`, `GenealogTraversalSink`, `GenealogContributionQuery`: keep the graph of every output and
  answer per-output and temporal how-much queries, i.e., the queries the paper times for GeneaLog.
- `GenealogLiveSink`, `GenealogMetadataProbe`, `GenealogCardinalityDiscardSink`, `GenealogGraphClone`,
  `GenealogBenchmarkTuple`: memory probes, output checks and the JMH fixture.
- Unit tests for the added classes (`src/test/java/.../genealog`, `.../util`).

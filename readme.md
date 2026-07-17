How-Much Provenance for Streaming Dataflows
===========================================

Abstract
--------
Streaming dataflows transform tuples from a set of sources into outputs at a
set of sinks. We propose how-much provenance, which reports how much data each
source contributed to an output tuple, or to all tuples that a sink produced
during a time window; e.g., how many taxi trips from each borough support an
hourly traffic report. Such answers directly serve monitoring, debugging, and
auditing, and require no tuple-level tracking: we instrument operators to
produce and propagate aggregates whose size is bounded by the number of sources
and processing paths of the dataflow, for each item that reaches a sink. The
model extends to virtual sources, which split a source by ad-hoc filtering or
classification of its tuples. Since sink outputs accumulate over time, we also
define versioning, which contracts the contribution of a source across
consecutive windows, and design a temporal index that stores provenance
meta-data as prefix sums and answers time-range queries in logarithmic time,
with or without expiration of old meta-data. Experiments on two real-world
datasets (NYC taxi trips and Twitter/X) and the Nexmark benchmark show that our
approach adds negligible overhead to the dataflow and answers how-much
provenance queries orders of magnitude faster than fine-grained provenance
systems.

Requirements
------------
Install before running:

  - Java 8 (OpenJDK 8). Flink 1.10 does not run on newer JDKs.
  - Apache Maven 3.x
  - curl (used by the how_much experiment)
  - Apache Flink 1.10.0

Data
-------------------
The input files live in data/nexmark/. auctions.zip is split into parts to
stay within upload size limits. Reassemble and unpack them once:

  cd data/nexmark
  cat auctions.zip.part* > auctions.zip
  unzip auctions.zip
  unzip persons.zip
  cd ../..

You should then see data/nexmark/persons.txt and data/nexmark/auctions.txt.

Run
---
From the repository root:

  ./run.sh

This builds the code, starts a local Flink cluster, runs all experiments
on the data, and prints the result CSVs.

A single experiment:

  ./experiments/how_much/run.sh
  ./experiments/queries/run.sh
  ./experiments/expiry/run.sh
  ./experiments/summarize/run.sh
  ./experiments/query_length/run.sh

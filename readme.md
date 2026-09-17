# How-Much Provenance for Streaming Dataflows

CAPS reports how much each source contributed to an output: how many taxi trips
from each borough are behind an hourly traffic report, for example. It asks the
same question of a whole time window, not just a single output tuple.

It does this without tracking individual tuples. Every record carries one
counter per channel, so the annotation is the size of the dataflow and not of
the data, and an index over the output timestamps answers queries over a time
range in logarithmic time.

This repository holds the implementation, the three baselines it is compared
against, and one script per table and figure of the paper.

- [Requirements](#requirements)
- [Quick start](#quick-start)
- [Repository layout](#repository-layout)
- [Dataflows](#dataflows)
- [Methods](#methods)
- [Datasets](#datasets)
- [Running the experiments](#running-the-experiments)
- [Results and where they are reported](#results-and-where-they-are-reported)
- [Memory](#memory)
- [Tests](#tests)
- [Offline query tool](#offline-query-tool)
- [Troubleshooting](#troubleshooting)

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

# Memory (measured meta-data volume)

Flink accumulator probes at every provenance-bearing operator, summed over the run.

This is **not** a memory metric the paper reports. Table 2 reports the retained
meta-data computed by the analytical model of Section 7.2
(`how_much/memory_model.csv`) and measured as the peak heap above No Prov
(`heap_check/`), and the peak live state (`live_memory/`). In the analytical
model, each method is
charged the meta-data it has to *retain* to answer a query - CAPS at the sink,
GeneaLog on every operator output, since it keeps the whole contribution graph.
The model file reproduces the GeneaLog-over-CAPS ratios stated in the paper (4.6x on
taxi_1, 26x on nexmark_2); the twitter_1 ratio of the paper (2.2x) uses the real
tweets, which are part of the artifact (`data/tweets/twitter.txt`).
`how_much/memory_model_detail.csv` splits the model
into `sink_bytes` (retained) and `intermediate_bytes` / `window_state_bytes`
(live).

The probes here are the measured cross-check on that model: cumulative bytes that
passed through the operators, which for GeneaLog is close to what it retains but
for CAPS is far above it, because CAPS's intermediate annotations die immediately.
Never mix the two in one comparison, and never take one method from one file and
another method from the other.

**Run:** `./experiments/memory/run.sh` (needs Flink, inputs, `build_all --with-competitors`); `PHASES=plot ./experiments/memory/run.sh` rebuilds the CSVs and the table from the probe files already there.

**Outputs:** `<dataflow>/volume_{caps,green,genealog}/*_metadata_volume.csv` per-operator probes; `<dataflow>/metadata_volume.csv` and `metadata_volume.csv` (method, records, bytes; experiment-level adds `dataflow`); `how_much_comparison.csv` (decimal MB). No figure: this is a cross-check, and the paper has no figure for it.

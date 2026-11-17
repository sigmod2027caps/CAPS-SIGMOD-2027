# Cross-method verification

Not timed. `run.sh` runs No Prov, CAPS, Green and GeneaLog on every dataflow with outputs in files and checks equal outputs, per-output counts (CAPS = Green per channel, CAPS = GeneaLog total), monotone sink input, window-end timestamps, and parallelism invariance. `index_check.sh` checks the two-tier index in the Flink sinks against the scan, GeneaLog traversal and Green projection at p = 1, 2, 4.

**Run:** `experiments/verify/run.sh`, `experiments/verify/index_check.sh` (`SMALL=1` quick check; `CAPS_PARALLELISM=2`; `PARALLELISMS="1 2 4"`).
**Outputs:** `verify_<tag>.csv`, `index_check_<tag>.csv`, `index_stats_<tag>.csv`; PASS or FAIL on the console.
**Takeaway:** all checks PASS on the six dataflows; the methods differ only in what they keep, not in the outputs.

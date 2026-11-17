# Temporal how-much and versioning queries

Latency of NUM_QUERIES how-much and versioning queries on the online index of CAPS, the scan baseline, Green and GeneaLog; also insertion latency and index size. Backs Figure 10 (and the insertion/index-size numbers of Section 7.4). Green and GeneaLog answers are verified against CAPS query by query.

**Run:** `experiments/queries/run.sh` (`SMALL=1 REPS=1 experiments/queries/run.sh` quick check, results in `small/`; `PHASES=plot` redraws).
**Outputs:** `queries_howmuch_bar.{pdf,csv}` (Figure 10), `queries_query_latency_bar.*`, `query_latency.csv`, `insert_latency.csv`, `index_stats.csv`, `verification.csv`.
**Takeaway:** the prefix-sum index answers in logarithmic time and orders of magnitude faster than the scan, the Green projection and the GeneaLog traversal.

# Query latency against window length

Query windows of 1, 10, 100 and 1,000 slides on the same online indexes as `queries`. Backs Figure 9b. Answers are verified against CAPS for every query of every length.

**Run:** `experiments/query_length/run.sh` (`SMALL=1 REPS=1 experiments/query_length/run.sh` quick check, results in `small/`; `PHASES=plot` redraws; long: `taxi_2` alone is hours).
**Outputs:** `query_length_latency_lines.{pdf,csv}`, `query_length_latency.csv`, `verification.csv`.
**Takeaway:** scan, Green and GeneaLog grow with the number of outputs in the window; the CAPS index is flat.

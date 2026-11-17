# Per source versus per path counters

CAPS with one counter per source versus one per processing path on `taxi_1` and `twitter_1`, with the online indexing sink. Backs Figure 9c.

**Run:** `experiments/paths/run.sh` (`SMALL=1 REPS=1 experiments/paths/run.sh` quick check, results in `small/`; `PHASES=plot` redraws).
**Outputs:** `paths_cost_bar.{pdf,csv}`, `paths.csv`.
**Takeaway:** the capture and insertion cost hardly depends on the number of counters, while path-level queries become possible.

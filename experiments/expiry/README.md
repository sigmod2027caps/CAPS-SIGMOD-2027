# Expiry of old provenance

A never-expiring index and one trimmed to a 7-day horizon are fed by the same sink; memory is sampled every 6 h of event time and in-horizon queries are cross-checked on both (mismatches must be 0). Backs Table 4 (expiry part) and Section 7.8.1.

**Run:** `experiments/expiry/run.sh` (`SMALL=1 REPS=1 experiments/expiry/run.sh` quick check, results in `small/`; `PHASES=plot` redraws).
**Outputs:** `expiry_table.{csv,tex}`, `expiry_stats.csv`, `expiry_memory.csv`, per-dataflow directories.
**Takeaway:** expiry bounds the index memory at no cost in query latency, and the in-horizon answers are identical.

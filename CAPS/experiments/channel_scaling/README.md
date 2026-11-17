# Channel scaling (synthetic dataflows)

Tests runtime, retained meta-data, per-output and temporal query latency of No Prov, GeneaLog, Green and CAPS as the number of channels m grows (two sweeps: m labels on one path, and m real branches). Backs the branch test of Section 7.5. Every point is verified against an oracle recomputed from the input before it is timed.

**Run:** `experiments/channel_scaling/run.sh` (`SMALL=1 experiments/channel_scaling/run.sh` quick check; `PHASES=plot` redraws; `CAPS_PARALLELISM`, `MODE=local`, `M_LIST`, `FORCE=1` as listed at the top of the script).
**Outputs:** `results/full_p1_cluster/{runtime,memory,query_total,query_vector,temporal}.{pdf,csv}`, `crossover.csv`, `verification.csv` (`results/small_*` for `SMALL=1`).
**Takeaway:** CAPS cost grows with M only through the width of the counter vector; its retained meta-data exceeds Green's from M of about 59 and GeneaLog's from M of about 240-475 (`crossover.csv`).

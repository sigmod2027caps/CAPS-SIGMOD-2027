# Checkpointing, failure and recovery (E4)

Runs CAPS with checkpointing and an injected failure (the source throws, or the TaskManager is killed) and compares answers, index digest and deduplicated output ids with a run without failure; also times the checkpointing overhead. Backs Section 7.7.

**Run:** `experiments/recovery/run.sh` (`SMALL=1 experiments/recovery/run.sh` quick check on all six subsets; `PHASES=verify` or `timing` for one part; `PHASES=plot` redraws; `E4_DATAFLOWS`, `FORCE=1`).
**Outputs:** `results_full_howmuch/` (p = 1) and `results_full_howmuch_p4_D60000_d60000/` (p = 4, D = delta = 60 s); `results_small_howmuch*/` for `SMALL=1`: `recovery_verify.csv`, `recovery_runs.csv`, `recovery_summary.csv`, `recovery_overhead.pdf`, `recovery_outage.pdf`. Raw runs in `work_*` (git-ignored).
**Takeaway:** after a failure the restored index equals the failure-free one (every check PASS); the checkpointing overhead is in `recovery_summary.csv` (`overhead_pct`).

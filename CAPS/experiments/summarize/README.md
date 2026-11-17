# Online summarization

One live index per bucket width; records older than the 7-day horizon are compressed into buckets, and the summarized region is queried against an exact index to get the relative error. Backs Table 4 (summarization part) and Section 7.8.2.

**Run:** `experiments/summarize/run.sh` (`SMALL=1 REPS=1 experiments/summarize/run.sh` quick check, results in `small/`; `PHASES=plot` redraws).
**Outputs:** `summarize_table.{csv,tex}`, `summarize.csv`, per-dataflow directories.
**Takeaway:** wider buckets cut storage at a small, bounded estimation error.

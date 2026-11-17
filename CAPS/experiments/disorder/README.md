# Disorder, skew, parallelism and late data (E3)

Tests the two-tier index under controlled out-of-order records (D), watermark delay (delta), slow readers, Zipf-skewed keys, parallelism 1..8, one global versus sharded sink, idle readers, and the Tier-1 variants. Backs Table 3 and Section 7.6.

**Run:** `experiments/disorder/run.sh` (all six dataflows, the paper's run); `SMALL=1 PHASES="late check" experiments/disorder/run.sh` quick check; `PACED=1` for the delta arms; `PHASES=plot` redraws.
**Outputs:** `results_full/` (`results_small/`): `e3_*.csv`, `e3_overhead_{D,p,skew}.pdf`, `e3_occupancy*.pdf`, `e3_finality.pdf`, `e3_tier1_*.pdf`, `tables.md`. Paced arms: `results_full_paced_howmuch/`. Raw runs in `work_*` (git-ignored).
**Takeaway:** with delta >= D the answers equal the ordered run; Tier-1 occupancy is bounded by the watermark lag; below D late records are counted, not silently merged.

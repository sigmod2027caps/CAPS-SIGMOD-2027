# Cost of the online index

End-to-end runtime of CAPS with capture only, with the online two-tier index (Tier 1 as tree, or as hybrid ordered run), against No Prov, on all six dataflows. Backs Section 7.4 (index maintenance).

**Run:** `experiments/index_overhead/run.sh` (`SMALL=1 REPS=1 experiments/index_overhead/run.sh` quick check; `PHASES=plot` redraws).
**Outputs:** `index_overhead.csv`, `reps.csv`, `index_overhead_runtime_bar.{pdf,csv}`.
**Takeaway:** see `index_overhead.csv` (`overhead_pct` against the matching No Prov reference); maintaining the index online adds a few percent at most on ordered input.

# Capture overhead (how much)

End-to-end runtime of No Prov, GeneaLog, Green and CAPS on the six dataflows (median of 3, no output files), the in-situ per-output provenance query time, and the analytical meta-data memory model. Backs Figure 8 (runtime) and the retained meta-data of Table 2 (computed).

**Run:** `experiments/how_much/run.sh` (`SMALL=1 REPS=1 experiments/how_much/run.sh` quick check, results in `small/`; `PHASES=plot` redraws).
**Outputs:** `how_much_runtime_norm.{pdf,csv}` (Figure 8), `how_much_runtime_bar.*`, `runtime_table.{csv,tex}`, `runtime_gc_table.tex`, `memory_model.csv`, `memory_model_detail.csv`, `how_much_memory_model_bar.*`, `query_ns.csv`, `times.csv`.
**Takeaway:** CAPS adds a small overhead over No Prov; GeneaLog and Green cost more time and far more meta-data.

# Live (peak) annotation memory

Counts the peak annotation bytes held in window state at once (instrumented model bytes: event-time schedule and wall clock) for GeneaLog, Green and CAPS, next to the retained bytes of the analytical model. Backs the peak-live columns of Table 2. Not heap: see `heap_check`.

**Run:** `experiments/live_memory/run.sh` (the paper's run: `LIVE_REPS=2`; `SMALL=1 experiments/live_memory/run.sh` quick check; `PHASES=plot` redraws).
**Outputs:** `results_p1/live_memory_table.{csv,tex}`, `live_memory_peak_bar.*`, `live_memory_peak_wall_bar.*`, `live_memory_retained_bar.*`, `live_memory.csv`, `live_memory_ops.csv`, `table_a_check.csv` (`results_small_p*` for `SMALL=1`).
**Takeaway:** the live peak is far below the retained total for every method; CAPS holds M counters per open window pane, Green grows with the contributors (see `live_memory_table.csv`).

# Heap check of the memory numbers (Table 2, retained memory measured)

Samples the TaskManager JVM heap in use after forced GCs while the jobs of `how_much` run (sources paced), on all six
dataflows, for the four methods, while the sink builds the index of the method. The peak heap above No Prov is the
"measured" retained memory of Table 2, next to the computed one of `how_much/memory_model.csv`.

**Run:** `experiments/heap_check/run.sh` (`SMALL=1 experiments/heap_check/run.sh` quick check; `PHASES=plot` redraws; `HEAP_REPS` (default 3), `HEAP_PERIOD_S`, `HEAP_PACE_S_FULL`).
**Outputs:** `results_index/heap_check.csv` (the numbers of Table 2), `results_index/heap_check_bar.{pdf,csv}`, raw samples per dataflow and method (`results_index/<dataflow>/`).
**Takeaway:** the heap above No Prov is smallest for CAPS and largest for GeneaLog, which keeps its graphs; it includes the object overhead of the JVM, so it is compared with, not subtracted from, the computed size.

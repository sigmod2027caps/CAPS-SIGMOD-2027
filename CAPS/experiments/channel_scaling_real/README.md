# Channel scaling on real data

Same question as `channel_scaling`, on the real taxi (keyed by taxi or by zone) and Twitter inputs with a synthetic window dataflow on top, adding CAPS with sparse annotations (`SparseHowMuch`). Backs Figure 11 (retained meta-data and per-output query latency against m, `results/channel_{pertaxi,perzone}_{mem,lat}.pdf`, `channel_legend.pdf`); the branch test is `channel_scaling`.

**Run:** `experiments/channel_scaling_real/run.sh` (`DATASET_LIST="tweets" M_LIST="1 8"` for a subset; `PHASES=plot` redraws; `CAPS_PARALLELISM`). `SMALL=1` runs on the 10% subsets (results in `results_small/`, never over the full ones).
**Outputs:** `results/channel_{pertaxi,perzone}_{mem,lat}.pdf` and `channel_legend.pdf` (Figure 11; the numbers are in `memory.csv` and `query.csv`), `channel_scaling_real_grid.{pdf,csv}` (the same in one figure), `channel_scaling_real_runtime.*`, `channel_scaling_real_query_vector.*`, `runtime.csv`, `memory.csv`, `query.csv`, `verification.csv`.
**Takeaway:** the dense counter vector costs O(M) per output, the sparse variant stores only non-zero channels; GeneaLog and No Prov do not depend on M.

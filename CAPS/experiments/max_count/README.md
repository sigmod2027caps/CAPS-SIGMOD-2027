# Largest per-channel count

Runs CAPS at parallelism 1 with every sink output written to a file and reports the largest per-channel counter per dataflow. Backs Section 4.3 (counters stay far below 2^63-1); the output count is checked against `verify`.

**Run:** `experiments/max_count/run.sh` (`SMALL=1 experiments/max_count/run.sh` quick check).
**Outputs:** `max_count_p1.{csv,pdf}` (`max_count_small_p1.*` for `SMALL=1`).
**Takeaway:** the largest counter needs at most 11 bits on the six dataflows, so 8 B per channel is ample.

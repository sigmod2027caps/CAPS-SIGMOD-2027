#!/usr/bin/env bash
# `flink` with a time limit (REAL_FLINK is the real or in-process launcher, JOB_TIMEOUT_S the limit in seconds): a run
# that does not finish (a sorted-buffer Tier 1 fed out of order with millions of records is quadratic) is itself a result.
exec timeout "${JOB_TIMEOUT_S:-900}" "$REAL_FLINK" "$@"

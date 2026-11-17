#!/usr/bin/env python3
"""Samples the live heap of the TaskManager JVM of a Flink job: every PERIOD seconds it forces a full GC
(jcmd <pid> GC.run) and reads the heap that is still in use afterwards (jstat -gc: survivors + eden + old, KB).
Runs until it is killed. Writes one CSV row per sample: epoch_s (time of the sample), used_mb, old_mb.

A forced full GC of a heap of many GB stops the job for seconds (GeneaLog on taxi_2: 9 s at 15 GB), so the sampler waits
at least OVERHEAD times as long as the last GC took before the next one (at most about a tenth of the time is spent in forced
GCs); the period is the lower bound.

Usage: heap_sampler.py OUT.csv [PERIOD_S] [PID]      (PID default: the TaskManagerRunner of the machine)
"""
import os
import subprocess
import sys
import time


def taskmanager_pid():
    out = subprocess.run(["pgrep", "-f", "taskexecutor.TaskManagerRunner"], capture_output=True, text=True).stdout.split()
    return out[0] if out else None


def used_after_gc(pid):
    """Forces a full GC and returns (used MB, old generation MB) afterwards."""
    subprocess.run(["jcmd", pid, "GC.run"], capture_output=True)
    lines = subprocess.run(["jstat", "-gc", pid], capture_output=True, text=True).stdout.strip().splitlines()
    head, vals = lines[0].split(), [float(x) for x in lines[1].split()]
    row = dict(zip(head, vals))
    used_kb = row["S0U"] + row["S1U"] + row["EU"] + row["OU"]
    return used_kb / 1024.0, row["OU"] / 1024.0


OVERHEAD = float(os.environ.get("HEAP_OVERHEAD", "9"))   # wait OVERHEAD x (duration of a forced GC) between two samples


if __name__ == "__main__":
    out = sys.argv[1]
    period = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0
    pid = sys.argv[3] if len(sys.argv) > 3 else taskmanager_pid()
    if not pid:
        sys.exit("heap_sampler: no TaskManager JVM")
    with open(out, "w", buffering=1) as f:
        f.write("epoch_s,used_mb,old_mb\n")
        while True:
            start = time.time()
            try:
                used, old = used_after_gc(pid)
            except Exception:  # the JVM is gone (the cluster was stopped)
                break
            f.write("%.3f,%.1f,%.1f\n" % (start, used, old))
            took = time.time() - start
            time.sleep(max(0.0, period - took, OVERHEAD * took - took))

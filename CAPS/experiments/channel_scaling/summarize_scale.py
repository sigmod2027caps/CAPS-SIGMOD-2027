#!/usr/bin/env python3
"""Collects the work directory of experiments/channel_scaling/run.sh into the result CSVs.

  verification.csv   one row per (sweep, M, check)           from verify_scale.py
  runtime_reps.csv   one row per timed submission             job duration (net runtime), wall clock, outputs
  runtime.csv        median / min / max / mean / stddev of the job duration over the repetitions
  memory.csv         retained meta-data per method (analytical, MB = 10^6 B)
  query.csv          per-output query latency (JMH, ns per query) on the shared 1,000 output ids
  temporal.csv       temporal how-much query of CAPS over the prefix-sum index (JMH, ns per query)
  crossover.csv      the M at which CAPS's N*M*8 B overtakes Green's and GeneaLog's retained bytes

Fails (exit 1) on a missing file, on a point without PASS, or on output counts that differ between
methods or repetitions.

Usage: summarize_scale.py --work W --out R --input F --sweeps ... --m-values ... --methods ...
"""
import argparse
import csv
import json
import statistics
import sys
from pathlib import Path

METHOD_KEY = {"noprov": "noprov", "genealog": "genealog", "green": "green", "caps": "caps"}


def write(path, header, rows):
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)
    print("  %s (%d rows)" % (path.name, len(rows)))


def read_one(path):
    with path.open(newline="") as handle:
        return next(csv.DictReader(handle))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--time-dir", default="time", help="timing directory of the point (time_<engine>)")
    parser.add_argument("--sweeps", nargs="+", required=True)
    parser.add_argument("--m-values", nargs="+", type=int, required=True)
    parser.add_argument("--methods", nargs="+", required=True)
    args = parser.parse_args()

    verification, reps, runtime, memory, query, temporal, crossover = [], [], [], [], [], [], []
    errors = []
    for sweep in args.sweeps:
        for M in args.m_values:
            W = args.work / ("%s_m%d" % (sweep, M))
            if not (W / "verify" / "PASS").exists():
                errors.append("%s M=%d: verification missing or failed" % (sweep, M))
                continue
            with (W / "verify" / "verify.csv").open(newline="") as handle:
                verification += [list(r.values()) for r in csv.DictReader(handle)]
            stats = json.loads((W / "verify" / "stats.json").read_text())

            # runtime: job duration of the repetitions, output counts must all agree
            for method in args.methods:
                durations = []
                for rep_dir in sorted((W / args.time_dir).glob(method + "_rep*")):
                    outputs = int((rep_dir / "outputs.txt").read_text().strip())
                    if outputs != stats["outputs"]:
                        errors.append("%s M=%d %s %s: %d outputs, verification has %d"
                                      % (sweep, M, method, rep_dir.name, outputs, stats["outputs"]))
                    wall = float((rep_dir / "wall_s.txt").read_text())
                    duration = int((rep_dir / "job_ms.txt").read_text()) / 1000.0
                    durations.append(duration)
                    reps.append([sweep, M, method, int(rep_dir.name.split("rep")[1]),
                                 "%.3f" % duration, "%.3f" % wall, outputs])
                if not durations:
                    errors.append("%s M=%d %s: no timed runs" % (sweep, M, method))
                    continue
                runtime.append([sweep, M, method, len(durations), "%.3f" % statistics.median(durations),
                                "%.3f" % min(durations), "%.3f" % max(durations),
                                "%.3f" % statistics.mean(durations),
                                "%.3f" % (statistics.stdev(durations) if len(durations) > 1 else 0.0),
                                stats["outputs"]])

            # retained meta-data (analytical)
            retained = {"noprov": 0, "genealog": stats["genealog_bytes"], "green": stats["green_bytes"],
                        "caps": stats["caps_bytes"]}
            for method in args.methods:
                b = retained[method]
                memory.append([sweep, M, method, stats["outputs"], b, "%.3f" % (b / 1e6),
                               "%.1f" % (b / stats["outputs"])])
            outputs, contrib = stats["outputs"], stats["total_contributions"]
            crossover.append([sweep, M, "%.2f" % (contrib / outputs),
                              "%.2f" % (4 * stats["genealog_tuples"] / outputs),
                              int(stats["caps_bytes"] > stats["green_bytes"]),
                              int(stats["caps_bytes"] > stats["genealog_bytes"])])

            # queries
            for spec in ("caps_total", "caps_vector", "green_total", "green_vector", "genealog_total"):
                f = W / "jmh" / (spec + ".csv")
                if not f.exists():
                    errors.append("%s M=%d: missing %s" % (sweep, M, f.name))
                    continue
                row = read_one(f)
                query.append([sweep, M, row["method"], row["kind"], row["score_ns"], row["error_ns"],
                              row["checksum"]])
            f = W / "jmh" / "temporal.csv"
            if f.exists():
                row = read_one(f)
                temporal.append([sweep, M, row["score_ns"], row["error_ns"], row["checksum"]])
            else:
                errors.append("%s M=%d: missing temporal.csv" % (sweep, M))

            # the three methods answered the same ids: checksums of the totals must agree
            sums = {r[2]: r[6] for r in query if r[0] == sweep and r[1] == M and r[3] == "total"}
            if len(set(sums.values())) > 1:
                errors.append("%s M=%d: query checksums differ between methods: %s" % (sweep, M, sums))

    args.out.mkdir(parents=True, exist_ok=True)
    write(args.out / "verification.csv", ["sweep", "M", "parallelism", "check", "status", "outputs",
                                          "mismatches", "detail"], verification)
    write(args.out / "runtime_reps.csv", ["sweep", "M", "method", "rep", "job_duration_s", "wall_s", "outputs"], reps)
    write(args.out / "runtime.csv", ["sweep", "M", "method", "n", "median_s", "min_s", "max_s", "mean_s",
                                     "stddev_s", "outputs"], runtime)
    write(args.out / "memory.csv", ["sweep", "M", "method", "outputs", "retained_bytes", "retained_MB",
                                    "bytes_per_output"], memory)
    write(args.out / "query.csv", ["sweep", "M", "method", "kind", "score_ns", "error_ns", "checksum"], query)
    write(args.out / "temporal.csv", ["sweep", "M", "score_ns", "error_ns", "checksum"], temporal)
    write(args.out / "crossover.csv", ["sweep", "M", "caps_vs_green_M_star", "caps_vs_genealog_M_star",
                                       "caps_exceeds_green", "caps_exceeds_genealog"], crossover)
    for e in errors:
        print("ERROR: " + e, file=sys.stderr)
    sys.exit(1 if errors else 0)

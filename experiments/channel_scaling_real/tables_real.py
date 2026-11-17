#!/usr/bin/env python3
"""Markdown tables of the real-data channel-scalability experiment, from the CSVs of summarize_real.py.

Usage: tables_real.py --res RESULTS_DIR
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import common  # noqa: E402

NAMES = {"noprov": "No Prov", "genealog": "GeneaLog", "green": "Green", "caps": "CAPS (dense)", "caps_sparse": "CAPS (sparse)"}
TITLES = {"taxi": "Taxi (taxi_1 input, 10M rides, key = taxi id, 1 h window, channel = pickup zone mod M)",
          "taxi_zone": "Taxi, per zone (taxi_1 input, 10M rides, key = pickup zone, 1 h window, channel = taxi id mod M)",
          "tweets": "Tweets (twitter_1 input, 100K tweets, key = hashtag, 30 min window, channel = hash(location) mod M)"}


def table(header, rows):
    out = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--res", type=Path, required=True)
    res = ap.parse_args().res
    stats, memory, query = (common.read_csv(res / n) for n in ("stats.csv", "memory.csv", "query.csv"))
    runtime = common.read_csv(res / "runtime.csv")
    datasets = sorted({r["dataset"] for r in stats})
    for ds in datasets:
        ms = sorted({int(r["M"]) for r in stats if r["dataset"] == ds})
        print("#### %s\n" % TITLES[ds])
        s = {int(r["M"]): r for r in stats if r["dataset"] == ds}
        print(table(["M", "outputs", "contributing records / output", "non-zero channels / output"],
                    [[m, s[m]["outputs"], s[m]["records_per_output"], s[m]["nonzero_channels_per_output"]] for m in ms]))
        print("\nRetained meta-data, MB (bytes per output in parentheses):\n")
        methods = ("genealog", "green", "caps", "caps_sparse")
        mem = {(int(r["M"]), r["method"]): r for r in memory if r["dataset"] == ds}
        print(table(["M"] + [NAMES[m] for m in methods],
                    [[m] + ["%s (%s)" % (mem[m, x]["retained_MB"], mem[m, x]["bytes_per_output"]) for x in methods] for m in ms]))
        for kind, label, mm in (("total", "Per-output query, total contributions (JMH, ns per query)", ("genealog", "green", "caps", "caps_sparse")),
                                ("vector", "Per-output query, the M channel counters (JMH, ns per query)", ("green", "caps", "caps_sparse"))):
            q = {(int(r["M"]), r["method"]): r for r in query if r["dataset"] == ds and r["kind"] == kind}
            print("\n%s:\n" % label)
            print(table(["M"] + [NAMES[m] for m in mm], [[m] + [q[m, x]["score_ns"] if (m, x) in q else "-" for x in mm] for m in ms]))
        rt = {(int(r["M"]), r["method"]): r for r in runtime if r["dataset"] == ds}
        print("\nJob duration, s (median of %s runs, min-max):\n" % (next(iter(rt.values()))["reps"] if rt else "?"))
        mm = ("noprov", "genealog", "green", "caps", "caps_sparse")
        print(table(["M"] + [NAMES[m] for m in mm],
                    [[m] + ["%s (%s-%s)" % (rt[m, x]["median_s"], rt[m, x]["min_s"], rt[m, x]["max_s"]) if (m, x) in rt else "-" for x in mm]
                            for m in ms]))
        print()

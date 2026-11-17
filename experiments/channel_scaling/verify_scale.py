#!/usr/bin/env python3
"""Cross-method verification of one point (sweep, M) of the channel-cardinality experiment.

Called by run.sh with the four sink files of the point. Row formats, one row per output:
  noprov.txt    key,timestamp,count,sum
  caps.txt      key,timestamp,count,sum,c0;c1;...;cM-1,metadataBytes
  green.txt     the same (the polynomial projected on the M channels at the sink)
  genealog.txt  key,timestamp,count,sum,traversal   (source tuples reached by the backward traversal)
(a trailing "--- OUTPUT END ---" line is skipped, and its absence is an error; GeneaLog has none)

Checks (an independent oracle recomputes everything from the input file, so the four methods are
not only compared with each other):
  outputs     the multisets of (key, timestamp, count, sum) of the four methods are equal to each
              other and to the oracle (the 60 s tumbling windows of the input, timestamp = window
              end - 1)
  howmuch     CAPS's M counters per output equal the oracle's and Green's, channel by channel
              (channel = sourceId % M for the source sweep, id % M for the path sweep)
  totals      per output, the sum of CAPS's counters = GeneaLog's traversal count = the output's count
  window_end  every timestamp is one millisecond before a multiple of the window length
  monotone    each method's sink received its input in non-decreasing derived time (a pass/fail
              check at parallelism 1; above it only reported)
  metadata    CAPS's bytes column is 8 * M per output; Green's is 8 B per contributing record

Appends one row per check to --csv, writes --stats (json: outputs, records, totals, retained bytes)
and exits 1 if a check fails.

Usage: verify_scale.py --dir D --input F --sweep source|path --channels M --parallelism P --csv C --stats S
"""
import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path

import numpy as np

WINDOW_MS = 60_000
CSV_HEADER = ["sweep", "M", "parallelism", "check", "status", "outputs", "mismatches", "detail"]
END = "--- OUTPUT END ---"


def oracle(input_path, sweep, channels):
    """The expected outputs, from the input file: dict (key, timestamp, count, sum) -> counters."""
    data = np.loadtxt(input_path, delimiter=",", dtype=np.int64)
    ids, ts, source, key, value = data.T
    window = ts // WINDOW_MS
    channel = (source if sweep == "source" else ids) % channels
    nwin = int(window.max()) + 1
    cell = key * nwin + window
    ncell = (int(key.max()) + 1) * nwin
    counts = np.bincount(cell * channels + channel, minlength=ncell * channels).reshape(ncell, channels)
    sums = np.bincount(cell, weights=value, minlength=ncell).astype(np.int64)
    expected = {}
    for c in np.nonzero(counts.sum(axis=1))[0]:
        k, w = divmod(int(c), nwin)
        vec = tuple(int(x) for x in counts[c])
        expected[(k, (w + 1) * WINDOW_MS - 1, sum(vec), int(sums[c]))] = vec
    return expected, len(ids)


def read_rows(path, marker=True):
    """Rows of a sink file as lists of strings, in arrival order. GeneaLog's dump has no end marker
    (a truncated file shows up as missing outputs in check "outputs")."""
    lines = Path(path).read_text().split("\n")
    if marker:
        if lines[-1] != END:
            raise ValueError("%s: no end marker, the job did not finish" % path)
        lines = lines[:-1]
    return [line.split(",") for line in lines if line]


def descents(timestamps):
    return sum(1 for a, b in zip(timestamps, timestamps[1:]) if b < a)


def check(results, name, ok, outputs, mismatches, detail=""):
    results.append((name, "PASS" if ok else "FAIL", outputs, mismatches, detail))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dir", type=Path, required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--sweep", choices=("source", "path"), required=True)
    parser.add_argument("--channels", type=int, required=True)
    parser.add_argument("--parallelism", type=int, default=1)
    parser.add_argument("--csv", type=Path, required=True)
    parser.add_argument("--stats", type=Path, required=True)
    args = parser.parse_args()
    M = args.channels

    expected, records = oracle(args.input, args.sweep, M)
    exp_payloads = Counter(expected.keys())
    rows = {m: read_rows(args.dir / (m + ".txt"), m != "genealog") for m in ("noprov", "caps", "green", "genealog")}
    results = []

    # (a) outputs: payload multisets
    payloads = {m: Counter(tuple(int(x) for x in r[:4]) for r in rs) for m, rs in rows.items()}
    bad = {m: sum(((p - exp_payloads) + (exp_payloads - p)).values()) for m, p in payloads.items()}
    check(results, "outputs", not any(bad.values()), len(expected), sum(bad.values()),
          "; ".join("%s %d" % (m, n) for m, n in bad.items() if n) or "four methods = oracle")

    # (b) howmuch: CAPS and Green channel by channel against the oracle
    vectors = {}
    for m in ("caps", "green"):
        vectors[m] = Counter((tuple(int(x) for x in r[:4]), tuple(int(x) for x in r[4].split(";")))
                             for r in rows[m])
    exp_vectors = Counter((p, v) for p, v in expected.items())
    wrong = {m: sum(((c - exp_vectors) + (exp_vectors - c)).values()) for m, c in vectors.items()}
    cg = sum(((vectors["caps"] - vectors["green"]) + (vectors["green"] - vectors["caps"])).values())
    check(results, "howmuch", not any(wrong.values()) and cg == 0, len(expected),
          cg + sum(wrong.values()),
          "caps vs green %d; caps vs oracle %d; green vs oracle %d" % (cg, wrong["caps"], wrong["green"]))

    # (c) totals: sum of the counters = GeneaLog traversal = count
    caps_total = Counter((tuple(int(x) for x in r[:4]), sum(int(x) for x in r[4].split(";"))) for r in rows["caps"])
    gl_total = Counter((tuple(int(x) for x in r[:4]), int(r[4])) for r in rows["genealog"])
    count_total = Counter((p, p[2]) for p in expected)
    t_bad = sum(((caps_total - gl_total) + (gl_total - caps_total)).values()) \
        + sum(((caps_total - count_total) + (count_total - caps_total)).values())
    total_contrib = sum(p[2] for p in expected)
    check(results, "totals", t_bad == 0, len(expected), t_bad,
          "total contributions %d (records %d)" % (total_contrib, records))

    # (d) window_end
    off = sum(1 for r in rows["caps"] if int(r[1]) % WINDOW_MS != WINDOW_MS - 1)
    check(results, "window_end", off == 0, len(rows["caps"]), off)

    # (e) monotone
    desc = {m: descents([int(r[1]) for r in rs]) for m, rs in rows.items()}
    if args.parallelism == 1:
        check(results, "monotone", not any(desc.values()), len(rows["caps"]), sum(desc.values()),
              ", ".join("%s %d" % kv for kv in desc.items()))
    else:
        results.append(("monotone", "INFO", len(rows["caps"]), sum(desc.values()),
                        ", ".join("%s %d" % kv for kv in desc.items())))

    # (f) metadata columns
    caps_bytes = [int(r[5]) for r in rows["caps"]]
    green_bytes = [int(r[5]) for r in rows["green"]]
    bad_caps = sum(1 for b in caps_bytes if b != 8 * M)
    bad_green = sum(1 for r in rows["green"] if int(r[5]) != 8 * int(r[2]))
    check(results, "metadata", bad_caps == 0 and bad_green == 0, len(caps_bytes), bad_caps + bad_green,
          "caps 8M per output, green 8 B per contributing record")

    outputs = len(rows["caps"])
    stats = {
        "sweep": args.sweep, "channels": M, "parallelism": args.parallelism,
        "records": records, "outputs": outputs, "total_contributions": total_contrib,
        "caps_bytes": sum(caps_bytes), "green_bytes": sum(green_bytes),
        # GeneaLog keeps a quadruple (uid, U1, U2, next) of 4 x 8 B on every tuple an operator emits and
        # that stays reachable: the source tuples, the tuples of the path maps (path sweep) and the outputs.
        "genealog_tuples": records + (records if args.sweep == "path" else 0) + outputs,
    }
    stats["genealog_bytes"] = 32 * stats["genealog_tuples"]
    args.stats.write_text(json.dumps(stats, indent=1) + "\n")

    new = not args.csv.exists()
    with args.csv.open("a", newline="") as handle:
        writer = csv.writer(handle)
        if new:
            writer.writerow(CSV_HEADER)
        for name, status, n, mism, detail in results:
            writer.writerow([args.sweep, M, args.parallelism, name, status, n, mism, detail])
    failed = False
    for name, status, n, mism, detail in results:
        print("  %-11s %-4s outputs %d, mismatches %d  %s" % (name, status, n, mism, detail))
        failed |= status == "FAIL"
    sys.exit(1 if failed else 0)

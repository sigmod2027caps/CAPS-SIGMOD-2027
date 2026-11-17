#!/usr/bin/env python3
"""Verification of one nested-path point (M declared path channels).

Rows of caps: key,timestamp,count,sum,c:n;c:n;...,metadataBytes (non-zero channels, ascending);
noprov: key,timestamp,count,sum. The oracle is recomputed from the input with the semantics of the
dataflow: leaf path p = id mod 512; the left input holds p mod 8 in 0..3, the right input 4..7; per
(key, 60 s tumbling window) the join emits one output iff both inputs have records in that window;
count = cL + cR, sum = sL + sR, timestamp = window end - 1, channel = p mod M.
Checks: output count, noprov = oracle, caps = oracle (count, sum and per-channel counts per output),
every caps row has sum of counters = count and metadataBytes = 8 M. Exits 1 on any failure.

Usage: verify_nested.py --dir DIR --input FILE --channels M [--noprov FILE] [--csv OUT] [--stats OUT.json]
"""
import argparse
import json
import sys
from collections import Counter
from pathlib import Path

import numpy as np

WINDOW_MS = 60_000
PATHS = 512


def oracle(path, channels):
    """Counter of 'key,ts,count,sum,c:n;...' rows expected from the input."""
    d = np.loadtxt(path, delimiter=",", dtype=np.int64)
    ids, ts, keys, vals = d[:, 0], d[:, 1], d[:, 3], d[:, 4]
    kmax = keys.max() + 1
    group = (ts // WINDOW_MS) * kmax + keys
    ug, inv = np.unique(group, return_inverse=True)
    right = ((ids & 7) >= 4)
    has_l = np.bincount(inv, weights=(~right).astype(np.int64), minlength=len(ug)) > 0
    has_r = np.bincount(inv, weights=right.astype(np.int64), minlength=len(ug)) > 0
    keep = has_l & has_r
    sel = keep[inv]
    inv_k, ids_k, vals_k = inv[sel], ids[sel], vals[sel]
    count = np.bincount(inv_k, minlength=len(ug))
    total = np.bincount(inv_k, weights=vals_k, minlength=len(ug)).astype(np.int64)
    chan = (ids_k & (PATHS - 1)) & (channels - 1)
    cc, cnt = np.unique(inv_k * channels + chan, return_counts=True)
    per = {}
    for code, n in zip((cc // channels).tolist(), zip((cc % channels).tolist(), cnt.tolist())):
        per.setdefault(code, []).append(n)
    out = Counter()
    for g in np.nonzero(keep)[0].tolist():
        w, k = divmod(int(ug[g]), int(kmax))
        pairs = ";".join("%d:%d" % p for p in per[g])
        out["%d,%d,%d,%d,%s" % (k, (w + 1) * WINDOW_MS - 1, count[g], total[g], pairs)] += 1
    return out


def rows(path):
    with open(path) as f:
        for line in f:
            line = line.rstrip("\n")
            if line and not line.startswith("---"):
                yield line


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dir", type=Path, required=True)
    ap.add_argument("--input", type=Path, required=True)
    ap.add_argument("--channels", type=int, required=True)
    ap.add_argument("--noprov", type=Path, help="noprov rows (default DIR/noprov.txt; noprov does not depend on M)")
    ap.add_argument("--csv", type=Path)
    ap.add_argument("--stats", type=Path)
    a = ap.parse_args()
    exp = oracle(a.input, a.channels)
    n_exp = sum(exp.values())
    results = []
    stats = {"channels": a.channels, "outputs": n_exp}

    def report(check, ok, detail=""):
        results.append((check, "PASS" if ok else "FAIL", detail))
        print("  %-24s %s %s" % (check, "PASS" if ok else "FAIL", detail))

    exp4 = Counter()
    for r, c in exp.items():
        exp4[r.rsplit(",", 1)[0]] += c
    got = Counter(rows(a.noprov or a.dir / "noprov.txt"))
    report("noprov_outputs=oracle", got == exp4, "%d outputs; expected %d" % (sum(got.values()), n_exp))

    c = Counter()
    bad_total = bad_bytes = 0
    for line in rows(a.dir / "caps.txt"):
        head, size = line.rsplit(",", 1)
        c[head] += 1
        f = head.split(",")
        if sum(int(p.split(":")[1]) for p in f[4].split(";")) != int(f[2]):
            bad_total += 1
        if int(size) != 8 * a.channels:
            bad_bytes += 1
    report("caps_outputs", sum(c.values()) == n_exp, "%d outputs; expected %d" % (sum(c.values()), n_exp))
    report("caps_totals", bad_total == 0, "%d rows with sum of counters != count" % bad_total)
    report("caps_bytes", bad_bytes == 0, "%d rows with metadataBytes != 8 M" % bad_bytes)
    report("caps=oracle", c == exp)
    if a.csv:
        a.csv.parent.mkdir(parents=True, exist_ok=True)
        a.csv.write_text("channels,check,result,detail\n" + "".join(
            "%d,%s,%s,%s\n" % (a.channels, k, r, d) for k, r, d in results))
    if a.stats:
        a.stats.write_text(json.dumps(stats))
    sys.exit(0 if all(r == "PASS" for _, r, _ in results) else 1)

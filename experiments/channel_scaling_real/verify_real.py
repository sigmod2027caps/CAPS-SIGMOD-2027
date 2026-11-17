#!/usr/bin/env python3
"""Verification of one point (dataset, M) of the real-data channel-scalability experiment.

The oracle is recomputed from the input file with the dataflow of the jobs: one record per ride
(taxi) / per (tweet, hashtag) (tweets) with (timestamp, key, channel, value); one output per
(key, tumbling window) with count, sum and the per-channel counters.
  taxi    key = taxi id, channel = pickup zone mod M, value = fare in cents, window 1 h
  taxi_zone  the same input and window; key = pickup zone, channel = taxi id mod M
  tweets  key = hashtag, channel = Java String.hashCode(location) mod M, value = likes + retweets, window 30 min

Row formats of the sink files, one row per output (a trailing "--- OUTPUT END ---" line is required):
  noprov.txt       key,timestamp,count,sum
  caps.txt         key,timestamp,count,sum,c:n;c:n (non-zero channels, ascending),metadataBytes
  caps_sparse.txt  the same
  green.txt        the same (the polynomial projected on the channels at the sink)
  genealog.txt     key,timestamp,count,sum,traversal (source tuples reached by the backward traversal)

Checks: outputs (the five methods = oracle), howmuch (caps = sparse = green = oracle, per channel per output),
totals (GeneaLog traversal = sum of the CAPS counters = count), window_end, metadata (caps 8 B x M, sparse
12 B per pair, green 8 B per contributing record). Writes --csv (one row per check) and --stats (json) and
exits 1 on any failure.

Usage: verify_real.py --dataset taxi|taxi_zone|tweets --input F --channels M --dir D --csv C --stats S
       verify_real.py --dataset taxi|taxi_zone|tweets --input F --outputs      (print the number of outputs)
"""
import argparse
import csv
import json
import struct
import sys
from pathlib import Path

import numpy as np
import pandas as pd

WINDOW_MS = {"taxi": 3_600_000, "taxi_zone": 3_600_000, "tweets": 1_800_000}
END = "--- OUTPUT END ---"
METHODS = ("noprov", "caps", "caps_sparse", "green", "genealog")


def java_hash(s):
    """String.hashCode of Java (UTF-16 code units), as a signed 32-bit int."""
    h = 0
    for u in struct.unpack("<%dH" % (len(s.encode("utf-16-le")) // 2), s.encode("utf-16-le", "surrogatepass")):
        h = (31 * h + u) & 0xFFFFFFFF
    return h - (1 << 32) if h >= (1 << 31) else h


def records(dataset, path):
    """DataFrame (key, ts, loc, value) of the records of the input; loc = zone (taxi) / location hash (tweets)."""
    if dataset in ("taxi", "taxi_zone"):
        d = pd.read_csv(path, header=None, usecols=[0, 1, 3, 5], names=["key", "ts", "loc", "fare"],
                        dtype={"key": str, "ts": np.int64, "loc": np.int64, "fare": np.float64})
        if dataset == "taxi_zone":   # key = pickup zone, channel = taxi id
            return pd.DataFrame({"key": d["loc"].astype(str), "ts": d["ts"], "loc": d["key"].astype(np.int64),
                                 "value": np.round(d["fare"].to_numpy() * 100).astype(np.int64)})
        return pd.DataFrame({"key": d["key"], "ts": d["ts"], "loc": d["loc"],
                             "value": np.round(d["fare"].to_numpy() * 100).astype(np.int64)})
    keys, ts, loc, val = [], [], [], []
    hashes = {}
    with open(path, encoding="utf-8", errors="replace", newline="") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            p = line.split(",", 7)
            if not p[2]:
                continue
            h = hashes.get(p[7])
            if h is None:
                h = hashes[p[7]] = java_hash(p[7])
            for tag in p[2].split("|"):
                if tag:
                    keys.append(tag)
                    ts.append(int(p[1]))
                    loc.append(h)
                    val.append(int(p[3]) + int(p[4]))
    return pd.DataFrame({"key": keys, "ts": np.array(ts, dtype=np.int64), "loc": np.array(loc, dtype=np.int64),
                         "value": np.array(val, dtype=np.int64)})


def oracle(dataset, path, channels):
    """(outputs DataFrame key, ts, count, sum, ann; number of records; number of (output, channel) pairs)."""
    r = records(dataset, path)
    w = WINDOW_MS[dataset]
    r["out"] = (r["ts"] // w + 1) * w - 1
    r["ch"] = r["loc"] % channels            # Python % is the floor mod of Java's Math.floorMod
    g = r.groupby(["key", "out"], sort=True).agg(count=("value", "size"), sum=("value", "sum")).reset_index()
    if channels == 1:
        g["ann"] = "0:" + g["count"].astype(str)
        pairs = len(g)
    else:
        c = r.groupby(["key", "out", "ch"], sort=True).size().reset_index(name="n")
        pairs = len(c)
        c["s"] = c["ch"].astype(str) + ":" + c["n"].astype(str)
        g["ann"] = c.groupby(["key", "out"], sort=True)["s"].agg(";".join).to_numpy()
    return g, len(r), pairs


def read_rows(path, ncols):
    p = Path(path)
    with open(p, "rb") as f:
        f.seek(max(0, p.stat().st_size - 40))
        if not f.read().decode("utf-8", "replace").endswith(END):
            raise ValueError("%s: no end marker, the job did not finish" % path)
    d = pd.read_csv(p, header=None, names=list(range(ncols)), dtype=str, keep_default_na=False, quoting=3,
                    encoding="utf-8", encoding_errors="replace")
    return d[d[0] != END].reset_index(drop=True)


def merged(exp, got, extra):
    """The oracle outputs with the rows of a method (inner join on key, window end). Returns (frame, missing, extra)."""
    got = got.rename(columns={0: "key", 1: "out", 2: "count_m", 3: "sum_m", 4: "ann_m", 5: "bytes_m"})
    got["out"] = got["out"].astype(np.int64)
    dup = int(got.duplicated(["key", "out"]).sum())
    m = exp.merge(got, on=["key", "out"], how="outer", indicator=True)
    return m, int((m["_merge"] == "left_only").sum()) + dup, int((m["_merge"] == "right_only").sum())


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dataset", choices=("taxi", "taxi_zone", "tweets"), required=True)
    ap.add_argument("--input", type=Path, required=True)
    ap.add_argument("--channels", type=int, default=1)
    ap.add_argument("--dir", type=Path)
    ap.add_argument("--csv", type=Path)
    ap.add_argument("--stats", type=Path)
    ap.add_argument("--outputs", action="store_true")
    a = ap.parse_args()
    M, W = a.channels, WINDOW_MS[a.dataset]
    exp, nrec, pairs = oracle(a.dataset, a.input, M)
    if a.outputs:
        print(len(exp))
        sys.exit(0)
    results = []

    def check(name, ok, mism, detail=""):
        results.append((name, "PASS" if ok else "FAIL", len(exp), mism, detail))
        print("  %-11s %-4s outputs %d, mismatches %d  %s" % (name, "PASS" if ok else "FAIL", len(exp), mism, detail))

    stats = {"dataset": a.dataset, "channels": M, "records": nrec, "outputs": len(exp),
             "oracle_pairs": pairs, "genealog_tuples": nrec + len(exp)}
    exp_cnt = exp["count"].to_numpy()
    exp_sum = exp["sum"].to_numpy()
    bad_out, bad_ann, bad_meta = {}, {}, {}
    rows = {}
    for m in METHODS:
        ncols = {"noprov": 4, "genealog": 5}.get(m, 6)
        d = read_rows(a.dir / (m + ".txt"), ncols)
        rows[m] = d
        j, missing, extra = merged(exp, d, True)
        both = j[j["_merge"] == "both"]
        wrong = int((both["count_m"].astype(np.int64) != both["count"]).sum() + (both["sum_m"].astype(np.int64) != both["sum"]).sum())
        bad_out[m] = missing + extra + wrong
        if m in ("caps", "caps_sparse", "green"):
            bad_ann[m] = int((both["ann_m"] != both["ann"]).sum())
            nb = both["bytes_m"].astype(np.int64)
            if m == "caps":
                bad_meta[m] = int((nb != 8 * M).sum())
                stats["caps_bytes"] = int(nb.sum())
            elif m == "caps_sparse":
                npairs = both["ann_m"].str.count(";") + 1
                bad_meta[m] = int((nb != 12 * npairs).sum())
                stats["sparse_bytes"] = int(nb.sum())
                stats["sparse_pairs"] = int(npairs.sum())
            else:
                bad_meta[m] = int((nb != 8 * both["count"]).sum())
                stats["green_bytes"] = int(nb.sum())
        if m == "genealog":
            # total = traversal; compared with the sum of the CAPS counters below
            gl = both.set_index(["key", "out"])["ann_m"].astype(np.int64)
    check("outputs", not any(bad_out.values()), sum(bad_out.values()),
          "; ".join("%s %d" % (m, n) for m, n in bad_out.items() if n) or "five methods = oracle")
    check("howmuch", not any(bad_ann.values()), sum(bad_ann.values()),
          "; ".join("%s %d" % (m, n) for m, n in bad_ann.items() if n) or "caps = sparse = green = oracle, per channel")

    # totals: GeneaLog's traversal = sum of CAPS's counters (parsed from the file) = count
    capsd = rows["caps"].rename(columns={0: "key", 1: "out", 4: "ann"})
    capsd["out"] = capsd["out"].astype(np.int64)
    e = capsd["ann"].str.extractall(r":(\d+)")[0].astype(np.int64)
    capsd["chsum"] = e.groupby(level=0).sum().reindex(capsd.index).to_numpy()
    glr = rows["genealog"].rename(columns={0: "key", 1: "out", 4: "trav"})
    glr["out"] = glr["out"].astype(np.int64)
    t = capsd[["key", "out", "chsum"]].merge(glr[["key", "out", "trav"]], on=["key", "out"]).merge(
        exp[["key", "out", "count"]], on=["key", "out"])
    bad_t = int(((t["trav"].astype(np.int64) != t["chsum"]) | (t["chsum"] != t["count"])).sum()) + abs(len(t) - len(exp))
    check("totals", bad_t == 0, bad_t, "traversal = channel sum = count; total contributions %d" % int(exp_cnt.sum()))

    off = sum(int(((d[1].astype(np.int64) + 1) % W != 0).sum()) for d in rows.values())
    check("window_end", off == 0, off)
    check("metadata", not any(bad_meta.values()), sum(bad_meta.values()),
          "caps 8 B x M, sparse 12 B per pair, green 8 B per contributing record")
    stats["total_contributions"] = int(exp_cnt.sum())
    if a.stats:
        a.stats.write_text(json.dumps(stats, indent=1) + "\n")
    if a.csv:
        a.csv.parent.mkdir(parents=True, exist_ok=True)
        with a.csv.open("w", newline="") as h:
            w = csv.writer(h)
            w.writerow(["dataset", "M", "check", "status", "outputs", "mismatches", "detail"])
            for name, status, n, mism, detail in results:
                w.writerow([a.dataset, M, name, status, n, mism, detail])
    sys.exit(0 if all(r[1] == "PASS" for r in results) else 1)

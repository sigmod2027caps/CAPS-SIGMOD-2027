#!/usr/bin/env python3
"""Verification and aggregation of the temporal-query experiments (experiments/queries, experiments/query_length).

One dataflow directory holds one directory per repetition, rep1 .. rep<N>. Each has the files of the CAPS sink
(array = CAPS without index, prefix = CAPS index), of the Green sink (*_green.csv) and of the GeneaLog sink
(*_genealog.csv), all with the same query windows. This script
  1. verifies every repetition: Green equals CAPS in every channel of every answer, GeneaLog's traversal count
     equals the sum over the channels (common.verify_answers); exits with status 1 on a mismatch;
  2. merges the three methods' rows and aggregates over the repetitions: median of the latency, plus min and max.

Fixed window (default) writes query_latency.csv, insert_latency.csv, index_stats.csv, verification.csv in the
dataflow directory; --sweep writes query_length_latency.csv and verification.csv.

  aggregate_queries.py --dir experiments/queries/taxi_1 --channels 4 [--sweep]
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common  # noqa: E402


def rep_dirs(root):
    dirs = sorted((d for d in Path(root).iterdir() if d.is_dir() and d.name.startswith("rep")),
                  key=lambda d: int(d.name[3:]))
    if not dirs:
        raise SystemExit("no rep* directories in %s" % root)
    return dirs


def merge_rep(rep, files, out_name):
    """Concatenates the rows of the method files of one repetition (same columns) into rep/<out_name>."""
    rows, header = [], None
    for name in files:
        path = rep / name
        if not path.is_file():
            continue
        table = common.read_csv(path)
        if header is None:
            header = list(table[0].keys())
        rows += [[r[c] for c in header] for r in table]
    if header is None:
        raise SystemExit("no result file in %s: %s" % (rep, files))
    common.write_csv(rep / out_name, header, rows)
    return rep / out_name


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dir", type=Path, required=True)
    ap.add_argument("--channels", type=int, required=True)
    ap.add_argument("--sweep", action="store_true")
    args = ap.parse_args()

    reps = rep_dirs(args.dir)
    checks = []
    failed = [False]

    def report(rep):
        def add(check, n, bad):
            checks.append([rep.name, check, n, bad, "PASS" if bad == 0 else "FAIL"])
            failed[0] |= bad != 0
            print("  %-6s %-18s %s (%d mismatches of %d)" % (rep.name, check, "PASS" if bad == 0 else "FAIL", bad, n))
        return add

    for rep in reps:
        common.verify_answers(rep, args.channels, args.sweep, report(rep))
    common.write_csv(args.dir / "verification.csv", ["rep", "check", "answers", "mismatches", "status"], checks)

    if args.sweep:
        merged = [merge_rep(r, ["query_length_latency.csv", "query_length_latency_green.csv",
                                "query_length_latency_genealog.csv"], "latency_merged.csv") for r in reps]
        header, rows = common.aggregate_reps(merged, ["method", "len_ms"], spread=["avg_us"])
        common.write_csv(args.dir / "query_length_latency.csv", header, rows)
    else:
        merged = [merge_rep(r, ["query_latency.csv", "query_latency_green.csv", "query_latency_genealog.csv"],
                            "latency_merged.csv") for r in reps]
        header, rows = common.aggregate_reps(merged, ["method", "query"], spread=["avg_us", "total_ms"])
        common.write_csv(args.dir / "query_latency.csv", header, rows)

        merged = [merge_rep(r, ["insert_latency.csv", "insert_latency_green.csv"], "insert_merged.csv")
                  for r in reps]
        header, rows = common.aggregate_reps(merged, ["method"], spread=["avg_ns", "total_ms"])
        common.write_csv(args.dir / "insert_latency.csv", header, rows)

        # retained size of what each index keeps: CAPS prefix sums (index_bytes), Green polynomials (poly_bytes, 8
        # bytes per variable); identical in every repetition, so the first one is enough
        caps = common.read_csv(reps[0] / "index_stats.csv")[0]
        stats = [["prefix", caps["records"], caps["index_bytes"]]]
        green = reps[0] / "index_stats_green.csv"
        if green.is_file():
            g = common.read_csv(green)[0]
            stats.append(["green", g["records"], g["poly_bytes"]])
        common.write_csv(args.dir / "index_stats.csv", ["method", "records", "bytes"], stats)
    sys.exit(1 if failed[0] else 0)

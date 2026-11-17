#!/usr/bin/env python3
"""Checks that a run with a failure ends in the same state as the run without one (E4).

Compares each --other run directory with the --ref run directory (the run without a failure):
  answers   query_answers.csv: every benchmark answer (how-much; versioning too if the run has them) (windows are derived from the
            index's time span, so equal files also mean equal spans)
  digest    index_digest.csv: Tier-2 record count, digest of the records and the per-channel totals over the whole range
  stats     tiered_stats.csv: records seen, indexed, late and migrated by each sink shard (a duplicate contribution or a
            lost record changes them); with p > 1 the files have one row per shard and are compared shard by shard (the
            shards hold the same records in every run, the stream between the operators and the sinks is partitioned by key)
            The runs must also have no late record and no output behind the watermark (delta >= D).
  outputs   the id logs (outputs_s*.log, rows operator TAB window end TAB key TAB ordinal TAB hash): the ref log has no duplicate id,
            the distinct rows of the other log are exactly the rows of the ref log, and no id carries two contents.
            --ids-only drops the hash: taxi_2 averages doubles in arrival order, which differs between two runs of p > 1
            readers even without a failure (plain vs ref differ), so only the ids can be compared there
Appends one row per other run to --csv: the check results and the raw numbers (rows in the log, distinct rows,
re-emitted duplicates = raw - distinct). Exit status 1 if any check fails.
"""
import argparse
import glob
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import common  # noqa: E402

HEADER = ["dataflow", "run", "answers", "digest", "stats", "outputs_equal", "id_conflicts", "ref_outputs",
          "raw_outputs", "distinct_outputs", "duplicates", "late", "behind_watermark", "status"]


def same_file(a, b):
    return Path(a).read_bytes() == Path(b).read_bytes()


def stats_key(run):
    rows = common.read_csv(Path(run) / "tiered_stats.csv")
    return [(r["records"], r["indexed"], r["late"], r["migrated"]) for r in rows]


def late_counts(run):
    """(records late at the sinks, outputs behind the watermark), summed over the shards; both must be 0 when delta >= D."""
    rows = common.read_csv(Path(run) / "tiered_stats.csv")
    return sum(int(r["late"]) for r in rows), sum(int(r["behind_watermark"]) for r in rows)


def sorted_unique(logs, out, tmp, key=None, ids_only=False):
    """`sort -u` of the logs into `out` (byte order, so equal sets give equal files); key: sort -k fields;
    ids_only: without the hash column (the id of every output, not its content)."""
    cmd = ["sort", "-u", "-S", "4G", "-T", str(tmp), "-o", str(out)]
    if key:
        cmd[2:2] = ["-t", "\t", "-k", key]
    env = dict(os.environ, LC_ALL="C")
    if ids_only:
        cut = subprocess.Popen(["cut", "-f1-4"] + logs, stdout=subprocess.PIPE, env=env)
        subprocess.run(cmd, stdin=cut.stdout, check=True, env=env)
        cut.stdout.close()
        if cut.wait() != 0:
            raise subprocess.CalledProcessError(cut.returncode, "cut")
    else:
        subprocess.run(cmd + logs, check=True, env=env)


def line_count(path):
    with open(path, "rb") as handle:
        return sum(1 for _ in handle)


def logs_of(run):
    return sorted(glob.glob(str(Path(run) / "outputs_s*.log")))


def check_outputs(ref, other, tmp, ids_only=False):
    """(equal, id_conflicts, ref_outputs, raw, distinct)"""
    tmp.mkdir(parents=True, exist_ok=True)
    ref_logs, other_logs = logs_of(ref), logs_of(other)
    ref_raw = sum(line_count(f) for f in ref_logs)
    raw = sum(line_count(f) for f in other_logs)
    ref_u, other_u, ids_u = tmp / "ref.u", tmp / "other.u", tmp / "ids.u"
    sorted_unique(ref_logs, ref_u, tmp, ids_only=ids_only)
    sorted_unique(other_logs, other_u, tmp, ids_only=ids_only)
    ref_distinct, distinct = line_count(ref_u), line_count(other_u)
    sorted_unique([str(other_u)], ids_u, tmp, key="1,4")  # one row per (operator, window end, key, ordinal)
    conflicts = distinct - line_count(ids_u)
    equal = ref_raw == ref_distinct and same_file(ref_u, other_u)
    for f in (ref_u, other_u, ids_u):
        f.unlink()
    return equal, conflicts, ref_raw, raw, distinct


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dataflow", required=True)
    ap.add_argument("--ref", required=True)
    ap.add_argument("--other", required=True, action="append")
    ap.add_argument("--csv", required=True)
    ap.add_argument("--ids-only", action="store_true",
                    help="compare the output ids only, not their hashes (taxi_2 at p > 1: a double average summed in arrival order)")
    ap.add_argument("--tmp", default="/tmp")
    args = ap.parse_args()

    rows, failed = [], False
    for other in args.other:
        answers = same_file(Path(args.ref) / "query_answers.csv", Path(other) / "query_answers.csv")
        digest = same_file(Path(args.ref) / "index_digest.csv", Path(other) / "index_digest.csv")
        stats = stats_key(args.ref) == stats_key(other)
        tmp = Path(args.tmp) / ("verify_recovery_%d" % os.getpid())
        equal, conflicts, ref_n, raw, distinct = check_outputs(args.ref, other, tmp, args.ids_only)
        late, behind = late_counts(other)
        ok = answers and digest and stats and equal and conflicts == 0 and late == 0 and behind == 0
        failed |= not ok
        rows.append([args.dataflow, Path(other).name, int(answers), int(digest), int(stats), int(equal), conflicts,
                     ref_n, raw, distinct, raw - distinct, late, behind, "PASS" if ok else "FAIL"])
        print("%s %-8s %s answers=%d digest=%d stats=%d outputs_equal=%d conflicts=%d raw=%d distinct=%d duplicates=%d late=%d behind=%d"
              % (args.dataflow, Path(other).name, "PASS" if ok else "FAIL", answers, digest, stats, equal, conflicts,
                 raw, distinct, raw - distinct, late, behind))
    existing = common.read_csv(args.csv) if Path(args.csv).is_file() else []
    keep = [[r[h] for h in HEADER] for r in existing
            if not (r["dataflow"] == args.dataflow and r["run"] in {row[1] for row in rows})]
    common.write_csv(args.csv, HEADER, keep + rows)
    sys.exit(1 if failed else 0)

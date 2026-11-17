#!/usr/bin/env python3
"""Compares the benchmark answers of the index sinks (see index_check.sh for what is checked).

Reads, per parallelism p, <dir>/caps_p<p>/{query_answers,tiered_stats,tiered_queries,provisional_queries}.csv,
<dir>/genealog_p<p>/{query_answers_genealog,tiered_stats_genealog}.csv and
<dir>/slackbuffer_p1/query_answers.csv; appends one row per check to --csv and one row per
parallelism to --stats.
"""
import argparse
import csv
import os
import sys

CHECK_HEADER = ["dataflow", "check", "parallelism", "queries", "mismatches", "status", "detail"]
STATS_HEADER = [
    "dataflow", "parallelism", "shards", "records", "late", "migrated", "tier1_peak_max_shard",
    "tier1_peak_sum_shards", "tier1_final", "watermarks_max_shard", "max_w_minus_ts_ms",
    "insert_ns_per_record", "split_ns_per_record", "tier2_append_ns_per_record",
    "queries_final", "queries_provisional", "queries_undefined", "provisional_batches",
    "provisional_queries", "provisional_nonfinal", "provisional_avg_us",
    "provisional_monotone_violations", "late_files",
]

# With the index sink at the job's parallelism it is chained to the last operator, so no output crosses
# a shuffle before the sink and GeneaLog's serializer (which would collapse the graph of a tuple that
# crosses one into a set of source tuples) does not act: the GeneaLog
# totals are compared strictly at every parallelism. If a dataflow ever ends in a shuffle, list it here
# to report the difference instead of failing.
GENEALOG_P_INCOMPARABLE = ()


def read_rows(path):
    with open(path, newline="") as handle:
        return list(csv.DictReader(handle))


def read_caps_answers(path, channels):
    answers = {}
    for row in read_rows(path):
        counts = tuple(int(row["c%d" % c]) for c in range(channels))
        answers[(row.get("query") or row["len_ms"], int(row["i"]))] = (
            int(row["from"]), int(row["to"]), counts,
            int(row["defined"]) if "defined" in row else None,
            int(row["final"]) if "final" in row else None)
    return answers


def read_genealog_answers(path):
    answers = {}
    for row in read_rows(path):
        answers[(row.get("query") or row["len_ms"], int(row["i"]))] = (int(row["from"]), int(row["to"]), int(row["total"]))
    return answers


class Report:
    def __init__(self, dataflow, csv_path):
        self.dataflow = dataflow
        self.csv_path = csv_path
        self.rows = []
        self.failed = False

    def add(self, check, p, queries, mismatches, detail=""):
        status = "PASS" if mismatches == 0 else "FAIL"
        self.failed |= mismatches != 0
        self.rows.append([self.dataflow, check, p, queries, mismatches, status, detail])
        print("  %-30s p=%-3s %-5s (%d mismatches of %d) %s" % (check, p, status, mismatches, queries, detail))

    def info(self, check, p, queries, detail):
        self.rows.append([self.dataflow, check, p, queries, 0, "INFO", detail])
        print("  %-30s p=%-3s INFO  %s" % (check, p, detail))

    def write(self):
        exists = os.path.isfile(self.csv_path) and os.path.getsize(self.csv_path) > 0
        with open(self.csv_path, "a", newline="") as handle:
            writer = csv.writer(handle)
            if not exists:
                writer.writerow(CHECK_HEADER)
            writer.writerows(self.rows)


def stats_row(dataflow, p, rundir):
    shards = read_rows(os.path.join(rundir, "tiered_stats.csv"))
    prov = read_rows(os.path.join(rundir, "provisional_queries.csv"))
    queries = read_rows(os.path.join(rundir, "tiered_queries.csv"))
    records = sum(int(s["records"]) for s in shards)
    migrated = sum(int(s["migrated"]) for s in shards)
    gaps = [int(s["max_w_minus_ts_ms"]) for s in shards if s["max_w_minus_ts_ms"] != "NA"]
    n_prov = sum(int(b["queries"]) for b in prov)
    prov_us = sum(float(b["avg_us"]) * int(b["queries"]) for b in prov)
    late_files = len([f for f in os.listdir(rundir) if f.startswith("late_")])
    return [
        dataflow, p, len(shards), records, sum(int(s["late"]) for s in shards), migrated,
        max(int(s["tier1_peak"]) for s in shards), sum(int(s["tier1_peak"]) for s in shards),
        sum(int(s["tier1_final"]) for s in shards), max(int(s["watermarks"]) for s in shards),
        max(gaps) if gaps else "NA",
        "%.1f" % (sum(int(s["insert_ns"]) for s in shards) / max(1, records)),
        "%.1f" % (sum(int(s["split_ns"]) for s in shards) / max(1, migrated)),
        "%.1f" % (sum(int(s["tier2_append_ns"]) for s in shards) / max(1, migrated)),
        sum(int(q["final"]) for q in queries), sum(int(q["provisional"]) for q in queries),
        sum(int(q["undefined"]) for q in queries),
        len(prov), n_prov, sum(int(b["provisional"]) for b in prov),
        "%.3f" % (prov_us / n_prov) if n_prov else "NA",
        sum(int(b["monotone_violations"]) for b in prov), late_files,
    ]


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataflow", required=True)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--parallelisms", required=True)
    ap.add_argument("--channels", type=int, required=True)
    ap.add_argument("--genealog", type=int, default=1)
    ap.add_argument("--green", type=int, default=0)
    ap.add_argument("--sweep", type=int, default=0)
    ap.add_argument("--legacy", type=int, default=1)
    ap.add_argument("--small", type=int, default=0)
    ap.add_argument("--csv", required=True)
    ap.add_argument("--stats", required=True)
    args = ap.parse_args()

    ps = args.parallelisms.split()
    report = Report(args.dataflow, args.csv)
    caps = {p: read_caps_answers(os.path.join(args.dir, "caps_p%s" % p, "query_answers.csv"), args.channels)
            for p in ps}
    n_queries = len(caps[ps[0]])

    # (a) identical across parallelism
    ref_p = ps[0]
    for p in ps[1:]:
        bad = sum(1 for k in caps[ref_p] if caps[p].get(k) != caps[ref_p][k])
        bad += len(set(caps[p]) - set(caps[ref_p]))
        report.add("caps_p%s_vs_p%s" % (p, ref_p), p, n_queries, bad)

    # (d) every post-job answer is final, zero late records; stats rows
    stats = []
    for p in ps:
        rundir = os.path.join(args.dir, "caps_p%s" % p)
        row = stats_row(args.dataflow, p, rundir)
        stats.append(row)
        s = dict(zip(STATS_HEADER, row))
        report.add("zero_late", p, int(s["records"]), int(s["late"]) + int(s["late_files"]),
                   "max(W-ts)=%s ms" % s["max_w_minus_ts_ms"])
        report.add("post_job_all_final", p, n_queries, int(s["queries_provisional"]))
        report.add("tier1_empty_at_end", p, int(s["shards"]), int(s["tier1_final"]))
        report.add("provisional_during_stream", p, int(s["provisional_queries"]),
                   int(s["provisional_queries"]) - int(s["provisional_nonfinal"])
                   + int(s["provisional_monotone_violations"]),
                   "%s batches, avg %s us" % (s["provisional_batches"], s["provisional_avg_us"]))

    # (b) GeneaLog index
    if args.genealog:
        for p in ps:
            path = os.path.join(args.dir, "genealog_p%s" % p, "query_answers_genealog.csv")
            gen = read_genealog_answers(path)
            bad = 0
            for k, (frm, to, counts, _, _) in caps[p].items():
                g = gen.get(k)
                if g is None or g[0] != frm or g[1] != to or g[2] != sum(counts):
                    bad += 1
            bad += abs(len(gen) - len(caps[p]))
            if p != "1" and args.dataflow in GENEALOG_P_INCOMPARABLE:
                report.info("genealog_vs_caps", p, len(gen),
                            "%d differ (GeneaLog serializer counts distinct source tuples across a shuffle)" % bad)
            else:
                report.add("genealog_vs_caps", p, len(gen), bad)
            gstats = read_rows(os.path.join(args.dir, "genealog_p%s" % p, "tiered_stats_genealog.csv"))
            glate = sum(int(s["late"]) for s in gstats)
            report.add("genealog_zero_late", p, sum(int(s["records"]) for s in gstats), glate,
                       "tier1 peak max shard %d" % max(int(s["tier1_peak"]) for s in gstats))
            # same indexed records as CAPS
            report.add("genealog_records_vs_caps", p, 1,
                       int(sum(int(s["indexed"]) for s in gstats)
                           != sum(int(s["indexed"]) for s in read_rows(
                               os.path.join(args.dir, "caps_p%s" % p, "tiered_stats.csv")))))

    # (b') Green index: the same file format as CAPS's, every channel of every answer must be equal
    if args.green:
        for p in ps:
            green = read_caps_answers(os.path.join(args.dir, "green_p%s" % p, "query_answers_green.csv"), args.channels)
            bad = sum(1 for k, v in caps[p].items() if green.get(k) != v) + len(set(green) - set(caps[p]))
            report.add("green_vs_caps", p, len(green), bad)
            gstats = read_rows(os.path.join(args.dir, "green_p%s" % p, "tiered_stats_green.csv"))
            report.add("green_zero_late", p, sum(int(s["records"]) for s in gstats),
                       sum(int(s["late"]) for s in gstats), "tier1 peak max shard %d" % max(int(s["tier1_peak"]) for s in gstats))

    # (a', b') the window-length sweep
    if args.sweep:
        sweep = {p: read_caps_answers(os.path.join(args.dir, "caps_len_p%s" % p, "query_length_answers.csv"),
                                      args.channels) for p in ps}
        for p in ps[1:]:
            bad = sum(1 for k in sweep[ref_p] if sweep[p].get(k) != sweep[ref_p][k])
            bad += len(set(sweep[p]) - set(sweep[ref_p]))
            report.add("sweep_caps_p%s_vs_p%s" % (p, ref_p), p, len(sweep[ref_p]), bad)
        if args.green:
            for p in ps:
                green = read_caps_answers(os.path.join(args.dir, "green_len_p%s" % p, "query_length_answers_green.csv"),
                                          args.channels)
                bad = sum(1 for k, v in sweep[p].items() if green.get(k) != v) + len(set(green) - set(sweep[p]))
                report.add("sweep_green_vs_caps", p, len(green), bad)
        undefined = sum(1 for v in sweep[ref_p].values() if v[3] == 0)
        report.info("sweep_undefined_windows", ref_p, len(sweep[ref_p]), "%d undefined (bottom)" % undefined)
        if args.genealog:
            for p in ps:
                gen = read_genealog_answers(
                    os.path.join(args.dir, "genealog_len_p%s" % p, "query_length_answers_genealog.csv"))
                bad = sum(1 for k, (frm, to, counts, _, _) in sweep[p].items()
                          if k not in gen or gen[k] != (frm, to, sum(counts))) + len(set(gen) - set(sweep[p]))
                if p != "1" and args.dataflow in GENEALOG_P_INCOMPARABLE:
                    report.info("sweep_genealog_vs_caps", p, len(gen), "%d differ (serializer)" % bad)
                else:
                    report.add("sweep_genealog_vs_caps", p, len(gen), bad)

    # (c) released slack-buffer logic
    if args.legacy:
        legacy = read_caps_answers(os.path.join(args.dir, "slackbuffer_p1", "query_answers.csv"), args.channels)
        base = caps["1"] if "1" in caps else caps[ref_p]
        bad = sum(1 for k, v in legacy.items() if k not in base or base[k][:3] != v[:3]) + abs(len(legacy) - len(base))
        report.add("slackbuffer_vs_caps_p1", 1, len(legacy), bad)

    report.write()
    exists = os.path.isfile(args.stats) and os.path.getsize(args.stats) > 0
    with open(args.stats, "a", newline="") as handle:
        writer = csv.writer(handle)
        if not exists:
            writer.writerow(STATS_HEADER)
        writer.writerows(stats)
    sys.exit(1 if report.failed else 0)

#!/usr/bin/env python3
"""Markdown tables of experiment E3 from the CSVs of analyze.py.

Usage: tables.py --results RESULTS_DIR > tables.md
"""
import argparse
import csv
import statistics
from collections import defaultdict
from pathlib import Path

D_LABELS = {"0": "0", "1000": "1 s", "10000": "10 s", "60000": "60 s", "300000": "5 min", "1800000": "30 min"}
DEFAULT = dict(p="4", slow="1", zipf="0", tier1="hybrid", interval="50", idle="0", batch="1024", D="60000", delta="60000", sinkp="4")


def load(path):
    with Path(path).open(newline="") as f:
        return list(csv.DictReader(f))


def num(x, default=float("nan")):
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def fmt(x, digits=3):
    if x is None or x == "" or (isinstance(x, float) and x != x):
        return "-"
    if isinstance(x, str):
        return x
    if abs(x) >= 1000:
        return "{:,.0f}".format(x)
    return ("%." + str(digits) + "g") % x


def table(header, rows):
    if not rows:
        return "none"
    out = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def pick(runs, df, **over):
    want = dict(DEFAULT)
    want.update({k: str(v) for k, v in over.items()})
    rr = [r for r in runs if r["dataflow"] == df and all(r.get(k) == v for k, v in want.items())]
    return rr[0] if rr else None


def correctness(runs):
    rows = []
    for df in sorted({r["dataflow"] for r in runs}):
        mine = [r for r in runs if r["dataflow"] == df and r["delta_ge_D"] == "1" and r["idle"] == "0"]
        if not mine:
            continue
        comp = sum(int(num(r["queries"], 0)) for r in mine)
        diff = sum(int(num(r["answers_differ"], 0)) for r in mine)
        miss = sum(int(num(r["indexed_missing"], 0)) for r in mine)
        late = sum(int(num(r["late_at_sink"], 0)) for r in mine)
        behind = sum(int(num(r["behind_watermark"], 0)) + int(num(r["nonmonotone_appends"], 0)) for r in mine)
        dropped = sum(int(num(r["late_total"], 0)) for r in mine)
        outs = [r for r in mine if r.get("only_in_run") not in (None, "")]
        out_diff = sum(int(num(r["only_in_run"], 0)) + int(num(r["only_in_ref"], 0)) for r in outs)
        out_rows = sum(int(num(r["outputs_ref"], 0)) for r in outs)
        rows.append([df, len(mine), comp, diff, miss, late, behind, dropped,
                     ("%s of %s rows (%d runs)" % (fmt(float(out_diff)), fmt(float(out_rows)), len(outs))) if outs else "not run"])
    return table(["dataflow", "configs (delta >= D)", "answers compared", "answers differing", "indexed records missing vs ordered run",
                  "late at sink index", "behind watermark / non-monotone appends", "late at window operators",
                  "CAPS output rows differing (run only + ref only)"], rows)


def below(runs):
    rows = []
    for df in sorted({r["dataflow"] for r in runs}):
        for D in ("60000", "300000"):
            for f in (0, 0.5, 2):
                d = str(int(D) * f if f != 0.5 else int(D) // 2)
                r = pick(runs, df, D=D, delta=d)
                if not r:
                    continue
                rows.append([df, "%s s" % (int(D) / 1000), "%s s (%sD)" % (int(d) / 1000, f), fmt(num(r["runs_violations_mean"])),
                             "%s (%s-%s)" % (fmt(num(r["runs_late_mean"])), fmt(num(r["runs_late_min"])), fmt(num(r["runs_late_max"]))),
                             "%s%%" % fmt(100 * num(r["runs_late_mean"]) / max(num(r["source_records"]), 1)),
                             "%s (%s-%s) of %s" % (fmt(num(r["runs_differ_mean"])), fmt(num(r["runs_differ_min"])), fmt(num(r["runs_differ_max"])), fmt(num(r["queries"]))),
                             fmt(num(r["only_in_run"]) + num(r["only_in_ref"])) if r.get("only_in_run") not in (None, "") else "-",
                             r["meas_runs"]])
    return table(["dataflow", "D", "delta", "records more than delta behind the newest of their reader", "late records (missed a window or dropped), mean (min-max)",
                  "of the input", "answers differing from the ordered run, mean (min-max)", "output rows differing (one run)", "runs"], rows)


def occupancy(runs):
    rows = []
    for df in sorted({r["dataflow"] for r in runs}):
        for sink, sinkp in (("shards", "4"), ("global", "1")):
            r = pick(runs, df, sinkp=sinkp)
            if not r:
                continue
            rows.append([df, sink, fmt(num(r["peak_max_shard"])), fmt(num(r["peak_sum_shards"])), fmt(num(r.get("spread_max_ms"))),
                         fmt(num(r.get("gap_max_ms"))), fmt(num(r.get("bound_avg_rate_measured_max_shard"))), fmt(num(r.get("bound_window_measured_max_shard"))),
                         fmt(num(r.get("peak_le_window_bound"))), fmt(num(r.get("peak_le_avg_bound")))])
    return table(["dataflow", "sink", "peak Tier 1, largest shard", "sum of shard peaks", "spread, max [ms]", "largest watermark gap [ms]",
                  "bound, average rate", "bound, peak rate", "peak <= peak-rate bound", "peak <= average-rate bound"], rows)


def occupancy_sweep(runs):
    rows = []
    for df in sorted({r["dataflow"] for r in runs}):
        for sinkp in ("4", "1"):
            for i in ("10", "50", "250", "1000"):
                r = pick(runs, df, sinkp=sinkp, interval=i)
                if not r:
                    continue
                rows.append([df, "shards" if sinkp == "4" else "global", i, fmt(num(r["peak_max_shard"])), fmt(num(r.get("bound_avg_rate_measured_max_shard"))),
                             fmt(num(r.get("bound_window_measured_max_shard"))), fmt(num(r.get("gap_max_ms"))), fmt(num(r.get("spread_max_ms")))])
    return table(["dataflow", "sink", "interval [ms]", "peak Tier 1 (largest shard)", "bound, average rate", "bound, peak rate",
                  "largest gap [ms]", "spread [ms]"], rows)


def finality(runs):
    rows = []
    for df in sorted({r["dataflow"] for r in runs}):
        r = pick(runs, df)
        if not r:
            continue
        trio = lambda k: " / ".join(fmt(num(r.get("%s_%s_ms" % (k, q)))) for q in ("median", "p99", "max"))
        rows.append([df, trio("local"), trio("global"), trio("ready"), trio("wm_latency"), fmt(num(r.get("bound_system_ms"))),
                     fmt(num(r.get("bound_wait_ms"))), fmt(num(r.get("ingest_speed_event_ms_per_proc_ms")))])
    return table(["dataflow", "sink: arrival to migration (median / p99 / max, ms)", "global: all shards", "after ingestion of delta + 1",
                  "watermark latency", "bound interval + spread [ms]", "delta + tau [ms]", "replay speed [event ms per ms]"], rows)


def finality_idle(runs):
    rows = []
    for df in sorted({r["dataflow"] for r in runs}):
        for u in ("0", "20"):
            r = pick(runs, df, slow="4", idle=u, batch="16384")
            if not r:
                continue
            rows.append([df, u, fmt(num(r.get("ready_median_ms"))) + " / " + fmt(num(r.get("ready_p99_ms"))), fmt(num(r.get("global_p99_ms"))),
                         fmt(num(r["idle_marks"])), fmt(num(r["runs_late_mean"])), "%s%%" % fmt(100 * num(r["runs_late_mean"]) / max(num(r["source_records"]), 1)),
                         fmt(num(r["runs_differ_mean"]))])
    return table(["dataflow", "tau [ms]", "ready delay median / p99 [ms]", "global delay p99 [ms]", "idle marks", "late records (mean)", "of the input",
                  "answers differing"], rows)


def overhead(runs, timing):
    by = {(t["dataflow"], t["cfg"], t["method"]): t for t in timing}
    arms = [("D = 0, p = 4", dict(D="0", delta="0")), ("D = 60 s", {}), ("D = 5 min", dict(D="300000", delta="300000")),
            ("p = 1", dict(p="1", sinkp="1")), ("p = 2", dict(p="2", sinkp="2")), ("p = 8", dict(p="8", sinkp="8")),
            ("slow x4", dict(slow="4")), ("Zipf 1", dict(zipf="1.0")), ("slow x4 + Zipf 1", dict(slow="4", zipf="1.0")), ("global sink", dict(sinkp="1"))]
    rows = []
    for df in sorted({r["dataflow"] for r in runs}):
        for label, over in arms:
            r = pick(runs, df, **over)
            if not r:
                continue
            n, c = by.get((df, r["cfg"], "noprov")), by.get((df, r["cfg"], "caps"))
            if not n or not c or num(n["n"], 0) == 0 or num(c["n"], 0) == 0:
                continue
            ratio = num(c["median"]) / num(n["median"])
            cv = ((num(c["std"]) / num(c["mean"])) ** 2 + (num(n["std"]) / num(n["mean"])) ** 2) ** 0.5
            rows.append([df, label, "%s +- %s" % (fmt(num(n["median"])), fmt(num(n["std"]))), "%s +- %s" % (fmt(num(c["median"])), fmt(num(c["std"]))),
                         "%s +- %s" % (fmt(ratio), fmt(ratio * cv)), r.get("timing_runner", "")])
    return table(["dataflow", "configuration", "noprov [s]", "CAPS (two-tier index) [s]", "CAPS / noprov", "runtime"], rows)


def latency(runs):
    rows = []
    for df in sorted({r["dataflow"] for r in runs}):
        for sink, sinkp in (("shards", "4"), ("global", "1")):
            r = pick(runs, df, sinkp=sinkp)
            if not r:
                continue
            rows.append([df, sink, fmt(num(r.get("prefix_howmuch_us"))), fmt(num(r.get("prefix_versioning_us"))), fmt(num(r.get("array_howmuch_us"))),
                         fmt(num(r.get("provisional_avg_us"))), fmt(num(r.get("provisional_max_batch_us"))), r.get("provisional_batches", ""), fmt(num(r.get("nonfinal_queries"))),
                         r.get("provisional_all_nonfinal", "")])
    return table(["dataflow", "sink", "final how-much [us]", "final versioning [us]", "scan how-much [us]", "provisional avg [us]",
                  "provisional, worst batch [us]", "provisional batches", "post-job queries not final", "all provisional non-final"], rows)


def tier1(runs):
    """One row per (dataflow, sink, interval); a variant that did not finish (a sorted buffer behind a global sink) is T/O."""
    rows = []
    cost = lambda r: num(r["insert_ns_per_rec"]) + num(r["split_ns_per_rec"])
    for df in sorted({r["dataflow"] for r in runs}):
        for sinkp, sink in (("4", "shards"), ("1", "global")):
            for i in ("10", "50", "250", "1000"):
                v = {t: pick(runs, df, sinkp=sinkp, interval=i, tier1=t) for t in ("tree", "hybrid", "buffer")}
                if not v["tree"] and not v["hybrid"]:
                    continue
                three = lambda f: " / ".join(f(v[t]) if v[t] else "T/O" for t in ("tree", "hybrid", "buffer"))
                rows.append([df, sink, i, fmt(num((v["tree"] or v["hybrid"])["peak_max_shard"])),
                             three(lambda r: fmt(cost(r))), three(lambda r: fmt(num(r["provisional_avg_us"]))),
                             three(lambda r: fmt(num(r.get("local_p99_ms"))))])
    return table(["dataflow", "sink", "interval [ms]", "peak Tier 1 (tree run)", "insert + split [ns/rec]: tree / hybrid / buffer",
                  "provisional query [us]: tree / hybrid / buffer", "arrival to migration p99 [ms]: tree / hybrid / buffer"], rows)


def paced_matrix(runs):
    """Every D and delta of a paced run: the late records and wrong answers, with the min-max over the repetitions."""
    rows = []
    for r in sorted(runs, key=lambda r: (r["dataflow"], num(r["D"]), num(r["delta"]))):
        D, delta = num(r["D"]), num(r["delta"])
        ratio = "" if D == 0 else " (%sD)" % fmt(delta / D)
        late = num(r["runs_late_mean"])
        frac = 100 * late / num(r["source_records"])
        both = [num(r.get("only_in_run")), num(r.get("only_in_ref"))]
        rows.append([r["dataflow"], fmt(D / 1000), fmt(delta / 1000) + ratio, fmt(num(r["source_violations"])),
                     "%s (%s-%s)" % (fmt(late), fmt(num(r["runs_late_min"])), fmt(num(r["runs_late_max"]))), fmt(frac) + "%",
                     "%s (%s-%s) of %s" % (fmt(num(r["runs_differ_mean"])), fmt(num(r["runs_differ_min"])),
                                           fmt(num(r["runs_differ_max"])), fmt(num(r["queries"]))),
                     fmt(num(r["runs_missing_mean"])), "-" if r.get("only_in_run", "") == "" else fmt(sum(both)),
                     "%s / %s" % (fmt(num(r["wm_step_median_s"])), fmt(num(r["wm_step_p90_s"]))), r["meas_runs"]])
    return table(["dataflow", "D [s]", "delta [s]", "records more than delta behind the newest of their reader",
                  "late records (missed a window or dropped), mean (min-max)", "of the input",
                  "answers differing from the ordered run, mean (min-max)", "indexed records missing (mean)",
                  "output rows differing (one run)", "watermark step at the sink, median / p90 [s of event time]", "runs"], rows)


def paced_lag(runs):
    """The speed of the paced readers and the run time of a configuration."""
    rows = []
    for df in sorted({r["dataflow"] for r in runs}):
        mine = [r for r in runs if r["dataflow"] == df]
        rows.append([df, fmt(statistics.median(num(r["reader_speed_event_ms_per_ms"]) for r in mine)),
                     fmt(statistics.median(num(r["late_runtime_s"]) for r in mine)), len(mine)])
    return table(["dataflow", "replay speed [event ms per processing ms]", "run time of a configuration, median [s]",
                  "configurations"], rows)


def timeouts(path):
    if not Path(path).exists():
        return "none"
    rows = [[r["dataflow"], r["cfg"], r["run"]] for r in load(path)]
    return table(["dataflow", "configuration", "run"], rows) if rows else "none"


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--results", type=Path, required=True)
    ap.add_argument("--paced", action="store_true", help="the tables of a paced run (arms D and delta, watermark interval 1 ms)")
    args = ap.parse_args()
    runs = load(args.results / "e3_runs.csv")
    timing = load(args.results / "e3_timing.csv")
    if args.paced:
        DEFAULT["interval"] = "1"
        for title, text in (("Correctness (delta >= D)", correctness(runs)), ("Every D and delta", paced_matrix(runs)),
                            ("Pace", paced_lag(runs))):
            print("### %s\n\n%s\n" % (title, text))
        raise SystemExit(0)
    for title, text in (("Correctness (delta >= D, no idle reader)", correctness(runs)), ("Price of delta < D (D = 60 s)", below(runs)),
                        ("Tier-1 occupancy and its bound (D = delta = 60 s, p = 4)", occupancy(runs)),
                        ("Tier-1 occupancy against the watermark interval", occupancy_sweep(runs)), ("Delay to finality", finality(runs)),
                        ("Idleness timeout (slow reader with long pauses)", finality_idle(runs)), ("Query latency", latency(runs)),
                        ("Overhead of CAPS against noprov", overhead(runs, timing)), ("Tier 1 in situ: tree, hybrid (the default) and sorted buffer", tier1(runs)),
                        ("Runs that did not finish within the time limit", timeouts(args.results / "e3_timeouts.csv"))):
        print("### %s\n\n%s\n" % (title, text))

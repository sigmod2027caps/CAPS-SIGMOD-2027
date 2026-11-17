#!/usr/bin/env python3
"""Figures of experiment E3 (disorder, skew, parallelism, late data), from the CSVs of analyze.py.

One subplot per dataflow (alphabetical), paper style (experiments/common.py). Every figure ships with a CSV of the
plotted series. Timings come from the in-process runtime unless the CSV says "cluster" (marked in the legend note).

  e3_overhead_D       job duration of noprov and CAPS (error bars: std over the repetitions) against the disorder D (delta = D)
  e3_overhead_p       the same against the parallelism (readers, operators and sinks)
  e3_overhead_skew    the same for the skew knobs: slow reader, Zipf keys, one global sink
  e3_late_delta       records that were late (missed a window or were dropped) against delta, at D = 60 s, 5 min (and 30 min if paced)
  e3_answers_delta    answers that differ from the ordered run, the same grid
  e3_occupancy        peak Tier-1 occupancy of a shard against the analytical bound, against the watermark interval
  e3_finality         delay to finality against D: sink-side, global (frontier of the shards), and after the readers
                      ingested the event time that justifies the output, with the bound interval + spread
  e3_occupancy_time   Tier-1 occupancy over processing time at every watermark step, one line per shard (one sink per
                      subtask) and the global sink (dashed), default configuration
  e3_tier1_cost       Tier-1 cost in situ, tree, hybrid and sorted buffer (insert + split per record), one global sink
  e3_tier1_cost_shards the same with one shard per sink subtask (ordered input: the hybrid appends)
  e3_tier1_query      latency of provisional queries in situ, tree, hybrid and sorted buffer (global sink; _shards: per-subtask shards)

Usage: plot_disorder.py --results RESULTS_DIR
"""
import argparse
import csv
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import common  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402

D_LABELS = {"0": "0", "1000": "1 s", "10000": "10 s", "60000": "60 s", "300000": "5 min"}   # (the overhead arm; the paced late figure adds 30 min)
BASE = dict(p="4", slow="1", zipf="0", tier1="hybrid", interval="50", idle="0", batch="1024")   # the default point of the arms
D_DEFAULT = "60000"


def load(path):
    with Path(path).open(newline="") as f:
        return list(csv.DictReader(f))


def num(x, default=float("nan")):
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def sel(rows, **where):
    return [r for r in rows if all(r.get(k) == str(v) for k, v in where.items())]


def runs_for(runs, dataflow, cond):
    return [r for r in runs if r["dataflow"] == dataflow and cond(r)]


def grouped_bars(ax, cats, series, log_y=False, floor=None):
    """series: list of (key, values, errs, color, hatch, label). Bars of one category side by side (grouped bar width)."""
    w = common.GROUP_BAR_WIDTH
    handles = []
    for i, (key, vals, errs, color, hatch, label) in enumerate(series):
        x = np.arange(len(cats)) + (i - (len(series) - 1) / 2) * w * 1.15
        v = np.array([floor if (floor is not None and (np.isnan(a) or a <= 0)) else a for a in vals], dtype=float)
        h = ax.bar(x, v, width=w, color=color, hatch=hatch, edgecolor="black", linewidth=0.5, label=label)
        if errs is not None:
            e = np.array([0 if np.isnan(a) else a for a in errs], dtype=float)
            ax.errorbar(x, v, yerr=e, fmt="none", ecolor="black", capsize=3, linewidth=1)
        handles.append(h)
    ax.set_xticks(np.arange(len(cats)))
    ax.set_xticklabels(cats)
    return handles


def has_data(ax):
    """True if some artist of the axes has a finite positive value (a log axis needs one)."""
    for patch in ax.patches:
        h = patch.get_height()
        if np.isfinite(h) and h > 0:
            return True
    for line in ax.lines:
        y = np.asarray(line.get_ydata(), dtype=float)
        if np.any(np.isfinite(y) & (y > 0)):
            return True
    return False


def log_axis(ax):
    """Log y axis in the paper's style, or a plain one if there is nothing positive to show."""
    if has_data(ax):
        ax.set_yscale("log")
        common.style_axis(ax, log_y=True)
    else:
        common.style_axis(ax)


def figure(dataflows, ylabel, xlabel):
    fig, axes = common.new_figure(len(dataflows))
    axes[0].set_ylabel(ylabel)
    for ax, df in zip(axes, dataflows):
        ax.set_title(common.DATAFLOW_NAMES.get(df, df))
        ax.set_xlabel(xlabel)
    return fig, axes


def write_csv(path, header, rows):
    with Path(path).open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def overhead_figure(res, name, runs, timing, dataflows, groups, xlabel):
    """groups: list of (category label, predicate on a run row). Bars: noprov, CAPS; y = median job duration, error = std."""
    by_cfg = {(t["dataflow"], t["cfg"], t["method"]): t for t in timing}
    fig, axes = figure(dataflows, "Job duration [s]", xlabel)
    out = []
    handles = {}
    for ax, df in zip(axes, dataflows):
        series = []
        for method in ("noprov", "caps"):
            vals, errs = [], []
            for label, pred in groups:
                rr = runs_for(runs, df, pred)
                t = by_cfg.get((df, rr[0]["cfg"], method)) if rr else None
                vals.append(num(t["median"]) if t else float("nan"))
                errs.append(num(t["std"]) if t else float("nan"))
                out.append([df, label, method, vals[-1], errs[-1], t["reps"] if t else "", rr[0].get("timing_runner", "") if rr else ""])
            series.append((method, vals, errs, common.METHOD_COLORS[method], common.METHOD_HATCHES[method],
                           common.METHOD_NAMES[method]))
        hs = grouped_bars(ax, [g[0] for g in groups], series)
        handles = dict(zip(("noprov", "caps"), hs))
        common.style_axis(ax)
    common.shared_legend(fig, [handles[m] for m in ("noprov", "caps")], [common.METHOD_NAMES[m] for m in ("noprov", "caps")])
    common.save_pdf(fig, res / (name + ".pdf"))
    write_csv(res / (name + ".csv"), ["dataflow", "x", "method", "median_s", "std_s", "reps_s", "runner"], out)
    print("  %s.pdf, %s.csv" % (name, name))


def late_figure(res, runs, dataflows, what="late"):
    """Bars of the records that were late (what = "late", % of the input, log scale) or of the answers that differ from the
    ordered run (what = "answers", % of the compared answers) against delta = 0, D/2, D, 2D, one series per D."""
    factors = [("0", "0"), ("0.5", "D/2"), ("1", "D"), ("2", "2D")]
    present = {r["D"] for r in runs}
    series_spec = [(D, common.DISORDER_COLORS[D], common.DISORDER_HATCHES[D], common.DISORDER_NAMES[D])
                   for D in common.DISORDER_ORDER if D in present]
    ylabel = "Late records [% of input]" if what == "late" else "Wrong answers [%]"
    fig, axes = figure(dataflows, ylabel, r"$\delta$")
    out = []
    handles = []
    floor = 1e-4
    for ax, df in zip(axes, dataflows):
        series = []
        tops = []
        for D, color, hatch, label in series_spec:
            vals, errs = [], []
            for f, _ in factors:
                delta = str(int(round(float(D) * float(f))))
                rr = runs_for(runs, df, lambda r: r["D"] == D and r["delta"] == delta and all(r[k] == v for k, v in BASE.items()) and r["sinkp"] == "4")
                if not rr or (what == "late" and "runs_late_mean" not in rr[0]):
                    vals.append(float("nan")); errs.append(float("nan")); continue
                r = rr[0]
                n = max(num(r["source_records"]), 1) if what == "late" else max(num(r["queries"]), 1)
                key = "runs_late" if what == "late" else "runs_differ"
                vals.append(100 * num(r[key + "_mean"]) / n)
                errs.append(100 * (num(r[key + "_max"]) - num(r[key + "_mean"])) / n)
                tops.append(vals[-1] + errs[-1])
                out.append([df, D, delta, vals[-1], 100 * num(r[key + "_min"]) / n, 100 * num(r[key + "_max"]) / n, r[key + "_mean"],
                            r["source_records"] if what == "late" else r["queries"], r["runs_violations_mean"], r["meas_runs"],
                            100 * num(r[key + "_median"]) / n if (key + "_median") in r else float("nan")])
            series.append((D, vals, errs, color, hatch, label))
        handles = grouped_bars(ax, [lab for _, lab in factors], series, floor=floor if what == "late" else None)
        if what == "late":
            for i, (_, vals, _, _, _, _) in enumerate(series):
                for xi, v in enumerate(vals):
                    if v == v and v <= 0:
                        ax.annotate("0", (xi + (i - (len(series) - 1) / 2) * common.GROUP_BAR_WIDTH * 1.15, floor), ha="center", va="bottom", fontsize=common.FONT["annotation"])
            top = max(tops) * 3 if tops else 1.0
            ax.set_yscale("log")
            ax.set_ylim(floor, max(1.0, top))
            common.style_axis(ax, log_y=True)
        else:
            ax.set_ylim(0, 100)
            common.style_axis(ax)
    common.shared_legend(fig, handles, [lab for *_, lab in series_spec])
    name = "e3_late_delta" if what == "late" else "e3_answers_delta"
    write_csv(res / (name + ".csv"), ["dataflow", "D_ms", "delta_ms", "pct_mean", "pct_min", "pct_max",
                                      "mean", "denominator", "source_violations_mean", "meas_runs", "pct_median"], out)
    common.save_pdf(fig, res / (name + ".pdf"))
    print("  %s.pdf, %s.csv" % (name, name))


def occupancy_figure(res, runs, dataflows, sink, name):
    intervals = ["10", "50", "250", "1000"]
    fig, axes = figure(dataflows, "Tier-1 occupancy [records]", "Watermark interval [ms]")
    out = []
    sinkp = "4" if sink == "shards" else "1"
    for ax, df in zip(axes, dataflows):
        peak, b_avg, b_win = [], [], []
        for i in intervals:
            rr = runs_for(runs, df, lambda r: r["D"] == D_DEFAULT and r["delta"] == D_DEFAULT and r["interval"] == i and r["tier1"] == BASE["tier1"]
                          and r["p"] == "4" and r["sinkp"] == sinkp and r["slow"] == "1" and r["zipf"] == "0" and r["idle"] == "0")
            r = rr[0] if rr else {}
            peak.append(num(r.get("peak_max_shard")))
            b_avg.append(num(r.get("bound_avg_rate_measured_max_shard")))
            b_win.append(num(r.get("bound_window_measured_max_shard")))
            out.append([df, sink, i, peak[-1], b_avg[-1], b_win[-1], r.get("spread_max_ms", ""), r.get("gap_max_ms", "")])
        hs = grouped_bars(ax, intervals, [("peak", peak, None, common.METHOD_COLORS["caps"], common.METHOD_HATCHES["caps"], "peak occupancy of a shard")])
        x = np.arange(len(intervals))
        (h1,) = ax.plot(x, b_avg, linestyle="none", marker="D", markersize=11, color="black", markerfacecolor="white", markeredgewidth=1.5)
        (h2,) = ax.plot(x, b_win, linestyle="none", marker="^", markersize=12, color="black", markerfacecolor="black")
        log_axis(ax)
    common.shared_legend(fig, [hs[0], h1, h2], ["peak occupancy", "bound, average rate", "bound, peak rate"])
    common.save_pdf(fig, res / (name + ".pdf"))
    write_csv(res / (name + "_plot.csv"), ["dataflow", "sink", "interval_ms", "peak_tier1_max_shard", "bound_avg_rate", "bound_peak_rate",
                                           "spread_max_ms", "step_gap_max_ms"], out)   # (e3_occupancy.csv itself is the per-shard table of analyze.py)
    print("  %s.pdf, %s_plot.csv" % (name, name))


def finality_figure(res, runs, dataflows):
    ds = ["0", "1000", "10000", "60000", "300000"]
    fig, axes = figure(dataflows, "Delay to finality, p99 [ms]", "Disorder $D$ (= $\\delta$)")
    out = []
    keys = tuple((st + "_p99_ms", common.STAGE_NAMES[st], common.STAGE_COLORS[st], common.STAGE_HATCHES[st])
                 for st in common.STAGE_ORDER)
    handles = []
    for ax, df in zip(axes, dataflows):
        series = {k: [] for k, *_ in keys}
        bound = []
        for d in ds:
            rr = runs_for(runs, df, lambda r: r["D"] == d and r["delta"] == d and r["sinkp"] == "4" and all(r[k] == v for k, v in BASE.items()))
            r = rr[0] if rr else {}
            for k, *_ in keys:
                series[k].append(num(r.get(k)))
            bound.append(num(r.get("bound_system_ms")))
            out.append([df, d] + [series[k][-1] for k, *_ in keys] + [bound[-1], r.get("spread_max_ms", ""), r.get("gap_max_ms", "")])
        hs = grouped_bars(ax, [D_LABELS[d] for d in ds], [(k, series[k], None, c, h, l) for k, l, c, h in keys])
        (hb,) = ax.plot(np.arange(len(ds)), bound, linestyle="--", marker="o", color="black", markersize=9, linewidth=2)
        handles = hs + [hb]
        common.style_axis(ax)
    common.shared_legend(fig, handles, [l for _, l, _, _ in keys] + ["interval + spread"])
    common.save_pdf(fig, res / "e3_finality.pdf")
    write_csv(res / "e3_finality_plot.csv", ["dataflow", "D_ms"] + [k for k, *_ in keys] + ["bound_interval_plus_spread_ms", "spread_max_ms", "step_gap_max_ms"], out)
    print("  e3_finality.pdf, e3_finality_plot.csv")


def occupancy_time_figure(res, work, dataflows):
    cfg = "p4_D%s_d%s_s1_z0_k%s_thybrid_i50_u0_b1024"
    fig, axes = figure(dataflows, "Tier-1 occupancy [records]", "Processing time [s]")
    out = []
    shard_colors = common.SHARD_COLORS
    handles = []
    for ax, df in zip(axes, dataflows):
        handles = []
        for sink, style, lw in (("shards", "-", 2), ("global", "--", 3)):
            d = Path(work) / df / (cfg % (D_DEFAULT, D_DEFAULT, sink)) / "meas"
            for shard, f in enumerate(sorted(d.glob("tier1_steps_index_s*.csv"))):
                a = np.loadtxt(f, delimiter=",", skiprows=1, ndmin=2)
                if a.size == 0:
                    continue
                t = (a[:, 0] - a[0, 0]) / 1e6
                (h,) = ax.plot(t, a[:, 2], linestyle=style, linewidth=lw, color="black" if sink == "global" else shard_colors[shard % len(shard_colors)])
                if shard == 0 or sink == "shards":
                    handles.append(h)
                out.extend([df, sink, shard, "%.4f" % ti, int(b), int(c), int(w)] for ti, b, c, w in zip(t, a[:, 2], a[:, 3], a[:, 1]))
        common.style_axis(ax)
    if handles:
        common.shared_legend(fig, handles[:4] + handles[-1:], ["shard %d" % i for i in range(4)] + ["global sink"][: len(handles[:4] + handles[-1:]) - 4])
    common.save_pdf(fig, res / "e3_occupancy_time.pdf")
    write_csv(res / "e3_occupancy_time.csv", ["dataflow", "sink", "shard", "t_s", "tier1_before", "tier1_after", "watermark"], out)
    print("  e3_occupancy_time.pdf, e3_occupancy_time.csv")


def tier1_figure(res, runs, dataflows, metric, ylabel, name, sink="global", timeouts=()):
    intervals = ["10", "50", "250", "1000"]
    sinkp = "1" if sink == "global" else "4"
    fig, axes = figure(dataflows, ylabel, "Watermark interval [ms]")
    out = []
    handles = []
    for ax, df in zip(axes, dataflows):
        series = []
        for v in common.VARIANT_ORDER:
            vals = []
            for i in intervals:
                rr = runs_for(runs, df, lambda r: r["D"] == D_DEFAULT and r["delta"] == D_DEFAULT and r["interval"] == i and r["tier1"] == v
                              and r["p"] == "4" and r["sinkp"] == sinkp and r["slow"] == "1" and r["zipf"] == "0" and r["idle"] == "0")
                r = rr[0] if rr else {}
                vals.append(metric(r) if r else float("nan"))
                out.append([df, sink, v, i, vals[-1], r.get("peak_max_shard", "")])
            series.append((v, vals, None, common.VARIANT_COLORS[v], common.VARIANT_HATCHES[v], common.VARIANT_NAMES[v]))
        handles = grouped_bars(ax, intervals, series)
        log_axis(ax)
        # runs that did not finish within the limit (a sorted buffer fed out of order is quadratic): marked T/O
        lo, hi = ax.get_ylim()
        for vi, v in enumerate(common.VARIANT_ORDER):
            for xi, i in enumerate(intervals):
                cfg = "p4_D60000_d60000_s1_z0_k%s_t%s_i%s_u0_b1024" % (sink, v, i)
                if (df, cfg) in timeouts:
                    x = xi + (vi - (len(common.VARIANT_ORDER) - 1) / 2) * common.GROUP_BAR_WIDTH * 1.15
                    ax.text(x, (lo * hi) ** 0.5, "T/O", rotation=90, ha="center", va="center", fontsize=common.FONT["annotation"],
                            color=common.VARIANT_COLORS[v], fontweight="bold")
    common.shared_legend(fig, handles, [common.VARIANT_NAMES[v] for v in common.VARIANT_ORDER])
    common.save_pdf(fig, res / (name + ".pdf"))
    write_csv(res / (name + ".csv"), ["dataflow", "sink", "tier1", "interval_ms", "value", "peak_tier1_max_shard"], out)
    print("  %s.pdf, %s.csv" % (name, name))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results", type=Path, required=True)
    ap.add_argument("--work", type=Path, help="raw runs of run.sh (for the occupancy over time)")
    ap.add_argument("--paced", action="store_true", help="a paced run (arms D and delta, watermark interval 1 ms): only the late-record and wrong-answer figures")
    args = ap.parse_args()
    res = args.results
    if args.paced:
        BASE["interval"] = "1"
    common.setup_style()
    runs = load(res / "e3_runs.csv")
    timing = load(res / "e3_timing.csv") if (res / "e3_timing.csv").exists() else []
    dataflows = sorted({r["dataflow"] for r in runs})

    if timing and not args.paced:
        base_ok = lambda r: all(r[k] == v for k, v in BASE.items()) and r["sinkp"] == "4"
        overhead_figure(res, "e3_overhead_D", runs, timing, dataflows,
                        [(D_LABELS[d], (lambda d: lambda r: base_ok(r) and r["D"] == d and r["delta"] == d)(d)) for d in D_LABELS],
                        "Disorder $D$ (= $\\delta$)")
        overhead_figure(res, "e3_overhead_p", runs, timing, dataflows,
                        [(p, (lambda p: lambda r: r["p"] == p and r["sinkp"] == p and r["D"] == D_DEFAULT and r["delta"] == D_DEFAULT
                              and r["slow"] == "1" and r["zipf"] == "0" and r["tier1"] == BASE["tier1"] and r["interval"] == BASE["interval"] and r["idle"] == "0")(p))
                         for p in ("1", "2", "4", "8")], "Parallelism $p$")
        skew = lambda slow, zipf, sinkp: (lambda r: r["p"] == "4" and r["sinkp"] == sinkp and r["D"] == D_DEFAULT and r["delta"] == D_DEFAULT
                                          and r["slow"] == slow and r["zipf"] == zipf and r["tier1"] == BASE["tier1"] and r["interval"] == BASE["interval"]
                                          and r["idle"] == "0" and r["batch"] == "1024")
        overhead_figure(res, "e3_overhead_skew", runs, timing, dataflows,
                        [("none", skew("1", "0", "4")), ("slow x4", skew("4", "0", "4")), ("Zipf 1", skew("1", "1.0", "4")),
                         ("both", skew("4", "1.0", "4")), ("global sink", skew("1", "0", "1"))], "Skew")
    if any(int(r["delta"]) < int(r["D"]) for r in runs):   # only the paced arms have a delta below D
        late_figure(res, runs, dataflows)
        late_figure(res, runs, dataflows, "answers")
    if args.paced:
        sys.exit(0)
    occupancy_figure(res, runs, dataflows, "shards", "e3_occupancy")
    occupancy_figure(res, runs, dataflows, "global", "e3_occupancy_global")
    finality_figure(res, runs, dataflows)
    if args.work:
        occupancy_time_figure(res, args.work, dataflows)
    to = {(r["dataflow"], r["cfg"]) for r in load(res / "e3_timeouts.csv")} if (res / "e3_timeouts.csv").exists() else set()
    tier1_figure(res, runs, dataflows, lambda r: num(r["insert_ns_per_rec"]) + num(r["split_ns_per_rec"]), "Tier-1 insert + split [ns/record]",
                 "e3_tier1_cost", sink="global", timeouts=to)
    tier1_figure(res, runs, dataflows, lambda r: num(r["provisional_avg_us"]), "Provisional query [$\\mu$s]", "e3_tier1_query", sink="global", timeouts=to)
    tier1_figure(res, runs, dataflows, lambda r: num(r["insert_ns_per_rec"]) + num(r["split_ns_per_rec"]), "Tier-1 insert + split [ns/record]",
                 "e3_tier1_cost_shards", sink="shards", timeouts=to)
    tier1_figure(res, runs, dataflows, lambda r: num(r["provisional_avg_us"]), "Provisional query [$\\mu$s]", "e3_tier1_query_shards", sink="shards", timeouts=to)

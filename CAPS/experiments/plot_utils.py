"""Figures of the experiments, in the paper's style (experiments/common.py).

One figure per paper figure, one subplot per dataflow (alphabetical, one row), one legend for the whole figure,
methods in chronological order with ours last. Every PDF has a CSV of the plotted numbers next to it (same stem).
Error bars are the minimum and maximum over the repetitions of the run (REPS = 3), the bar or marker is the median.

  how_much       how_much_runtime_bar.pdf        runtime of GeneaLog, Green, CAPS; No Prov as a reference line  (Fig. 8)
                 how_much_memory_model_bar.pdf   retained meta-data in MB (analytical), log scale               (Table 2)
  queries        queries_query_latency_bar.pdf   temporal how-much and versioning latency, four methods, log    (Fig. 10)
  query_length   query_length_latency_lines.pdf  latency against the query window length, log-log               (Fig. 9b)
  paths          paths_cost_bar.pdf              per-source against per-path counters, relative cost            (Fig. 9c)

Usage: plot_utils.py <experiment directory or one of its dataflow subdirectories>, or the small/ subdirectory of an
experiment (the SMALL=1 results) or one of its dataflow subdirectories. Reads the experiment-wide CSVs of that
directory (the run.sh scripts rebuild them from the per-dataflow ones before plotting).
"""
import argparse
import math
import sys
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.ticker import LogLocator

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common  # noqa: E402

MB = 1e6
OUT = None   # --out: write the figures (and their CSVs) here instead of next to the input CSVs

# Figures of the paper (acmart sigconf: column 241 pt, text 506 pt). inserted: width of the figure in the paper; native: width of the PDF
# (pt, after bbox tight); ratio: height / width, fixed by the page budget (the figure of the submitted version); tick, legend, label:
# font sizes in the paper (pt); head, foot: inches above and below the axes at font scale 1 (legend, titles / tick and axis labels).
COLUMN, TEXT = 241.0, 506.0
PAPER = {
    "how_much_runtime_norm": dict(trim=-6, inserted=0.9 * COLUMN, native=912.0, ratio=282.0 / 880, tick=6.5, legend=6.0, label=6.5, head=0.85, foot=0.5, gap=0.35),
    "queries_howmuch_bar": dict(trim=-2, inserted=0.9 * COLUMN, native=912.0, ratio=244.0 / 918, tick=6.5, legend=6.0, label=6.0, head=0.5, foot=0.5, gap=0.08),
    "query_length_latency_lines": dict(xlabel=5.0, trim=12, inserted=0.32 * TEXT, native=792.0, ratio=341.0 / 802, tick=5.0, legend=5.5, label=6.5, title=7.0, head=0.88, foot=0.85, gap=0.45),
    "paths_cost_bar": dict(xtick=4.2, trim=-55, inserted=0.32 * TEXT, native=792.0, ratio=347.0 / 799, tick=5.5, legend=5.5, label=6.5, title=7.0, head=0.88, foot=1.05, gap=0.45),
}


def paper(name, **figure):
    """Fonts of the named paper figure (common.paper_fonts); returns the figure, its axes and the legend anchor."""
    sp = PAPER[name]
    common.paper_fonts(sp["inserted"], sp["native"], sp["tick"], sp["legend"], sp.get("label"), sp.get("title"))
    return common.new_paper_figure(ratio=sp["ratio"], native_pt=sp["native"], head=sp["head"], foot=sp["foot"], legend_gap=sp["gap"], trim=sp["trim"], **figure)


def load(path):
    path = Path(path)
    return common.read_csv(path) if path.is_file() else []


def present(rows):
    """The dataflows that have rows, alphabetical."""
    have = {r["dataflow"] for r in rows}
    return [d for d in common.DATAFLOWS if d in have]


def finish(fig, handles, labels, pdf, csv_header, csv_rows, anchor=common.LEGEND_ANCHOR, ncol=None, compact=False, tight=True):
    if OUT is not None:
        pdf = OUT / Path(pdf).name
    common.shared_legend(fig, handles, labels, anchor, ncol, compact)
    common.save_pdf(fig, pdf, tight)
    common.write_csv(Path(pdf).with_suffix(".csv"), csv_header, csv_rows)
    print("wrote %s and %s" % (pdf, Path(pdf).with_suffix(".csv")))


def err_bar(ax, x, y, lo, hi, width, method, top=None):
    """One bar of a method with its min-max whisker; a bar above `top` is capped there (and marked)."""
    h = y if top is None else min(y, top)
    common.bar(ax, x, h, width, common.METHOD_COLORS[method], common.METHOD_HATCHES[method])
    if hi > lo:
        ax.errorbar([x], [y], yerr=[[y - lo], [hi - y]], fmt="none", ecolor="black", elinewidth=1.5, capsize=4, zorder=4)


def method_handles(methods):
    return [common.legend_handle(common.METHOD_COLORS[m], common.METHOD_HATCHES[m]) for m in methods], \
        [common.METHOD_NAMES[m] for m in methods]


# how_much
def runtime_figure(expdir):
    stats = load(expdir / "times_stats.csv")
    if not stats:
        return
    dfs = present(stats)
    methods = ("genealog", "green", "caps")
    fig, axes = common.new_figure(len(dfs))
    out = []
    ref = None
    for ax, d in zip(axes, dfs):
        rows = {common.HOWMUCH_CSV_METHOD[r["method"]]: r for r in stats if r["dataflow"] == d and r["method"] in common.HOWMUCH_CSV_METHOD}
        top = 0.0
        for i, m in enumerate(methods):
            r = rows[m]
            med, lo, hi = (float(r["job_duration_" + k + "_s"]) for k in ("median", "min", "max"))
            err_bar(ax, i, med, lo, hi, common.BAR_WIDTH, m)
            top = max(top, hi)
            out.append([d, m, med, lo, hi, r["n"]])
        n = rows["noprov"]
        nm, nlo, nhi = (float(n["job_duration_" + k + "_s"]) for k in ("median", "min", "max"))
        ref = ax.axhline(nm, color=common.METHOD_COLORS["noprov"], linestyle="--", linewidth=2, zorder=5)
        ax.axhspan(nlo, nhi, color=common.METHOD_COLORS["noprov"], alpha=0.25, zorder=2)
        out.append([d, "noprov", nm, nlo, nhi, n["n"]])
        ax.set_ylim(0, max(top, nhi) * 1.15)
        ax.set_xticks([])
        ax.set_title(common.DATAFLOW_NAMES[d])
        common.style_axis(ax)
    axes[0].set_ylabel("Runtime [s]")
    handles, labels = method_handles(methods)
    finish(fig, [ref] + handles, ["No Prov"] + labels, expdir / "how_much_runtime_bar.pdf",
           ["dataflow", "method", "median_s", "min_s", "max_s", "reps"], out)


# The two single-column figures (runtime, temporal queries): one panel wider and shorter than the 5.5 x 5.5 in square,
# so that the axes fill the column; the grouped bars are wider than the 0.14 default (groups one unit apart, not touching).
WIDE_SIZE = (13.0, 4.2)      # inches; full column width, horizontal dataflow names (each group ~2 in wide fits 'nexmark_1' at 22 pt)
WIDE_SIZE_4 = (13.0, 4.2)    # same size for the temporal-query figure
WIDE_AXES_HEIGHT = 2.8      # inches; the axes of the single-column figures (the text-sized margins come on top)
WIDE_BAR_WIDTH = 0.24        # three bars per group (runtime)
WIDE_BAR_WIDTH_4 = 0.19      # four bars per group (temporal queries)
RUNTIME_TOP = 2.0   # y-limit of the normalized runtime axis; a bar above it is capped and labelled with its value


def runtime_norm_figure(expdir):
    """Single-column version of the runtime figure: one panel, runtime normalized to the median runtime of No Prov of the
    same dataflow, one group of bars (GeneaLog, Green, CAPS) per dataflow. Dashed line at 1 (No Prov); the whiskers of a bar are the fastest and slowest run of the method, divided by the
    same median. The absolute No Prov runtimes are in the CSV (noprov_median_s) and in the caption of the paper."""
    stats = load(expdir / "times_stats.csv")
    if not stats:
        return
    dfs = present(stats)
    methods = ("genealog", "green", "caps")
    fig, (ax,), anchor = paper("how_much_runtime_norm", ncols=1, width=WIDE_SIZE[0], left=0.12, right=0.99)
    out = []
    from matplotlib.lines import Line2D
    for g, d in enumerate(dfs):
        rows = {common.HOWMUCH_CSV_METHOD[r["method"]]: r for r in stats if r["dataflow"] == d and r["method"] in common.HOWMUCH_CSV_METHOD}
        n = rows["noprov"]
        nm, nlo, nhi = (float(n["job_duration_" + k + "_s"]) for k in ("median", "min", "max"))
        out.append([d, "noprov", nm, nlo, nhi, n["n"], nm, 1.0, nlo / nm, nhi / nm])
        for mi, m in enumerate(methods):
            r = rows[m]
            med, lo, hi = (float(r["job_duration_" + k + "_s"]) for k in ("median", "min", "max"))
            x = g + (mi - (len(methods) - 1) / 2) * WIDE_BAR_WIDTH
            y = med / nm
            common.bar(ax, x, min(y, RUNTIME_TOP), WIDE_BAR_WIDTH, common.METHOD_COLORS[m], common.METHOD_HATCHES[m])
            if y <= RUNTIME_TOP:
                ax.errorbar([x], [y], yerr=[[y - lo / nm], [hi / nm - y]], fmt="none", zorder=4, **common.WHISKER)
            else:
                ax.text(x, RUNTIME_TOP * 1.02, "%.1f" % y, ha="center", va="bottom", fontsize=common.FONT["annotation"])
            out.append([d, m, med, lo, hi, r["n"], nm, y, lo / nm, hi / nm])
    ax.axhline(1.0, color="black", linestyle="--", linewidth=3, zorder=5)   # black: the gray of No Prov disappears over the bars
    ax.set_xlim(-0.6, len(dfs) - 0.4)
    ax.set_ylim(0, RUNTIME_TOP)
    ax.set_xticks(range(len(dfs)))
    ax.set_xticklabels([common.DATAFLOW_NAMES[d] for d in dfs])
    ax.set_ylabel("Runtime /\nNo Prov")
    common.style_axis(ax)
    handles, labels = method_handles(methods)
    noprov = Line2D([0], [0], color="black", linestyle="--", linewidth=3)
    finish(fig, [noprov] + handles, ["No Prov"] + labels, expdir / "how_much_runtime_norm.pdf",
           ["dataflow", "method", "median_s", "min_s", "max_s", "reps", "noprov_median_s", "ratio_median", "ratio_min", "ratio_max"],
           out, anchor=anchor, compact=True)


def memory_figure(expdir):
    rows = load(expdir / "memory_model.csv")
    if not rows:
        return
    dfs = present(rows)
    methods = ("genealog", "green", "caps")
    fig, axes = common.new_figure(len(dfs))
    out = []
    for ax, d in zip(axes, dfs):
        vals = {common.HOWMUCH_CSV_METHOD[r["method"]]: float(r["bytes"]) / MB for r in rows if r["dataflow"] == d}
        lowest, highest = min(vals[m] for m in methods), max(vals[m] for m in methods)
        top = 10.0 ** math.ceil(math.log10(highest * 1.6))   # head room for the value label
        ax.set_yscale("log")
        ax.set_ylim(10.0 ** (math.floor(math.log10(lowest)) - 1), top)
        for i, m in enumerate(methods):
            err_bar(ax, i, vals[m], vals[m], vals[m], common.BAR_WIDTH, m, top=top)
            ax.text(i, min(vals[m], top) * 1.15, ("%.0f" % vals[m]) if vals[m] >= 10 else "%.1f" % vals[m],
                    ha="center", va="bottom", fontsize=common.FONT["annotation"])
            out.append([d, m, vals[m]])
        ax.set_xticks([])
        ax.set_title(common.DATAFLOW_NAMES[d])
        common.style_axis(ax, log_y=True)
    axes[0].set_ylabel("Retained meta-data [MB]")
    handles, labels = method_handles(methods)
    finish(fig, handles, labels, expdir / "how_much_memory_model_bar.pdf", ["dataflow", "method", "retained_MB"], out)


# queries
QUERY_NAMES = {"howmuch": "How-much", "versioning": "Versioning"}


def queries_figure(expdir):
    rows = load(expdir / "query_latency.csv")
    if not rows:
        return
    dfs = present(rows)
    methods = [m for m in common.QUERY_METHOD_ORDER if m in {common.QUERY_CSV_METHOD[r["method"]] for r in rows}]
    queries = ("howmuch", "versioning")
    top = 10 ** len(str(int(max(float(r["avg_us_max"]) for r in rows))))
    fig, axes = common.new_figure(len(dfs))
    out = []
    width = common.QUERIES_BAR_WIDTH   # the bars of one group touch; the groups are one unit apart (4 x 0.2: a gap of 0.2)
    for ax, d in zip(axes, dfs):
        ax.set_yscale("log")
        ax.set_ylim(0.1, top)
        for qi, q in enumerate(queries):
            for mi, m in enumerate(methods):
                r = next(x for x in rows if x["dataflow"] == d and x["query"] == q and common.QUERY_CSV_METHOD[x["method"]] == m)
                med, lo, hi = float(r["avg_us"]), float(r["avg_us_min"]), float(r["avg_us_max"])
                x = qi + (mi - (len(methods) - 1) / 2) * width
                err_bar(ax, x, med, lo, hi, width, m, top=top)
                out.append([d, q, m, med, lo, hi, r["reps"]])
        ax.set_xticks(range(len(queries)))
        ax.set_xticklabels([QUERY_NAMES[q] for q in queries])
        ax.set_title(common.DATAFLOW_NAMES[d])
        common.style_axis(ax, log_y=True)
    axes[0].set_ylabel("Latency [$\\mu$s]")
    handles, labels = method_handles(methods)
    finish(fig, handles, labels, expdir / "queries_query_latency_bar.pdf",
           ["dataflow", "query", "method", "median_us", "min_us", "max_us", "reps"], out)


def queries_howmuch_figure(expdir):
    """Single-column temporal-query figure: how-much queries only (a versioning query is two how-much queries), one group of
    bars per dataflow, one bar per method, log axis. Bars: median over the repetitions, whiskers min-max."""
    rows = [r for r in load(expdir / "query_latency.csv") if r["query"] == "howmuch"]
    if not rows:
        return
    dfs = present(rows)
    methods = [m for m in common.QUERY_METHOD_ORDER if m in {common.QUERY_CSV_METHOD[r["method"]] for r in rows}]
    top = 10 ** len(str(int(max(float(r["avg_us_max"]) for r in rows))))
    fig, (ax,), anchor = paper("queries_howmuch_bar", ncols=1, width=WIDE_SIZE_4[0], left=0.12, right=0.99)
    out = []
    ax.set_yscale("log")
    ax.set_ylim(0.01, top)
    for g, d in enumerate(dfs):
        for mi, m in enumerate(methods):
            r = next(x for x in rows if x["dataflow"] == d and common.QUERY_CSV_METHOD[x["method"]] == m)
            med, lo, hi = float(r["avg_us"]), float(r["avg_us_min"]), float(r["avg_us_max"])
            x = g + (mi - (len(methods) - 1) / 2) * WIDE_BAR_WIDTH_4
            err_bar(ax, x, med, lo, hi, WIDE_BAR_WIDTH_4, m, top=top)
            out.append([d, "howmuch", m, med, lo, hi, r["reps"]])
    ax.set_xlim(-0.6, len(dfs) - 0.4)
    ax.set_xticks(range(len(dfs)))
    ax.set_xticklabels([common.DATAFLOW_NAMES[d] for d in dfs])
    ax.set_ylabel("Latency [$\\mu$s]")
    common.style_axis(ax, log_y=True)
    ax.yaxis.set_major_locator(LogLocator(base=100, numticks=12))   # labels every other decade: the axis is short
    ax.yaxis.set_minor_locator(LogLocator(base=10, subs=range(1, 10), numticks=12))
    handles, labels = method_handles(methods)
    finish(fig, handles, labels, expdir / "queries_howmuch_bar.pdf",
           ["dataflow", "query", "method", "median_us", "min_us", "max_us", "reps"], out,
           anchor=anchor, compact=True)


def query_length_figure(expdir):
    rows = load(expdir / "query_length_latency.csv")
    if not rows:
        return
    dfs = [d for d in present(rows) if d in common.QUERY_LENGTH_DATAFLOWS]
    if not dfs:
        return
    methods = [m for m in common.QUERY_METHOD_ORDER if m in {common.QUERY_CSV_METHOD[r["method"]] for r in rows}]
    sp = PAPER["query_length_latency_lines"]
    common.paper_fonts(sp["inserted"], sp["native"], sp["tick"], sp["legend"], sp.get("label"), sp.get("title"))
    fig, axes, anchor = common.new_fixed_figure(len(dfs), **{k: v for k, v in common.FIG9.items()})
    out = []
    handles = {}
    for ax, d in zip(axes, dfs):
        mine = [r for r in rows if r["dataflow"] == d]
        base = min(int(r["len_ms"]) for r in mine)   # the lengths are 1, 10, 100, 1000 slides
        for m in methods:
            pts = sorted((int(r["len_ms"]) // base, float(r["avg_us"]), float(r["avg_us_min"]), float(r["avg_us_max"]))
                         for r in mine if common.QUERY_CSV_METHOD[r["method"]] == m)
            x = [p[0] for p in pts]
            y = [p[1] for p in pts]
            handles[m] = ax.errorbar(x, y, yerr=[[p[1] - p[2] for p in pts], [p[3] - p[1] for p in pts]],
                                     color=common.METHOD_COLORS[m], marker=common.METHOD_MARKERS[m], markersize=9,
                                     linewidth=2, markeredgecolor="black", markeredgewidth=0.5, capsize=4)
            out += [[d, m, p[0], p[1], p[2], p[3]] for p in pts]
        ax.set_xscale("log")
        ax.set_xticks(x)
        ax.set_xticklabels([str(v) for v in x])
        ax.xaxis.set_minor_locator(plt.NullLocator())
        ax.set_title(common.DATAFLOW_NAMES[d])
        common.style_axis(ax, log_y=True)
        ax.set_xlabel("Query window [slides]", fontsize=PAPER["query_length_latency_lines"]["xlabel"] * sp["native"] / sp["inserted"])
    axes[0].set_ylabel("Latency [$\\mu$s]")
    finish(fig, [handles[m] for m in methods], [common.METHOD_NAMES[m] for m in methods],
           expdir / "query_length_latency_lines.pdf",
           ["dataflow", "method", "slides", "median_us", "min_us", "max_us"], out, anchor, compact=True, tight=False)


# paths
PATHS_TOP = 2.2   # y-limit of the relative-cost axis
PATHS_BAR_WIDTH = 0.25   # two bars per group; wider than the 0.14 grouped default
PATH_METRICS = (("job_duration_s", "Runtime"), ("insert_avg_ns", "Insert"), ("howmuch_avg_us", "Query"),
                ("index_bytes", "Memory"))


def paths_figure(expdir):
    rows = load(expdir / "paths.csv")
    if not rows:
        return
    dfs = present(rows)
    sp = PAPER["paths_cost_bar"]
    s9 = sp["inserted"] / sp["native"]
    common.paper_fonts(sp["inserted"], sp["native"], sp["tick"], sp["legend"], sp.get("label"), sp.get("title"))
    fig, axes, anchor = common.new_fixed_figure(len(dfs), **common.FIG9)
    out = []
    from matplotlib.patches import Patch
    for ax, d in zip(axes, dfs):
        by = {r["granularity"]: r for r in rows if r["dataflow"] == d}
        src = by["source"]
        for mi, (col, name) in enumerate(PATH_METRICS):
            for gi, g in enumerate(common.GRANULARITY_ORDER):
                r = by[g]
                med, lo, hi = (float(r[col + s]) for s in ("", "_min", "_max"))
                base = float(src[col])
                # relative to the median of the source-level run; the whisker is the interval of the repetitions;
                # a bar above the axis is capped at the y-limit
                x = mi + (gi - 0.5) * PATHS_BAR_WIDTH
                common.bar(ax, x, min(med / base, PATHS_TOP), PATHS_BAR_WIDTH, common.GRANULARITY_COLORS[g],
                           common.GRANULARITY_HATCHES[g])
                ax.errorbar([x], [med / base], yerr=[[(med - lo) / base], [(hi - med) / base]], fmt="none", zorder=4,
                            **common.WHISKER)
                out.append([d, name, g, r["channels"], med / base, lo / base, hi / base])
        ax.axhline(1.0, color="#888888", linestyle="--", linewidth=1.5, zorder=2)
        ax.set_xticks(range(len(PATH_METRICS)))
        ax.set_xticklabels([("\n" if i % 2 else "") + n for i, (_, n) in enumerate(PATH_METRICS)], fontsize=PAPER["paths_cost_bar"]["xtick"] * PAPER["paths_cost_bar"]["native"] / PAPER["paths_cost_bar"]["inserted"])
        ax.set_xlim(-0.6, len(PATH_METRICS) - 0.4)   # the names alternate between two rows: neighbours do not touch
        ax.tick_params(axis="x", pad=2 / s9)
        ax.set_ylim(0, PATHS_TOP)
        ax.set_title(common.DATAFLOW_NAMES[d])
        common.style_axis(ax)
    axes[0].set_ylabel("Cost relative\nto per source")   # two lines: the panel is short
    handles = [common.legend_handle(common.GRANULARITY_COLORS[g], common.GRANULARITY_HATCHES[g]) for g in common.GRANULARITY_ORDER]
    finish(fig, handles, [common.GRANULARITY_NAMES[g] for g in common.GRANULARITY_ORDER],
           expdir / "paths_cost_bar.pdf", ["dataflow", "metric", "granularity", "channels", "median_rel", "min_rel", "max_rel"],
           out, anchor, compact=True, tight=False)


GENERATORS = {
    "how_much": (runtime_figure, runtime_norm_figure, memory_figure),
    "queries": (queries_figure, queries_howmuch_figure),
    "query_length": (query_length_figure,),
    "paths": (paths_figure,),
}

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("dir", type=Path)
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()
    target = args.dir.resolve()
    if args.out is not None:
        OUT = args.out.resolve()
        OUT.mkdir(parents=True, exist_ok=True)
    if target.name in common.DATAFLOWS:
        target = target.parent
    experiment = target.parent.name if target.name == "small" else target.name   # <experiment>/small: SMALL=1 results
    common.setup_style()
    if experiment not in GENERATORS:
        raise SystemExit("no figures for experiment %s (figures: %s)" % (experiment, ", ".join(GENERATORS)))
    for generate in GENERATORS[experiment]:
        common.setup_style()   # back to the default sizes: only the four paper figures use paper_fonts
        generate(target)

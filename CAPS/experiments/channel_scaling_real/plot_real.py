#!/usr/bin/env python3
"""Figures of the real-data channel-scalability experiment, from the CSVs of summarize_real.py.

Lines against M (log2 x axis), the style of experiments/common.py; datasets in alphabetical order (taxi, tweets).
  channel_scaling_real.pdf              one row, four panels: Taxi retained | Taxi query | Tweets retained | Tweets query
                                        (retained meta-data in MB, analytical; per-output query = total contributions, JMH ns)
  channel_scaling_real_runtime.pdf      Flink job runtime, median with min/max bars, one panel per dataset
  channel_scaling_real_query_vector.pdf per-output query of the M channel counters (Green, CAPS dense, CAPS sparse)
  channel_scaling_real_grid.pdf         the figure of the paper (Sec. 7.5), m = 2, 8, 32, 128, 512: two rows, memory | latency: per taxi
                                        (taxi: key = taxi id, channel = zone mod M) on top, per zone (taxi_zone: key = zone, channel = taxi id mod M) below
Every figure ships with its plotted series as CSV next to it (<figure>.csv).

Usage: plot_real.py --res RESULTS_DIR [--only grid]   (--only grid: just the figure of the paper, leaving the others as they are)
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import common  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402

NAMES = dict(common.METHOD_NAMES, caps="CAPS (dense)", caps_sparse="CAPS (sparse)")
ORDER = ("noprov", "genealog", "green", "caps", "caps_sparse")
TITLES = {"taxi": "Taxi", "tweets": "Tweets"}
LEGEND_Y = 0.82
GRID_ROWS = (("taxi", "Per taxi"), ("taxi_zone", "Per zone"))   # (dataset, title prefix)
PAPER = (0.7 * 241.0, 816.0)   # grid figure: inserted width in the paper, native width of the PDF (pt)
PAPER_RATIO, TICK, LEGEND = 790.0 / 846, 6.0, 5.5   # height / width fixed by the page budget; font sizes in the paper (pt)
HEAD, FOOT, ROW_GAP = 0.95, 0.85, 1.3                # inches (font scale 1): above the panels (legend, title), below (ticks, label), between rows
GRID_MS = (2, 8, 32, 128, 512)   # the m of the paper's figure


def line(ax, rows, method, xcol, ycol, err=None):
    pts = sorted((int(r["M"]), float(r[ycol]), r) for r in rows if r["method"] == method)
    if not pts:
        return None, []
    x, y = [p[0] for p in pts], [p[1] for p in pts]
    kw = dict(color=common.METHOD_COLORS[method], marker=common.METHOD_MARKERS[method], markersize=9, linewidth=2,
              markeredgecolor="black", markeredgewidth=0.5)
    if err:
        lo = [yy - float(p[2][err[0]]) for yy, p in zip(y, pts)]
        hi = [float(p[2][err[1]]) - yy for yy, p in zip(y, pts)]
        h = ax.errorbar(x, y, yerr=[lo, hi], capsize=4, **kw)
    else:
        (h,) = ax.plot(x, y, **kw)
    return h, [(m, v) for m, v, _ in pts]


def figure(res, name, panels, methods, ms, log_y_default=True):
    """panels: list of (title, ylabel, rows, ycol, log_y, err)."""
    fig, axes = common.new_figure(len(panels))
    handles, out = {}, []
    for ax, (title, ylabel, rows, ycol, log_y, err) in zip(axes, panels):
        for method in methods:
            h, pts = line(ax, rows, method, "M", ycol, err)
            if h is not None:
                handles[method] = h
                out += [[title, m, method, v] for m, v in pts]
        ax.set_title(title, fontsize=common.FONT["title"])
        ax.set_xlabel("Channels $m$")
        ax.set_ylabel(ylabel)
        common.style_axis(ax, ticks=ms, log_y=log_y, log2_x=True)
    present = [m for m in methods if m in handles]
    fig.legend([handles[m] for m in present], [NAMES[m] for m in present], loc="lower center",
               bbox_to_anchor=(0.5, LEGEND_Y), frameon=False, ncol=len(present), handlelength=1.6,
               columnspacing=1.2, handletextpad=0.5)   # closer to the panels than common.LEGEND_ANCHOR
    common.save_pdf(fig, res / (name + ".pdf"))
    common.write_csv(res / (name + ".csv"), ["panel", "M", "method", "value"], out)
    print("  %s.pdf, %s.csv" % (name, name))


def figure_grid(res, name, memory, query, methods, ms):
    """Two rows (GRID_ROWS) x two columns (retained meta-data, per-output total query), same style as figure().
    The legend sits LEGEND_Y - LAYOUT["top"] of a panel height (single-row figure) above the top row of panels."""
    rows = len(GRID_ROWS)
    k = common.SCALE   # margins and gaps are text-sized: they grow with the font scale
    W, H = 2 * common.SUBPLOT_SIZE, PAPER_RATIO * PAPER[1] / 72   # the height is fixed by the page budget; the panels take what the text leaves
    head, foot, gap_in = HEAD * k, FOOT * k, ROW_GAP * k
    axh = (H - head - foot - (rows - 1) * gap_in) / rows
    fig, axes = plt.subplots(rows, 2, figsize=(W, H), squeeze=False)
    m = common.scaled_margins(W, H, common.LAYOUT["left"], common.LAYOUT["right"], 0.0, 1.0, wspace=0.42, ncols=2)
    top = 1 - head / H
    fig.subplots_adjust(left=m["left"], right=m["right"], wspace=m["wspace"], top=top, bottom=foot / H, hspace=gap_in / axh)
    handles, out = {}, []
    for r, (ds, title) in enumerate(GRID_ROWS):
        for c, (metric, ylabel, data, ycol) in enumerate((
                ("retained meta-data", "Memory [MB]", [x for x in memory if x["dataset"] == ds], "retained_MB"),
                ("per-output query", "Latency [ns]", [x for x in query if x["dataset"] == ds and x["kind"] == "total"], "score_ns"))):
            ax = axes[r][c]
            for method in methods:
                h, pts = line(ax, data, method, "M", ycol)
                if h is not None:
                    handles[method] = h
                    out += [[title, metric, m, method, v] for m, v in pts]
            ax.set_title(title, fontsize=common.FONT["title"])   # the metric is in the y label
            ax.set_xlabel("Channels $m$")
            ax.set_ylabel(ylabel)
            common.style_axis(ax, ticks=ms[::2], log_y=True, log2_x=True)   # every other m: the labels of all five touch
    present = [m for m in methods if m in handles]
    fig.legend([handles[m] for m in present], [NAMES[m] for m in present], loc="lower center",
               bbox_to_anchor=(0.5, top + 0.40 * k / H),
               frameon=False, ncol=len(present), handlelength=1.1, columnspacing=0.8, handletextpad=0.35)
    common.save_pdf(fig, res / (name + ".pdf"))
    common.write_csv(res / (name + ".csv"), ["panel", "metric", "M", "method", "value"], out)
    print("  %s.pdf, %s.csv" % (name, name))


# The row of the paper (Sec. 7.5): a legend of 0.7 column width, then four panels of 0.24 column width each, every one its own PDF of
# the same size (saved without bbox tight, the margins are fixed in paper pt), so that they have the same height in the paper.
COLUMN = 241.0
ROW_PANEL = dict(inserted=0.24 * COLUMN, native=288.0, ratio=0.94, tick=5.0, xtick=4.0, label=5.0, title=5.5,   # sizes in paper pt
                 margins=(19.0, 7.0, 11.0, 15.0))   # left, right, top, bottom of a panel in paper pt
ROW_LEGEND = dict(inserted=0.7 * COLUMN, native=1000.0, font=5.5)   # the legend fills its width: the paper font follows from the spacing
ROW_PANELS = (("channel_pertaxi_mem", "taxi", "Per taxi: memory", "Memory [MB]", "mem"),
              ("channel_pertaxi_lat", "taxi", "Per taxi: latency", "Latency [ns]", "lat"),
              ("channel_perzone_mem", "taxi_zone", "Per zone: memory", "Memory [MB]", "mem"),
              ("channel_perzone_lat", "taxi_zone", "Per zone: latency", "Latency [ns]", "lat"))


def figure_row(res, memory, query, methods):
    """channel_legend.pdf and the four panels of ROW_PANELS (all m of the data)."""
    sp = ROW_PANEL
    s = sp["inserted"] / sp["native"]   # paper pt per native pt
    common.paper_fonts(sp["inserted"], sp["native"], sp["tick"], label_pt=sp["label"], title_pt=sp["title"])
    W, H = sp["native"] / 72, sp["native"] * sp["ratio"] / 72   # inches
    ml, mr, mt, mb = (v / s / 72 for v in sp["margins"])
    ms = sorted({int(r["M"]) for r in memory})
    handles, out = {}, []
    for name, ds, title, ylabel, kind in ROW_PANELS:
        fig, ax = plt.subplots(1, 1, figsize=(W, H))
        fig.subplots_adjust(left=ml / W, right=1 - mr / W, top=1 - mt / H, bottom=mb / H)
        data = [x for x in memory if x["dataset"] == ds] if kind == "mem" else [x for x in query if x["dataset"] == ds and x["kind"] == "total"]
        for method in methods:
            h, pts = line(ax, data, method, "M", "retained_MB" if kind == "mem" else "score_ns")
            h.set_markersize(11)   # native pt: 2 pt in the paper
            h.set_linewidth(2.5)
            handles[method] = h
            out += [[name, m, method, v] for m, v in pts]
        fig.text(0.5, 1 - 1.0 / s / 72 / H, title, ha="center", va="top", fontsize=common.FONT["title"])   # centered on the panel, wider than its axes
        ax.set_xlabel("Channels $m$", labelpad=1 / s)
        ax.set_ylabel(ylabel, labelpad=1 / s)
        common.style_axis(ax, ticks=[2, 8, 32, 128, 512], log_y=True, log2_x=True)
        ax.tick_params(pad=1 / s, length=1.2 / s)
        ax.tick_params(axis="x", labelsize=sp["xtick"] / s)   # all five m, horizontal: a smaller font
        ax.margins(x=0.0)
        ax.get_xticklabels()[-1].set_ha("left")   # 512 starts at its tick (the right edge): it does not touch 128
        for h in ax.get_lines():
            h.set_clip_on(False)   # markers at the edges stay whole
        common.save_pdf(fig, res / (name + ".pdf"), tight=False)
    common.write_csv(res / "channel_row.csv", ["panel", "M", "method", "value"], out)
    sp = ROW_LEGEND
    common.setup_style()
    fig = plt.figure(figsize=(sp["native"] / 72, 0.5))
    plt.rcParams["legend.fontsize"] = sp["font"] * 4
    common.shared_legend(fig, [handles[m] for m in methods], [NAMES[m] for m in methods], anchor=(0.5, 0.5), compact=True)
    common.save_pdf(fig, res / "channel_legend.pdf", pad=0.02)
    print("  channel_legend.pdf and the four channel_* panels")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--res", type=Path, required=True)
    ap.add_argument("--only", choices=["grid", "row"], default=None)
    ap.add_argument("--out", type=Path, default=None, help="write the figures here instead of into --res")
    args = ap.parse_args()
    res = args.out or args.res
    if args.out:
        args.out.mkdir(parents=True, exist_ok=True)
    common.setup_style()
    runtime = common.read_csv(args.res / "runtime.csv")
    memory = common.read_csv(args.res / "memory.csv")
    query = common.read_csv(args.res / "query.csv")
    datasets = [d for d in sorted({r["dataset"] for r in memory}) if d in TITLES]   # the original figures: taxi (and tweets)
    ms = sorted({int(r["M"]) for r in memory})

    def of(rows, ds, **where):
        return [r for r in rows if r["dataset"] == ds and all(r.get(k) == v for k, v in where.items())]

    def grid():
        common.paper_fonts(PAPER[0], PAPER[1], TICK, LEGEND)
        figure_grid(res, "channel_scaling_real_grid", [r for r in memory if int(r["M"]) in GRID_MS], [r for r in query if int(r["M"]) in GRID_MS],
                    ("genealog", "green", "caps", "caps_sparse"), list(GRID_MS))

    if args.only == "row":
        figure_row(res, [r for r in memory if r["dataset"] in ("taxi", "taxi_zone") and int(r["M"]) in GRID_MS], [r for r in query if int(r["M"]) in GRID_MS], ("genealog", "green", "caps", "caps_sparse"))
        raise SystemExit(0)
    if args.only == "grid":
        grid()
        raise SystemExit(0)
    main = []
    for ds in datasets:
        main.append((TITLES[ds] + ": retained meta-data", "Memory [MB]", of(memory, ds), "retained_MB", True, None))
        main.append((TITLES[ds] + ": per-output query", "Latency [ns]", of(query, ds, kind="total"), "score_ns", True, None))
    methods = ("genealog", "green", "caps", "caps_sparse")
    figure(res, "channel_scaling_real", main, methods, ms)
    figure(res, "channel_scaling_real_runtime",
           [(TITLES[ds], "Job duration [s]", of(runtime, ds), "median_s", False, ("min_s", "max_s")) for ds in datasets],
           ORDER, ms)
    figure(res, "channel_scaling_real_query_vector",
           [(TITLES[ds] + ": all channels", "Latency [ns]", of(query, ds, kind="vector"), "score_ns", True, None) for ds in datasets],
           ("green", "caps", "caps_sparse"), ms)
    if {ds for ds, _ in GRID_ROWS} <= {r["dataset"] for r in memory}:
        grid()

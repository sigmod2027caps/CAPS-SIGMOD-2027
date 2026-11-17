"""Shared plotting style and helpers of the experiments.

Every method has one color, hatch, marker and display name, the same in every plot. Methods appear in
chronological order (oldest competitor first), ours last. Import with
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))   # experiments/
    import common
"""
import csv
import os
import statistics
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FixedLocator, LogLocator, NullFormatter

# Chronological order, ours last. noprov is the baseline without provenance.
METHOD_ORDER = ("noprov", "genealog", "green", "caps")
METHOD_NAMES = {"noprov": "No Prov", "genealog": "GeneaLog", "green": "Green", "caps": "CAPS"}
METHOD_COLORS = {"noprov": "#555555", "genealog": "#DD8452", "green": "#55A868", "caps": "#4C72B0"}
# Any hatch except the star.
METHOD_HATCHES = {"noprov": "//", "genealog": "\\\\", "green": "xx", "caps": ".."}
METHOD_MARKERS = {"noprov": "x", "genealog": "s", "green": "^", "caps": "o"}

# CAPS without its index (the plain array of outputs, scanned per query): a variant of ours, so it sits right
# before CAPS (the index) in the temporal-query figures. Added for WP4 (experiments/queries, query_length).
METHOD_NAMES["caps_scan"] = "CAPS (no index)"
METHOD_COLORS["caps_scan"] = "#A1C4E8"
METHOD_HATCHES["caps_scan"] = "oo"
METHOD_MARKERS["caps_scan"] = "v"
# CAPS with the sparse annotation (SparseHowMuch, experiments/prefix_compression): a variant of ours, right of the dense CAPS.
METHOD_NAMES["caps_sparse"] = "CAPS (sparse)"
METHOD_COLORS["caps_sparse"] = "#8CCBF2"
METHOD_HATCHES["caps_sparse"] = "++"
METHOD_MARKERS["caps_sparse"] = "D"
QUERY_METHOD_ORDER = ("genealog", "green", "caps_scan", "caps")
# method column of the temporal-query CSVs (array / prefix are the names the CAPS sink writes) -> method key
QUERY_CSV_METHOD = {"array": "caps_scan", "prefix": "caps", "green": "green", "genealog": "genealog"}
# method column of the how_much CSVs -> method key
HOWMUCH_CSV_METHOD = {"noprov": "noprov", "genealog": "genealog", "green_polynomial": "green", "caps": "caps"}

# The six dataflows, in the alphabetical order every figure and table uses.
DATAFLOWS = ("nexmark_1", "nexmark_2", "taxi_1", "taxi_2", "twitter_1", "twitter_2")
# a run on some of the dataflows (DATAFLOWS="twitter_1 twitter_2" ./run.sh) tabulates and plots only those
if os.environ.get("DATAFLOWS"):
    DATAFLOWS = tuple(d for d in DATAFLOWS if d in os.environ["DATAFLOWS"].split())

# Display names of the dataflows in the plots (the CSV keys stay as they are).
DATAFLOW_NAMES = {"nexmark_1": "NX1", "nexmark_2": "NX2", "taxi_1": "TX1", "taxi_2": "TX2", "twitter_1": "TW1", "twitter_2": "TW2"}

# Counter granularity of experiments/paths: one counter per source or one per processing path (both are CAPS).
GRANULARITY_ORDER = ("source", "path")
GRANULARITY_NAMES = {"source": "Per source", "path": "Per path"}
GRANULARITY_COLORS = {"source": "#CCB974", "path": "#4C72B0"}
GRANULARITY_HATCHES = {"source": "--", "path": ".."}

# Variants of one structure compared in situ (experiments/disorder: Tier 1 of the two-tier index). The simple
# structure first, the one we use last; fixed like the methods above.
VARIANT_ORDER = ("buffer", "tree", "hybrid")
VARIANT_NAMES = {"buffer": "Sorted buffer", "tree": "RB-tree", "hybrid": "Run + RB-tree"}
VARIANT_COLORS = {"buffer": "#8172B3", "tree": "#64B5CD", "hybrid": "#CCB974"}
VARIANT_HATCHES = {"buffer": "++", "tree": "--", "hybrid": "xx"}

# Arms of experiments/index_overhead (all CAPS, except the reference): capture only, then capture plus the online two-tier index
# with the hybrid Tier 1 (ordered run + red-black tree, the default), with the red-black tree alone, and with the hybrid plus the
# per-insert nanoTime instrumentation (off in every timed run since the review fix).
OVERHEAD_ORDER = ("capture", "tree", "hybrid_timed", "hybrid")
OVERHEAD_NAMES = {"capture": "Capture only", "tree": "+ index (tree)", "hybrid_timed": "+ index (run + tree, timers)",
                  "hybrid": "+ index (run + tree)"}
OVERHEAD_COLORS = {"capture": "#4C72B0", "tree": "#64B5CD", "hybrid_timed": "#DA8BC3", "hybrid": "#CCB974"}
OVERHEAD_HATCHES = {"capture": "..", "tree": "--", "hybrid_timed": "++", "hybrid": "xx"}

# Runs of experiments/recovery (all CAPS): without checkpointing, with it, with an injected failure, with a killed TaskManager.
# No color of a method is reused here: these are variants of one method, not methods.
RUN_ORDER = ("plain", "ckpt", "inject", "kill")
RUN_NAMES = {"plain": "No checkpointing", "ckpt": "Checkpointing", "inject": "Injected failure", "kill": "TaskManager killed"}
RUN_COLORS = {"plain": "#E5C494", "ckpt": "#66C2A5", "inject": "#C44E52", "kill": "#8172B3"}
RUN_HATCHES = {"plain": "||", "ckpt": "--", "inject": "++", "kill": "\\\\"}

# Disorder D of experiments/disorder (late records against delta, one series per D) and the three stages of the
# delay to finality; again no method colors. Shards of the sink (occupancy over time), one color each.
DISORDER_ORDER = ("10000", "60000", "300000", "1800000")
DISORDER_NAMES = {"10000": "D = 10 s", "60000": "D = 60 s", "300000": "D = 5 min", "1800000": "D = 30 min"}
DISORDER_COLORS = {"10000": "#CCB974", "60000": "#937860", "300000": "#DA8BC3", "1800000": "#8C8C8C"}
DISORDER_HATCHES = {"10000": "..", "60000": "--", "300000": "++", "1800000": "xx"}
STAGE_ORDER = ("local", "global", "ready")
STAGE_NAMES = {"local": "sink: arrival to migration", "global": "global: all shards", "ready": "after the event time was ingested"}
STAGE_COLORS = {"local": "#66C2A5", "global": "#8DA0CB", "ready": "#FFD92F"}
STAGE_HATCHES = {"local": "..", "global": "xx", "ready": "\\\\"}
SHARD_COLORS = ("#66C2A5", "#8DA0CB", "#FFD92F", "#E78AC3")

# Lighter look: every fill color is mixed with white, every hatch has one line per tile
# instead of two (sparser) and is drawn thin in a darker shade of the fill, while the bar outline stays black, 0.5 pt.
# Done once here, so every method / variant keeps one color and one hatch in every plot. Never the star hatch.
BRIGHTEN = 0.30             # share of white mixed into a fill color
HATCH_LINEWIDTH = 0.6
HATCH_DARKEN = 0.50         # share of black mixed into the fill color to get the hatch color


def _mix(color, other, share):
    c = matplotlib.colors.to_rgb(color)
    o = matplotlib.colors.to_rgb(other)
    return matplotlib.colors.to_hex(tuple(a * (1 - share) + b * share for a, b in zip(c, o)))


_SPARSE = {"//": "/", "\\\\": "\\", "xx": "x", "..": ".", "oo": "o", "++": "+", "--": "-", "||": "|"}
for _name, _value in list(globals().items()):
    if _name.endswith("_COLORS") and isinstance(_value, dict):
        for _k in _value:
            _value[_k] = _mix(_value[_k], "white", BRIGHTEN)
    elif _name == "SHARD_COLORS":
        SHARD_COLORS = tuple(_mix(c, "white", BRIGHTEN) for c in _value)
    elif _name.endswith("_HATCHES") and isinstance(_value, dict):
        for _k in _value:
            _value[_k] = _SPARSE.get(_value[_k], _value[_k])
            assert "*" not in _value[_k]


def hatch_color(fill):
    return _mix(fill, "black", HATCH_DARKEN)


def bar(ax, x, height, width, color, hatch, zorder=3, **kw):
    """A bar in the paper's style: fill and thin hatch (darker shade of the fill) without outline, plus the black 0.5 pt
    outline drawn separately (matplotlib ties the hatch color to the edge color). kw (yerr, ...) go to the fill bar."""
    container = ax.bar(x, height, width, color=color, edgecolor=hatch_color(color), linewidth=0, hatch=hatch, zorder=zorder, **kw)
    ax.bar(x, height, width, fill=False, edgecolor="black", linewidth=0.5, zorder=zorder + 0.1)
    return container


def legend_handle(color, hatch):
    """Legend handle of a bar: the fill with its hatch under the black outline (a tuple, see shared_legend)."""
    from matplotlib.patches import Patch
    return (Patch(facecolor=color, edgecolor=hatch_color(color), linewidth=0, hatch=hatch),
            Patch(fill=False, edgecolor="black", linewidth=0.5))


SUBPLOT_SIZE = 5.5          # inches, square
BAR_WIDTH = 0.5             # simple bars
GROUP_BAR_WIDTH = 0.14      # grouped bars
WHISKER = dict(ecolor="black", elinewidth=3, capsize=7, capthick=3)   # thick whiskers: the figures are scaled down a lot
QUERIES_BAR_WIDTH = 0.14    # temporal-query figure: the grouped default (the 0.20 override was meant for the per-output figure)
QUERY_LENGTH_DATAFLOWS = ("taxi_1", "twitter_1")   # the window-length sweep is plotted for these two (as in the submitted paper)
LAYOUT = dict(bottom=0.15, left=0.08, right=0.98, wspace=0.25, top=0.80)  # top leaves room for titles and legend
LEGEND_ANCHOR = (0.5, 0.90)

FONT_DEFAULT = dict(base=26, title=28, suptitle=32, label=26, tick=22, legend=24, annotation=22)
FONT = dict(FONT_DEFAULT)   # the sizes in use; setup_style(scale) rescales them from FONT_DEFAULT
SCALE = 1.0                 # font scale in use (setup_style); margins, gaps and the room for the legend grow with it
PAPER_TICK_PT = 7.0         # tick labels (and, scaled alike, legend 7.6, axis labels 8.3, titles 8.9, bar labels 7) in the paper


def paper_fonts(inserted_pt, native_pt, tick_pt=PAPER_TICK_PT, legend_pt=None, label_pt=None, title_pt=None):
    """Fonts for a figure that the paper shrinks by s = inserted_pt / native_pt (width of the PDF after bbox tight): the sizes of
    FONT_DEFAULT times f, so that the tick labels measure tick_pt in the paper (legend and axis labels may be given in paper pt
    too). Returns f."""
    s = inserted_pt / native_pt
    f = tick_pt / (FONT_DEFAULT["tick"] * s)
    setup_style(f)
    if legend_pt:
        FONT["legend"] = legend_pt / s
        plt.rcParams["legend.fontsize"] = FONT["legend"]
    if label_pt:
        FONT["label"] = label_pt / s
        plt.rcParams["axes.labelsize"] = FONT["label"]
    if title_pt:
        FONT["title"] = title_pt / s
        plt.rcParams["axes.titlesize"] = FONT["title"]
    return f


def setup_style(scale=1.0):
    """rcParams of the paper's plots; fonts are embedded as TrueType (Type 42). scale: factor on every font size."""
    global SCALE
    SCALE = scale
    FONT.update({k: v * scale for k, v in FONT_DEFAULT.items()})
    plt.rcParams.update({
        "font.size": FONT["base"],
        "axes.titlesize": FONT["title"],
        "figure.titlesize": FONT["suptitle"],
        "axes.labelsize": FONT["label"],
        "xtick.labelsize": FONT["tick"],
        "ytick.labelsize": FONT["tick"],
        "legend.fontsize": FONT["legend"],
        "pdf.fonttype": 42,
        "ps.fonttype": 42,
        "hatch.linewidth": HATCH_LINEWIDTH,
    })


def scaled_margins(width, height, left, right, bottom, top, wspace=0.0, ncols=1):
    """subplots_adjust arguments for margins given as figure fractions at font scale 1: the margins (and the gap between panels)
    are text-sized, so in inches they grow with SCALE (the gap between panels does not: it holds tick labels only); the axes take the rest."""
    k = SCALE
    axw = width * (right - left) / (ncols + (ncols - 1) * wspace)   # panel width at scale 1
    l, r, b, t = left * width * k, (1 - right) * width * k, bottom * height * k, (1 - top) * height * k
    gap = wspace * axw   # already wide enough for the tick labels of the next panel (they are the only text in it)
    new_axw = (width - l - r - (ncols - 1) * gap) / ncols
    return dict(left=l / width, right=1 - r / width, bottom=b / height, top=1 - t / height,
                wspace=gap / new_axw if ncols > 1 else 0.0)


def new_figure(ncols):
    """A single row of square subplots with the paper's margins."""
    fig, axes = plt.subplots(1, ncols, figsize=(SUBPLOT_SIZE * ncols, SUBPLOT_SIZE), squeeze=False)
    fig.subplots_adjust(**scaled_margins(SUBPLOT_SIZE * ncols, SUBPLOT_SIZE, ncols=ncols, **LAYOUT))
    return fig, axes[0]


def new_paper_figure(ncols, ratio, native_pt, head, foot, width=None, left=LAYOUT["left"], right=LAYOUT["right"],
                     wspace=LAYOUT["wspace"], legend_gap=0.3, trim=0.0):
    """A single row of ncols panels in a figure whose height is fixed by the paper: height / width = ratio, the width being
    native_pt (the PDF after bbox tight). head and foot are the inches (at font scale 1) above and below the axes that hold the
    legend and titles, and the tick and axis labels; they grow with SCALE, and the axes take the rest of the height.
    width: inches (default SUBPLOT_SIZE per panel). Returns the figure, the axes and the legend anchor, legend_gap inches
    (scale 1) above the axes (the titles go in between)."""
    W = width or SUBPLOT_SIZE * ncols
    H = (ratio * native_pt - trim) / 72 - 0.2   # bbox tight adds pad_inches (0.1) on every side; trim (pt): text that sticks out of the margins
    fig, axes = plt.subplots(1, ncols, figsize=(W, H), squeeze=False)
    m = scaled_margins(W, H, left, right, 0.0, 1.0, wspace=wspace, ncols=ncols)
    m.update(top=1 - head * SCALE / H, bottom=foot * SCALE / H)
    fig.subplots_adjust(**m)
    return fig, axes[0], (0.5, m["top"] + legend_gap * SCALE / H)


FIG9 = dict(native=792.0, inserted=0.32 * 506.0, ratio=0.46, head=22.0, foot=19.0, left=24.0, right=6.0, gap=18.0, legend_gap=11.0)


def new_fixed_figure(ncols, ratio, native, inserted, head, foot, left, right, gap, legend_gap):
    """A single row of ncols panels in a figure of exactly native pt wide and ratio * native tall (save it with tight=False), so that
    figures of one row in the paper have identical sizes and the axes and legends in the same places. Margins in paper pt (the
    figure is shrunk by s = inserted / native): head above the axes (legend, titles), foot below (tick and axis labels), left, right,
    gap between panels; legend_gap: from the top of the axes to the bottom of the legend. Returns the figure, axes, legend anchor."""
    s = inserted / native
    W, H = native / 72, ratio * native / 72
    l, r, g, hd, ft = (v / s / 72 for v in (left, right, gap, head, foot))
    axw = (W - l - r - (ncols - 1) * g) / ncols
    fig, axes = plt.subplots(1, ncols, figsize=(W, H), squeeze=False)
    fig.subplots_adjust(left=l / W, right=1 - r / W, top=1 - hd / H, bottom=ft / H, wspace=g / axw)
    return fig, axes[0], (0.5, 1 - hd / H + legend_gap / s / 72 / H)


def style_axis(ax, ticks=None, log_y=False, log2_x=False):
    """y grid dashed; log y with base-10 major ticks and unlabeled minor ticks; at least 2 y ticks."""
    ax.grid(axis="y", linestyle="--", alpha=0.3, linewidth=0.5)
    ax.set_axisbelow(True)
    if log2_x:
        ax.set_xscale("log", base=2)
        if ticks is not None:
            ax.xaxis.set_major_locator(FixedLocator(ticks))
            ax.set_xticklabels([str(t) for t in ticks])
            ax.xaxis.set_minor_locator(FixedLocator([]))
    if log_y:
        ax.set_yscale("log", base=10)
        ax.yaxis.set_major_locator(LogLocator(base=10, numticks=12))
        ax.yaxis.set_minor_locator(LogLocator(base=10, subs=range(2, 10), numticks=12))
        ax.yaxis.set_minor_formatter(NullFormatter())
    ensure_two_yticks(ax)


def ensure_two_yticks(ax):
    """Fewer than two labeled y ticks inside the limits: widen to the surrounding powers of ten
    (log) or let matplotlib add ticks (linear)."""
    lo, hi = ax.get_ylim()
    inside = [t for t in ax.get_yticks() if lo <= t <= hi]
    if len(inside) >= 2:
        return
    if ax.get_yscale() == "log":
        import math
        a, b = math.floor(math.log10(lo)), math.ceil(math.log10(hi))
        if b == a:
            b += 1
        ax.set_ylim(10.0 ** a, 10.0 ** b)
        ax.set_yticks([10.0 ** e for e in range(a, b + 1)])
    else:
        ax.set_ylim(lo, hi)
        ax.yaxis.set_major_locator(plt.MaxNLocator(nbins=4, min_n_ticks=2))


def shared_legend(fig, handles, labels, anchor=LEGEND_ANCHOR, ncol=None, compact=False):
    """One legend for the figure; ncol columns (default: all methods in one row); compact: closer handles and columns."""
    from matplotlib.legend_handler import HandlerTuple
    fig.legend(handles, labels, loc="lower center", bbox_to_anchor=anchor, frameon=False,
               ncol=ncol or len(handles), handlelength=1.1 if compact else 1.6, columnspacing=0.8 if compact else 1.2,
               handletextpad=0.35 if compact else 0.5, borderpad=0.1 if compact else 0.4,
               handler_map={tuple: HandlerTuple(ndivide=1, pad=0)})


def save_pdf(fig, path, tight=True, pad=0.1):
    """PDF with embedded fonts; check with `pdffonts` (every font `emb yes`). tight=False: the figure as laid out, so that several
    figures (the panels of one row in the paper) have exactly the same size."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    if tight:
        fig.savefig(path, bbox_inches="tight", pad_inches=pad, dpi=150)
    else:
        fig.savefig(path, dpi=150)
    plt.close(fig)


# result CSVs: repetitions, aggregation, tables
def read_csv(path):
    with open(path, newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path, header, rows):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)


def fmt(x):
    """Numbers as they go into a CSV: integers without a decimal point, floats with up to 6 significant digits."""
    if isinstance(x, int):
        return str(x)
    if x == int(x) and abs(x) < 1e15:
        return str(int(x))
    return "%.6g" % x


def aggregate_reps(rep_files, keys, spread=()):
    """One row per key over the repetitions of an experiment.

    rep_files: one CSV per repetition, all with the same layout. Rows are matched on the columns `keys`. A column
    that is numeric in every repetition becomes its median; the columns named in `spread` also get <col>_min and
    <col>_max; any other column keeps the value of the first repetition. A column `reps` counts the repetitions.
    Returns (header, rows) in the row order of the first file."""
    tables = [read_csv(f) for f in rep_files]
    base = tables[0]
    columns = list(base[0].keys())
    by_key = [{tuple(r[k] for k in keys): r for r in t} for t in tables]
    header = list(keys)
    for c in columns:
        if c in keys:
            continue
        header.append(c)
        if c in spread:
            header += [c + "_min", c + "_max"]
    header.append("reps")
    rows = []
    for r0 in base:
        key = tuple(r0[k] for k in keys)
        group = [t[key] for t in by_key if key in t]
        row = list(key)
        for c in columns:
            if c in keys:
                continue
            try:
                values = [float(g[c]) for g in group]
            except (ValueError, KeyError):
                values = None
            if values is None:
                row.append(group[0].get(c, ""))
                if c in spread:
                    row += ["", ""]
                continue
            row.append(fmt(statistics.median(values)))
            if c in spread:
                row += [fmt(min(values)), fmt(max(values))]
        row.append(len(group))
        rows.append(row)
    return header, rows


def tex_name(name):
    return "\\textsf{%s}" % name.replace("_", "\\_")


def write_latex_table(path, header, rows, align=None):
    """A booktabs tabular (no table environment, no caption) as a snippet for the paper."""
    align = align or "l" + "r" * (len(header) - 1)
    lines = ["\\begin{tabular}{%s}" % align, "\\toprule", " & ".join(header) + " \\\\", "\\midrule"]
    for r in rows:
        lines.append(" & ".join(str(c) for c in r) + " \\\\")
    lines += ["\\bottomrule", "\\end{tabular}"]
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text("\n".join(lines) + "\n")


# verification of the temporal-query answers
def _answers(path, channels, genealog=False):
    """{(query or window length, i): (from, to, counts tuple, defined)} of a CAPS / Green answer file; for the
    GeneaLog file counts is the one-element tuple (total,)."""
    out = {}
    for r in read_csv(path):
        key = (r.get("query") or r["len_ms"], int(r["i"]))
        if genealog:
            counts, defined = (int(r["total"]),), None
        else:
            counts, defined = tuple(int(r["c%d" % c]) for c in range(channels)), int(r["defined"])
        out[key] = (int(r["from"]), int(r["to"]), counts, defined)
    return out


def verify_answers(rundir, channels, sweep, report):
    """Green and GeneaLog answers against CAPS, query by query: Green must equal CAPS in every channel (and the
    window and definedness), GeneaLog's traversal count must equal the sum over the channels. Files in rundir:
    query_answers[_green|_genealog].csv, or query_length_answers[...] for the window sweep. `report(check, n,
    mismatches)` receives the result of each check; returns the total number of mismatches."""
    stem = "query_length_answers" if sweep else "query_answers"
    rundir = Path(rundir)
    caps = _answers(rundir / (stem + ".csv"), channels)
    bad_total = 0
    for method in ("green", "genealog"):
        path = rundir / (stem + "_" + method + ".csv")
        if not path.is_file():
            continue
        other = _answers(path, channels, genealog=(method == "genealog"))
        bad = abs(len(other) - len(caps))
        for k, (frm, to, counts, defined) in caps.items():
            o = other.get(k)
            if o is None or o[0] != frm or o[1] != to:
                bad += 1
            elif method == "green" and (o[2] != counts or o[3] != defined):
                bad += 1
            elif method == "genealog" and o[2][0] != sum(counts):
                bad += 1
        report(method + "_vs_caps", len(caps), bad)
        bad_total += bad
    return bad_total

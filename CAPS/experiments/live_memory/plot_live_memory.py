"""Figures of experiments/live_memory (paper style, experiments/common.py): one subplot per dataflow (alphabetical),
GeneaLog, Green, CAPS (ours last), log y axis, one shared legend. Each PDF has a CSV of the plotted numbers next to it.

  live_memory_peak_bar.pdf       peak annotation bytes in the window state of all operators at a time, in MB. These are
                                 INSTRUMENTED MODEL BYTES, not heap: the running job counts the annotation bytes that the method
                                 adds to each (key, window pane) (CAPS M * 8 B, Green 8 B per variable, GeneaLog 16 B / 32 B) and
                                 subtracts them when the pane is purged; the peak is taken on an event-time schedule (the
                                 events re-ordered by event time with a stage order, as a live stream would deliver them), a
                                 function of the data only. For GeneaLog the bar is the core model of its window operators (aggregate: two pointers per
                                 window, join: 32 B per buffered tuple copy); the triangle adds what GeneaLog also holds
                                 there (the tuples its aggregate chains keep alive, the provenance sets of its tuples after
                                 an exchange); the diamond is the provenance graph that its pointers keep reachable from
                                 the outputs (nodes x 32 B), which a query needs.
  live_memory_peak_wall_bar.pdf  the same instrumented counts, peak over WALL-CLOCK time of the replay (a replay races ahead
                                 of itself, so this depends on how far the operator threads run apart and differs between
                                 runs). GeneaLog is the full model (core + pinned tuples + provenance sets): the wall-time
                                 series does not separate the core.
  live_memory_retained_bar.pdf   RETAINED meta-data (analytical, the retained column of Table 2), in MB.
The JVM heap that the jobs really keep alive is in experiments/heap_check.

Usage: plot_live_memory.py <results directory with live_memory.csv>
"""
import argparse
import math
import sys
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import common  # noqa: E402
import plot_utils  # noqa: E402

MB = 1e6
METHODS = ("genealog", "green", "caps")


def marker(ax, x, y, shape):
    """A hollow marker at the value y (its number is in the CSV next to the PDF)."""
    ax.plot([x], [y], marker=shape, markersize=14, markerfacecolor="white", markeredgecolor="black", markeredgewidth=1.5,
            linestyle="none", zorder=5)


def value_label(v):
    """Value label of a bar, in MB, without exponents."""
    if v == 0:
        return "0"
    if v >= 100:
        return "%.0f" % v
    if v >= 10:
        return "%.1f" % v
    if v >= 1:
        return "%.2f" % v
    return "%.2g" % v


def bar_figure(rows, column, ylabel, pdf, markers=()):
    """markers: (csv column, marker shape, legend label) drawn for GeneaLog, to the left of its bar."""
    dfs = [d for d in common.DATAFLOWS if any(r["dataflow"] == d for r in rows)]
    fig, axes = common.new_figure(len(dfs))
    out = []
    for ax, d in zip(axes, dfs):
        val = {r["method"]: float(r[column]) / MB for r in rows if r["dataflow"] == d}
        extra = [(shape, float([r for r in rows if r["dataflow"] == d and r["method"] == "genealog"][0][col]) / MB)
                 for col, shape, _ in markers]
        pos = [v for v in val.values() if v > 0] + [v for _, v in extra if v > 0]
        lowest, highest = min(pos), max(pos)
        top = 10.0 ** math.ceil(math.log10(highest * 2.5))   # head room for the value labels
        bottom = 10.0 ** (math.floor(math.log10(lowest)) - 1)
        ax.set_yscale("log")
        ax.set_ylim(bottom, top)
        first = 0
        for i, m in enumerate(METHODS):
            v = val[m]
            if v > 0:
                plot_utils.err_bar(ax, first + i, v, v, v, common.BAR_WIDTH, m, top=top)
            ax.text(first + i, max(v, bottom) * 1.15, value_label(v), ha="center", va="bottom", fontsize=common.FONT["annotation"])
            out.append([d, m, v])
        for slot, ((col, shape, label), (_, v)) in enumerate(zip(markers, extra)):
            marker(ax, -0.5, v, shape)
            out.append([d, label, v])
        ax.set_xlim(-0.85, len(METHODS) - 0.4)
        ax.set_xticks([])
        ax.set_title(common.DATAFLOW_NAMES.get(d, d))
        common.style_axis(ax, log_y=True)
    axes[0].set_ylabel(ylabel)
    handles, labels = plot_utils.method_handles(METHODS)
    for k, (col, shape, label) in enumerate(markers):
        handles.insert(1 + k, Line2D([0], [0], marker=shape, markersize=14, markerfacecolor="white", markeredgecolor="black",
                                     markeredgewidth=1.5, linestyle="none"))
        labels.insert(1 + k, label)
    plot_utils.finish(fig, handles, labels, pdf, ["dataflow", "method", "MB"], out)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path)
    args = parser.parse_args()
    common.setup_style()
    rows = [r for r in common.read_csv(args.results / "live_memory.csv") if r["method"] != "noprov"]
    bar_figure(rows, "peak_live_bytes", "Peak window state, event-time order [MB]", args.results / "live_memory_peak_bar.pdf",
               markers=(("peak_live_full_bytes", "v", "GeneaLog + pinned"), ("graph_reachable_bytes", "D", "GeneaLog graph")))
    bar_figure(rows, "peak_live_wall_bytes", "Peak window state, wall-clock [MB]", args.results / "live_memory_peak_wall_bar.pdf",
               markers=(("graph_reachable_bytes", "D", "GeneaLog graph"),))
    bar_figure(rows, "retained_bytes", "Retained meta-data [MB]", args.results / "live_memory_retained_bar.pdf")

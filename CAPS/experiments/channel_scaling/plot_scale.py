#!/usr/bin/env python3
"""Figures of the channel-cardinality experiment, from the CSVs of summarize_scale.py.

One figure per metric against M (log2 x axis), one subplot per sweep (alphabetical: path, source),
one line per method in the paper's style (experiments/common.py). Every figure ships with a CSV of
the plotted series next to it (plot_<figure>.csv).

  runtime        Flink job duration, median with min/max bars       (all four methods)
  memory         retained meta-data in MB (analytical)              (GeneaLog, Green, CAPS)
  query_total    per-output query, total contributions, ns          (GeneaLog, Green, CAPS)
  query_vector   per-output query, all M channels, ns               (Green, CAPS)
  temporal       temporal how-much query over the prefix-sum index  (CAPS)

Usage: plot_scale.py --res RESULTS_DIR
"""
import argparse
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import common  # noqa: E402

SWEEP_TITLES = {"path": "Path sweep", "source": "Source sweep"}


def load(path):
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def series(rows, sweep, method=None, **where):
    """(M, row) pairs of one line, sorted by M."""
    out = []
    for r in rows:
        if r["sweep"] != sweep or (method is not None and r.get("method") != method):
            continue
        if all(r.get(k) == v for k, v in where.items()):
            out.append((int(r["M"]), r))
    return sorted(out, key=lambda t: t[0])


def plot(res, name, rows, methods, value, ylabel, log_y, where=None, err=None, linestyles=None):
    """err = (lo_column, hi_column) of absolute bounds, or None. Writes <name>.pdf and <name>.csv."""
    where = where or {}
    sweeps = sorted({r["sweep"] for r in rows})
    ms = sorted({int(r["M"]) for r in rows})
    fig, axes = common.new_figure(len(sweeps))
    out_rows = []
    handles = {}
    for ax, sweep in zip(axes, sweeps):
        for method in methods:
            pts = series(rows, sweep, method, **where)
            if not pts:
                continue
            x = [m for m, _ in pts]
            y = [float(r[value]) for _, r in pts]
            style = (linestyles or {}).get(method, "-")
            kw = dict(color=common.METHOD_COLORS[method], marker=common.METHOD_MARKERS[method],
                      markersize=9, linewidth=2, linestyle=style, markeredgecolor="black",
                      markeredgewidth=0.5)
            if err:
                lo = [yy - float(r[err[0]]) for yy, (_, r) in zip(y, pts)]
                hi = [float(r[err[1]]) - yy for yy, (_, r) in zip(y, pts)]
                h = ax.errorbar(x, y, yerr=[lo, hi], capsize=4, **kw)
            else:
                (h,) = ax.plot(x, y, **kw)
            handles[method] = h
            for (m, r), yy in zip(pts, y):
                out_rows.append([sweep, m, method, yy] + ([r[err[0]], r[err[1]]] if err else []))
        ax.set_title(SWEEP_TITLES.get(sweep, sweep))
        ax.set_xlabel("Channels $m$")
        common.style_axis(ax, ticks=ms, log_y=log_y, log2_x=True)
    axes[0].set_ylabel(ylabel)
    present = [m for m in common.METHOD_ORDER if m in handles]
    common.shared_legend(fig, [handles[m] for m in present], [common.METHOD_NAMES[m] for m in present])
    common.save_pdf(fig, res / (name + ".pdf"))
    with (res / ("plot_" + name + ".csv")).open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["sweep", "M", "method", value] + (list(err) if err else []))
        writer.writerows(out_rows)
    print("  %s.pdf, plot_%s.csv" % (name, name))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--res", type=Path, required=True)
    args = parser.parse_args()
    res = args.res
    common.setup_style()

    runtime = load(res / "runtime.csv")
    plot(res, "runtime", runtime, common.METHOD_ORDER, "median_s", "Job duration [s]", False,
         err=("min_s", "max_s"))

    memory = load(res / "memory.csv")
    plot(res, "memory", memory, ("genealog", "green", "caps"), "retained_MB", "Retained meta-data [MB]", True)

    query = load(res / "query.csv")
    plot(res, "query_total", query, ("genealog", "green", "caps"), "score_ns", "Query [ns/output]", True,
         where={"kind": "total"})
    plot(res, "query_vector", query, ("green", "caps"), "score_ns", "Query [ns/output]", True,
         where={"kind": "vector"})

    temporal = [dict(r, method="caps") for r in load(res / "temporal.csv")]
    plot(res, "temporal", temporal, ("caps",), "score_ns", "Temporal query [ns]", True)

#!/usr/bin/env python3
"""Summary and figure of experiments/heap_check: the heap of the TaskManager JVM that is still in use after a forced GC,
sampled while the how_much job of each method runs, next to the numbers that the other memory experiments compute.

Per (dataflow, method, repetition) the unit directory holds heap_samples.csv (epoch_s, used_mb, old_mb: one forced GC per
sample) and times.txt (job start and end, epoch seconds). The idle baseline is the median of the samples before the job;
the peak is the largest sample while the job ran. The computed numbers come from experiments/live_memory/results_p1/
(logical peak of the window state, wall-time peak, retained model and, for GeneaLog, the reachable graph, all in MB = 10^6 B;
the heap here is in MiB, converted).

Writes heap_check.csv and heap_check_bar.pdf (+ csv) into the results directory.
Usage: summarize_heap.py RESULTS_DIR
"""
import argparse
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import common  # noqa: E402
import plot_utils  # noqa: E402

MIB = 1024 * 1024 / 1e6          # one MiB in MB
METHODS = ("noprov", "genealog", "green_polynomial", "caps")
KEY = {"noprov": "noprov", "genealog": "genealog", "green_polynomial": "green", "caps": "caps"}


def read_kv(path):
    out = {}
    for line in Path(path).read_text().splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            out[k.strip()] = float(v)
    return out


def unit_summary(rep_dir):
    t = read_kv(rep_dir / "times.txt")
    rows = [tuple(float(x) for x in line.split(",")) for line in (rep_dir / "heap_samples.csv").read_text().splitlines()[1:]]
    idle = [u for (e, u, _) in rows if e < t["job_start_epoch_s"]]
    during = [(e, u, o) for (e, u, o) in rows if t["job_start_epoch_s"] <= e <= t["job_end_epoch_s"] + 1.0]
    if not idle or not during:
        raise SystemExit("%s: no samples before or during the job" % rep_dir)
    base = statistics.median(idle)
    peak = max(u for _, u, _ in during)
    return dict(idle_mb=base * MIB, peak_mb=peak * MIB, peak_above_idle_mb=(peak - base) * MIB, samples=len(during),
                last_above_idle_mb=(during[-1][1] - base) * MIB, job_s=t["job_end_epoch_s"] - t["job_start_epoch_s"])


def computed(live_dir):
    """MB: retained model, logical live peak, wall-time live peak, GeneaLog reachable graph (one row per dataflow and method)."""
    out = {}
    for r in common.read_csv(live_dir / "live_memory.csv"):
        out[(r["dataflow"], r["method"])] = dict(
            retained=float(r["retained_bytes"] or 0) / 1e6, logical=float(r["peak_live_bytes"] or 0) / 1e6,
            logical_full=float(r["peak_live_full_bytes"] or 0) / 1e6, wall=float(r["peak_live_wall_bytes"] or 0) / 1e6,
            graph=float(r["graph_reachable_bytes"] or 0) / 1e6)
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("results", type=Path)
    args = ap.parse_args()
    res = args.results
    live = Path(__file__).resolve().parents[1] / "live_memory" / ("results_small_p1" if res.name == "small" else "results_p1")
    comp = computed(live)
    rows, out = [], []
    dataflows = sorted(d.name for d in res.iterdir() if d.is_dir())
    for df in dataflows:
        base_peak = None
        per = {}
        for m in METHODS:
            reps = sorted((res / df / m).glob("rep*"))
            if not reps:
                continue
            s = [unit_summary(r) for r in reps]
            med = {k: statistics.median(x[k] for x in s) for k in s[0]}
            per[m] = med
        noprov = per.get("noprov", {}).get("peak_above_idle_mb", 0.0)
        for m, med in per.items():
            c = comp.get((df, KEY[m]), {})
            rows.append([df, m, "%.1f" % med["idle_mb"], "%.1f" % med["peak_mb"], "%.1f" % med["peak_above_idle_mb"],
                         "%.1f" % (med["peak_above_idle_mb"] - noprov), "%.1f" % med["last_above_idle_mb"], med["samples"], "%.1f" % med["job_s"],
                         "%.3f" % c.get("retained", 0), "%.3f" % c.get("logical", 0), "%.3f" % c.get("wall", 0),
                         "%.3f" % c.get("logical_full", 0), "%.3f" % c.get("graph", 0)])
    header = ["dataflow", "method", "idle_heap_mb", "peak_heap_after_gc_mb", "peak_above_idle_mb", "peak_above_noprov_mb",
              "last_sample_above_idle_mb", "samples_during_job", "job_s", "retained_model_mb", "live_logical_mb", "live_wall_mb",
              "genealog_full_logical_mb", "genealog_graph_mb"]
    common.write_csv(res / "heap_check.csv", header, rows)
    print("wrote %s" % (res / "heap_check.csv"))

    import matplotlib.pyplot as plt  # noqa: E402
    from matplotlib.lines import Line2D  # noqa: E402
    import math  # noqa: E402
    common.setup_style()
    dfs = [d for d in common.DATAFLOWS if d in dataflows]
    fig, axes = common.new_figure(len(dfs))
    csv_rows = []
    shown = ("genealog", "green_polynomial", "caps")
    for ax, df in zip(axes, dfs):
        raw = {r[1]: float(r[5]) for r in rows if r[0] == df}
        vals = {m: max(v, 0.2) for m, v in raw.items()}
        top = 10.0 ** math.ceil(math.log10(max(vals.values()) * 2.5))
        ax.set_yscale("log")
        ax.set_ylim(0.1, top)
        for i, m in enumerate(shown):
            if m not in vals:
                continue
            plot_utils.err_bar(ax, i, vals[m], vals[m], vals[m], common.BAR_WIDTH, KEY[m], top=top)
            ax.text(i, vals[m] * 1.15, ("%.0f" % raw[m]) if raw[m] > 0 else "<= 0", ha="center", va="bottom", fontsize=common.FONT["annotation"])
            c = comp.get((df, KEY[m]), {})
            for shape, key, dx in (("D", "retained", 0.34), ("v", "wall", -0.34), ("s", "graph", 0.0)):
                v = c.get(key, 0)
                if v > 0:
                    ax.plot([i + dx], [v], marker=shape, markersize=14, markerfacecolor="white", markeredgecolor="black",
                            markeredgewidth=1.5, linestyle="none", zorder=5)
            csv_rows.append([df, m, vals[m], c.get("retained", 0), c.get("wall", 0)])
        ax.set_xticks([])
        ax.set_xlim(-0.7, len(shown) - 0.3)
        ax.set_title(common.DATAFLOW_NAMES.get(df, df))
        common.style_axis(ax, log_y=True)
    axes[0].set_ylabel("Heap above No Prov [MB]")
    handles, labels = plot_utils.method_handles(shown and [KEY[m] for m in shown])
    handles += [Line2D([0], [0], marker="D", markersize=14, markerfacecolor="white", markeredgecolor="black", markeredgewidth=1.5, linestyle="none"),
                Line2D([0], [0], marker="v", markersize=14, markerfacecolor="white", markeredgecolor="black", markeredgewidth=1.5, linestyle="none"),
                Line2D([0], [0], marker="s", markersize=14, markerfacecolor="white", markeredgecolor="black", markeredgewidth=1.5, linestyle="none")]
    labels += ["retained (model)", "window state (wall)", "GeneaLog graph (measured)"]
    plot_utils.finish(fig, handles, labels, res / "heap_check_bar.pdf", ["dataflow", "method", "heap_above_noprov_mb", "retained_model_mb", "live_wall_mb"], csv_rows)

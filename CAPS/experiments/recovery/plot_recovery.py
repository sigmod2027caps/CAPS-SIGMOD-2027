#!/usr/bin/env python3
"""Figures of the recovery experiment (E4), from recovery_runs.csv.

  recovery_overhead.pdf  job runtime without and with checkpointing (unpaced runs; bars = median, whiskers = min and max)
  recovery_outage.pdf    outage of the paced runs, from the failure (the kill, or the injected exception) until the restored index
                         sink is open again: injected failure, TaskManager killed (heartbeat detection + restart + restore)
Each figure has one subplot per dataflow (alphabetical); the data is in recovery_runs.csv / recovery_summary.csv.
"""
import argparse
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import common  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import Patch  # noqa: E402


def groups(rows, dataflow, kinds):
    """{variant: [durations]} of the dataflow; kinds maps a CSV kind to a variant key."""
    out = {}
    for r in rows:
        if r["dataflow"] == dataflow and r["kind"] in kinds and r["duration_s"] != "":
            out.setdefault(kinds[r["kind"]], []).append(float(r["duration_s"]))
    return out


def draw(rows, variants, kinds, ylabel, path):
    dataflows = [d for d in common.DATAFLOWS if groups(rows, d, kinds)]
    if not dataflows:
        return
    common.setup_style()
    fig, axes = common.new_figure(len(dataflows))
    for ax, dataflow in zip(axes, dataflows):
        g = groups(rows, dataflow, kinds)
        for i, v in enumerate(variants):
            if v not in g:
                continue
            med = statistics.median(g[v])
            ax.bar(i, med, width=common.BAR_WIDTH, color=common.RUN_COLORS[v], hatch=common.RUN_HATCHES[v],
                   edgecolor="black", linewidth=0.5)
            if len(g[v]) > 1:
                ax.errorbar(i, med, yerr=[[med - min(g[v])], [max(g[v]) - med]], color="black", capsize=4, linewidth=1)
        ax.set_title(common.DATAFLOW_NAMES.get(dataflow, dataflow))
        ax.set_xticks([])
        ax.set_ylim(bottom=0)
        if ax is axes[0]:
            ax.set_ylabel(ylabel)
        common.style_axis(ax)
    handles = [Patch(facecolor=common.RUN_COLORS[v], hatch=common.RUN_HATCHES[v], edgecolor="black", linewidth=0.5,
                     label=common.RUN_NAMES[v]) for v in variants]
    common.shared_legend(fig, handles, [common.RUN_NAMES[v] for v in variants])
    common.save_pdf(fig, path)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--results", required=True)
    args = ap.parse_args()
    results = Path(args.results)
    rows = common.read_csv(results / "recovery_runs.csv")
    draw(rows, ("plain", "ckpt"), {"time_plain": "plain", "time_ckpt": "ckpt"}, "Runtime (s)", results / "recovery_overhead.pdf")
    summary = common.read_csv(results / "recovery_summary.csv")
    outage = [{"dataflow": r["dataflow"], "kind": k, "duration_s": float(r[k + "_outage_ms"]) / 1000.0}
              for r in summary for k in ("inject", "kill") if r.get(k + "_outage_ms", "") != ""]
    draw(outage, ("inject", "kill"), {"inject": "inject", "kill": "kill"}, "Outage (s)", results / "recovery_outage.pdf")

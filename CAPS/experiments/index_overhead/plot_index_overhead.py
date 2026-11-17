"""Table and figure of experiments/index_overhead: runtime of CAPS capture only and with the online two-tier index, against No Prov.

Reads <dir>/reps.csv (dataflow, arm, rep, job_duration_s), writes index_overhead.csv (median, min, max over the repetitions and the
overhead of every arm over its No Prov reference: noprov for capture, noprov_nocoin for the index arms), index_overhead_runtime_bar.{pdf,csv}. One subplot per dataflow (alphabetical); No Prov is the
dashed line with its min-max band; the bars are the median with min-max whiskers.

Usage: plot_index_overhead.py <results directory>
"""
import statistics
import sys
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import Patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import common  # noqa: E402

ARMS = ("noprov", "noprov_nocoin") + common.OVERHEAD_ORDER
REF = {"capture": "noprov", "tree": "noprov_nocoin", "hybrid_timed": "noprov_nocoin", "hybrid": "noprov_nocoin"}

if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    res = Path(sys.argv[1]).resolve()
    rows = common.read_csv(res / "reps.csv")
    dataflows = [d for d in common.DATAFLOWS if any(r["dataflow"] == d for r in rows)]
    stats = {}
    for d in dataflows:
        for arm in ARMS:
            v = [float(r["job_duration_s"]) for r in rows if r["dataflow"] == d and r["arm"] == arm]
            if v:
                stats[(d, arm)] = (statistics.median(v), min(v), max(v), len(v))
    table = []
    for d in dataflows:
        for arm in ARMS:
            if (d, arm) in stats:
                med, lo, hi, n = stats[(d, arm)]
                ref = REF.get(arm, arm)
                table.append([d, arm, "%.3f" % med, "%.3f" % lo, "%.3f" % hi, n,
                              "%.1f" % (100.0 * (med / stats[(d, ref)][0] - 1)), ref])
    common.write_csv(res / "index_overhead.csv",
                     ["dataflow", "arm", "median_s", "min_s", "max_s", "reps", "overhead_pct", "reference"], table)

    common.setup_style()
    fig, axes = common.new_figure(len(dataflows))
    ref = None
    for ax, d in zip(axes, dataflows):
        top = 0.0
        for i, arm in enumerate(common.OVERHEAD_ORDER):
            med, lo, hi, _ = stats[(d, arm)]
            ax.bar(i, med, common.BAR_WIDTH, color=common.OVERHEAD_COLORS[arm], edgecolor="black", linewidth=0.5,
                   hatch=common.OVERHEAD_HATCHES[arm], zorder=3)
            ax.errorbar([i], [med], yerr=[[med - lo], [hi - med]], fmt="none", ecolor="black", elinewidth=1.5, capsize=4, zorder=4)
            top = max(top, hi)
        nm, nlo, nhi, _ = stats[(d, "noprov")]
        ref = ax.axhline(nm, color=common.METHOD_COLORS["noprov"], linestyle="--", linewidth=2, zorder=5)
        ax.axhspan(nlo, nhi, color=common.METHOD_COLORS["noprov"], alpha=0.25, zorder=2)
        cm, clo, chi, _ = stats[(d, "noprov_nocoin")]
        ref2 = ax.axhline(cm, color="black", linestyle=":", linewidth=2, zorder=5)
        ax.set_ylim(0, max(top, nhi, chi) * 1.15)
        ax.set_xticks([])
        ax.set_title(common.DATAFLOW_NAMES.get(d, d))
        common.style_axis(ax)
    axes[0].set_ylabel("Runtime [s]")
    handles = [ref, ref2] + [Patch(facecolor=common.OVERHEAD_COLORS[a], edgecolor="black", linewidth=0.5, hatch=common.OVERHEAD_HATCHES[a])
                       for a in common.OVERHEAD_ORDER]
    common.shared_legend(fig, handles, ["No Prov", "No Prov, no coin"] + [common.OVERHEAD_NAMES[a] for a in common.OVERHEAD_ORDER])
    common.save_pdf(fig, res / "index_overhead_runtime_bar.pdf")
    common.write_csv(res / "index_overhead_runtime_bar.csv", ["dataflow", "arm", "median_s", "min_s", "max_s", "reps"],
                     [[d, a] + ["%.3f" % x for x in stats[(d, a)][:3]] + [stats[(d, a)][3]]
                      for d in dataflows for a in ARMS])
    print("wrote %s, %s" % (res / "index_overhead.csv", res / "index_overhead_runtime_bar.pdf"))

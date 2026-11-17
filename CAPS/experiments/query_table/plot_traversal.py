"""Per-output query figure of the revised paper (Section 7.3): two panels in one row, grouped bars per dataflow.

  left   JMH latency per query [ns], whiskers = 99.9% confidence interval (3 forks x 5 iterations)
  right  in-situ latency [ns]: median over the ~1,000 queries timed in the stream (median of three runs)

Reads query_table_summary.csv of the output directory and writes traversal.pdf and traversal.csv (the plotted numbers).
Style: experiments/common.py. Usage: plot_traversal.py [output directory, default: next to this script]
"""
import csv
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
import common  # noqa: E402

METHODS = ("genealog", "green", "caps")
# Figure in the paper: inserted width, native width of the PDF (pt), height / width, tick font size (pt), inches above and below the axes
XTICK = 4.0   # x tick labels (paper pt)
INSERTED, NATIVE, RATIO, TICK, LEGEND, HEAD, FOOT = 0.32 * 506.0, 792.0, 386.7 / 792.9, 6.0, 5.5, 0.88, 0.6
PANELS = (("jmh_ns", "jmh_ci_ns", "JMH"), ("insitu_sample_median_ns", None, "In situ (median)"))

if __name__ == "__main__":
    common.setup_style()
    common.paper_fonts(INSERTED, NATIVE, TICK, LEGEND, label_pt=6.5, title_pt=7.0)
    OUT = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else HERE
    rows = {(r["dataflow"], r["method"]): r for r in common.read_csv(OUT / "query_table_summary.csv")}
    fig, axes, anchor = common.new_fixed_figure(len(PANELS), **common.FIG9)
    width = 0.26   # wider than the 0.14 grouped default (three bars, gap 0.22 between dataflows)
    out_rows = []
    for ax, (col, err, title) in zip(axes, PANELS):
        for i, method in enumerate(METHODS):
            xs, ys, es = [], [], []
            for j, d in enumerate(common.DATAFLOWS):
                r = rows[(d, method)]
                xs.append(j + (i - 1) * width)
                ys.append(float(r[col]))
                es.append(float(r[err]) if err else 0.0)
                out_rows.append([title, d, method, r[col], r[err] if err else ""])
            common.bar(ax, xs, ys, width, common.METHOD_COLORS[method], common.METHOD_HATCHES[method],
                       yerr=es if err else None, capsize=common.WHISKER["capsize"],
                       error_kw=dict(elinewidth=common.WHISKER["elinewidth"], capthick=common.WHISKER["capthick"]))
        ax.set_title(title)
        ax.set_xticks(range(len(common.DATAFLOWS)))
        ax.set_xticklabels([common.DATAFLOW_NAMES[d] for d in common.DATAFLOWS])
        ax.tick_params(axis="x", labelsize=XTICK * NATIVE / INSERTED)   # six names side by side in a panel 60 pt wide
        common.style_axis(ax, log_y=True)
    axes[0].set_ylabel("Latency [ns]")
    handles = [common.legend_handle(common.METHOD_COLORS[m], common.METHOD_HATCHES[m]) for m in METHODS]
    common.shared_legend(fig, handles, [common.METHOD_NAMES[m] for m in METHODS], anchor, compact=True)
    common.save_pdf(fig, OUT / "traversal.pdf", tight=False)
    with (OUT / "traversal.csv").open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["panel", "dataflow", "method", "latency_ns", "ci_ns"])
        writer.writerows(out_rows)

#!/usr/bin/env python3
"""Figure of the nested-path test, from results/nested.csv (summarize_nested.py).

Three panels against the declared path channels M (8, 64, 512): runtime / No Prov, retained meta-data,
per-output total query. Style of experiments/common.py. Writes nested.pdf and plot_nested.csv.

Usage: plot_nested.py --res RESULTS_DIR
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import common  # noqa: E402

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--res", type=Path, required=True)
    res = ap.parse_args().res
    common.setup_style()
    rows = [r for r in common.read_csv(res / "nested.csv") if r["verification"] == "PASS"]
    ms = [int(r["m"]) for r in rows]
    panels = [
        ("Runtime / No Prov", "Runtime / No Prov", False, "runtime_rel_noprov"),
        ("Retained meta-data", "Retained [MB]", True, "retained_MB"),
        ("Query: total", "Query [ns/output]", True, "query_total_ns"),
    ]
    fig, axes = common.new_figure(len(panels))
    handles, out = {}, []
    for ax, (title, ylabel, log_y, col) in zip(axes, panels):
        pts = [(int(r["m"]), float(r[col])) for r in rows if r[col] != ""]
        (h,) = ax.plot([p[0] for p in pts], [p[1] for p in pts], color=common.METHOD_COLORS["caps"],
                       marker=common.METHOD_MARKERS["caps"], markersize=9, linewidth=2,
                       markeredgecolor="black", markeredgewidth=0.5)
        handles["caps"] = h
        out += [[title, "caps", m, v] for m, v in pts]
        if col == "runtime_rel_noprov":
            (h,) = ax.plot(ms, [1.0] * len(ms), color=common.METHOD_COLORS["noprov"], marker=common.METHOD_MARKERS["noprov"],
                           markersize=9, linewidth=2, markeredgewidth=2)
            handles["noprov"] = h
            out += [[title, "noprov", m, 1.0] for m in ms]
        ax.set_title(title, fontsize=22)
        ax.set_xlabel("Declared path channels $m$")
        ax.set_ylabel(ylabel)
        common.style_axis(ax, ticks=ms, log_y=log_y, log2_x=True)
        if not log_y:
            ax.set_ylim(bottom=0)
    present = [m for m in ("noprov", "caps") if m in handles]
    common.shared_legend(fig, [handles[m] for m in present], [common.METHOD_NAMES[m] for m in present])
    common.save_pdf(fig, res / "nested.pdf")
    common.write_csv(res / "plot_nested.csv", ["panel", "method", "M", "value"], out)
    print("  nested.pdf, plot_nested.csv")

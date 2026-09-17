"""Combined experiment figures.

One figure per experiment, with one subfigure per dataflow (all in a single
row) and a single legend shared by all subfigures, placed above them.

Usage:  plot_utils.py <dir>
where <dir> is either an experiment directory (e.g. experiments/how_much,
containing one subdirectory per dataflow) or a single dataflow directory
(e.g. experiments/how_much/taxi_2), in which case the parent experiment
directory is used. The figure is regenerated from ALL dataflows that have
results, so it is safe to call from each dataflow's run.sh.
"""

from __future__ import annotations
import argparse
import csv
from pathlib import Path
import matplotlib.pyplot as plt

# All plot text — titles, axis labels, ticks, legends — is large.
plt.rcParams.update({
    "font.size": 40,
    "axes.titlesize": 44,
    "axes.labelsize": 42,
    "xtick.labelsize": 40,
    "ytick.labelsize": 40,
    "legend.fontsize": 36,
    "figure.titlesize": 44,
})

METHOD_ORDER = ("noprov", "genealog", "ink", "green_polynomial", "caps")
METHOD_LABELS = {"noprov": "No Prov", "genealog": "GeneaLog", "caps": "CAPS",
                 "ink": "Green", "green_polynomial": "Green"}
# One color per method, consistent across ALL plots:
# GeneaLog orange, ours (caps/prefix) green, no-index/noprov blue.
METHOD_COLORS = {"noprov": "#555555", "genealog": "#DD8452", "caps": "#55A868",
                 "ink": "#4C72B0", "green_polynomial": "#4C72B0"}

DATAFLOW_ORDER = ("taxi_1", "taxi_2", "nexmark_1", "nexmark_2")

# Per-subfigure size (inches); the figure is n_dataflows wide.
SUB_W, SUB_H = 12, 10


def load_csv(path: Path, value_col: str) -> dict[str, float]:
    rows: dict[str, float] = {}
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            method = row["method"].strip()
            rows[method] = float(row[value_col])
    return rows


def runtime_seconds(row: dict[str, str]) -> float:
    """Prefer Flink job duration over wall-clock `flink run`.

    `job_duration_s` is the REST job `duration` and excludes client JVM
    startup. Fall back to `runtime_s` (or `seconds`) when the job column
    is absent or empty so older CSVs still plot.
    """
    job = (row.get("job_duration_s") or "").strip()
    if job:
        return float(job)
    wall = (row.get("runtime_s") or row.get("seconds") or "").strip()
    return float(wall) if wall else 0.0


def load_runtime(path: Path) -> dict[str, float]:
    rows: dict[str, float] = {}
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            method = row["method"].strip()
            rows[method] = runtime_seconds(row)
    return rows


def _methods(data: dict[str, float], exclude: frozenset[str]) -> list[str]:
    return [m for m in METHOD_ORDER if m in data and m not in exclude]


def _dataflows(expdir: Path, *csv_names: str) -> list[str]:
    """Dataflow subdirectories that have all the given result CSVs, in
    canonical order (taxis first, then nexmark, then anything else)."""
    def has_data(f: Path) -> bool:
        # header plus at least one data row (partial runs leave header-only CSVs)
        return f.is_file() and sum(1 for _ in f.open()) >= 2

    present = [
        d.name for d in expdir.iterdir()
        if d.is_dir() and not d.name.startswith("_")
        and all(has_data(d / c) for c in csv_names)
    ]
    ordered = [d for d in DATAFLOW_ORDER if d in present]
    ordered += sorted(set(present) - set(ordered))
    return ordered


def _combined(expdir: Path, csv_names: tuple[str, ...], draw, out_path: Path,
              legend_ncol: int | None = None) -> Path | None:
    """One row of subfigures (one per dataflow) with a single shared legend.

    `draw(ax, dfdir, dataflow)` renders one dataflow into one axis and
    returns (handles, labels) for the legend (empty if the subplot's x-axis
    already identifies the bars)."""
    dataflows = _dataflows(expdir, *csv_names)
    if not dataflows:
        return None

    n = len(dataflows)
    fig, axes = plt.subplots(1, n, figsize=(SUB_W * n, SUB_H), squeeze=False)

    handles, labels = [], []
    for ax, dataflow in zip(axes[0], dataflows):
        h, l = draw(ax, expdir / dataflow, dataflow)
        if not handles:
            handles, labels = h, l
        ax.set_title(dataflow, pad=20)

    if handles:
        fig.legend(handles, labels, loc="upper center",
                   ncol=legend_ncol or len(labels), frameon=False,
                   bbox_to_anchor=(0.5, 1.0))
        fig.tight_layout(rect=(0, 0, 1, 0.87))
    else:
        fig.tight_layout()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    return out_path


# ── per-dataflow subfigure renderers ────────────────────────────


def _draw_method_bar(ax, data: dict[str, float], exclude: frozenset[str],
                     ylabel: str, yerr: dict[str, float] | None = None):
    methods = _methods(data, exclude)
    labels = [METHOD_LABELS.get(m, m) for m in methods]
    values = [data[m] for m in methods]
    colors = [METHOD_COLORS.get(m, "#888888") for m in methods]
    # When times_stats.csv is present, yerr is the job-duration standard
    # deviation. REPS=1 leaves stddev empty, so no error bar is drawn.
    errs = None
    if yerr:
        series = []
        any_err = False
        for method in methods:
            raw = yerr.get(method)
            if raw is None:
                series.append(0.0)
            else:
                series.append(raw)
                any_err = True
        if any_err:
            errs = series
    if errs is not None:
        ax.bar(labels, values, color=colors, edgecolor="black", linewidth=0.6,
               yerr=errs, capsize=8)
        ymax = max(v + e for v, e in zip(values, errs)) if values else 0.0
    else:
        ax.bar(labels, values, color=colors, edgecolor="black", linewidth=0.6)
        ymax = max(values) if values else 0.0
    ax.set_ylabel(ylabel)
    ax.set_ylim(0, ymax * 1.1 if ymax > 0 else 1.0)
    return [], []


def _load_runtime_yerr(dfdir: Path) -> dict[str, float] | None:
    stats = dfdir / "times_stats.csv"
    if not stats.is_file():
        return None
    yerr: dict[str, float] = {}
    with stats.open(newline="") as handle:
        for row in csv.DictReader(handle):
            std = (row.get("job_duration_stddev_s") or "").strip()
            if std:
                yerr[row["method"].strip()] = float(std)
    return yerr or None


def draw_runtime(exclude: frozenset[str]):
    def draw(ax, dfdir: Path, dataflow: str):
        data = load_runtime(dfdir / "times.csv")
        return _draw_method_bar(ax, data, exclude, "Runtime (seconds)",
                               _load_runtime_yerr(dfdir))
    return draw


def draw_memory(exclude: frozenset[str]):
    """Figure 10, bottom: the retained meta-data of memory_model.csv, one bar
    per method. This is the only memory figure: live state is a different
    quantity and is not plotted."""
    def draw(ax, dfdir: Path, dataflow: str):
        raw = load_csv(dfdir / "memory_model.csv", "bytes")
        # Decimal MB, the unit the paper's table reports.
        mb = {m: v / 1e6 for m, v in raw.items()}
        return _draw_method_bar(ax, mb, exclude, "Meta-data (MB)")
    return draw


QUERY_METHOD_ORDER = ("genealog", "array", "prefix")
QUERY_METHOD_LABELS = {"genealog": "GeneaLog", "array": "No-index", "prefix": "CAPS"}
QUERY_METHOD_COLORS = {
    "genealog": METHOD_COLORS["genealog"],
    "array": "#4C72B0",
    "prefix": METHOD_COLORS["caps"],
}
QUERY_LABELS = {"howmuch": "How-much", "versioning": "Versioning"}


def draw_query_latency(ax, dfdir: Path, dataflow: str):
    """Grouped bars per query type, log scale."""
    with (dfdir / "query_latency.csv").open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    queries = list(dict.fromkeys(r["query"] for r in rows))
    present = {r["method"] for r in rows}
    methods = [m for m in QUERY_METHOD_ORDER if m in present]
    values = {(r["method"], r["query"]): float(r["avg_us"]) for r in rows}

    width = 0.8 / len(methods)
    xs = range(len(queries))
    handles, labels, all_vals = [], [], []
    for j, m in enumerate(methods):
        vals = [values[(m, q)] for q in queries]
        all_vals += vals
        offset = (j - (len(methods) - 1) / 2) * width
        bars = ax.bar([x + offset for x in xs], vals, width,
                      color=QUERY_METHOD_COLORS.get(m, "#888888"),
                      edgecolor="black", linewidth=0.6)
        handles.append(bars)
        labels.append(QUERY_METHOD_LABELS.get(m, m))

    ax.set_yscale("log")
    ax.set_xticks(list(xs))
    ax.set_xticklabels([QUERY_LABELS.get(q, q) for q in queries])
    ax.set_ylabel("Query latency (µs)")
    ax.set_ylim(top=max(all_vals) * 5)
    return handles, labels


def draw_query_length(ax, dfdir: Path, dataflow: str):
    """Query latency vs query window length (in window slides of the
    dataflow), one line per method, log-log scale."""
    with (dfdir / "query_length_latency.csv").open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    lens = sorted({int(r["len_ms"]) for r in rows})
    slides = [l // lens[0] for l in lens]  # lengths are multiples of 1 slide
    values = {(r["method"], int(r["len_ms"])): float(r["avg_us"]) for r in rows}
    present = {r["method"] for r in rows}

    handles, labels = [], []
    for m in QUERY_METHOD_ORDER:
        if m not in present:
            continue
        line, = ax.plot(slides, [values[(m, l)] for l in lens],
                        color=QUERY_METHOD_COLORS.get(m, "#888888"),
                        linewidth=3, marker="o", markersize=14)
        handles.append(line)
        labels.append(QUERY_METHOD_LABELS.get(m, m))

    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xticks(slides)
    ax.set_xticklabels([str(s) for s in slides])
    ax.set_xlabel("Query window (slides)")
    ax.set_ylabel("Query latency (µs)")
    return handles, labels


PATHS_METRICS = (("runtime_s", "Runtime"), ("insert_avg_ns", "Insert"),
                 ("howmuch_avg_us", "Query"), ("index_bytes", "Memory"))
PATHS_COLORS = {"source": "#4C72B0", "path": METHOD_COLORS["caps"]}


def _paths_metric(row: dict[str, str], col: str) -> float:
    if col == "runtime_s":
        return runtime_seconds(row)
    return float(row[col])


def draw_paths(ax, dfdir: Path, dataflow: str):
    """Capture/index cost with path-level counters relative to source-level
    counters (paths experiment): one bar pair per metric."""
    with (dfdir / "paths.csv").open(newline="") as handle:
        rows = {r["granularity"]: r for r in csv.DictReader(handle)}
    source = rows["source"]

    labels = {"source": "Source-level counters", "path": "Path-level counters"}
    xs = range(len(PATHS_METRICS))
    width = 0.35
    handles = []
    for j, gran in enumerate(("source", "path")):
        vals = [_paths_metric(rows[gran], col) / _paths_metric(source, col)
                for col, _ in PATHS_METRICS]
        offset = (j - 0.5) * width
        bars = ax.bar([x + offset for x in xs], vals, width,
                      color=PATHS_COLORS[gran], edgecolor="black",
                      linewidth=0.6)
        handles.append((bars, labels[gran]))

    ax.axhline(1.0, color="#888888", linewidth=1.5, linestyle="--")
    ax.set_xticks(list(xs))
    ax.set_xticklabels([name for _, name in PATHS_METRICS])
    ax.set_ylabel("Cost relative to source-level")
    ax.set_ylim(0, 2.0)
    return [h for h, _ in handles], [l for _, l in handles]


# ── figure generation ───────────────────────────────────────────


def generate(expdir: Path) -> None:
    """Write the paper figures the experiment directory has data for.

    One figure per paper figure and nothing else: the experiments whose
    results the paper reports as a table (expiry, summarization, the
    per-output query table) get CSVs only.
    """
    exp = expdir.resolve().name
    wrote = []

    def emit(path: Path | None):
        if path is not None:
            wrote.append(path)
            print("Wrote", path)

    # Figure 10, top: end-to-end runtime. The no-provenance baseline is the
    # reference the overhead is measured against, not a bar of its own.
    emit(_combined(expdir, ("times.csv",), draw_runtime(frozenset({"noprov"})),
                   expdir / f"{exp}_runtime_bar.pdf"))

    # Figure 10, bottom: retained meta-data (the no-prov bar is always 0)
    emit(_combined(expdir, ("memory_model.csv",),
                   draw_memory(frozenset({"noprov"})),
                   expdir / f"{exp}_memory_model_bar.pdf"))

    # Figure 11: temporal how-much and versioning query latency
    emit(_combined(expdir, ("query_latency.csv",), draw_query_latency,
                   expdir / f"{exp}_query_latency_bar.pdf"))

    # Figure 12: latency against query window length
    emit(_combined(expdir, ("query_length_latency.csv",), draw_query_length,
                   expdir / f"{exp}_latency_lines.pdf"))

    # Figure 13: source-level against path-level counters
    emit(_combined(expdir, ("paths.csv",), draw_paths,
                   expdir / f"{exp}_cost_bar.pdf"))

    if not wrote:
        print(f"No experiment CSVs found under {expdir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate one combined figure per experiment: one row of "
                    "subfigures (one per dataflow) sharing a single legend.")
    parser.add_argument("dir", type=Path,
                        help="experiment directory, or one of its dataflow "
                             "subdirectories (the parent is used)")
    args = parser.parse_args()

    target = args.dir.resolve()
    # Testing for the result CSVs instead would misfire, since an experiment
    # directory also holds the combined copies of its dataflows' CSVs.
    if target.name in DATAFLOW_ORDER:
        target = target.parent  # called with a dataflow dir
    generate(target)

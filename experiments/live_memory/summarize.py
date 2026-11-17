"""Verifies the live-memory units and writes the result CSVs of experiments/live_memory.

Input: <results dir>/<dataflow>/<method>/rep<i>/{live_state.csv, retained_bytes.txt (GeneaLog), job.json}
and <results dir>/expected_outputs.txt ("dataflow outputs" lines).

Checks, per unit: the live total returns to 0 (every add matched by a clear, logical and wall-time series), the sink saw
the expected number of outputs (the same for the three methods), and over repetitions the logical numbers are identical.

Writes
  live_memory.csv          dataflow, method, retained_bytes, peak_live_bytes, peak_live_full_bytes, peak_live_wall_bytes,
                           graph_reachable_bytes, sink_outputs, reps; noprov rows are 0 (it keeps no annotation).
                           peak_live_bytes is the core model (what the window operators hold in the method's own annotation:
                           CAPS, Green; GeneaLog: two pointers per aggregate window and 32 B per buffered tuple copy),
                           peak_live_full_bytes adds, for GeneaLog, the tuples that its aggregate chains keep alive and the
                           provenance sets that its tuples carry after a network exchange (equal to the core for CAPS and Green)
  live_memory_ops.csv      the operators' share of the peak: dataflow, method, operator, bytes_at_peak
  live_memory_table.csv    the same in MB (10^6 B), one row per dataflow, a column per method
  live_memory_table.tex    the table for the paper: retained (analytical) and peak live annotation state (instrumented model bytes, event-time schedule), MB, per dataflow and method
  table_a_check.csv        (full data) retained MB against the retained column of Table 2
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import common  # noqa: E402

MB = 1e6
METHODS = ("genealog", "green_polynomial", "caps")
KEY = {"genealog": "genealog", "green_polynomial": "green", "caps": "caps"}

# Retained memory in MB as reported in Table 2: (CAPS, Green, GeneaLog) per dataflow.
TABLE_A = {
    "nexmark_1": (5.5, 10.9, 112.8), "nexmark_2": (0.8, 0.8, 19.5), "taxi_1": (150.2, 126.8, 692.4),
    "taxi_2": (215.4, 17124.4, 780.7), "twitter_1": (318.4, 409.9, 699.7), "twitter_2": (9.5, 8.5, 24.6),
}


def read_unit(path):
    """{name: value} of a live_state.csv."""
    return {r["name"]: int(r["value"]) for r in common.read_csv(path)}


def check(unit, name, expected_outputs, failures):
    """The invariants of one unit; appends the failed ones to failures."""
    if unit["logical_end_total"] != 0:
        failures.append("%s: logical end total %d != 0" % (name, unit["logical_end_total"]))
    if unit["phys_end_total"] != 0:
        failures.append("%s: wall-time end total %d != 0" % (name, unit["phys_end_total"]))
    if unit["elements"] == 0:
        failures.append("%s: no element reached an instrumented window (is CAPS_LIVE_STATE=1 set?)" % name)
    if unit["adds"] != 0 and unit["clears"] == 0:
        failures.append("%s: bytes were added but never cleared" % name)
    if expected_outputs is not None and unit["sink_records"] != expected_outputs:
        failures.append("%s: %d sink outputs, expected %d" % (name, unit["sink_records"], expected_outputs))


def operators(unit):
    return {k[len("logical_at_peak."):]: v for k, v in unit.items() if k.startswith("logical_at_peak.")}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path)
    parser.add_argument("--reps", type=int, default=1)
    args = parser.parse_args()
    out = args.results

    expected = {}
    if (out / "expected_outputs.txt").is_file():
        for line in (out / "expected_outputs.txt").read_text().split("\n"):
            if line.strip():
                d, n = line.split()
                expected[d] = int(n)
    small = out.name.startswith("results_small")

    dataflows = [d for d in common.DATAFLOWS if (out / d).is_dir()]
    failures = []
    rows, op_rows = [], []
    for d in dataflows:
        sink_counts = {}
        for method in METHODS:
            rep_dirs = sorted((out / d / method).glob("rep*"))
            if not rep_dirs:
                failures.append("%s %s: no unit" % (d, method))
                continue
            units = []
            for rd in rep_dirs:
                u = read_unit(rd / "live_state.csv")
                check(u, "%s %s %s" % (d, method, rd.name), expected.get(d), failures)
                units.append(u)
            for u in units[1:]:      # logical numbers are a function of the data only
                for k in ("logical_peak_total", "logical_peak_core", "sink_records", "elements", "adds", "clears"):
                    if u[k] != units[0][k]:
                        failures.append("%s %s: %s differs between repetitions (%d vs %d)"
                                        % (d, method, k, units[0][k], u[k]))
            u = units[0]
            sink_counts[method] = u["sink_records"]
            if method == "genealog":
                retained = int((rep_dirs[0] / "retained_bytes.txt").read_text().strip())
                graph = u.get("genealog_reachable_nodes", 0) * 32
            else:
                retained, graph = u["sink_bytes"], ""
            wall = sorted(x["phys_peak_total"] for x in units)[len(units) // 2]
            rows.append([d, common.HOWMUCH_CSV_METHOD[method], retained, u["logical_peak_core"], u["logical_peak_total"],
                         wall, graph, u["sink_records"], len(units)])
            for op, b in sorted(operators(u).items()):
                op_rows.append([d, common.HOWMUCH_CSV_METHOD[method], op, b])
        if len(set(sink_counts.values())) > 1:
            failures.append("%s: sink outputs differ between methods: %s" % (d, sink_counts))
        rows.append([d, "noprov", 0, 0, 0, 0, "", next(iter(sink_counts.values()), ""), 1])

    header = ["dataflow", "method", "retained_bytes", "peak_live_bytes", "peak_live_full_bytes", "peak_live_wall_bytes",
              "graph_reachable_bytes", "sink_outputs", "reps"]
    common.write_csv(out / "live_memory.csv", header, rows)
    common.write_csv(out / "live_memory_ops.csv", ["dataflow", "method", "operator", "bytes_at_peak"], op_rows)

    by = {(r[0], r[1]): r for r in rows}
    table = []
    for d in dataflows:
        row = [d]
        for m in ("genealog", "green", "caps"):
            r = by.get((d, m))
            row += ["%.3f" % (r[2] / MB), "%.6f" % (r[3] / MB)] if r else ["", ""]
        g = by.get((d, "genealog"))
        row.append("%.6f" % (g[4] / MB) if g else "")
        row.append("%.3f" % (g[6] / MB) if g and g[6] != "" else "")
        table.append(row)
    thead = ["dataflow", "genealog_retained_MB", "genealog_live_MB", "green_retained_MB", "green_live_MB",
             "caps_retained_MB", "caps_live_MB", "genealog_live_full_MB", "genealog_graph_reachable_MB"]
    common.write_csv(out / "live_memory_table.csv", thead, table)

    def tex_mb(x):
        x = float(x)
        return "0" if x == 0 else ("%.1f" % x if x >= 10 else ("%.2f" % x if x >= 1 else "%.3f" % x))
    tex = [[common.tex_name(r[0])] + [tex_mb(r[i]) for i in (1, 3, 5, 2, 4, 6)] for r in table]
    common.write_latex_table(out / "live_memory_table.tex",
                             ["Workflow", "GeneaLog", "Green", "CAPS", "GeneaLog", "Green", "CAPS"], tex, align="lrrrrrr")

    if not small:
        check_rows = []
        for d in dataflows:
            for i, m in enumerate(("caps", "green", "genealog")):
                if (d, m) in by:
                    got = by[(d, m)][2] / MB
                    check_rows.append([d, m, TABLE_A[d][i], "%.1f" % got, "%.2f" % (100.0 * (got / TABLE_A[d][i] - 1))])
        common.write_csv(out / "table_a_check.csv", ["dataflow", "method", "table_a_MB", "measured_MB", "diff_percent"],
                         check_rows)

    for r in table:
        print("  ".join("%-12s" % c for c in r))
    if failures:
        print("\nFAIL")
        for f in failures:
            print("  " + f)
        sys.exit(1)
    print("\nPASS: live totals return to 0, sink outputs match across methods%s" %
          (", repetitions identical" if args.reps > 1 else ""))

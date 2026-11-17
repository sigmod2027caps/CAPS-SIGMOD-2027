#!/usr/bin/env python3
"""CSV tables and LaTeX tabular snippets of the experiments the paper reports as tables.

  tables.py query_table  experiments/query_table   per-output query: JMH and in-situ, three methods (Figure 9a)
  tables.py expiry       experiments/expiry        expiry with a 7-day horizon (Table 4)
  tables.py summarize    experiments/summarize     online summarization (Table 4)
  tables.py memory       experiments/memory        retained meta-data (analytical) and probed volume
  tables.py runtime      experiments/how_much      runtime, retained memory and per-output query in one table


Each writes <name>.csv and <name>.tex next to the inputs. Sizes are MB = 10^6 bytes. A dataflow that has not been
run is left out. The directory may also be the small/ subdirectory of an experiment (SMALL=1 results): the results of
another experiment that a table reads (how_much) are then taken from its small/ as well.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common  # noqa: E402

MB = 1e6


def sibling(expdir, name):
    """The results directory of experiment `name` that goes with expdir (both full, or both in small/)."""
    if expdir.name == "small":
        return expdir.parent.parent / name / "small"
    return expdir.parent / name


def dataflows_in(rows):
    present = {r["dataflow"] for r in rows}
    return [d for d in common.DATAFLOWS if d in present]


def count(n):
    n = float(n)
    if n >= 1e6:
        return "%.1fM" % (n / 1e6)
    if n >= 1e3:
        return "%.0fK" % (n / 1e3) if n >= 1e5 else "%.1fK" % (n / 1e3)
    return "%d" % n


def mb(b):
    v = float(b) / MB
    return "%.1f" % v if v >= 10 else "%.2f" % v


def label_ms(ms):
    ms = int(float(ms))
    if ms == 0:
        return "exact"
    for unit, name in ((86400000, "d"), (3600000, "h"), (60000, "min")):
        if ms % unit == 0:
            return "%d%s" % (ms // unit, name)
    return "%.1fh" % (ms / 3600000.0)


def pm(score, err, digits=2):
    return "$%.*f \\pm %.*f$" % (digits, float(score), digits, float(err))


def query_table(expdir):
    jmh = common.read_csv(expdir / "query_table.csv")
    in_situ = {}
    stats_path = sibling(expdir, "how_much") / "times_stats.csv"
    cal_path = sibling(expdir, "how_much") / "query_ns.csv"
    if stats_path.is_file() and cal_path.is_file():
        for r in common.read_csv(stats_path):
            in_situ[(r["dataflow"], r["method"])] = r
        for r in common.read_csv(cal_path):
            in_situ.setdefault((r["dataflow"], r["method"]), {}).update(
                {"calibrated_ns": r["calibrated_ns"], "timer_ns": r["timer_ns"],
                 "calibrated_sample_median_ns": r.get("calibrated_sample_median_ns", "")})
    header = ["dataflow", "method", "channels", "ids", "jmh_ns", "jmh_ci_ns", "insitu_median_ns", "insitu_min_ns",
              "insitu_max_ns", "insitu_calibrated_ns", "insitu_sample_median_ns", "insitu_sample_median_calibrated_ns"]
    rows = []
    methods = (("genealog", "genealog"), ("green", "green_polynomial"), ("caps", "caps"))
    for d in dataflows_in(jmh):
        for m, how_much_name in methods:
            r = next(x for x in jmh if x["dataflow"] == d and x["method"] == m)
            s = in_situ.get((d, how_much_name), {})
            rows.append([d, m, r["channels"], r["ids"], r["score_ns"], r["error_ns"],
                         s.get("prov_query_median_ns", ""), s.get("prov_query_min_ns", ""),
                         s.get("prov_query_max_ns", ""), s.get("calibrated_ns", ""),
                         s.get("prov_query_sample_median_ns", ""), s.get("calibrated_sample_median_ns", "")])
    common.write_csv(expdir / "query_table_summary.csv", header, rows)
    # LaTeX: JMH +- CI, then the in-situ median, GeneaLog | Green | CAPS
    tex = []
    for d in dataflows_in(jmh):
        cells = [common.tex_name(d)]
        sel = {r[1]: r for r in rows if r[0] == d}
        for m, _ in methods:
            cells.append(pm(sel[m][4], sel[m][5]))
        for m, _ in methods:
            v = sel[m][6]
            cells.append("%.0f" % float(v) if v != "" else "--")
        tex.append(cells)
    common.write_latex_table(
        expdir / "query_table.tex",
        ["Workflow", "GeneaLog", "Green", "CAPS", "GeneaLog", "Green", "CAPS"], tex, "lrrrrrr")
    print("wrote %s, %s" % (expdir / "query_table_summary.csv", expdir / "query_table.tex"))


def expiry(expdir):
    rows = common.read_csv(expdir / "expiry_stats.csv")
    header = ["dataflow", "index", "records", "kept_pct", "memory_MB", "expiry_us", "query_us"]
    out, tex = [], []
    for d in dataflows_in(rows):
        r = next(x for x in rows if x["dataflow"] == d)
        total, kept = int(float(r["records_total"])), int(float(r["records_retained"]))
        out.append([d, "never expire", total, "100", "%.3f" % (float(r["full_bytes"]) / MB), "", r["full_query_us"]])
        out.append([d, "7-day horizon", kept, "%.1f" % (100.0 * kept / total), "%.3f" % (float(r["expiring_bytes"]) / MB),
                    r["avg_expiry_us"], r["expiring_query_us"]])
        tex.append(["\\multirow{2}{*}{%s}" % common.tex_name(d), "never expire", count(total), "100\\%",
                    mb(r["full_bytes"]), "n/a", "%.2f" % float(r["full_query_us"])])
        tex.append(["", "7-day horizon", count(kept), "%.1f\\%%" % (100.0 * kept / total), mb(r["expiring_bytes"]),
                    "%.1f" % float(r["avg_expiry_us"]), "%.2f" % float(r["expiring_query_us"])])
    common.write_csv(expdir / "expiry_table.csv", header, out)
    common.write_latex_table(expdir / "expiry_table.tex",
                             ["", "Index", "Records", "Kept", "Memory (MB)", "Expiry ($\\mu$s)", "Query ($\\mu$s)"],
                             tex, "llrrrrr")
    print("wrote %s, %s" % (expdir / "expiry_table.csv", expdir / "expiry_table.tex"))


def summarize(expdir):
    rows = common.read_csv(expdir / "summarize.csv")
    header = ["dataflow", "bucket", "records_per_bucket", "exact_MB", "summary_KB", "query_us", "avg_error_pct",
              "max_error_pct"]
    out, tex = [], []
    for d in dataflows_in(rows):
        mine = [r for r in rows if r["dataflow"] == d]
        for i, r in enumerate(mine):
            exact = label_ms(r["bucket_ms"]) == "exact"
            out.append([d, label_ms(r["bucket_ms"]), r["avg_tuples_per_bucket"], "%.3f" % (float(r["exact_bytes"]) / MB),
                        "%.1f" % (float(r["summary_bytes"]) / 1e3), r["avg_query_us"], r["avg_rel_err_pct"],
                        r["max_rel_err_pct"]])
            tex.append([("\\multirow{%d}{*}{%s}" % (len(mine), common.tex_name(d))) if i == 0 else "",
                        label_ms(r["bucket_ms"]), "--" if exact else count(float(r["avg_tuples_per_bucket"])),
                        mb(r["exact_bytes"]), "0" if exact else "%.1fKB" % (float(r["summary_bytes"]) / 1e3),
                        "%.2f" % float(r["avg_query_us"]), "%.1f\\%%" % float(r["avg_rel_err_pct"])])
    common.write_csv(expdir / "summarize_table.csv", header, out)
    common.write_latex_table(expdir / "summarize_table.tex",
                             ["Dataflow", "Bucket", "Rec./bkt", "Exact (MB)", "Summary", "Query ($\\mu$s)", "Error"],
                             tex, "llrrrrr")
    print("wrote %s, %s" % (expdir / "summarize_table.csv", expdir / "summarize_table.tex"))


def memory(expdir):
    """Retained meta-data (analytical, the same model as experiments/how_much: CAPS 8 bytes per channel per output,
    Green 8 bytes per variable of every output's polynomial, GeneaLog 32 bytes per emitted tuple) next to the
    cumulative volume that the per-operator probes count."""
    model = sibling(expdir, "how_much") / "memory_model.csv"
    retained = common.read_csv(model) if model.is_file() else []
    volume = common.read_csv(expdir / "metadata_volume.csv")
    names = {"green_polynomial": "green", "genealog": "genealog", "caps": "caps"}
    header = ["dataflow", "method", "retained_MB", "probed_volume_MB", "probed_records"]
    rows = []
    for d in common.DATAFLOWS:
        for m in ("genealog", "green_polynomial", "caps"):
            ret = next((r for r in retained if r["dataflow"] == d and r["method"] == m), None)
            vol = next((r for r in volume if r["dataflow"] == d and r["method"] == m), None)
            if ret is None and vol is None:
                continue
            rows.append([d, names[m], "%.3f" % (float(ret["bytes"]) / MB) if ret else "",
                         "%.3f" % (float(vol["bytes"]) / MB) if vol else "", vol["records"] if vol else ""])
    common.write_csv(expdir / "memory_table.csv", header, rows)
    tex = []
    for d in dataflows_in([{"dataflow": r[0]} for r in rows]):
        sel = {r[1]: r for r in rows if r[0] == d}
        cells = [common.tex_name(d)]
        for col in (2, 3):
            for m in ("genealog", "green", "caps"):
                v = sel.get(m, [None] * 5)[col]
                cells.append(mb(float(v) * MB) if v not in (None, "") else "--")
        tex.append(cells)
    common.write_latex_table(expdir / "memory_table.tex",
                             ["Workflow", "GeneaLog", "Green", "CAPS", "GeneaLog", "Green", "CAPS"], tex, "lrrrrrr")
    print("wrote %s, %s" % (expdir / "memory_table.csv", expdir / "memory_table.tex"))


def runtime(expdir):
    """Runtime (median [min-max] s), GC pause time, retained memory (MB) and in-situ query (ns) of the methods, with noprov."""
    stats = common.read_csv(expdir / "times_stats.csv")
    mem = {(r["dataflow"], r["method"]): r for r in common.read_csv(expdir / "memory_model.csv")} \
        if (expdir / "memory_model.csv").is_file() else {}
    header = ["dataflow", "method", "runtime_median_s", "runtime_min_s", "runtime_max_s", "runtime_stddev_s", "reps",
              "overhead_vs_noprov_pct", "overlaps_noprov", "retained_MB", "insitu_query_ns",
              "gc_median_s", "gc_min_s", "gc_max_s", "gc_pauses", "gc_full_pauses", "gc_pct_of_runtime"]
    rows = []
    csv_methods = ("noprov", "genealog", "green_polynomial", "caps")
    for d in dataflows_in(stats):
        np_row = next(r for r in stats if r["dataflow"] == d and r["method"] == "noprov")
        base = float(np_row["job_duration_median_s"])
        np_lo, np_hi = float(np_row["job_duration_min_s"]), float(np_row["job_duration_max_s"])
        for m in csv_methods:
            r = next((x for x in stats if x["dataflow"] == d and x["method"] == m), None)
            if r is None:
                continue
            med = float(r["job_duration_median_s"])
            retained = mem.get((d, m))
            # the min-max intervals of the repetitions overlap: the difference to No Prov is within the run-to-run spread
            overlap = float(r["job_duration_min_s"]) <= np_hi and float(r["job_duration_max_s"]) >= np_lo
            gc = r.get("gc_median_s", "")
            gc_pct = "%.1f" % (100.0 * float(gc) / med) if gc != "" else ""
            rows.append([d, common.HOWMUCH_CSV_METHOD.get(m, m), r["job_duration_median_s"], r["job_duration_min_s"],
                         r["job_duration_max_s"], r["job_duration_stddev_s"], r["n"], "%.1f" % (100.0 * (med / base - 1)),
                         "yes" if overlap else "no", "%.3f" % (float(retained["bytes"]) / MB) if retained else "", r["prov_query_median_ns"],
                         gc, r.get("gc_min_s", ""), r.get("gc_max_s", ""), r.get("gc_pauses_median", ""),
                         r.get("gc_full_pauses_median", ""), gc_pct])
    common.write_csv(expdir / "runtime_table.csv", header, rows)
    tex = []
    gc_tex = []
    for d in dataflows_in(stats):
        sel = {r[1]: r for r in rows if r[0] == d}
        cells = [common.tex_name(d), "%.2f" % float(sel["noprov"][2])]
        for m in ("genealog", "green", "caps"):
            cells.append("%.2f [%.2f--%.2f]" % (float(sel[m][2]), float(sel[m][3]), float(sel[m][4])))
        for m in ("genealog", "green", "caps"):
            cells.append(mb(float(sel[m][9]) * MB) if sel[m][9] != "" else "--")
        tex.append(cells)
        if sel["noprov"][11] != "":
            gc_tex.append([common.tex_name(d)] + [
                "%.2f / %.2f" % (float(sel[m][2]), float(sel[m][11])) for m in ("noprov", "genealog", "green", "caps")]
)
    common.write_latex_table(expdir / "runtime_table.tex",
                             ["Workflow", "No Prov", "GeneaLog", "Green", "CAPS", "GeneaLog", "Green", "CAPS"],
                             tex, "lrrrrrrr")
    print("wrote %s, %s" % (expdir / "runtime_table.csv", expdir / "runtime_table.tex"))
    if gc_tex:
        common.write_latex_table(expdir / "runtime_gc_table.tex",
                                 ["Workflow", "No Prov", "GeneaLog", "Green", "CAPS"],
                                 gc_tex, "l" + "r" * (len(gc_tex[0]) - 1))
        print("wrote %s (cells: median runtime / median GC pause time, s)" % (expdir / "runtime_gc_table.tex"))


if __name__ == "__main__":
    commands = {"query_table": query_table, "expiry": expiry, "summarize": summarize, "memory": memory,
                "runtime": runtime}
    if len(sys.argv) != 3 or sys.argv[1] not in commands:
        raise SystemExit(__doc__)
    commands[sys.argv[1]](Path(sys.argv[2]).resolve())

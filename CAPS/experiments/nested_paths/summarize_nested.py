#!/usr/bin/env python3
"""results/nested.csv of the nested-path test, from the work directory of run.sh.

One row per M: m, outputs, noprov_ms (median, min, max), caps_ms (median, min, max), runtime_rel_noprov
(median caps / median noprov), retained_MB (analytical: 8 B x outputs x M, MB = 1e6 B),
query_total_ns / query_vector_ns (JMH score, with error), verification status.

Usage: summarize_nested.py --work DIR --out DIR --m-values M...
"""
import argparse
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import common  # noqa: E402


def times(paths):
    return [int((p / "job_ms.txt").read_text()) for p in sorted(paths) if (p / "job_ms.txt").is_file()]


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--work", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--m-values", type=int, nargs="+", required=True)
    a = ap.parse_args()
    npv = times((a.work / "noprov" / "time").glob("rep*"))
    rows = []
    for M in sorted(a.m_values):
        w = a.work / ("m%d" % M)
        caps = times((w / "time").glob("caps_rep*"))
        outputs = ""
        stats = w / "verify" / "stats.json"
        if stats.is_file():
            import json
            outputs = json.loads(stats.read_text())["outputs"]
        q = {}
        for kind in ("total", "vector"):
            f = w / "jmh" / ("caps_%s.csv" % kind)
            r = common.read_csv(f)[0] if f.is_file() else None
            q[kind] = ("%.2f" % float(r["score_ns"]), "%.2f" % float(r["error_ns"])) if r else ("", "")
        verified = "PASS" if (w / "verify" / "PASS").is_file() else "not verified"
        rel = "%.3f" % (statistics.median(caps) / statistics.median(npv)) if caps and npv else ""
        retained = "%.3f" % (8 * M * outputs / 1e6) if outputs != "" else ""
        rows.append([M, outputs,
                     statistics.median(npv) if npv else "", min(npv) if npv else "", max(npv) if npv else "",
                     statistics.median(caps) if caps else "", min(caps) if caps else "", max(caps) if caps else "",
                     len(caps), rel, retained, q["total"][0], q["total"][1], q["vector"][0], q["vector"][1], verified])
    common.write_csv(a.out / "nested.csv",
                     ["m", "outputs", "noprov_ms", "noprov_min_ms", "noprov_max_ms", "caps_ms", "caps_min_ms", "caps_max_ms",
                      "reps", "runtime_rel_noprov", "retained_MB", "query_total_ns", "query_total_err_ns",
                      "query_vector_ns", "query_vector_err_ns", "verification"], rows)
    print("  nested.csv: %d rows (noprov reps: %s ms)" % (len(rows), npv))

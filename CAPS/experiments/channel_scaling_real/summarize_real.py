#!/usr/bin/env python3
"""CSVs of the real-data channel-scalability experiment, from the work directory of run.sh.

  runtime.csv       dataset, M, method, median_s, min_s, max_s, reps, outputs           (Flink job runtime)
  memory.csv        dataset, M, method, outputs, stored_items, bytes_per_output, retained_MB   (analytical)
  stats.csv         dataset, M, records, outputs, records_per_output, nonzero_channels_per_output
  query.csv         dataset, M, method, kind, score_ns, error_ns, fixture_size, checksum   (JMH, ns per query)
  verification.csv  dataset, M, check, status, outputs, mismatches, detail

Retained meta-data (analytical, MB = 1e6 B), per dataset and M:
  caps (dense)   8 B x M per output                                (stored items: M x outputs counters)
  caps_sparse    12 B per stored (channel, count) pair              (4 B channel id + 8 B count)
  green          8 B per variable of the output's monomial          (one variable per contributing record)
  genealog       32 B per tuple an operator emitted and that stays reachable (source tuples + window outputs)
The caps / sparse / green byte counts are checked against the metadataBytes column the jobs wrote.

Usage: summarize_real.py --work DIR --out DIR --datasets D... --m-values M... --methods METHOD...
"""
import argparse
import json
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import common  # noqa: E402

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--work", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--datasets", nargs="+", required=True)
    ap.add_argument("--m-values", type=int, nargs="+", required=True)
    ap.add_argument("--methods", nargs="+", required=True)
    a = ap.parse_args()
    runtime, memory, stats_rows, query, verification = [], [], [], [], []
    for ds in sorted(a.datasets):
        for M in sorted(a.m_values):
            w = a.work / ("%s_m%d" % (ds, M))
            sf = w / "verify" / "stats.json"
            if sf.is_file():
                s = json.loads(sf.read_text())
                n, rec = s["outputs"], s["records"]
                assert s["caps_bytes"] == 8 * M * n and s["sparse_bytes"] == 12 * s["sparse_pairs"] == 12 * s["oracle_pairs"] \
                    and s["green_bytes"] == 8 * rec, "metadata bytes of the jobs disagree with the formulas"
                stats_rows.append([ds, M, rec, n, "%.4f" % (rec / n), "%.4f" % (s["oracle_pairs"] / n)])
                for method, items, nbytes in (("genealog", s["genealog_tuples"], 32 * s["genealog_tuples"]),
                                              ("green", rec, 8 * rec),
                                              ("caps", M * n, 8 * M * n),
                                              ("caps_sparse", s["oracle_pairs"], 12 * s["oracle_pairs"])):
                    memory.append([ds, M, method, n, items, "%.3f" % (nbytes / n), "%.3f" % (nbytes / 1e6)])
            vf = w / "verify" / "verify.csv"
            if vf.is_file():
                verification += [[r["dataset"], r["M"], r["check"], r["status"], r["outputs"], r["mismatches"], r["detail"]]
                                 for r in common.read_csv(vf)]
            for method in a.methods:
                ms = [int((p / "job_ms.txt").read_text()) for p in sorted((w / "time").glob(method + "_rep*"))
                      if (p / "job_ms.txt").is_file()]
                if ms:
                    t = [x / 1000 for x in ms]
                    outs = int((w / "time" / (method + "_rep1") / "outputs.txt").read_text())
                    runtime.append([ds, M, method, "%.3f" % statistics.median(t), "%.3f" % min(t), "%.3f" % max(t), len(t), outs])
            for f in sorted((w / "jmh").glob("*.csv")) if (w / "jmh").is_dir() else []:
                r = common.read_csv(f)[0]
                method = f.name.rsplit("_", 1)[0]
                query.append([ds, M, method, r["kind"], "%.2f" % float(r["score_ns"]), "%.2f" % float(r["error_ns"]),
                              r["fixture_size"], r["checksum"]])
    a.out.mkdir(parents=True, exist_ok=True)
    common.write_csv(a.out / "runtime.csv", ["dataset", "M", "method", "median_s", "min_s", "max_s", "reps", "outputs"], runtime)
    common.write_csv(a.out / "memory.csv", ["dataset", "M", "method", "outputs", "stored_items", "bytes_per_output", "retained_MB"], memory)
    common.write_csv(a.out / "stats.csv", ["dataset", "M", "records", "outputs", "records_per_output", "nonzero_channels_per_output"], stats_rows)
    common.write_csv(a.out / "query.csv", ["dataset", "M", "method", "kind", "score_ns", "error_ns", "fixture_size", "checksum"], query)
    common.write_csv(a.out / "verification.csv", ["dataset", "M", "check", "status", "outputs", "mismatches", "detail"], verification)
    for name, rows in (("runtime", runtime), ("memory", memory), ("stats", stats_rows), ("query", query), ("verification", verification)):
        print("  %s.csv: %d rows" % (name, len(rows)))

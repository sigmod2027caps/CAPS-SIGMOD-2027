#!/usr/bin/env python3
"""Collects the runs of the recovery experiment (E4) into CSVs.

Reads <work>/<dataflow>/<kind>[_<rep>]/ (kinds: time_plain, time_ckpt, plain, ref, inject, kill; written by run_one.sh)
and <results>/recovery_verify.csv (verify_recovery.py). Writes to <results>:
  recovery_runs.csv     one row per run: duration, checkpoints (count, duration, Flink-managed state size), Tier-2 segment
                        bytes, what a full snapshot would have written, failure and recovery times, duplicates
  recovery_summary.csv  one row per dataflow: runtime without and with checkpointing (median of the repetitions, min, max,
                        overhead), the checkpoint cost, and for the injected failure and the killed TaskManager the
                        recovery times, the extra job time and the duplicates re-emitted at the sink
"""
import argparse
import json
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import common  # noqa: E402

RUN_HEADER = ["dataflow", "kind", "rep", "ckpt_ms", "duration_s", "checkpoints", "ckpt_failed", "ckpt_duration_avg_ms",
              "ckpt_duration_max_ms", "state_size_avg_bytes", "state_size_max_bytes", "segment_bytes_total",
              "full_snapshot_bytes_total", "sink_snapshot_ms_total", "restores", "restore_ms", "restored_checkpoint",
              "detect_ms", "restart_ms", "outage_ms", "raw_outputs", "duplicates", "status"]
SUMMARY_HEADER = ["dataflow", "outputs", "timing_ckpt_ms", "plain_s", "plain_min_s", "plain_max_s", "ckpt_s", "ckpt_min_s",
                  "ckpt_max_s", "overhead_pct", "reps", "paced_s", "ckpt_ms", "checkpoints", "ckpt_duration_avg_ms",
                  "ckpt_duration_max_ms", "state_size_avg_bytes", "segment_bytes_total", "full_snapshot_bytes_total",
                  "sink_snapshot_ms_total"]
FAIL_COLUMNS = ["extra_s", "detect_ms", "restart_ms", "outage_ms", "restore_ms", "restored_checkpoint", "duplicates", "status"]


def load(path):
    p = Path(path)
    return json.loads(p.read_text()) if p.is_file() else None


def keyvals(path):
    out = {}
    for line in Path(path).read_text().splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            out[k] = v
    return out


def run_row(dataflow, kind, rep, d, verify):
    info = keyvals(d / "run.txt")
    job = load(d / "job.json") or {}
    ck = load(d / "checkpoints.json") or {}
    exc = load(d / "exceptions.json") or {}
    row = dict.fromkeys(RUN_HEADER, "")
    restored_at = 0
    row.update(dataflow=dataflow, kind=kind, rep=rep, ckpt_ms=info.get("ckpt_ms", ""))
    row["duration_s"] = job["duration"] / 1000.0 if "duration" in job else ""
    done = [h for h in ck.get("history", []) if h.get("status") == "COMPLETED"]
    if ck:
        counts = ck.get("counts", {})
        row["checkpoints"], row["ckpt_failed"] = counts.get("completed", 0), counts.get("failed", 0)
    if done:
        durations = [h["end_to_end_duration"] for h in done]
        sizes = [h["state_size"] for h in done]
        row.update(ckpt_duration_avg_ms=statistics.mean(durations), ckpt_duration_max_ms=max(durations),
                   state_size_avg_bytes=statistics.mean(sizes), state_size_max_bytes=max(sizes))
    stats = (d / "recovery_stats.csv")
    if stats.is_file():
        rows = common.read_csv(stats)
        row["segment_bytes_total"] = sum(int(r["segment_bytes_total"]) for r in rows)
        row["full_snapshot_bytes_total"] = sum(int(r["full_snapshot_bytes_total"]) for r in rows)
        row["sink_snapshot_ms_total"] = sum(float(r["snapshot_ms"]) for r in rows)
        row["restores"] = max(int(r["restores"]) for r in rows)
        row["restore_ms"] = max(float(r["restore_ms_last_execution"]) for r in rows)
        restored_at = max(int(r["restored_at_ms"]) for r in rows)
    restored = (ck.get("latest") or {}).get("restored") or {}
    if restored:
        row["restored_checkpoint"] = restored.get("id", "")
    t_kill = int(info.get("t_kill_ms", 0))
    if t_kill and exc.get("timestamp"):
        row["detect_ms"] = exc["timestamp"] - t_kill
    if restored and exc.get("timestamp") and stats.is_file() and restored_at:
        # detect: kill -> the JobManager notices; restart: failure -> the restored sink is open (restart delay, slots of
        # the replacement TaskManager, deployment, state restore); outage: kill (or the exception) -> the restored sink is open
        row["restart_ms"] = restored_at - exc["timestamp"]
        row["outage_ms"] = restored_at - (t_kill or exc["timestamp"])
    v = verify.get((dataflow, kind))
    if v:
        row["raw_outputs"], row["duplicates"], row["status"] = v["raw_outputs"], v["duplicates"], v["status"]
    return row


def med(values):
    return statistics.median(values) if values else ""


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--work", required=True)
    ap.add_argument("--results", required=True)
    ap.add_argument("--outputs", default="", help="csv: dataflow,outputs (sink outputs of each dataflow)")
    ap.add_argument("--timing-ckpt-ms", default="", help="csv: dataflow,ms (interval of the timing runs)")
    args = ap.parse_args()
    work, results = Path(args.work), Path(args.results)

    verify_path = results / "recovery_verify.csv"
    verify = {(r["dataflow"], r["run"]): r for r in common.read_csv(verify_path)} if verify_path.is_file() else {}
    outputs = {r["dataflow"]: r["outputs"] for r in common.read_csv(args.outputs)} if args.outputs else {}
    timing_ms = {r["dataflow"]: r["ms"] for r in common.read_csv(args.timing_ckpt_ms)} if args.timing_ckpt_ms else {}

    runs, summary = [], []
    for dataflow in common.DATAFLOWS:
        base = work / dataflow
        if not base.is_dir():
            continue
        by_kind = {}
        for d in sorted(base.iterdir()):
            if not (d / "run.txt").is_file():
                continue
            kind, rep = d.name.rsplit("_", 1) if d.name.startswith("time_") else (d.name, "")
            row = run_row(dataflow, kind, rep, d, verify)
            runs.append([row[h] for h in RUN_HEADER])
            by_kind.setdefault(kind, []).append(row)
        if not by_kind:
            continue
        s = dict.fromkeys(SUMMARY_HEADER, "")
        s["dataflow"], s["outputs"], s["timing_ckpt_ms"] = dataflow, outputs.get(dataflow, ""), timing_ms.get(dataflow, "")
        plain = [r["duration_s"] for r in by_kind.get("time_plain", []) if r["duration_s"] != ""]
        ckpt = [r["duration_s"] for r in by_kind.get("time_ckpt", []) if r["duration_s"] != ""]
        if plain and ckpt:
            s.update(plain_s=med(plain), plain_min_s=min(plain), plain_max_s=max(plain), ckpt_s=med(ckpt),
                     ckpt_min_s=min(ckpt), ckpt_max_s=max(ckpt), reps=len(plain),
                     overhead_pct=100.0 * (med(ckpt) - med(plain)) / med(plain))
        ref = (by_kind.get("ref") or [None])[0]
        if ref:
            s.update(ckpt_ms=ref["ckpt_ms"], paced_s=ref["duration_s"], checkpoints=ref["checkpoints"],
                     ckpt_duration_avg_ms=ref["ckpt_duration_avg_ms"], ckpt_duration_max_ms=ref["ckpt_duration_max_ms"],
                     state_size_avg_bytes=ref["state_size_avg_bytes"], segment_bytes_total=ref["segment_bytes_total"],
                     full_snapshot_bytes_total=ref["full_snapshot_bytes_total"],
                     sink_snapshot_ms_total=ref["sink_snapshot_ms_total"])
        for kind in ("inject", "kill"):
            r = (by_kind.get(kind) or [None])[0]
            for c in FAIL_COLUMNS:
                s[kind + "_" + c] = ""
            if r:
                s[kind + "_extra_s"] = r["duration_s"] - ref["duration_s"] if ref and r["duration_s"] != "" else ""
                for c in FAIL_COLUMNS[1:]:
                    s[kind + "_" + c] = r[c]
        summary.append(s)

    header = SUMMARY_HEADER + [k + "_" + c for k in ("inject", "kill") for c in FAIL_COLUMNS]
    common.write_csv(results / "recovery_runs.csv", RUN_HEADER, [[common.fmt(x) if isinstance(x, (int, float)) else x for x in r] for r in runs])
    common.write_csv(results / "recovery_summary.csv", header,
                     [[common.fmt(s[h]) if isinstance(s[h], (int, float)) else s[h] for h in header] for s in summary])
    print("wrote %d runs and %d dataflows to %s" % (len(runs), len(summary), results))

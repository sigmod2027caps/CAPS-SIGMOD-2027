#!/usr/bin/env python3
"""Aggregates the raw runs of experiments/disorder/run.sh into the CSVs of experiment E3.

work_<tag>/<dataflow>/<cfg>/
    config.txt            of the measurement run (dataflow, p, D, delta, slow, zipf, sink parallelism, ...)
    meas/                 one untimed CAPS run with traces (answers, occupancy, finality, late records, latencies)
    time_noprov_<r>/, time_caps_<r>/   net runtime of REPS timing runs
    file_caps/caps.out    (SMALL only, delta >= D) every output with its counters, to compare with the reference
work_<tag>/<dataflow>/ref_z<zipf>/   the ordered reference run (D = 0, delta = 0, p = 1) of a Zipf setting

Writes to --out
    e3_runs.csv         one row per (dataflow, configuration): configuration, correctness against the reference,
                        Tier-1 occupancy and the analytical bound, delay to finality, late records, query latency,
                        Tier-1 costs, runtimes of noprov and CAPS (median, mean, std over the repetitions)
    e3_correctness.csv  the correctness table (answers and indexed records against the ordered run, late at the sink)
    e3_occupancy.csv    per sink shard: peak Tier-1 occupancy, step gaps, rates, spread and bound
    e3_finality.csv     delay to finality (sink-side, global frontier, event-time lag) with its bounds
    e3_late.csv         late records per operator
    e3_timing.csv       per (dataflow, configuration, method): the repetitions, median, mean, std
    e3_overhead.csv     CAPS against noprov per configuration, with the error of the ratio

Timings and traces come from the standalone cluster (runner = cluster); late-record counts and the repeated correctness runs (late_r*) from the in-process runtime.

Analytical bound of the Tier-1 occupancy of a shard: (watermark interval + inter-subtask watermark spread) x the
sink output rate. The interval is the configured one and the largest gap between two watermarks the shard saw
(Flink fires its periodic timer late under load); the spread is the largest difference between the times at which two
upstream subtasks reached the same watermark; the rate is the shard's average arrival rate, and the largest number
of arrivals in any window of that length (a window emits in bursts, so the average rate undercounts).
"""
import argparse
import csv
import glob
import json
import os
import re
import statistics
import sys
from collections import defaultdict

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "verify"))
import verify  # noqa: E402  (load_vectors, difference: the output comparison of experiments/verify)

CHANNELS = {"nexmark_1": 2, "nexmark_2": 2, "taxi_1": 4, "taxi_2": 2, "twitter_1": 2, "twitter_2": 2}
MIN_LONG = -(2 ** 63)


def read_kv(path):
    out = {}
    with open(path) as f:
        for line in f:
            if "=" in line:
                k, v = line.strip().split("=", 1)
                out[k] = v
    return out


def read_csv(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def load_array(paths, cols):
    """Concatenates the numeric csv files, returns a list of int64 arrays (one per file) with the given columns."""
    arrays = []
    for p in sorted(paths):
        with open(p) as f:
            header = f.readline().strip().split(",")
            idx = [header.index(c) for c in cols]
            data = np.loadtxt(f, delimiter=",", dtype=np.int64, ndmin=2)
        arrays.append(data[:, idx] if data.size else np.zeros((0, len(cols)), dtype=np.int64))
    return arrays


def quantiles(x):
    if len(x) == 0:
        return float("nan"), float("nan"), float("nan")
    return float(np.median(x)), float(np.percentile(x, 99)), float(np.max(x))


# correctness against the ordered reference
def read_answers(path):
    out = {}
    for r in read_csv(path):
        counts = tuple(int(r[k]) for k in r if re.fullmatch(r"c\d+", k))
        out[(r["query"], int(r["i"]))] = (int(r["from"]), int(r["to"]), counts, int(r["defined"]), int(r["final"]))
    return out


def compare_answers(run, ref):
    """Number of queries (3 per i: how-much, the two versioning windows) whose answer differs from the reference's."""
    mismatched = defined_diff = total = 0
    max_abs = 0
    rel = []
    for key, (f, t, counts, defined, fin) in run.items():
        rf, rt, rcounts, rdefined, _ = ref[key]
        total += 1
        if (f, t) != (rf, rt):
            raise ValueError("query windows differ at %s" % (key,))
        if defined != rdefined:
            defined_diff += 1
        if counts != rcounts or defined != rdefined:
            mismatched += 1
            d = max(abs(a - b) for a, b in zip(counts, rcounts))
            max_abs = max(max_abs, d)
            rel.append(sum(abs(a - b) for a, b in zip(counts, rcounts)) / max(sum(rcounts), 1))
    # versioning: delta = w2 - w1 per query index
    ver_total = ver_mismatch = 0
    for (kind, i), (f, t, counts, defined, fin) in run.items():
        if kind != "versioning_w1":
            continue
        c2 = run[("versioning_w2", i)][2]
        r1 = ref[("versioning_w1", i)][2]
        r2 = ref[("versioning_w2", i)][2]
        d_run = tuple(b - a for a, b in zip(counts, c2))
        d_ref = tuple(b - a for a, b in zip(r1, r2))
        ver_total += 1
        ver_mismatch += d_run != d_ref
    return dict(queries=total, answers_differ=mismatched, defined_differ=defined_diff, versioning=ver_total,
                versioning_differ=ver_mismatch, max_abs_count_diff=max_abs,
                mean_rel_diff=float(np.mean(rel)) if rel else 0.0, max_rel_diff=float(np.max(rel)) if rel else 0.0)


def read_stats(meas):
    rows = read_csv(os.path.join(meas, "tiered_stats.csv"))
    tot = lambda k: sum(int(r[k]) for r in rows)
    return dict(
        shards=len(rows), records=tot("records"), indexed=tot("indexed"), late_at_sink=tot("late"),
        migrated=tot("migrated"), peak_max_shard=max(int(r["tier1_peak"]) for r in rows),
        peak_sum_shards=tot("tier1_peak"), tier1_final=tot("tier1_final"), watermarks=tot("watermarks"),
        behind_watermark=tot("behind_watermark"), nonmonotone_appends=tot("nonmonotone_appends"),
        insert_ns_per_rec=tot("insert_ns") / max(tot("indexed"), 1),
        split_ns_per_rec=tot("split_ns") / max(tot("migrated"), 1),
        append_ns_per_rec=tot("tier2_append_ns") / max(tot("migrated"), 1),
        records_per_shard=[int(r["records"]) for r in rows])


def read_latency(meas):
    out = {}
    for r in read_csv(os.path.join(meas, "query_latency.csv")):
        out["%s_%s_us" % (r["method"], r["query"])] = float(r["avg_us"])
    prov = read_csv(os.path.join(meas, "provisional_queries.csv"))
    n = sum(int(r["queries"]) for r in prov)
    out["provisional_batches"] = len(prov)
    out["provisional_queries"] = n
    out["provisional_avg_us"] = sum(float(r["avg_us"]) * int(r["queries"]) for r in prov) / n if n else float("nan")
    out["provisional_max_batch_us"] = max((float(r["avg_us"]) for r in prov), default=float("nan"))
    out["provisional_all_nonfinal"] = int(all(int(r["provisional"]) == int(r["queries"]) for r in prov))
    q = read_csv(os.path.join(meas, "tiered_queries.csv"))
    out["final_queries"] = sum(int(r["final"]) for r in q)
    out["nonfinal_queries"] = sum(int(r["provisional"]) for r in q)
    out["undefined_queries"] = sum(int(r["undefined"]) for r in q)
    return out


# late records
def read_late(meas):
    """Per window operator, summed over subtasks: records in; records dropped because all their windows had fired (the
    side output of an aggregate, Flink's numLateRecordsDropped at a join); records that missed at least one window but
    still reached the newer ones (MissedWindowTrigger); late = both."""
    ops = defaultdict(lambda: dict(inp=0, late=0, side=0, dropped=0, missed=0))
    metrics = os.path.join(meas, "ops_metrics.csv")
    if os.path.exists(metrics):
        for r in read_csv(metrics):
            op = r["operator"]
            v = int(r["value"])
            if r["metric"] == "numRecordsIn" and (op.startswith("AGG-") or op == "JOIN"):
                ops[op]["inp"] += v
            elif r["metric"] == "numLateRecordsDropped" and (op.startswith("AGG-") or op == "JOIN"):
                ops[op]["dropped"] += v
            elif r["metric"] == "recordsMissedWindow" and (op.startswith("AGG-") or op == "JOIN"):
                ops[op]["missed"] += v
            elif r["metric"] == "lateSideOutput" and op.startswith("Sink: LATE-"):
                ops[op[len("Sink: LATE-"):]]["side"] += v
    files = defaultdict(int)
    for p in glob.glob(os.path.join(meas, "late_op_*_s*.csv")):  # the same counts, written by the side-output sinks
        for r in read_csv(p):
            files[r["operator"]] += int(r["late"])
    for op, n in files.items():  # (without the metric reporter, e.g. on a cluster, these are the only counts)
        ops[op]["side"] = max(ops[op]["side"], n)
    for op, d in ops.items():
        d["late"] = d["side"] + d["dropped"] + d["missed"]  # lost entirely, or missed at least one window
    return dict(ops)


def read_sources(meas):
    rows = []
    for p in sorted(glob.glob(os.path.join(meas, "src_stats_*_s*.csv"))):
        rows.extend(read_csv(p))
    emitted = sum(int(r["emitted"]) for r in rows)
    return dict(source_records=emitted, source_violations=sum(int(r["violations"]) for r in rows),
                source_max_disorder_ms=max((int(r["max_disorder_ms"]) for r in rows), default=0),
                idle_marks=sum(int(r["idle_marks"]) for r in rows),
                slept_ms=max((int(r["sleep_ms"]) for r in rows), default=0))


# occupancy, bound, finality
def probe_series(meas):
    """Per upstream subtask: (processing time us, watermark), monotone."""
    return [a for a in load_array(glob.glob(os.path.join(meas, "wm_probe_sink_s*.csv")), ["proc_us", "watermark"]) if len(a)]


def spread_us(series):
    """Largest, 99th percentile and median difference between the times at which two upstream subtasks reached the
    same watermark, over a grid of watermark levels."""
    if len(series) < 2:
        return 0.0, 0.0, 0.0
    lo = max(s[0, 1] for s in series)
    hi = min(s[-1, 1] for s in series)
    if hi <= lo:
        return 0.0, 0.0, 0.0
    levels = np.linspace(lo, hi, 4000)
    times = []
    for s in series:
        idx = np.searchsorted(s[:, 1], levels, side="left")
        idx = np.minimum(idx, len(s) - 1)
        times.append(s[idx, 0])
    times = np.vstack(times)
    sp = times.max(axis=0) - times.min(axis=0)
    return float(sp.max()), float(np.percentile(sp, 99)), float(np.median(sp))


def window_bound(t, a, length):
    """Largest number of arrivals in a window of at least `length` us, from the cumulative arrival curve (t, a)."""
    if len(t) == 0:
        return 0
    j = np.searchsorted(t, t - length, side="right") - 1
    base = np.where(j >= 0, a[np.maximum(j, 0)], 0)
    return int(np.max(a - base))


def occupancy(meas, cfg, spread):
    """One row per sink shard."""
    rows = []
    interval_us = int(cfg["interval"]) * 1000
    steps = load_array(glob.glob(os.path.join(meas, "tier1_steps_index_s*.csv")),
                       ["proc_us", "watermark", "tier1_before", "tier1_after", "arrivals", "migrated"])
    for shard, st in enumerate(steps):
        if len(st) < 2:
            rows.append(dict(shard=shard, steps=len(st)))
            continue
        t, before, after, arr = st[:, 0], st[:, 2], st[:, 3], st[:, 4]
        gaps = np.diff(t)
        dur = max(t[-1] - t[0], 1)
        rate = (arr[-1] - arr[0]) / dur  # arrivals per us
        L_nominal = interval_us + spread
        L_measured = float(gaps.max()) + spread
        rows.append(dict(
            shard=shard, steps=len(st), peak_before=int(before.max()), peak_after=int(after.max()),
            gap_median_ms=float(np.median(gaps)) / 1000, gap_max_ms=float(gaps.max()) / 1000,
            rate_per_s=rate * 1e6, spread_ms=spread / 1000,
            bound_avg_rate_nominal=rate * L_nominal, bound_avg_rate_measured=rate * L_measured,
            bound_window_nominal=window_bound(t, arr, L_nominal), bound_window_measured=window_bound(t, arr, L_measured)))
    return rows


def reader_series(meas):
    """Per reader: (processing time us, newest event time emitted so far), the newest event time non-decreasing."""
    out = []
    for p in sorted(glob.glob(os.path.join(meas, "src_trace_*_s*.csv"))):
        a = load_array([p], ["proc_us", "max_ts"])[0]
        a = a[a[:, 1] > MIN_LONG]
        if len(a):
            out.append((a[:, 0], np.maximum.accumulate(a[:, 1])))
    return out


def watermark_lag(meas):
    """How far the watermark that the window operators see trails the data, in event time (the point of a paced run):
    the median and the 90th percentile of its steps at the sink [s] (the steps of the minimum over the readers of the
    periodic watermarks), and the speed at which the readers move through event time [event ms per processing ms]."""
    steps = []
    for a in load_array(glob.glob(os.path.join(meas, "tier1_steps_index_s*.csv")), ["watermark"]):
        w = a[:, 0]
        w = w[w > MIN_LONG]
        d = np.diff(w)
        steps.append(d[d > 0])
    steps = np.concatenate(steps) if steps else np.zeros(0)
    speeds = [(m[-1] - m[0]) / max((t[-1] - t[0]) / 1000.0, 1e-9) for t, m in reader_series(meas) if len(t) > 1]
    return dict(wm_step_median_s=float(np.median(steps)) / 1000.0 if len(steps) else float("nan"),
                wm_step_p90_s=float(np.percentile(steps, 90)) / 1000.0 if len(steps) else float("nan"),
                reader_speed_event_ms_per_ms=float(np.median(speeds)) if speeds else float("nan"))


def ingested_time(readers, event_time):
    """Processing time (us) at which every reader had emitted a record with timestamp >= event_time: the moment the
    event time was ingested by all partitions (a reader that never gets there counts at its last row)."""
    times = []
    for t, m in readers:
        idx = np.minimum(np.searchsorted(m, event_time, side="left"), len(t) - 1)
        times.append(t[idx])
    return np.max(np.vstack(times), axis=0)


def finality(meas, cfg, series, spread, dataflow):
    """Delay to finality of the sampled outputs, three ways (see e3_finality.csv).
    local   arrival at the sink -> migration into Tier 2 (the shard's own watermark reached the output's time)
    global  arrival at the sink -> every upstream subtask's watermark reached it (the frontier of the sharded index)
    ready   all readers have ingested event time ts + delta + 1 (the watermark condition) -> migration: what the
            system adds to the unavoidable wait for delta (the interval, the spread, queueing)"""
    samples = load_array(glob.glob(os.path.join(meas, "finality_samples_index_s*.csv")), ["ts", "arrival_us", "final_us"])
    samples = [s for s in samples if len(s)]
    if not samples:
        return {}, []
    s = np.vstack(samples)
    ts, arrival, final = s[:, 0], s[:, 1], s[:, 2]
    delta = int(cfg["delta"])
    idle = int(cfg["idle"])
    local_ms = (final - arrival) / 1000.0
    out = dict(finality_samples=len(s))
    out["local_median_ms"], out["local_p99_ms"], out["local_max_ms"] = quantiles(local_ms)
    if series:
        end = max(x[-1, 0] for x in series)
        t_reach = []
        for x in series:
            idx = np.searchsorted(x[:, 1], ts, side="left")
            t_reach.append(np.where(idx < len(x), x[np.minimum(idx, len(x) - 1), 0], end))
        g_final = np.max(np.vstack(t_reach), axis=0)
        glob_ms = np.maximum(g_final - arrival, 0) / 1000.0
        out["global_median_ms"], out["global_p99_ms"], out["global_max_ms"] = quantiles(glob_ms)
    readers = reader_series(meas)
    if readers:
        ready_ms = (final - ingested_time(readers, ts + delta + 1)) / 1000.0
        out["ready_median_ms"], out["ready_p99_ms"], out["ready_max_ms"] = quantiles(ready_ms)
        # watermark latency: a watermark W of an upstream subtask is justified once all readers have ingested W + delta + 1
        lat = []
        for x in series:
            lat.append((x[:, 0] - ingested_time(readers, x[:, 1] + delta + 1)) / 1000.0)
        if lat:
            lat = np.concatenate(lat)
            out["wm_latency_median_ms"], out["wm_latency_p99_ms"], out["wm_latency_max_ms"] = quantiles(lat)
        # event-time view: the newest event time any reader had ingested when the output became final, minus its time
        ft = np.concatenate([t for t, m in readers])
        fm = np.concatenate([m for t, m in readers])
        order = np.argsort(ft, kind="stable")
        ft, fm = ft[order], np.maximum.accumulate(fm[order])
        idx = np.searchsorted(ft, final, side="right") - 1
        lag = (fm[np.maximum(idx, 0)] - ts)[idx >= 0]
        out["lag_event_median_ms"], out["lag_event_p99_ms"], out["lag_event_max_ms"] = quantiles(lag)
        interval_us = int(cfg["interval"]) * 1000
        L = interval_us + spread + idle * 1000
        j = np.searchsorted(ft, ft - L, side="right") - 1
        dF = fm - np.where(j >= 0, fm[np.maximum(j, 0)], fm[0])
        out["ingest_speed_event_ms_per_proc_ms"] = float(dF.max()) / (L / 1000.0) if L else float("nan")
    out["bound_wait_ms"] = delta + idle  # the plan's delta + tau (event time = processing time in a live stream)
    out["bound_system_ms"] = int(cfg["interval"]) + spread / 1000.0 + idle  # interval + spread (+ tau), processing time
    return out, local_ms


# timing
def read_runtimes(cfg_dir, method):
    vals = []
    failed = 0
    for p in sorted(glob.glob(os.path.join(cfg_dir, "time_%s_*" % method, "net_runtime_ms.txt"))):
        s = open(p).read().strip()
        if re.fullmatch(r"\d+", s):
            vals.append(int(s) / 1000.0)
        else:
            failed += 1
    return vals, failed


def stats(vals):
    if not vals:
        return dict(n=0, median=float("nan"), mean=float("nan"), std=float("nan"), min=float("nan"), max=float("nan"))
    return dict(n=len(vals), median=statistics.median(vals), mean=statistics.mean(vals),
                std=statistics.stdev(vals) if len(vals) > 1 else 0.0, min=min(vals), max=max(vals))


# output equality (SMALL)
def check_outputs(dataflow, run_file, ref_file):
    channels = CHANNELS[dataflow]
    ref_a, _ = verify.load_vectors(ref_file, width_of(ref_file, channels), channels)
    run_a, _ = verify.load_vectors(run_file, width_of(run_file, channels), channels)
    n_run, n_ref, only_run, only_ref = verify.difference(run_a, ref_a, verify.FLOAT_COLS[dataflow])
    return dict(outputs_run=sum(run_a.values()), outputs_ref=sum(ref_a.values()), only_in_run=n_run, only_in_ref=n_ref)


def width_of(path, channels):
    for cols in verify.rows(path):
        return len(cols) - channels
    raise ValueError("empty output " + path)


# driver
def flat(prefix, d):
    return {prefix + k: v for k, v in d.items()}


def analyse(work, small, reps):
    runs, correctness, occ_rows, fin_rows, late_rows, timing_rows, overhead_rows = [], [], [], [], [], [], []
    for df_dir in sorted(glob.glob(os.path.join(work, "*"))):
        dataflow = os.path.basename(df_dir)
        for cfg_dir in sorted(glob.glob(os.path.join(df_dir, "*"))):
            name = os.path.basename(cfg_dir)
            # the cluster run (traces: processing-time quantities) is the primary one; the in-process runs (late_r*)
            # carry Flink's operator metrics and the repeated correctness measurements
            late_dirs = [d for d in sorted(glob.glob(os.path.join(cfg_dir, "late_r*"))) if os.path.exists(os.path.join(d, "query_answers.csv"))]
            meas = os.path.join(cfg_dir, "meas")
            if not os.path.exists(os.path.join(meas, "query_answers.csv")):
                meas = late_dirs[0] if late_dirs else None
            if name.startswith("ref_") or meas is None:
                continue
            late_dirs = late_dirs or [meas]
            cfg = read_kv(os.path.join(meas, "config.txt"))
            zipf = cfg["zipf"]
            ref_dir = os.path.join(df_dir, "ref_z%s" % zipf, "late_r1")
            row = dict(dataflow=dataflow, cfg=name, small=int(small))
            row.update({k: cfg[k] for k in ("p", "D", "delta", "slow", "zipf", "sinkp", "tier1", "interval", "idle", "batch", "runner")})
            row["delta_ge_D"] = int(int(cfg["delta"]) >= int(cfg["D"]))
            # correctness
            st = read_stats(meas)
            ref_st = read_stats(ref_dir)
            cmp = compare_answers(read_answers(os.path.join(meas, "query_answers.csv")),
                                  read_answers(os.path.join(ref_dir, "query_answers.csv")))
            row.update({k: v for k, v in st.items() if k != "records_per_shard"})
            row["ref_indexed"] = ref_st["indexed"]
            row["indexed_missing"] = ref_st["indexed"] - st["indexed"]
            row.update(cmp)
            row["records_per_shard_min"] = min(st["records_per_shard"])
            row["records_per_shard_max"] = max(st["records_per_shard"])
            # outputs file check
            fc, rc = (os.path.join(cfg_dir, "file_caps", "caps.out"), os.path.join(df_dir, "ref_z%s" % zipf, "file_caps", "caps.out"))
            if os.path.exists(fc) and os.path.exists(rc):
                row.update(check_outputs(dataflow, fc, rc))
            # late records and sources
            late = read_late(late_dirs[0])
            src = read_sources(late_dirs[0])
            row.update(src)
            row.update(watermark_lag(late_dirs[0]))
            rt_file = os.path.join(late_dirs[0], "net_runtime_ms.txt")
            row["late_runtime_s"] = float(open(rt_file).read().strip()) / 1000.0 if os.path.exists(rt_file) and open(rt_file).read().strip().isdigit() else float("nan")
            row["late_total"] = sum(d["late"] for d in late.values())
            row["late_dropped_total"] = sum(d["side"] + d["dropped"] for d in late.values())
            row["late_ops"] = ";".join("%s:%d" % (op, d["late"]) for op, d in sorted(late.items()))
            row["late_frac_source"] = src["source_violations"] / max(src["source_records"], 1)
            for op, d in sorted(late.items()):
                late_rows.append(dict(dataflow=dataflow, cfg=name, operator=op, records_in=d["inp"], late=d["late"],
                                      late_side_output=d["side"], dropped_by_flink=d["dropped"], missed_a_window=d["missed"],
                                      late_frac_of_operator_input=d["late"] / max(d["inp"], 1)))
            row["late_frac_op_max"] = max((d["late"] / max(d["inp"], 1) for d in late.values()), default=0.0)
            # repeated measurements (delta < D or an idle reader: what is late depends on the progress of the readers)
            reps = []
            for md in late_dirs:
                st_r = read_stats(md)
                cmp_r = compare_answers(read_answers(os.path.join(md, "query_answers.csv")),
                                        read_answers(os.path.join(ref_dir, "query_answers.csv")))
                late_r = read_late(md)
                src_r = read_sources(md)
                reps.append(dict(late=sum(d["late"] for d in late_r.values()), dropped=sum(d["side"] + d["dropped"] for d in late_r.values()),
                                 differ=cmp_r["answers_differ"], missing=ref_st["indexed"] - st_r["indexed"],
                                 violations=src_r["source_violations"]))
            row["meas_runs"] = len(reps)
            row["meas_runner"] = cfg["runner"]
            row["late_runner"] = read_kv(os.path.join(late_dirs[0], "config.txt"))["runner"]
            for k in ("late", "dropped", "differ", "missing", "violations"):
                vals = [r[k] for r in reps]
                row["runs_%s_min" % k], row["runs_%s_mean" % k], row["runs_%s_max" % k] = min(vals), sum(vals) / len(vals), max(vals)
                row["runs_%s_median" % k] = statistics.median(vals)   # the paper reports medians (methodology)
            # latency
            row.update(read_latency(meas))
            # occupancy / bound / finality
            series = probe_series(meas)
            sp_max, sp_p99, sp_med = spread_us(series)
            row["spread_max_ms"], row["spread_p99_ms"], row["spread_median_ms"] = sp_max / 1000, sp_p99 / 1000, sp_med / 1000
            occ = occupancy(meas, cfg, sp_max)
            for o in occ:
                occ_rows.append(dict(dataflow=dataflow, cfg=name, **o))
            good = [o for o in occ if "peak_before" in o]
            if good:
                row["peak_before_max_shard"] = max(o["peak_before"] for o in good)
                for k in ("bound_avg_rate_nominal", "bound_avg_rate_measured", "bound_window_nominal", "bound_window_measured"):
                    row[k + "_max_shard"] = max(o[k] for o in good)
                row["gap_max_ms"] = max(o["gap_max_ms"] for o in good)
                row["peak_le_window_bound"] = int(all(o["peak_before"] <= o["bound_window_measured"] for o in good))
                row["peak_le_avg_bound"] = int(all(o["peak_before"] <= o["bound_avg_rate_measured"] for o in good))
            fin, _ = finality(meas, cfg, series, sp_max, dataflow)
            row.update(fin)
            fin_rows.append(dict(dataflow=dataflow, cfg=name, delta=cfg["delta"], idle=cfg["idle"],
                                 interval=cfg["interval"], **fin))
            # timing
            rt_n, fail_n = read_runtimes(cfg_dir, "noprov")
            rt_c, fail_c = read_runtimes(cfg_dir, "caps")
            sn, sc = stats(rt_n), stats(rt_c)
            tc = glob.glob(os.path.join(cfg_dir, "time_caps_*", "config.txt"))
            timing_runner = read_kv(tc[0])["runner"] if tc else cfg["runner"]
            row["timing_runner"] = timing_runner
            for method, s_, f_, vals in (("noprov", sn, fail_n, rt_n), ("caps", sc, fail_c, rt_c)):
                timing_rows.append(dict(dataflow=dataflow, cfg=name, method=method, runner=timing_runner, failed=f_,
                                        reps=";".join("%.3f" % v for v in vals), **s_))
                row.update(flat("time_%s_" % method, s_))
            if sn["n"] and sc["n"]:
                ratio = sc["median"] / sn["median"]
                cv = ((sc["std"] / sc["mean"]) ** 2 + (sn["std"] / sn["mean"]) ** 2) ** 0.5
                overhead_rows.append(dict(dataflow=dataflow, cfg=name, runner=timing_runner, noprov_median_s=sn["median"],
                                          caps_median_s=sc["median"], ratio=ratio, ratio_err=ratio * cv,
                                          overhead_pct=(ratio - 1) * 100, overhead_pct_err=ratio * cv * 100))
                row["overhead_ratio"] = ratio
                row["overhead_ratio_err"] = ratio * cv
            # sink-side time of the index (Tier-1 insert + split + Tier-2 append) as a share of the CAPS runtime
            if sc["n"]:
                idx_s = (st["insert_ns_per_rec"] + st["split_ns_per_rec"] + st["append_ns_per_rec"]) * st["indexed"] / 1e9
                row["index_time_s_all_shards"] = idx_s
                row["index_time_share_of_caps"] = idx_s / max(int(cfg["sinkp"]), 1) / sc["median"]
            runs.append(row)
            correctness.append({k: row.get(k) for k in (
                "dataflow", "cfg", "p", "D", "delta", "slow", "zipf", "sinkp", "delta_ge_D", "indexed", "ref_indexed",
                "indexed_missing", "queries", "answers_differ", "versioning", "versioning_differ", "defined_differ",
                "max_rel_diff", "late_at_sink", "behind_watermark", "nonmonotone_appends", "late_total",
                "source_violations", "source_records", "outputs_run", "outputs_ref", "only_in_run", "only_in_ref")})
    return runs, correctness, occ_rows, fin_rows, late_rows, timing_rows, overhead_rows


def timeouts(work):
    """Runs that did not finish within the limit (a result: e.g. a sorted-buffer Tier 1 fed out of order)."""
    rows = []
    for p in sorted(glob.glob(os.path.join(work, "*", "*", "*", "net_runtime_ms.txt"))):
        if open(p).read().strip() == "TIMEOUT":
            parts = p.split(os.sep)
            rows.append(dict(dataflow=parts[-4], cfg=parts[-3], run=parts[-2]))
    return rows


def write(path, rows):
    if not rows:
        return
    keys = []
    for r in rows:
        for k in r:
            if k not in keys:
                keys.append(k)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--work", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--small", type=int, default=0)
    ap.add_argument("--reps", type=int, default=3)
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)
    runs, correctness, occ, fin, late, timing, overhead = analyse(args.work, args.small, args.reps)
    for name, rows in (("e3_runs", runs), ("e3_correctness", correctness), ("e3_occupancy", occ), ("e3_finality", fin),
                       ("e3_late", late), ("e3_timing", timing), ("e3_overhead", overhead), ("e3_timeouts", timeouts(args.work))):
        write(os.path.join(args.out, name + ".csv"), rows)
    print("analysed %d configurations -> %s" % (len(runs), args.out))

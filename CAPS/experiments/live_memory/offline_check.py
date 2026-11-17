"""Independent check of the measured live-state peak of taxi_2 (CAPS and Green), recomputed from the input file alone.

The dataflow (taxi_2): card rides and cash rides each go through a sliding 1 h / 10 min window average per zone; the two
averages are joined on the zone in a sliding 1 h / 10 min window. Panes (zone, start) of the aggregates hold one
accumulator: CAPS 8*M bytes, Green 8 bytes per ride (every ride is a distinct variable). An aggregate pane is emitted at
its right boundary with that timestamp, and the join buffers it once per join pane, with the aggregate's annotation size.

The state is replayed in event time in the order of experiments/live_memory (bucket = window end, phases: adds of stage 0,
purges of stage 0, adds of stage 1, purges of stage 1) and its maximum is printed. Compare with logical_peak_total of
results_*/taxi_2/{caps,green_polynomial}/rep1/live_state.csv.

Usage: offline_check.py <taxis.txt> [live_state.csv of caps] [live_state.csv of green]
"""
import argparse
import csv
import sys
from collections import defaultdict

SIZE = 3600000
SLIDE = 600000
CHANNELS = 2


def panes(ts):
    """Starts of the sliding windows that contain ts (offset 0)."""
    last = ts - ts % SLIDE
    return [last - k * SLIDE for k in range(SIZE // SLIDE)]


def replay(events):
    """events: (bucket, phase, delta). Returns the maximum of the running sum in (bucket, phase) order."""
    delta = defaultdict(int)
    for bucket, phase, d in events:
        delta[(bucket, phase)] += d
    total = peak = 0
    for key in sorted(delta):
        total += delta[key]
        peak = max(peak, total)
    return peak, total


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("taxis")
    parser.add_argument("live_caps", nargs="?")
    parser.add_argument("live_green", nargs="?")
    args = parser.parse_args()

    # (branch, zone, pane start) -> [first ts, rides]
    agg = {}
    with open(args.taxis) as handle:
        for line in handle:
            f = line.strip().split(",")
            if len(f) < 7 or f[4] not in ("CRD", "CSH"):
                continue
            branch, zone, ts = f[4], int(f[3]), int(f[1])
            for s in panes(ts):
                p = agg.setdefault((branch, zone, s), [ts, 0])
                p[0] = min(p[0], ts)
                p[1] += 1

    for name, acc_bytes, annotation in (("caps", lambda n: 8 * CHANNELS, lambda n: 8 * CHANNELS),
                                        ("green", lambda n: 8 * n, lambda n: 8 * n)):
        events = []
        for (branch, zone, s), (first_ts, n) in agg.items():
            end = s + SIZE
            if name == "caps":
                events.append((first_ts // SLIDE + 1, 0, acc_bytes(n)))       # stage 0 add at the first element
            else:
                # Green: the accumulator grows 8 B per ride, all at the ride's own time; replayed below
                pass
            events.append((end // SLIDE, 1, -acc_bytes(n)))                  # stage 0 purge at the window end
            # the aggregate is emitted at end - 1 and buffered by the join once per join pane
            for js in panes(end - 1):
                events.append((end // SLIDE, 2, annotation(n)))
                events.append(((js + SIZE) // SLIDE, 3, -annotation(n)))
        if name == "green":
            # per-ride growth: re-read the rides of every pane
            with open(args.taxis) as handle:
                for line in handle:
                    f = line.strip().split(",")
                    if len(f) < 7 or f[4] not in ("CRD", "CSH"):
                        continue
                    ts = int(f[1])
                    for _ in panes(ts):
                        events.append((ts // SLIDE + 1, 0, 8))
        peak, end_total = replay(events)
        print("%s: offline logical peak %d B, final total %d" % (name, peak, end_total))
        live = args.live_caps if name == "caps" else args.live_green
        if live:
            rows = {r["name"]: int(r["value"]) for r in csv.DictReader(open(live))}
            ok = rows["logical_peak_total"] == peak
            print("  measured %d  %s" % (rows["logical_peak_total"], "MATCH" if ok else "DIFFERENT"))
            if not ok:
                sys.exit(1)

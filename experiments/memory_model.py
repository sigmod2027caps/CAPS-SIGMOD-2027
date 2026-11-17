"""Analytical meta-data memory, split in the three components of the paper.

The single number of memory_model.csv hides where the bytes sit, so here we
charge every method separately for

  sink_bytes          the annotation the sink must retain to answer queries,
  intermediate_bytes  the annotation riding on every operator output,
  window_state_bytes  the annotation inside open window accumulators.

Nothing here is measured heap. Heap conflates the annotation with Flink's
network buffers, with the retention policy of the sink and with the timing of
the garbage collector, none of which is a property of a provenance scheme. The
measured counterpart of this file is experiments/memory/, which counts the same
bytes with per-operator probes.

Written by experiments/how_much/run.sh, one file per dataflow.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from pathlib import Path

BYTES_PER_FIELD = 8            # a long, or a reference with compressed oops
GENEALOG_FIELDS = 4            # GenealogData: uid, U1, U2, next
GENEALOG_WINDOW_POINTERS = 2   # sortedPtr accumulator: earliest, latest

# HotSpot 64-bit with compressed oops: mark word 8 B + klass pointer 4 B +
# length 4 B. We charge it on the intermediate and the window components,
# where the long[] really sits on the tuple, but not on the sink component,
# so sink_bytes stays the same number the paper always reported.
HOTSPOT_ARRAY_HEADER = 16

DETAIL_HEADER = ("method", "sink_bytes", "intermediate_bytes",
                 "window_state_bytes", "total_bytes")

MS_5MIN = 5 * 60 * 1000
MS_10MIN = 10 * 60 * 1000
MS_30MIN = 30 * 60 * 1000
MS_1H = 60 * 60 * 1000
MS_6H = 6 * 60 * 60 * 1000
MS_12H = 12 * 60 * 60 * 1000

# Windows and key domains as they are written in the Java sources, not invented
# here. Every windowed operator contributes #keys x ceil(W/S) live
# accumulators: taxi_1 counts the 13,257 taxi ids of the input, taxi_2 the
# 1,000 zones of the grid, the twitter ones the 19,113 distinct hashtags of the
# tweets, and the nexmark ones the persons of the generator.
DATAFLOW_WINDOWS = {
    "taxi_1": {
        "keys": 13257,
        "key_name": "taxiId",
        "windows": (
            ("AGG-SOLO", MS_1H, MS_10MIN),
            ("AGG-CROWDED", MS_1H, MS_10MIN),
            ("JOIN", MS_1H, MS_10MIN),
        ),
    },
    "taxi_2": {
        "keys": 1000,
        "key_name": "zone",
        "windows": (
            ("AGG-CARD", MS_1H, MS_10MIN),
            ("AGG-CASH", MS_1H, MS_10MIN),
            ("JOIN", MS_1H, MS_10MIN),
        ),
    },
    "twitter_1": {
        "keys": 19113,
        "key_name": "hashtag",
        "windows": (
            ("AGG-ALL", MS_30MIN, MS_5MIN),
            ("AGG-VERIFIED", MS_30MIN, MS_5MIN),
            ("JOIN", MS_30MIN, MS_5MIN),
        ),
    },
    "twitter_2": {
        "keys": 19113,
        "key_name": "hashtag",
        "windows": (
            ("AGG-6H", MS_6H, MS_1H),
        ),
    },
    "nexmark_1": {
        "keys": 500000,
        "key_name": "personId",
        "windows": (
            ("JOIN", MS_12H, MS_12H),
            ("AGG-COUNT", MS_12H, MS_12H),
        ),
    },
    "nexmark_2": {
        "keys": 179508,
        "key_name": "personId",
        "windows": (
            ("JOIN", MS_12H, MS_12H),
        ),
    },
}


def align8(n: int) -> int:
    return (n + 7) // 8 * 8


def caps_annotation_bytes(channels: int) -> int:
    """One HowMuch long[] as it sits on a tuple, header inluded."""
    return align8(channels * BYTES_PER_FIELD + HOTSPOT_ARRAY_HEADER)


def write_records(job_json: Path) -> int:
    """Tuples emitted by all operators, from Flink's own job metrics."""
    job = json.loads(job_json.read_text())
    total = 0
    for vertex in job.get("vertices", []):
        total += max(vertex.get("metrics", {}).get("write-records", 0), 0)
    return total


def open_accumulators(dataflow: str) -> int:
    spec = DATAFLOW_WINDOWS[dataflow]
    keys = spec["keys"]
    total = 0
    for name, window_ms, slide_ms in spec["windows"]:
        panes = math.ceil(window_ms / slide_ms)
        total += keys * panes
        print(f"  window {name}: {keys} keys x {panes} panes "
              f"= {keys * panes} accumulators", file=sys.stderr)
    return total


def green_mean_bytes(ink_meta: Path) -> tuple[int, float]:
    """Sink bytes and mean bytes per output of the Green polynomials.

    A polynomial grows with the tuples that contributed, so we do not model it:
    the verification run measured it and metadata_ink.csv is the only source.
    """
    with ink_meta.open(newline="") as handle:
        row = next(csv.DictReader(handle))
    outputs = int(row["outputs"])
    sink_bytes = int(row["bytes"])
    if outputs <= 0:
        raise SystemExit(f"{ink_meta}: outputs must be > 0")
    return sink_bytes, sink_bytes / outputs


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataflow", required=True, choices=sorted(DATAFLOW_WINDOWS))
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--channels", type=int, required=True)
    parser.add_argument("--caps-job", type=Path, required=True)
    parser.add_argument("--genealog-job", type=Path, required=True)
    parser.add_argument("--caps-outputs", type=int, required=True)
    parser.add_argument("--genealog-outputs", type=int, required=True)
    parser.add_argument("--ink-meta", type=Path)
    args = parser.parse_args()

    for path in (args.caps_job, args.genealog_job):
        if not path.is_file():
            raise SystemExit(f"missing job JSON: {path}")

    spec = DATAFLOW_WINDOWS[args.dataflow]
    print(f"[{args.dataflow}] key domain {spec['keys']} {spec['key_name']}, "
          f"channels {args.channels}", file=sys.stderr)
    accumulators = open_accumulators(args.dataflow)

    rows = [("noprov", 0, 0, 0, 0)]

    # GeneaLog retains the whole graph, so every emitted tuple keeps its
    # quadruple; the sink outputs are chained and counted on their own.
    gen_ops = write_records(args.genealog_job)
    gen_unit = GENEALOG_FIELDS * BYTES_PER_FIELD
    gen_sink = args.genealog_outputs * gen_unit
    gen_inter = gen_ops * gen_unit
    gen_window = accumulators * GENEALOG_WINDOW_POINTERS * BYTES_PER_FIELD
    rows.append(("genealog", gen_sink, gen_inter, gen_window,
                 gen_sink + gen_inter + gen_window))

    caps_ops = write_records(args.caps_job)
    caps_unit = caps_annotation_bytes(args.channels)
    caps_sink = args.caps_outputs * args.channels * BYTES_PER_FIELD
    caps_inter = caps_ops * caps_unit
    caps_window = accumulators * caps_unit

    if args.ink_meta is not None and args.ink_meta.is_file():
        green_sink, green_mean = green_mean_bytes(args.ink_meta)
        # Same topology as CAPS, so the operator counts are the CAPS ones. The
        # mean sink polynomial is an upper bound upstream, where they are smaller.
        green_inter = round(caps_ops * green_mean)
        green_window = round(accumulators * green_mean)
        rows.append(("green_polynomial", green_sink, green_inter, green_window,
                     green_sink + green_inter + green_window))

    # chronological order, ours last
    rows.append(("caps", caps_sink, caps_inter, caps_window,
                 caps_sink + caps_inter + caps_window))

    with args.out.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(DETAIL_HEADER)
        for row in rows:
            writer.writerow(row)
    for row in rows:
        print("  %-16s sink %d, intermediate %d, window %d" % row[:4], file=sys.stderr)
    print(f"Wrote {args.out}", file=sys.stderr)

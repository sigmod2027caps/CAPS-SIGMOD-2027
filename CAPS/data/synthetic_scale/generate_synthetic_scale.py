"""Synthetic input of the channel-cardinality experiment (experiments/channel_scaling).

One record per line:  id,timestamp,sourceId,key,value
  id         0, 1, 2, ... (the record's position)
  timestamp  event time in ms, strictly increasing: floor(id * span / records), so that a punctuated
             watermark after every record never makes the next record late
  sourceId   uniform in [0, SOURCES); the channel of a record is sourceId % M (source sweep) or
             id % M (path sweep), so every M that divides SOURCES gives balanced channels
  key        uniform in [0, keys)
  value      uniform in [1, 100]

The dataflow is a 60 s tumbling window per key, so the sink gets about keys x windows outputs, each
with about records / (keys x windows) contributing records. That ratio is the number that decides
where CAPS's O(M) vector per output overtakes the polynomial's one variable per contributing record
(M ~ records per output).

Fixed seed: the same arguments always give the same file. The data files are git-ignored.
"""

import argparse
import sys
from pathlib import Path

import numpy as np

WINDOW_MS = 60_000


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--records", type=int, default=2_000_000)
    parser.add_argument("--keys", type=int, default=1000)
    parser.add_argument("--windows", type=int, default=34, help="number of 60 s windows spanned")
    parser.add_argument("--sources", type=int, default=128, help="distinct sourceId values")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    span = args.windows * WINDOW_MS
    if args.records > span:
        sys.exit("records (%d) > span in ms (%d): timestamps would not be strictly increasing"
                 % (args.records, span))

    rng = np.random.default_rng(args.seed)
    ids = np.arange(args.records, dtype=np.int64)
    ts = ids * span // args.records
    sources = rng.integers(0, args.sources, args.records)
    keys = rng.integers(0, args.keys, args.records)
    values = rng.integers(1, 101, args.records)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w") as handle:
        for i in range(args.records):
            handle.write("%d,%d,%d,%d,%d\n" % (ids[i], ts[i], sources[i], keys[i], values[i]))
    print("%s: %d records, %d keys, %d windows, %.1f records per (key, window)"
          % (args.out, args.records, args.keys, args.windows,
             args.records / (args.keys * args.windows)))

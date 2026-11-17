#!/usr/bin/env python3
"""Aggregates one result CSV over the repetitions of an experiment: <dir>/rep<k>/<file> -> <dir>/<file>.

Rows are matched on --keys; every numeric column becomes its median over the repetitions, the columns named in
--spread also get <col>_min and <col>_max, and a column `reps` counts the repetitions (common.aggregate_reps).
--copy-first copies the first repetition's file instead (a time series that is the same in every repetition).

  aggregate_reps.py --dir experiments/expiry/taxi_1 --file expiry_stats.csv --keys horizon_ms --spread avg_expiry_us
"""
import argparse
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common  # noqa: E402

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dir", type=Path, required=True)
    ap.add_argument("--file", required=True)
    ap.add_argument("--keys", default="")
    ap.add_argument("--spread", default="")
    ap.add_argument("--copy-first", action="store_true")
    args = ap.parse_args()
    reps = sorted((d for d in args.dir.iterdir() if d.is_dir() and d.name.startswith("rep")),
                  key=lambda d: int(d.name[3:]))
    files = [d / args.file for d in reps if (d / args.file).is_file()]
    if not files:
        raise SystemExit("no %s in %s/rep*" % (args.file, args.dir))
    if args.copy_first:
        shutil.copy(files[0], args.dir / args.file)
    else:
        header, rows = common.aggregate_reps(files, [k for k in args.keys.split(",") if k],
                                             spread=[c for c in args.spread.split(",") if c])
        common.write_csv(args.dir / args.file, header, rows)

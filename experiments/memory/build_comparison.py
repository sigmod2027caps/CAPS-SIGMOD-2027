#!/usr/bin/env python3
"""Build the shareable memory comparison table from probe totals.

Decimal MB (10^6 bytes) is what the paper reports; not peak or live heap.
We keep bytes and MB seperate so the rounding is auditable.
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

DATAFLOWS = ("taxi_1", "taxi_2", "nexmark_1", "nexmark_2")
METHODS = ("caps", "green_polynomial", "genealog")
MEMORY_COLUMN = "Cumulative provenance metadata volume (MB)"
CSV_COLUMNS = ("dataflow", "method", "metadata_volume_bytes", MEMORY_COLUMN)


class ComparisonError(Exception):
    pass


def _fail(message: str) -> None:
    raise ComparisonError(message)


def decimal_mb(byte_count: int) -> float:
    return byte_count / 1_000_000.0


def format_mb(byte_count: int) -> str:
    return "%.3f" % decimal_mb(byte_count)


def load_volume(path: Path) -> dict[tuple[str, str], int]:
    path = Path(path)
    if not path.is_file():
        _fail("missing metadata volume: %s" % path)
    found: dict[tuple[str, str], int] = {}
    with path.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        _fail("empty metadata volume: %s" % path)
    for row in rows:
        dataflow = row.get("dataflow", "").strip()
        method = row.get("method", "").strip()
        if dataflow not in DATAFLOWS or method not in METHODS:
            continue
        key = (dataflow, method)
        if key in found:
            _fail("duplicate volume row for %s %s" % key)
        raw = row.get("bytes", "").strip()
        if not raw:
            _fail("missing bytes for %s %s in %s" % (dataflow, method, path))
        try:
            value = int(raw)
        except ValueError:
            _fail("malformed bytes %r for %s %s in %s"
                  % (raw, dataflow, method, path))
        if value < 0:
            _fail("bytes must be nonnegative for %s %s in %s"
                  % (dataflow, method, path))
        found[key] = value
    missing = [(df, method) for df in DATAFLOWS for method in METHODS
               if (df, method) not in found]
    if missing:
        _fail("missing probe volume for %s"
              % ", ".join("%s/%s" % key for key in missing))
    return found


def build_rows(volume_stats: dict[tuple[str, str], int]) -> list[dict]:
    rows = []
    for dataflow in DATAFLOWS:
        for method in METHODS:
            bytes_ = volume_stats[(dataflow, method)]
            rows.append({
                "dataflow": dataflow,
                "method": method,
                "metadata_volume_bytes": str(bytes_),
                MEMORY_COLUMN: format_mb(bytes_),
            })
    return rows


def write_comparison(volume_path: Path, out_dir: Path) -> None:
    volume = load_volume(volume_path)
    rows = build_rows(volume)
    out_csv = out_dir / "how_much_comparison.csv"
    with out_csv.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(CSV_COLUMNS))
        writer.writeheader()
        writer.writerows(rows)
    print("wrote %s" % out_csv)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Build memory-only how_much_comparison table from probes.")
    parser.add_argument(
        "expdir", nargs="?", type=Path,
        default=Path(__file__).resolve().parent,
        help="memory experiment directory")
    args = parser.parse_args(argv)
    expdir = args.expdir.resolve()
    try:
        write_comparison(expdir / "metadata_volume.csv", expdir)
    except ComparisonError as exc:
        print(exc, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

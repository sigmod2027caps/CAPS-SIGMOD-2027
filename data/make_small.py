#!/usr/bin/env python3
"""Builds the 10% subsets under data/small/ (SMALL=1 reads them instead of the full inputs).

Each subset is a prefix of the full file, so it covers a tenth of the time span of the full stream:
  taxis/taxis.txt        the first 10% of the rides
  tweets/twitter.txt     the first 10% of the tweets (part of the artifact)
  nexmark/persons.txt    the first 10% of the persons
  nexmark/auctions.txt   the auctions up to the time of the last person of the subset (the first auction later
                         than that person ends the file), so that both streams span the same period

Usage: make_small.py [--force]     (existing subsets are kept unless --force)
"""
import argparse
import itertools
import sys
from pathlib import Path

DATA = Path(__file__).resolve().parent
SMALL = DATA / "small"
FRACTION = 0.10


def line_count(path):
    with open(path, "rb") as f:
        return sum(1 for _ in f)


def write_prefix(src, dst, n):
    dst.parent.mkdir(parents=True, exist_ok=True)
    with open(src, "rb") as f, open(dst, "wb") as out:
        out.writelines(itertools.islice(f, n))
    print("%s: %d lines" % (dst.relative_to(DATA), n))


def prefix_fraction(rel, force):
    src, dst = DATA / rel, SMALL / rel
    if not src.is_file() or src.stat().st_size == 0:
        print("skipping %s: no full file" % rel)
        return None
    if dst.is_file() and dst.stat().st_size > 0 and not force:
        print("%s: already there" % dst.relative_to(DATA))
        return dst
    write_prefix(src, dst, int(line_count(src) * FRACTION))
    return dst


def timestamp(line):
    return int(line.split(b",", 2)[1])


def small_auctions(force):
    src, dst = DATA / "nexmark/auctions.txt", SMALL / "nexmark/auctions.txt"
    persons = SMALL / "nexmark/persons.txt"
    if not src.is_file() or not persons.is_file():
        return
    if dst.is_file() and dst.stat().st_size > 0 and not force:
        print("%s: already there" % dst.relative_to(DATA))
        return
    with open(persons, "rb") as f:
        for last in f:
            pass
    end = timestamp(last)
    with open(src, "rb") as f:
        n = sum(1 for _ in itertools.takewhile(lambda l: timestamp(l) <= end, f))
    write_prefix(src, dst, n)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()
    prefix_fraction("taxis/taxis.txt", args.force)
    prefix_fraction("tweets/twitter.txt", args.force)
    prefix_fraction("nexmark/persons.txt", args.force)
    small_auctions(args.force)

#!/usr/bin/env python3
"""Cross-method verification of one dataflow (called by experiments/verify/run.sh).

Reads the sink output of the four methods and checks

  outputs     (a) the four methods produced the same multiset of outputs, values
                  and derived timestamps (noprov is the reference)
  howmuch     (b) the same per-output how-much counts: CAPS against Green
                  channel by channel, and the sum over the channels against the
                  number of source tuples GeneaLog's backward traversal reaches.
                  Above parallelism 1 the GeneaLog comparison is only reported:
                  its sink is then on another subtask than the last operator, so
                  GeneaLog flattens the graph at that shuffle into the set of
                  distinct source tuples, while CAPS and the traversal at the
                  chained sink of parallelism 1 count one per path (a tweet that
                  reaches the output on two paths counts twice)
  monotone    (c) CAPS's sink received its input in non-decreasing derived time
                  (the monotonicity lemma; a pass/fail check at parallelism 1,
                  where the count of out-of-order arrivals must be 0, and only a
                  count above it, where the order of the subtasks is undefined)
  window_end  (d) every output timestamp is the right boundary of a window
                  (one millisecond before a multiple of the last window's slide)
  invariance  (e) above parallelism 1 only: CAPS's outputs and per-channel counts
                  are those of CAPS at parallelism 1 (--caps-ref)

Row formats (see run_*_file in experiments/common.sh), one row per output:
  noprov.out    the data columns
  caps.out      the data columns, then one counter per channel
  green.out     the same
  genealog.out  the data columns, then the traversal count (a tuple with a
                trailing value2 that the dataflow does not output, like twitter_2,
                prints it as 0.0 and is trimmed to the width of the noprov rows)

At parallelism 1 rows are compared as strings, exactly. Above it Flink adds the
values of a window in another order, which moves the last digits of a
floating-point sum, so the rows that differ as strings are matched again with
the float columns compared to a relative 1e-9.

Appends one row per check to --csv and prints PASS or FAIL for each.
"""
import argparse
import csv
import os
import sys
from collections import Counter, defaultdict

# Columns of the data part that are doubles (the others are keys, timestamps and strings).
FLOAT_COLS = {
    "nexmark_1": (2, 3),
    "nexmark_2": (),
    "taxi_1": (2, 3),
    "taxi_2": (2, 3),
    "twitter_1": (2, 3),
    "twitter_2": (2,),
}

REL_TOL = 1e-9

CSV_HEADER = ["dataflow", "small", "parallelism", "check", "status", "outputs", "mismatches", "detail"]


def rows(path):
    """The output rows of a sink file, in arrival order (the trailer line is skipped)."""
    with open(path) as handle:
        for line in handle:
            line = line.strip()
            if line and not line.startswith("---"):
                yield line.split(",")


def load_plain(path):
    """noprov rows: multiset of the data columns, and their number."""
    payloads = Counter()
    width = None
    for cols in rows(path):
        width = len(cols)
        payloads[",".join(cols)] += 1
    return payloads, width


def load_vectors(path, width, channels):
    """CAPS or Green rows: multiset of data|c0,c1,.. and of the data alone."""
    annotated = Counter()
    payloads = Counter()
    for cols in rows(path):
        if len(cols) != width + channels:
            raise ValueError("%s: %d columns, expected %d+%d: %s" % (path, len(cols), width, channels, cols))
        key = ",".join(cols[:width])
        payloads[key] += 1
        annotated[key + "|" + ",".join(cols[width:])] += 1
    return annotated, payloads


def load_totals(path, width):
    """GeneaLog rows: multiset of data|count and of the data alone."""
    annotated = Counter()
    payloads = Counter()
    for cols in rows(path):
        payload, count = cols[:-1], cols[-1]
        if len(payload) > width:
            if any(c != "0.0" for c in payload[width:]):
                raise ValueError("%s: unexpected extra columns: %s" % (path, cols))
            payload = payload[:width]
        if len(payload) != width:
            raise ValueError("%s: %d data columns, expected %d: %s" % (path, len(payload), width, cols))
        key = ",".join(payload)
        payloads[key] += 1
        annotated[key + "|" + count] += 1
    return annotated, payloads


def totals(vectors):
    """data|c0,c1,.. -> data|sum, to compare with GeneaLog's count."""
    out = Counter()
    for key, n in vectors.items():
        payload, counts = key.split("|")
        out[payload + "|" + str(sum(int(c) for c in counts.split(",")))] += n
    return out


def close(x, y):
    return all(abs(a - b) <= REL_TOL * max(abs(a), abs(b)) for a, b in zip(x, y))


def fuzzy_cancel(only_a, only_b, float_cols):
    """Matches again the rows that differ as strings, comparing the float columns with a tolerance.

    Returns what is left of only_a and of only_b. Rows are grouped by everything but the floats
    and the floats sorted, so the pairing is a two-pointer sweep.
    """
    groups = defaultdict(lambda: ([], []))
    for side, counter in ((0, only_a), (1, only_b)):
        for key, n in counter.items():
            payload, bar, note = key.partition("|")
            cols = payload.split(",")
            rest = ",".join(c for i, c in enumerate(cols) if i not in float_cols) + bar + note
            floats = tuple(float(cols[i]) for i in float_cols)
            # Sorted by the floats rounded to nine digits: two values that differ in the last bits
            # sort alike, whereas the exact floats would order rows of one group differently.
            order = tuple(float("%.9g" % f) for f in floats)
            groups[rest][side].extend([(order, floats, key)] * n)
    left_a, left_b = Counter(), Counter()
    for xs, ys in groups.values():
        xs.sort()
        ys.sort()
        i = j = 0
        while i < len(xs) and j < len(ys):
            if close(xs[i][1], ys[j][1]):
                i += 1
                j += 1
            elif xs[i][0] < ys[j][0]:
                left_a[xs[i][2]] += 1
                i += 1
            else:
                left_b[ys[j][2]] += 1
                j += 1
        for _, _, key in xs[i:]:
            left_a[key] += 1
        for _, _, key in ys[j:]:
            left_b[key] += 1
    return left_a, left_b


def difference(a, b, float_cols):
    """Rows (with multiplicity) in a and not in b and the other way round: counts and the rows."""
    only_a = a - b
    only_b = b - a
    if float_cols and (only_a or only_b):
        only_a, only_b = fuzzy_cancel(only_a, only_b, float_cols)
    return sum(only_a.values()), sum(only_b.values()), only_a, only_b


def sample(counter, limit=3):
    return "; ".join("%s x%d" % (k, n) for k, n in list(counter.items())[:limit])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dataflow", required=True, choices=sorted(FLOAT_COLS))
    parser.add_argument("--dir", required=True, help="directory with noprov.out, caps.out, green.out, genealog.out")
    parser.add_argument("--channels", type=int, required=True)
    parser.add_argument("--slide-ms", type=int, required=True)
    parser.add_argument("--parallelism", type=int, default=1)
    parser.add_argument("--small", type=int, default=0)
    parser.add_argument("--caps-ref", help="CAPS sink file of a parallelism-1 run, for the invariance check")
    parser.add_argument("--csv", required=True, help="appended to, one row per check")
    args = parser.parse_args()

    exact = args.parallelism == 1
    float_cols = () if exact else FLOAT_COLS[args.dataflow]
    path = lambda name: os.path.join(args.dir, name + ".out")

    reference, width = load_plain(path("noprov"))
    caps, caps_payloads = load_vectors(path("caps"), width, args.channels)
    green, green_payloads = load_vectors(path("green"), width, args.channels)
    genealog_totals, genealog_payloads = load_totals(path("genealog"), width)

    results = []  # (check, status, outputs, mismatches, detail)
    n_out = sum(reference.values())
    tolerance = "" if exact else " (floats to a relative %g)" % REL_TOL

    # (a) identical output multisets
    detail = []
    mismatches = 0
    for name, payloads in (("caps", caps_payloads), ("green", green_payloads), ("genealog", genealog_payloads)):
        extra, missing, only_m, only_ref = difference(payloads, reference, float_cols)
        mismatches += extra + missing
        detail.append("%s=%d" % (name, sum(payloads.values())))
        if extra or missing:
            detail.append("%s: %d extra [%s], %d missing [%s]"
                          % (name, extra, sample(only_m), missing, sample(only_ref)))
    results.append(("outputs", "PASS" if mismatches == 0 else "FAIL", n_out, mismatches,
                    "noprov=%d " % n_out + " ".join(detail) + tolerance))

    # (b) identical per-output how-much counts
    detail = []
    cg_extra, cg_missing, only_caps, only_green = difference(caps, green, float_cols)
    detail.append("caps_vs_green_by_channel=%d" % (cg_extra + cg_missing))
    gl_extra, gl_missing, only_caps_t, only_gl = difference(totals(caps), genealog_totals, float_cols)
    detail.append("caps_vs_genealog_total=%d" % (gl_extra + gl_missing))
    if cg_extra + cg_missing:
        detail.append("caps-green: [%s] vs [%s]" % (sample(only_caps), sample(only_green)))
    if gl_extra + gl_missing:
        detail.append("caps-genealog: [%s] vs [%s]" % (sample(only_caps_t), sample(only_gl)))
    if exact:
        mismatches = cg_extra + cg_missing + gl_extra + gl_missing
    else:
        mismatches = cg_extra + cg_missing  # the GeneaLog count is only reported, see the docstring
    results.append(("howmuch", "PASS" if mismatches == 0 else "FAIL", n_out, mismatches,
                    " ".join(detail) + tolerance))

    # (c) the sink of CAPS received its input in non-decreasing derived time
    # (the three other files are measured too, for the record)
    descents = {}
    backward = {}
    for name in ("caps", "green", "noprov", "genealog"):
        last = None
        count = 0
        worst = 0
        for cols in rows(path(name)):
            ts = int(cols[1])
            if last is not None and ts < last:
                count += 1
                worst = max(worst, last - ts)
            last = ts
        descents[name] = count
        backward[name] = worst
    detail = " ".join("%s=%d(max_jump_ms=%d)" % (n, descents[n], backward[n]) for n in descents)
    if exact:
        status = "PASS" if descents["caps"] == 0 else "FAIL"
    else:
        status = "INFO"  # more than one subtask: the order is not defined, only counted
    results.append(("monotone", status, n_out, descents["caps"], "out_of_order_arrivals " + detail))

    # (d) every timestamp is a window right boundary
    bad = 0
    first_bad = None
    for cols in rows(path("noprov")):
        ts = int(cols[1])
        if (ts + 1) % args.slide_ms != 0:
            bad += 1
            first_bad = first_bad or ts
    results.append(("window_end", "PASS" if bad == 0 else "FAIL", n_out, bad,
                    "slide_ms=%d%s" % (args.slide_ms, "" if first_bad is None else " first_bad=%d" % first_bad)))

    # (e) CAPS at this parallelism against CAPS at parallelism 1
    if args.caps_ref:
        ref, _ = load_vectors(args.caps_ref, width, args.channels)
        extra, missing, only_p, only_ref = difference(caps, ref, float_cols)
        detail = "parallelism=%d vs 1" % args.parallelism + tolerance
        if extra or missing:
            detail += " [%s] vs [%s]" % (sample(only_p), sample(only_ref))
        results.append(("invariance", "PASS" if extra + missing == 0 else "FAIL", n_out, extra + missing, detail))

    new_file = not os.path.exists(args.csv) or os.path.getsize(args.csv) == 0
    with open(args.csv, "a", newline="") as handle:
        writer = csv.writer(handle)
        if new_file:
            writer.writerow(CSV_HEADER)
        for check, status, outputs, mismatches, detail in results:
            writer.writerow([args.dataflow, args.small, args.parallelism, check, status,
                             outputs, mismatches, detail])
    failed = False
    for check, status, outputs, mismatches, detail in results:
        print("  %-4s %-10s outputs=%d mismatches=%d  %s" % (status, check, outputs, mismatches, detail))
        failed = failed or status == "FAIL"
    sys.exit(1 if failed else 0)

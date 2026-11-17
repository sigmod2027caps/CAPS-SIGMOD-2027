"""Garbage-collection pause time of one TaskManager from its HotSpot GC log (-Xloggc -XX:+PrintGCDetails -XX:+PrintGCDateStamps).

The how_much experiment starts every fresh cluster with the GC log of its TaskManager switched on (HotSpot 8 here:
ParallelGC, one line per pause "[GC (Allocation Failure) [PSYoungGen: ...] ..., 0.0078 secs] [Times: ...]", "[Full GC ...") (common.sh,
CAPS_GC_LOG_DIR), so that the runtime of every method can be read together with the time the JVM spent in stop-the-world
pauses. One line per pause carries the pause time of the whole pause as its last field:
    2026-10-28T08:00:00.123+0800: 12.345: [GC pause (G1 Evacuation Pause) (young), 0.0123456 secs]
    2026-10-28T08:00:09.000+0800: 20.001: [Full GC (Allocation Failure)  900M->800M(1000M), 1.5000000 secs]
    2026-10-28T08:00:10.000+0800: 21.001: [GC remark ..., 0.0100000 secs]
    2026-10-28T08:00:10.100+0800: 21.101: [GC cleanup 10M->9M(100M), 0.0010000 secs]
Concurrent phases run beside the application and are not pauses, so they are not counted.

Usage: gc_stats.py [--skip N] <gc log> [<gc log> ...]
prints one CSV line: pauses,full_pauses,total_s,max_s over the pauses after the first N (the JVM start-up ones, which the
job does not cause: common.sh capture_gc counts them just before the job is submitted).
"""

import re
import sys

PAUSE = re.compile(r"^\d{4}-\d\d-\d\dT[^ ]+: (?:[\d.]+: )?\[(Full GC|GC)(?! concurrent)")
TIMES = re.compile(r", ([\d.]+) secs\]")


def read_pauses(paths):
    """The pauses of the logs in order: (seconds, is a full collection)."""
    pauses = []
    for path in paths:
        with open(path, errors="replace") as handle:
            for line in handle:
                m = PAUSE.match(line)
                t = TIMES.findall(line) if m else None
                if t:
                    # the last ", X secs]" of the line closes the pause itself (nested phases of a G1 remark come first;
                    # the "[Times: ... real=X secs]" of a ParallelGC line has no comma before the number)
                    pauses.append((float(t[-1]), m.group(1) == "Full GC"))
    return pauses


def summarise(paths, skip=0):
    pauses = read_pauses(paths)[skip:]
    times = [p[0] for p in pauses]
    return len(pauses), sum(1 for p in pauses if p[1]), sum(times), max(times, default=0.0)


if __name__ == "__main__":
    args = sys.argv[1:]
    skip = 0
    if args[:1] == ["--skip"]:
        skip, args = int(args[1]), args[2:]
    if not args:
        raise SystemExit(__doc__)
    n, full, total, longest = summarise(args, skip)
    print("%d,%d,%.4f,%.4f" % (n, full, total, longest))

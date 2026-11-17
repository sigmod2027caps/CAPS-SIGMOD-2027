"""Print stats for tweets_100k_processed.csv - useful for picking window sizes."""

from __future__ import annotations

import csv
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent
INPUT = DATA_DIR / "tweets_100k_processed.csv"
WINDOW_SIZES = (5, 60, 300, 1800, 3600, 21600)  # 5s, 1m, 5m, 30m, 1h, 6h


def fmt_ts(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def load_rows() -> list[dict[str, str]]:
    with INPUT.open(newline="", encoding="utf-8", errors="replace") as f:
        return list(csv.DictReader(f))


def hashtag_event_timestamps(rows: list[dict[str, str]]) -> list[int]:
    events: list[int] = []
    for row in rows:
        ts = int(row["timestamp"])
        for tag in row["hashtags"].split("|"):
            if tag:
                events.append(ts)
    return events


def bucket_counts(timestamps_ms: list[int], window_sec: int) -> Counter[int]:
    step = window_sec * 1000
    counts: Counter[int] = Counter()
    for ts in timestamps_ms:
        counts[ts // step] += 1
    return counts


def summarize_buckets(counts: Counter[int], label: str) -> None:
    vals = list(counts.values())
    total = sum(vals)
    print(f"\n{label}")
    print(f"  non-empty windows:         {len(vals):,}")
    print(f"  events in those windows:   {total:,}")
    print(f"  avg per non-empty window:  {total / len(vals):.1f}")
    print(f"  median / max:              {sorted(vals)[len(vals) // 2]} / {max(vals)}")


if __name__ == "__main__":
    rows = load_rows()
    timestamps = sorted(int(r["timestamp"]) for r in rows)
    events = hashtag_event_timestamps(rows)
    verified = sum(1 for r in rows if r["verified"] == "True")
    scores = [int(r["score"]) for r in rows]
    tags_per_tweet = [len([t for t in r["hashtags"].split("|") if t]) for r in rows]

    span_ms = timestamps[-1] - timestamps[0]
    span_h = span_ms / 3_600_000

    print(f"File: {INPUT.name}")
    print(f"Tweets:              {len(rows):,}")
    print(f"Hashtag events:      {len(events):,}  (what the map step emits)")
    print(f"Verified tweets:     {verified:,}  ({100 * verified / len(rows):.1f}%)")
    print(f"Time range:          {fmt_ts(timestamps[0])}  ->  {fmt_ts(timestamps[-1])}")
    print(f"Span:                {span_h:.1f} hours ({span_h / 24:.1f} days)")
    print(f"Mean rate:             {len(rows) / span_h:.2f} tweets/hr, {len(events) / span_h:.2f} hashtag-events/hr")
    print(f"Hashtags per tweet:  avg {sum(tags_per_tweet) / len(tags_per_tweet):.2f}, max {max(tags_per_tweet)}")
    print(f"Score:               min {min(scores)}, max {max(scores)}, avg {sum(scores) / len(scores):.1f}")

    print("\n--- Hashtag events per window (event-time buckets) ---")
    for sec in WINDOW_SIZES:
        counts = bucket_counts(events, sec)
        possible = span_ms // (sec * 1000) + 1
        pct = 100 * len(counts) / possible
        summarize_buckets(
            counts,
            f"{sec}s windows: {len(counts):,} non-empty / {possible:,} possible ({pct:.2f}%)",
        )

    print("\n--- 5s window verdict ---")
    c5 = bucket_counts(events, 5)
    possible5 = span_ms // 5000 + 1
    print(f"  Only {100 * len(c5) / possible5:.2f}% of 5s windows have any data.")
    print(f"  Data spans {span_h / 24:.0f} days - tweets arrive in bursts, not every 5 seconds.")
    print(f"  For meaningful averages, try 1h (3600s) or 6h (21600s) windows.")

"""Convert raw tweet CSV into a single workflow-ready file used by ALL dataflows.

Input:  tweets_hashtags_100K.csv  (raw scrape)
Output: twitter.txt               (sorted by timestamp, no header)

Columns (comma-separated):
  tweetId, timestamp, hashtags, likes, retweets, isVerified, isBlueVerified, location

- timestamp:      UTC epoch milliseconds
- hashtags:       pipe-separated (e.g. Bitcoin|Crypto), empty string if none
- likes:          integer
- retweets:       integer
- isVerified:     true/false
- isBlueVerified: true/false
- location:       author profile location (commas replaced with spaces), empty if absent

Each dataflow computes what it needs from these raw columns:
  twitter_1: score = likes + retweets; verified = isVerified OR isBlueVerified
  twitter_2: hasLocation = (location != "")
"""

from __future__ import annotations
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent
INPUT_FILE = DATA_DIR / "tweets_hashtags_100K.csv"
OUTPUT_FILE = DATA_DIR / "twitter.txt"


def parse_timestamp_ms(value: str) -> int:
    dt = datetime.strptime(value.strip(), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def parse_int(value: str) -> int:
    if value is None or value == "":
        return 0
    return int(float(value))


def parse_hashtags(entities_json: str) -> str:
    if not entities_json:
        return ""
    try:
        entities = json.loads(entities_json)
    except (json.JSONDecodeError, ValueError):
        return ""
    # Dedup, preserving order: raw scrapes list the same hashtag twice for
    # ~2.6% of tweets (e.g. "PEPE|PEPE"); one tweet mentions a tag once.
    tags = list(dict.fromkeys(
        tag["text"] for tag in entities.get("hashtags", []) if tag.get("text")))
    return "|".join(tags)


def parse_author(author_json: str) -> tuple[bool, bool, str]:
    """Returns (isVerified, isBlueVerified, location)."""
    if not author_json:
        return False, False, ""
    try:
        author = json.loads(author_json)
    except (json.JSONDecodeError, ValueError):
        return False, False, ""
    is_verified = bool(author.get("isVerified"))
    is_blue = bool(author.get("isBlueVerified"))
    location = author.get("location", "").strip().replace(",", " ")
    return is_verified, is_blue, location


def convert_row(row: dict[str, str]) -> str:
    tweet_id = row["id"]
    timestamp = parse_timestamp_ms(row["createdatutc"])
    hashtags = parse_hashtags(row.get("entities", ""))
    likes = parse_int(row.get("likecount"))
    retweets = parse_int(row.get("retweetcount"))
    is_verified, is_blue, location = parse_author(row.get("author", ""))

    return (f"{tweet_id},{timestamp},{hashtags},{likes},{retweets},"
            f"{str(is_verified).lower()},{str(is_blue).lower()},{location}")


if __name__ == "__main__":
    rows: list[tuple[int, str]] = []

    with INPUT_FILE.open(newline="", encoding="utf-8", errors="replace") as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            line = convert_row(row)
            ts = int(line.split(",", 3)[1])
            rows.append((ts, line))

    rows.sort(key=lambda x: x[0])

    # Make timestamps unique: the raw data has second precision, so ties are
    # common (3.3K shared timestamps). Bump ties by +1 ms each, preserving
    # order and staying within the same second. Distinct timestamps guarantee
    # no two tuples are value-equal downstream.
    prev_ts = -1
    for i, (ts, line) in enumerate(rows):
        if ts <= prev_ts:
            ts = prev_ts + 1
            tweet_id, _, rest = line.split(",", 2)
            rows[i] = (ts, f"{tweet_id},{ts},{rest}")
        prev_ts = ts

    with OUTPUT_FILE.open("w", encoding="utf-8") as out:
        for _, line in rows:
            out.write(line + "\n")

    print(f"Wrote {len(rows):,} tweets to {OUTPUT_FILE.name}")
    print(f"Format: tweetId,timestamp,hashtags,likes,retweets,isVerified,isBlueVerified,location")

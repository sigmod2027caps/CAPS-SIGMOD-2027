"""Generate Nexmark-style Person and Auction streams for Q8 and Q3.

Usage:
    python generate_nexmark.py                 # defaults: 500K persons, 2M auctions
    python generate_nexmark.py 100000 500000   # custom counts

Outputs (workflow-ready, comma-separated, sorted by timestamp):
  persons.txt:  personID,ts_ms,name,city,state
  auctions.txt: auctionID,ts_ms,sellerID,category,itemName

Timestamps are epoch milliseconds and globally unique across BOTH files
(duplicates bumped by 1 ms), so no two stream tuples are ever fully
value-equal — required for the exact cross-checking of provenance answers
between CAPS and GeneaLog.
"""

import os
import sys
import random

random.seed(42)

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
PERSONS_FILE = os.path.join(DATA_DIR, "persons.txt")
AUCTIONS_FILE = os.path.join(DATA_DIR, "auctions.txt")

DEFAULT_N_PERSONS = 500_000
DEFAULT_N_AUCTIONS = 2_000_000

# Q3 needs persons in OR, ID, CA — make those states frequent
US_STATES = [
    "OR", "ID", "CA", "WA", "NV", "AZ", "TX", "NY", "FL", "IL",
    "PA", "OH", "GA", "NC", "MI", "NJ", "VA", "MA", "CO", "TN",
]
# Boost OR/ID/CA to ~30% combined
STATE_WEIGHTS = [
    12, 10, 12, 5, 3, 4, 5, 5, 5, 4,
    3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
]

CITIES = {
    "OR": ["Portland", "Eugene", "Salem", "Bend"],
    "ID": ["Boise", "Meridian", "Nampa", "Idaho Falls"],
    "CA": ["Los Angeles", "San Francisco", "San Diego", "San Jose"],
    "WA": ["Seattle", "Tacoma", "Spokane"],
    "NV": ["Las Vegas", "Reno"],
    "AZ": ["Phoenix", "Tucson", "Mesa"],
    "TX": ["Houston", "Austin", "Dallas", "San Antonio"],
    "NY": ["New York", "Buffalo", "Albany"],
    "FL": ["Miami", "Orlando", "Tampa"],
}

FIRST_NAMES = [
    "Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Hank",
    "Ivy", "Jack", "Kate", "Leo", "Mia", "Noah", "Olivia", "Pete",
    "Quinn", "Rosa", "Sam", "Tina", "Uma", "Vic", "Wendy", "Xander",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Lopez", "Wilson", "Anderson", "Thomas",
]

# Categories 1-15; Q3 filters category == 10
CATEGORIES = list(range(1, 16))
CATEGORY_WEIGHTS = [6] * 15
CATEGORY_WEIGHTS[9] = 20  # boost category 10

ITEM_NOUNS = ["Widget", "Gadget", "Device", "Tool", "Module", "Unit", "Part", "Kit"]
ITEM_ADJS = ["Premium", "Basic", "Pro", "Ultra", "Mini", "Mega", "Smart", "Turbo"]

# Time range: 30 days starting 2025-01-01 UTC, epoch milliseconds
BASE_MS = 1_735_689_600_000
SPAN_MS = 30 * 24 * 3600 * 1000
HOUR_MS = 3600 * 1000


def generate_persons(n):
    persons = []
    for pid in range(1, n + 1):
        state = random.choices(US_STATES, weights=STATE_WEIGHTS, k=1)[0]
        city = random.choice(CITIES.get(state, ["Springfield"]))
        name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
        reg_ms = BASE_MS + random.randint(0, SPAN_MS)
        persons.append([pid, reg_ms, name, city, state])
    return persons


def generate_auctions(n, persons):
    """~60% of sellers are existing persons opening within 24h of their
    registration (about half of those inside the 12h Q8 horizon)."""
    auctions = []
    for aid in range(1, n + 1):
        category = random.choices(CATEGORIES, weights=CATEGORY_WEIGHTS, k=1)[0]
        item_name = f"{random.choice(ITEM_ADJS)} {random.choice(ITEM_NOUNS)} #{aid}"
        if random.random() < 0.6:
            person = random.choice(persons)
            seller_id = person[0]
            offset_h = min(random.expovariate(1.0 / 6.0), 24.0)
            open_ms = person[1] + int(offset_h * HOUR_MS)
        else:
            seller_id = len(persons) + random.randint(1, 100_000)
            open_ms = BASE_MS + random.randint(0, SPAN_MS)
        auctions.append([aid, open_ms, seller_id, category, item_name])
    return auctions


def uniquify_timestamps(persons, auctions):
    """Make timestamps unique across BOTH streams by bumping duplicates
    by 1 ms (keeps them sorted)."""
    events = [(p[1], 0, p) for p in persons] + [(a[1], 1, a) for a in auctions]
    events.sort(key=lambda e: (e[0], e[1], e[2][0]))
    bumped = 0
    last = -1
    for ts, _, rec in events:
        ts = max(rec[1], last + 1)
        if ts != rec[1]:
            bumped += 1
        rec[1] = ts
        last = ts
    return bumped


if __name__ == "__main__":
    n_persons = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_N_PERSONS
    n_auctions = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_N_AUCTIONS

    print(f"Generating Nexmark data: {n_persons:,} persons, {n_auctions:,} auctions")
    persons = generate_persons(n_persons)
    auctions = generate_auctions(n_auctions, persons)
    bumped = uniquify_timestamps(persons, auctions)
    print(f"  bumped {bumped:,} duplicate timestamps")

    persons.sort(key=lambda p: p[1])
    auctions.sort(key=lambda a: a[1])

    with open(PERSONS_FILE, "w") as f:
        for pid, ts, name, city, state in persons:
            f.write(f"{pid},{ts},{name},{city},{state}\n")
    print(f"  Wrote {len(persons):,} persons -> {PERSONS_FILE}")

    with open(AUCTIONS_FILE, "w") as f:
        for aid, ts, seller, category, item in auctions:
            f.write(f"{aid},{ts},{seller},{category},{item}\n")
    print(f"  Wrote {len(auctions):,} auctions -> {AUCTIONS_FILE}")

    # Quick stats for Q8/Q3
    reg = {p[0]: p[1] for p in persons}
    q8_hits = sum(
        1 for aid, ts, seller, cat, item in auctions
        if seller in reg and 0 <= ts - reg[seller] <= 12 * HOUR_MS)
    q3_persons = sum(1 for p in persons if p[4] in ("OR", "ID", "CA"))
    q3_auctions = sum(1 for a in auctions if a[3] == 10)
    print(f"\n  Q8 stats: {q8_hits:,} auctions opened within 12h of their "
          f"seller's registration")
    print(f"  Q3 stats: {q3_persons:,} persons in OR/ID/CA, "
          f"{q3_auctions:,} auctions in category 10")
    print("\nDone.")

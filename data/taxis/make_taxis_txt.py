"""Build taxis.txt (the workflow input) from the 2013 FOIL NYC taxi release.

Input (January 2013, extracted from the archive.org FOIL dump):
  old_data/trip_data_1.csv  medallion, hack_license, vendor_id, rate_code,
                            store_and_fwd_flag, pickup_datetime,
                            dropoff_datetime, passenger_count,
                            trip_time_in_secs, trip_distance,
                            pickup_longitude/latitude, dropoff_longitude/latitude
  old_data/trip_fare_1.csv  medallion, hack_license, vendor_id,
                            pickup_datetime, payment_type, fare_amount,
                            surcharge, mta_tax, tip_amount, tolls_amount,
                            total_amount

The two files describe the same rides, so they are joined on
(medallion, hack_license, pickup_datetime).

Processing:
  - join trips with fares to get payment type / fare / tip per ride
  - keep rides with valid coordinates inside the NYC bounding box and
    at least one passenger
  - map the pickup coordinates to a 40x25 = 1000-cell grid (the paper:
    "We divided the area of NY into 1000 distinct regions")
  - map each medallion (32-char hash) to a compact integer taxi id
  - sort by pickup time, cap at MAX_ROWS, and bump duplicate timestamps
    by +1ms so every ride has a unique timestamp (exact verification
    across systems needs unique keys)

Output, one ride per line, sorted by timestamp:
  taxi_id,ts_ms,passengers,zone,payment,fare,tip
  payment is CRD / CSH / OTH.

Usage: python make_taxis_txt.py [max_rows]   (default 10_000_000)
"""
import os
import sys
import pandas as pd

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
TRIP_CSV = os.path.join(DATA_DIR, "old_data", "trip_data_1.csv")
FARE_CSV = os.path.join(DATA_DIR, "old_data", "trip_fare_1.csv")
OUT_TXT = os.path.join(DATA_DIR, "taxis.txt")
MAX_ROWS = int(sys.argv[1]) if len(sys.argv) > 1 else 10_000_000

# NYC bounding box and the 1000-region grid over it.
LON_MIN, LON_MAX = -74.05, -73.75
LAT_MIN, LAT_MAX = 40.58, 40.92
LON_CELLS, LAT_CELLS = 40, 25

KEY = ["medallion", "hack_license", "pickup_datetime"]


if __name__ == "__main__":
    print("reading trips ...")
    trips = pd.read_csv(
        TRIP_CSV,
        usecols=KEY + ["passenger_count", "pickup_longitude", "pickup_latitude"],
        dtype={"medallion": str, "hack_license": str, "pickup_datetime": str},
    )
    print(f"  {len(trips):,} rides")

    print("reading fares ...")
    fares = pd.read_csv(
        FARE_CSV,
        skipinitialspace=True,
        usecols=KEY + ["payment_type", "fare_amount", "tip_amount"],
        dtype={"medallion": str, "hack_license": str, "pickup_datetime": str},
    )
    print(f"  {len(fares):,} fares")

    # The join key must be unique on both sides for a clean 1:1 merge.
    trips = trips.drop_duplicates(subset=KEY, keep="first")
    fares = fares.drop_duplicates(subset=KEY, keep="first")

    print("joining ...")
    df = trips.merge(fares, on=KEY, how="inner")
    print(f"  {len(df):,} joined rides")
    del trips, fares

    print("filtering ...")
    df = df[
        (df["passenger_count"] >= 1)
        & (df["pickup_longitude"] >= LON_MIN) & (df["pickup_longitude"] < LON_MAX)
        & (df["pickup_latitude"] >= LAT_MIN) & (df["pickup_latitude"] < LAT_MAX)
        & (df["fare_amount"] >= 0) & (df["tip_amount"] >= 0)
    ]
    print(f"  {len(df):,} rides after filters")

    print("deriving columns ...")
    lon_bin = ((df["pickup_longitude"] - LON_MIN) / (LON_MAX - LON_MIN) * LON_CELLS).astype(int)
    lat_bin = ((df["pickup_latitude"] - LAT_MIN) / (LAT_MAX - LAT_MIN) * LAT_CELLS).astype(int)
    df["zone"] = lat_bin * LON_CELLS + lon_bin
    df["payment"] = df["payment_type"].where(df["payment_type"].isin(["CRD", "CSH"]), "OTH")
    # Unit-independent epoch-ms (pandas may parse at s/us/ns resolution).
    df["ts"] = (pd.to_datetime(df["pickup_datetime"])
                - pd.Timestamp("1970-01-01")) // pd.Timedelta(milliseconds=1)

    print("sorting, capping, uniquifying timestamps ...")
    df = df.sort_values("ts", kind="mergesort").head(MAX_ROWS)
    ts = df["ts"].to_numpy().copy()
    bumped = 0
    for i in range(1, len(ts)):
        if ts[i] <= ts[i - 1]:
            ts[i] = ts[i - 1] + 1
            bumped += 1
    df["ts"] = ts
    print(f"  {bumped:,} timestamps bumped (+1ms) to be unique")

    df["taxi_id"] = pd.factorize(df["medallion"])[0]
    print(f"  {df['taxi_id'].nunique():,} distinct taxis, {df['zone'].nunique()} zones in use")
    print(f"  payment breakdown:\n{df['payment'].value_counts().to_string()}")

    print(f"writing {OUT_TXT} ...")
    out = df[["taxi_id", "ts", "passenger_count", "zone", "payment", "fare_amount", "tip_amount"]]
    out.to_csv(OUT_TXT, index=False, header=False, float_format="%.2f")
    print(f"done: {len(out):,} lines")

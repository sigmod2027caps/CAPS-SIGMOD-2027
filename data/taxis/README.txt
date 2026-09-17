NYC Yellow Taxi Dataset (2013 FOIL release)
============================================

Source: the 2013 NYC taxi trip data obtained by FOIL request, hosted at
https://archive.org/details/nycTaxiTripData2013 (trip_data.7z, trip_fare.7z).
Unlike the current official TLC files, this release still contains the taxi
identifier (medallion), which the taxi_1 workflow groups and joins on. The
official releases (all years, including re-processed 2013) had it scrubbed
for privacy.

Build the workflow input (needs ~5 GB of free disk and p7zip):

  ./download.sh            # fetches and extracts the January 2013 files
  python3 make_taxis_txt.py

Files:
  old_data/trip_data_1.csv   January 2013 rides (14.8M): medallion,
                             hack_license, pickup/dropoff datetime,
                             passenger_count, pickup/dropoff lat/lon
  old_data/trip_fare_1.csv   January 2013 fares for the same rides:
                             payment_type (CRD/CSH/...), fare_amount,
                             tip_amount
  make_taxis_txt.py          joins the two on (medallion, hack_license,
                             pickup_datetime), filters to the NYC bounding
                             box and >=1 passenger, maps pickup coordinates
                             to a 40x25 = 1000-region grid (paper: "We
                             divided the area of NY into 1000 distinct
                             regions"), maps medallions to compact integer
                             taxi ids, sorts by pickup time, caps at 10M
                             rides, and bumps duplicate timestamps by +1ms
                             so every ride has a unique timestamp (needed
                             for exact cross-system verification)
  taxis.txt                  the workflow input produced by the script:
                             taxi_id,ts_ms,passengers,zone,payment,fare,tip
                             (payment: CRD / CSH / OTH)

The script has no randomness and sorts with a stable mergesort, so the same
input CSVs always produce a byte-identical taxis.txt. The archive.org files
are a frozen 2013 snapshot, so the numbers reported in the paper are
reproducible from this recipe.

Used in workflows:
  taxi_1  per 1-hour window: taxis with exactly one 1-passenger ride and
          at least two rides with more than two passengers
  taxi_2  card vs cash virtual sources: avg tip (card) and avg fare (cash)
          per (zone, hour), joined on (zone, hour)

To regenerate with fewer rides: python3 make_taxis_txt.py 1000000

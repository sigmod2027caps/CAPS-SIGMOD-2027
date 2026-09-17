#!/usr/bin/env bash
# Fetches the January 2013 NYC yellow taxi trip and fare files from the
# archive.org FOIL release and extracts them into old_data/, which is what
# make_taxis_txt.py reads. Needs curl and 7z (p7zip-full).
#
# Resumes incomplete downloads and re-fetches archives that are truncated
# or fail to open with 7z (common when a previous curl was interrupted).
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
BASE="https://archive.org/download/nycTaxiTripData2013"
OUT="$DIR/old_data"

command -v 7z >/dev/null || { echo "7z not found: apt install p7zip-full" >&2; exit 1; }

mkdir -p "$OUT" "$DIR/archive"

remote_size() {
  local url=$1
  curl -fsSL -I "$url" | tr -d '\r' | awk 'tolower($1)=="content-length:" {print $2; exit}'
}

ensure_archive() {
  local name=$1
  local path="$DIR/archive/$name.7z"
  local url="$BASE/$name.7z"
  local expected local_size

  expected="$(remote_size "$url" || true)"
  local_size=0
  [[ -f "$path" ]] && local_size=$(stat -c%s "$path")

  if [[ -n "$expected" && "$local_size" -eq "$expected" ]]; then
    if 7z t "$path" >/dev/null 2>&1; then
      return 0
    fi
    echo "corrupt $name.7z; re-downloading ..."
    rm -f "$path"
    local_size=0
  elif [[ -f "$path" && -n "$expected" && "$local_size" -lt "$expected" ]]; then
    echo "incomplete $name.7z (${local_size}/${expected} bytes); resuming ..."
  elif [[ ! -f "$path" ]]; then
    echo "downloading $name.7z ..."
  elif [[ -z "$expected" ]]; then
    # Could not read Content-Length; trust a file that 7z can open.
    if 7z t "$path" >/dev/null 2>&1; then
      return 0
    fi
    echo "unreadable $name.7z; re-downloading ..."
    rm -f "$path"
  else
    echo "size mismatch for $name.7z (${local_size} vs ${expected}); re-downloading ..."
    rm -f "$path"
  fi

  curl -fL --retry 5 --retry-delay 2 -C - -o "$path" "$url"

  local_size=$(stat -c%s "$path")
  if [[ -n "$expected" && "$local_size" -ne "$expected" ]]; then
    echo "ERROR: $name.7z still incomplete (${local_size}/${expected} bytes)" >&2
    exit 1
  fi
  if ! 7z t "$path" >/dev/null 2>&1; then
    echo "ERROR: $name.7z failed integrity check after download" >&2
    exit 1
  fi
}

for archive in trip_data trip_fare; do
  ensure_archive "$archive"
  # Each archive holds one CSV per month; only January is used.
  echo "extracting ${archive}_1.csv ..."
  7z e -y -o"$OUT" "$DIR/archive/$archive.7z" "${archive}_1.csv" >/dev/null
done

ls -la "$OUT"
echo "Now run: python3 make_taxis_txt.py"

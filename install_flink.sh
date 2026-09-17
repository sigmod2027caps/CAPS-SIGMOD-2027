#!/usr/bin/env bash
# Downloads Apache Flink 1.10.0 into code/flink-1.10.0 and configures it for
# the experiments. Flink 1.10 is the version GeneaLog was developed on, which
# is why the comparison uses it; it requires Java 8.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
VERSION=1.10.0
TARGET="$ROOT/code/flink-$VERSION"
ARCHIVE="flink-$VERSION-bin-scala_2.11.tgz"
MIRROR="https://archive.apache.org/dist/flink/flink-$VERSION/$ARCHIVE"

if [[ -d "$TARGET" ]]; then
  echo "Flink already installed at $TARGET"
  exit 0
fi

mkdir -p "$ROOT/code"
cd "$ROOT/code"

# The release is checked against the sha512 the Apache archive publishes next to
# it. A resumed transfer can be silently wrong, and tar alone does not catch it.
SHA512=feb80a1a64a0ef50ff7c1dec07071c6c22cdb37fac53961c9ebd1f1e15c3b7530893b9f90ead1d23951487bb5ebf0fb7bb02f3055fadfa7d36fae0bdfc0a1502

verified() {
  [[ -s "$ARCHIVE" ]] || return 1
  echo "$SHA512  $ARCHIVE" | sha512sum --check --status
}

# The Apache archive is slow and closes the connection in the middle, so we
# resume instead of starting from zero. A transfer that crawls under 30 kB/s for
# a minute is dropped on purpose and the next attempt continues from there.
# Any tarball you place in code/ yourself is picked up here, no download at all.
if verified; then
  echo "$ARCHIVE already downloaded and verified"
else
  # Keeps resuming on its own until the deadline, so one ./run.sh is enough even
  # when the archive gives us 50 kB/s. Raise FLINK_DOWNLOAD_MINUTES if needed.
  deadline=$(( $(date +%s) + 60 * ${FLINK_DOWNLOAD_MINUTES:-120} ))
  echo "downloading $ARCHIVE (resumes by itself, up to ${FLINK_DOWNLOAD_MINUTES:-120} min) ..."
  attempt=1
  until verified; do
    if (( $(date +%s) > deadline )); then
      echo "could not download $ARCHIVE in ${FLINK_DOWNLOAD_MINUTES:-120} minutes" >&2
      echo "  the partial file is kept, run ./run.sh again to resume from there" >&2
      echo "  or put $MIRROR in $ROOT/code/ by hand" >&2
      exit 1
    fi
    echo "  attempt $attempt ..."
    curl -fL -C - --speed-limit 30000 --speed-time 60 -o "$ARCHIVE" "$MIRROR" || true
    # A complete but corrupted file can not be resumed, it must go.
    if [[ -s "$ARCHIVE" ]] && ! verified; then
      size=$(stat -c%s "$ARCHIVE")
      if [[ $size -ge 275000000 ]]; then
        echo "  checksum mismatch on a complete file, starting again" >&2
        rm -f "$ARCHIVE"
      fi
    fi
    attempt=$((attempt + 1))
  done
fi

tar xzf "$ARCHIVE"
rm -f "$ARCHIVE"

CONF="$TARGET/conf/flink-conf.yaml"

# The GeneaLog query baseline retains every output tuple and its provenance
# graph in the sink, which needs a large task manager heap; the heartbeat
# timeout is raised so that a long stop-the-world collection does not look
# like a dead task manager.
python3 - "$CONF" <<'EOF'
import re, sys

path = sys.argv[1]
settings = {
    "jobmanager.heap.size": "2048m",
    "taskmanager.memory.process.size": "48g",
    "taskmanager.numberOfTaskSlots": "8",
    "heartbeat.timeout": "300000",
}

with open(path) as handle:
    lines = handle.read().splitlines()

for key, value in settings.items():
    pattern = re.compile(rf"^#?\s*{re.escape(key)}\s*:.*$")
    for i, line in enumerate(lines):
        if pattern.match(line):
            lines[i] = f"{key}: {value}"
            break
    else:
        lines.append(f"{key}: {value}")

with open(path, "w") as handle:
    handle.write("\n".join(lines) + "\n")
EOF

echo "Flink $VERSION installed at $TARGET"
grep -E "heap.size|process.size|numberOfTaskSlots|heartbeat.timeout" "$CONF"

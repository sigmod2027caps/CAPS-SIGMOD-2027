#!/usr/bin/env bash
# Sourced by run.sh. Makes sure Java 8, Maven and 7-Zip are available. Whatever the machine already has is used;
# whatever is missing is downloaded into tools/ (no root rights needed). Only Linux, bash, curl, tar and Python 3
# are assumed. CAPS_OWN_TOOLS=1 always uses the downloaded tools.

TOOLS="$ROOT/tools"
case "$(uname -m)" in
  x86_64|amd64)  JDK_ARCH=x64;     SZ_ARCH=x64 ;;
  aarch64|arm64) JDK_ARCH=aarch64; SZ_ARCH=arm64 ;;
  *) echo "unsupported CPU architecture $(uname -m)" >&2; return 1 ;;
esac

# fetch <file> <url> [<mirror url> ...]: download with resume; servers that drop the connection are retried, then the
# next mirror is tried. Fails only when every attempt on every URL failed.
RETRY_ALL=(); curl --help all 2>/dev/null | grep -q -- --retry-all-errors && RETRY_ALL=(--retry-all-errors)
fetch() {
  local file=$1 url attempt; shift
  for attempt in 1 2 3 4 5; do
    for url in "$@"; do
      curl -fL --retry 5 --retry-delay 3 "${RETRY_ALL[@]}" --connect-timeout 30 -C - -o "$file" "$url" && return 0
      # keep the partial file for the next attempt, unless the server refused to resume it (33) or it is complete/bad (22)
      case $? in 33|22) rm -f "$file" ;; esac
      echo "  download failed ($url), retrying ..." >&2
      sleep 5
    done
  done
  echo "could not download $file" >&2
  return 1
}

is_java8() { [[ -x "$1/bin/java" && -x "$1/bin/javac" ]] && "$1/bin/java" -version 2>&1 | grep -q 'version "1\.8'; }

# --- Java 8 (Flink 1.10 does not run on newer JDKs) ---
java8=""
if [[ ${CAPS_OWN_TOOLS:-0} != 1 ]]; then
  for cand in "${JAVA_HOME:-}" /usr/lib/jvm/java-8-openjdk-* /usr/lib/jvm/java-1.8.0-openjdk* /usr/lib/jvm/temurin-8-jdk*; do
    [[ -n $cand ]] && is_java8 "$cand" && { java8=$cand; break; }
  done
fi
if [[ -z $java8 ]]; then
  java8="$TOOLS/jdk8"
  if ! is_java8 "$java8"; then
    echo "Java 8 not found: downloading a JDK 8 (Eclipse Temurin) into tools/jdk8 ..."
    rm -rf "$java8" "$TOOLS/jdk8.tar.gz" && mkdir -p "$java8"
    fetch "$TOOLS/jdk8.tar.gz" "https://api.adoptium.net/v3/binary/latest/8/ga/linux/$JDK_ARCH/jdk/hotspot/normal/eclipse"
    tar -xzf "$TOOLS/jdk8.tar.gz" -C "$java8" --strip-components=1 && rm -f "$TOOLS/jdk8.tar.gz"
    is_java8 "$java8" || { echo "the downloaded JDK 8 does not work" >&2; return 1; }
  fi
fi
export JAVA_HOME="$java8"
export PATH="$JAVA_HOME/bin:$PATH"
echo "Java 8: $JAVA_HOME"

# --- Maven ---
if [[ ${CAPS_OWN_TOOLS:-0} == 1 ]] || ! command -v mvn >/dev/null 2>&1; then
  if [[ ! -x "$TOOLS/maven/bin/mvn" ]]; then
    echo "Maven not found: downloading Maven 3.9.9 into tools/maven ..."
    rm -rf "$TOOLS/maven" "$TOOLS/maven.tar.gz" && mkdir -p "$TOOLS/maven"
    fetch "$TOOLS/maven.tar.gz" \
      "https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/3.9.9/apache-maven-3.9.9-bin.tar.gz" \
      "https://archive.apache.org/dist/maven/maven-3/3.9.9/binaries/apache-maven-3.9.9-bin.tar.gz"
    tar -xzf "$TOOLS/maven.tar.gz" -C "$TOOLS/maven" --strip-components=1 && rm -f "$TOOLS/maven.tar.gz"
  fi
  export PATH="$TOOLS/maven/bin:$PATH"
fi
echo "Maven: $(command -v mvn)"

# mvn wrapper: on a slow or unstable network a dependency download can time out or be reset. Such builds are retried
# (with -U, so Maven does not keep the cached failure); other build errors stop at once.
real_mvn=$(command -v mvn)
if [[ $real_mvn != "$TOOLS/bin/mvn" ]]; then
  mkdir -p "$TOOLS/bin"
  cat > "$TOOLS/bin/mvn" <<EOF
#!/usr/bin/env bash
log=\$(mktemp); extra=()
for attempt in 1 2 3 4 5 6 7 8; do
  "$real_mvn" -Dmaven.wagon.http.retryHandler.count=10 -Daether.connector.http.retryHandler.count=10 \\
    -Daether.connector.connectTimeout=60000 "\${extra[@]}" "\$@" 2>&1 | tee "\$log"
  status=\${PIPESTATUS[0]}
  [[ \$status == 0 ]] && break
  # only network errors are retried; a missing local module ("Could not resolve dependencies") fails at once
  grep -qE "Could not transfer|timed out|Connection reset|Connect(ion)? refused|Remote host terminated|UnknownHost|Network is unreachable" "\$log" || break
  echo "  Maven could not download a dependency (attempt \$attempt), retrying ..." >&2
  extra=(-U); sleep 15
done
rm -f "\$log"; exit \$status
EOF
  chmod +x "$TOOLS/bin/mvn"
  export PATH="$TOOLS/bin:$PATH"
fi

# --- 7-Zip (only for the taxi archives) ---
if [[ ${CAPS_OWN_TOOLS:-0} == 1 ]] || ! command -v 7z >/dev/null 2>&1; then
  if [[ ! -x "$TOOLS/bin/7z" ]]; then
    echo "7z not found: downloading 7-Zip 25.01 into tools/ ..."
    rm -rf "$TOOLS/7zip" "$TOOLS/7zip.tar.xz" && mkdir -p "$TOOLS/7zip" "$TOOLS/bin"
    fetch "$TOOLS/7zip.tar.xz" "https://www.7-zip.org/a/7z2501-linux-$SZ_ARCH.tar.xz" \
      "https://github.com/ip7z/7zip/releases/download/25.01/7z2501-linux-$SZ_ARCH.tar.xz"
    tar -xJf "$TOOLS/7zip.tar.xz" -C "$TOOLS/7zip" && rm -f "$TOOLS/7zip.tar.xz"
    printf '#!/bin/sh\nexec "%s" "$@"\n' "$TOOLS/7zip/7zz" > "$TOOLS/bin/7z" && chmod +x "$TOOLS/bin/7z"
  fi
  export PATH="$TOOLS/bin:$PATH"
fi
echo "7z: $(command -v 7z)"

# A plain Ubuntu with only the basics that every Linux installation has. Java 8, Maven, 7-Zip and the Python
# packages are installed by run.sh itself (bootstrap_tools.sh), exactly as on a machine without Docker.
FROM ubuntu:24.04
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      bash curl ca-certificates python3 xz-utils procps iproute2 util-linux \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /caps

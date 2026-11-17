#!/usr/bin/env bash
# Runs ./run.sh inside a Docker container (docker/Dockerfile). The repository is mounted, so the inputs, the tools
# and every result (CSV, PDF, LaTeX) end up in this directory, exactly as without Docker. The same environment
# variables as for run.sh can be given, e.g.:
#   ./docker_run.sh                      everything (the numbers of the paper)
#   SMALL=1 REPS=1 ./docker_run.sh       quick check on the 10% subsets
#   PHASES=plot ./docker_run.sh          after a run: redraw the figures and tables from its results
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
docker build -q -t caps-artifact "$ROOT/docker" >/dev/null
TTY=(); [[ -t 0 && -t 1 ]] && TTY=(-it)
ENVS=()
for v in SMALL REPS PHASES DATAFLOWS EXPERIMENTS CAPS_PARALLELISM TM_MEMORY SKIP_BUILD M_LIST NESTED_M_LIST HEAP_REPS LOCAL_CORES; do
  [[ -n ${!v:-} ]] && ENVS+=(-e "$v=${!v}")
done
# the image has no user for the host's UID (501 on a Mac); without one Maven and jcmd/jstat misbehave
mkdir -p "$ROOT/tools"
printf 'root:x:0:0::/root:/bin/bash\ncaps:x:%s:%s::/caps/tools/home:/bin/bash\n' "$(id -u)" "$(id -g)" > "$ROOT/tools/passwd"
printf 'root:x:0:\ncaps:x:%s:\n' "$(id -g)" > "$ROOT/tools/group"
# ${A[@]+"${A[@]}"}: an empty array is an error under set -u in the bash 3.2 of macOS
exec docker run --rm ${TTY[@]+"${TTY[@]}"} --user "$(id -u):$(id -g)" -v "$ROOT":/caps \
  -v "$ROOT/tools/passwd":/etc/passwd:ro -v "$ROOT/tools/group":/etc/group:ro \
  -e HOME=/caps/tools/home ${ENVS[@]+"${ENVS[@]}"} \
  caps-artifact bash -c 'mkdir -p "$HOME" && ./run.sh'

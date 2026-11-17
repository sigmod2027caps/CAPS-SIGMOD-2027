#!/usr/bin/env bash
# One Flink job of the recovery experiment (E4), called by run.sh under `flock -o $LOCK_FILE`:
# start the standalone cluster, run the job (and the failure, if the kind has one), save the job's REST records, stop the
# cluster. Everything between cluster start and stop is inside the one lock, and a cluster is never left running.
#   run_one.sh <kind> <dataflow> <dir>
#     time_plain  CAPS (two-tier index only, as in the timing runs of the other experiments), no checkpointing
#     time_ckpt   the same with checkpointing every CKPT_MS
#     plain       CAPS with the scan baseline, the id log and the index digest, no checkpointing
#     ref         the same with checkpointing, no failure (the run (a) of the experiment)
#   The verification kinds (plain, ref, inject, kill) pace the sources so that the job lasts about PACE_S seconds: a
#   job that is over in two seconds has no middle to fail in. The time_* kinds are never paced.
#     inject      the same with checkpointing and an exception thrown once by the source after FAIL_FRAC of its records
#     kill        the same with checkpointing and kill -9 of the TaskManager once KILL_AFTER_CKPTS checkpoints have
#                 completed plus KILL_DELAY_MS; a replacement TaskManager is started by taskmanager.sh right after
# Environment from run.sh: FLINK_CONF_DIR (the cluster configuration of the experiment), CKPT_MS, RESTART_DELAY_MS,
# FAIL_FRAC, KILL_AFTER_CKPTS, KILL_DELAY_MS, PID_DIR, NUM_QUERIES, PACE_S.
set -euo pipefail
source "$(cd "$(dirname "$0")/.." && pwd)/common.sh"

kind=$1; dataflow=$2; dir=$3
rm -rf "$dir"; mkdir -p "$dir"
len=${QUERY_LEN_MS[$dataflow]}
rest="$FLINK_REST"

# The input whose reader injects the failure, and the number of records after which it throws.
fail_source=SOURCE; fail_file=$(dataflow_inputs "$dataflow" | awk '{print $NF}')
[[ $dataflow == nexmark_* ]] && fail_source=SOURCE-AUCTIONS
# (the p readers of a file take its lines in turn: the reader of subtask 0 emits about n / p records)
fail_at=$(awk -v n="$(wc -l < "$fail_file")" -v f="${FAIL_FRAC:-0.6}" -v p="$CAPS_PARALLELISM" 'BEGIN{printf "%d", n * f / p}')

json_get() { "$PYTHON" -c "import json,sys; d=json.load(sys.stdin); print($1)"; }

# the id of the job that is running (empty if none)
running_job() {
  curl -sf "$rest/jobs/overview" 2>/dev/null | json_get "next((j['jid'] for j in d['jobs'] if j['state'] in ('RUNNING','RESTARTING')), '')" 2>/dev/null || true
}

completed_checkpoints() { # <jid>
  curl -sf "$rest/jobs/$1/checkpoints" 2>/dev/null | json_get "d['counts']['completed']" 2>/dev/null || echo 0
}

now_ms() { date +%s%3N; }

taskmanager_pid() { tail -1 "$PID_DIR"/flink-*-taskexecutor.pid; }

"$FLINK_DIR/bin/stop-cluster.sh" > /dev/null 2>&1 9>&- || true
rm -rf "$PID_DIR"; mkdir -p "$PID_DIR"
sleep 3
"$FLINK_DIR/bin/start-cluster.sh" > "$dir/cluster.log" 2>&1 9>&-
trap '"$FLINK_DIR/bin/stop-cluster.sh" >> "$dir/cluster.log" 2>&1 9>&- || true' EXIT
sleep 8

# Another cluster on this machine that was started without the lock (same ports) would swallow our job, or the TaskManager
# we kill: check that the JobManager that listens on the RPC port is ours, and give the run back (status 75) if it is not.
jm=$(tail -1 "$PID_DIR"/flink-*-standalonesession.pid 2>/dev/null || true)
if [[ -z $jm ]] || ! kill -0 "$jm" 2>/dev/null || ! ss -ltnp 2>/dev/null | grep -E ':6123 ' | grep -q "pid=$jm,"; then
  echo "the ports of the cluster are taken by another cluster; try again later" >&2
  exit 75
fi

# --- the job ---
export CAPS_OUTPUT_LOG=0 CAPS_INDEX_DIGEST=0
ckpt=0
case $kind in
  time_plain) export CAPS_PROVISIONAL=0 CAPS_NO_SCAN=1; queries=0 ;;
  time_ckpt)  export CAPS_PROVISIONAL=0 CAPS_NO_SCAN=1; queries=0; ckpt=$CKPT_MS ;;
  plain)      export CAPS_OUTPUT_LOG=1 CAPS_INDEX_DIGEST=1; queries=$NUM_QUERIES ;;
  ref|inject|kill)
              export CAPS_OUTPUT_LOG=1 CAPS_INDEX_DIGEST=1; queries=$NUM_QUERIES; ckpt=$CKPT_MS ;;
  *) echo "unknown kind $kind" >&2; exit 2 ;;
esac
[[ $kind == time_* ]] || export CAPS_PACE_SECONDS=${PACE_S:-0} CAPS_WM_INTERVAL_MS=${VERIFY_WM_INTERVAL_MS:-$CAPS_WM_INTERVAL_MS}
export CAPS_CHECKPOINT_MS=$ckpt CAPS_RESTART_ATTEMPTS=3 CAPS_RESTART_DELAY_MS=${RESTART_DELAY_MS:-2000}
export CAPS_SEGMENT_DIR="$dir/segments"
if [[ $kind == inject ]]; then export CAPS_FAIL_AT=$fail_at CAPS_FAIL_SOURCE=$fail_source; fi

t_submit=$(now_ms)
run_caps "$dataflow" queries "$dir" "$queries" "$len" > "$dir/flink.log" 2>&1 9>&- &
job_pid=$!

t_kill=0; t_new_tm=0
if [[ $kind == kill ]]; then
  jid=""
  for _ in $(seq 1 300); do jid=$(running_job); [[ -n $jid ]] && break; sleep 0.2; done
  [[ -n $jid ]] || { echo "no running job to kill a TaskManager under" >&2; wait $job_pid || true; exit 3; }
  while (( $(completed_checkpoints "$jid") < ${KILL_AFTER_CKPTS:-2} )); do
    kill -0 $job_pid 2>/dev/null || { echo "the job finished before the kill point (use a shorter CKPT_MS)" >&2; exit 3; }
    sleep 0.1
  done
  sleep "$(awk -v d="${KILL_DELAY_MS:-0}" 'BEGIN{print d/1000}')"
  kill -0 $job_pid 2>/dev/null || { echo "the job finished before the kill point (use a shorter CKPT_MS)" >&2; exit 3; }
  tm=$(taskmanager_pid)
  tr '\0' ' ' < "/proc/$tm/cmdline" | grep -q -- "$FLINK_CONF_DIR" || { echo "pid $tm is not our TaskManager" >&2; exit 75; }
  t_kill=$(now_ms)
  kill -9 "$tm"
  echo "killed TaskManager $tm" >> "$dir/cluster.log"
  # standalone mode restarts nothing by itself: start a replacement TaskManager so that the restarted job has slots
  "$FLINK_DIR/bin/taskmanager.sh" start >> "$dir/cluster.log" 2>&1 9>&-
  t_new_tm=$(now_ms)
fi

rc=0; wait $job_pid || rc=$?
t_done=$(now_ms)
jobs_in_cluster=$(curl -sf "$rest/jobs/overview" | json_get "len(d['jobs'])" || echo 0)
jid=$(grep -oE 'JobID [0-9a-f]+' "$dir/flink.log" | tail -1 | awk '{print $2}' || true)
if [[ -n $jid ]]; then
  curl -sf "$rest/jobs/$jid" -o "$dir/job.json" || true
  curl -sf "$rest/jobs/$jid/checkpoints" -o "$dir/checkpoints.json" || true
  curl -sf "$rest/jobs/$jid/exceptions" -o "$dir/exceptions.json" || true
fi
printf 'kind=%s\ndataflow=%s\nsmall=%s\nckpt_ms=%s\nfail_at=%s\nfail_source=%s\nt_submit_ms=%s\nt_kill_ms=%s\nt_new_tm_ms=%s\nt_done_ms=%s\nrc=%s\n' \
  "$kind" "$dataflow" "$SMALL" "$ckpt" "$([[ $kind == inject ]] && echo $fail_at || echo 0)" "$fail_source" \
  "$t_submit" "$t_kill" "$t_new_tm" "$t_done" "$rc" > "$dir/run.txt"
runtime=$(awk '/^CAPS_NET_RUNTIME_MS/ {print $2}' "$dir/flink.log" | tail -1)
echo "${runtime:-NA}" > "$dir/net_runtime_ms.txt"
[[ $jobs_in_cluster == 1 ]] || { echo "$jobs_in_cluster jobs in the cluster: someone else submitted to it" >&2; exit 75; }
if [[ $kind == inject || $kind == kill ]]; then # a failure run without a failure is not a result
  "$PYTHON" -c "import json,sys; sys.exit(0 if json.load(open('$dir/exceptions.json')).get('timestamp') else 1)" \
    || { echo "no failure was recorded in the $kind run" >&2; exit 75; }
fi
[[ $rc == 0 ]] || { echo "job failed ($kind $dataflow), see $dir/flink.log" >&2; exit $rc; }

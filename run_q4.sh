#!/usr/bin/env bash
# Q4 end-to-end: main run (reducers limited to 512 MB = the memory proof) and a 1-reducer run (output must be identical).
# Run INSIDE the Ubuntu VM where Hadoop lives:   ./run_q4.sh <local q4 folder>
#   REDUCERS=8 ./run_q4.sh ~/q4data        # reducers of Job 1
# The local folder must contain the *.log click files (any sub-folders are searched, e.g. q4/clicks/*.log).
set -euo pipefail

LOCAL=${1:?usage: ./run_q4.sh <local q4 folder>}
USER_NAME=$(whoami)
BASE=/user/$USER_NAME/ca2/q4
JAR=target/ca2.jar
RED=${REDUCERS:-4}
RES=results_q4
rm -rf "$RES"; mkdir -p "$RES"

FILES=$(find "$LOCAL" -type f -iname "*.log" | sort)
[ -n "$FILES" ] || { echo "no *.log files under $LOCAL"; exit 1; }
echo "click files: $(echo "$FILES" | wc -l)   lines: $(cat $FILES | wc -l)"

hdfs dfs -rm -r -f "$BASE/input" || true
hdfs dfs -mkdir -p "$BASE/input/clicks"
for f in $FILES; do hdfs dfs -put -f "$f" "$BASE/input/clicks/"; done

run () {   # run <label> <-D options...>
  local label=$1; shift
  echo "=================== $label ==================="
  hadoop jar "$JAR" ca2.q4.Q4Driver "$@" "$BASE/input/clicks" "$BASE/out_$label" 2>&1 | tee "$RES/$label.log"
  rm -rf "$RES/$label"
  hdfs dfs -get "$BASE/out_$label" "$RES/$label"
}

# main: the reducer container is limited to 512 MB (heap 410 MB): proves that the reducer streams the events of a user
run main -D q4.reducers=$RED -D mapreduce.reduce.memory.mb=512 -D mapreduce.reduce.java.opts=-Xmx410m
run r1   -D q4.reducers=1

echo "=================== outputs identical? ==================="
for part in session_job/sessions session_job/bots; do
  if [ "$(cat $RES/main/$part/part-r-* | sort | md5sum)" = "$(cat $RES/r1/$part/part-r-* | sort | md5sum)" ]; then
    echo "IDENTICAL $part: $RED reducers vs 1 reducer"; else echo "DIFFERENT $part"; fi
done
[ "$(cat $RES/main/funnel/part-r-* | md5sum)" = "$(cat $RES/r1/funnel/part-r-* | md5sum)" ] && echo "IDENTICAL funnel" || echo "DIFFERENT funnel"

echo "=================== metrics for the report ==================="
{
  for v in main r1; do
    L=$RES/$v.log
    echo "--- $v"
    grep -E "map\(s\)|reduce\(s\)|Elapsed|GC_TIME|COMMITTED_HEAP|REDUCE_SHUFFLE_BYTES|SPILLED|reduce memory" $L | grep "\[Q4\]" | head -8
  done
  echo "--- largest sessions (events) :"
  cat $RES/main/session_job/bots/part-r-* $RES/main/session_job/sessions/part-r-* | sort -t$'\t' -k6,6nr | head -5 | cut -f1-7,10
} | tee "$RES/metrics.txt"

echo "=================== funnel report (date sessions home product cart success h>p p>c c>s h>s) ==================="
cat $RES/main/funnel/part-r-00000 | tee "$RES/funnel.txt"

echo "=================== first 20 session lines ==================="
cat $RES/main/session_job/sessions/part-r-* | sort -t$'\t' -k1,1 -k2,2n | head -20 | tee "$RES/first20_sessions.txt"
echo "=================== first 5 bot lines ==================="
cat $RES/main/session_job/bots/part-r-* | sort -t$'\t' -k6,6nr | head -5

echo "Check :  python3 tools/q4_verify.py $LOCAL $RES/main | tee q4_verify.txt     (also prints the evidence of the planted cases)"
echo "Report: logs in $RES/*.log (grep '\[Q4\]'), JobHistory (:19888) and ResourceManager (:8088) screenshots."

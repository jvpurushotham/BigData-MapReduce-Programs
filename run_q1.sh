#!/usr/bin/env bash
# Q1 end-to-end: with combiner, without combiner (+ diff), and the deliberately wrong combiner (1d).
# Run INSIDE the Ubuntu VM where Hadoop lives:   ./run_q1.sh  [local_readings_dir]
set -euo pipefail

USER_NAME=$(whoami)
BASE=/user/$USER_NAME/ca2/q1
JAR=target/ca2.jar
LOCAL_IN=${1:-}
SPLIT=${SPLIT:-500000}          # bytes; small enough to force >= 3 input splits for 1d (check your file sizes)
RED=${REDUCERS:-2}
RES=results_q1
rm -rf "$RES"; mkdir -p "$RES"

# 1. copy the input to HDFS (only if a local dir was given)
if [ -n "$LOCAL_IN" ]; then
  hdfs dfs -rm -r -f "$BASE/input" || true
  hdfs dfs -mkdir -p "$BASE/input"
  hdfs dfs -put "$LOCAL_IN"/*.csv "$BASE/input/"
fi

run () {   # run <label> <hadoop -D options...>
  local label=$1; shift
  echo "=================== $label ==================="
  hadoop jar "$JAR" ca2.q1.Q1Driver -D q1.reducers=$RED "$@" "$BASE/input" "$BASE/out_$label" 2>&1 | tee "$RES/$label.log"
  rm -rf "$RES/$label"; mkdir -p "$RES/$label"
  hdfs dfs -get "$BASE/out_$label"/stats* "$RES/$label/" 2>/dev/null || true
  hdfs dfs -get "$BASE/out_$label"/outliers "$RES/$label/" 2>/dev/null || true
}

run with_combiner    -D q1.combiner=true
run without_combiner -D q1.combiner=false
run wrong            -D q1.wrong=true -D mapreduce.input.fileinputformat.split.maxsize=$SPLIT

echo "=================== 1c : outputs byte-identical? ==================="
if diff -r "$RES/with_combiner/stats" "$RES/without_combiner/stats" > "$RES/diff_stats.txt"; then
  echo "IDENTICAL (stats)"; else echo "DIFFERENT - see $RES/diff_stats.txt"; fi
if diff -r "$RES/with_combiner/outliers" "$RES/without_combiner/outliers" > "$RES/diff_outliers.txt"; then
  echo "IDENTICAL (outliers)"; else echo "DIFFERENT - see $RES/diff_outliers.txt"; fi
echo "(cmp on concatenated, sorted outputs:)"
cat "$RES"/with_combiner/stats/part-r-* | sort | md5sum
cat "$RES"/without_combiner/stats/part-r-* | sort | md5sum

echo "=================== 1d : which meters went wrong? ==================="
python3 tools/q1_find_wrong.py "$RES/with_combiner/stats" "$RES/wrong/stats_wrong" || true

echo "Counters for the report are in $RES/*.log (grep '\[Q1\]'). Also screenshot the JobHistory UI (:19888) and RM UI (:8088)."

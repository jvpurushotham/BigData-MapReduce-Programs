#!/usr/bin/env bash
# Q2 end-to-end: index with delta encoding, index WITHOUT delta encoding (+ diff + shuffle bytes), phrase queries.
# Run INSIDE the Ubuntu VM where Hadoop lives:   ./run_q2.sh <local q2 folder>   (the folder that holds corpus/ and stopwords.txt)
#   PHRASE="big data analytics" ./run_q2.sh ~/q2data          # change the phrase
#   REDUCERS=4 ./run_q2.sh ~/q2data                           # change the number of reducers
set -euo pipefail

LOCAL=${1:?usage: ./run_q2.sh <local q2 folder containing corpus/ and stopwords.txt>}
USER_NAME=$(whoami)
BASE=/user/$USER_NAME/ca2/q2
JAR=target/ca2.jar
RED=${REDUCERS:-2}
PHRASE=${PHRASE:-big data analytics}
RES=results_q2
rm -rf "$RES"; mkdir -p "$RES"

# 1. input to HDFS
hdfs dfs -rm -r -f "$BASE/input" || true
hdfs dfs -mkdir -p "$BASE/input/corpus"
hdfs dfs -put "$LOCAL"/corpus/*.tsv "$BASE/input/corpus/"
hdfs dfs -put -f "$LOCAL"/stopwords.txt "$BASE/input/stopwords.txt"

run () {   # run <label> <extra -D options...>
  local label=$1; shift
  echo "=================== $label ==================="
  hadoop jar "$JAR" ca2.q2.Q2Driver -D q2.reducers=$RED -D phrase="$PHRASE" "$@" \
        "$BASE/input/corpus" "$BASE/input/stopwords.txt" "$BASE/out_$label" 2>&1 | tee "$RES/$label.log"
  rm -rf "$RES/$label"; mkdir -p "$RES/$label"
  hdfs dfs -get "$BASE/out_$label"/index "$RES/$label/index"
  hdfs dfs -get "$BASE/out_$label"/phrase "$RES/$label/phrase"
}

run delta   -D q2.delta=true
run nodelta -D q2.delta=false

echo "=================== delta vs no-delta: outputs identical? ==================="
if diff -r "$RES/delta/index" "$RES/nodelta/index" > "$RES/diff_index.txt" && diff -r "$RES/delta/phrase" "$RES/nodelta/phrase" >> "$RES/diff_index.txt"; then
  echo "IDENTICAL (index, corpus_stop, rare, phrase)"; else echo "DIFFERENT - see $RES/diff_index.txt"; fi

echo "=================== shuffle bytes (report table) ==================="
printf "%-10s %18s %20s\n" "" "MAP_OUTPUT_BYTES" "REDUCE_SHUFFLE_BYTES"
for l in nodelta delta; do
  printf "%-10s %18s %20s\n" "$l" "$(grep -m1 'MAP_OUTPUT_BYTES' $RES/$l.log | awk '{print $NF}')" "$(grep -m1 'REDUCE_SHUFFLE_BYTES' $RES/$l.log | awk '{print $NF}')"
done | tee "$RES/shuffle_bytes.txt"

echo "=================== first lines ==================="
head -5 "$RES/delta/index"/part-r-00000 | cut -c1-200
echo "--- phrase \"$PHRASE\" (doc_id, occurrences):"; head -10 "$RES/delta/phrase"/part-r-*
echo "Logs for the report: $RES/*.log (grep '\[Q2\]'). Screenshot JobHistory (:19888) and ResourceManager (:8088)."
echo "Check:  python3 tools/q2_verify.py $LOCAL/corpus $LOCAL/stopwords.txt $RES/delta --phrase \"$PHRASE\""

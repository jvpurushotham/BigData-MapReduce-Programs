#!/usr/bin/env bash
# Q3 end-to-end: main run, run without the Combiner, run with UNBOUNDED mappers (+ diffs, metrics, evidence).
# Run INSIDE the Ubuntu VM where Hadoop lives:   ./run_q3.sh <local q3 folder>
#   REDUCERS=6 ./run_q3.sh ~/q3data       # reducers of Job 3 (routed by category; at least 3 are required)
#   TOPN=3 ./run_q3.sh ~/q3data           # change N
# The local folder must contain orders*.csv, returns*.csv and products*.csv (any sub-folders are searched).
set -euo pipefail

LOCAL=${1:?usage: ./run_q3.sh <local q3 folder>}
USER_NAME=$(whoami)
BASE=/user/$USER_NAME/ca2/q3
JAR=target/ca2.jar
RED=${REDUCERS:-4}
TOPN=${TOPN:-5}
RES=results_q3
rm -rf "$RES"; mkdir -p "$RES"

find_files () {   # find_files <word>  -> csv files whose name starts with <word> or that sit in a folder called <word>
  find "$LOCAL" -type f \( -iname "$1*.csv" -o -ipath "*/$1/*.csv" \) | sort
}
ORD=$(find_files orders); RET=$(find_files returns); PRD=$(find_files products | head -1)
[ -n "$ORD" ] && [ -n "$RET" ] && [ -n "$PRD" ] || { echo "could not find orders*/returns*/products* csv files under $LOCAL"; exit 1; }
echo "orders : $(echo "$ORD" | wc -l) file(s)   returns: $(echo "$RET" | wc -l) file(s)   products: $PRD"

# 1. input to HDFS
hdfs dfs -rm -r -f "$BASE/input" || true
hdfs dfs -mkdir -p "$BASE/input/orders" "$BASE/input/returns"
for f in $ORD; do hdfs dfs -put -f "$f" "$BASE/input/orders/"; done
for f in $RET; do hdfs dfs -put -f "$f" "$BASE/input/returns/"; done
hdfs dfs -put -f "$PRD" "$BASE/input/products.csv"

run () {   # run <label> <-D options...>
  local label=$1; shift
  echo "=================== $label ==================="
  hadoop jar "$JAR" ca2.q3.Q3Driver -D q3.reducers=$RED -D q3.topn.n=$TOPN "$@" \
        "$BASE/input/orders" "$BASE/input/returns" "$BASE/input/products.csv" "$BASE/out_$label" 2>&1 | tee "$RES/$label.log"
  rm -rf "$RES/$label"
  hdfs dfs -get "$BASE/out_$label" "$RES/$label"
}

run main        -D q3.keep.intermediate=true            # kept: the report needs net_lines / revenue as evidence
run nocombiner  -D q3.combiner=false
run unbounded   -D q3.topn.bounded=false

echo "=================== outputs identical? ==================="
for v in nocombiner unbounded; do
  if [ "$(cat $RES/main/top5/part-r-* | sort | md5sum)" = "$(cat $RES/$v/top5/part-r-* | sort | md5sum)" ]; then
    echo "IDENTICAL top5: main vs $v"; else echo "DIFFERENT top5: main vs $v"; fi
done

echo "=================== metrics for the report ==================="
{
  printf "%-12s %12s %12s %14s %14s\n" "run" "map_out_rec" "map_out_B" "shuffle_B" "combine_out"
  echo "Job 2 (net revenue):"
  for v in main nocombiner; do
    L=$RES/$v.log
    s=$(awk '/Job 2/{f=1} /Job 3/{f=0} f' $L)
    printf "%-12s %12s %12s %14s %14s\n" $v "$(echo "$s" | awk '/MAP_OUTPUT_RECORDS/{print $NF}')" "$(echo "$s" | awk '/MAP_OUTPUT_BYTES/{print $NF}')" \
      "$(echo "$s" | awk '/REDUCE_SHUFFLE_BYTES/{print $NF}')" "$(echo "$s" | awk '/COMBINE_OUTPUT_RECORDS/{print $NF}')"
  done
  echo "Job 3 (Top-N):"
  for v in main unbounded; do
    L=$RES/$v.log
    s=$(awk '/Job 3/{f=1} f' $L)
    printf "%-12s %12s %12s %14s %14s\n" $v "$(echo "$s" | awk '/MAP_OUTPUT_RECORDS/{print $NF}')" "$(echo "$s" | awk '/MAP_OUTPUT_BYTES/{print $NF}')" \
      "$(echo "$s" | awk '/REDUCE_SHUFFLE_BYTES/{print $NF}')" "-"
  done
} | tee "$RES/metrics.txt"

echo "=================== planted cases (evidence for the report) ==================="
set +o pipefail; set +e      # `| head` closes pipes early; that must not stop the script
{
  N=$RES/main/net_lines; R=$RES/main/revenue
  echo "--- (1) products with identical revenue AND units in one category-month (category, month, revenue_E4, units : products):"
  cat $R/part-r-* | awk -F'\t' '{k=$1" | "$2" | "$4" | "$5; n[k]++; p[k]=p[k]" "$3} END{for(k in n) if(n[k]>1) print k" :"p[k]}' | head -10
  echo "--- (2) order lines returned in TWO OR MORE separate returns (order_id,line_no  count):"
  cat $RET | awk -F, '$1!="return_id" && NF>=5{c[$2","$3]++} END{for(k in c) if(c[k]>1) print k"  "c[k]}' | head -10
  echo "--- (3) returned quantity exceeds ordered quantity (order_id line_no month product qty returned net):"
  cat $N/part-r-* | awk -F'\t' '$6>$5{print $1,$2,$3,$4,$5,$6,$7}' | head -10
  echo "--- (4) order on 31 Jan returned in February: the line must stay in 2026-01 (order_id,line_no  net_lines month):"
  cat $RET | awk -F, '$1!="return_id" && $5 ~ /^2026-02/{print $2","$3}' | sort -u > $RES/_feb.txt
  cat $ORD | awk -F, '$3 ~ /^2026-01-31/{print $1","$2}' | sort -u > $RES/_jan31.txt
  comm -12 $RES/_feb.txt $RES/_jan31.txt | while IFS=, read o l; do
    cat $N/part-r-* | awk -F'\t' -v o="$o" -v l="$l" '$1==o && $2==l{print $1","$2"  month "$3"  qty "$5"  returned "$6"  net "$7}'; done | head -5
  echo "--- (5) product missing from products.csv -> UNMAPPED (category, month, product, revenue_E4, units):"
  cat $R/part-r-* | awk -F'\t' '$1=="UNMAPPED"' | head -5
  echo "--- orphan returns (return_id,order_id,line_no,qty,ts):"
  cat $RES/main/orphan_returns/*-r-* | head -5
} 2>&1 | tee "$RES/planted_evidence.txt"
rm -f "$RES"/_feb.txt "$RES"/_jan31.txt

echo "=================== part 3(d): counter-example ==================="
python3 tools/q3_counterexample.py "$RES/main/net_lines" "$PRD" --n $TOPN | tee "$RES/counterexample.txt"

echo "=================== first lines of the Top-N output ==================="
head -20 $RES/main/top5/part-r-00000
echo "Check:  python3 tools/q3_verify.py $LOCAL $RES/main --n $TOPN"
echo "Report: logs in $RES/*.log (grep '\[Q3\]'), JobHistory (:19888) and ResourceManager (:8088) screenshots."

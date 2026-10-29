#!/usr/bin/env bash
# Q5 end-to-end: main run (4 reducers) and a 1-reducer run (outputs must be identical).
# Run INSIDE the Ubuntu VM where Hadoop lives:   ./run_q5.sh <local q5 folder>
#   REDUCERS=6 ./run_q5.sh ~/q5data
# The local folder must contain orders*.csv, gateway_txns*.csv, bank_settlements*.csv and fx_rates*.csv (sub-folders are searched).
set -euo pipefail

LOCAL=${1:?usage: ./run_q5.sh <local q5 folder>}
USER_NAME=$(whoami)
BASE=/user/$USER_NAME/ca2/q5
JAR=target/ca2.jar
RED=${REDUCERS:-4}
RES=results_q5
rm -rf "$RES"; mkdir -p "$RES"

find_files () { find "$LOCAL" -type f \( -iname "$1*.csv" -o -ipath "*/$1/*.csv" \) | sort; }
ORD=$(find_files orders); TXN=$(find_files gateway_txns); SET=$(find_files bank_settlements); FX=$(find_files fx_rates | head -1)
[ -n "$ORD" ] && [ -n "$TXN" ] && [ -n "$SET" ] && [ -n "$FX" ] || { echo "need orders*, gateway_txns*, bank_settlements*, fx_rates* csv files under $LOCAL"; exit 1; }
echo "orders: $(echo "$ORD" | wc -l) file(s)  txns: $(echo "$TXN" | wc -l)  settlements: $(echo "$SET" | wc -l)  fx: $FX"
echo "--- first lines of every input (check the column order against the assignment):"
for f in $ORD $TXN $SET $FX; do echo "[$f]"; head -3 "$f"; done

hdfs dfs -rm -r -f "$BASE/input" || true
hdfs dfs -mkdir -p "$BASE/input/orders" "$BASE/input/txns" "$BASE/input/settlements"
for f in $ORD; do hdfs dfs -put -f "$f" "$BASE/input/orders/"; done
for f in $TXN; do hdfs dfs -put -f "$f" "$BASE/input/txns/"; done
for f in $SET; do hdfs dfs -put -f "$f" "$BASE/input/settlements/"; done
hdfs dfs -put -f "$FX" "$BASE/input/fx_rates.csv"

run () {   # run <label> <-D options...>
  local label=$1; shift
  echo "=================== $label ==================="
  hadoop jar "$JAR" ca2.q5.Q5Driver "$@" "$BASE/input/orders" "$BASE/input/txns" "$BASE/input/settlements" "$BASE/input/fx_rates.csv" "$BASE/out_$label" 2>&1 | tee "$RES/$label.log"
  rm -rf "$RES/$label"
  hdfs dfs -get "$BASE/out_$label" "$RES/$label"
}
run main -D q5.reducers=$RED
run r1   -D q5.reducers=1

echo "=================== outputs identical? ==================="
for c in UNPAID UNDERPAID OVERPAID PAID_NOT_SETTLED SETTLEMENT_MISMATCH MATCHED ORPHAN_TXN ORPHAN_SETTLEMENT; do
  a=$(cat $RES/main/$c/part-r-* 2>/dev/null | sort | md5sum); b=$(cat $RES/r1/$c/part-r-* 2>/dev/null | sort | md5sum)
  [ "$a" = "$b" ] && echo "IDENTICAL $c ($(cat $RES/main/$c/part-r-* 2>/dev/null | wc -l) rows)" || echo "DIFFERENT $c"
done

echo "=================== summary (category count order_inr net_paid_inr settled_inr) ==================="
cat $RES/main/summary/part-r-00000 | tee "$RES/summary.txt"

echo "=================== first 20 output lines (category, then the line) ==================="
for c in UNPAID UNDERPAID OVERPAID PAID_NOT_SETTLED SETTLEMENT_MISMATCH MATCHED ORPHAN_TXN ORPHAN_SETTLEMENT; do
  cat $RES/main/$c/part-r-* 2>/dev/null | sort | head -3 | sed "s/^/$c\t/"
done | head -20 | tee "$RES/first20.txt"

echo "Check :  python3 tools/q5_verify.py $LOCAL $RES/main | tee q5_verify.txt     (also prints the evidence of the planted cases)"
echo "Report: logs in $RES/*.log (grep '\[Q5\]'), JobHistory (:19888) and ResourceManager (:8088) screenshots."

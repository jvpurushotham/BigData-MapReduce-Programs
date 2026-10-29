#!/usr/bin/env python3
"""Question 1(d): print the per-mapper table for the traced meter-month and the two means.

    python3 tools/q1_trace_report.py results_q1/trace.log
"""
import re, sys
n, sm = {}, {}
for line in open(sys.argv[1]):
    m = re.search(r"map(\d+)_(n|sum_micro)=(\d+)", line)
    if m:
        (n if m.group(2) == "n" else sm)[int(m.group(1))] = int(m.group(3))
if not n:
    sys.exit("no Q1_TRACE counters found: did you pass -D q1.trace.key=... and use -D q1.wrong=true?")
ids = sorted(n)
avgs = [round(sm[i] / n[i]) / 1e6 for i in ids]
print("%-8s %10s %14s" % ("mapper", "readings", "local average"))
for i, a in zip(ids, avgs):
    print("map%03d   %10d %14.6f" % (i, n[i], a))
tot_n, tot_s = sum(n.values()), sum(sm.values())
print("\nreadings in total              : %d (in %d mappers)" % (tot_n, len(ids)))
print("CORRECT mean = sum / count     : %.6f   (weights every reading equally)" % (tot_s / tot_n / 1e6))
print("WRONG mean = avg of averages   : %.6f   (weights every mapper equally)" % (sum(avgs) / len(avgs)))

#!/usr/bin/env python3
"""Question 1(d): compare the correct stats with the output of the WRONG averaging combiner and list
the meters whose mean became wrong.

    python3 tools/q1_find_wrong.py <dir with stats/part-r-*> <dir with stats_wrong/part-r-*>
"""
import glob, sys

def load(d):
    r = {}
    for p in glob.glob(d + "/part-r-*"):
        for line in open(p):
            k, v = line.rstrip("\n").split("\t")
            c, mn, mx, mean, sd = v.split(",")
            r[k] = (int(c), float(mean))
    return r

good, bad = load(sys.argv[1]), load(sys.argv[2])
diffs = sorted(((abs(good[k][1] - bad[k][1]), k) for k in good if k in bad and abs(good[k][1] - bad[k][1]) > 1e-9), reverse=True)
print("meter-months with a wrong mean: %d of %d" % (len(diffs), len(good)))
for d, k in diffs[:10]:
    print("  %-28s readings=%d  correct mean=%.6f  wrong mean=%.6f  error=%.6f" % (k, good[k][0], good[k][1], bad[k][1], d))

#!/usr/bin/env python3
"""Independent check of Q1: recomputes everything in plain Python (Decimal arithmetic) and compares
with what your MapReduce jobs wrote.

    python3 tools/q1_verify.py <raw readings dir> <job output dir>
e.g.  python3 tools/q1_verify.py testdata/q1/readings out_q1/with_combiner

<job output dir> must contain  stats/part-r-*  and  outliers/part-r-*  (copy them out of HDFS with
hdfs dfs -get).  Prints the expected value of every rejection counter so you can compare them with the
counters printed by the driver / the JobHistory UI.
"""
import glob, math, re, sys
from collections import defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

raw_dir, out_dir = sys.argv[1], sys.argv[2]
TS = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")

cnt = defaultdict(int)
seen = {}
for path in sorted(glob.glob(raw_dir + "/*.csv")):
    for line in open(path, encoding="utf-8"):
        cnt["RECORDS_READ"] += 1
        s = line.strip()
        if not s:
            cnt["REJECTED_EMPTY_FIELD"] += 1; continue
        if s.startswith("meter_id"):
            cnt["HEADER_SKIPPED"] += 1; continue
        f = s.split(",")
        if len(f) != 4:
            cnt["REJECTED_FIELD_COUNT"] += 1; continue
        meter, city, ts, k = [x.strip() for x in f]
        if not (meter and city and ts and k):
            cnt["REJECTED_EMPTY_FIELD"] += 1; continue
        try:
            if not TS.match(ts): raise ValueError
            datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            cnt["REJECTED_BAD_TIMESTAMP"] += 1; continue
        try:
            kwh = Decimal(k)
            if not kwh.is_finite(): raise InvalidOperation
        except InvalidOperation:
            cnt["REJECTED_BAD_KWH"] += 1; continue
        if kwh < 0:
            cnt["REJECTED_NEGATIVE_KWH"] += 1; continue
        # same rule as CleanJob: among copies of one (meter, ts) keep the lexicographically smallest "city,month,micro"
        micro = int((kwh * 1000000).quantize(Decimal(1), rounding=ROUND_HALF_UP))
        cand = "%s,%s,%d" % (city, ts[:7], micro)
        key = (meter, ts)
        if key in seen:
            cnt["DUPLICATE_READING"] += 1
            if cand < seen[key]:
                seen[key] = cand
            continue
        seen[key] = cand
cnt["UNIQUE_READINGS"] = len(seen)

groups = defaultdict(list)
for (meter, ts), cand in seen.items():
    city, month, micro = cand.split(",")
    groups[(city, meter, month)].append(Decimal(micro) / 1000000)

expected = {}
for key, vals in groups.items():
    n = len(vals)
    mean = sum(vals) / n
    var = sum((v - mean) ** 2 for v in vals) / n
    sd = var.sqrt()
    thr = mean + 2 * sd
    out = sum(1 for v in vals if v > thr)
    expected[",".join(key)] = (n, min(vals), max(vals), mean, sd, out)

print("== expected counters (compare with Job A counters) ==")
for k in ["RECORDS_READ", "HEADER_SKIPPED", "REJECTED_FIELD_COUNT", "REJECTED_EMPTY_FIELD", "REJECTED_BAD_TIMESTAMP",
          "REJECTED_BAD_KWH", "REJECTED_NEGATIVE_KWH", "DUPLICATE_READING", "UNIQUE_READINGS"]:
    print("  %-26s %d" % (k, cnt[k]))
print("  meter-months:", len(expected))

bad = 0
got = {}
for p in glob.glob(out_dir + "/stats/part-r-*"):
    for line in open(p):
        k, v = line.rstrip("\n").split("\t")
        got[k] = v.split(",")
print("\n== Job B (stats) ==")
if set(got) != set(expected):
    print("  KEY MISMATCH: missing", len(set(expected) - set(got)), "extra", len(set(got) - set(expected)))
    bad += 1
for k, (n, mn, mx, mean, sd, _) in expected.items():
    g = got.get(k)
    if not g: continue
    ok = (int(g[0]) == n and abs(Decimal(g[1]) - mn) < Decimal("1e-6") and abs(Decimal(g[2]) - mx) < Decimal("1e-6")
          and abs(Decimal(g[3]) - mean) < Decimal("1e-8") and abs(Decimal(g[4]) - sd) < Decimal("1e-8"))
    if not ok:
        bad += 1
        if bad <= 10: print("  MISMATCH", k, "expected", (n, mn, mx, float(mean), float(sd)), "got", g)
print("  compared %d meter-months, mismatches: %d" % (len(expected), bad))

gotc = {}
for p in glob.glob(out_dir + "/outliers/part-r-*"):
    for line in open(p):
        k, t, o, pct = line.rstrip("\n").split("\t")
        gotc[k] = (int(t), int(o), float(pct))
print("\n== Job C (outliers) ==")
bad_c = 0
if set(gotc) != set(expected):
    print("  KEY MISMATCH: missing", len(set(expected) - set(gotc)), "extra", len(set(gotc) - set(expected))); bad_c += 1
for k, (n, _, _, _, _, o) in expected.items():
    g = gotc.get(k)
    if g and (g[0] != n or g[1] != o or abs(g[2] - 100.0 * o / n) > 1e-3):
        bad_c += 1
        if bad_c <= 10: print("  MISMATCH", k, "expected", (n, o), "got", g)
print("  mismatches: %d   (total outlier readings expected: %d)" % (bad_c, sum(v[5] for v in expected.values())))

print("\n== planted cases ==")
for k in sorted(expected):
    if ",M99001," in k or ",M99002," in k or ",M99003," in k:
        n, mn, mx, mean, sd, o = expected[k]
        print("  %-28s n=%d min=%s max=%s mean=%.6f sd=%.6f outliers=%d" % (k, n, mn, mx, mean, sd, o))
sys.exit(1 if (bad or bad_c) else 0)

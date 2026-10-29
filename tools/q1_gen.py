#!/usr/bin/env python3
"""Seeded generator of a SMALL Q1-style test dataset (smart-meter readings) with the planted cases.

Use it to test your jobs quickly before running on the real q1 data.
    python3 tools/q1_gen.py testdata/q1/readings          # writes readings_part0..3.csv
"""
import os, random, sys
from datetime import datetime, timedelta

out_dir = sys.argv[1] if len(sys.argv) > 1 else "testdata/q1/readings"
n_meters = int(sys.argv[2]) if len(sys.argv) > 2 else 40
days = int(sys.argv[3]) if len(sys.argv) > 3 else 30
os.makedirs(out_dir, exist_ok=True)
rnd = random.Random(2026)

cities = ["Pune", "Nagpur", "Mumbai", "Delhi", "Chennai"]
files = [[] for _ in range(4)]
start = datetime(2026, 2, 20, 0, 0, 0)          # crosses the 28 Feb / 1 Mar boundary
fmt = "%Y-%m-%d %H:%M:%S"

for m in range(n_meters):
    meter = "M%05d" % (m + 1)
    city = cities[m % len(cities)]
    base = rnd.uniform(0.3, 2.0)
    t = start
    for _ in range(days * 96):
        kwh = max(0.0, rnd.gauss(base, base * 0.35))
        if rnd.random() < 0.01:                  # occasional spike -> outliers
            kwh *= 5
        line = "%s,%s,%s,%.3f" % (meter, city, t.strftime(fmt), kwh)
        f = files[rnd.randrange(4)]
        f.append(line)
        r = rnd.random()
        if r < 0.010: f.append(line)                                                      # retransmit duplicate (same file)
        elif r < 0.012: files[rnd.randrange(4)].append(line)                             # duplicate in another file
        elif r < 0.016: f.append("%s,%s,%s,%.3f" % (meter, city, t.strftime(fmt), -rnd.uniform(0.1, 1)))  # negative
        elif r < 0.020: f.append("%s,%s,%s,%.3f" % (meter, city, t.strftime("%d/%m/%Y %H:%M"), kwh))      # bad timestamp
        elif r < 0.022: f.append("%s,%s,%s" % (meter, city, t.strftime(fmt)))                             # missing field
        elif r < 0.024: f.append("%s,%s,%s,abc" % (meter, city, t.strftime(fmt)))                         # non-numeric kWh
        elif r < 0.025: f.append("")                                                                      # blank line
        t += timedelta(minutes=15)

# ---- planted cases ------------------------------------------------------------------------------
# 1. exactly one reading in a month (sigma = 0): M99001 has several March readings and ONE April reading
for i in range(5):
    files[0].append("M99001,Pune,2026-03-%02d 10:00:00,%.3f" % (i + 1, 1.0 + i * 0.1))
files[1].append("M99001,Pune,2026-04-10 12:00:00,2.500")
# 2. month boundary: 23:45 on the 28th and 00:00 on the 1st must fall in different months
files[2].append("M99002,Mumbai,2026-02-28 23:45:00,1.200")
files[2].append("M99002,Mumbai,2026-03-01 00:00:00,3.400")
# 3. duplicate pair in two different input files (reaches two different mappers)
files[0].append("M99003,Nagpur,2026-03-14 18:15:00,1.732")
files[3].append("M99003,Nagpur,2026-03-14 18:15:00,1.732")
files[1].append("M99003,Nagpur,2026-03-14 18:30:00,2.000")
# the three examples printed in the assignment text
files[0].append("M02231,Nagpur,2026-03-14 18:15:00,-0.410")
files[0].append("M00451,Mumbai,14/03/2026 18:30,0.988")

for i, f in enumerate(files):
    rnd.shuffle(f)
    with open(os.path.join(out_dir, "readings_part%d.csv" % i), "w") as fh:
        fh.write("\n".join(f) + "\n")
print("wrote", sum(len(f) for f in files), "lines to", out_dir)

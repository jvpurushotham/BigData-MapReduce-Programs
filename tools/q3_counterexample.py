#!/usr/bin/env python3
"""Question 3(d): why Top-N cannot be applied inside Job 2's mappers (before revenue is fully aggregated).

Job 2's mappers read the part files of Job 1. This script plays the role of those mappers on YOUR data: every part file
of Job 1 is one mapper's input. Each "mapper" computes partial revenue per (category, month, product), keeps only its own
top-N per group and the "reducer" adds up what survived. The result is compared with the true Top-N.

    python3 tools/q3_counterexample.py <job1 output dir (net_lines)> <products.csv> [--n 5] [--chunks K]

--chunks K splits every part file further into K contiguous pieces (more mappers) to show the effect gets worse.
"""
import argparse, csv, glob, os
from collections import defaultdict

ap = argparse.ArgumentParser()
ap.add_argument("net_lines"); ap.add_argument("products")
ap.add_argument("--n", type=int, default=5); ap.add_argument("--chunks", type=int, default=1)
a = ap.parse_args()

cat = {}
with open(a.products, newline="", encoding="utf-8") as f:
    for r in csv.reader(f):
        if r and r[0].strip().lower() != "product_id" and len(r) >= 3:
            cat.setdefault(r[0].strip(), r[2].strip() or "UNMAPPED")

lines_by_file = []
for p in sorted(glob.glob(os.path.join(a.net_lines, "part-r-*"))):
    ls = [l.rstrip("\n").split("\t") for l in open(p) if l.strip()]
    k = a.chunks
    size = max(1, -(-len(ls) // k))
    for i in range(0, len(ls), size): lines_by_file.append(ls[i:i + size])
print("simulated mappers (input pieces): %d" % len(lines_by_file))

def key_order(item):                                  # strict total order: revenue desc, units desc, product asc
    prod, (rev, units) = item
    return (-rev, -units, prod)

partial = []                                           # per mapper: {(cat, month): {product: [rev, units]}}
true = defaultdict(lambda: defaultdict(lambda: [0, 0]))
for ls in lines_by_file:
    m = defaultdict(lambda: defaultdict(lambda: [0, 0]))
    for f in ls:                                       # order_id, line_no, month, product, qty, returned, net, price_E4
        net, pe4 = int(f[6]), int(f[7]); c = cat.get(f[3], "UNMAPPED"); g = (c, f[2])
        for d in (m[g][f[3]], true[g][f[3]]):
            d[0] += net * pe4; d[1] += net
    partial.append(m)

wrong = []
naive = defaultdict(lambda: defaultdict(lambda: [0, 0]))
for mi, m in enumerate(partial):
    for g, prods in m.items():
        for prod, (rev, units) in sorted(prods.items(), key=key_order)[: a.n]:      # mapper-side Top-N
            naive[g][prod][0] += rev; naive[g][prod][1] += units

for g, prods in true.items():
    t = [p for p, _ in sorted(prods.items(), key=key_order)[: a.n]]
    nv = [p for p, _ in sorted(naive[g].items(), key=key_order)[: a.n]]
    if t != nv or any(naive[g][p][0] != prods[p][0] for p in t):
        wrong.append(g)
print("groups (category, month): %d   groups whose Top-%d is WRONG when Top-N is applied in the mappers: %d" % (len(true), a.n, len(wrong)))

def m2(e4): return "%.2f" % (e4 / 10000)
shown = 0
for g in sorted(wrong, key=lambda g: g):
    t = sorted(true[g].items(), key=key_order)[: a.n]
    for rank, (prod, (rev, units)) in enumerate(t, 1):
        got = naive[g].get(prod)
        if got is None or got[0] != rev:
            print("\nCOUNTER-EXAMPLE  category=%s  month=%s  product=%s" % (g[0], g[1], prod))
            print("  true net revenue = %s (rank %d of %d products)" % (m2(rev), rank, len(true[g])))
            for mi, m in enumerate(partial):
                if g in m and prod in m[g]:
                    ordered = [p for p, _ in sorted(m[g].items(), key=key_order)]
                    pos = ordered.index(prod) + 1
                    print("    mapper %d: partial revenue %-10s -> rank %d of %d in this mapper %s" %
                          (mi, m2(m[g][prod][0]), pos, len(ordered), "(kept)" if pos <= a.n else "(DISCARDED by mapper-side Top-%d)" % a.n))
            print("  revenue the reducer sees after mapper-side Top-%d: %s   (true: %s)" %
                  (a.n, m2(got[0]) if got else "product lost completely", m2(rev)))
            shown += 1
            break
    if shown >= 3: break

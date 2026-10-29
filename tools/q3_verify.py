#!/usr/bin/env python3
"""Independent check of Q3: re-implements the specification in plain Python (csv module, Decimal money) and compares
it with the output of the MapReduce jobs.

    python3 tools/q3_verify.py <q3 folder with orders/returns/products csv> <result dir> [--n 5]

<result dir> = the driver's output copied out of HDFS:  top5/part-r-*, orphan_returns/*, and (if kept) net_lines/, revenue/.
Prints the expected value of every counter so you can compare with the [Q3] lines of the log. Exit 1 on any difference.
"""
import argparse, csv, glob, os, sys
from collections import defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

ap = argparse.ArgumentParser()
ap.add_argument("data"); ap.add_argument("out"); ap.add_argument("--n", type=int, default=5)
a = ap.parse_args()

def files(kind):
    r = []
    for p in sorted(glob.glob(os.path.join(a.data, "**", "*.csv"), recursive=True)):
        if kind in p.lower().replace(a.data.lower(), ""):
            r.append(p)
    return r

def rows(kind):
    for p in files(kind):
        with open(p, newline="", encoding="utf-8") as f:
            for line in f:
                yield line

def parse_csv(line):
    return next(csv.reader([line.rstrip("\r\n")]))

def good_ts(s):
    try: datetime.strptime(s, "%Y-%m-%d %H:%M:%S"); return len(s) == 19
    except ValueError: return False

C = defaultdict(int)
orders = defaultdict(set); order_copies = defaultdict(int)
for line in rows("orders"):
    if not line.strip(): C["ORDERS_BLANK_LINES"] += 1; continue
    f = parse_csv(line)
    if f[0].strip().lower() == "order_id": C["ORDERS_HEADER_SKIPPED"] += 1; continue
    C["ORDERS_READ"] += 1
    if len(f) != 8: C["ORDERS_BAD_FIELD_COUNT"] += 1; continue
    oid = f[0].strip()
    try: ln = int(f[1].strip())
    except ValueError: C["ORDERS_BAD_LINE_NO"] += 1; continue
    if not oid: C["ORDERS_BAD_LINE_NO"] += 1; continue
    ts = f[2].strip()
    if not good_ts(ts): C["ORDERS_BAD_TIMESTAMP"] += 1; continue
    prod = f[4].strip()
    if not prod: C["ORDERS_BAD_PRODUCT"] += 1; continue
    try: qty = int(f[5].strip())
    except ValueError: C["ORDERS_BAD_QTY"] += 1; continue
    if qty <= 0: C["ORDERS_BAD_QTY"] += 1; continue
    try:
        p = Decimal(f[6].strip())
        if p < 0 or not p.is_finite(): raise InvalidOperation
    except InvalidOperation:
        C["ORDERS_BAD_PRICE"] += 1; continue
    pe4 = int((p * 10000).quantize(Decimal(1), rounding=ROUND_HALF_UP))
    status = f[7].strip()
    rec = "%s\t%s\t%d\t%d\t%s" % (ts[:7], prod, qty, pe4, status)
    orders[(oid, ln)].add(rec); order_copies[(oid, ln)] += 1

returns = defaultdict(dict)                 # key -> {return_id: (qty, raw)}
for line in rows("returns"):
    if not line.strip(): C["RETURNS_BLANK_LINES"] += 1; continue
    f = parse_csv(line)
    if f[0].strip().lower() == "return_id": C["RETURNS_HEADER_SKIPPED"] += 1; continue
    C["RETURNS_READ"] += 1
    if len(f) != 5 or not f[0].strip() or not f[1].strip(): C["RETURNS_BAD_FIELD_COUNT"] += 1; continue
    try: ln = int(f[2].strip())
    except ValueError: C["RETURNS_BAD_LINE_NO"] += 1; continue
    try: q = int(f[3].strip())
    except ValueError: C["RETURNS_BAD_QTY"] += 1; continue
    if q <= 0: C["RETURNS_BAD_QTY"] += 1; continue
    if not good_ts(f[4].strip()): C["RETURNS_BAD_TIMESTAMP"] += 1; continue
    key = (f[1].strip(), ln); rid = f[0].strip(); raw = line.strip()
    rest = (q, raw)
    if rid in returns[key]:
        C["DUPLICATE_RETURN"] += 1
        if ("%d\t%s" % rest) < ("%d\t%s" % returns[key][rid]): returns[key][rid] = rest
    else:
        returns[key][rid] = rest

products = {}
for line in open(files("products")[0], encoding="utf-8"):
    if not line.strip(): continue
    f = parse_csv(line)
    if f[0].strip().lower() == "product_id": continue
    if len(f) < 3 or not f[0].strip(): C["PRODUCT_ROWS_MALFORMED"] += 1; continue
    if f[0].strip() in products: C["DUPLICATE_PRODUCT_ROWS"] += 1; continue
    products[f[0].strip()] = (f[1].strip(), f[2].strip() or "UNMAPPED")

net_lines = []; orphans = []
for key in set(orders) | set(returns):
    if key not in orders:
        for rid, (q, raw) in returns[key].items(): orphans.append(raw); C["ORPHAN_RETURNS"] += 1
        continue
    C["ORDER_LINES_JOINED"] += 1
    copies = order_copies[key]
    if copies > 1: C["DUPLICATE_ORDER_LINE" if len(orders[key]) == 1 else "CONFLICTING_ORDER_LINE"] += copies - 1
    month, prod, qty, pe4, status = sorted(orders[key])[0].split("\t"); qty = int(qty); pe4 = int(pe4)
    if status.upper() == "CANCELLED":
        C["CANCELLED_LINES"] += 1
        if returns.get(key): C["RETURNS_ON_CANCELLED_LINES"] += len(returns[key])
        continue
    ret = sum(q for q, _ in returns.get(key, {}).values())
    if len(returns.get(key, {})) > 1: C["LINES_WITH_MULTIPLE_RETURNS"] += 1
    net = qty - ret
    if net < 0: C["RETURN_EXCEEDS_ORDER"] += 1; net = 0
    if net == 0: C["FULLY_RETURNED_LINES"] += 1
    C["NET_LINES_WRITTEN"] += 1
    net_lines.append((key, month, prod, qty, ret, net, pe4))

agg = defaultdict(lambda: [0, 0])           # (category, month, product) -> [revenue_E4, units]
for key, month, prod, qty, ret, net, pe4 in net_lines:
    cat = products[prod][1] if prod in products else "UNMAPPED"
    if prod not in products: C["UNMAPPED_LINES"] += 1
    g = agg[(cat, month, prod)]; g[0] += net * pe4; g[1] += net
C["GROUPS_WRITTEN"] = len(agg)

def money(e4):
    s = format(Decimal(e4) / 10000, "f")
    if "." in s: s = s.rstrip("0")
    if "." not in s: s += ".00"
    elif len(s.split(".")[1]) < 2: s += "0" * (2 - len(s.split(".")[1]))
    return s

groups = defaultdict(list)
for (cat, month, prod), (rev, units) in agg.items(): groups[(cat, month)].append((-rev, -units, prod))
expected = []
for (cat, month), lst in sorted(groups.items()):
    for rank, (nrev, nunits, prod) in enumerate(sorted(lst)[: a.n], 1):
        name = products[prod][0] if prod in products else "(unknown)"
        expected.append("\t".join([cat, month, str(rank), prod, name, money(-nrev), str(-nunits)]))

print("== expected counters (compare with the [Q3] lines) ==")
for k in sorted(C): print("  %-30s %d" % (k, C[k]))
print("  (category, month) groups: %d   product groups: %d" % (len(groups), len(agg)))

bad = 0
got = []
for p in sorted(glob.glob(os.path.join(a.out, "top5", "part-r-*"))):
    got += [l.rstrip("\n") for l in open(p, encoding="utf-8") if l.strip()]
print("\n== Top-%d ==" % a.n)
print("  expected %d rows, job wrote %d rows, identical (as sets): %s" % (len(expected), len(got), sorted(expected) == sorted(got)))
if sorted(expected) != sorted(got):
    bad += 1
    for l in sorted(set(expected) - set(got))[:5]: print("   missing:", l)
    for l in sorted(set(got) - set(expected))[:5]: print("   extra  :", l)
sizes = {os.path.basename(p): os.path.getsize(p) for p in glob.glob(os.path.join(a.out, "top5", "part-r-*"))}
print("  reducer outputs (bytes):", dict(sorted(sizes.items())), "-> reducers with data:", sum(1 for v in sizes.values() if v > 0))
cats_by_file = defaultdict(set)
for p in glob.glob(os.path.join(a.out, "top5", "part-r-*")):
    for l in open(p, encoding="utf-8"): cats_by_file[os.path.basename(p)].add(l.split("\t")[0])
seen = defaultdict(list)
for fn, cs in cats_by_file.items():
    for c in cs: seen[c].append(fn)
split_cats = [c for c, fl in seen.items() if len(fl) > 1]
print("  every category in exactly one reducer file (partitioner):", not split_cats)
if split_cats: bad += 1

orph = []
for p in glob.glob(os.path.join(a.out, "orphan_returns", "*-r-*")):
    orph += [l.strip() for l in open(p) if l.strip()]
print("\n== orphan_returns ==\n  expected %d, job wrote %d, identical: %s" % (len(orphans), len(orph), sorted(orphans) == sorted(orph)))
if sorted(orphans) != sorted(orph): bad += 1

nl = glob.glob(os.path.join(a.out, "net_lines", "part-r-*"))
if nl:
    got_nl = sorted(l.rstrip("\n") for p in nl for l in open(p) if l.strip())
    exp_nl = sorted("\t".join([k[0], str(k[1]), m, p, str(q), str(r), str(n), str(pe)]) for k, m, p, q, r, n, pe in net_lines)
    print("\n== Job 1 net lines ==\n  expected %d, job wrote %d, identical: %s" % (len(exp_nl), len(got_nl), exp_nl == got_nl))
    if exp_nl != got_nl: bad += 1

print("\n== planted cases ==")
for line in expected:
    f = line.split("\t")
    if f[0] in ("TESTCAT", "UNMAPPED"): print("  ", line)
for key, month, prod, qty, ret, net, pe4 in net_lines:
    if key[0] in ("O_TWICE", "O_OVER", "O_JAN31", "O_MISS"):
        print("   %-8s line %d month %s product %-9s qty %d returned %d net %d" % (key[0], key[1], month, prod, qty, ret, net))
print("\nRESULT:", "all identical" if not bad else "%d differences" % bad)
sys.exit(1 if bad else 0)

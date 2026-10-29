#!/usr/bin/env python3
"""Independent check of Q5: re-implements the specification in plain Python (csv module, Decimal) and compares it with the output.

    python3 tools/q5_verify.py <q5 folder with orders/gateway_txns/bank_settlements/fx_rates csv> <result dir> [--refund-settlements subtract|ignore]

<result dir> = the driver's output copied out of HDFS (UNPAID/, UNDERPAID/, ..., ORPHAN_TXN/, ORPHAN_SETTLEMENT/, summary/).
Rules: net paid = sum(SUCCESS) - sum(|REFUNDED|), every transaction converted at the rate of the ORDER date; compared in INR;
tolerance 1.00; classification order UNPAID, UNDERPAID/OVERPAID, PAID_NOT_SETTLED, SETTLEMENT_MISMATCH, MATCHED.
"""
import argparse, bisect, csv, glob, os, sys
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP, getcontext, InvalidOperation

getcontext().prec = 60
ap = argparse.ArgumentParser()
ap.add_argument("data"); ap.add_argument("out"); ap.add_argument("--refund-settlements", default="subtract")
a = ap.parse_args()
SUB = a.refund_settlements.lower() != "ignore"
TOL = 10000

def files(prefix):
    r = []
    for p in sorted(glob.glob(os.path.join(a.data, "**", "*.csv"), recursive=True)):
        b = os.path.basename(p).lower()
        if b.startswith(prefix) or ("/" + prefix + "/") in p.lower().replace(a.data.lower(), ""):
            r.append(p)
    return r
def lines(prefix):
    for p in files(prefix):
        with open(p, newline="", encoding="utf-8") as f:
            for l in f:
                if l.strip(): yield l
def fields(l): return next(csv.reader([l.rstrip("\r\n")]))
def dec(s):
    try: return Decimal(s.strip())
    except InvalidOperation: return None
def e4(d): return int((d * 10000).quantize(Decimal(1), rounding=ROUND_HALF_UP))
def date10(ts):
    t = ts.strip()
    if len(t) < 10: return None
    for i in range(10):
        c = t[i]
        if (c != "-") if i in (4, 7) else (not c.isdigit()): return None
    return t[:10]
def money(x):
    s = format(Decimal(x) / 10000, "f")
    if "." in s: s = s.rstrip("0")
    if "." not in s: s += ".00"
    elif len(s.split(".")[1]) < 2: s += "0" * (2 - len(s.split(".")[1]))
    return s

C = defaultdict(int)
# ---- fx ----
fx = defaultdict(dict)
for l in lines("fx_rates"):
    f = fields(l)
    if f[0].strip().lower() == "date": continue
    d = date10(f[0]) if len(f) >= 3 else None; r = dec(f[2]) if len(f) >= 3 else None; c = f[1].strip().upper() if len(f) >= 3 else ""
    if d is None or r is None or r <= 0 or not c: C["FX_ROWS_MALFORMED"] += 1; continue
    if d in fx[c]: C["FX_DUPLICATE_ROWS"] += 1
    fx[c][d] = r; C["FX_ROWS_LOADED"] += 1
def rate(ccy, d):
    m = fx.get(ccy)
    if m is None:
        if ccy == "INR": return Decimal(1)
        sys.exit("no fx rate at all for " + ccy)
    d = d or ""
    if d in m:
        if ccy != "INR": C["FX_CONVERSIONS"] += 1
        return m[d]
    ds = sorted(m); i = bisect.bisect_right(ds, d)
    if i > 0: C["FX_FALLBACK_EARLIER_DATE"] += 1; return m[ds[i - 1]]
    C["FX_FALLBACK_LATER_DATE"] += 1; return m[ds[0]]
def conv(amount, ccy, d): return e4(amount * rate(ccy, d))

# ---- settlements, txns (stage A) ----
settl = defaultdict(lambda: [0, 0]); orphan_settlements = []; txn_ids = {}
sets = []
for l in lines("bank_settlements"):
    f = fields(l)
    if f[0].strip().lower() == "settlement_id": C["SETTLEMENTS_HEADER_SKIPPED"] += 1; continue
    C["SETTLEMENTS_READ"] += 1
    if len(f) < 4 or not f[0].strip() or not f[1].strip(): C["SETTLEMENTS_BAD_FIELD_COUNT"] += 1; continue
    d = dec(f[2])
    if d is None: C["SETTLEMENTS_BAD_AMOUNT"] += 1; continue
    sets.append((f[0].strip(), f[1].strip(), e4(d), f[3].strip()))
txn_rec = {}
for l in lines("gateway_txns"):
    f = fields(l)
    if f[0].strip().lower() == "txn_id": C["TXNS_HEADER_SKIPPED"] += 1; continue
    C["TXNS_READ"] += 1
    if len(f) < 6 or not f[0].strip() or not f[1].strip(): C["TXNS_BAD_FIELD_COUNT"] += 1; continue
    if dec(f[2]) is None: C["TXNS_BAD_AMOUNT"] += 1; continue
    ccy = f[3].strip().upper()
    if not ccy: C["TXNS_BAD_CURRENCY"] += 1; continue
    st = f[4].strip().upper()
    if st not in ("SUCCESS", "FAILED", "REFUNDED"): C["TXNS_BAD_STATUS"] += 1; continue
    if date10(f[5]) is None: C["TXNS_BAD_TS"] += 1
    rec = "\t".join([f[1].strip(), f[2].strip(), ccy, st, f[5].strip()])
    k = f[0].strip()
    if k in txn_rec:
        C["DUPLICATE_TXN_ID"] += 1
        if rec < txn_rec[k]: txn_rec[k] = rec
    else: txn_rec[k] = rec
for sid, tx, amt, d in sets:
    if tx in txn_rec: settl[tx][0] += amt; settl[tx][1] += 1
    else: orphan_settlements.append((sid, tx, amt, d)); C["ORPHAN_SETTLEMENT"] += 1
for k in txn_rec:
    C["TXNS_JOINED"] += 1
    if settl[k][1] == 0: C["TXNS_WITHOUT_SETTLEMENT"] += 1
    if settl[k][1] > 1: C["TXNS_SETTLED_MORE_THAN_ONCE"] += 1

# ---- orders (stage B) ----
order_rec = {}
for l in lines("orders"):
    f = fields(l)
    if f[0].strip().lower() == "order_id": C["ORDERS_HEADER_SKIPPED"] += 1; continue
    C["ORDERS_READ"] += 1
    if len(f) < 5 or not f[0].strip(): C["ORDERS_BAD_FIELD_COUNT"] += 1; continue
    if dec(f[2]) is None: C["ORDERS_BAD_AMOUNT"] += 1; continue
    ccy = f[3].strip().upper()
    if not ccy: C["ORDERS_BAD_CURRENCY"] += 1; continue
    if date10(f[4]) is None: C["ORDERS_BAD_TS"] += 1; continue
    rec = "\t".join([f[1].strip(), f[2].strip(), ccy, f[4].strip()]); k = f[0].strip()
    if k in order_rec:
        C["DUPLICATE_ORDER_ID"] += 1
        if rec < order_rec[k]: order_rec[k] = rec
    else: order_rec[k] = rec
by_order = defaultdict(list)
for k, rec in txn_rec.items():
    o = rec.split("\t")[0]; by_order[o].append((k, rec.split("\t"), settl[k]))

cats = defaultdict(list); feed = defaultdict(lambda: [0, 0, 0, 0])
def add_feed(cat, o, n, s): f = feed[cat]; f[0] += 1; f[1] += o; f[2] += n; f[3] += s
for oid, rec in order_rec.items():
    cust, amt, ccy, ts = rec.split("\t"); d = date10(ts)
    oinr = conv(Decimal(amt), ccy, d)
    if ccy != "INR": C["ORDERS_NON_INR"] += 1
    success = refunded = settled = 0; nS = nR = nF = 0; unsettled = False
    for tid, f, (ssum, scount) in by_order.get(oid, []):
        tccy, st = f[2], f[3]
        inr = conv(Decimal(f[1]), tccy, d)
        if tccy != ccy: C["TXN_CURRENCY_DIFFERS_FROM_ORDER"] += 1
        if st == "SUCCESS":
            success += inr; nS += 1; settled += ssum
            if scount == 0: unsettled = True
        elif st == "REFUNDED":
            refunded += abs(inr); nR += 1
            if scount > 0:
                C["SETTLEMENTS_ON_REFUND_TXN"] += 1
                if SUB: settled -= abs(ssum)
        else:
            nF += 1
            if scount > 0: C["SETTLEMENTS_ON_FAILED_TXN"] += 1
    txns = len(by_order.get(oid, [])); net = success - refunded
    C["ORDERS_JOINED"] += 1
    if txns == 0: C["ORDERS_WITHOUT_TXN"] += 1
    if nF and nS: C["ORDERS_FAILED_AND_SUCCESS"] += 1
    if nR: C["ORDERS_WITH_REFUND"] += 1
    if net < 0: C["NEGATIVE_NET_PAID"] += 1
    if net == 0: cat = "UNPAID"
    elif abs(net - oinr) > TOL: cat = "UNDERPAID" if net < oinr else "OVERPAID"
    elif unsettled: cat = "PAID_NOT_SETTLED"
    elif abs(settled - net) > TOL: cat = "SETTLEMENT_MISMATCH"
    else: cat = "MATCHED"
    C["CAT_" + cat] += 1
    cats[cat].append("\t".join([oid, cust, ccy, amt, money(oinr), money(net), money(settled), str(txns), str(nS), str(nR), str(nF), money(net - oinr)]))
    add_feed(cat, oinr, net, settled)
for oid, lst in by_order.items():
    if oid in order_rec: continue
    for tid, f, (ssum, scount) in lst:
        inr = conv(Decimal(f[1]), f[2], date10(f[4]))
        if f[3] == "SUCCESS": si, ss = inr, ssum
        elif f[3] == "REFUNDED": si, ss = -abs(inr), (-abs(ssum) if SUB else 0)
        else: si = ss = 0
        cats["ORPHAN_TXN"].append("\t".join([tid, oid, f[1], f[2], f[3], f[4], money(si), money(ss), str(scount)]))
        add_feed("ORPHAN_TXN", 0, si, ss); C["ORPHAN_TXN"] += 1
for sid, tx, amt, d in orphan_settlements:
    cats["ORPHAN_SETTLEMENT"].append("\t".join([sid, tx, money(amt), d])); add_feed("ORPHAN_SETTLEMENT", 0, 0, amt)

print("== expected counters (compare with the [Q5] lines) ==")
for k in sorted(C): print("  %-34s %d" % (k, C[k]))
bad = 0
ORDER = ["UNPAID", "UNDERPAID", "OVERPAID", "PAID_NOT_SETTLED", "SETTLEMENT_MISMATCH", "MATCHED", "ORPHAN_TXN", "ORPHAN_SETTLEMENT"]
def read(pattern):
    r = []
    for p in sorted(glob.glob(os.path.join(a.out, pattern))): r += [l.rstrip("\n") for l in open(p, encoding="utf-8") if l.strip()]
    return r
print("\n== categories ==")
for c in ORDER:
    got = read(c + "/part-r-*"); exp = cats.get(c, [])
    same = sorted(got) == sorted(exp)
    print("  %-20s expected %-7d job wrote %-7d identical: %s" % (c, len(exp), len(got), same))
    if not same:
        bad += 1
        for l in sorted(set(exp) - set(got))[:3]: print("     missing:", l)
        for l in sorted(set(got) - set(exp))[:3]: print("     extra  :", l)
tot = sum(len(v) for k, v in cats.items() if k not in ("ORPHAN_TXN", "ORPHAN_SETTLEMENT"))
print("  every order is in exactly one category: %s (orders %d, classified %d)" % (tot == len(order_rec), len(order_rec), tot))
if tot != len(order_rec): bad += 1
exp_sum = sorted("\t".join([c, str(f[0]), money(f[1]), money(f[2]), money(f[3])]) for c, f in feed.items())
got_sum = sorted(read("summary/part-r-*"))
print("\n== summary (category, count, order_amount_inr, net_paid_inr, settled_inr) ==\n  identical: %s" % (exp_sum == got_sum))
for l in exp_sum: print("   " + l)
if exp_sum != got_sum:
    bad += 1
    for l in sorted(set(exp_sum) - set(got_sum))[:3]: print("     missing:", l)
    for l in sorted(set(got_sum) - set(exp_sum))[:3]: print("     extra  :", l)

print("\n== planted cases (any order whose id starts with ORD_/ or special ids; plus examples by property) ==")
def show(title, pred, n=3):
    print("  -- " + title)
    k = 0
    for c, lst in cats.items():
        for l in lst:
            try: ok = pred(c, l)
            except IndexError: ok = False                      # orphan lines have fewer columns than order lines
            if ok and k < n: print("     %-18s %s" % (c, l)); k += 1
    if k == 0: print("     none found")
cases = [("FAILED then SUCCESS (several transactions, one SUCCESS)", lambda c, l: l.split("\t")[10] != "0" and l.split("\t")[8] != "0" and c not in ("ORPHAN_TXN", "ORPHAN_SETTLEMENT")),
         ("SUCCESS + partial REFUND", lambda c, l: c not in ("ORPHAN_TXN", "ORPHAN_SETTLEMENT") and l.split("\t")[9] != "0" and l.split("\t")[8] != "0"),
         ("non-INR order (FX conversion)", lambda c, l: c not in ("ORPHAN_TXN", "ORPHAN_SETTLEMENT") and l.split("\t")[2] != "INR"),
         ("order without any transaction", lambda c, l: c not in ("ORPHAN_TXN", "ORPHAN_SETTLEMENT") and l.split("\t")[7] == "0"),
         ("orphan transaction", lambda c, l: c == "ORPHAN_TXN"), ("orphan settlement", lambda c, l: c == "ORPHAN_SETTLEMENT")]
for t, p in cases: show(t, p)
print("  -- transaction settled twice (txn_id, settlement count, order):")
k = 0
for tid, rec in txn_rec.items():
    if settl[tid][1] > 1 and k < 3: print("     %s settled %d times, sum %s -> order %s" % (tid, settl[tid][1], money(settl[tid][0]), rec.split("\t")[0])); k += 1
if k == 0: print("     none found")
print("\nRESULT:", "all identical" if not bad else "%d differences" % bad)
sys.exit(1 if bad else 0)

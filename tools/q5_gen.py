#!/usr/bin/env python3
"""Seeded generator of a SMALL Q5-style dataset with the planted cases (only for local testing; use your real q5 data).

    python3 tools/q5_gen.py testdata_small/q5 [orders=3000]
Writes orders.csv, gateway_txns.csv, bank_settlements.csv, fx_rates.csv (with header rows, formats as in the assignment).
Planted: ORD_FS (FAILED then SUCCESS), ORD_REF (SUCCESS + partial REFUND), ORD_USD (USD, fx), ORD_TWICE (settled twice),
TXN_ORPHAN (unknown order), SET_ORPHAN (unknown txn), ORD_NOTXN, ORD_TOL100/ORD_TOL101 (diff exactly 1.00 / 1.01).
"""
import os, random, sys
from datetime import date, datetime, timedelta

out = sys.argv[1] if len(sys.argv) > 1 else "testdata/q5"
n = int(sys.argv[2]) if len(sys.argv) > 2 else 3000
os.makedirs(out, exist_ok=True)
rnd = random.Random(2026)
D0 = date(2026, 3, 1)
orders, txns, sets = [], [], []
tid = [0]; sid = [0]
def T(order, amt, ccy, st, ts):
    tid[0] += 1; x = "T%07d" % tid[0]; txns.append([x, order, amt, ccy, st, ts]); return x
def S(txn, amt, d):
    sid[0] += 1; sets.append(["S%07d" % sid[0], txn, amt, d])
fx = {}
for i in range(0, 70):
    d = D0 + timedelta(days=i)
    fx[(d.isoformat(), "USD")] = "%.4f" % (82.5 + 0.05 * i + (i % 7) * 0.01)
    fx[(d.isoformat(), "EUR")] = "%.4f" % (90.1 + 0.04 * i)
fx_rows = [[d, c, r] for (d, c), r in sorted(fx.items())]
fx_rows = [r for r in fx_rows if not (r[1] == "EUR" and r[0] == "2026-03-20")]      # a missing day -> fallback to the earlier day

def money(x): return "%.2f" % x
for i in range(n):
    oid = "O%07d" % (i + 1); day = D0 + timedelta(days=rnd.randint(0, 40)); ts = "%s %02d:%02d:%02d" % (day, rnd.randint(0, 23), rnd.randint(0, 59), rnd.randint(0, 59))
    ccy = rnd.choices(["INR", "USD", "EUR"], [88, 8, 4])[0]
    amt = round(rnd.uniform(100, 9000), 2) if ccy == "INR" else round(rnd.uniform(5, 300), 2)
    orders.append([oid, "C%05d" % rnd.randint(1, 800), money(amt), ccy, ts])
    r = rnd.random(); sd = (day + timedelta(days=2)).isoformat()
    inr = float(amt) * (1 if ccy == "INR" else float(fx.get((day.isoformat(), ccy), "90")))
    if r < 0.70:
        x = T(oid, money(amt), ccy, "SUCCESS", ts); S(x, money(round(inr, 2)), sd)
    elif r < 0.78:
        T(oid, money(amt), ccy, "FAILED", ts); x = T(oid, money(amt), ccy, "SUCCESS", ts); S(x, money(round(inr, 2)), sd)
    elif r < 0.83:
        T(oid, money(amt), ccy, "SUCCESS", ts)                                          # paid, not settled
    elif r < 0.87:
        x = T(oid, money(round(amt * 0.8, 2)), ccy, "SUCCESS", ts); S(x, money(round(float(amt) * 0.8 * (1 if ccy == "INR" else 85), 2)), sd)
    elif r < 0.89:
        x = T(oid, money(round(amt * 1.3, 2)), ccy, "SUCCESS", ts); S(x, money(round(float(amt) * 1.3 * (1 if ccy == "INR" else 85), 2)), sd)
    elif r < 0.92:
        T(oid, money(amt), ccy, "FAILED", ts)                                           # only failed -> unpaid
    elif r < 0.94:
        pass                                                                            # no transaction at all
    elif r < 0.97:
        x = T(oid, money(amt), ccy, "SUCCESS", ts); S(x, money(round(inr, 2)), sd)
        T(oid, money(round(amt * 0.25, 2)), ccy, "REFUNDED", ts)
    else:
        x = T(oid, money(amt), ccy, "SUCCESS", ts); S(x, money(round(inr + rnd.choice([-7, 5, 12.5]), 2)), sd)   # settlement mismatch
# ---- planted ----
orders.append(["ORD_FS", "C90001", "1000.00", "INR", "2026-03-10 09:00:00"])
T("ORD_FS", "1000.00", "INR", "FAILED", "2026-03-10 09:00:05"); x = T("ORD_FS", "1000.00", "INR", "SUCCESS", "2026-03-10 09:03:11"); S(x, "1000.00", "2026-03-12")
orders.append(["ORD_REF", "C90002", "2000.00", "INR", "2026-03-11 10:00:00"])
x = T("ORD_REF", "2000.00", "INR", "SUCCESS", "2026-03-11 10:01:00"); S(x, "2000.00", "2026-03-13"); T("ORD_REF", "500.00", "INR", "REFUNDED", "2026-03-15 11:00:00")
orders.append(["ORD_USD", "C90003", "100.00", "USD", "2026-03-12 12:00:00"])
x = T("ORD_USD", "100.00", "USD", "SUCCESS", "2026-03-12 12:02:00"); S(x, "%.2f" % (100 * float(fx[("2026-03-12", "USD")])), "2026-03-14")
orders.append(["ORD_TWICE", "C90004", "700.00", "INR", "2026-03-13 13:00:00"])
x = T("ORD_TWICE", "700.00", "INR", "SUCCESS", "2026-03-13 13:01:00"); S(x, "700.00", "2026-03-15"); S(x, "700.00", "2026-03-16")
txns.append(["T_ORPHAN", "ORD_NOSUCH", "300.00", "INR", "SUCCESS", "2026-03-14 14:00:00"])
sets.append(["S_ORPHAN", "T_NOSUCH", "450.00", "2026-03-15"])
orders.append(["ORD_NOTXN", "C90005", "999.00", "INR", "2026-03-14 15:00:00"])
orders.append(["ORD_TOL100", "C90006", "500.00", "INR", "2026-03-15 10:00:00"]); x = T("ORD_TOL100", "499.00", "INR", "SUCCESS", "2026-03-15 10:01:00"); S(x, "499.00", "2026-03-16")
orders.append(["ORD_TOL101", "C90007", "500.00", "INR", "2026-03-15 10:00:00"]); x = T("ORD_TOL101", "498.99", "INR", "SUCCESS", "2026-03-15 10:01:00"); S(x, "498.99", "2026-03-16")
orders.append(["ORD_EUR20", "C90008", "50.00", "EUR", "2026-03-20 08:00:00"]); x = T("ORD_EUR20", "50.00", "EUR", "SUCCESS", "2026-03-20 08:01:00"); S(x, "%.2f" % (50 * float(fx[("2026-03-19", "EUR")])), "2026-03-22")
txns.append(["T_BAD", "O0000001", "abc", "INR", "SUCCESS", "2026-03-01 00:00:00"])
sets.append(["S_BAD", "T0000001"])
def w(name, header, rows, files=1):
    rnd.shuffle(rows)
    with open(os.path.join(out, name), "w") as f:
        f.write(header + "\n")
        for r in rows: f.write(",".join(r) + "\n")
w("orders.csv", "order_id,customer_id,order_amount,currency,order_ts", orders)
w("gateway_txns.csv", "txn_id,order_id,amount,currency,status,txn_ts", txns)
w("bank_settlements.csv", "settlement_id,txn_id,settled_amount_inr,settlement_date", sets)
w("fx_rates.csv", "date,currency,inr_rate", fx_rows)
print("orders %d txns %d settlements %d fx %d -> %s" % (len(orders), len(txns), len(sets), len(fx_rows), out))

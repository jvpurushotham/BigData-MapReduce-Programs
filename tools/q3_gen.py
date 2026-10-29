#!/usr/bin/env python3
"""Seeded generator of a SMALL Q3-style dataset with the planted cases.

    python3 tools/q3_gen.py testdata/q3          # writes orders.csv, returns.csv, products.csv

Files (with header rows, like the schemas in the assignment):
  orders.csv   order_id,line_no,order_ts,customer_id,product_id,qty,unit_price,status
  returns.csv  return_id,order_id,line_no,qty_returned,return_ts
  products.csv product_id,product_name,category

Planted cases (category TESTCAT, month 2026-03 unless noted):
  * T01..T06: T05 and T06 have IDENTICAL net revenue and units -> they compete for rank 5; the strict tie-break
    (units desc, then product_id asc) keeps T05 and drops T06.
  * O_TWICE line 1 is returned in TWO separate returns (R_TWICE_A, R_TWICE_B).
  * O_OVER line 1 (qty 2) is returned with qty 5 -> RETURN_EXCEEDS_ORDER, net_qty capped at 0.
  * O_JAN31 line 1 is ordered 2026-01-31 22:47:10 and returned 2026-02-02 -> revenue stays in 2026-01.
  * product PMISSING is not in products.csv -> category UNMAPPED.
  plus: an orphan return, a CANCELLED line with a return, an exactly duplicated order line, a duplicated return row,
  malformed rows (bad qty, bad timestamp, wrong field count), a product name containing a comma.
"""
import os, random, sys
from datetime import datetime, timedelta

out = sys.argv[1] if len(sys.argv) > 1 else "testdata/q3"
n_lines = int(sys.argv[2]) if len(sys.argv) > 2 else 6000
os.makedirs(out, exist_ok=True)
rnd = random.Random(2026)

cats = ["Books", "Toys", "Garden", "Kitchen", "Sports", "Music", "Office", "Pets", "Tools", "Beauty", "Games", "Travel"]
products = []                                     # (id, name, category)
for i in range(150):
    pid = "P%05d" % (i + 1)
    name = "Item %d" % (i + 1)
    if i == 7: name = "Wrench, adjustable \"pro\""      # comma + quotes inside the name -> needs a CSV parser
    products.append((pid, name, cats[i % len(cats)]))

orders, returns = [], []
fmt = "%Y-%m-%d %H:%M:%S"
start = datetime(2026, 1, 1)
oid = 0
while len(orders) < n_lines:
    oid += 1
    order_id = "O%07d" % oid
    ts = start + timedelta(seconds=rnd.randint(0, 150 * 86400 - 1))
    cust = "C%05d" % rnd.randint(1, 900)
    for ln in range(1, rnd.randint(1, 4) + 1):
        pid = rnd.choice(products)[0]
        qty = rnd.randint(1, 5)
        price = round(rnd.uniform(5, 400), 2)
        status = rnd.choices(["DELIVERED", "SHIPPED", "CANCELLED"], [80, 15, 5])[0]
        orders.append([order_id, str(ln), ts.strftime(fmt), cust, pid, str(qty), "%.2f" % price, status])
        if status != "CANCELLED" and rnd.random() < 0.06:
            rts = ts + timedelta(days=rnd.randint(1, 20))
            returns.append(["R%07d" % (len(returns) + 1), order_id, str(ln), str(rnd.randint(1, qty)), rts.strftime(fmt)])

# ---- planted ---------------------------------------------------------------------------------------------
for i in range(1, 7):
    products.append(("T%02d" % i, "Test product %d" % i, "TESTCAT"))
revenue_plan = {1: (10, 100.00), 2: (8, 100.00), 3: (6, 100.00), 4: (4, 100.00), 5: (3, 100.00), 6: (3, 100.00)}
for i, (qty, price) in revenue_plan.items():          # T05 and T06: same units (3) and same price -> same revenue
    orders.append(["OT%03d" % i, "1", "2026-03-%02d 12:00:00" % (10 + i), "C90001", "T%02d" % i, str(qty), "%.2f" % price, "DELIVERED"])
orders.append(["O_TWICE", "1", "2026-03-05 09:00:00", "C90002", "P00001", "4", "50.00", "DELIVERED"])
returns.append(["R_TWICE_A", "O_TWICE", "1", "1", "2026-03-07 10:00:00"])
returns.append(["R_TWICE_B", "O_TWICE", "1", "2", "2026-03-09 11:30:00"])
orders.append(["O_OVER", "1", "2026-03-06 09:00:00", "C90003", "P00002", "2", "80.00", "DELIVERED"])
returns.append(["R_OVER", "O_OVER", "1", "5", "2026-03-08 10:00:00"])
orders.append(["O_JAN31", "1", "2026-01-31 22:47:10", "C90004", "P00003", "3", "1299.00", "DELIVERED"])
returns.append(["R_JAN31", "O_JAN31", "1", "1", "2026-02-02 10:05:00"])
orders.append(["O_MISS", "1", "2026-03-15 08:00:00", "C90005", "PMISSING", "2", "75.50", "DELIVERED"])
orders.append(["O_CANC", "1", "2026-03-16 08:00:00", "C90006", "P00004", "1", "20.00", "CANCELLED"])
returns.append(["R_CANC", "O_CANC", "1", "1", "2026-03-18 08:00:00"])
returns.append(["R_ORPHAN", "O_NOPE", "1", "1", "2026-03-18 08:00:00"])
orders.append(list(orders[5]))                                           # exact duplicate of an order line
returns.append(list(returns[2]))                                         # duplicated return row
bad_orders = ["O_BAD1,1,2026-03-01 10:00:00,C1,P00005,abc,10.00,DELIVERED",     # non-numeric qty
              "O_BAD2,1,01/03/2026 10:00,C1,P00005,1,10.00,DELIVERED",           # bad timestamp
              "O_BAD3,1,2026-03-01 10:00:00,C1,P00005,1",                        # too few fields
              "O_BAD4,1,2026-03-01 10:00:00,C1,P00005,0,10.00,DELIVERED",        # qty 0
              ""]
bad_returns = ["R_BAD1,O_TWICE,1,x,2026-03-10 10:00:00", "R_BAD2,O_TWICE"]

def w(path, header, rows, extra=()):
    with open(path, "w") as f:
        f.write(header + "\n")
        lines = [",".join('"%s"' % c.replace('"', '""') if ("," in c or '"' in c) else c for c in r) for r in rows] + list(extra)
        rnd.shuffle(lines)
        f.write("\n".join(lines) + "\n")

w(os.path.join(out, "orders.csv"), "order_id,line_no,order_ts,customer_id,product_id,qty,unit_price,status", orders, bad_orders)
w(os.path.join(out, "returns.csv"), "return_id,order_id,line_no,qty_returned,return_ts", returns, bad_returns)
w(os.path.join(out, "products.csv"), "product_id,product_name,category", products)
print("orders %d  returns %d  products %d -> %s" % (len(orders) + len(bad_orders), len(returns) + len(bad_returns), len(products), out))

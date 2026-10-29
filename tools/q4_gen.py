#!/usr/bin/env python3
"""Seeded generator of a SMALL Q4-style click log with the planted cases (only for local testing; use your real q4 data).

    python3 tools/q4_gen.py testdata_small/q4/clicks [big_user_events=30000]

Format: user_id|event_ts_epoch_ms|url|referrer|user_agent   (6 files, lines shuffled over the files)
Planted users: P_30MIN (exactly 30:00 apart = same session, 30:00.001 = new session), P_MIDNIGHT (crosses 00:00 IST),
P_REVERSE (events written newest-first, spread over several files), P_SAMETS (identical timestamps),
P_CARTFIRST (/home, /cart, /product, never converts), P_FULL (whole funnel), P_BOTFAST (avg gap < 1 s), P_BIG (many events).
"""
import os, random, sys

out = sys.argv[1] if len(sys.argv) > 1 else "testdata/q4/clicks"
big = int(sys.argv[2]) if len(sys.argv) > 2 else 30000
os.makedirs(out, exist_ok=True)
rnd = random.Random(2026)
IST = 19800000
BASE = 1791000000000 - (1791000000000 + IST) % 86400000        # 00:00 IST of some day, in epoch ms
UA = ["Mozilla/5.0 (X11; Linux x86_64)", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15)", "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X)"]
ev = []                                                       # (user, ts, url, ref)
def add(u, ts, url, ref="/home"): ev.append((u, ts, url, ref))
H = 3600000; M = 60000

pages = ["/home", "/search", "/about", "/cart"] + ["/product/P%05d" % i for i in range(1, 60)]
for i in range(400):                                           # ordinary users: 1-3 visits over 3 days
    u = "U%07d" % (1000 + i)
    for _ in range(rnd.randint(1, 3)):
        t = BASE + rnd.randint(0, 3 * 86400000 - 1)
        seq = ["/home"] if rnd.random() < 0.8 else []
        for _ in range(rnd.randint(1, 8)): seq.append(rnd.choice(pages))
        if rnd.random() < 0.3: seq += ["/cart"]
        if "/cart" in seq and rnd.random() < 0.5: seq += ["/checkout/success"]
        for url in seq:
            t += rnd.randint(2000, 12 * M); add(u, t, url)
t = BASE + 10 * H                                              # exactly 30:00 -> same session, 30:00.001 -> new session
add("P_30MIN", t, "/home"); add("P_30MIN", t + 30 * M, "/product/P00001"); add("P_30MIN", t + 60 * M + 1, "/cart")
add("P_30MIN", t + 90 * M + 1, "/checkout/success")
t = BASE + 86400000 - 10 * M                                   # session crossing midnight IST: splits at 00:00
add("P_MIDNIGHT", t, "/home"); add("P_MIDNIGHT", t + 5 * M, "/product/P00002"); add("P_MIDNIGHT", t + 12 * M, "/cart"); add("P_MIDNIGHT", t + 14 * M, "/checkout/success")
t = BASE + 11 * H                                              # reverse order, written newest first
for k, url in reversed(list(enumerate(["/home", "/product/P00003", "/product/P00004", "/cart", "/checkout/success"]))): add("P_REVERSE", t + k * 3 * M, url)
t = BASE + 12 * H                                              # identical timestamps
add("P_SAMETS", t, "/home"); add("P_SAMETS", t + 1000, "/product/P00005"); add("P_SAMETS", t + 1000, "/product/P00006"); add("P_SAMETS", t + 1000, "/cart")
t = BASE + 13 * H                                              # cart before product
for k, url in enumerate(["/home", "/cart", "/product/P00007", "/search"]): add("P_CARTFIRST", t + k * 2 * M, url)
t = BASE + 14 * H
for k, url in enumerate(["/home", "/product/P00008", "/cart", "/checkout/success"]): add("P_FULL", t + k * M, url)
t = BASE + 15 * H                                              # avg gap 100 ms
for k in range(30): add("P_BOTFAST", t + k * 100, rnd.choice(pages))
t = BASE + 86400000 + 8 * H                                    # one huge user: bursts of > 500 events plus small normal visits
tt = t
for k in range(big):
    tt += rnd.choice([200, 800, 1500, 3000])
    if k % 5000 == 4999: tt += 3 * H
    add("P_BIG", tt, rnd.choice(pages))
tt += 5 * H
for url in ["/home", "/product/P00009", "/cart", "/checkout/success"]: tt += M; add("P_BIG", tt, url)

# some malformed rows
bad = ["BAD_USER||/home|-|x", "U0009999|notanumber|/home|-|x", "U0009999|1791000000000|/home"]
lines = ["%s|%d|%s|%s|%s" % (u, ts, url, ref, rnd.choice(UA)) for u, ts, url, ref in ev] + bad
rnd.shuffle(lines)
files = [open(os.path.join(out, "clicks_%d.log" % i), "w") for i in range(6)]
for ln in lines: files[rnd.randrange(6)].write(ln + "\n")
for f in files: f.close()
print("events %d -> %s (6 files)" % (len(lines), out))

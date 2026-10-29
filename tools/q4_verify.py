#!/usr/bin/env python3
"""Independent check of Q4 (plain Python re-implementation of the specification).

    python3 tools/q4_verify.py <clicks dir> <result dir>   [--gap-min 30] [--bot-events 500] [--bot-gap-ms 1000]

<result dir> = the driver's output copied out of HDFS (session_job/sessions, session_job/bots, funnel/).
Rules re-implemented: sort by (user, ts, url); new session when gap > 30 min (strict) or the IST date changes; bot when
events > 500 or (events >= 2 and average gap < 1000 ms); funnel = greedy in-order stages /home -> /product/* -> /cart -> /checkout/success.
"""
import argparse, glob, os, sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

ap = argparse.ArgumentParser()
ap.add_argument("clicks"); ap.add_argument("out")
ap.add_argument("--gap-min", type=int, default=30); ap.add_argument("--bot-events", type=int, default=500)
ap.add_argument("--bot-gap-ms", type=int, default=1000)
a = ap.parse_args()
OFF = 19800000
IST = timezone(timedelta(hours=5, minutes=30))

def iso(ms):
    d = datetime.fromtimestamp(ms // 1000, IST)
    return d.strftime("%Y-%m-%dT%H:%M:%S") + ".%03d+05:30" % (ms % 1000)
def day(ms): return (ms + OFF) // 86400000
def date(ms): return datetime.fromtimestamp(ms // 1000, IST).strftime("%Y-%m-%d")
def path(u):
    cut = [i for i in (u.find("?"), u.find("#")) if i >= 0]
    return u[:min(cut)] if cut else u

C = defaultdict(int)
users = defaultdict(list)
EV = defaultdict(list)                     # evidence examples for the report
last_seen = {}                             # user -> (file index, ts) of the previous line in READ order
files = sorted(glob.glob(os.path.join(a.clicks, "**", "*.log"), recursive=True))
if not files: sys.exit("no *.log files under " + a.clicks)
for fi, p in enumerate(files):
    for line in open(p, encoding="utf-8", errors="replace"):
        line = line.rstrip("\r\n")
        if not line.strip(): continue
        C["EVENTS_READ"] += 1
        f = line.split("|", 4)
        if len(f) < 5: C["BAD_FIELD_COUNT"] += 1; continue
        u = f[0].strip()
        if not u: C["BAD_USER"] += 1; continue
        try: ts = int(f[1].strip())
        except ValueError: C["BAD_TIMESTAMP"] += 1; continue
        if ts < 0: C["BAD_TIMESTAMP"] += 1; continue
        url = f[2].strip()
        if not url: C["BAD_URL"] += 1; continue
        users[u].append((ts, url))
        prev = last_seen.get(u)
        if prev and ts < prev[1] and fi != prev[0] and len(EV["reverse"]) < 5:
            EV["reverse"].append("%s: ts %d (file %s) is older than ts %d seen before in file %s" % (u, ts, os.path.basename(p), prev[1], os.path.basename(files[prev[0]])))
        last_seen[u] = (fi, ts)

gap = a.gap_min * 60000
sessions = []   # (user, seq, start, end, count, distinct, entry, exit, stage)
for u, evs in users.items():
    evs.sort()
    C["USERS"] += 1
    cur = None; seq = 0
    for ts, url in evs:
        if cur is not None:
            g = ts - cur["last"]
            if g == 0:
                C["EVENTS_SAME_TS_AS_PREVIOUS"] += 1
                if len(EV["same_ts"]) < 5: EV["same_ts"].append("%s: two events at %s (%d)" % (u, iso(ts), ts))
            if g == gap and len(EV["exact30"]) < 5: EV["exact30"].append("%s: %s and %s are exactly %d min apart -> same session" % (u, iso(cur["last"]), iso(ts), a.gap_min))
            if g == gap + 1 and len(EV["gap_plus1"]) < 5: EV["gap_plus1"].append("%s: %s and %s are %d min + 1 ms apart -> new session" % (u, iso(cur["last"]), iso(ts), a.gap_min))
            by_gap = g > gap; by_date = day(ts) != day(cur["last"])
            if by_gap or by_date:
                C["SPLIT_BY_GAP" if by_gap else "SPLIT_BY_MIDNIGHT"] += 1
                if not by_gap and len(EV["midnight"]) < 5: EV["midnight"].append("%s: session %d ends %s, next event %s (date changed, gap only %d s)" % (u, seq, iso(cur["last"]), iso(ts), g // 1000))
                sessions.append(cur); cur = None
            elif g == gap: C["GAP_EXACTLY_30_MIN_KEPT"] += 1
        if cur is None:
            seq += 1
            cur = dict(user=u, seq=seq, start=ts, last=ts, n=0, urls=set(), entry=url, exit=url, stage=0, cart_first=False)
        cur["n"] += 1; cur["last"] = ts; cur["exit"] = url; cur["urls"].add(url)
        p = path(url)
        if cur["stage"] < 2 and p == "/cart": cur["cart_first"] = True
        s = cur["stage"]
        if (s == 0 and p == "/home") or (s == 1 and p.startswith("/product/") and len(p) > 9) or (s == 2 and p == "/cart") or (s == 3 and p == "/checkout/success"):
            cur["stage"] = s + 1
    if cur is not None: sessions.append(cur)

normal, bots = [], []
fun = defaultdict(lambda: [0] * 5)
for s in sessions:
    C["SESSIONS"] += 1
    dur = s["last"] - s["start"]
    many = s["n"] > a.bot_events
    fast = s["n"] >= 2 and dur < a.bot_gap_ms * (s["n"] - 1)
    line = "\t".join([s["user"], str(s["seq"]), iso(s["start"]), iso(s["last"]), "%d.%03d" % (dur // 1000, dur % 1000),
                      str(s["n"]), str(len(s["urls"])), s["entry"], s["exit"]])
    if many or fast:
        C["SESSIONS_BOT"] += 1
        C["BOT_BOTH_REASONS" if many and fast else "BOT_TOO_MANY_EVENTS" if many else "BOT_FAST_AVG_GAP"] += 1
        reason = "EVENTS+AVG_GAP" if many and fast else ("EVENTS>%d" % a.bot_events) if many else ("AVG_GAP<%dms" % a.bot_gap_ms)
        bots.append(line + "\t" + reason); continue
    C["SESSIONS_NORMAL"] += 1; normal.append(line); C["FUNNEL_RECORDS"] += 1
    if s["cart_first"]:
        C["CART_BEFORE_PRODUCT_SESSIONS"] += 1
        if len(EV["cart_first"]) < 5: EV["cart_first"].append("%s session %d (%s): /cart seen before any /product, deepest stage counted = %d" % (s["user"], s["seq"], iso(s["start"]), s["stage"]))
    for tot in (date(s["start"]), "ALL"):
        f = fun[tot]; f[0] += 1
        for i in range(1, 5):
            if s["stage"] >= i: f[i] += 1
    for i, nm in enumerate(["HOME", "PRODUCT", "CART", "SUCCESS"], 1):
        if s["stage"] >= i: C["FUNNEL_STAGE_" + nm] += 1

def pct(n, d):
    if d == 0: return "NA"
    h = (2 * n * 10000 + d) // (2 * d)
    return "%d.%02d" % (h // 100, h % 100)
funnel = []
for k in sorted(fun):
    c = fun[k]
    funnel.append("\t".join([k] + [str(x) for x in c] + [pct(c[2], c[1]), pct(c[3], c[2]), pct(c[4], c[3]), pct(c[4], c[1])]))

def read(pattern):
    r = []
    for p in sorted(glob.glob(os.path.join(a.out, pattern))):
        r += [l.rstrip("\n") for l in open(p, encoding="utf-8") if l.strip()]
    return r

print("== expected counters (compare with the [Q4] lines) ==")
for k in sorted(C): print("  %-30s %d" % (k, C[k]))
bad = 0
for name, exp, pattern in (("sessions", normal, "session_job/sessions/part-r-*"), ("bots", bots, "session_job/bots/part-r-*"),
                           ("funnel", funnel, "funnel/part-r-*")):
    got = read(pattern)
    same = sorted(exp) == sorted(got)
    print("\n== %s ==\n  expected %d rows, job wrote %d rows, identical: %s" % (name, len(exp), len(got), same))
    if not same:
        bad += 1
        for l in sorted(set(exp) - set(got))[:5]: print("   missing:", l)
        for l in sorted(set(got) - set(exp))[:5]: print("   extra  :", l)
# partitioner check: every user in exactly one reducer file
seen = defaultdict(set)
for p in glob.glob(os.path.join(a.out, "session_job/sessions/part-r-*")) + glob.glob(os.path.join(a.out, "session_job/bots/part-r-*")):
    for l in open(p, encoding="utf-8"): seen[l.split("\t")[0]].add(p.rsplit("-", 1)[1])
split = [u for u, s in seen.items() if len(s) > 1]
print("\n== partitioner ==\n  every user handled by exactly one reducer:", not split)
if split: bad += 1
print("\n== planted users (expected sessions) ==")
for s in sessions:
    if s["user"].startswith("P_") and s["user"] != "P_BIG" or (s["user"] == "P_BIG" and s["n"] > 500):
        print("  %-12s #%d %s -> %s events %-6d distinct %-3d stage %d %s" % (s["user"], s["seq"], iso(s["start"]), iso(s["last"]),
              s["n"], len(s["urls"]), s["stage"], "BOT" if (s["n"] > a.bot_events or (s["n"] >= 2 and s["last"] - s["start"] < a.bot_gap_ms * (s["n"] - 1))) else ""))
print("\n== evidence of the planted cases (first examples; look the users up in sessions/) ==")
for k, t in (("exact30", "events exactly 30:00 apart (same session)"), ("gap_plus1", "events 30:00.001 apart (new session)"),
             ("midnight", "session cut at midnight IST"), ("reverse", "events of one user in reverse order across files"),
             ("same_ts", "identical timestamps"), ("cart_first", "/cart before /product")):
    print("  -- " + t + ": " + ("none found" if not EV[k] else ""))
    for l in EV[k]: print("     " + l)
print("\nRESULT:", "all identical" if not bad else "%d differences" % bad)
sys.exit(1 if bad else 0)

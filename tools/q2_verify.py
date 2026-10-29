#!/usr/bin/env python3
"""Independent check of Q2: rebuilds the index from the RAW corpus in plain Python and compares it with the job output.

    python3 tools/q2_verify.py <corpus_dir> <stopwords.txt> <output_dir> [--phrase "big data analytics"] [--positions filtered|original]

<output_dir> must contain  index/part-r-*, index/corpus_stop-r-*, index/rare-r-*   (and  phrase/part-r-*  with --phrase).
Copy them out of HDFS first:   hdfs dfs -get /user/hadoop/ca2/q2/out q2_out
Exit status is 1 when anything differs.
"""
import argparse, glob, math, os, re, sys
from decimal import Decimal, ROUND_HALF_UP

ap = argparse.ArgumentParser()
ap.add_argument("corpus"); ap.add_argument("stopwords"); ap.add_argument("out")
ap.add_argument("--phrase"); ap.add_argument("--positions", default="filtered")
a = ap.parse_args()

stop = {w.strip().lower() for w in open(a.stopwords, encoding="utf-8") if w.strip() and not w.startswith("#")}
tok = lambda s: re.findall(r"[^\W_]+", s.lower())

cnt = {"BLANK_LINES": 0, "MALFORMED_LINES": 0, "HEADER_SKIPPED": 0, "EXTRA_FIELDS_MERGED": 0, "EMPTY_BODY_DOCS": 0,
       "EMPTY_TITLE_DOCS": 0, "DOCS": 0}
docs = {}                                   # doc_id -> list of (term, position) in stream order, plus tf parts
doc_ids = []
for path in sorted(glob.glob(os.path.join(a.corpus, "*.tsv"))):
    for raw in open(path, encoding="utf-8", newline=""):
        if not raw.strip():
            cnt["BLANK_LINES"] += 1; continue
        f = raw.rstrip("\r\n").split("\t")
        if len(f) < 2 or not f[0].strip():
            cnt["MALFORMED_LINES"] += 1; continue
        did = f[0].strip()
        if did.lower() == "doc_id":
            cnt["HEADER_SKIPPED"] += 1; continue
        title = f[1]
        body = "" if len(f) == 2 else (f[2] if len(f) == 3 else " ".join(f[2:]))
        if len(f) > 3: cnt["EXTRA_FIELDS_MERGED"] += 1
        if not title.strip(): cnt["EMPTY_TITLE_DOCS"] += 1
        if not body.strip(): cnt["EMPTY_BODY_DOCS"] += 1
        cnt["DOCS"] += 1
        doc_ids.append(did)
        stream = []                         # (term, position, in_title)
        pos = 0
        for part, text in ((True, title), (False, body)):
            for t in tok(text):
                if len(t) < 2 or t in stop:
                    if a.positions == "original": pos += 1
                    continue
                stream.append((t, pos, part)); pos += 1
        docs[did] = stream                  # duplicated doc_ids would overwrite: reported below
N = cnt["DOCS"]
print("== expected counters ==")
for k, v in cnt.items(): print("  %-22s %d" % (k, v))
if len(set(doc_ids)) != len(doc_ids):
    print("  WARNING: %d duplicated doc_ids in the corpus (the job counts every line)" % (len(doc_ids) - len(set(doc_ids))))

# ---- expected index --------------------------------------------------------------------------------------
terms = {}                                  # term -> {doc: [tf, [positions]]}
for did, stream in docs.items():
    for t, p, in_title in stream:
        e = terms.setdefault(t, {}).setdefault(did, [0, []])
        e[0] += 3 if in_title else 1
        e[1].append(p)

def row(term):
    d = terms[term]; df = len(d); idf = math.log10(N / df)
    items = []
    for did, (tf, ps) in d.items():
        items.append((-Decimal(tf * idf).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP), did, tf, ps))
    items.sort(key=lambda x: (x[0], x[1]))
    return df, " ".join("%s:%d:%s:[%s]" % (did, tf, format(-neg, "f"), ",".join(map(str, ps))) for neg, did, tf, ps in items)

exp = {"index": {}, "corpus_stop": {}, "rare": {}}
for t in terms:
    df, postings = row(t)
    kind = "corpus_stop" if df > 0.4 * N else ("rare" if df == 1 else "index")
    exp[kind][t] = (df, postings)

def load(pattern):
    r = {}
    for p in glob.glob(pattern):
        for line in open(p, encoding="utf-8"):
            line = line.rstrip("\n")
            if not line: continue
            term, df, postings = line.split("\t", 2)
            r[term] = (int(df), postings)
    return r

got = {"index": load(os.path.join(a.out, "index", "part-r-*")),
       "corpus_stop": load(os.path.join(a.out, "index", "corpus_stop-r-*")),
       "rare": load(os.path.join(a.out, "index", "rare-r-*"))}
bad = 0
print("\n== index files ==")
for kind in ("index", "corpus_stop", "rare"):
    e, g = exp[kind], got[kind]
    miss, extra = set(e) - set(g), set(g) - set(e)
    diff = [t for t in e if t in g and e[t] != g[t]]
    print("  %-12s expected %5d terms, job wrote %5d, missing %d, extra %d, different rows %d" % (kind, len(e), len(g), len(miss), len(extra), len(diff)))
    for t in diff[:3]: print("     DIFF", t, "\n       expected", e[t][1][:150], "\n       got     ", g[t][1][:150])
    for t in sorted(miss)[:3]: print("     missing", t)
    for t in sorted(extra)[:3]: print("     extra", t)
    bad += len(miss) + len(extra) + len(diff)
all_main = set(exp["index"]) | set(exp["corpus_stop"]) | set(exp["rare"])
print("  0.4 * N = %.1f, distinct terms = %d" % (0.4 * N, len(all_main)))

# ---- phrase ----------------------------------------------------------------------------------------------
if a.phrase:
    words = tok(a.phrase)
    print("\n== phrase %r (positions=%s) ==" % (words, a.positions))
    searchable = all(w in exp["index"] for w in words)
    expected = {}
    if searchable:                          # the job can only use the main index
        for did, stream in docs.items():
            at = {}
            for t, p, _ in stream: at.setdefault(t, set()).add(p)
            if all(w in at for w in words):
                c = sum(all((p + k) in at[words[k]] for k in range(1, len(words))) for p in sorted(at[words[0]]))
                if c: expected[did] = c
    else:
        print("  not every word is in the main index -> no result expected")
    ranked = sorted(expected.items(), key=lambda x: (-x[1], x[0]))
    pf = glob.glob(os.path.join(a.out, "phrase", "part-r-*"))
    res = []
    for p in pf:
        for line in open(p):
            if line.strip():
                d, c = line.rstrip("\n").split("\t"); res.append((d, int(c)))
    print("  expected %d documents, job returned %d, identical order: %s" % (len(ranked), len(res), ranked == res))
    for d, c in ranked[:10]: print("    %s  %d" % (d, c))
    if ranked != res: bad += 1

# ---- planted cases ---------------------------------------------------------------------------------------
print("\n== planted cases ==")
for d in ("D900001", "D900004", "D900005", "D900006", "D900007"):
    if d in docs: print("  %s tokens: %s" % (d, " ".join("%s@%d" % (t, p) for t, p, _ in docs[d][:14])))
for t in ("zephyrine", "quasar"):
    for kind in exp:
        if t in exp[kind]: print("  term %-10s -> %-11s df=%d  %s" % (t, kind, exp[kind][t][0], exp[kind][t][1]))
print("\nRESULT:", "all identical" if not bad else "%d differences" % bad)
sys.exit(1 if bad else 0)

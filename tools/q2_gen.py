#!/usr/bin/env python3
"""Seeded generator of a SMALL Q2-style corpus (doc_id TAB title TAB body) with planted cases.

    python3 tools/q2_gen.py testdata/q2          # writes testdata/q2/corpus/*.tsv and testdata/q2/stopwords.txt

Planted cases (all doc ids start with D9):
  D900001  body contains "big data big data analytics"                      (phrase occurrences: 1 for the 3-word phrase, 2 for "big data")
  D900002  "zephyrine" occurs ONLY in the title (df = 1 -> goes to rare)
  D900003  "quasar" occurs ONLY in the title; D900008 too (df = 2, equal tf-idf -> doc_id tie-break)
  D900004  empty body (3 columns, last one empty);  D900005 has only 2 columns
  D900006  contains big, data and analytics but NEVER next to each other
  D900007  "big the data analytics": adjacent only if stop-words do not occupy a position
  plus: blank line, line without tabs, line with empty doc_id, line with extra tabs, mixed case, digits, 1-letter tokens.
"""
import os, random, sys

out = sys.argv[1] if len(sys.argv) > 1 else "testdata/q2"
n_docs = int(sys.argv[2]) if len(sys.argv) > 2 else 2000
os.makedirs(os.path.join(out, "corpus"), exist_ok=True)
rnd = random.Random(2026)

stop = ["the", "a", "an", "of", "and", "to", "in", "is", "it", "for", "on", "with", "as", "by", "at", "this", "that", "from", "or", "be"]
common = ["system", "process"]                                     # will be in > 40 % of the documents -> corpus_stop
vocab = ["hadoop", "spark", "yarn", "hdfs", "avro", "driver", "mapper", "reducer", "shuffle", "combiner", "cluster", "node",
         "block", "replica", "scheduler", "queue", "metrics", "counter", "partition", "sort", "join", "filter", "pattern",
         "index", "query", "stream", "batch", "record", "key", "value", "cache", "memory", "disk", "network", "latency",
         "throughput", "schema", "parquet", "orc", "hive", "pig", "oozie", "kafka", "flume", "sqoop", "zookeeper", "big",
         "data", "analytics", "cloud", "storage", "compute", "engine", "tuning", "skew", "window", "session", "event"]
weights = [1.0 / (i + 1) ** 0.7 for i in range(len(vocab))]


def word():
    return rnd.choices(vocab, weights)[0]


def made_up():
    return "".join(rnd.choice("bcdfghjklmnpqrstvwxz") + rnd.choice("aeiou") for _ in range(rnd.randint(3, 5)))


def noisy(w):
    r = rnd.random()
    if r < 0.05: return w.upper()
    if r < 0.10: return w.capitalize()
    if r < 0.13: return w + ","
    if r < 0.15: return "(" + w + ")"
    return w


def sentence(k):
    toks = []
    for _ in range(k):
        r = rnd.random()
        if r < 0.22: toks.append(rnd.choice(stop))
        elif r < 0.25: toks.append(rnd.choice("xyzq") if rnd.random() < 0.5 else str(rnd.randint(0, 9)))
        elif r < 0.30: toks.append(made_up())
        elif r < 0.33: toks.append(str(rnd.randint(10, 999)))
        elif r < 0.36: toks.append(word() + "-" + word())
        else: toks.append(word())
    return " ".join(noisy(t) for t in toks)


docs = []
for i in range(n_docs):
    title = " ".join(noisy(word()) for _ in range(rnd.randint(1, 4)))
    body = sentence(rnd.randint(15, 60))
    if rnd.random() < 0.6:                                            # corpus-stop terms
        body += " " + " ".join(rnd.choice(common) for _ in range(rnd.randint(1, 3)))
    if rnd.random() < 0.015:                                          # genuine adjacent phrase in a few docs
        body += " big data analytics"
    if rnd.random() < 0.01:
        title += " big data"
    docs.append("D%06d\t%s\t%s" % (i + 1, title, body))

planted = [
    "D900001\tStreaming Pipelines Overview\tBig Data Big Data Analytics runs here",
    "D900002\tZephyrine Notes\tregular words about hadoop and yarn",
    "D900003\tQuasar Dispatch\thadoop yarn queue notes",
    "D900004\tEmpty Body Title\t",
    "D900005\tOnly Two Columns",
    "D900006\tScattered Words\tbig hadoop yarn data queue shuffle analytics end",
    "D900007\tStopword Gap\tbig the data analytics appears with a stop word inside",
    "D900008\tQuasar Weekly\tcluster metrics counter",
]
bad = [
    "",                                                              # blank line
    "this line has no tabs at all",                                  # malformed
    "\tNo Id Title\tbody text without an id",                        # empty doc_id
    "D900009\tExtra\tTabs\tinside body",                             # 4 columns
]

lines = docs + planted + bad
rnd.shuffle(lines)
files = [[] for _ in range(4)]
for ln in lines:
    files[rnd.randrange(4)].append(ln)
for i, f in enumerate(files):
    with open(os.path.join(out, "corpus", "corpus_part%d.tsv" % i), "w") as fh:
        fh.write("\n".join(f) + "\n")
with open(os.path.join(out, "stopwords.txt"), "w") as fh:
    fh.write("\n".join(stop) + "\n")
print("wrote %d lines (%d documents incl. planted) to %s" % (len(lines), len(docs) + len(planted) + 1, out))

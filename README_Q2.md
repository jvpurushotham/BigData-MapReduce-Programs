# Q2 – Positional inverted index, TF-IDF, phrase search (notes for the report and the viva)

## Pipeline (one driver: `ca2.q2.Q2Driver`)

| Job | Class | Pattern | Map in → out | Reduce in → out |
|---|---|---|---|---|
| 1 | `CountDocsJob` | Counting (map-only) | `LongWritable, Text` → nothing (counter `DOCS`) | – |
| 2 | `IndexJob` | Summarization: inverted index, Filtering, Distributed Cache, MultipleOutputs | `LongWritable, Text` → `Text term, PostingWritable` | `Text term, Iterable<PostingWritable>` → `Text term, Text "df TAB postings"` (main) or the side files `corpus_stop`, `rare` |
| 3 | `PhraseJob` | Filtering + reduce-side positional join per document | index line (`LongWritable, Text`) → `Text doc_id, Text "k|p,p,.."` | `Text doc_id, Iterable<Text>` → `Text doc_id, Text count` (ranked in `cleanup`) |

Driver flow: Job 1 → reads counter `DOCS` → `conf.setLong("q2.N", N)` → Job 2 → Job 3 (only when `-Dphrase=...` is given).
`-Dq2.only.phrase=true` re-runs just Job 3 on an existing index, so other phrases cost one small job.

## Decisions to state in the report

* **Tokenising**: lower-case, split on every non-alphanumeric character, keep length ≥ 2, drop stop-words (Distributed Cache file).
* **Positions** (`-Dq2.positions=filtered|original`): by default positions number only the tokens that are *kept*
  (title first, then body, starting at 0) – "title and body form one token stream" is read as the stream of indexed tokens.
  With `original`, dropped short tokens and stop-words still occupy a position. The two differ only for a phrase whose
  words have a stop-word between them (planted document `big the data analytics` in the test data).
  Check the dataset README for the rule your teacher intends; switching is one `-D` option.
* **tf** = 3 × (title occurrences) + (body occurrences). **tf-idf** = tf × log10(N / df), exact decimal expansion rounded HALF_UP to 4 places;
  postings sorted by that rounded value descending, then doc_id ascending.
* **corpus_stop** is tested first (`df > 0.4 N`), then **rare** (`df == 1`); neither is in the main index.
* A line with only `doc_id TAB title` is a valid document with an empty body; lines with < 2 fields or an empty doc_id are
  counted in `MALFORMED_LINES`; stray extra tabs are merged into the body (`EXTRA_FIELDS_MERGED`).
* Phrase search only sees the main index, so a phrase containing a stop-word, a corpus_stop term or a rare term returns
  nothing; the driver prints a warning naming the word.
* Phrase ranking: occurrences descending, then doc_id ascending; overlapping occurrences all count.
  Documents that contain every word but never adjacent go to `all_words_not_adjacent-r-*` (evidence for the planted case).

## Delta (gap) encoding

`PostingWritable.write` stores `[3, 10, 25]` as the VInts `3, 7, 15`. A VInt takes 1 byte up to 127 and 2 bytes above,
so documents with positions beyond 127 shrink. Run `./run_q2.sh` – it runs the job twice (`q2.delta=true|false`),
proves the outputs identical and prints MAP_OUTPUT_BYTES / REDUCE_SHUFFLE_BYTES for both (table in `results_q2/shuffle_bytes.txt`).
In a synthetic test with ~900-token documents the map output fell from 1,898,450 to 1,706,932 bytes (about 10 %).
On short documents (all positions < 128) there is no difference – say so if your numbers look equal.

## Is a Combiner applicable in Job 2?  No (answer with reasons)

1. A document is one input line, so the mapper already merges all occurrences of a term in that document into one
   posting (in-mapper combining). Every `(term, doc)` pair therefore leaves the mapper exactly once; a Combiner would only
   ever see groups with distinct documents and has nothing to merge, so it cannot reduce records or bytes.
2. What the reducer computes – df, tf-idf (needs the global N and df), the corpus_stop / rare decision and the final
   sort – needs *all* postings of the term. None of it can be produced from partial results.
3. The only associative merge available (concatenating postings into a list) needs a different map-output type
   (a posting-list Writable), saves no bytes because every posting carries different doc ids and positions, and just moves
   the sorting work. The Combiner contract (output type = map-output type, result independent of how often it runs) would be
   kept only by that list type, and it brings no benefit.
(Contrast with Q1, where `(count, sum, sumSq, min, max)` merges associatively and shrinks millions of records to hundreds.)

## Planted cases and where the evidence is

| Case | Evidence |
|---|---|
| `big data big data analytics` | phrase `big data analytics` → 1 occurrence, `big data` → 2 occurrences for that document |
| term only in a title | term row with `tf` = 3 (title boost) and no body occurrence (e.g. a df = 1 term lands in `rare`) |
| empty body | counter `EMPTY_BODY_DOCS`; the document still counts in N and its title tokens are indexed |
| words present but never adjacent | counter `PHRASE_DOCS_ALL_WORDS_NOT_ADJACENT` and the file `all_words_not_adjacent-r-00000` |

## Reproduce

```
python3 tools/q2_gen.py testdata/q2                    # seeded small corpus with the planted cases
./run_q2.sh testdata/q2                                # or the real q2 folder (corpus/ + stopwords.txt)
python3 tools/q2_verify.py <q2 folder>/corpus <q2 folder>/stopwords.txt results_q2/delta --phrase "big data analytics"
```

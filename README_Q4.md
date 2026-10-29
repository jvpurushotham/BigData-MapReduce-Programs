# Q4 – Clickstream sessionization with secondary sort (notes for the report and the viva)

## Pipeline (one driver: `ca2.q4.Q4Driver`)

| Job | Class | Pattern | Map in → out | Reduce in → out |
|---|---|---|---|---|
| 1 | `SessionJob` | **Secondary sort** (composite key, Partitioner, SortComparator, GroupingComparator), MultipleOutputs | `LongWritable, Text` line → `UserTsKey(user, ts, url), NullWritable` | `UserTsKey, Iterable<NullWritable>` (one call per user, time-ordered stream) → `NullWritable, Text` session line; `bots/…`, `funnel_input/…` |
| 2 | `FunnelJob` | **Summarization** (+ Combiner) | `"date\tstage"` → `Text date` (and `"ALL"`), `FunnelWritable(5 longs)` | `Text, Iterable<FunnelWritable>` → `NullWritable, Text` report row |

Output: `<out>/session_job/sessions/part-r-*`, `<out>/session_job/bots/part-r-*`, `<out>/session_job/funnel_input/part-r-*`, `<out>/funnel/part-r-00000`.

## The four mandatory secondary-sort pieces
* `UserTsKey` – `WritableComparable` (user_id, event_ts, url). `url` is only a **tie-breaker** for identical timestamps (see below).
* `UserPartitioner` – `hash(user_id) mod reducers` – **user_id only**, so all events of a user meet in one reducer, whatever the timestamp.
* `UserTsSortComparator` – orders the shuffle by (user_id, event_ts[, url]).
* `UserGroupingComparator` – compares **user_id only** → one `reduce()` call per user; Hadoop refreshes the key object for every value, so `key.getTs()` inside the value loop is the timestamp of the current event.

## Session rules (reducer state machine, streaming)
* New session when `gap > 30 min` **strictly** (exactly 30:00.000 stays in the session; 30:00.001 splits) **or** the IST calendar date changes (IST = fixed +05:30, day = `floor((ts+19 800 000)/86 400 000)`). Counters: `SPLIT_BY_GAP`, `SPLIT_BY_MIDNIGHT`, `GAP_EXACTLY_30_MIN_KEPT`.
* A session that crosses midnight is therefore cut at the date change into two sessions (the second starts at the first event of the new date).
* `session_seq` counts **all** sessions of the user (bots included), starting at 1.
* start/end are ISO-8601 with milliseconds and `+05:30`; `duration_sec` is exact seconds with 3 decimals; `distinct_urls` counts the raw url strings.
* **Bot** if `events > 500` **or** (events ≥ 2 and `(end − start) < 1000 ms × (events − 1)`, i.e. average gap < 1 s). Bot sessions go **only** to `bots/` (with a reason column) and are excluded from the funnel and from `sessions/`. A one-event session has no gap and is not a bot. Thresholds are `-Dq4.bot.events`, `-Dq4.bot.avggap.ms`, `-Dq4.gap.minutes` (viva-friendly).
* Identical timestamps: the shuffle is not stable, so without a tie-breaker the order of those events (entry/exit url, funnel) would be random. The key therefore also sorts by `url` for exact ties – still sorted on (user_id, event_ts) first. Counter `EVENTS_SAME_TS_AS_PREVIOUS`.
* Exact duplicate lines are **not** removed (they are separate page views). Malformed lines (fewer than 5 fields, bad user/timestamp/url) are counted and skipped.

## Funnel (Job 1 reducer decides the stage, Job 2 aggregates)
Greedy in-order matching on the path of the url (query string / fragment removed): `/home` → `/product/*` → `/cart` → `/checkout/success`; stage *k* is reached only after stage *k−1*. A session that visits `/cart` before any `/product` does not get product→cart credit (counter `CART_BEFORE_PRODUCT_SESSIONS`); it can still convert later if `/product` and then `/cart` follow. Bot sessions are excluded.
Job 2 columns: `date, sessions, reached_home, reached_product, reached_cart, reached_success, home→product %, product→cart %, cart→success %, home→success %` (percentages HALF_UP with 2 decimals, `NA` when the denominator is 0); the pseudo date **`ALL`** is the total. The Combiner of Job 2 is valid because the five counters are plain sums (associative and commutative).
Reducer of Job 1: **no Combiner** – the reducer needs every event in time order; nothing can be merged earlier.

## Memory proof (the 300 000-event user)
The reducer holds only the *open session*: counters, two timestamps, entry/exit url, the stage and a `HashSet` of the **distinct urls of the open session**, cleared when the session closes. It never keeps events. The set is bounded by the number of distinct urls, not by the number of events (this is the only collection; it is needed for `distinct_urls`).
`run_q4.sh` runs the main job with `-D mapreduce.reduce.memory.mb=512 -D mapreduce.reduce.java.opts=-Xmx410m` and prints `COMMITTED_HEAP_BYTES`, `GC_TIME_MILLIS`, `SPILLED_RECORDS`. The shuffle itself spills to disk, so no reducer ever holds a user in memory. Skew: one reducer gets the huge user (partition by user_id is mandatory) – mention it as the trade-off.

## Planted cases and where the evidence is
`python3 tools/q4_verify.py <clicks dir> results_q4/main` prints the examples (user, timestamps) of every case, and the sessions you then look up in `results_q4/main/session_job/sessions` or `bots`:
| Case | Evidence |
|---|---|
| two events exactly 30:00 apart → same session | `exact30` block + counter `GAP_EXACTLY_30_MIN_KEPT` |
| session crossing midnight IST | `midnight` block + counter `SPLIT_BY_MIDNIGHT` |
| one user's events in reverse order across files | `reverse` block (output sessions are still in time order) |
| two identical timestamps | `same_ts` block + counter `EVENTS_SAME_TS_AS_PREVIOUS` |
| /cart before /product | `cart_first` block + counter `CART_BEFORE_PRODUCT_SESSIONS` |

## Reproduce
```
mvn -q clean package
./run_q4.sh ~/BigData2CA2-Answers/ca2-mapreduce/data/q4     # folder that contains the *.log files
python3 tools/q4_verify.py data/q4 results_q4/main | tee q4_verify.txt
```

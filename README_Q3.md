# Q3 – Net revenue Top-N per category after returns (notes for the report and the viva)

## Pipeline (one driver: `ca2.q3.Q3Driver`)

| Job | Class | Pattern | Map in → out | Reduce in → out |
|---|---|---|---|---|
| 1 | `JoinJob` | **Reduce-side join** (MultipleInputs, tagged values), MultipleOutputs | orders: `LongWritable,Text → Text "order_id\|line_no", Text "O\t…"`; returns: `… → Text "order_id\|line_no", Text "R\t…"` | `Text, Iterable<Text>` → `NullWritable, Text` net line; orphans to file `orphan_returns` |
| 2 | `RevenueJob` | **Replicated join** (`products.csv` in Distributed Cache, loaded in `Mapper.setup()`), Combiner | net line → `Text "category\tmonth\tproduct_id", NetWritable(units, revenue_E4)` | `Text, Iterable<NetWritable>` → `Text, Text "revenue_E4\tunits"` |
| 3 | `TopNJob` | **Top-N filtering**, custom Partitioner, custom Writables | Job 2 row → (cleanup) `GroupKey(category, month), EntryWritable(product, revenue, units)` | `GroupKey, Iterable<EntryWritable>` → `NullWritable, Text "category month rank product_id name revenue units"` |

Driver flow: Job 1 → moves `orphan_returns-r-*` into `<out>/orphan_returns` → Job 2 → Job 3 → deletes `net_lines` and `revenue`
(intermediate directories) unless `-Dq3.keep.intermediate=true` (`run_q3.sh` keeps them: they are the evidence). Non-zero exit on any failure.

## Decisions to state in the report

* **Join key** `(order_id, line_no)`; `line_no` is parsed as an integer, so `01` and `1` are the same line.
* **CANCELLED** order lines are excluded (status compared case-insensitively). Returns that point at a cancelled line are *not* orphans
  (the line exists); they are dropped with it and counted in `RETURNS_ON_CANCELLED_LINES`.
* **net_qty = qty − Σ qty_returned**, never below 0: when the returns exceed the ordered quantity `RETURN_EXCEEDS_ORDER` is incremented and net_qty is 0.
* **Orphan returns** (no order line with that key at all) are written by MultipleOutputs to `orphan_returns`.
* **Two separate returns of one line** are both subtracted. The same `return_id` repeated for a line counts once (`DUPLICATE_RETURN`).
  An exactly repeated order line collapses to one (`DUPLICATE_ORDER_LINE`); differing copies keep the smallest record (`CONFLICTING_ORDER_LINE`).
* **Month = month of the order timestamp**, never of the return: an order on 31 Jan returned on 2 Feb stays in January.
* **Money** is carried as an exact `long` in 1/10 000 units (E4) so sums do not depend on the order of addition; printed with at least 2 decimals.
* **UNMAPPED**: a product that is not in `products.csv` gets category `UNMAPPED` (`UNMAPPED_LINES`); its name prints as `(unknown)`.
* Lines that are fully returned (net_qty 0) stay in the aggregation with revenue 0.
* `PRODUCTS_LOADED` is counted once per task that loads the cache file, so it is (rows in products.csv) × (number of tasks).

## Top-N (3c): the strict total order

`EntryWritable.RANK`: revenue descending, then units descending, then product_id ascending.
Two entries compare equal only when all three fields are equal, and a product appears once per group, so **two products with identical
revenue and units are both kept in the set and ordered by product_id** – nothing is silently overwritten (a `TreeMap<revenue, product>`
would overwrite one of them). The mapper keeps one `TreeSet` of at most N entries per group (`pollLast()` after every insert), so mapper
memory is O(groups × N); survivors are emitted in `cleanup()`. `CategoryPartitioner` sends all groups of a category to one reducer
(`hash(category) mod reducers`); the driver prints how many reducers received data (≥ 3 required, default 4 reducers).

Experiments (`run_q3.sh`): Job 2 with / without Combiner (sums are associative → identical output, fewer shuffled bytes); Job 3 bounded /
unbounded mappers (identical output, `MAP_OUTPUT_RECORDS` shows what the bounded structure saves).

## Combiners
* Job 1: **not applicable** – the reducer needs the order record and its returns of the same key together; nothing can be merged earlier.
* Job 2: **applicable** – `(units, revenue)` are plain sums (associative and commutative).
* Job 3: the bounded `TreeSet` in the mapper does the same job as a Combiner (a partial Top-N is mergeable, because every row is already a complete total).

## 3(d): why Top-N cannot be applied inside Job 2's mappers

Job 2's mapper sees only the lines of its own input split, so it only knows a *partial* revenue per product; the product's revenue is
the sum of the partial revenues of every mapper (and returns have already reduced them). A product can be small in every single split yet
large in total – or large in one split and ranked 6th or lower in another. If every mapper kept only its own top 5, the rest would be
discarded before the sum exists, and the reducer would add up incomplete numbers (a wrong revenue) or never see the product at all (a wrong
Top-5). Top-N is only valid on **complete** totals – exactly what Job 3 receives (one row per category, month, product).
Counter-example from your own data: `python3 tools/q3_counterexample.py results_q3/main/net_lines <products.csv>` (the part files of Job 1
play the mappers of Job 2) prints a product, its partial revenue and rank inside each mapper, and the revenue the reducer would see.

## Planted cases and where the evidence is (`results_q3/planted_evidence.txt`)

| Case | Evidence |
|---|---|
| two products with identical revenue and units | evidence (1) lists them; both appear in the Top-N output ordered by product_id (or only the smaller id if they straddle rank N) |
| one line returned in two returns | evidence (2); counter `LINES_WITH_MULTIPLE_RETURNS`; both quantities are in `returned` of the net line |
| return exceeding the ordered quantity | evidence (3); counter `RETURN_EXCEEDS_ORDER`; net 0 |
| order on 31 Jan returned on 2 Feb | evidence (4): the net line carries month `2026-01` |
| product missing from products.csv | evidence (5); category `UNMAPPED`; counter `UNMAPPED_LINES` |

## Reproduce

```
python3 tools/q3_gen.py testdata_small/q3                       # seeded small dataset with the planted cases
./run_q3.sh testdata_small/q3                                   # or the real q3 folder
python3 tools/q3_verify.py <q3 folder> results_q3/main          # independent re-implementation of the specification
```

# Q5 – Three-way payment reconciliation with reduce-side joins (notes for the report and the viva)

## Pipeline (one driver: `ca2.q5.Q5Driver`) – three chained jobs

| Job | Class | Pattern | Map in → out | Reduce in → out |
|---|---|---|---|---|
| A | `TxnSettlementJob` | **Reduce-side join, one-to-many, on `txn_id`**; tagged key + secondary sort; MultipleOutputs | transactions: `TaggedKey(txn_id,0), Text`; settlements: `TaggedKey(txn_id,1), Text` | `TaggedKey, Iterable<Text>` → one **enriched transaction** per txn (adds `settled_sum`, `settlement_count`); `ORPHAN_SETTLEMENT/…` |
| B | `OrderJoinJob` | **Reduce-side join, one-to-many, on `order_id`** + **replicated join** (`fx_rates.csv` in the Distributed Cache) + classification; MultipleOutputs | orders: `TaggedKey(order_id,0), Text`; enriched txns: `TaggedKey(order_id,1), Text` | `TaggedKey, Iterable<Text>` → one line per order in its category directory; `ORPHAN_TXN/…` |
| C | `SummaryJob` | **Summarization** (+ Combiner) | feed line → `Text category, SummaryWritable(count, 3 sums)` | `Text, Iterable<SummaryWritable>` → `category, count, order_amount_inr, net_paid_inr, settled_inr` |

Output: `<out>/UNPAID`, `UNDERPAID`, `OVERPAID`, `PAID_NOT_SETTLED`, `SETTLEMENT_MISMATCH`, `MATCHED`, `ORPHAN_TXN`, `ORPHAN_SETTLEMENT` (one MultipleOutputs directory each, empty ones are created) and `<out>/summary/part-r-00000`; `stage_a`, `stage_b` are kept as evidence.

## Why this order of joins (txn_id first, then order_id)
The two relations hang off different keys: a settlement knows only its `txn_id`, a transaction knows its `order_id`. Joining **settlements to transactions first** collapses the many settlements of a transaction into two numbers (sum, count) on the transaction record; the second join (order ⋈ transactions on `order_id`) then sees one small record per transaction and computes everything per order. The opposite order (orders ⋈ transactions first, then ⋈ settlements on `txn_id`) would have to re-key by `txn_id` for the settlements and then re-aggregate back to `order_id` – an extra shuffle of the widest records, and the orphan settlements (no transaction) would still need the txn_id join. Each join also has exactly one "one" side per key: a transaction per `txn_id`, an order per `order_id`.

## Mandatory constraint: tagged composite key, secondary sort, no buffering of the many side
* `TaggedKey(id, tag)`: tag **0 = the "one" side** (transaction in A, order in B), tag **1 = the "many" side** (settlements in A, transactions in B).
* `IdPartitioner` – partitions on `id` only; `TagSortComparator` – sorts by (id, tag); `IdGroupingComparator` – groups by `id` only → one `reduce()` call per id and the single "one" record always comes **first**.
* The reducer holds only that one record plus **running aggregates** (count, sums, flags). The many side is streamed once and never put in a list. A group that starts with a "many" record (no "one" record) is an orphan and is written immediately.

## Why a replicated join is unsuitable for orders
Orders (200 005 rows, and in real life unbounded) are the *large* side and they are needed for *every* transaction: a map-side join would have to hold all orders in every mapper's memory (or Distributed Cache copy per task) – memory grows with the orders table, which is exactly what "replicated" cannot afford. Only `fx_rates.csv` (a few thousand rows) is small enough, so it **is** replicated (loaded into a `TreeMap` per currency in `setup()` of the Job B reducer). A replicated join also cannot find *orphans* on the large side (a transaction without an order) without a second pass.

## Rules and decisions (state them in the report)
* **Net paid** = Σ SUCCESS − Σ |REFUNDED| (FAILED ignored), each transaction converted at the rate of its **order date** (`order_ts`, date part) and its own currency. `order_amount` is converted the same way and everything is compared in **INR**. Money is an exact `long` in 1/10 000 INR (E4), `HALF_UP` on the exact decimal product.
* **Classification (first rule that fits):** `UNPAID` (net = 0, includes no transaction, only FAILED, fully refunded) → `UNDERPAID`/`OVERPAID` (|net − order| **> 1.00**; exactly 1.00 is within tolerance) → `PAID_NOT_SETTLED` (any SUCCESS transaction with no settlement) → `SETTLEMENT_MISMATCH` (|Σ settled − net| > 1.00) → `MATCHED`.
* **Σ settled** = Σ settlements of SUCCESS transactions − |Σ settlements of REFUNDED transactions| (`-Dq5.refund.settlements=ignore` ignores them); settlements on FAILED transactions are ignored (counter `SETTLEMENTS_ON_FAILED_TXN`). A transaction settled twice therefore counts twice → `SETTLEMENT_MISMATCH` (counter `TXNS_SETTLED_MORE_THAN_ONCE`).
* **FX lookup:** exact date; else the latest earlier date of that currency (`FX_FALLBACK_EARLIER_DATE`); else the earliest later date; a currency that is not in the file at all stops the job (no invented rate). `INR` is 1 unless the file says otherwise.
* **Orphans:** `ORPHAN_TXN` = transaction whose `order_id` is not in orders (value converted at the transaction's own date); `ORPHAN_SETTLEMENT` = settlement whose `txn_id` is not in the transactions file.
* **Duplicates:** conflicting copies of the same `txn_id` / `order_id` keep the lexicographically smallest record (`DUPLICATE_TXN_ID`, `DUPLICATE_ORDER_ID`); settlements are never de-duplicated (a settlement twice is a planted case). Malformed rows are counted (`*_BAD_*`) and skipped; headers are skipped by name.
* **Summary:** per category `count`, Σ order amount INR, Σ net paid INR, Σ settled INR (orphans: transaction value in the net column, settlement amount in the settled column). A category with no records has no row.

## Combiners
Jobs A and B: **not applicable** – the reducer needs the "one" record and the individual "many" records of the same key (status-dependent signs, flags). Job C: applicable (plain sums).

## Planted cases and where the evidence is
`python3 tools/q5_verify.py <q5 folder> results_q5/main` prints the examples (order/txn ids) and the category in which each lands:
| Case | Expected result |
|---|---|
| order with FAILED then SUCCESS | counter `ORDERS_FAILED_AND_SUCCESS`; the FAILED attempt does not count; usually MATCHED |
| SUCCESS followed by a partial REFUND | counter `ORDERS_WITH_REFUND`; net = success − refund → UNDERPAID (unless the order amount equals the net) |
| USD order | counters `ORDERS_NON_INR`, `FX_CONVERSIONS`; compared in INR at the order-date rate |
| transaction settled twice | `TXNS_SETTLED_MORE_THAN_ONCE`; SETTLEMENT_MISMATCH |
| transaction for an unknown order | `ORPHAN_TXN` directory |
| settlement for an unknown transaction | `ORPHAN_SETTLEMENT` directory |
| order with no transaction at all | `ORDERS_WITHOUT_TXN`; UNPAID |

## Reproduce
```
mvn -q clean package
./run_q5.sh data/q5                                   # folder with orders*/gateway_txns*/bank_settlements*/fx_rates* csv files
python3 tools/q5_verify.py data/q5 results_q5/main | tee q5_verify.txt
```

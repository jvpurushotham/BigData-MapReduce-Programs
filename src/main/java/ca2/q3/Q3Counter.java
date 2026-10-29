package ca2.q3;

/** Custom counters for Q3 (one per dirty / special record category). */
public enum Q3Counter {
    // ---- Job 1, orders side ----
    ORDERS_READ,
    ORDERS_HEADER_SKIPPED,
    ORDERS_BLANK_LINES,
    ORDERS_BAD_FIELD_COUNT,
    ORDERS_BAD_LINE_NO,
    ORDERS_BAD_TIMESTAMP,
    ORDERS_BAD_QTY,
    ORDERS_BAD_PRICE,
    ORDERS_BAD_PRODUCT,
    PRICE_ROUNDED_TO_4_DECIMALS,

    // ---- Job 1, returns side ----
    RETURNS_READ,
    RETURNS_HEADER_SKIPPED,
    RETURNS_BLANK_LINES,
    RETURNS_BAD_FIELD_COUNT,
    RETURNS_BAD_LINE_NO,
    RETURNS_BAD_QTY,
    RETURNS_BAD_TIMESTAMP,

    // ---- Job 1, reducer ----
    ORDER_LINES_JOINED,        // distinct (order_id, line_no) with at least one valid order record
    DUPLICATE_ORDER_LINE,      // extra identical copies of an order line
    CONFLICTING_ORDER_LINE,    // extra copies that differ (smallest record is kept)
    DUPLICATE_RETURN,          // same return_id seen again for the same order line
    CANCELLED_LINES,           // excluded
    RETURNS_ON_CANCELLED_LINES,
    LINES_WITH_MULTIPLE_RETURNS,
    RETURN_EXCEEDS_ORDER,      // sum of returns > ordered qty, net_qty capped (at 0)
    ORPHAN_RETURNS,            // match no order line -> orphan_returns
    NET_LINES_WRITTEN,
    FULLY_RETURNED_LINES,

    // ---- Job 2 ----
    PRODUCTS_LOADED,
    DUPLICATE_PRODUCT_ROWS,
    PRODUCT_ROWS_MALFORMED,
    UNMAPPED_LINES,            // line whose product is not in products.csv -> category UNMAPPED
    GROUPS_WRITTEN,            // distinct (category, month, product)

    // ---- Job 3 ----
    TOPN_INPUT_GROUP_ROWS,
    TOPN_MAPPER_EVICTIONS,     // entries pushed out of a mapper's bounded structure
    TOPN_MAPPER_ENTRIES_EMITTED,
    TOPN_GROUPS,               // (category, month) groups ranked
    TOPN_ROWS_WRITTEN
}

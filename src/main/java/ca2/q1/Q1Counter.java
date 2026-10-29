package ca2.q1;

/** Custom counters for Q1. Every rejection category has its own counter (question 1a). */
public enum Q1Counter {
    RECORDS_READ,
    HEADER_SKIPPED,
    REJECTED_FIELD_COUNT,
    REJECTED_EMPTY_FIELD,
    REJECTED_BAD_TIMESTAMP,
    REJECTED_BAD_KWH,
    REJECTED_NEGATIVE_KWH,
    DUPLICATE_READING,        // extra copies removed (copies - 1 per meter+timestamp)
    UNIQUE_READINGS,          // clean readings that survive
    NEGATIVE_VARIANCE_CLAMPED,// floating-point guard fired in the stats reducer
    MONTHS_WITHOUT_STATS,     // reading whose meter-month is missing from the side file
    OUTLIER_READINGS
}

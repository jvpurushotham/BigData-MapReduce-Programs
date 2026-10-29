package ca2.q2;

/** Custom counters for Q2 (every dirty / special record category has its own counter). */
public enum Q2Counter {
    // ---- document level (Job 1 and Job 2 use the same parser, so these agree) ----
    DOCS,                    // valid documents = N
    BLANK_LINES,
    HEADER_SKIPPED,
    MALFORMED_LINES,         // fewer than 2 tab-separated fields, or empty doc_id
    EXTRA_FIELDS_MERGED,     // more than 3 fields: the extra tab-separated parts are appended to the body
    EMPTY_BODY_DOCS,
    EMPTY_TITLE_DOCS,
    DOCS_WITHOUT_INDEXABLE_TOKENS,

    // ---- token level (Job 2 mapper) ----
    TOKENS_TOTAL,
    TOKENS_DROPPED_SHORT,
    TOKENS_DROPPED_STOPWORD,
    TOKENS_KEPT,
    TERM_IN_TITLE_ONLY_OF_DOC, // (term, doc) pairs whose term occurs in the title and never in the body
    STOPWORDS_LOADED,

    // ---- term level (Job 2 reducer) ----
    TERMS_INDEXED,
    TERMS_CORPUS_STOP,       // df > 0.4 N          -> corpus_stop
    TERMS_RARE,              // df == 1             -> rare
    POSTINGS_WRITTEN,

    // ---- phrase search (Job 3) ----
    PHRASE_INDEX_LINES_READ,
    PHRASE_DOCS_WITH_ALL_WORDS,
    PHRASE_DOCS_MATCHED,
    PHRASE_DOCS_ALL_WORDS_NOT_ADJACENT,
    PHRASE_OCCURRENCES
}

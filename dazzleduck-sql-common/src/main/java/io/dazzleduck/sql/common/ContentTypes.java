package io.dazzleduck.sql.common;

/** Canonical content types for query results, shared across modules and reusable by consumers. */
public final class ContentTypes {

    public static final String APPLICATION_JSON = "application/json";
    public static final String APPLICATION_ARROW = "application/vnd.apache.arrow.stream";
    public static final String TEXT_TSV = "text/tab-separated-values";
    public static final String TEXT_TSV_UTF8 = "text/tab-separated-values; charset=utf-8";

    /** JSON Lines / newline-delimited JSON: one JSON object per row, per line. */
    public static final String APPLICATION_JSONL = "application/jsonl";
    public static final String APPLICATION_JSONL_UTF8 = "application/jsonl; charset=utf-8";
    /** De-facto NDJSON media type; treated as an alias for {@link #APPLICATION_JSONL}. */
    public static final String APPLICATION_X_NDJSON = "application/x-ndjson";

    private ContentTypes() {
    }
}

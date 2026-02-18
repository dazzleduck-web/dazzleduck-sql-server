package io.dazzleduck.sql.common;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class Headers {

    public static final int DEFAULT_ARROW_FETCH_SIZE = 10000;
    public static long DEFAULT_SPLIT_SIZE = 1024 * 1024 * 1024;
    public static final Map<Class<?>, Function<String, Object>> EXTRACTOR = Map.of(
            Integer.class, Integer::parseInt,
            Long.class, Long::parseLong,
            Boolean.class, Boolean::parseBoolean,
            String.class, a -> a
    );
    public static final String HEADER_FETCH_SIZE = "x-dd-fetch-size";
    public static final String HEADER_DATABASE = "database";
    public static final String HEADER_SCHEMA = "schema";
    public static final String HEADER_TABLE = "table";
    public static final String HEADER_PATH = "path";

    public static final String QUERY_PARAMETER_INGESTION_QUEUE = "ingestion_queue";
    public static final String HEADER_FUNCTION = "function";
    public static final String HEADER_FILTER = "filter";
    public static final String HEADER_ACCESS_TYPE = "access-type";
    public static final String HEADER_SPLIT_SIZE = "x-dd-split-size";
    public static final String HEADER_DATA_PARTITION = "x-dd-partition";
    public static final String HEADER_DATA_FORMAT = "x-dd-format";
    public static final String HEADER_PRODUCER_ID = "x-dd-producer-id";
    public static final String HEADER_QUERY_ID = "x-dd-query-id";
    public static final String HEADER_PRODUCER_BATCH_ID = "x-dd-producer-batch-id";
    public static final String HEADER_SORT_ORDER = "x-dd-sort-order";
    public static final String HEADER_DATA_PROJECT = "x-dd-project";

    public static final String HEADER_DATA_LIMIT = "x-dd-limit";
    public static final String HEADER_APP_DATA_TRANSFORMATION = "x-dd-udf-transformation";
    public static final String HEADER_ARROW_COMPRESSION = "x-dd-arrow-compression";

    public static final Set<String> SUPPORTED_HEADERS = Set.of(HEADER_FETCH_SIZE, HEADER_DATABASE, HEADER_SCHEMA, HEADER_SPLIT_SIZE,
            HEADER_DATA_PARTITION, HEADER_DATA_FORMAT, HEADER_PRODUCER_ID, HEADER_PRODUCER_BATCH_ID, HEADER_SORT_ORDER,
            HEADER_DATA_PROJECT, HEADER_APP_DATA_TRANSFORMATION, HEADER_PATH, HEADER_TABLE, HEADER_FUNCTION, HEADER_FILTER, HEADER_ACCESS_TYPE,
            HEADER_ARROW_COMPRESSION);

}

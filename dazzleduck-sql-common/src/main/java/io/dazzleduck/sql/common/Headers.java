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
    public static final String HEADER_FETCH_SIZE = "fetch_size";
    public static final String HEADER_DATABASE = "database";
    public static final String HEADER_SCHEMA = "schema";
    public static final String HEADER_TABLE = "table";
    public static final String HEADER_PATH = "path";
    public static final String HEADER_FUNCTION = "function";
    public static final String HEADER_FILTER = "filter";
    public static final String HEADER_SPLIT_SIZE = "split_size";
    public static final String HEADER_DATA_PARTITION = "partition";
    public static final String HEADER_DATA_FORMAT = "format";
    public static final String HEADER_PRODUCER_ID = "producer_id";
    public static final String HEADER_QUERY_ID = "query_id";
    public static final String HEADER_PRODUCER_BATCH_ID = "producer_batch_id";
    public static final String HEADER_SORT_ORDER = "sort_order";
    public static final String HEADER_DATA_TRANSFORMATION ="transformation";
    public static final String HEADER_APP_DATA_TRANSFORMATION ="udf_transformation";

    public static final Set<String> SUPPORTED_HEADERS = Set.of(HEADER_FETCH_SIZE, HEADER_DATABASE, HEADER_SCHEMA,  HEADER_SPLIT_SIZE,
            HEADER_DATA_PARTITION, HEADER_DATA_FORMAT, HEADER_PRODUCER_ID, HEADER_PRODUCER_BATCH_ID, HEADER_SORT_ORDER,
            HEADER_DATA_TRANSFORMATION, HEADER_APP_DATA_TRANSFORMATION, HEADER_PATH, HEADER_TABLE, HEADER_FUNCTION, HEADER_FILTER);

}

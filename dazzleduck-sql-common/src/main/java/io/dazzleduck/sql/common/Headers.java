package io.dazzleduck.sql.common;

import java.util.Map;
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
    public static final String HEADER_SPLIT_SIZE = "split_size";
    public static final String HEADER_PARALLELIZE = "parallelize";
    public static final String HEADER_DATA_PARTITION = "partition";
    public static final String HEADER_DATA_FORMAT = "format";

    public static final String HEADER_DATA_TRANSFORMATION ="transformation";
    public static final String HEADER_APP_DATA_TRANSFORMATION ="app_transformation";

}

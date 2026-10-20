package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.util.MutableClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DuckLakeIngestionHandlerTest {

    @TempDir
    Path tempDir;

    static final String CATALOG = "test_factory_lake";
    static final String SCHEMA = "main";
    static final String TABLE = "events";
    static final String QUEUE_ID = "events";

    @BeforeEach
    void setUp() throws Exception {
        Files.createDirectories(tempDir.resolve("data"));
        ConnectionPool.execute("INSTALL arrow FROM community");
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "LOAD arrow",
                    "ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s')".formatted(
                            tempDir.resolve("catalog"), CATALOG, tempDir.resolve("data")),
                    "CREATE TABLE %s.%s.%s (id BIGINT, msg VARCHAR)".formatted(CATALOG, SCHEMA, TABLE)
            });
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "DETACH " + CATALOG);
        }
    }

    private QueueIdToTableMapping mapping(String queueId, String transformation) {
        return new QueueIdToTableMapping(queueId, CATALOG, SCHEMA, TABLE, Map.of(), transformation);
    }

    // -----------------------------------------------------------------------
    // getTargetPath
    // -----------------------------------------------------------------------

    @Test
    void shouldResolveTargetPathFromCatalogOnConstruction() {
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, null)));
        String path = factory.getTargetPath(QUEUE_ID);
        assertNotNull(path, "Expected resolved path, got null");
        assertFalse(path.isBlank());
        assertTrue(path.contains(tempDir.resolve("data").toString()),
                "Expected path under data dir, got: " + path);
    }

    @Test
    void shouldReturnNullTargetPathForUnknownQueueId() {
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, null)));
        assertNull(factory.getTargetPath("unknown-queue"));
    }

    /**
     * Regression: a queue added via {@link DuckLakeIngestionHandler#updateMappings} (as the dynamic
     * handler does) has no cached state yet — its path must still be derived lazily on first access.
     * This was broken when the state-key lookup consulted the state cache instead of the mappings.
     */
    @Test
    void shouldResolveTargetPathForQueueAddedViaUpdateMappings() {
        var factory = new DuckLakeIngestionHandler(Map.of()); // empty: no eagerly-built state
        assertNull(factory.getTargetPath(QUEUE_ID), "unknown before it is registered");

        factory.updateMappings(Map.of(QUEUE_ID, mapping(QUEUE_ID, null)));

        String path = factory.getTargetPath(QUEUE_ID);
        assertNotNull(path, "path must be derived lazily for a queue added via updateMappings");
        assertTrue(path.contains(tempDir.resolve("data").toString()), "got: " + path);
    }

    // -----------------------------------------------------------------------
    // createPostIngestionTask — direct match
    // -----------------------------------------------------------------------

    @Test
    void shouldCreateTaskForDirectQueueIdMatch() {
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, null)));
        var result = new IngestionResult(QUEUE_ID, 1L, "app", Map.of(), 0L, List.of());
        PostIngestionTask task = factory.createPostIngestionTask(result);
        assertNotNull(task);
        assertInstanceOf(DuckLakePostIngestionTask.class, task);
    }

    // -----------------------------------------------------------------------
    // createPostIngestionTask — suffix fallback
    // -----------------------------------------------------------------------

    @Test
    void shouldCreateTaskUsingSuffixFallbackWhenQueueNameIsAPath() {
        // Mapping key is "events"; result queueName is a path whose last segment is "events"
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, null)));
        String pathQueueName = "/tmp/otel-output/" + QUEUE_ID;
        var result = new IngestionResult(pathQueueName, 1L, "app", Map.of(), 0L, List.of());
        PostIngestionTask task = factory.createPostIngestionTask(result);
        assertNotNull(task);
        assertInstanceOf(DuckLakePostIngestionTask.class, task);
    }

    @Test
    void shouldCreateTaskUsingSuffixFallbackWithBackslashPath() {
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, null)));
        String windowsPath = "C:\\output\\" + QUEUE_ID;
        var result = new IngestionResult(windowsPath, 1L, "app", Map.of(), 0L, List.of());
        PostIngestionTask task = factory.createPostIngestionTask(result);
        assertNotNull(task);
        assertInstanceOf(DuckLakePostIngestionTask.class, task);
    }

    // -----------------------------------------------------------------------
    // createPostIngestionTask — no mapping found → NOOP (not an error)
    // -----------------------------------------------------------------------

    @Test
    void shouldReturnNoopForUnknownQueueIdWithNoSuffixMatch() {
        // A unified handler may serve signals that have no DuckLake mapping (e.g. traces/metrics
        // in a config where only logs are registered in DuckLake). NOOP is the correct result.
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, null)));
        var result = new IngestionResult("completely-unknown", 1L, "app", Map.of(), 0L, List.of());
        PostIngestionTask task = factory.createPostIngestionTask(result);
        assertNotNull(task, "Should return NOOP task, not null");
        task.execute(); // must not throw
    }

    @Test
    void shouldReturnNoopWhenPathSuffixDoesNotMatchAnyMappingKey() {
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, null)));
        // last segment is "other", not "events"
        var result = new IngestionResult("/tmp/output/other", 1L, "app", Map.of(), 0L, List.of());
        PostIngestionTask task = factory.createPostIngestionTask(result);
        assertNotNull(task, "Should return NOOP task, not null");
        task.execute(); // must not throw
    }

    // -----------------------------------------------------------------------
    // getTransformation
    // -----------------------------------------------------------------------

    @Test
    void shouldReturnTransformationForKnownQueue() {
        String sql = "SELECT id, upper(msg) AS msg FROM __this";
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, sql)));
        assertEquals(sql, factory.getTransformation(QUEUE_ID));
    }

    @Test
    void shouldReturnNullTransformationWhenNotConfigured() {
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, null)));
        assertNull(factory.getTransformation(QUEUE_ID));
    }

    @Test
    void shouldReturnNullTransformationForUnknownQueue() {
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, null)));
        assertNull(factory.getTransformation("no-such-queue"));
    }

    @Test
    void shouldReturnTransformationViaSuffixFallback() {
        String sql = "SELECT * FROM __this WHERE id > 0";
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, sql)));
        assertEquals(sql, factory.getTransformation("/var/data/" + QUEUE_ID));
    }

    // -----------------------------------------------------------------------
    // Multiple mappings
    // -----------------------------------------------------------------------

    @Test
    void shouldSupportMultipleMappings() throws Exception {
        String table2 = "metrics";
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "CREATE TABLE %s.%s.%s (id BIGINT, val DOUBLE)".formatted(CATALOG, SCHEMA, table2));
        }

        var mappings = Map.of(
                "events", new QueueIdToTableMapping("events", CATALOG, SCHEMA, TABLE, Map.of(), null),
                "metrics", new QueueIdToTableMapping("metrics", CATALOG, SCHEMA, table2, Map.of(), null)
        );
        var factory = new DuckLakeIngestionHandler(mappings);

        assertNotNull(factory.getTargetPath("events"));
        assertNotNull(factory.getTargetPath("metrics"));
        assertNotEquals(factory.getTargetPath("events"), factory.getTargetPath("metrics"));

        assertInstanceOf(DuckLakePostIngestionTask.class,
                factory.createPostIngestionTask(new IngestionResult("events", 1L, "app", Map.of(), 0L, List.of())));
        assertInstanceOf(DuckLakePostIngestionTask.class,
                factory.createPostIngestionTask(new IngestionResult("metrics", 1L, "app", Map.of(), 0L, List.of())));
    }

    // -----------------------------------------------------------------------
    // getPartitionBy() - partitioned table
    // -----------------------------------------------------------------------

    @Test
    void shouldReturnPartitionColumnsForPartitionedTable() throws Exception {
        String partitionedTable = "partitioned_events";
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "CREATE TABLE %s.%s.%s (id BIGINT, value VARCHAR, date DATE, level VARCHAR)".formatted(CATALOG, SCHEMA, partitionedTable),
                    "ALTER TABLE %s.%s.%s SET PARTITIONED BY (date, level)".formatted(CATALOG, SCHEMA, partitionedTable)
            });
        }

        var mapping = new QueueIdToTableMapping(partitionedTable, CATALOG, SCHEMA, partitionedTable, Map.of(), null);
        var factory = new DuckLakeIngestionHandler(Map.of(partitionedTable, mapping));
        String[] partitionColumns = factory.getPartitionBy(partitionedTable);
        assertNotNull(partitionColumns, "Expected non-null partition columns array");
        assertEquals(2, partitionColumns.length, "Expected exactly 2 partition columns");
        assertEquals("date", partitionColumns[0], "Expected first partition column to be 'date'");
        assertEquals("level", partitionColumns[1], "Expected second partition column to be 'level'");
    }

    // -----------------------------------------------------------------------
    // getPartitionBy() - non-partitioned table
    // -----------------------------------------------------------------------

    @Test
    void shouldReturnEmptyArrayForNonPartitionedTable() throws Exception {
        String nonPartitionedTable = "non_partitioned_events";
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn,
                    "CREATE TABLE %s.%s.%s (id BIGINT, value VARCHAR)".formatted(CATALOG, SCHEMA, nonPartitionedTable));
        }

        var mapping = new QueueIdToTableMapping(nonPartitionedTable, CATALOG, SCHEMA, nonPartitionedTable, Map.of(), null);
        var factory = new DuckLakeIngestionHandler(Map.of(nonPartitionedTable, mapping));

        String[] partitionColumns = factory.getPartitionBy(nonPartitionedTable);
        assertNotNull(partitionColumns, "Expected non-null partition columns array");
        assertEquals(0, partitionColumns.length, "Expected empty array for non-partitioned table");
    }

    // -----------------------------------------------------------------------
    // Time-transform partition columns (year/month/day/hour) — PR #363
    //
    // DuckLake stores a partition column's `transform` as the transform name for time transforms
    // (year/month/day/hour) and "identity" for plain columns. A time transform cannot be named
    // directly in COPY's PARTITION_BY (which accepts only column names), so the handler resolves it
    // into a token (the transform name) plus a derived-column projection that must be added to the
    // COPY relation for the token to resolve.
    // -----------------------------------------------------------------------

    private DuckLakeIngestionHandler handlerForPartitionedTable(String table, String partitionBySpec,
                                                                String columnsDdl) throws SQLException {
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "CREATE TABLE %s.%s.%s (%s)".formatted(CATALOG, SCHEMA, table, columnsDdl),
                    "ALTER TABLE %s.%s.%s SET PARTITIONED BY (%s)".formatted(CATALOG, SCHEMA, table, partitionBySpec)
            });
        }
        var mapping = new QueueIdToTableMapping(table, CATALOG, SCHEMA, table, Map.of(), null);
        return new DuckLakeIngestionHandler(Map.of(table, mapping));
    }

    @Test
    void shouldReturnTransformNamesAsPartitionTokensForTimeTransforms() throws Exception {
        String table = "ts_transform_events";
        var factory = handlerForPartitionedTable(table,
                "year(ts), month(ts), day(ts), hour(ts)", "id BIGINT, ts TIMESTAMP");

        // PARTITION_BY receives the transform name (lower-cased), not the source column.
        assertArrayEquals(new String[]{"year", "month", "day", "hour"},
                factory.getPartitionBy(table),
                "Time transforms must be partitioned by the transform name, not the source column");
    }

    @Test
    void shouldReturnDerivedProjectionsForTimeTransforms() throws Exception {
        String table = "ts_projection_events";
        var factory = handlerForPartitionedTable(table,
                "year(ts), month(ts), day(ts), hour(ts)", "id BIGINT, ts TIMESTAMP");

        // Each transform is projected as `fn("col") AS "fn"` so the PARTITION_BY token resolves.
        assertArrayEquals(new String[]{
                        "year(\"ts\") AS \"year\"",
                        "month(\"ts\") AS \"month\"",
                        "day(\"ts\") AS \"day\"",
                        "hour(\"ts\") AS \"hour\""},
                factory.getPartitionProjections(table),
                "Expected one derived-column projection per time transform");
    }

    @Test
    void shouldReturnNoProjectionsForIdentityPartitionedTable() throws Exception {
        String table = "identity_partitioned_events";
        var factory = handlerForPartitionedTable(table,
                "date, level", "id BIGINT, date DATE, level VARCHAR");

        // Identity columns already exist in the relation — they are partitioned directly and need
        // no projection.
        assertArrayEquals(new String[]{"date", "level"}, factory.getPartitionBy(table));
        assertEquals(0, factory.getPartitionProjections(table).length,
                "Identity partitions must not emit derived-column projections");
    }

    @Test
    void shouldResolveMixedIdentityAndTransformPartitions() throws Exception {
        String table = "mixed_partitioned_events";
        var factory = handlerForPartitionedTable(table,
                "region, day(ts)", "id BIGINT, region VARCHAR, ts TIMESTAMP");

        // Order follows partition_key_index: identity first, then the transform.
        assertArrayEquals(new String[]{"region", "day"}, factory.getPartitionBy(table));
        // Only the transform contributes a projection; the identity column does not.
        assertArrayEquals(new String[]{"day(\"ts\") AS \"day\""},
                factory.getPartitionProjections(table));
    }

    @Test
    void shouldReturnEmptyProjectionsForNonPartitionedTable() throws Exception {
        String table = "no_partition_projections";
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn,
                    "CREATE TABLE %s.%s.%s (id BIGINT, value VARCHAR)".formatted(CATALOG, SCHEMA, table));
        }
        var mapping = new QueueIdToTableMapping(table, CATALOG, SCHEMA, table, Map.of(), null);
        var factory = new DuckLakeIngestionHandler(Map.of(table, mapping));

        assertEquals(0, factory.getPartitionProjections(table).length);
    }

    @Test
    void shouldReturnEmptyProjectionsForUnknownQueue() {
        var factory = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping(QUEUE_ID, null)));
        String[] projections = factory.getPartitionProjections("no-such-queue");
        assertNotNull(projections, "Unknown queue must yield an empty array, not null");
        assertEquals(0, projections.length);
    }

    /**
     * End-to-end invariant that {@code ParquetIngestionQueue} relies on: adding the handler's
     * projections to the relation makes every PARTITION_BY token a resolvable column. Builds the
     * same wrapped relation the queue builds ({@code SELECT *, <projections> FROM (...)}) and asserts
     * each token resolves to a real column.
     */
    @Test
    void derivedProjectionsMakePartitionTokensResolvable() throws Exception {
        String table = "resolvable_tokens_events";
        var factory = handlerForPartitionedTable(table,
                "year(ts), month(ts), day(ts), hour(ts)", "id BIGINT, ts TIMESTAMP");

        String[] tokens = factory.getPartitionBy(table);
        String[] projections = factory.getPartitionProjections(table);

        String wrapped = "SELECT *, %s FROM (SELECT * FROM %s.%s.%s)".formatted(
                String.join(", ", projections), CATALOG, SCHEMA, table);

        // If a token did not resolve to a column in the wrapped relation, selecting it would throw.
        String tokenList = String.join(", ",
                java.util.Arrays.stream(tokens).map(t -> "\"" + t + "\"").toArray(String[]::new));
        try (Connection conn = ConnectionPool.getConnection()) {
            assertDoesNotThrow(() ->
                    ConnectionPool.execute(conn, "SELECT %s FROM (%s) LIMIT 0".formatted(tokenList, wrapped)),
                    "Every PARTITION_BY token must resolve as a column in the projected relation");
        }
    }

    // -----------------------------------------------------------------------
    // bucket(N) hash-partition transform
    //
    // COPY's PARTITION_BY needs a real column, and DuckLake's bucket() is murmur3 with no DuckDB
    // builtin, so the handler projects _dd_iceberg_bucket(col, N). The hive key follows DuckLake's
    // own directory naming ("bucket", then "bucket_<col>" on collision) so ducklake_add_data_files
    // — which matches files to partitioning by hive key name — accepts the written files.
    // -----------------------------------------------------------------------

    @Test
    void shouldResolveBucketTransformsWithDuckLakeHiveKeys() throws Exception {
        String table = "bucketed_events";
        var factory = handlerForPartitionedTable(table,
                "bucket(4, group_id), event_date, bucket(4, user_id)",
                "group_id BIGINT, user_id BIGINT, event_date DATE");

        // First bucket dir is "bucket"; the second collides and is disambiguated as "bucket_user_id"
        // (DuckLake's own writer convention). The identity column keeps its name.
        assertArrayEquals(new String[]{"bucket", "event_date", "bucket_user_id"},
                factory.getPartitionBy(table));
        // Identity contributes no projection; each bucket column projects the murmur3 bucket macro.
        assertArrayEquals(new String[]{
                        "_dd_iceberg_bucket(\"group_id\", 4) AS \"bucket\"",
                        "_dd_iceberg_bucket(\"user_id\", 4) AS \"bucket_user_id\""},
                factory.getPartitionProjections(table));
    }

    @Test
    void bucketProjectionsMakePartitionTokensResolvable() throws Exception {
        String table = "bucket_resolvable_events";
        var factory = handlerForPartitionedTable(table,
                "bucket(8, group_id), event_date, bucket(8, user_id)",
                "group_id BIGINT, user_id BIGINT, event_date DATE");

        String[] tokens = factory.getPartitionBy(table);
        String[] projections = factory.getPartitionProjections(table);
        String wrapped = "SELECT *, %s FROM (SELECT * FROM %s.%s.%s)".formatted(
                String.join(", ", projections), CATALOG, SCHEMA, table);
        String tokenList = String.join(", ",
                java.util.Arrays.stream(tokens).map(t -> "\"" + t + "\"").toArray(String[]::new));
        try (Connection conn = ConnectionPool.getConnection()) {
            assertDoesNotThrow(() ->
                    ConnectionPool.execute(conn, "SELECT %s FROM (%s) LIMIT 0".formatted(tokenList, wrapped)),
                    "Every bucket PARTITION_BY token must resolve as a column in the projected relation");
        }
    }

    /**
     * The projected {@code _dd_iceberg_bucket(col, N)} must equal DuckLake's native {@code bucket()}
     * — otherwise files register under the wrong partition and pruning silently drops rows. Ground
     * truth was captured from DuckLake 1.5.5's on-disk {@code bucket=<v>} directories (N=16) across
     * the long domain: 0, positives, snowflake ids, {@code Long.MIN/MAX_VALUE}, negatives, and the
     * int32 boundaries (int columns hash as promoted longs).
     */
    @Test
    void icebergBucketMacroMatchesDuckLakeNativeBucket() throws Exception {
        // Resolving a bucket-partitioned table registers the macros (lazily, on first bucket use).
        handlerForPartitionedTable("macro_bootstrap", "bucket(4, id)", "id BIGINT")
                .getPartitionProjections("macro_bootstrap");

        String probe = """
                SELECT count(*) FILTER (WHERE truth <> _dd_iceberg_bucket(g, 16)) AS mismatches
                FROM (VALUES (0,12),(1,4),(2,4),(3,3),(7,3),(11,7),(42,14),(255,7),(1000000,6),
                             (123456789,1),(1000000000000,12),(361193724270612480,9),
                             (361193725126246400,5),(9223372036854775807,15),
                             (-1,8),(-2,5),(-100,14),(2147483647,14),(-2147483648,8),
                             (-9223372036854775808,5)) v(g, truth)
                """;
        try (Connection conn = ConnectionPool.getConnection()) {
            long mismatches = ConnectionPool.collectFirst(conn, probe, Long.class);
            assertEquals(0L, mismatches,
                    "_dd_iceberg_bucket must match DuckLake's native bucket(16, ...) for every probe value");
        }
    }

    /**
     * Stronger than the static probe above: compares the macro to DuckLake's <em>own</em> bucketing
     * computed at test time. Inlining is disabled so each row lands in a file whose hive dir carries
     * the native bucket value; the macro must reproduce it for every value and sign.
     */
    @Test
    void icebergBucketMatchesDuckLakeNativeComputedAtTestTime() throws Exception {
        String table = "native_bucket_probe";
        handlerForPartitionedTable("macro_bootstrap2", "bucket(4, id)", "id BIGINT")
                .getPartitionProjections("macro_bootstrap2");   // register the macros

        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn,
                    "CALL %s.set_option('data_inlining_row_limit', 0)".formatted(CATALOG));
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "CREATE TABLE %s.%s.%s (g BIGINT)".formatted(CATALOG, SCHEMA, table),
                    "ALTER TABLE %s.%s.%s SET PARTITIONED BY (bucket(16, g))".formatted(CATALOG, SCHEMA, table),
                    ("INSERT INTO %s.%s.%s VALUES (0),(1),(42),(255),(-1),(-2),(-100),"
                            + "(2147483647),(-2147483648),(9223372036854775807),(-9223372036854775808),"
                            + "(361193724270612480)").formatted(CATALOG, SCHEMA, table)
            });
            String dataGlob = tempDir.resolve("data").resolve(SCHEMA).resolve(table)
                    .resolve("**").resolve("*.parquet").toString();
            String cmp = ("SELECT count(*) FROM ("
                    + "SELECT g, _dd_iceberg_bucket(g, 16) AS macro, "
                    + "regexp_extract(filename, 'bucket=([0-9]+)', 1)::BIGINT AS native "
                    + "FROM read_parquet('%s', filename=true)) WHERE macro <> native").formatted(dataGlob);
            long mismatches = ConnectionPool.collectFirst(conn, cmp, Long.class);
            assertEquals(0L, mismatches,
                    "_dd_iceberg_bucket must equal DuckLake's on-disk native bucket for every value/sign");
        }
    }

    @Test
    void shouldAcceptBucketOnInt32Column() throws Exception {
        // Iceberg promotes int32 to a long for hashing, so the long-based macro is correct.
        var factory = handlerForPartitionedTable("int32_bucket_events", "bucket(4, uid)", "uid INTEGER");
        assertArrayEquals(new String[]{"bucket"}, factory.getPartitionBy("int32_bucket_events"));
        assertArrayEquals(new String[]{"_dd_iceberg_bucket(\"uid\", 4) AS \"bucket\""},
                factory.getPartitionProjections("int32_bucket_events"));
    }

    private void createPartitionedTable(String table, String columnsDdl, String partitionBySpec) throws SQLException {
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "CREATE TABLE %s.%s.%s (%s)".formatted(CATALOG, SCHEMA, table, columnsDdl),
                    "ALTER TABLE %s.%s.%s SET PARTITIONED BY (%s)".formatted(CATALOG, SCHEMA, table, partitionBySpec)
            });
        }
    }

    private QueueIdToTableMapping tableMapping(String table) {
        return new QueueIdToTableMapping(table, CATALOG, SCHEMA, table, Map.of(), null);
    }

    /**
     * An unsupported bucket column (a float: DuckLake hashes its IEEE-754 bits, which SQL cannot
     * reinterpret exactly) must fail only its own queue. The handler still constructs and every other
     * queue keeps resolving its partitioning.
     */
    @Test
    void unsupportedBucketColumnFailsOnlyItsOwnQueue() throws Exception {
        createPartitionedTable("float_bucket_events", "id BIGINT, score DOUBLE", "bucket(4, score)");
        createPartitionedTable("healthy_bucket_events", "id BIGINT", "bucket(4, id)");

        var handler = assertDoesNotThrow(() -> new DuckLakeIngestionHandler(Map.of(
                        "float_bucket_events", tableMapping("float_bucket_events"),
                        "healthy_bucket_events", tableMapping("healthy_bucket_events"))),
                "one bad table must not prevent the handler from starting");

        // The healthy queue is unaffected.
        assertArrayEquals(new String[]{"bucket"}, handler.getPartitionBy("healthy_bucket_events"));
        assertArrayEquals(new String[]{"_dd_iceberg_bucket(\"id\", 4) AS \"bucket\""},
                handler.getPartitionProjections("healthy_bucket_events"));
        assertNotNull(handler.getTargetPath("float_bucket_events"), "the bad queue is still known");

        // The bad queue's writes are rejected with a message naming the column and type.
        var ex = assertThrows(UnsupportedPartitionTransformException.class,
                () -> handler.getPartitionBy("float_bucket_events"));
        assertTrue(ex.getMessage().contains("score") && ex.getMessage().contains("float64"),
                "message must name the column and type, was: " + ex.getMessage());
        assertThrows(UnsupportedPartitionTransformException.class,
                () -> handler.getPartitionProjections("float_bucket_events"));
    }

    /**
     * The same isolation when the partitioning is changed at runtime: the refresh after the ALTER
     * fails only that queue, and changing the partitioning back recovers it.
     */
    @Test
    void unsupportedPartitioningAddedAtRuntimeFailsOnlyThatQueueAndRecovers() throws Exception {
        createPartitionedTable("runtime_events", "id BIGINT, score DOUBLE", "bucket(4, id)");
        createPartitionedTable("bystander_events", "id BIGINT", "bucket(4, id)");
        var handler = new DuckLakeIngestionHandler(Map.of(
                "runtime_events", tableMapping("runtime_events"),
                "bystander_events", tableMapping("bystander_events")), Duration.ZERO); // refresh on every read
        assertArrayEquals(new String[]{"bucket"}, handler.getPartitionBy("runtime_events"));

        String alter = "ALTER TABLE %s.%s.runtime_events SET PARTITIONED BY (%s)";
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, alter.formatted(CATALOG, SCHEMA, "bucket(4, score)"));
        }
        assertThrows(UnsupportedPartitionTransformException.class, () -> handler.getPartitionBy("runtime_events"));
        assertArrayEquals(new String[]{"bucket"}, handler.getPartitionBy("bystander_events"),
                "other queues keep working after one table's partitioning becomes unsupported");

        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, alter.formatted(CATALOG, SCHEMA, "bucket(4, id)"));
        }
        assertArrayEquals(new String[]{"bucket"}, handler.getPartitionBy("runtime_events"),
                "fixing the partitioning recovers the queue on the next refresh");
    }

    @Test
    void shouldProjectStringBucketOverUtf8Bytes() throws Exception {
        var factory = handlerForPartitionedTable("string_bucket_events", "bucket(8, name)", "name VARCHAR");
        assertArrayEquals(new String[]{"bucket"}, factory.getPartitionBy("string_bucket_events"));
        assertArrayEquals(new String[]{"_dd_iceberg_bucket_hex(hex(encode(\"name\"::VARCHAR)), 8) AS \"bucket\""},
                factory.getPartitionProjections("string_bucket_events"));
    }

    /**
     * For every column type the handler supports, the generated bucket projection must equal the bucket
     * DuckLake itself writes. Uses bucket(2^31-1), so the directory value is the full masked murmur3
     * hash and any encoding difference shows up. Covers negatives, pre-epoch times, fractional
     * seconds, exact-4-byte and multibyte UTF-8 strings, the empty string, and wide/unsigned integers.
     */
    @Test
    void bucketProjectionMatchesDuckLakeNativeForEveryType() throws Exception {
        String[][] cases = {
                {"TINYINT", "-3"}, {"SMALLINT", "-7"}, {"INTEGER", "34"}, {"BIGINT", "-9223372036854775808"},
                {"BOOLEAN", "true"}, {"BOOLEAN", "false"},
                {"DATE", "'2017-11-16'"}, {"DATE", "'1901-01-01'"},
                {"TIME", "'22:31:08.5'"}, {"TIMETZ", "'22:31:08+02'"},
                {"TIMESTAMP", "'2017-11-16 22:31:08.123456'"}, {"TIMESTAMP", "'1960-01-01 00:00:00.5'"},
                {"TIMESTAMPTZ", "'2017-11-16 14:31:08-08:00'"}, {"TIMESTAMP_S", "'2017-11-16 22:31:08'"},
                {"TIMESTAMP_MS", "'2017-11-16 22:31:08.123'"}, {"TIMESTAMP_NS", "'2017-11-16 22:31:08.123456789'"},
                {"DECIMAL(4,2)", "-12.34"}, {"DECIMAL(9,0)", "42"}, {"DECIMAL(18,3)", "123456789.123"},
                {"DECIMAL(38,2)", "12345678901234567890.12"},
                {"VARCHAR", "'iceberg'"}, {"VARCHAR", "''"}, {"VARCHAR", "'abcdefgh'"}, {"VARCHAR", "'héllo wörld ✓'"},
                {"UUID", "'f79c3e09-677c-4bbd-a479-3f349cb785e7'"}, {"INTERVAL", "INTERVAL 3 DAY"},
                {"UTINYINT", "200"}, {"USMALLINT", "60000"}, {"UINTEGER", "4000000000"},
                {"UBIGINT", "18446744073709551615"}, {"HUGEINT", "-170141183460469231731687303715884105727"},
                {"UHUGEINT", "340282366920938463463374607431768211455"}, {"BLOB", "'\\x00\\x01\\x02\\x03\\xAB'"},
        };
        List<String> mismatches = new java.util.ArrayList<>();
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "CALL %s.set_option('data_inlining_row_limit', 0)".formatted(CATALOG));
            for (int i = 0; i < cases.length; i++) {
                String type = cases[i][0], literal = "CAST(%s AS %s)".formatted(cases[i][1], type);
                String table = "bucket_type_" + i;
                ConnectionPool.executeBatchInTxn(conn, new String[]{
                        "CREATE TABLE %s.%s.%s (c %s)".formatted(CATALOG, SCHEMA, table, type),
                        "ALTER TABLE %s.%s.%s SET PARTITIONED BY (bucket(2147483647, c))".formatted(CATALOG, SCHEMA, table),
                        "INSERT INTO %s.%s.%s VALUES (%s)".formatted(CATALOG, SCHEMA, table, literal)
                });
                String nativeBucket = ConnectionPool.collectFirst(conn,
                        "SELECT regexp_extract(filename, 'bucket=([^/]+)', 1) FROM %s.%s.%s"
                                .formatted(CATALOG, SCHEMA, table), String.class);

                var mapping = new QueueIdToTableMapping(table, CATALOG, SCHEMA, table, Map.of(), null);
                String projection = new DuckLakeIngestionHandler(Map.of(table, mapping)).getPartitionProjections(table)[0];
                String computed = ConnectionPool.collectFirst(conn,
                        "SELECT \"bucket\"::VARCHAR FROM (SELECT %s FROM (SELECT %s AS c))".formatted(projection, literal),
                        String.class);
                if (!nativeBucket.equals(computed)) {
                    mismatches.add("%s %s: native=%s handler=%s".formatted(type, cases[i][1], nativeBucket, computed));
                }
            }
        }
        assertEquals(List.of(), mismatches, "bucket projection must equal DuckLake's native bucket for every type");
    }

    /**
     * The collector's actual write path, on a table whose hive keys collide: two bucket columns
     * ({@code bucket}, {@code bucket_uid}) and two day() columns ({@code day}, {@code day_b}). COPY
     * with the handler's projections and PARTITION_BY tokens, then ducklake_add_data_files matches
     * files to the partitioning by hive key name — so this also proves the disambiguated names are the
     * ones DuckLake expects. Includes a string bucket with multibyte and NULL keys. Pruned lookups on
     * every partition column must return the right rows.
     */
    @Test
    void copyWrittenFilesWithCollidingPartitionKeysRegisterAndPrune() throws Exception {
        String table = "copy_colliding_keys";
        var factory = handlerForPartitionedTable(table, "bucket(8, name), bucket(4, uid), day(a), day(b)",
                "name VARCHAR, uid BIGINT, a TIMESTAMP, b TIMESTAMP, n BIGINT");
        assertArrayEquals(new String[]{"bucket", "bucket_uid", "day", "day_b"}, factory.getPartitionBy(table));

        Path stage = tempDir.resolve("stage");
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, ("COPY (SELECT *, %s FROM (VALUES "
                    + "('alice', 361193724270612480, TIMESTAMP '2026-01-05 10:00', TIMESTAMP '2026-02-20 11:00', 1), "
                    + "('bob',   42,                 TIMESTAMP '2026-01-15 10:00', TIMESTAMP '2026-03-01 00:00', 2), "
                    + "(NULL,    -7,                 TIMESTAMP '2026-01-05 23:59', TIMESTAMP '2026-02-20 11:00', 3), "
                    + "('héllo', 9223372036854775807, TIMESTAMP '2026-01-31 00:00', TIMESTAMP '2026-02-28 12:00', 4)"
                    + ") v(name, uid, a, b, n)) TO '%s' (FORMAT parquet, PARTITION_BY(%s))")
                    .formatted(String.join(", ", factory.getPartitionProjections(table)), stage,
                            String.join(", ", factory.getPartitionBy(table))));
            ConnectionPool.execute(conn, ("CALL ducklake_add_data_files('%s', '%s', '%s', hive_partitioning => true, "
                    + "ignore_extra_columns => true, allow_missing => true)")
                    .formatted(CATALOG, table, stage.resolve("**").resolve("*.parquet")));

            String fq = "%s.%s.%s".formatted(CATALOG, SCHEMA, table);
            assertEquals(4L, ConnectionPool.collectFirst(conn, "SELECT count(*) FROM " + fq, Long.class));
            assertEquals(2L, ConnectionPool.collectFirst(conn, "SELECT n FROM " + fq + " WHERE name = 'bob'", Long.class));
            assertEquals(4L, ConnectionPool.collectFirst(conn, "SELECT n FROM " + fq + " WHERE name = 'héllo'", Long.class));
            assertEquals(3L, ConnectionPool.collectFirst(conn, "SELECT n FROM " + fq + " WHERE name IS NULL", Long.class));
            assertEquals(1L, ConnectionPool.collectFirst(conn,
                    "SELECT n FROM " + fq + " WHERE uid = 361193724270612480", Long.class));
            assertEquals(3L, ConnectionPool.collectFirst(conn, "SELECT n FROM " + fq + " WHERE uid = -7", Long.class));
            assertEquals(2L, ConnectionPool.collectFirst(conn,
                    "SELECT count(*) FROM " + fq + " WHERE b = TIMESTAMP '2026-02-20 11:00'", Long.class));
            assertEquals(4L, ConnectionPool.collectFirst(conn,
                    "SELECT n FROM " + fq + " WHERE a = TIMESTAMP '2026-01-31 00:00' AND b = TIMESTAMP '2026-02-28 12:00'",
                    Long.class));
        }
    }

    @Test
    void shouldDisambiguateCollidingTimeTransforms() throws Exception {
        // Two day() transforms collide on the "day" key; DuckLake names them day / day_b, and the
        // handler must match so ducklake_add_data_files accepts the files.
        var factory = handlerForPartitionedTable("dual_day_events", "day(a), day(b)",
                "a TIMESTAMP, b TIMESTAMP");
        assertArrayEquals(new String[]{"day", "day_b"}, factory.getPartitionBy("dual_day_events"));
        assertArrayEquals(new String[]{"day(\"a\") AS \"day\"", "day(\"b\") AS \"day_b\""},
                factory.getPartitionProjections("dual_day_events"));
    }

    // -----------------------------------------------------------------------
    // getVariables
    // -----------------------------------------------------------------------

    private static final String VARS_RELATION = "handler_vars";

    private static IngestionVariables variablesFrom(String hocon) {
        return IngestionVariables.fromConfig(ConfigFactory.parseString(hocon));
    }

    @Test
    void shouldResolveStaticVariablesAtConstruction() {
        var mapping = mapping(QUEUE_ID, null)
                .withVariables(variablesFrom("variables { env = \"prod\" }"));
        var handler = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping));

        assertEquals(Map.of("env", "prod"), handler.getVariables(QUEUE_ID));
        assertEquals(Map.of(), handler.getVariables("unknown-queue"));
    }

    @Test
    void shouldLoadRelationVariablesAtStartupAndReloadThemOnRefresh() throws Exception {
        ConnectionPool.execute("CREATE OR REPLACE TABLE %s(key VARCHAR, value VARCHAR)".formatted(VARS_RELATION));
        try {
            ConnectionPool.execute("INSERT INTO %s VALUES ('env', 'prod')".formatted(VARS_RELATION));
            var mapping = mapping(QUEUE_ID, null)
                    .withVariables(variablesFrom("variables_view = \"%s\"".formatted(VARS_RELATION)));
            var clock = new MutableClock(Instant.now(), ZoneId.of("UTC"));
            Duration refreshInterval = Duration.ofMinutes(1);
            var handler = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping), refreshInterval, clock);

            // Loaded into memory at startup, without any write having happened.
            assertEquals(Map.of("env", "prod"), handler.getVariables(QUEUE_ID));

            ConnectionPool.execute("UPDATE %s SET value = 'staging' WHERE key = 'env'".formatted(VARS_RELATION));
            assertEquals(Map.of("env", "prod"), handler.getVariables(QUEUE_ID),
                    "within the refresh interval the cached value must be reused");

            // Changing a row is data, not DDL, so the DuckLake schema_version shortcut must not be
            // what decides this: the refresh tick alone has to pick the new value up.
            clock.advanceBy(refreshInterval.plusSeconds(1));
            assertEquals(Map.of("env", "staging"), handler.getVariables(QUEUE_ID));
        } finally {
            ConnectionPool.execute("DROP TABLE IF EXISTS " + VARS_RELATION);
        }
    }

    @Test
    void shouldStopApplyingAVariableOnceItsRowHasExpired() throws Exception {
        ConnectionPool.execute(
                "CREATE OR REPLACE TABLE %s(key VARCHAR, value VARCHAR, expires_at TIMESTAMPTZ)"
                        .formatted(VARS_RELATION));
        try {
            ConnectionPool.execute(("INSERT INTO %s VALUES ('token', 'live', now() + INTERVAL 1 HOUR)")
                    .formatted(VARS_RELATION));
            var mapping = mapping(QUEUE_ID, null).withVariables(variablesFrom("""
                    variables_view              = "%s"
                    variables_expiration_column = "expires_at"
                    """.formatted(VARS_RELATION)));
            var clock = new MutableClock(Instant.now(), ZoneId.of("UTC"));
            Duration refreshInterval = Duration.ofMinutes(1);
            var handler = new DuckLakeIngestionHandler(Map.of(QUEUE_ID, mapping), refreshInterval, clock);

            assertEquals(Map.of("token", "live"), handler.getVariables(QUEUE_ID));

            // Expire the row. Its expiry is compared against DuckDB's now(), so move the row rather
            // than the test clock; the test clock is what releases the next refresh.
            ConnectionPool.execute("UPDATE %s SET expires_at = now() - INTERVAL 1 HOUR".formatted(VARS_RELATION));
            clock.advanceBy(refreshInterval.plusSeconds(1));

            assertEquals(Map.of(), handler.getVariables(QUEUE_ID),
                    "an expired variable must no longer be applied");
        } finally {
            ConnectionPool.execute("DROP TABLE IF EXISTS " + VARS_RELATION);
        }
    }
}

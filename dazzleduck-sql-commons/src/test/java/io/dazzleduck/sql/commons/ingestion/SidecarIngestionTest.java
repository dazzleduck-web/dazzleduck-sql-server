package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.util.MutableClock;
import org.jmock.lib.concurrent.DeterministicScheduler;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Sidecars on the direct path: a queue's sidecar tables are created or checked at startup, and every
 * batch writes each sidecar's rows from the batch's rows as {@code __base}, registered in the same
 * transaction as the base files.
 */
class SidecarIngestionTest {

    @TempDir
    Path tempDir;

    static final String CATALOG = "sidecar_lake";
    static final String SCHEMA = "main";
    static final String TABLE = "events";
    static final String METADATA = "__ducklake_metadata_" + CATALOG;

    Path outputDir;
    Path src;

    @BeforeEach
    void setUp() throws Exception {
        Files.createDirectories(tempDir.resolve("data"));
        outputDir = Files.createDirectories(tempDir.resolve("output"));
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.executeBatchInTxn(conn, new String[]{
                    "ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s', DATA_INLINING_ROW_LIMIT 0)".formatted(
                            tempDir.resolve("catalog"), CATALOG, tempDir.resolve("data")),
                    "CREATE TABLE %s.%s.%s (id BIGINT, value BIGINT, category VARCHAR)".formatted(CATALOG, SCHEMA, TABLE)
            });
        }
        src = tempDir.resolve("src.parquet");
        ConnectionPool.execute("COPY (SELECT i AS id, i*2 AS value, 'cat'||(i%%3) AS category FROM range(0, 100) t(i)) TO '%s' (FORMAT PARQUET)"
                .formatted(src));
    }

    @AfterEach
    void tearDown() throws SQLException {
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, "DETACH " + CATALOG);
        }
    }

    @Test
    void aMissingSidecarTableIsCreatedAndWrittenInTheBatchsSnapshot() throws Exception {
        var handler = handler("""
                sidecars = [{ table = "category_counts"
                              transformation = "SELECT category, count(*) AS n FROM __base GROUP BY ALL" }]
                """);
        assertEquals(List.of(new SidecarTarget.Column("category", "VARCHAR"), new SidecarTarget.Column("n", "BIGINT")),
                columns("category_counts"), "created from the query's columns");

        var scheduler = new DeterministicScheduler();
        try (var queue = queue(handler, scheduler)) {
            IngestionResult result = write(queue, scheduler, 0);
            assertEquals(100, result.rowCount());
            assertEquals(List.of("category_counts"), List.copyOf(result.sidecarFiles().keySet()));
        }
        assertEquals(100L, scalar("SELECT count(*) FROM %s.%s.%s".formatted(CATALOG, SCHEMA, TABLE)));
        assertEquals(34L, scalar("SELECT n FROM %s.%s.category_counts WHERE category = 'cat0'".formatted(CATALOG, SCHEMA)));
        assertEquals(1L, scalar(("SELECT count(DISTINCT begin_snapshot) FROM %s.ducklake_data_file "
                + "WHERE end_snapshot IS NULL").formatted(METADATA)), "the base and its sidecar commit together");
        assertEquals(2L, scalar("SELECT count(*) FROM %s.ducklake_data_file WHERE end_snapshot IS NULL".formatted(METADATA)));
    }

    @Test
    void anExistingTableIsWrittenByColumnNameAndTypeWhateverItsColumnOrder() throws Exception {
        execute("CREATE TABLE %s.%s.category_totals (total BIGINT, category VARCHAR)".formatted(CATALOG, SCHEMA));
        var handler = handler("""
                sidecars = [{ table = "category_totals"
                              transformation = "SELECT category, sum(value)::BIGINT AS total FROM __base GROUP BY ALL" }]
                """);
        var scheduler = new DeterministicScheduler();
        try (var queue = queue(handler, scheduler)) {
            write(queue, scheduler, 0);
        }
        assertEquals(3366L, scalar("SELECT total FROM %s.%s.category_totals WHERE category = 'cat0'".formatted(CATALOG, SCHEMA)));
    }

    @Test
    void aTableThatDoesNotMatchItsQueryFailsStartup() {
        execute("CREATE TABLE %s.%s.category_counts (category VARCHAR, n INTEGER)".formatted(CATALOG, SCHEMA));
        var e = assertThrows(IllegalArgumentException.class, () -> handler("""
                sidecars = [{ table = "category_counts"
                              transformation = "SELECT category, count(*) AS n FROM __base GROUP BY ALL" }]
                """));
        assertTrue(e.getMessage().contains("category_counts") && e.getMessage().contains("'n' is INTEGER in the table but BIGINT"),
                e.getMessage());

        execute("CREATE TABLE %s.%s.wide (n INTEGER)".formatted(CATALOG, SCHEMA));
        e = assertThrows(IllegalArgumentException.class, () -> handler("""
                sidecars = [{ table = "wide", transformation = "SELECT 1::INTEGER AS n, 'x' AS missing FROM __base" }]
                """));
        assertTrue(e.getMessage().contains("'missing' VARCHAR is not in the table"), e.getMessage());
    }

    @Test
    void aTableColumnTheQueryDoesNotProduceIsWrittenAsNull() throws Exception {
        execute("CREATE TABLE %s.%s.ids (id BIGINT, note VARCHAR)".formatted(CATALOG, SCHEMA));
        var handler = handler("""
                sidecars = [{ table = "ids", transformation = "SELECT id FROM __base" }]
                """);
        var scheduler = new DeterministicScheduler();
        try (var queue = queue(handler, scheduler)) {
            write(queue, scheduler, 0);
        }
        assertEquals(100L, scalar("SELECT count(*) FROM %s.%s.ids WHERE note IS NULL".formatted(CATALOG, SCHEMA)));
    }

    @Test
    void aColumnAddedToTheSidecarTableUnderARunningQueueDoesNotStopIt() throws Exception {
        // The README's way to change a sidecar's columns: change the table first, then the query.
        var handler = handler("""
                sidecars = [{ table = "ids", transformation = "SELECT id FROM __base" }]
                """);
        var scheduler = new DeterministicScheduler();
        try (var queue = queue(handler, scheduler)) {
            write(queue, scheduler, 0);
            execute("ALTER TABLE %s.%s.ids ADD COLUMN note VARCHAR".formatted(CATALOG, SCHEMA));
            handler.refreshNow(TABLE);
            assertEquals(100, write(queue, scheduler, 1).rowCount(), "batches keep being written");
        }
        assertEquals(200L, scalar("SELECT count(*) FROM %s.%s.ids".formatted(CATALOG, SCHEMA)));
        assertEquals(200L, scalar("SELECT count(*) FROM %s.%s.ids WHERE note IS NULL".formatted(CATALOG, SCHEMA)));
    }

    @Test
    void aQueryThatDoesNotRunOverTheQueuesRowsFailsStartup() {
        var e = assertThrows(IllegalArgumentException.class, () -> handler("""
                sidecars = [{ table = "bad", transformation = "SELECT no_such_column FROM __base" }]
                """));
        assertTrue(e.getMessage().contains("sidecar 'bad'") && e.getMessage().contains("no_such_column"), e.getMessage());
    }

    @Test
    void aColumnDuckLakeCannotRegisterFromParquetFailsStartup() {
        // sum() over BIGINT is HUGEINT, which Parquet stores as DOUBLE.
        var e = assertThrows(IllegalArgumentException.class, () -> handler("""
                sidecars = [{ table = "totals", transformation = "SELECT category, sum(value) AS total FROM __base GROUP BY ALL" }]
                """));
        assertTrue(e.getMessage().contains("'total' is HUGEINT") && e.getMessage().contains("cast it"), e.getMessage());
        assertEquals(List.of(), assertDoesNotThrow(() -> columns("totals")), "no table is created");
    }

    @Test
    void aPartitionedSidecarTableFailsStartup() {
        execute("CREATE TABLE %s.%s.category_counts (category VARCHAR, n BIGINT)".formatted(CATALOG, SCHEMA));
        execute("ALTER TABLE %s.%s.category_counts SET PARTITIONED BY (category)".formatted(CATALOG, SCHEMA));
        var e = assertThrows(IllegalArgumentException.class, () -> handler("""
                sidecars = [{ table = "category_counts"
                              transformation = "SELECT category, count(*) AS n FROM __base GROUP BY ALL" }]
                """));
        assertTrue(e.getMessage().contains("partitioned"), e.getMessage());
    }

    @Test
    void aSidecarThatFailsFailsTheBatchAndNothingIsRegistered() throws Exception {
        // Describes fine over no rows; fails on the first real one.
        var handler = handler("""
                sidecars = [{ table = "bad_cast", transformation = "SELECT CAST(category AS INTEGER) AS c FROM __base" }]
                """);
        var scheduler = new DeterministicScheduler();
        try (var queue = queue(handler, scheduler)) {
            var failure = assertThrows(ExecutionException.class, () -> write(queue, scheduler, 0));
            assertTrue(String.valueOf(failure.getCause().getMessage()).contains("cat"), failure.toString());
            assertEquals(0, queue.getOrphanFilesDeleted(), "the base was only staged: nothing reached the output");
            assertEquals(1, queue.getStats().failedWriteBatches());
        }
        assertEquals(0L, scalar("SELECT count(*) FROM %s.%s.%s".formatted(CATALOG, SCHEMA, TABLE)));
        assertEquals(0L, scalar("SELECT count(*) FROM %s.ducklake_data_file".formatted(METADATA)));
        assertEquals(List.of(), parquetFilesUnder(outputDir));
        assertEquals(List.of(), parquetFilesUnder(tempDir.resolve("data").resolve(SCHEMA).resolve("bad_cast")),
                "nothing is left of the failed sidecar's file");
        assertEquals(List.of(), stagingDirectories(), "the staging directory is deleted");
    }

    @Test
    void aTableNamedInAnotherCaseIsReportedByName() {
        execute("CREATE TABLE %s.%s.\"Ids\" (id BIGINT)".formatted(CATALOG, SCHEMA));
        var e = assertThrows(IllegalArgumentException.class, () -> handler("""
                sidecars = [{ table = "ids", transformation = "SELECT id FROM __base" }]
                """));
        assertTrue(e.getMessage().contains("exists as 'Ids'"), e.getMessage());
    }

    @Test
    void aStagedWriteUploadsEveryFileAndLeavesNoStagingBehind() throws Exception {
        var handler = handler("""
                sidecars = [{ table = "category_counts"
                              transformation = "SELECT category, count(*) AS n FROM __base GROUP BY ALL" }]
                """);
        var scheduler = new DeterministicScheduler();
        IngestionResult result;
        try (var queue = queue(handler, scheduler)) {
            result = write(queue, scheduler, 0);
        }
        assertEquals(1, result.filesCreated().size());
        assertTrue(result.filesCreated().get(0).startsWith(outputDir.toString()), result.filesCreated().toString());
        assertTrue(Files.exists(Path.of(result.filesCreated().get(0))), "the base file was uploaded to the output path");
        String sidecarFile = result.sidecarFiles().get("category_counts").get(0);
        assertTrue(sidecarFile.contains("category_counts"), sidecarFile);
        assertTrue(Files.exists(Path.of(sidecarFile)), "the sidecar file was uploaded under its table's path");
        assertEquals(List.of(), stagingDirectories(), "the staging directory is deleted");
    }

    @Test
    void aPartitionedBaseIsReadBackWithItsPartitionColumnsTypedNotGuessed() throws Exception {
        execute("ALTER TABLE %s.%s.%s ADD COLUMN code VARCHAR".formatted(CATALOG, SCHEMA, TABLE));
        execute("ALTER TABLE %s.%s.%s SET PARTITIONED BY (code)".formatted(CATALOG, SCHEMA, TABLE));
        var handler = handler("""
                transformation = "SELECT *, printf('2026-1-%d', 1 + id % 3) AS code FROM __this"
                sidecars = [{ table = "codes", transformation = "SELECT code, count(*) AS n FROM __base GROUP BY ALL" }]
                """);
        var scheduler = new DeterministicScheduler();
        IngestionResult result;
        try (var queue = queue(handler, scheduler)) {
            result = write(queue, scheduler, 0);
        }
        assertTrue(result.filesCreated().stream().allMatch(f -> f.contains("code=")), result.filesCreated().toString());
        // Guessed from the directory name, 2026-1-1 would be read back as the DATE 2026-01-01, and
        // the sidecar would get '2026-01-01' for a base row whose code is '2026-1-1'.
        assertEquals(34L, scalar("SELECT n FROM %s.%s.codes WHERE code = '2026-1-1'".formatted(CATALOG, SCHEMA)),
                "a VARCHAR partition value is read back as the text it was, not a guessed type");
        assertEquals(100L, scalar("SELECT count(*) FROM %s.%s.%s WHERE code IN ('2026-1-1', '2026-1-2', '2026-1-3')".formatted(CATALOG, SCHEMA, TABLE)));
    }

    @Test
    void aKeyValueDirectoryAboveTheInputIsNotReadAsAColumn() throws Exception {
        // Read back as hive partitioned, category=zzz would replace every row's category.
        Path inputDir = Files.createDirectories(tempDir.resolve("category=zzz"));
        var handler = handler("""
                sidecars = [{ table = "category_counts"
                              transformation = "SELECT category, count(*) AS n FROM __base GROUP BY ALL" }]
                """);
        var scheduler = new DeterministicScheduler();
        try (var queue = queue(handler, scheduler)) {
            Path input = Files.copy(src, inputDir.resolve("in.parquet"));
            var future = queue.add(new Batch<>(null, null, input.toString(), "producer1", 0,
                    Files.size(input), "parquet", Instant.now()));
            scheduler.tick(1, TimeUnit.MILLISECONDS);
            future.get(30, TimeUnit.SECONDS);
        }
        assertEquals(34L, scalar("SELECT n FROM %s.%s.category_counts WHERE category = 'cat0'".formatted(CATALOG, SCHEMA)));
        assertEquals(0L, scalar("SELECT count(*) FROM %s.%s.category_counts WHERE category = 'zzz'".formatted(CATALOG, SCHEMA)));
    }

    @Test
    void aStagingPathDuckDbWouldMisreadIsNotUsed() {
        assertEquals(tempDir, ParquetIngestionQueue.stagingRoot(tempDir));
        for (String bad : List.of("env=prod", "a*b", "a?b", "a[1]", "a{b}")) {
            Path dir = tempDir.resolve(bad);
            assertFalse(ParquetIngestionQueue.isSafeStagingPath(dir), bad);
            assertNotEquals(dir, ParquetIngestionQueue.stagingRoot(dir), "falls back to the system temp directory: " + bad);
        }
    }

    @Test
    void aSidecarWithNoRowsForTheBatchRegistersNoFile() throws Exception {
        var handler = handler("""
                sidecars = [{ table = "negatives", transformation = "SELECT id FROM __base WHERE value < 0" }]
                """);
        var scheduler = new DeterministicScheduler();
        try (var queue = queue(handler, scheduler)) {
            IngestionResult result = write(queue, scheduler, 0);
            assertEquals(Map.of(), result.sidecarFiles());
        }
        assertEquals(100L, scalar("SELECT count(*) FROM %s.%s.%s".formatted(CATALOG, SCHEMA, TABLE)));
        assertEquals(List.of(), parquetFilesUnder(tempDir.resolve("data").resolve(SCHEMA).resolve("negatives")),
                "the empty file is deleted");
    }

    @Test
    void baseIsTheTransformedRowsAndSidecarsSeeTheQueuesVariables() throws Exception {
        var handler = handler("""
                transformation = "SELECT id, value * 10 AS value, category FROM __this"
                variables { env = "prod" }
                sidecars = [
                  { table = "maxima", transformation = "SELECT max(value) AS max_value, getvariable('env') AS env FROM __base" }
                  { table = "ids", transformation = "SELECT id FROM __base WHERE id < 5" }
                ]
                """);
        var scheduler = new DeterministicScheduler();
        try (var queue = queue(handler, scheduler)) {
            write(queue, scheduler, 0);
        }
        assertEquals(1980L, scalar("SELECT max_value FROM %s.%s.maxima WHERE env = 'prod'".formatted(CATALOG, SCHEMA)));
        assertEquals(5L, scalar("SELECT count(*) FROM %s.%s.ids".formatted(CATALOG, SCHEMA)));
    }

    @Test
    void aSidecarTableDroppedUnderARunningQueueIsCreatedAgain() throws Exception {
        var handler = handler("""
                sidecars = [{ table = "ids", transformation = "SELECT id FROM __base" }]
                """);
        execute("DROP TABLE %s.%s.ids".formatted(CATALOG, SCHEMA));
        var scheduler = new DeterministicScheduler();
        try (var queue = queue(handler, scheduler)) {
            // The cached state still has the table, so registration fails; the refresh after it
            // creates the table again, and the next batch is written.
            assertThrows(ExecutionException.class, () -> write(queue, scheduler, 0));
            assertEquals(100, write(queue, scheduler, 1).rowCount());
        }
        assertEquals(100L, scalar("SELECT count(*) FROM %s.%s.ids".formatted(CATALOG, SCHEMA)));
        assertEquals(100L, scalar("SELECT count(*) FROM %s.%s.%s".formatted(CATALOG, SCHEMA, TABLE)));
    }

    @Test
    void aSidecarTablePartitionedUnderARunningQueueRejectsItsBatches() throws Exception {
        var handler = handler("""
                sidecars = [{ table = "ids", transformation = "SELECT id FROM __base" }]
                """);
        execute("ALTER TABLE %s.%s.ids SET PARTITIONED BY (id)".formatted(CATALOG, SCHEMA));
        handler.refreshNow(TABLE);
        var scheduler = new DeterministicScheduler();
        try (var queue = queue(handler, scheduler)) {
            var failure = assertThrows(ExecutionException.class, () -> write(queue, scheduler, 0));
            assertTrue(hasCause(failure, TransformationUnavailableException.class), failure.toString());
            assertTrue(failure.getCause().getMessage().contains("partitioned"), failure.getCause().getMessage());
        }
        assertEquals(0L, scalar("SELECT count(*) FROM %s.%s.%s".formatted(CATALOG, SCHEMA, TABLE)));
    }

    @Test
    void theBaseAndItsSidecarsSeeTheSameRowsFromANonDeterministicTransformation() throws Exception {
        execute("ALTER TABLE %s.%s.%s ADD COLUMN r DOUBLE".formatted(CATALOG, SCHEMA, TABLE));
        var handler = handler("""
                transformation = "SELECT *, random() AS r FROM __this"
                sidecars = [{ table = "r_sum", transformation = "SELECT sum(r) AS s FROM __base" }]
                """);
        var scheduler = new DeterministicScheduler();
        try (var queue = queue(handler, scheduler)) {
            write(queue, scheduler, 0);
        }
        assertEquals(1L, scalar(("SELECT (abs((SELECT sum(r) FROM %1$s.%2$s.%3$s) - (SELECT s FROM %1$s.%2$s.r_sum)) < 1e-9)::BIGINT")
                .formatted(CATALOG, SCHEMA, TABLE)), "the sidecar is computed from the rows the table got");
    }

    @Test
    void anUnsupportedTablePartitioningIsTheReasonGivenNotTheSidecars() throws Exception {
        execute("ALTER TABLE %s.%s.%s ADD COLUMN score DOUBLE".formatted(CATALOG, SCHEMA, TABLE));
        execute("ALTER TABLE %s.%s.%s SET PARTITIONED BY (bucket(2, score))".formatted(CATALOG, SCHEMA, TABLE));
        var handler = handler("""
                sidecars = [{ table = "ids", transformation = "SELECT id FROM __base" }]
                """);
        assertEquals(1, columns("ids").size(), "the sidecar table is still created");
        var scheduler = new DeterministicScheduler();
        try (var queue = queue(handler, scheduler)) {
            var failure = assertThrows(ExecutionException.class, () -> write(queue, scheduler, 0));
            assertTrue(hasCause(failure, UnsupportedPartitionTransformException.class), failure.toString());
        }
    }

    @Test
    void aPartitionedTableOfTheSameNameInAnotherSchemaIsNotTheSidecars() throws Exception {
        execute("CREATE SCHEMA %s.staging".formatted(CATALOG));
        execute("CREATE TABLE %s.staging.ids (id BIGINT)".formatted(CATALOG));
        execute("ALTER TABLE %s.staging.ids SET PARTITIONED BY (id)".formatted(CATALOG));
        var handler = handler("""
                sidecars = [{ table = "ids", transformation = "SELECT id FROM __base" }]
                """);
        var scheduler = new DeterministicScheduler();
        try (var queue = queue(handler, scheduler)) {
            write(queue, scheduler, 0);
        }
        assertEquals(100L, scalar("SELECT count(*) FROM %s.%s.ids".formatted(CATALOG, SCHEMA)));
    }

    @Test
    void baseCarriesTheTablesPartitionProjections() throws Exception {
        execute("ALTER TABLE %s.%s.%s SET PARTITIONED BY (bucket(2, id))".formatted(CATALOG, SCHEMA, TABLE));
        var handler = handler("""
                sidecars = [{ table = "per_bucket", transformation = "SELECT bucket::INTEGER AS bucket, count(*) AS n FROM __base GROUP BY ALL" }]
                """);
        var scheduler = new DeterministicScheduler();
        try (var queue = queue(handler, scheduler)) {
            write(queue, scheduler, 0);
        }
        assertEquals(100L, scalar("SELECT sum(n)::BIGINT FROM %s.%s.per_bucket".formatted(CATALOG, SCHEMA)));
        assertEquals(2L, scalar("SELECT count(*) FROM %s.%s.per_bucket".formatted(CATALOG, SCHEMA)));
    }

    @Test
    void aSidecarFileThatCannotBeRegisteredRollsBackTheBase() throws Exception {
        execute("CREATE TABLE %s.%s.ids (id BIGINT)".formatted(CATALOG, SCHEMA));
        Path base = tempDir.resolve("data").resolve("base.parquet");
        execute("COPY (SELECT 1::BIGINT AS id, 2::BIGINT AS value, 'c' AS category) TO '%s' (FORMAT parquet)".formatted(base));
        var result = new IngestionResult(TABLE, 1L, "app", Map.of(), 1L, List.of(base.toString()), null,
                Map.of("ids", List.of(tempDir.resolve("missing.parquet").toString())));

        var e = assertThrows(FilesNotRegisteredException.class,
                () -> new DuckLakePostIngestionTask(result, CATALOG, TABLE, SCHEMA).execute());
        assertEquals(2, e.files().size(), "the base and sidecar files may all be deleted");
        assertEquals(0L, scalar("SELECT count(*) FROM %s.ducklake_data_file".formatted(METADATA)));
    }

    // -----------------------------------------------------------------------

    /** The handler for one queue on {@link #TABLE}, built from a mapping entry with {@code extra} keys. */
    private DuckLakeIngestionHandler handler(String extra) {
        var provider = new DuckLakeIngestionTaskFactoryProvider();
        provider.setConfig(ConfigFactory.parseString("""
                ingestion_queue_table_mapping = [{
                  ingestion_queue = "%s", catalog = "%s", schema = "%s", table = "%s"
                  %s
                }]
                """.formatted(TABLE, CATALOG, SCHEMA, TABLE, extra)));
        provider.validate();
        return new DuckLakeIngestionHandler(provider.loadMappings(), Duration.ofHours(1));
    }

    private ParquetIngestionQueue queue(IngestionHandler handler, DeterministicScheduler scheduler) {
        return new ParquetIngestionQueue("test-app", "parquet", outputDir.toString(), TABLE,
                1L, Long.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE, Duration.ofSeconds(5),
                handler, scheduler, new MutableClock(Instant.now(), ZoneId.systemDefault()));
    }

    private IngestionResult write(ParquetIngestionQueue queue, DeterministicScheduler scheduler, long batchId) throws Exception {
        // The queue deletes its input file after the write, so hand it a copy.
        Path input = Files.copy(src, tempDir.resolve("in-" + batchId + "-" + System.nanoTime() + ".parquet"));
        var future = queue.add(new Batch<>(null, null, input.toString(), "producer1", batchId,
                Files.size(input), "parquet", Instant.now()));
        scheduler.tick(1, TimeUnit.MILLISECONDS);
        return future.get(30, TimeUnit.SECONDS);
    }

    private List<SidecarTarget.Column> columns(String table) throws SQLException {
        try (var conn = ConnectionPool.getConnection()) {
            return SidecarTables.tableColumns(conn, CATALOG, SCHEMA, table);
        }
    }

    private static void execute(String sql) {
        try (Connection conn = ConnectionPool.getConnection()) {
            ConnectionPool.execute(conn, sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static long scalar(String sql) throws SQLException {
        try (Connection conn = ConnectionPool.getConnection()) {
            return ConnectionPool.collectFirst(conn, sql, Long.class);
        }
    }

    /** Staging directories a write left next to its input files. */
    private List<Path> stagingDirectories() throws Exception {
        try (var files = Files.list(tempDir)) {
            return files.filter(p -> p.getFileName().toString().startsWith("dd_stage_")).toList();
        }
    }

    private static List<Path> parquetFilesUnder(Path dir) throws Exception {
        if (!Files.exists(dir)) {
            return List.of();
        }
        try (var files = Files.walk(dir)) {
            return files.filter(p -> p.toString().endsWith(".parquet")).toList();
        }
    }

    private static boolean hasCause(Throwable t, Class<? extends Throwable> type) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (type.isInstance(c)) return true;
        }
        return false;
    }
}

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
    void aPartitionedSidecarTableIsWrittenWithItsPartitioning() throws Exception {
        execute("CREATE TABLE %s.%s.category_counts (category VARCHAR, n BIGINT)".formatted(CATALOG, SCHEMA));
        execute("ALTER TABLE %s.%s.category_counts SET PARTITIONED BY (category)".formatted(CATALOG, SCHEMA));
        var handler = handler("""
                sidecars = [{ table = "category_counts"
                              transformation = "SELECT category, count(*) AS n FROM __base GROUP BY ALL" }]
                """);
        var scheduler = new DeterministicScheduler();
        IngestionResult result;
        try (var queue = queue(handler, scheduler)) {
            result = write(queue, scheduler, 0);
        }
        List<String> files = result.sidecarFiles().get("category_counts");
        assertEquals(3, files.size(), "one file per partition: " + files);
        assertTrue(files.stream().allMatch(f -> f.contains("/category_counts/category=cat")), files.toString());
        assertEquals(34L, scalar("SELECT n FROM %s.%s.category_counts WHERE category = 'cat0'".formatted(CATALOG, SCHEMA)));

        // A second batch writes new files: it must not overwrite the first batch's at the same partition paths.
        IngestionResult second;
        try (var queue = queue(handler, scheduler)) {
            second = write(queue, scheduler, 1);
        }
        assertTrue(java.util.Collections.disjoint(files, second.sidecarFiles().get("category_counts")),
                files + " vs " + second.sidecarFiles());
        assertEquals(68L, scalar("SELECT sum(n)::BIGINT FROM %s.%s.category_counts WHERE category = 'cat0'".formatted(CATALOG, SCHEMA)),
                "both batches' rows are in the table");
        assertEquals(6L, scalar("SELECT count(*) FROM %s.%s.category_counts".formatted(CATALOG, SCHEMA)));
    }

    @Test
    void aSidecarTablePartitionedByATransformIsWrittenWithIt() throws Exception {
        execute("CREATE TABLE %s.%s.per_value (value BIGINT, n BIGINT)".formatted(CATALOG, SCHEMA));
        execute("ALTER TABLE %s.%s.per_value SET PARTITIONED BY (bucket(2, value))".formatted(CATALOG, SCHEMA));
        var handler = handler("""
                sidecars = [{ table = "per_value", transformation = "SELECT value, count(*) AS n FROM __base GROUP BY ALL" }]
                """);
        var scheduler = new DeterministicScheduler();
        IngestionResult result;
        try (var queue = queue(handler, scheduler)) {
            result = write(queue, scheduler, 0);
        }
        assertTrue(result.sidecarFiles().get("per_value").stream().allMatch(f -> f.contains("/bucket=")),
                result.sidecarFiles().toString());
        assertEquals(100L, scalar("SELECT sum(n)::BIGINT FROM %s.%s.per_value".formatted(CATALOG, SCHEMA)));
    }

    @Test
    void aSidecarTablePartitionedByAColumnItsQueryDoesNotProduceFailsStartup() {
        execute("CREATE TABLE %s.%s.ids (id BIGINT, note VARCHAR)".formatted(CATALOG, SCHEMA));
        execute("ALTER TABLE %s.%s.ids SET PARTITIONED BY (note)".formatted(CATALOG, SCHEMA));
        var e = assertThrows(IllegalArgumentException.class, () -> handler("""
                sidecars = [{ table = "ids", transformation = "SELECT id FROM __base" }]
                """));
        assertTrue(e.getMessage().contains("partitioned by 'note'"), e.getMessage());
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
    void aSidecarTablePartitionedUnderARunningQueueIsWrittenWithItsNewPartitioning() throws Exception {
        var handler = handler("""
                sidecars = [{ table = "cats", transformation = "SELECT id, category FROM __base" }]
                """);
        var scheduler = new DeterministicScheduler();
        try (var queue = queue(handler, scheduler)) {
            write(queue, scheduler, 0);
            execute("ALTER TABLE %s.%s.cats SET PARTITIONED BY (category)".formatted(CATALOG, SCHEMA));
            // The cached state still writes the sidecar unpartitioned: registration rejects it, the
            // catalog-change retry re-reads the partitioning and writes the batch again.
            IngestionResult result = write(queue, scheduler, 1);
            assertTrue(result.sidecarFiles().get("cats").stream().allMatch(f -> f.contains("category=")),
                    result.sidecarFiles().toString());
        }
        assertEquals(200L, scalar("SELECT count(*) FROM %s.%s.cats".formatted(CATALOG, SCHEMA)));
        assertEquals(200L, scalar("SELECT count(*) FROM %s.%s.%s".formatted(CATALOG, SCHEMA, TABLE)));
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

    @Test
    void aSidecarReadsItsQueryFromAViewOverTheQueuesTable() throws Exception {
        execute("CREATE VIEW %1$s.%2$s.v_counts AS SELECT category, count(*) AS n FROM %1$s.%2$s.%3$s GROUP BY ALL"
                .formatted(CATALOG, SCHEMA, TABLE));
        var handler = handler("""
                sidecars = [{ table = "category_counts", transformation_view = "%s.%s.v_counts" }]
                """.formatted(CATALOG, SCHEMA));
        var scheduler = new DeterministicScheduler();
        try (var queue = queue(handler, scheduler)) {
            write(queue, scheduler, 0);
        }
        assertEquals(34L, scalar("SELECT n FROM %s.%s.category_counts WHERE category = 'cat0'".formatted(CATALOG, SCHEMA)),
                "the view's reference to the queue's table became the batch's rows");
    }

    @Test
    void aChangedSidecarViewTakesEffectWithoutARestart() throws Exception {
        execute("CREATE VIEW %1$s.%2$s.v_counts AS SELECT category, count(*) AS n FROM %1$s.%2$s.%3$s GROUP BY ALL"
                .formatted(CATALOG, SCHEMA, TABLE));
        execute("CREATE TABLE %s.%s.category_counts (category VARCHAR, n BIGINT, max_value BIGINT)".formatted(CATALOG, SCHEMA));
        var handler = handler("""
                sidecars = [{ table = "category_counts", transformation_view = "%s.%s.v_counts" }]
                """.formatted(CATALOG, SCHEMA));
        var scheduler = new DeterministicScheduler();
        try (var queue = queue(handler, scheduler)) {
            write(queue, scheduler, 0);
            execute(("CREATE OR REPLACE VIEW %1$s.%2$s.v_counts AS SELECT category, count(*) AS n, max(value) AS max_value "
                    + "FROM %1$s.%2$s.%3$s GROUP BY ALL").formatted(CATALOG, SCHEMA, TABLE));
            handler.refreshNow(TABLE);
            write(queue, scheduler, 1);
        }
        assertEquals(3L, scalar("SELECT count(*) FROM %s.%s.category_counts WHERE max_value IS NULL".formatted(CATALOG, SCHEMA)),
                "the first batch was written with the old view");
        assertEquals(3L, scalar("SELECT count(*) FROM %s.%s.category_counts WHERE max_value IS NOT NULL".formatted(CATALOG, SCHEMA)),
                "the second with the new one");
    }

    @Test
    void aChangedSidecarViewThatNoLongerFitsItsTableIsRejectedAndTheOldQueryKept() throws Exception {
        execute("CREATE VIEW %1$s.%2$s.v_counts AS SELECT category, count(*) AS n FROM %1$s.%2$s.%3$s GROUP BY ALL"
                .formatted(CATALOG, SCHEMA, TABLE));
        var handler = handler("""
                sidecars = [{ table = "category_counts", transformation_view = "%s.%s.v_counts" }]
                """.formatted(CATALOG, SCHEMA));
        // n becomes VARCHAR: the table has it as BIGINT.
        execute("CREATE OR REPLACE VIEW %1$s.%2$s.v_counts AS SELECT category, count(*)::VARCHAR AS n FROM %1$s.%2$s.%3$s GROUP BY ALL"
                .formatted(CATALOG, SCHEMA, TABLE));
        handler.refreshNow(TABLE);
        var scheduler = new DeterministicScheduler();
        try (var queue = queue(handler, scheduler)) {
            assertEquals(100, write(queue, scheduler, 0).rowCount(), "ingestion goes on with the previous query");
        }
        assertEquals(34L, scalar("SELECT n FROM %s.%s.category_counts WHERE category = 'cat0'".formatted(CATALOG, SCHEMA)));
    }

    @Test
    void onlyTheChangedViewThatDoesNotFitIsRejected() throws Exception {
        execute("CREATE VIEW %1$s.%2$s.v_a AS SELECT category, count(*) AS n FROM %1$s.%2$s.%3$s GROUP BY ALL"
                .formatted(CATALOG, SCHEMA, TABLE));
        execute("CREATE VIEW %1$s.%2$s.v_b AS SELECT category, count(*) AS n FROM %1$s.%2$s.%3$s GROUP BY ALL"
                .formatted(CATALOG, SCHEMA, TABLE));
        execute("CREATE TABLE %s.%s.a (category VARCHAR, n BIGINT, max_value BIGINT)".formatted(CATALOG, SCHEMA));
        var handler = handler("""
                sidecars = [{ table = "a", transformation_view = "%1$s.%2$s.v_a" }
                            { table = "b", transformation_view = "%1$s.%2$s.v_b" }]
                """.formatted(CATALOG, SCHEMA));
        // a: a valid change (a column its table has); b: one that does not fit (n as VARCHAR).
        execute(("CREATE OR REPLACE VIEW %1$s.%2$s.v_a AS SELECT category, count(*) AS n, max(value) AS max_value "
                + "FROM %1$s.%2$s.%3$s GROUP BY ALL").formatted(CATALOG, SCHEMA, TABLE));
        execute("CREATE OR REPLACE VIEW %1$s.%2$s.v_b AS SELECT category, count(*)::VARCHAR AS n FROM %1$s.%2$s.%3$s GROUP BY ALL"
                .formatted(CATALOG, SCHEMA, TABLE));
        handler.refreshNow(TABLE);
        var scheduler = new DeterministicScheduler();
        try (var queue = queue(handler, scheduler)) {
            write(queue, scheduler, 0);
        }
        assertEquals(3L, scalar("SELECT count(*) FROM %s.%s.a WHERE max_value IS NOT NULL".formatted(CATALOG, SCHEMA)),
                "a's valid change is applied");
        assertEquals(34L, scalar("SELECT n FROM %s.%s.b WHERE category = 'cat0'".formatted(CATALOG, SCHEMA)),
                "b keeps its previous query");
    }

    @Test
    void aRegistryChangeKeepsTheQueryOfARejectedView() throws Exception {
        execute("CREATE VIEW %1$s.%2$s.v_counts AS SELECT category, count(*) AS n FROM %1$s.%2$s.%3$s GROUP BY ALL"
                .formatted(CATALOG, SCHEMA, TABLE));
        var viewSidecar = new Sidecar("category_counts", null, CATALOG + "." + SCHEMA + ".v_counts", null);
        var mapping = new QueueIdToTableMapping(TABLE, CATALOG, SCHEMA, TABLE, Map.of(), null).withSidecars(List.of(viewSidecar));
        var handler = new DuckLakeIngestionHandler(Map.of(TABLE, mapping), Duration.ofHours(1));
        execute("CREATE OR REPLACE VIEW %1$s.%2$s.v_counts AS SELECT category, count(*)::VARCHAR AS n FROM %1$s.%2$s.%3$s GROUP BY ALL"
                .formatted(CATALOG, SCHEMA, TABLE));
        handler.refreshNow(TABLE); // rejected: the previous query is kept
        // An unrelated change to the queue's mapping, as a registry reload brings: another sidecar.
        handler.updateMappings(Map.of(TABLE, mapping.withSidecars(List.of(viewSidecar,
                new Sidecar("ids", "SELECT id FROM __base")))));
        var scheduler = new DeterministicScheduler();
        try (var queue = queue(handler, scheduler)) {
            assertEquals(100, write(queue, scheduler, 0).rowCount(), "the rejected view still does not stop ingestion");
        }
        assertEquals(34L, scalar("SELECT n FROM %s.%s.category_counts WHERE category = 'cat0'".formatted(CATALOG, SCHEMA)));
        assertEquals(100L, scalar("SELECT count(*) FROM %s.%s.ids".formatted(CATALOG, SCHEMA)), "the new sidecar is written");
    }

    @Test
    void aRejectedViewQueryIsCheckedAgainOnceTheViewWasChangedInBetween() throws Exception {
        // The views live in the in-memory catalog, so changing them does not move the lake's
        // schema_version: every refresh here takes the fast path.
        String good = "CREATE OR REPLACE VIEW memory.main.v_sc AS SELECT category, count(*) AS n FROM %s.%s.%s GROUP BY ALL"
                .formatted(CATALOG, SCHEMA, TABLE);
        String needsDim = ("CREATE OR REPLACE VIEW memory.main.v_sc AS SELECT category, count(*) AS n FROM %s.%s.%s "
                + "WHERE category IN (SELECT category FROM memory.main.dim_sc) GROUP BY ALL").formatted(CATALOG, SCHEMA, TABLE);
        try {
            execute(good);
            var handler = handler("""
                    sidecars = [{ table = "category_counts", transformation_view = "memory.main.v_sc", input_table = "%s.%s.%s" }]
                    """.formatted(CATALOG, SCHEMA, TABLE));
            execute("CREATE OR REPLACE TABLE memory.main.dim_sc AS SELECT 'cat0' AS category");
            execute(needsDim);
            execute("DROP TABLE memory.main.dim_sc");
            handler.refreshNow(TABLE);  // rejected: dim_sc is missing, so the query does not run
            execute(good);
            handler.refreshNow(TABLE);  // reverted
            execute("CREATE OR REPLACE TABLE memory.main.dim_sc AS SELECT 'cat0' AS category");
            execute(needsDim);
            handler.refreshNow(TABLE);  // the same query as the rejected one, but it now runs
            var scheduler = new DeterministicScheduler();
            try (var queue = queue(handler, scheduler)) {
                write(queue, scheduler, 0);
            }
            assertEquals(1L, scalar("SELECT count(*) FROM %s.%s.category_counts".formatted(CATALOG, SCHEMA)),
                    "the view's query was applied: only cat0");
        } finally {
            execute("DROP VIEW IF EXISTS memory.main.v_sc");
            execute("DROP TABLE IF EXISTS memory.main.dim_sc");
        }
    }

    @Test
    void aViewThatDoesNotRunYetIsAppliedOnceItDoes() throws Exception {
        String needsDim = ("CREATE OR REPLACE VIEW memory.main.v_dim AS SELECT category, count(*) AS n FROM %s.%s.%s "
                + "WHERE category IN (SELECT category FROM memory.main.dim_later) GROUP BY ALL").formatted(CATALOG, SCHEMA, TABLE);
        try {
            execute("CREATE OR REPLACE VIEW memory.main.v_dim AS SELECT category, count(*) AS n FROM %s.%s.%s GROUP BY ALL"
                    .formatted(CATALOG, SCHEMA, TABLE));
            // A refresh interval of zero: every read of the queue's state is a periodic refresh.
            var handler = handler("""
                    sidecars = [{ table = "category_counts", transformation_view = "memory.main.v_dim", input_table = "%s.%s.%s" }]
                    """.formatted(CATALOG, SCHEMA, TABLE), Duration.ZERO);
            execute("CREATE OR REPLACE TABLE memory.main.dim_later AS SELECT 'cat0' AS category");
            execute(needsDim);
            execute("DROP TABLE memory.main.dim_later");
            handler.refreshNow(TABLE);   // does not run: the old query is kept, rejected but not permanently
            handler.refreshNow(TABLE);   // a forced refresh (after a failed batch, say) does not check it again
            execute("CREATE OR REPLACE TABLE memory.main.dim_later AS SELECT 'cat0' AS category");
            // The view is unchanged, but it runs now: the next periodic refresh, on the write, applies it.
            var scheduler = new DeterministicScheduler();
            try (var queue = queue(handler, scheduler)) {
                write(queue, scheduler, 0);
            }
            assertEquals(1L, scalar("SELECT count(*) FROM %s.%s.category_counts".formatted(CATALOG, SCHEMA)),
                    "the view's query is applied once its table exists: only cat0");
        } finally {
            execute("DROP VIEW IF EXISTS memory.main.v_dim");
            execute("DROP TABLE IF EXISTS memory.main.dim_later");
        }
    }

    @Test
    void aMismatchCausedByATableInAnotherCatalogIsCheckedAgainOnceThatTableIsFixed() throws Exception {
        try {
            execute("CREATE OR REPLACE TABLE memory.main.dim_type AS SELECT 'x'::VARCHAR AS n");
            execute("CREATE OR REPLACE VIEW memory.main.v_t AS SELECT category, count(*) AS n FROM %s.%s.%s GROUP BY ALL"
                    .formatted(CATALOG, SCHEMA, TABLE));
            var handler = handler("""
                    sidecars = [{ table = "category_counts", transformation_view = "memory.main.v_t", input_table = "%s.%s.%s" }]
                    """.formatted(CATALOG, SCHEMA, TABLE), Duration.ZERO);
            // n now comes from a table in another catalog, as VARCHAR: it does not fit the BIGINT column.
            execute(("CREATE OR REPLACE VIEW memory.main.v_t AS SELECT category, (SELECT max(n) FROM memory.main.dim_type) AS n "
                    + "FROM %s.%s.%s GROUP BY ALL").formatted(CATALOG, SCHEMA, TABLE));
            handler.refreshNow(TABLE);   // rejected, but not permanently: the cause is outside the lake
            execute("CREATE OR REPLACE TABLE memory.main.dim_type AS SELECT 7::BIGINT AS n");   // fixed there
            var scheduler = new DeterministicScheduler();
            try (var queue = queue(handler, scheduler)) {
                write(queue, scheduler, 0);   // a periodic refresh checks the view again and applies it
            }
            assertEquals(3L, scalar("SELECT count(*) FROM %s.%s.category_counts WHERE n = 7".formatted(CATALOG, SCHEMA)));
        } finally {
            execute("DROP VIEW IF EXISTS memory.main.v_t");
            execute("DROP TABLE IF EXISTS memory.main.dim_type");
        }
    }

    @Test
    void forcedRefreshesDoNotPostponeTheRecheckOfARejectedView() throws Exception {
        String needsDim = ("CREATE OR REPLACE VIEW memory.main.v_pend AS SELECT category, count(*) AS n FROM %s.%s.%s "
                + "WHERE category IN (SELECT category FROM memory.main.dim_pend) GROUP BY ALL").formatted(CATALOG, SCHEMA, TABLE);
        try {
            execute("CREATE OR REPLACE VIEW memory.main.v_pend AS SELECT category, count(*) AS n FROM %s.%s.%s GROUP BY ALL"
                    .formatted(CATALOG, SCHEMA, TABLE));
            var clock = new MutableClock(Instant.now(), ZoneId.systemDefault());
            var mapping = new QueueIdToTableMapping(TABLE, CATALOG, SCHEMA, TABLE, Map.of(), null)
                    .withSidecars(List.of(new Sidecar("category_counts", null, "memory.main.v_pend", CATALOG + "." + SCHEMA + "." + TABLE)));
            var handler = new DuckLakeIngestionHandler(Map.of(TABLE, mapping), Duration.ofMinutes(1), clock);
            execute("CREATE OR REPLACE TABLE memory.main.dim_pend AS SELECT 'cat0' AS category");
            execute(needsDim);
            execute("DROP TABLE memory.main.dim_pend"); // the view's query does not run: rejected
            handler.refreshNow(TABLE);
            execute("CREATE OR REPLACE TABLE memory.main.dim_pend AS SELECT 'cat0' AS category");
            clock.advanceBy(Duration.ofSeconds(40));
            handler.refreshNow(TABLE);   // a forced refresh (a failed batch): skips the re-check, must not reset the timer
            clock.advanceBy(Duration.ofSeconds(30));   // 70 s since the last periodic refresh: one is due
            var scheduler = new DeterministicScheduler();
            try (var queue = queue(handler, scheduler)) {
                write(queue, scheduler, 0);
            }
            assertEquals(1L, scalar("SELECT count(*) FROM %s.%s.category_counts".formatted(CATALOG, SCHEMA)),
                    "the periodic refresh came round and applied the view: only cat0");
        } finally {
            execute("DROP VIEW IF EXISTS memory.main.v_pend");
            execute("DROP TABLE IF EXISTS memory.main.dim_pend");
        }
    }

    @Test
    void aBriefQueueErrorDoesNotTakeAwayARejectedViewsLastGoodQuery() throws Exception {
        String queueView = "CREATE OR REPLACE VIEW memory.main.v_q AS SELECT id, value, category FROM %s.%s.events_in"
                .formatted(CATALOG, SCHEMA);
        try {
            execute("CREATE TABLE %s.%s.events_in (id BIGINT, value BIGINT, category VARCHAR)".formatted(CATALOG, SCHEMA));
            execute(queueView);
            execute("CREATE OR REPLACE VIEW memory.main.v_sc AS SELECT category, count(*) AS n FROM %s.%s.%s GROUP BY ALL"
                    .formatted(CATALOG, SCHEMA, TABLE));
            var mapping = new QueueIdToTableMapping(TABLE, CATALOG, SCHEMA, TABLE, Map.of(), null,
                    "memory.main.v_q", CATALOG + "." + SCHEMA + ".events_in")
                    .withSidecars(List.of(new Sidecar("category_counts", null, "memory.main.v_sc", CATALOG + "." + SCHEMA + "." + TABLE)));
            var handler = new DuckLakeIngestionHandler(Map.of(TABLE, mapping), Duration.ofHours(1));
            execute("CREATE OR REPLACE VIEW memory.main.v_sc AS SELECT category, count(*)::VARCHAR AS n FROM %s.%s.%s GROUP BY ALL"
                    .formatted(CATALOG, SCHEMA, TABLE));
            handler.refreshNow(TABLE);                // the sidecar view is rejected; its old query kept
            execute("DROP VIEW memory.main.v_q");
            handler.refreshNow(TABLE);                // the queue's own view is gone: the queue fails
            execute(queueView);
            handler.refreshNow(TABLE);                // back: the sidecar falls back on its last good query
            var scheduler = new DeterministicScheduler();
            try (var queue = queue(handler, scheduler)) {
                assertEquals(100, write(queue, scheduler, 0).rowCount());
            }
            assertEquals(34L, scalar("SELECT n FROM %s.%s.category_counts WHERE category = 'cat0'".formatted(CATALOG, SCHEMA)));
        } finally {
            execute("DROP VIEW IF EXISTS memory.main.v_sc");
            execute("DROP VIEW IF EXISTS memory.main.v_q");
        }
    }

    @Test
    void mainTableAndSidecarChangesTogetherAreAllLoaded() throws Exception {
        execute("CREATE VIEW %1$s.%2$s.v_b AS SELECT category, count(*) AS n FROM %1$s.%2$s.%3$s GROUP BY ALL"
                .formatted(CATALOG, SCHEMA, TABLE));
        var handler = handler("""
                sidecars = [
                  { table = "a", transformation = "SELECT id, category FROM __base" }
                  { table = "b", transformation_view = "%1$s.%2$s.v_b" }
                  { table = "c", transformation = "SELECT category, sum(value)::BIGINT AS total FROM __base GROUP BY ALL" }
                ]
                """.formatted(CATALOG, SCHEMA));
        var scheduler = new DeterministicScheduler();
        IngestionResult second;
        try (var queue = queue(handler, scheduler)) {
            write(queue, scheduler, 0);

            // All at once, under the running queue:
            execute("ALTER TABLE %s.%s.%s SET PARTITIONED BY (category)".formatted(CATALOG, SCHEMA, TABLE)); // main table
            execute("ALTER TABLE %s.%s.a ADD COLUMN note VARCHAR".formatted(CATALOG, SCHEMA));               // a: a new column
            execute("ALTER TABLE %s.%s.b ADD COLUMN max_value BIGINT".formatted(CATALOG, SCHEMA));           // b: table first,
            execute(("CREATE OR REPLACE VIEW %1$s.%2$s.v_b AS SELECT category, count(*) AS n, max(value) AS max_value "
                    + "FROM %1$s.%2$s.%3$s GROUP BY ALL").formatted(CATALOG, SCHEMA, TABLE));                   //    then its view
            execute("ALTER TABLE %s.%s.c SET PARTITIONED BY (category)".formatted(CATALOG, SCHEMA));         // c: partitioned

            // The cached state is stale: the batch is written the old way, rejected at registration,
            // and retried once with everything re-read.
            second = write(queue, scheduler, 1);
        }
        assertTrue(second.filesCreated().stream().allMatch(f -> f.contains("category=")),
                "base: written with the new partitioning " + second.filesCreated());
        assertTrue(second.sidecarFiles().get("c").stream().allMatch(f -> f.contains("/c/category=")),
                "c: written with its new partitioning " + second.sidecarFiles());
        assertEquals(200L, scalar("SELECT count(*) FROM %s.%s.%s".formatted(CATALOG, SCHEMA, TABLE)));
        assertEquals(200L, scalar("SELECT count(*) FROM %s.%s.a WHERE note IS NULL".formatted(CATALOG, SCHEMA)),
                "a: both batches, the new column NULL");
        assertEquals(3L, scalar("SELECT count(*) FROM %s.%s.b WHERE max_value IS NULL".formatted(CATALOG, SCHEMA)),
                "b: the first batch with the old view");
        assertEquals(3L, scalar("SELECT count(*) FROM %s.%s.b WHERE max_value IS NOT NULL".formatted(CATALOG, SCHEMA)),
                "b: the second with the new one");
        assertEquals(6732L, scalar("SELECT sum(total)::BIGINT FROM %s.%s.c WHERE category = 'cat0'".formatted(CATALOG, SCHEMA)),
                "c: both batches' totals");
    }

    @Test
    void aDynamicQueuesMainTableAndRegistryChangesTogetherAreAllLoaded() throws Exception {
        execute("CREATE VIEW %1$s.%2$s.v_b AS SELECT category, count(*) AS n FROM %1$s.%2$s.%3$s GROUP BY ALL"
                .formatted(CATALOG, SCHEMA, TABLE));
        String dbPath = tempDir.resolve("registry-combined.db").toString();
        try (var repo = new DynamicQueueRepository(dbPath)) {
            repo.init();
            registry(dbPath, "INSERT INTO reg.ingestion_queues (ingestion_queue, catalog, schema_name, table_name) "
                    + "VALUES ('%s', '%s', '%s', '%s')".formatted(TABLE, CATALOG, SCHEMA, TABLE));
            registry(dbPath, "INSERT INTO reg.%s (ingestion_queue, sidecar_table, transformation_view) VALUES ('%s', 'b', '%s.%s.v_b')"
                    .formatted(DynamicQueueRepository.SIDECARS_TABLE, TABLE, CATALOG, SCHEMA));
            Connection readConn = repo.openReadOnlyConnection();
            var handler = new DynamicIngestionHandler(dbPath, readConn, DynamicQueueRepository.loadAll(readConn),
                    Duration.ofHours(1));
            try {
                var scheduler = new DeterministicScheduler();
                IngestionResult second;
                try (var queue = queue(handler, scheduler)) {
                    write(queue, scheduler, 0);

                    // The main table re-partitioned, b's view changed, and a new sidecar in the registry:
                    execute("ALTER TABLE %s.%s.%s SET PARTITIONED BY (category)".formatted(CATALOG, SCHEMA, TABLE));
                    execute("ALTER TABLE %s.%s.b ADD COLUMN max_value BIGINT".formatted(CATALOG, SCHEMA));
                    execute(("CREATE OR REPLACE VIEW %1$s.%2$s.v_b AS SELECT category, count(*) AS n, max(value) AS max_value "
                            + "FROM %1$s.%2$s.%3$s GROUP BY ALL").formatted(CATALOG, SCHEMA, TABLE));
                    registry(dbPath, "INSERT INTO reg.%s (ingestion_queue, sidecar_table, transformation) VALUES ('%s', 'ids', "
                            .formatted(DynamicQueueRepository.SIDECARS_TABLE, TABLE) + "'SELECT id FROM __base')");
                    handler.updateMappings(DynamicQueueRepository.loadAll(readConn, handler.currentMappings()));

                    second = write(queue, scheduler, 1);
                }
                assertTrue(second.filesCreated().stream().allMatch(f -> f.contains("category=")), second.filesCreated().toString());
                assertEquals(java.util.Set.of("b", "ids"), second.sidecarFiles().keySet());
            } finally {
                handler.closeQueues();
            }
        }
        assertEquals(200L, scalar("SELECT count(*) FROM %s.%s.%s".formatted(CATALOG, SCHEMA, TABLE)));
        assertEquals(3L, scalar("SELECT count(*) FROM %s.%s.b WHERE max_value IS NOT NULL".formatted(CATALOG, SCHEMA)),
                "b: the second batch with its new view");
        assertEquals(100L, scalar("SELECT count(*) FROM %s.%s.ids".formatted(CATALOG, SCHEMA)), "the new sidecar: the second batch");
    }

    private static void registry(String dbPath, String sql) throws Exception {
        try (Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:");
             var st = conn.createStatement()) {
            st.execute("LOAD sqlite");
            st.execute("ATTACH '%s' AS reg (TYPE sqlite)".formatted(dbPath));
            st.execute(sql);
        }
    }

    @Test
    void aSidecarViewOverAnEmptyTemplateViewOfTheQueuesTableIsWrittenFromTheBatch() throws Exception {
        // A guard against costly direct queries on a sidecar view: the view reads an empty template
        // view of the queue's table, and names it as its input_table. Queried directly it returns
        // nothing without reading the table; as a sidecar, the template reference becomes __base, so
        // the template's WHERE false never reaches the sidecar's query.
        execute("CREATE VIEW %1$s.%2$s.events_shape AS SELECT * FROM %1$s.%2$s.%3$s WHERE false".formatted(CATALOG, SCHEMA, TABLE));
        execute("CREATE VIEW %1$s.%2$s.v_shape_counts AS SELECT category, count(*) AS n FROM %1$s.%2$s.events_shape GROUP BY ALL"
                .formatted(CATALOG, SCHEMA));
        var handler = handler("""
                sidecars = [{ table = "category_counts", transformation_view = "%1$s.%2$s.v_shape_counts", input_table = "%1$s.%2$s.events_shape" }]
                """.formatted(CATALOG, SCHEMA));
        var scheduler = new DeterministicScheduler();
        try (var queue = queue(handler, scheduler)) {
            write(queue, scheduler, 0);
        }
        assertEquals(0L, scalar("SELECT count(*) FROM %s.%s.v_shape_counts".formatted(CATALOG, SCHEMA)),
                "queried directly, the view returns nothing although the table now has rows");
        assertEquals(34L, scalar("SELECT n FROM %s.%s.category_counts WHERE category = 'cat0'".formatted(CATALOG, SCHEMA)),
                "as a sidecar it is computed from the batch");
    }

    @Test
    void aViewChangedToABodyThatIsNotASidecarQueryIsRejectedAndTheOldQueryKept() throws Exception {
        execute("CREATE VIEW %1$s.%2$s.v_counts AS SELECT category, count(*) AS n FROM %1$s.%2$s.%3$s GROUP BY ALL"
                .formatted(CATALOG, SCHEMA, TABLE));
        var handler = handler("""
                sidecars = [{ table = "category_counts", transformation_view = "%s.%s.v_counts" }]
                """.formatted(CATALOG, SCHEMA));
        // A self-join: the queue's table is read twice, so the body cannot become a query over __base.
        execute(("CREATE OR REPLACE VIEW %1$s.%2$s.v_counts AS SELECT a.category, count(*) AS n FROM %1$s.%2$s.%3$s a "
                + "JOIN %1$s.%2$s.%3$s b ON a.id = b.id GROUP BY ALL").formatted(CATALOG, SCHEMA, TABLE));
        handler.refreshNow(TABLE);
        var scheduler = new DeterministicScheduler();
        try (var queue = queue(handler, scheduler)) {
            assertEquals(100, write(queue, scheduler, 0).rowCount(), "ingestion goes on with the previous query");
        }
        assertEquals(34L, scalar("SELECT n FROM %s.%s.category_counts WHERE category = 'cat0'".formatted(CATALOG, SCHEMA)));
    }

    @Test
    void aSidecarTableWithAColumnNamedLikeItsPartitionKeyFailsStartup() {
        // day(ts) partitions under the key "day"; a COPY cannot also write a column of that name.
        execute("CREATE TABLE %s.%s.per_day (ts TIMESTAMP, day BIGINT, n BIGINT)".formatted(CATALOG, SCHEMA));
        execute("ALTER TABLE %s.%s.per_day SET PARTITIONED BY (day(ts))".formatted(CATALOG, SCHEMA));
        var e = assertThrows(IllegalArgumentException.class, () -> handler("""
                sidecars = [{ table = "per_day", transformation = "SELECT TIMESTAMP '2026-01-01' AS ts, id AS day, value AS n FROM __base" }]
                """));
        assertTrue(e.getMessage().contains("column named 'day'"), e.getMessage());
    }

    @Test
    void aSidecarViewThatCannotBeReReadKeepsItsLastQuery() throws Exception {
        execute("CREATE VIEW %1$s.%2$s.v_counts AS SELECT category, count(*) AS n FROM %1$s.%2$s.%3$s GROUP BY ALL"
                .formatted(CATALOG, SCHEMA, TABLE));
        var handler = handler("""
                sidecars = [{ table = "category_counts", transformation_view = "%s.%s.v_counts" }]
                """.formatted(CATALOG, SCHEMA));
        execute("DROP VIEW %s.%s.v_counts".formatted(CATALOG, SCHEMA));
        handler.refreshNow(TABLE);
        var scheduler = new DeterministicScheduler();
        try (var queue = queue(handler, scheduler)) {
            assertEquals(100, write(queue, scheduler, 0).rowCount());
        }
        assertEquals(34L, scalar("SELECT n FROM %s.%s.category_counts WHERE category = 'cat0'".formatted(CATALOG, SCHEMA)));
    }

    @Test
    void aSidecarViewThatDoesNotReadItsInputTableFailsStartup() {
        execute("CREATE TABLE %s.%s.other (id BIGINT)".formatted(CATALOG, SCHEMA));
        execute("CREATE VIEW %1$s.%2$s.v_other AS SELECT id FROM %1$s.%2$s.other".formatted(CATALOG, SCHEMA));
        var e = assertThrows(IllegalArgumentException.class, () -> handler("""
                sidecars = [{ table = "ids", transformation_view = "%s.%s.v_other" }]
                """.formatted(CATALOG, SCHEMA)));
        assertTrue(e.getMessage().contains("v_other") && e.getMessage().contains("not found in view body"), e.getMessage());
    }

    @Test
    void aDynamicQueuesSidecarsAreCreatedAndWritten() throws Exception {
        String dbPath = tempDir.resolve("registry.db").toString();
        try (var repo = new DynamicQueueRepository(dbPath)) {
            repo.init();
            try (Connection conn = java.sql.DriverManager.getConnection("jdbc:duckdb:");
                 var st = conn.createStatement()) {
                st.execute("LOAD sqlite");
                st.execute("ATTACH '%s' AS reg (TYPE sqlite)".formatted(dbPath));
                st.execute("INSERT INTO reg.ingestion_queues (ingestion_queue, catalog, schema_name, table_name) "
                        + "VALUES ('%s', '%s', '%s', '%s')".formatted(TABLE, CATALOG, SCHEMA, TABLE));
                st.execute("INSERT INTO reg.%s (ingestion_queue, sidecar_table, transformation) VALUES ('%s', "
                        .formatted(DynamicQueueRepository.SIDECARS_TABLE, TABLE)
                        + "'category_counts', 'SELECT category, count(*) AS n FROM __base GROUP BY ALL')");
            }
            Connection readConn = repo.openReadOnlyConnection();
            var handler = new DynamicIngestionHandler(dbPath, readConn, DynamicQueueRepository.loadAll(readConn),
                    Duration.ofHours(1));
            try {
                var scheduler = new DeterministicScheduler();
                try (var queue = queue(handler, scheduler)) {
                    write(queue, scheduler, 0);
                }
            } finally {
                handler.closeQueues();
            }
        }
        assertEquals(34L, scalar("SELECT n FROM %s.%s.category_counts WHERE category = 'cat0'".formatted(CATALOG, SCHEMA)),
                "the dynamic queue's sidecar table was created and written");
    }

    // -----------------------------------------------------------------------

    /** The handler for one queue on {@link #TABLE}, built from a mapping entry with {@code extra} keys. */
    private DuckLakeIngestionHandler handler(String extra) {
        return handler(extra, Duration.ofHours(1));
    }

    private DuckLakeIngestionHandler handler(String extra, Duration refreshInterval) {
        var provider = new DuckLakeIngestionTaskFactoryProvider();
        provider.setConfig(ConfigFactory.parseString("""
                ingestion_queue_table_mapping = [{
                  ingestion_queue = "%s", catalog = "%s", schema = "%s", table = "%s"
                  %s
                }]
                """.formatted(TABLE, CATALOG, SCHEMA, TABLE, extra)));
        provider.validate();
        return new DuckLakeIngestionHandler(provider.loadMappings(), refreshInterval);
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

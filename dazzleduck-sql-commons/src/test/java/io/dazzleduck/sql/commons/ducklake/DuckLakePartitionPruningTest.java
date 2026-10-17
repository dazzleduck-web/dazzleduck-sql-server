package io.dazzleduck.sql.commons.ducklake;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.dazzleduck.sql.commons.ConnectionPool;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;


public class DuckLakePartitionPruningTest {

    // Expected file counts after partition pruning.
    // DuckDB 1.5.2+ stores INSERTs as inlined data; ducklake_flush_inlined_data materialises them into
    // parquet files. With inlining off (see setup) each of the 7 inserts writes its own file, so
    // there is nothing to collapse: 7 from inserts + 2 manual = 9. They were previously inlined and
    // then flushed, which grouped them by partition into 4.
    private static final int EXPECTED_FILES_WITH_FILTER = 2; // Files matching key = 'k52' filter
    private static final int EXPECTED_FILES_NO_FILTER = 9;

    @TempDir
    static Path WORKSPACE;

    public static final String DATABASE = "my_ducklake";
    public static final String METADATA_DATABASE = "__ducklake_metadata_%s".formatted(DATABASE);
    public static final String PARTITIONED_TABLE = "tt_p";
    public static final String NON_PARTITIONED_TABLE = "tt";
    public static final String QUALIFIED_PARTITIONED_TABLE = "%s.%s".formatted(DATABASE, PARTITIONED_TABLE);
    public static final String QUALIFIED_NON_PARTITIONED_TABLE = "%s.%s".formatted(DATABASE, NON_PARTITIONED_TABLE);

    private static String settingBefore;

    @AfterAll
    public static void tearDown() {
        if (settingBefore != null) {
            ConnectionPool.execute("SET GLOBAL ducklake_default_data_inlining_row_limit = " + settingBefore);
        }
    }

    @BeforeAll
    public static void setup() throws Exception {
        String workspacePath = WORKSPACE.toString();
        ConnectionPool.executeBatch(new String[]{
                "INSTALL ducklake",
                "LOAD ducklake",
                "ATTACH 'ducklake:%s/metadata' AS %s (DATA_PATH '%s/data')".formatted(workspacePath, DATABASE, workspacePath),
        });
        // Pruning reads ducklake_data_file, so every row has to be IN a file. Inlining keeps small
        // inserts in the catalog instead, and its limit belongs to the whole DuckDB instance —
        // which other tests in this JVM also move — so this fixture states what it needs rather
        // than inheriting whatever ran before it.
        // Remembered and restored in tearDown: setting it and leaving it is the same pollution in
        // the other direction, changing how many files later tests' inserts produce.
        settingBefore = ConnectionPool.collectFirst(
                "SELECT current_setting('ducklake_default_data_inlining_row_limit')::VARCHAR", String.class);
        ConnectionPool.execute("SET GLOBAL ducklake_default_data_inlining_row_limit = 0");
        DuckLakeTestFixture.createTestTable(QUALIFIED_PARTITIONED_TABLE, true);
        DuckLakeTestFixture.addDataFile(DATABASE, workspacePath + "/data/main/" + PARTITIONED_TABLE, PARTITIONED_TABLE, true);
        DuckLakeTestFixture.createTestTable(QUALIFIED_NON_PARTITIONED_TABLE, false);
        DuckLakeTestFixture.addDataFile(DATABASE, workspacePath + "/data/main/" + NON_PARTITIONED_TABLE, NON_PARTITIONED_TABLE, false);
        // A no-op now that inlining is off above, but kept: it is still correct if anything lands
        // inlined, and pruning can only see rows that are in a file.
        ConnectionPool.execute("CALL ducklake_flush_inlined_data('" + DATABASE + "')");
    }

    @Test
    public void testTransformation() throws SQLException, JsonProcessingException {
        var sql = "select * from %s where key = 'k52'".formatted(PARTITIONED_TABLE);
        var pruning = new DucklakePartitionPruning(METADATA_DATABASE);
        var files = pruning.pruneFiles("main", PARTITIONED_TABLE, sql);
        Assertions.assertEquals(EXPECTED_FILES_WITH_FILTER, files.size(), "Expected 2 files matching key = 'k52' filter");
    }

    @Test
    public void testTransformationNoFilter() throws SQLException, JsonProcessingException {
        var sql = "select * from %s".formatted(PARTITIONED_TABLE);
        var pruning = new DucklakePartitionPruning(METADATA_DATABASE);
        var files = pruning.pruneFiles("main", PARTITIONED_TABLE, sql);
        Assertions.assertEquals(EXPECTED_FILES_NO_FILTER, files.size(), "Expected all 9 files without filter");
    }

    @Test
    public void testTransformationFakeFilter() throws SQLException, JsonProcessingException {
        var sql = "select * from %s where true".formatted(PARTITIONED_TABLE);
        var pruning = new DucklakePartitionPruning(METADATA_DATABASE);
        var files = pruning.pruneFiles("main", PARTITIONED_TABLE, sql);
        Assertions.assertEquals(EXPECTED_FILES_NO_FILTER, files.size(), "Expected all 9 files for 'where true' filter");
    }

    @Test
    public void testTransformationNoConstantsFilter() throws SQLException, JsonProcessingException {
        var sql = "select * from %s where key = value and key = 'k52'".formatted(PARTITIONED_TABLE);
        var pruning = new DucklakePartitionPruning(METADATA_DATABASE);
        var files = pruning.pruneFiles("main", PARTITIONED_TABLE, sql);
        Assertions.assertEquals(EXPECTED_FILES_WITH_FILTER, files.size(), "Expected 2 files for complex filter with key = 'k52'");
    }

    @Test
    public void testNonExistentTable() {
        var sql = "select * from non_existent_table where key = 'k52'";
        var pruning = new DucklakePartitionPruning(METADATA_DATABASE);
        Assertions.assertThrows(SQLException.class, () -> pruning.pruneFiles("main", "non_existent_table", sql), "Should throw SQLException for non-existent table");
    }

    @Test
    public void testInvalidSQL() {
        var sql = "this is not valid SQL";
        var pruning = new DucklakePartitionPruning(METADATA_DATABASE);
        Assertions.assertThrows(Exception.class, () -> pruning.pruneFiles("main", PARTITIONED_TABLE, sql), "Should throw exception for invalid SQL syntax");
    }
}

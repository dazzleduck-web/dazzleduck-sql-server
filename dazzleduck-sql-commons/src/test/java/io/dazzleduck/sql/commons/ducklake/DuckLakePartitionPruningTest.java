package io.dazzleduck.sql.commons.ducklake;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.FileStatus;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.SQLException;


public class DuckLakePartitionPruningTest {

    // Expected file counts after partition pruning
    private static final int EXPECTED_FILES_WITH_FILTER = 2; // Files matching key = 'k52' filter
    private static final int EXPECTED_FILES_NO_FILTER = 8; // Total files without any filter

    @TempDir
    static Path WORKSPACE;

    public static final String DATABASE = "my_ducklake";
    public static final String METADATA_DATABASE = "__ducklake_metadata_%s".formatted(DATABASE);
    public static final String PARTITIONED_TABLE = "tt_p";
    public static final String NON_PARTITIONED_TABLE = "tt";
    public static final String QUALIFIED_PARTITIONED_TABLE = "%s.%s".formatted(DATABASE, PARTITIONED_TABLE);
    public static final String QUALIFIED_NON_PARTITIONED_TABLE = "%s.%s".formatted(DATABASE, NON_PARTITIONED_TABLE);

    @BeforeAll
    public static void setup() {
        String workspacePath = WORKSPACE.toString();
        String[] setups = {
                "INSTALL ducklake",
                "LOAD ducklake",
                "ATTACH 'ducklake:%s/metadata' AS %s (DATA_PATH '%s/data')".formatted(workspacePath, DATABASE, workspacePath),
        };
        ConnectionPool.executeBatch(setups);
        createTestTable(QUALIFIED_PARTITIONED_TABLE, true);
        addDataFile(PARTITIONED_TABLE);
        createTestTable(QUALIFIED_NON_PARTITIONED_TABLE, false);
        addDataFile(NON_PARTITIONED_TABLE);
    }

    private static void addDataFile(String table) {
        String path = WORKSPACE.toString() + "/data/main/" + table + "/manual.parquet";
        String sql = "COPY (SELECT * FROM (VALUES('k119', 'v119', 9),('k118', 'v118', 8)) AS my_value(key, value, partition)) TO '%s' (FORMAT parquet)".formatted(path);
        String addTableSql = "CALL ducklake_add_data_files('%s', '%s', '%s', allow_missing => true)".formatted(DATABASE, table, path);
        String[] batch = {sql, addTableSql};
        ConnectionPool.executeBatch(batch);
    }


    public static void createTestTable(String table , boolean partitioned) {
        String[] statements = getStatements(table, partitioned);
        for(var statement : statements) {
            ConnectionPool.execute(statement);
        }
    }

    @NotNull
    private static String[] getStatements(String table, boolean partition) {
        var createStatement = "CREATE TABLE %s(key string, value string, partition int)".formatted( table);
        var insertStatement0 = "INSERT INTO %s VALUES ('k00', 'v00', 0), ('k00', 'v00', 0) ".formatted(table);
        var partitionTable = partition ? "ALTER TABLE %s SET PARTITIONED BY (partition)".formatted(table)
                : "select 1";
        var insertStatement1 = "INSERT INTO %s VALUES (null, null, 1)".formatted(table);
        var insertStatement2 = "INSERT INTO %s VALUES ('k11', 'v11', 1), ('k21', 'v21', 1) ".formatted(table);
        var insertStatement3 = "INSERT INTO %s VALUES ('k31', 'v31', 1), ('k41', 'v41', 1) ".formatted(table);
        var insertStatement4 = "INSERT INTO %s VALUES ('k51', 'v51', 1), ('k61', 'v61', 1) ".formatted(table);
        var addColumn = "ALTER TABLE %s ADD COLUMN version INT".formatted(table);
        var insertStatement5 = "INSERT INTO %s VALUES ('k51', 'v51', 1, 0 ), ('k61', 'v61', 1, 0) ".formatted(table);
        var insertStatement6 = "INSERT INTO %s VALUES ('k72', 'v72', 2, 0 ), ('k82', 'v82', 2, 0) ".formatted(table);
        return new String[]{
                createStatement,
                insertStatement0,
                partitionTable,
                insertStatement1,
                insertStatement2,
                insertStatement3,
                insertStatement4,
                addColumn,
                insertStatement5,
                insertStatement6,
        };
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
        Assertions.assertEquals(EXPECTED_FILES_NO_FILTER, files.size(), "Expected all 8 files without filter");
    }

    @Test
    public void testTransformationFakeFilter() throws SQLException, JsonProcessingException {
        var sql = "select * from %s where true".formatted(PARTITIONED_TABLE);
        var pruning = new DucklakePartitionPruning(METADATA_DATABASE);
        var files = pruning.pruneFiles("main", PARTITIONED_TABLE, sql);
        Assertions.assertEquals(EXPECTED_FILES_NO_FILTER, files.size(), "Expected all 8 files for 'where true' filter");
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

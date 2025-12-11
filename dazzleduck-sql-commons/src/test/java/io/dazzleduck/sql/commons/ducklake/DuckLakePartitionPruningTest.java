package io.dazzleduck.sql.commons.ducklake;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.dazzleduck.sql.commons.ConnectionPool;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.List;

import static io.dazzleduck.sql.commons.ducklake.DucklakePartitionPruning.constructQuery;
import static org.junit.Assert.assertEquals;

public class DuckLakePartitionPruningTest {

    public static String WORKSPACE;
    public static final String DATABASE = "test_ducklake_partition";
    public static final String METADATA_DATABASE = "__ducklake_metadata_%s".formatted(DATABASE);
    public static final String FILE_COLUMN_STAT_TABLE = "%s.ducklake_file_column_stats".formatted(METADATA_DATABASE);
    public static final String PARTITIONED_TABLE = "tt_p";
    public static final String NO_PARTITIONED_TABLE = "tt";
    public static final String QUALIFIED_PARTITIONED_TABLE = "%s.%s".formatted(DATABASE, PARTITIONED_TABLE);
    public static final String QUALIFIED_NO_PARTITIONED_TABLE = "%s.%s".formatted(DATABASE, NO_PARTITIONED_TABLE);
    public static final String COLUMN_MAPPING_TABLE = "ducklake_column_mapping";
    public static final String QUALIFIED_COLUMN_MAPPING_TABLE = "%s.%s".formatted(METADATA_DATABASE, COLUMN_MAPPING_TABLE);

    static {
        try {
            WORKSPACE = Files.createTempDirectory("my-temp-dir-").toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeAll
    public static void setup() {
        String[] setups = {
                "INSTALL ducklake",
                "LOAD ducklake",
                "ATTACH 'ducklake:%s/metadata' AS %s (DATA_PATH '%s/data')".formatted(WORKSPACE, DATABASE, WORKSPACE),
        };
        ConnectionPool.executeBatch(setups);
        createTestTable(QUALIFIED_PARTITIONED_TABLE, true);
        addDataFile(PARTITIONED_TABLE);
        createTestTable(QUALIFIED_NO_PARTITIONED_TABLE, false);
        addDataFile(NO_PARTITIONED_TABLE);
    }

    private static void addDataFile(String table) {
        String path = WORKSPACE + "/data/main/" + table + "/manual.parquet";
        String sql = "COPY (SELECT * FROM (VALUES('k119', 'v119', 9),('k118', 'v118', 8)) AS my_value(key, value, partition)) TO '%s' (FORMAT parquet)".formatted(path);
        String addTableSql = "CALL ducklake_add_data_files('%s', '%s', '%s', allow_missing => true)".formatted(DATABASE, table, path);
        String[] batch = {sql, addTableSql};
        ConnectionPool.executeBatch(batch);
    }

    @Test
    @Disabled
    public void testPartitioned() {
        String  tableName = "";
        var tableId = getTableId(tableName);
        String query = constructQuery(METADATA_DATABASE, 1, List.of(1L, 2L, 3L, 4L));
        ConnectionPool.printResult(query);
    }

    private Object getTableId(String tableName) {
        return null;
    }

    @Test
    public void testUnPartitioned() {


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
    public void testTransformation() throws SQLException, JsonProcessingException, NoSuchMethodException {
        var sql = "select * from %s where key = 'k52'".formatted(PARTITIONED_TABLE);
        var files = DucklakePartitionPruning.pruneFiles("main", PARTITIONED_TABLE, sql, METADATA_DATABASE );
        Assertions.assertEquals(2, files.size());
    }



    public static void createNonPartitionedTable() {
        // create table
        // add rows with nulls
        // add column
        // add rows with few null
        // add external parquet file
    }

    public static void createPartitionedTable() {
        // create table
        // add rows with nulls
        // add column
        // add rows with few null
        // add external parquet file
    }
}

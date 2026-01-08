package io.dazzleduck.sql.commons.planner;

import com.fasterxml.jackson.databind.JsonNode;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.Transformations;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import static io.dazzleduck.sql.commons.util.TestConstants.*;

public class SplitPlannerTest {
    @Test
    public void testSplitHive() throws SQLException, IOException {
        var splits = SplitPlanner.getSplitTreeAndSize(Transformations.parseToTree(SUPPORTED_HIVE_PATH_QUERY), 1024 * 1024 * 1024);
        Assertions.assertEquals(1, splits.size());
        Assertions.assertEquals(762, splits.get(0).size());
    }

    @Test
    public void testSplitDelta() throws SQLException, IOException {
        var splits = SplitPlanner.getSplitTreeAndSize(Transformations.parseToTree(SUPPORTED_DELTA_PATH_QUERY),
                1024 * 1024 * 1024);
        Assertions.assertEquals(1, splits.size());
        Assertions.assertEquals(5378, splits.get(0).size());
    }

    @BeforeAll
    static void attachDuckLake() throws Exception {
        try (Connection c = ConnectionPool.getConnection()) {
            c.createStatement().execute("""
            ATTACH 'ducklake:my_data.ducklake' AS my_data
            (DATA_PATH 'target/test-data/ducklake')
        """);
        }
    }

    @BeforeAll
    static void setupDuckLakeTable() throws Exception {
        try (Connection c = ConnectionPool.getConnection()) {

            c.createStatement().execute("""
                    CREATE SCHEMA IF NOT EXISTS my_data.my_schema
                    """);

            c.createStatement().execute("""
                    CREATE TABLE IF NOT EXISTS my_data.my_schema.my_table ( key VARCHAR, value VARCHAR, p INT )
                    """);

            c.createStatement().execute("""
                    ALTER TABLE my_data.my_schema.my_table SET PARTITIONED BY (p);
                    """);

            c.createStatement().execute("""
                        INSERT INTO my_data.my_schema.my_table VALUES
                        ('k1','v1',1),
                        ('k2','v2',2),
                        ('k3','v1',1)
                    """);
        }
    }


    @Test
    public void testDuckLakePlannerSelection() throws Exception {

        var table = new Transformations.CatalogSchemaTable(
                "my_data",
                "my_schema",
                "my_table",
                Transformations.TableType.BASE_TABLE
        );

        PartitionPrunerV2 planner = PartitionPrunerV2.getPlannerForTable(table);

        Assertions.assertTrue(planner instanceof DucklakeSplitPlanner);
    }
    @Test
    public void testDuckLakeSplitPlanner() throws Exception {

        String sql = "SELECT * FROM my_data.my_schema.my_table WHERE p = 1";
        JsonNode tree = Transformations.parseToTree(sql);
        var splits = SplitPlanner.getSplitTreeAndSize(tree, 1024L * 1024 * 1024);

        Assertions.assertEquals(1, splits.size());

    }
    @Test
    public void testDuckLakePartitionPruning() throws Exception {

        var allFiles = SplitPlanner.getSplitTreeAndSize(Transformations.parseToTree("SELECT * FROM my_data.my_schema.my_table"), Long.MAX_VALUE).get(0);
        var prunedFiles = SplitPlanner.getSplitTreeAndSize(Transformations.parseToTree("SELECT * FROM my_data.my_schema.my_table WHERE p = 1"), Long.MAX_VALUE).get(0);
        System.out.println(prunedFiles.size());
        System.out.println(allFiles.size());
        Assertions.assertTrue(prunedFiles.size() <allFiles.size());
    }
    @AfterAll
    static void cleanupDuckLake() throws Exception {
        try {
            ConnectionPool.execute("DETACH my_data");
        } catch (Exception ignored) {
            // Detach may already be done or fail silently
        }

        deleteRecursively(new File("target/test-data/ducklake"));
    }
    private static void deleteRecursively(File f) {
        if (f == null || !f.exists()) return;

        if (f.isDirectory()) {
            File[] files = f.listFiles();
            if (files != null) {
                for (File c : files) {
                    deleteRecursively(c);
                }
            }
        }

        if (!f.delete()) {
            System.err.println("Failed to delete: " + f.getAbsolutePath());
        }
    }

}

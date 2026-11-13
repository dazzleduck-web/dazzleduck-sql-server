package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** How a queue's {@code sidecars} are read from its mapping entry, and what is rejected there. */
class SidecarConfigTest {

    private static Map<String, QueueIdToTableMapping> load(String sidecars) {
        var provider = new DuckLakeIngestionTaskFactoryProvider();
        provider.setConfig(ConfigFactory.parseString("""
                ingestion_queue_table_mapping = [{
                  ingestion_queue = "q1", catalog = "lake", schema = "main", table = "logs"
                  sidecars = %s
                }]
                """.formatted(sidecars)));
        return provider.loadMappings();
    }

    private static String rejected(String sidecars) {
        return assertThrows(IllegalArgumentException.class, () -> load(sidecars)).getMessage();
    }

    @Test
    void sidecarsAreReadInOrder() {
        var mapping = load("""
                [ { table = "a", transformation = "SELECT 1 AS x FROM __base" }
                  { table = "b", transformation = "WITH t AS (SELECT * FROM __base) SELECT count(*) AS n FROM t" } ]
                """).get("q1");
        assertEquals(List.of(new Sidecar("a", "SELECT 1 AS x FROM __base"),
                        new Sidecar("b", "WITH t AS (SELECT * FROM __base) SELECT count(*) AS n FROM t")),
                mapping.sidecars());
    }

    @Test
    void aQueueWithoutSidecarsHasNone() {
        var provider = new DuckLakeIngestionTaskFactoryProvider();
        provider.setConfig(ConfigFactory.parseString("""
                ingestion_queue_table_mapping = [{ ingestion_queue = "q1", catalog = "lake", schema = "main", table = "logs" }]
                """));
        assertEquals(List.of(), provider.loadMappings().get("q1").sidecars());
    }

    @Test
    void anUnusableEntryIsRejectedNamingTheQueue() {
        String message = rejected("[ { table = \"a\", transformation = \"SELECT 1 FROM __base\", transformation_view = \"lake.main.v\" } ]");
        assertTrue(message.contains("q1") && message.contains("exactly one of"), message);

        message = rejected("[ { table = \"a\", transformation_view = \"main.v\" } ]");
        assertTrue(message.contains("fully qualified"), message);

        message = rejected("[ { table = \"a\", transformation = \"SELECT 1 FROM __base\", input_table = \"lake.main.t\" } ]");
        assertTrue(message.contains("'input_table' goes with 'transformation_view'"), message);

        message = rejected("[ { table = \"a\", transformation = \"SELECT 1 FROM __base\", mode = \"x\" } ]");
        assertTrue(message.contains("unknown sidecar key 'mode'"), message);

        message = rejected("[ { transformation = \"SELECT 1 FROM __base\" } ]");
        assertTrue(message.contains("q1") && message.contains("'table'"), message);

        message = rejected("[ { table = \"a\" } ]");
        assertTrue(message.contains("sidecar 'a' needs exactly one of"), message);
    }

    @Test
    void aSidecarMayReadItsQueryFromAView() {
        var sidecars = load("""
                [ { table = "a", transformation_view = "lake.main.v_a" }
                  { table = "b", transformation_view = "lake.main.v_b", input_table = "lake.main.raw" } ]
                """).get("q1").sidecars();
        assertEquals(new Sidecar("a", null, "lake.main.v_a", null), sidecars.get(0));
        assertEquals(new Sidecar("b", null, "lake.main.v_b", "lake.main.raw"), sidecars.get(1));
    }

    @Test
    void aQueryThatDoesNotReadBaseIsRejected() {
        String message = rejected("[ { table = \"a\", transformation = \"SELECT * FROM lake.main.logs\" } ]");
        assertTrue(message.contains("must read '__base'"), message);

        message = rejected("[ { table = \"a\", transformation = \"SELECT 1 FROM __base; SELECT 2 FROM __base\" } ]");
        assertTrue(message.contains("single statement"), message);

        message = rejected("[ { table = \"a\", transformation = \"WITH __base AS (SELECT * FROM lake.main.other) SELECT * FROM __base\" } ]");
        assertTrue(message.contains("must read '__base'"), "a CTE of that name hides the batch's rows: " + message);
    }

    @Test
    void baseIsMatchedInAnyCase() {
        assertEquals(1, load("[ { table = \"a\", transformation = \"SELECT count(*) AS n FROM __BASE\" } ]")
                .get("q1").sidecars().size());
    }

    @Test
    void tablesMustBeDistinctAndNotTheQueuesOwn() {
        String message = rejected("""
                [ { table = "a", transformation = "SELECT 1 AS x FROM __base" }
                  { table = "A", transformation = "SELECT 2 AS x FROM __base" } ]
                """);
        assertTrue(message.contains("declared more than once"), message);

        message = rejected("[ { table = \"logs\", transformation = \"SELECT 1 AS x FROM __base\" } ]");
        assertTrue(message.contains("the queue's own table"), message);
    }
}

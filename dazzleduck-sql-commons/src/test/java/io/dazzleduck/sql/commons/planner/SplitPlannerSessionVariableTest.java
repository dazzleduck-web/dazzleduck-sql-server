package io.dazzleduck.sql.commons.planner;

import io.dazzleduck.sql.commons.Transformations;
import io.dazzleduck.sql.commons.authorization.SessionVariables;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import static io.dazzleduck.sql.commons.util.TestConstants.SUPPORTED_HIVE_PATH_QUERY;

/**
 * Split planning prunes partitions on connections it opens itself, and the tree it prunes with
 * already carries the injected row-level-security filter. A filter referencing
 * {@code getvariable('d')} therefore evaluates against NULL — pruning away every file — unless the
 * request's session variables are applied to those connections too.
 */
public class SplitPlannerSessionVariableTest {

    /** example/data/hive_table has dt=2024-01-01 (1 file) and dt=2025-01-01 (2 files). */
    private static final String QUERY_FILTERED_BY_VARIABLE =
            SUPPORTED_HIVE_PATH_QUERY + " WHERE dt = getvariable('d')::DATE";

    @Test
    public void sessionVariableIsAppliedWhenPruningPartitions() throws SQLException, IOException {
        var setupSqls = SessionVariables.toSetStatements("{\"d\":\"2025-01-01\"}");
        Assertions.assertEquals(List.of("SET VARIABLE \"d\" = '2025-01-01'"), setupSqls);

        var statuses = SplitPlanner.getSplitStatus(
                Transformations.parseToTree(QUERY_FILTERED_BY_VARIABLE), 1024 * 1024 * 1024, setupSqls);

        var files = statuses.stream().flatMap(List::stream).toList();
        Assertions.assertEquals(2, files.size(),
                "expected the two dt=2025-01-01 files; 0 means the filter was evaluated with the "
                        + "variable unset (getvariable -> NULL) and every file was pruned");
        Assertions.assertTrue(files.stream().allMatch(f -> f.fileName().contains("dt=2025-01-01")), files.toString());
    }

    @Test
    public void withoutTheSessionVariableEveryFileIsPruned() throws SQLException, IOException {
        // Documents the failure mode the parameter exists to prevent: same query, no variables.
        var statuses = SplitPlanner.getSplitStatus(
                Transformations.parseToTree(QUERY_FILTERED_BY_VARIABLE), 1024 * 1024 * 1024, List.of());

        Assertions.assertEquals(0, statuses.stream().flatMap(List::stream).count());
    }
}

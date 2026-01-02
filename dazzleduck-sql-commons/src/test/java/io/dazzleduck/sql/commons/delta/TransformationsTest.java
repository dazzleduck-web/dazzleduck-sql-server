package io.dazzleduck.sql.commons.delta;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.delta.kernel.expressions.Expression;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;

public class TransformationsTest {
    @Test
    public void testTransformation() throws SQLException, IOException {
//        String sql = "select a, b from t where x = 10 or y = 15";
        String sql = "select a, b from t where y = cast('2024-01-01' as DATE)";
        JsonNode tree = io.dazzleduck.sql.commons.Transformations.parseToTree(sql);
        ArrayNode statements = (ArrayNode)tree.get("statements");
        JsonNode firstStatement =  statements.get(0);
        JsonNode whereClause = firstStatement.get("node").get("where_clause");
        Assertions.assertNotNull(whereClause);
        Expression predicate = Transformations.toDeltaPredicate(whereClause);
        Assertions.assertNotNull(predicate);
    }
}

package io.dazzleduck.sql.commons.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.dazzleduck.sql.commons.AstNodes;
import io.dazzleduck.sql.commons.Transformations;
import io.dazzleduck.sql.common.types.DataType;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

public class SchemaUtils {

    /**
     *
     * @param sql
     * @return
     * @throws SQLException
     * @throws JsonProcessingException
     * build the schema based on reference cast. for example select cast( a as int), cast(b as varchar) from t where cast(c.x as bigint) = 10  group by  cast( g as int).
     * Since 'a' is casted to int and b is casted to varchar and c.x is casted as bigint
     * the schema will be ( int a, varchar b, c struct(x bigint))
     */
    public static DataType.Struct buildSchema(String sql) throws SQLException, JsonProcessingException {
        JsonNode tree = Transformations.parseToTree(sql);
        if(tree.get("error").asBoolean()) {
            throw new SQLException(tree.get("error_message").asText());
        }
        DataType.Struct result = new DataType.Struct();
        JsonNode firstStatementNode = tree.get("statements").get(0).get("node");
        ArrayNode selectList = (ArrayNode) firstStatementNode.get("select_list");
        JsonNode whereClause = firstStatementNode.get("where_clause");
        ArrayNode groupExpression = (ArrayNode) firstStatementNode.get("group_expressions");
        processReferences(result, selectList);
        processReferences(result, whereClause);
        processReferences(result, groupExpression);
        return result;
    }


    private static void processReferences(DataType.Struct result, JsonNode node){
        if(node == null){
            return;
        }
        List<JsonNode> referencesWithCast = Transformations.collectReferencesWithCast(node);
        List<JsonNode> references = Transformations.collectReferences(node);
        buildSchemaWithCast(result, referencesWithCast);
        if(references.size() != referencesWithCast.size()) {
            buildSchemaNoCast(result, references);
        }
    }

    private static void buildSchemaWithCast(DataType.Struct result, List<JsonNode> referencesWithCast) {
        for(JsonNode node : referencesWithCast) {
            processReferenceWithCast(result, node);
        }
    }

    private static void buildSchemaNoCast(DataType.Struct result, List<JsonNode> references) {
        for(JsonNode node : references) {
            processReference(result, node);
        }
    }

    private static void processReferenceWithCast(DataType.Struct result, JsonNode node) {
        String[] names = getColumnNames(node.get("child"));
        DataType type = DataType.getDataType(AstNodes.castType(node));
        result.add(names, type);
    }

    private static void processReference(DataType.Struct result, JsonNode node) {
        String[] names = getColumnNames(node);
        result.add(names, DataType.NULL);
    }

    private static String[] getColumnNames(JsonNode jsonNode) {
        ArrayNode arrayNode = (ArrayNode)jsonNode.get("column_names");
        Iterator<JsonNode> itr = arrayNode.elements();
        String[] resultArray = new String[arrayNode.size()];
        int index = 0;
        while (itr.hasNext()) {
            JsonNode node = itr.next();
            resultArray[index++] = node.asText();
        }
        return resultArray;
    }
}


package io.dazzleduck.sql.commons;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.dazzleduck.sql.commons.ExpressionConstants.*;
import static io.dazzleduck.sql.commons.ExpressionFactory.createFunction;

public class TransformationTests {

    @Test
    public void testSplitStatements() throws SQLException, JsonProcessingException {
        String sql = " select * from generate_series(10); \n" +
                "select * from generate_series(11);";
        JsonNode node = Transformations.parseToTree(sql);
        List<JsonNode> statements = Transformations.splitStatements(node);
        for(JsonNode n : statements) {
            String s = Transformations.parseToSql(n);
            ConnectionPool.execute(s);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"select * from t where x in (select y from t2) "})
    public void testSubQueries(String sql) throws SQLException, JsonProcessingException {
        var jsonNode = Transformations.parseToTree(sql);
        var statements = (ArrayNode) jsonNode.get("statements");
        var statement = statements.get(0);
        var statementNode = statement.get("node");
        var where = statementNode.get("where_clause");
        var qs = Transformations.collectSubQueries(where );
        Assertions.assertEquals(1, qs.size());
    }

    @Test
    public void getCast() throws SQLException, JsonProcessingException {
        var schema = "a int, b string, c STRUCT(i  int, d STRUCT( x int)), e Int[], f Map(string, string), g decimal(18,3)";
        var sql = Transformations.getCast(schema);
        ConnectionPool.printResult(sql);
    }

    @Test
    public void getPartitionSchema() throws SQLException, JsonProcessingException {
        var query = "select * from read_parquet('abc', hive_types = {a : INT, b : STRING})";
        var hivePartition = Transformations.getHivePartition(Transformations.parseToTree(query));
        Assertions.assertEquals(2, hivePartition.length);
    }


    @Test
    void testChangeMatching() throws SQLException, JsonProcessingException {
        var input = "select * from read_parquet('file.parquet') where search( x, 't1', 't2', 't3') and y = 'abc'";
        var expected = "select * from read_parquet('file.parquet') where row_num in ( select row_num from read_parquet('index.parquet') where t in ('t1', 't2', 't3')) and y = 'abc'";
        var inputTree = Transformations.parseToTree(input);
        var copy = inputTree.deepCopy();
        var selectQueryTemplate = Transformations.identity().andThen(Transformations::getFirstStatementNode)
                .apply(Transformations.parseToTree("select row_num from read_parquet('a') where t in ('t',  't2')"));
        Function<String, String> amalyzer = x -> x ;
        var fileMapping = Map.of("file.parquet", "index.parquet");
        Function<String, String> fileMapppingFunction = fileMapping::get;
        var inputSelect = Transformations.identity().andThen(Transformations::getFirstStatementNode).apply(inputTree);
        var function = Transformations.getAllTablesOrPathsFromSelect(inputSelect, null, null);
        var file =  function.get(0).tableOrPath();
        var indexFile = fileMapppingFunction.apply(file);
        Consumer<JsonNode> changeFunction =  n -> {
            var node = (ObjectNode)n;
            var children = node.get("children");
            var columnRef = Transformations.getReferenceName(children.get(0));
            var tokens = new ArrayList<String>();
            for( int i = 1; i <  children.size(); i ++) {
                String constant = Transformations.getConstant(children.get(i));
                tokens.add(amalyzer.apply(constant));
            }
            node.removeAll();
            var newNode = new ObjectNode(JsonNodeFactory.instance);
            var subQuery = buildSubQuery(selectQueryTemplate, indexFile,  columnRef[0], tokens );
            newNode.set("node", subQuery);
            node.put("type", SUBQUERY_TYPE);
            node.put("class", SUBQUERY_CLASS);
            node.put("subquery_type", "ANY");
            node.set("subquery", newNode);
            node.set("child", ExpressionFactory.reference(new String[]  {"row_num"}));
            node.put("comparison_type", COMPARE_TYPE_EQUAL);
        };

        Transformations.identity()
                .andThen(Transformations::getFirstStatementNode)
                .andThen( x -> x.get("where_clause"))
                        .andThen(Transformations.changeMatching(Transformations.isFunction("search"),  changeFunction))
                                .apply(copy);

        var toAnalyze = Transformations.parseToTree(expected);
        System.out.println("correct ---> " + getSubQuery(toAnalyze));
        System.out.println("result  ---> " + getSubQuery(copy));
        System.out.println(Transformations.parseToSql(copy));
        System.out.println(Transformations.parseToSql(toAnalyze));
    }

    JsonNode getSubQuery(JsonNode tree) {
        var where = Transformations.identity().andThen(Transformations::getFirstStatementNode).apply(tree).get("where_clause");
        var c = where.get("children");
        return c.get(0).get("subquery");
    }

    public static JsonNode buildSubQuery(JsonNode template,
                                         String indexFile,
                                         String field, List<String> tokens) {
        // Change IN clause
        var where = template.get("where_clause");
        Transformations.changeMatching(Transformations.IS_COMPARE_IN, node -> {
            var children = (ArrayNode)node.get("children");
            children.removeAll();
            var mappedField = field;
            assert mappedField != null;
            children.add(ExpressionFactory.reference(new String[]{mappedField}));
            for(var c : tokens) {
                children.add(ExpressionFactory.constant(c));
            }
        })
                .apply(where);
        var select = template.get("from_table");

        // Change the parquet function
        Transformations.changeMatching(Transformations.isTableFunction("read_parquet"), n -> {
            var function = n.get("function");
            var children = (ArrayNode) function.get("children");
            children.remove(0);
            children.add(ExpressionFactory.constant(indexFile));
        }).apply(select);
        return template;
    }
}

package io.dazzleduck.sql.commons;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.dazzleduck.sql.commons.ExpressionConstants.*;

public class Transformations {

    public enum TableType {
        TABLE_FUNCTION, BASE_TABLE
    }
    public record CatalogSchemaTable(String catalog, String schema, String tableOrPath, TableType type, String functionName) {
        public CatalogSchemaTable(String catalog, String schema, String tableOrPath, TableType type) {
            this(catalog, schema, tableOrPath, type, null);
        }
    }
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static final String JSON_SERIALIZE_SQL = "SELECT  cast(json_serialize_sql('%s') as string)";

    public static final String JSON_DESERIALIZE_SQL = "SELECT json_deserialize_sql( cast('%s' as json))";

    public static final Function<JsonNode, Boolean> IS_CONSTANT = isClassAndType(CONSTANT_CLASS,
            CONSTANT_TYPE);
    public static final Function<JsonNode, Boolean> IS_REFERENCE = isClassAndType(COLUMN_REF_CLASS,
            COLUMN_REF_TYPE);
    public static final Function<JsonNode, Boolean> IS_CONJUNCTION_AND = isClassAndType(CONJUNCTION_CLASS,
            CONJUNCTION_TYPE_AND);

    public static final Function<JsonNode, Boolean> IS_CONJUNCTION_OR = isClassAndType(CONJUNCTION_CLASS,
            CONJUNCTION_TYPE_OR);

    public static final Function<JsonNode, Boolean> IS_CAST = isClassAndType(CAST_CLASS, CAST_TYPE_OPERATOR);

    public static final Function<JsonNode, Boolean> IS_EMPTY_CONJUNCTION_AND = n -> {
        if (!IS_CONJUNCTION_AND.apply(n)) {
            return false;
        }
        JsonNode children = n.get("children");
        return children == null || children.isEmpty();
    };

    public static final Function<JsonNode, Boolean> IS_REFERENCE_CAST = node -> {
        return IS_CAST.apply(node) && node.get("child") !=null && IS_REFERENCE.apply(node.get("child"));
    };

    public static final Function<JsonNode, Boolean> IS_SELECT = isType(SELECT_NODE_TYPE);

    public static final Function<JsonNode, Boolean> IS_COMPARISON = isClass(COMPARISON_CLASS);

    public static final Function<JsonNode, Boolean> IS_SUBQUERY = isType(SUBQUERY_TYPE);

    /**
     * Set string to ?, Integer to -1 and Decimal to -1.0
     */
    public static final Function<JsonNode, JsonNode> REPLACE_CONSTANT = node -> {
        JsonNode result = node.deepCopy();
        ObjectNode valueNode = (ObjectNode) result.get("value");
        ObjectNode type = (ObjectNode) valueNode.get("type");
        String id = type.get("id").asText();
        if (id.equals("VARCHAR")) {
            valueNode.put("value", "?");
        } else if (id.equals("INTEGER")) {
            valueNode.put("value", -1);
        } else if (id.equals("DECIMAL")) {
            valueNode.put("value", -1);
            ObjectNode typeInfo = (ObjectNode) type.get("type_info").deepCopy();
            typeInfo.put("width", 1);
            typeInfo.put("scale", 1);
            valueNode.set("type_info", typeInfo);
        }
        return result;
    };

    public static Function<JsonNode, Boolean> isClassAndType(String clazz, String type) {
        return node -> {
            JsonNode classNode = node.get("class");
            JsonNode typeNode = node.get("type");
            return classNode != null && typeNode != null && classNode.asText().equals(clazz)
                    && typeNode.asText().equals(type);
        };
    }

    public static Function<JsonNode, Boolean> isClass(String clazz) {
        return node -> {
            JsonNode classNode = node.get("class");
            return classNode != null && classNode.asText().equals(clazz);
        };
    }

    public static Function<JsonNode, Boolean> isType(String type) {
        return node -> {
            JsonNode typeNode = node.get("type");
            return typeNode != null && typeNode.asText().equals(type);
        };
    }

    public static Function<JsonNode, JsonNode> replaceEqualMinMaxFromComparison(Map<String, String> minMapping,
                                                                                Map<String, String> maxMapping,
                                                                                Map<String, String> dataTypeMap) {
        return n -> {
            JsonNode node = n.deepCopy();
            List<JsonNode> references = collectReferences(node);
            if (references.isEmpty()) {
                return ExpressionFactory.trueExpression();
            }

            List<List<String>> columnNames = collectColumnNames(references);
            boolean failed = false;
            for (List<String> columnName : columnNames) {
                if (columnName.size() != 1) {
                    failed = true;
                    break;
                }
                if (!minMapping.containsKey(columnName.get(0))) {
                    failed = true;
                    break;
                }
            }
            List<JsonNode> literalObjects = collectLiterals(node);
            if (literalObjects.size() != 1 || references.size() != 1) {
                failed = true;
            }
            if (failed) {
                return ExpressionFactory.trueExpression();
            }
            JsonNode literalObject = literalObjects.get(0);
            JsonNode refObject = references.get(0);
            String[] col = getReferenceName(refObject);
            var upperBound = isUpperBound(node);
            var lowerBound = isLowerBound(node);
            if(upperBound && lowerBound) {
                String minC = minMapping.get(col[0]);
                String maxC = maxMapping.get(col[0]);
                return constructEqualPredicate( new String[]{minC}, new String[]{maxC}, literalObject, dataTypeMap.get(col[0]));
            }
            if (upperBound) {
                String minC = minMapping.get(col[0]);
                return constructUpperBoundPredicate(new String[]{minC}, literalObject, dataTypeMap.get(col[0]));
            }
            if (lowerBound) {
                String maxC = maxMapping.get(col[0]);
                return constructLowerBoundPredicate(new String[]{maxC}, literalObject, dataTypeMap.get(col[0]));
            }
            return ExpressionFactory.trueExpression();
        };
    }

    public static Function<JsonNode, JsonNode> replaceEqualMinMaxFromAndConjunction(Map<String, String> minMapping,
                                                                                    Map<String, String> maxMapping,
                                                                                    Map<String, String> dataTypeMap) {
        return n -> {
            JsonNode copyNode = n.deepCopy();
            Iterator<JsonNode> it = copyNode.get("children").iterator();
            ArrayNode newFilter = new ArrayNode(new JsonNodeFactory(false));
            Function<JsonNode, JsonNode> replaceRule = replaceEqualMinMaxFromComparison(minMapping, maxMapping, dataTypeMap);
            while (it.hasNext()) {
                JsonNode node = it.next();
                JsonNode newNode = replaceRule.apply(node);
                if (newNode != null) {
                    newFilter.add(newNode);
                }
            }
            ((ObjectNode) copyNode).set("children", newFilter);
            return copyNode;
        };
    }


    public static Function<JsonNode, JsonNode> removeNonPartitionColumnsPredicatesInQuery(Set<String> partitionColumns) {
        return n -> {
            ObjectNode c = n.deepCopy();
            JsonNode where = n.get("where_clause");
            if (where == null || where instanceof NullNode) {
                return c;
            }
            if (IS_CONJUNCTION_AND.apply(where)) {
                JsonNode w = transform(where, Transformations.IS_CONJUNCTION_AND,
                        removeNonPartitionColumnsPredicatesFromAndConjunction(partitionColumns));
                c.set("where_clause", w);
            } else if (IS_COMPARISON.apply(where)) {
                JsonNode w = transform(where, IS_COMPARISON,
                        removeNonPartitionColumnsPredicatesFromComparison(partitionColumns));
                c.set("where_clause", w);
            } else {
                c.set("where_clause", null);
            }
            return c;
        };
    }

    public static Function<JsonNode, JsonNode> replaceEqualMinMaxInQuery(String statTable,
                                                                         Map<String, String> minMapping,
                                                                         Map<String, String> maxMapping,
                                                                         Map<String, String> dataTypeMap) {
        return n -> {
            ObjectNode query = n.deepCopy();
            var firstStatementNode = (ObjectNode) Transformations.getFirstStatementNode(query);
            ObjectNode from_table = (ObjectNode) firstStatementNode.get("from_table");
            from_table.put("table_name", statTable);
            JsonNode where = firstStatementNode.get("where_clause");
            if (where == null || where instanceof NullNode) {
                return query;
            }
            if (IS_CONJUNCTION_AND.apply(where)) {
                JsonNode w = Transformations.transform(where, IS_CONJUNCTION_AND, replaceEqualMinMaxFromAndConjunction(minMapping, maxMapping, dataTypeMap));
                firstStatementNode.set("where_clause", w);
            } else if (IS_COMPARISON.apply(where)) {
                JsonNode w = Transformations.transform(where, IS_COMPARISON, replaceEqualMinMaxFromComparison(minMapping, maxMapping, dataTypeMap));
                firstStatementNode.set("where_clause", w);
            } else {
                firstStatementNode.set("where_clause", null);
            }
            return query;
        };
    }

    public static String[] getReferenceName(JsonNode node) {
        ArrayNode array = (ArrayNode) node.get("column_names");
        String[] res = new String[array.size()];
        for (int i = 0; i < res.length; i++) {
            res[i] = array.get(i).asText();
        }
        return res;
    }

    /**
     * a &lt; 10
     * a &lt;= 10
     * 10 &gt; a
     * 10 &gt;=a
     * a = 10
     * 10 = a
     */
    public static boolean isUpperBound(JsonNode node) {
        JsonNode left = node.get("left");
        JsonNode right = node.get("right");
        String clazz = node.get("class").asText();
        String type = node.get("type").asText();
        return (IS_REFERENCE.apply(left) && IS_CONSTANT.apply(right) && clazz.equals(COMPARISON_CLASS)
                && (type.equals(COMPARE_TYPE_LESSTHAN) || type.equals(COMPARE_TYPE_LESSTHANOREQUALTO) || type.equals(COMPARE_TYPE_EQUAL))
                || IS_REFERENCE.apply(right) && IS_CONSTANT.apply(left) && clazz.equals(COMPARISON_CLASS)
                && (type.equals(COMPARE_TYPE_GREATERTHAN) || type.equals(COMPARE_TYPE_GREATERTHANOREQUALTO) || type.equals(COMPARE_TYPE_EQUAL)));
    }


    /**
     * a &gt; 10
     * a &gt;= 10
     * 10 &lt; a
     * 10 &lt;= a
     * a = 10
     * 10 = a
     */
    public static boolean isLowerBound(JsonNode node) {
        JsonNode left = node.get("left");
        JsonNode right = node.get("right");
        String clazz = node.get("class").asText();
        String type = node.get("type").asText();
        return (IS_REFERENCE.apply(left) && IS_CONSTANT.apply(right) && clazz.equals(COMPARISON_CLASS)
                && (type.equals(COMPARE_TYPE_GREATERTHAN) || type.equals(COMPARE_TYPE_GREATERTHANOREQUALTO) || type.equals(COMPARE_TYPE_EQUAL))
                || IS_REFERENCE.apply(right) && IS_CONSTANT.apply(left) && clazz.equals(COMPARISON_CLASS)
                && (type.equals(COMPARE_TYPE_LESSTHAN) || type.equals(COMPARE_TYPE_LESSTHANOREQUALTO) || type.equals(COMPARE_TYPE_EQUAL)));
    }


    /**
     *  if(max_a=null, true, cast(min_a as int) &lt;= cast(x as int)
     **/
    public static JsonNode constructUpperBoundPredicate(String[] minCol, JsonNode literal, String datatype) {
        JsonNode nullNode = ExpressionFactory.constant(null);
        JsonNode referenceNode = ExpressionFactory.reference(minCol);
        JsonNode ifCondition = ExpressionFactory.equalExpr(referenceNode, nullNode);
        JsonNode then = ExpressionFactory.cast(ExpressionFactory.constant("t"), "BOOLEAN");
        JsonNode elseExpression = ExpressionFactory.lessThanOrEqualExpr(ExpressionFactory.cast(referenceNode.deepCopy(), datatype), ExpressionFactory.cast(literal, datatype));
        return ExpressionFactory.ifExpr(ifCondition, then, elseExpression);
    }

    public static JsonNode constructEqualPredicate(String[] minCol, String[] maxCol, JsonNode literal, String datatype) {
        JsonNode nullNode = ExpressionFactory.constant(null);
        JsonNode minReferenceNode = ExpressionFactory.reference(minCol);
        JsonNode maxReferenceNode = ExpressionFactory.reference(maxCol);
        JsonNode ifCondition = ExpressionFactory.equalExpr(minReferenceNode, nullNode);
        JsonNode then = ExpressionFactory.cast(ExpressionFactory.constant("t"), "BOOLEAN");
        JsonNode elseRightExpression = ExpressionFactory.lessThanOrEqualExpr(ExpressionFactory.cast(literal, datatype), ExpressionFactory.cast(maxReferenceNode.deepCopy(), datatype));
        JsonNode elseLeftExpression = ExpressionFactory.lessThanOrEqualExpr(ExpressionFactory.cast(minReferenceNode.deepCopy(), datatype), ExpressionFactory.cast(literal, datatype));
        JsonNode andExpression = ExpressionFactory.andFilters(elseLeftExpression, elseRightExpression);
        return ExpressionFactory.ifExpr(ifCondition, then, andExpression);
    }

    /**
     *  if(min_a=null, true, cast(max_a as int) &lt;= cast(x as int)
     **/
    public static JsonNode constructLowerBoundPredicate(String[] maxCol, JsonNode literal, String datatype) {
        JsonNode nullNode = ExpressionFactory.constant(null);
        JsonNode referenceNode = ExpressionFactory.reference(maxCol);
        JsonNode ifCondition = ExpressionFactory.equalExpr(referenceNode, nullNode);
        JsonNode then = ExpressionFactory.cast(ExpressionFactory.constant("t"), "BOOLEAN");
        JsonNode elseExpression = ExpressionFactory.greaterThanOrEqualExpr(ExpressionFactory.cast(referenceNode.deepCopy(), datatype), ExpressionFactory.cast(literal, datatype));
        return ExpressionFactory.ifExpr(ifCondition, then, elseExpression);
    }


    public static Function<JsonNode, JsonNode> removeNonPartitionColumnsPredicatesFromComparison(Set<String> partitions) {
        return n -> {
            JsonNode node = n.deepCopy();
            List<JsonNode> references = collectReferences(node);
            if (references.isEmpty()) {
                return ExpressionFactory.trueExpression();
            }
            List<List<String>> columnNames = collectColumnNames(references);
            boolean failed = false;
            for (List<String> columnName : columnNames) {
                if (columnName.size() != 1) {
                    return ExpressionFactory.trueExpression();
                }
                if (!partitions.contains(columnName.get(0))) {
                    return ExpressionFactory.trueExpression();
                }
            }
            return node;
        };
    }

    public static Function<JsonNode, JsonNode> removeNonPartitionColumnsPredicatesFromAndConjunction(Set<String> partitions) {
        return n -> {
            ObjectNode node = n.deepCopy();
            Iterator<JsonNode> it = node.get("children").iterator();
            ArrayNode newChildren = new ArrayNode(JsonNodeFactory.instance);

            while (it.hasNext()) {
                JsonNode next = it.next();
                JsonNode nc = removeNonPartitionColumnsPredicatesFromComparison(partitions).apply(next);
                newChildren.add(nc);
            }
            node.set("children", newChildren);
            return node;
        };
    }

    static List<List<String>> collectColumnNames(List<JsonNode> references) {
        List<List<String>> result = new ArrayList<>();
        for (JsonNode node : references) {
            JsonNode names = node.get("column_names");
            List<String> currentList = new ArrayList<>();
            for (JsonNode n : names) {
                currentList.add(n.asText());
            }
            result.add(currentList);
        }
        return result;
    }

    public static JsonNode transform(JsonNode node, Function<JsonNode, Boolean> matchFn,
                                     Function<JsonNode, JsonNode> transformFn) {
        if (matchFn.apply(node)) {
            return transformFn.apply(node);
        }

        if (node instanceof ObjectNode objectNode) {
            for (Iterator<String> it = objectNode.fieldNames(); it.hasNext(); ) {
                String field = it.next();
                JsonNode current = objectNode.get(field);
                JsonNode newNode = transform(current, matchFn, transformFn);
                objectNode.set(field, newNode);
            }
            return objectNode;
        }
        if (node instanceof ArrayNode arrayNode) {
            for (int i = 0; i < arrayNode.size(); i++) {
                JsonNode current = arrayNode.get(i);
                JsonNode newNode = transform(current, matchFn, transformFn);
                arrayNode.set(i, newNode);
            }
            return arrayNode;
        }
        return node;
    }

    public static List<JsonNode> collectLiterals(JsonNode tree) {
        final List<JsonNode> list = new ArrayList<>();
        find(tree, IS_CONSTANT, list::add);
        return list;
    }

    public static List<JsonNode> collectReferences(JsonNode tree) {
        final List<JsonNode> list = new ArrayList<>();
        find(tree, IS_REFERENCE, list::add);
        return list;
    }

    public static List<JsonNode> collectSubQueries(JsonNode tree){
        final List<JsonNode> list = new ArrayList<>();
        find(tree, IS_SUBQUERY, list::add);
        return list;
    }

    public static void find(JsonNode node, Function<JsonNode, Boolean> matchFn,
                            Consumer<JsonNode> collectFn) {
        if (matchFn.apply(node)) {
            collectFn.accept(node);
            return;
        }
        if(node instanceof ArrayNode arrayNode) {
            for (Iterator<JsonNode> it = arrayNode.elements(); it.hasNext(); ) {
                JsonNode elem = it.next();
                find(elem, matchFn, collectFn);
            }
            return;
        }
        Iterable<JsonNode> children = getChildren(node);
        for (JsonNode c : children) {
            find(c, matchFn, collectFn);
        }
    }

    public static Iterable<JsonNode> getChildren(JsonNode node) {
        JsonNode children = node.get("children");
        if (children != null) {
            return children;
        }
        JsonNode child = node.get("child");

        if(child != null) {
            return List.of(child);
        }
        JsonNode left = node.get("left");
        JsonNode right = node.get("right");
        if (left != null && right != null) {
            return List.of(left, right);
        } else {
            return List.of();
        }
    }

    public static JsonNode parseToTree(Connection connection, String sql) throws JsonProcessingException {
        String escapeSql = escapeSpecialChar(sql);
        String jsonString = ConnectionPool.collectFirst(connection, String.format(JSON_SERIALIZE_SQL, escapeSql), String.class);
        return objectMapper.readTree(jsonString);
    }

    public static JsonNode parseToTree(String sql) throws SQLException, JsonProcessingException {
        String escapeSql = escapeSpecialChar(sql);
        String jsonString = ConnectionPool.collectFirst(String.format(JSON_SERIALIZE_SQL, escapeSql), String.class);
        return objectMapper.readTree(jsonString);
    }

    public static Function<String, JsonNode> start(String sql) {
        return s -> {
            try {
                return parseToTree(sql);
            } catch (SQLException | JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        };
    }

    public static Function<JsonNode, JsonNode> identity() {
        return node -> node;
    }

    public static String parseToSql(Connection connection, JsonNode node) throws SQLException {
        String sql = String.format(JSON_DESERIALIZE_SQL, node.toString());
        return ConnectionPool.collectFirst(connection, sql, String.class);
    }

    public static String parseToSql(JsonNode node) throws SQLException {
        String sql = String.format(JSON_DESERIALIZE_SQL, node.toString());
        return ConnectionPool.collectFirst(sql, String.class);
    }

    public static List<JsonNode> collectReferencesWithCast(JsonNode tree) {
        ArrayList<JsonNode> result = new ArrayList<>();
        find(tree, IS_REFERENCE_CAST, result::add);
        return result;
    }

    public static List<JsonNode> splitStatements(JsonNode tree) {
        ArrayNode statements = (ArrayNode) tree.get("statements");
        List<JsonNode> results = new ArrayList<>();
        for (JsonNode s : statements) {
            ObjectNode res = tree.deepCopy();
            ArrayNode newStatements = new ArrayNode(JsonNodeFactory.instance, 1);
            newStatements.add(s);
            res.set("statements", newStatements);
            results.add(res);
        }
        return results;
    }

    public static List<CatalogSchemaTable> getAllTablesOrPathsFromSelect(JsonNode statement, String catalogName, String schemaName) {
        var  result = new ArrayList<CatalogSchemaTable>();
        getAllTablesOrPathsFromSelect(statement, catalogName, schemaName, result);
        return result;
    }

    public static JsonNode getFirstStatementNode(JsonNode jsonNode) {
        var statements = (ArrayNode) jsonNode.get("statements");
        var statement = statements.get(0);
        return statement.get("node");
    }

    public static Function<JsonNode, JsonNode> FIRST_STATEMENT_NODE = Transformations::getFirstStatementNode;

    public static JsonNode getTableFunction(JsonNode tree) {
        var fromNode = tree.get("from_table");
        var fromTableType = fromNode.get("type").asText();
        switch (fromTableType) {
            case "BASE_TABLE" -> {
                throw new IllegalStateException("Reached BASE_TABLE");
            }
            case "TABLE_FUNCTION" -> {
                return fromNode.get("function");
            }
            case "SUBQUERY" -> {
                var node = fromNode.get("subquery").get("node");
                var type = node.get("type").asText();
                if (type.equals("SET_OPERATION_NODE")) {
                    return getTableFunction(node.get("right"));
                } else if (type.equals("SELECT_NODE")) {
                    getTableFunction(node);
                }
                return null;
            }
        }
        return null;
    }

    public static JsonNode getTableFunctionParent(JsonNode tree) {
        var fromNode = tree.get("from_table");
        var fromTableType = fromNode.get("type").asText();
        switch (fromTableType) {
            case "BASE_TABLE" -> {
                throw new IllegalStateException("Reached BASE_TABLE");
            }
            case "TABLE_FUNCTION" -> {
                return fromNode;
            }
            case "SUBQUERY" -> {
                var node = fromNode.get("subquery").get("node");
                var type = node.get("type").asText();
                if (type.equals("SET_OPERATION_NODE")) {
                    return getTableFunctionParent(node.get("right"));
                } else if (type.equals("SELECT_NODE")) {
                    getTableFunctionParent(node);
                }
                return null;
            }
        }
        return null;
    }

    public static JsonNode getSelectForBaseTable(JsonNode statementNode) {
        var fromTable = statementNode.get("from_table");
        switch (fromTable.get("type").asText()) {
            case "BASE_TABLE" -> {
                return statementNode;
            }
            case "SUBQUERY" -> {
                return getSelectForBaseTable(fromTable.get("subquery").get("node"));
            }
            default -> {
                return null;
            }
        }
    }

    public static JsonNode getSelectForTableFunction(JsonNode statementNode) {
        var fromTable = statementNode.get("from_table");
        var type = fromTable.get("type").asText();
        switch (type) {
            case "BASE_TABLE" -> {
                return statementNode;
            }
            case "SUBQUERY" -> {
                var subQueryNode = fromTable.get("subquery").get("node");
                var subQueryNodeType = subQueryNode.get("type").asText();
                switch (subQueryNodeType) {
                    case "SELECT_NODE" -> {
                        return getSelectForTableFunction(subQueryNode);
                    }
                    case "SET_OPERATION_NODE" -> {
                        if(subQueryNode.get("right").get("from_table").get("type").asText().equals("TABLE_FUNCTION")) {
                            return statementNode;
                        }
                        return null;
                    }
                    default -> {
                        return null;
                    }
                }
            }
            case "TABLE_FUNCTION" -> {
                return null;
            }
            default -> {
                return null;
            }
        }
    }

    public static JsonNode getWhereClauseForBaseTable(JsonNode statementNode) {
        return getSelectForBaseTable(statementNode).get("where_clause");
    }

    public static JsonNode getWhereClauseForTableFunction(JsonNode statementNode) {
        return getSelectForTableFunction(statementNode).get("where_clause");
    }

    public static String[][]  getHivePartition(JsonNode tree){
        var select = getFirstStatementNode(tree);
        var tableFunction = Transformations.getTableFunction(select);
        var children = (ArrayNode) tableFunction.get("children");
        JsonNode partition = null;
        for( var c :  children){
            if(isHivePartition(c)){
                partition = c;
                break;
            }
        }
        return extractPartition(partition);
    }


    private static void getAllTablesOrPathsFromSelect(JsonNode statement, String catalogName, String schemaName, List<CatalogSchemaTable> collector) {
        var fromNode = statement.get("from_table");
        var fromTableType = fromNode.get("type").asText();
        switch (fromTableType) {
            case "BASE_TABLE" -> {
                var schemaFromQuery = fromNode.get("schema_name").asText();
                var catalogFromQuery = fromNode.get("catalog_name").asText();
                var s = schemaFromQuery == null || schemaFromQuery.isEmpty() ? schemaName : schemaFromQuery;
                var c = catalogFromQuery == null || catalogFromQuery.isEmpty() ? catalogName : catalogFromQuery;
                collector.add(new CatalogSchemaTable(c, s, fromNode.get("table_name").asText(), TableType.BASE_TABLE));
            }
            case "TABLE_FUNCTION" -> {
                var function = fromNode.get("function");
                var functionName = function.get("function_name").asText();
                var functionChildren = function.get("children");
                var firstChild = functionChildren.get(0);
                collector.add(new CatalogSchemaTable(null, null, firstChild.get("value").get("value").asText(), TableType.TABLE_FUNCTION, functionName));
            }
            case "SUBQUERY" -> {
                var node = fromNode.get("subquery").get("node");
                if(node.get("type").asText().equals("SET_OPERATION_NODE")) {
                    getAllTablesOrPathsFromSetOperationNode(node, catalogName, schemaName, collector);
                } else {
                    getAllTablesOrPathsFromSelect(fromNode.get("subquery").get("node"), catalogName, schemaName, collector);
                }
            }

            case "SET_OPERATION_NODE" -> {
                getAllTablesOrPathsFromSelect(fromNode.get("left"), catalogName, schemaName, collector);
                getAllTablesOrPathsFromSelect(fromNode.get("right"), catalogName, schemaName, collector);
            }
        }
    }

    private static void getAllTablesOrPathsFromSetOperationNode(JsonNode setOperation, String catalogName, String schemaName, List<CatalogSchemaTable> collector) {
        getAllTablesOrPathsFromSelect(setOperation.get("left"), catalogName, schemaName, collector);
        getAllTablesOrPathsFromSelect(setOperation.get("right"), catalogName, schemaName, collector);
    }

    private static boolean isHivePartition(JsonNode node) {
        if(node.get("type") != null  && node.get("type").asText().equals("COMPARE_EQUAL") && node.get("left") != null){
            var left = node.get("left");
            var names = left.get("column_names");
            return names.get(0).asText().equals("hive_types");
        }
        return false;
    }

    private static String[][] extractPartition(JsonNode node){
        var children = (ArrayNode) node.get("right").get("children");
        var res = new String[children.size()][];
        int index = 0;
        for(JsonNode  c : children) {
            var a = c.get("alias").asText();
            var cn =  c.get("column_names").get(0).asText();
            res[index++] = new String[]{a, cn};
        }
        return res;
    }

    public static String getCast(String schema) throws SQLException, JsonProcessingException {
        var query = String.format("select null::struct(%s)", schema);
        var statement = Transformations.getFirstStatementNode(Transformations.parseToTree(query));
        var selectList = (ArrayNode)statement.get("select_list");
        var firstSelect = selectList.get(0);
        var castType= firstSelect.get("cast_type");
        var childType = (ArrayNode) castType.get("type_info").get("child_types");
        List<String> childTypeString = new ArrayList<>();
        for( var c : childType){
            var exp = "null::%s as %s".formatted(getStructChildTypeString(c), getStrcutChildNameString(c));
            childTypeString.add(exp);
        }
        return "select "  + String.join(",", childTypeString) + " where false";
    }


    private static String structCast(JsonNode node) {
        var typeInfo = (ArrayNode) node.get("type_info").get("child_types");
        List<String> res = new ArrayList<>();
        for(JsonNode n : typeInfo) {
            var s = getStructChildTypeString(n);
            var first = n.get("first").asText();
            res.add(first + " " + s);
        }
        return "STRUCT (" + String.join(",", res) + ")";
    }

    private static String mapCast(JsonNode node) {
        var typeInfo = (ArrayNode)node.get("type_info").get("child_type").get("type_info").get("child_types");
        var key = getTypeString(typeInfo.get(1).get("second"));
        var value = getTypeString(typeInfo.get(1).get("second"));
        return "Map(" + key +"," + value +")";
    }

    private static String listCast(JsonNode node){
        var childType = getTypeString(node.get("type_info").get("child_type"));
        return childType + "[]";
    }

    public static String decimalCast(JsonNode node) {
        var width = node.get("type_info").get("width");
        var scale = node.get("type_info").get("scale");
        return String.format("DECIMAL(%s,%s)", width, scale);
    }

    private static String getStructChildTypeString(JsonNode jsonNode){
        return getTypeString(jsonNode.get("second"));
    }

    private static String getStrcutChildNameString(JsonNode jsonNode) {
        return jsonNode.get("first").asText();
    }

    private static String getTypeString(JsonNode jsonNode){
        var id = jsonNode.get("id").asText();
        switch (id) {
            case "STRUCT" -> {
                return structCast(jsonNode);
            }
            case "MAP" -> {
                return mapCast(jsonNode);
            }
            case "LIST" -> {
                return listCast(jsonNode);
            }
            case "DECIMAL" -> {
                return decimalCast(jsonNode);
            }
            default -> {
                return id;
            }
        }
    }
    private static String escapeSpecialChar(String sql) {
        return sql.replaceAll("'", "''");
    }
}

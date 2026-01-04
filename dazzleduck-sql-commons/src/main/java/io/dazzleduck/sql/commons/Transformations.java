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

/**
 * Utility class for SQL Abstract Syntax Tree (AST) transformations.
 *
 * <p>This class provides functions to:
 * <ul>
 *   <li>Parse SQL queries into JSON AST representation using DuckDB's json_serialize_sql</li>
 *   <li>Transform AST nodes (filter pushdown, predicate rewriting, table extraction)</li>
 *   <li>Serialize AST back to SQL using json_deserialize_sql</li>
 * </ul>
 *
 * <p>The JSON AST structure follows DuckDB's logical plan format with nodes containing:
 * <ul>
 *   <li>"class" - node class (e.g., COMPARISON, CONJUNCTION)</li>
 *   <li>"type" - specific type (e.g., SELECT_NODE, BASE_TABLE)</li>
 *   <li>"children" - child nodes</li>
 *   <li>type-specific fields (e.g., "left"/"right" for comparisons)</li>
 * </ul>
 *
 * @see ExpressionFactory for creating new AST nodes
 */
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

    public static final Function<JsonNode, Boolean> IS_COMPARE_IN = isClassAndType(OPERATOR_CLASS,
            COMPARE_IN_TYPE);

    public static final Function<JsonNode, Boolean> IS_CONJUNCTION_OR = isClassAndType(CONJUNCTION_CLASS,
            CONJUNCTION_TYPE_OR);

    public static final Function<JsonNode, Boolean> IS_CAST = isClassAndType(CAST_CLASS, CAST_TYPE_OPERATOR);

    public static final Function<JsonNode, Boolean>  IS_BASE_TABLE   = isClassAndType(TABLE_FUNCTION_TYPE, TABLE_FUNCTION_TYPE);

    public static final Function<JsonNode, Boolean> IS_EMPTY_CONJUNCTION_AND = n -> {
        if (!IS_CONJUNCTION_AND.apply(n)) {
            return false;
        }
        JsonNode children = n.get(FIELD_CHILDREN);
        return children == null || !children.isArray() || children.isEmpty();
    };

    public static Function<JsonNode, Boolean> isFunction(String functionName) {
        return n -> {
            var name = n.get(FIELD_FUNCTION_NAME);
            if (name == null)
                return false;

            return isClassAndType(FUNCTION_CLASS, FUNCTION_TYPE).apply(n)
                    && functionName.equals(name.asText());
        };
    }

    public static Function<JsonNode, Boolean> isTableFunction(String functionName) {
        return n -> {
            if (n == null || functionName == null) {
                return false;
            }
            var function = n.get(FIELD_FUNCTION);
            if (function == null) {
                return false;
            }
            JsonNode functionNameNode = function.get(FIELD_FUNCTION_NAME);
            if (functionNameNode == null) {
                return false;
            }
            return isType(TABLE_FUNCTION_TYPE).apply(n)
                    && functionName.equals(functionNameNode.asText());
        };
    }

    public static Function<JsonNode, Boolean> isBaseTable(String functionName) {
        return n -> {
            if (n == null || functionName == null) {
                return false;
            }
            var function = n.get(FIELD_FUNCTION);
            if (function == null) {
                return false;
            }
            JsonNode functionNameNode = function.get(FIELD_FUNCTION_NAME);
            if (functionNameNode == null) {
                return false;
            }
            return isType(TABLE_FUNCTION_TYPE).apply(n)
                    && functionName.equals(functionNameNode.asText());
        };
    }

    public static final Function<JsonNode, Boolean> IS_REFERENCE_CAST = node -> {
        return IS_CAST.apply(node) && node.get(FIELD_CHILD) != null && IS_REFERENCE.apply(node.get(FIELD_CHILD));
    };

    public static final Function<JsonNode, Boolean> IS_SELECT = isType(SELECT_NODE_TYPE);

    public static final Function<JsonNode, Boolean> IS_COMPARISON = isClass(COMPARISON_CLASS);

    public static final Function<JsonNode, Boolean> IS_SUBQUERY = isType(SUBQUERY_TYPE);

    /**
     * Replaces constant values with placeholder values for pattern matching.
     * <ul>
     *   <li>VARCHAR values become "?"</li>
     *   <li>INTEGER values become -1</li>
     *   <li>DECIMAL values become -1.0</li>
     * </ul>
     */
    public static final Function<JsonNode, JsonNode> REPLACE_CONSTANT = node -> {
        JsonNode result = node.deepCopy();
        ObjectNode valueNode = (ObjectNode) result.get(FIELD_VALUE);
        ObjectNode type = (ObjectNode) valueNode.get(FIELD_TYPE);
        String id = type.get("id").asText();
        if (id.equals(TYPE_VARCHAR)) {
            valueNode.put(FIELD_VALUE, "?");
        } else if (id.equals(TYPE_INTEGER)) {
            valueNode.put(FIELD_VALUE, -1);
        } else if (id.equals(TYPE_DECIMAL)) {
            valueNode.put(FIELD_VALUE, -1);
            ObjectNode typeInfo = (ObjectNode) type.get("type_info").deepCopy();
            typeInfo.put("width", 1);
            typeInfo.put("scale", 1);
            valueNode.set("type_info", typeInfo);
        }
        return result;
    };

    public static Function<JsonNode, Boolean> isClassAndType(String clazz, String type) {
        return node -> {
            JsonNode classNode = node.get(FIELD_CLASS);
            JsonNode typeNode = node.get(FIELD_TYPE);
            return classNode != null && typeNode != null && classNode.asText().equals(clazz)
                    && typeNode.asText().equals(type);
        };
    }

    public static Function<JsonNode, Boolean> isClass(String clazz) {
        return node -> {
            JsonNode classNode = node.get(FIELD_CLASS);
            return classNode != null && classNode.asText().equals(clazz);
        };
    }

    public static Function<JsonNode, Boolean> isType(String type) {
        return node -> {
            JsonNode typeNode = node.get(FIELD_TYPE);
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
            Iterator<JsonNode> it = copyNode.get(FIELD_CHILDREN).iterator();
            ArrayNode newFilter = new ArrayNode(new JsonNodeFactory(false));
            Function<JsonNode, JsonNode> replaceRule = replaceEqualMinMaxFromComparison(minMapping, maxMapping, dataTypeMap);
            while (it.hasNext()) {
                JsonNode node = it.next();
                JsonNode newNode = replaceRule.apply(node);
                if (newNode != null) {
                    newFilter.add(newNode);
                }
            }
            ((ObjectNode) copyNode).set(FIELD_CHILDREN, newFilter);
            return copyNode;
        };
    }


    public static Function<JsonNode, JsonNode> removeNonPartitionColumnsPredicatesInQuery(Set<String> partitionColumns) {
        return n -> {
            ObjectNode c = n.deepCopy();
            JsonNode where = n.get(FIELD_WHERE_CLAUSE);
            if (where == null || where instanceof NullNode) {
                return c;
            }
            if (IS_CONJUNCTION_AND.apply(where)) {
                JsonNode w = transform(where, Transformations.IS_CONJUNCTION_AND,
                        removeNonPartitionColumnsPredicatesFromAndConjunction(partitionColumns));
                c.set(FIELD_WHERE_CLAUSE, w);
            } else if (IS_COMPARISON.apply(where)) {
                JsonNode w = transform(where, IS_COMPARISON,
                        removeNonPartitionColumnsPredicatesFromComparison(partitionColumns));
                c.set(FIELD_WHERE_CLAUSE, w);
            } else {
                c.set(FIELD_WHERE_CLAUSE, null);
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
            ObjectNode from_table = (ObjectNode) firstStatementNode.get(FIELD_FROM_TABLE);
            from_table.put(FIELD_TABLE_NAME, statTable);
            from_table.put(FIELD_SCHEMA_NAME, "");
            from_table.put(FIELD_CATALOG_NAME, "");
            JsonNode where = firstStatementNode.get(FIELD_WHERE_CLAUSE);
            if (where == null || where instanceof NullNode) {
                return query;
            }
            if (IS_CONJUNCTION_AND.apply(where)) {
                JsonNode w = Transformations.transform(where, IS_CONJUNCTION_AND, replaceEqualMinMaxFromAndConjunction(minMapping, maxMapping, dataTypeMap));
                firstStatementNode.set(FIELD_WHERE_CLAUSE, w);
            } else if (IS_COMPARISON.apply(where)) {
                JsonNode w = Transformations.transform(where, IS_COMPARISON, replaceEqualMinMaxFromComparison(minMapping, maxMapping, dataTypeMap));
                firstStatementNode.set(FIELD_WHERE_CLAUSE, w);
            } else {
                firstStatementNode.set(FIELD_WHERE_CLAUSE, null);
            }
            return query;
        };
    }

    public static String[] getReferenceName(JsonNode node) {
        if (node == null) {
            throw new IllegalArgumentException("Node cannot be null");
        }
        JsonNode columnNamesNode = node.get(FIELD_COLUMN_NAMES);
        if (columnNamesNode == null || !columnNamesNode.isArray()) {
            throw new IllegalStateException("Node has no 'column_names' array field. Node: " + node);
        }
        ArrayNode array = (ArrayNode) columnNamesNode;
        String[] res = new String[array.size()];
        for (int i = 0; i < res.length; i++) {
            res[i] = array.get(i).asText();
        }
        return res;
    }

    /**
     * Checks if a comparison node represents an upper bound predicate.
     * <p>Matches patterns like:
     * <ul>
     *   <li>a &lt; 10</li>
     *   <li>a &lt;= 10</li>
     *   <li>10 &gt; a</li>
     *   <li>10 &gt;= a</li>
     *   <li>a = 10</li>
     *   <li>10 = a</li>
     * </ul>
     *
     * @param node the comparison node to check
     * @return true if the node represents an upper bound, false otherwise
     */
    public static boolean isUpperBound(JsonNode node) {
        if (node == null) {
            return false;
        }
        JsonNode left = node.get("left");
        JsonNode right = node.get("right");
        JsonNode classNode = node.get(FIELD_CLASS);
        JsonNode typeNode = node.get(FIELD_TYPE);

        if (classNode == null || typeNode == null) {
            return false;
        }

        String clazz = classNode.asText();
        String type = typeNode.asText();
        return (IS_REFERENCE.apply(left) && IS_CONSTANT.apply(right) && clazz.equals(COMPARISON_CLASS)
                && (type.equals(COMPARE_TYPE_LESSTHAN) || type.equals(COMPARE_TYPE_LESSTHANOREQUALTO) || type.equals(COMPARE_TYPE_EQUAL))
                || IS_REFERENCE.apply(right) && IS_CONSTANT.apply(left) && clazz.equals(COMPARISON_CLASS)
                && (type.equals(COMPARE_TYPE_GREATERTHAN) || type.equals(COMPARE_TYPE_GREATERTHANOREQUALTO) || type.equals(COMPARE_TYPE_EQUAL)));
    }


    /**
     * Checks if a comparison node represents a lower bound predicate.
     * <p>Matches patterns like:
     * <ul>
     *   <li>a &gt; 10</li>
     *   <li>a &gt;= 10</li>
     *   <li>10 &lt; a</li>
     *   <li>10 &lt;= a</li>
     *   <li>a = 10</li>
     *   <li>10 = a</li>
     * </ul>
     *
     * @param node the comparison node to check
     * @return true if the node represents a lower bound, false otherwise
     */
    public static boolean isLowerBound(JsonNode node) {
        if (node == null) {
            return false;
        }
        JsonNode left = node.get("left");
        JsonNode right = node.get("right");
        JsonNode classNode = node.get(FIELD_CLASS);
        JsonNode typeNode = node.get(FIELD_TYPE);

        if (classNode == null || typeNode == null) {
            return false;
        }

        String clazz = classNode.asText();
        String type = typeNode.asText();
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
            // Check all column names are single-part and in partition set
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
            Iterator<JsonNode> it = node.get(FIELD_CHILDREN).iterator();
            ArrayNode newChildren = new ArrayNode(JsonNodeFactory.instance);

            while (it.hasNext()) {
                JsonNode next = it.next();
                JsonNode nc = removeNonPartitionColumnsPredicatesFromComparison(partitions).apply(next);
                newChildren.add(nc);
            }
            node.set(FIELD_CHILDREN, newChildren);
            return node;
        };
    }

    static List<List<String>> collectColumnNames(List<JsonNode> references) {
        List<List<String>> result = new ArrayList<>();
        for (JsonNode node : references) {
            JsonNode names = node.get(FIELD_COLUMN_NAMES);
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

    public static List<JsonNode> collectFunction(JsonNode tree, String functionName) {
        final List<JsonNode> list = new ArrayList<>();
        find(tree, isFunction(functionName), list::add);
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
        JsonNode children = node.get(FIELD_CHILDREN);
        if (children != null) {
            return children;
        }
        JsonNode child = node.get(FIELD_CHILD);

        if(child != null) {
            return List.of(child);
        }
        JsonNode left = node.get(FIELD_LEFT);
        JsonNode right = node.get(FIELD_RIGHT);
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


    public static Function<JsonNode, JsonNode> changeMatching(Function<JsonNode, Boolean> matching,
                                                              Consumer<JsonNode> changeFunction) {
        return input -> {
            changeMatching(input, matching, changeFunction);
            return input;
        };
    }

    private static void changeMatching(JsonNode input,
                                       Function<JsonNode, Boolean> matching,
                                       Consumer<JsonNode> changeFunction) {
        if (matching.apply(input)) {
            changeFunction.accept(input);
        }
        var children = (ArrayNode) input.get(FIELD_CHILDREN);
        if (children == null) {
            return;
        }
        for (var c : children) {
            changeMatching(c, matching, changeFunction);
        }
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
        ArrayNode statements = (ArrayNode) tree.get(FIELD_STATEMENTS);
        List<JsonNode> results = new ArrayList<>();
        for (JsonNode s : statements) {
            ObjectNode res = tree.deepCopy();
            ArrayNode newStatements = new ArrayNode(JsonNodeFactory.instance, 1);
            newStatements.add(s);
            res.set(FIELD_STATEMENTS, newStatements);
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
        if (jsonNode == null) {
            throw new IllegalArgumentException("JsonNode cannot be null");
        }
        JsonNode statementsNode = jsonNode.get(FIELD_STATEMENTS);
        if (statementsNode == null || !statementsNode.isArray()) {
            throw new IllegalStateException("JSON node has no 'statements' array field. Node: " + jsonNode);
        }
        ArrayNode statements = (ArrayNode) statementsNode;
        if (statements.isEmpty()) {
            throw new IllegalStateException("Statements array is empty. Node: " + jsonNode);
        }
        JsonNode statement = statements.get(0);
        JsonNode node = statement.get(FIELD_NODE);
        if (node == null) {
            throw new IllegalStateException("Statement has no 'node' field. Statement: " + statement);
        }
        return node;
    }

    public static Function<JsonNode, JsonNode> FIRST_STATEMENT_NODE = Transformations::getFirstStatementNode;

    public static JsonNode getTableFunction(JsonNode tree) {
        var fromNode = tree.get(FIELD_FROM_TABLE);
        var fromTableType = fromNode.get(FIELD_TYPE).asText();
        switch (fromTableType) {
            case NODE_TYPE_BASE_TABLE -> {
                throw new IllegalStateException("Cannot get table function for BASE_TABLE node. Expected TABLE_FUNCTION or SUBQUERY. Tree: " + tree);
            }
            case NODE_TYPE_TABLE_FUNCTION -> {
                return fromNode.get(FIELD_FUNCTION);
            }
            case NODE_TYPE_SUBQUERY -> {
                var node = fromNode.get(FIELD_SUBQUERY).get(FIELD_NODE);
                var type = node.get(FIELD_TYPE).asText();
                if (type.equals(NODE_TYPE_SET_OPERATION_NODE)) {
                    return getTableFunction(node.get(FIELD_RIGHT));
                } else if (type.equals(NODE_TYPE_SELECT_NODE)) {
                    return getTableFunction(node);
                }
                return null;
            }
        }
        return null;
    }

    public static JsonNode getTableFunctionParent(JsonNode tree) {
        var fromNode = tree.get(FIELD_FROM_TABLE);
        var fromTableType = fromNode.get(FIELD_TYPE).asText();
        switch (fromTableType) {
            case NODE_TYPE_BASE_TABLE -> {
                throw new IllegalStateException("Cannot get table function parent for BASE_TABLE node. Tree: " + tree);
            }
            case NODE_TYPE_TABLE_FUNCTION -> {
                return fromNode;
            }
            case NODE_TYPE_SUBQUERY -> {
                var node = fromNode.get(FIELD_SUBQUERY).get(FIELD_NODE);
                var type = node.get(FIELD_TYPE).asText();
                if (type.equals(NODE_TYPE_SET_OPERATION_NODE)) {
                    return getTableFunctionParent(node.get(FIELD_RIGHT));
                } else if (type.equals(NODE_TYPE_SELECT_NODE)) {
                    return getTableFunctionParent(node);
                }
                return null;
            }
        }
        return null;
    }

    public static JsonNode getSelectForBaseTable(JsonNode statementNode) {
        var fromTable = statementNode.get(FIELD_FROM_TABLE);
        switch (fromTable.get(FIELD_TYPE).asText()) {
            case NODE_TYPE_BASE_TABLE -> {
                return statementNode;
            }
            case NODE_TYPE_SUBQUERY -> {
                return getSelectForBaseTable(fromTable.get(FIELD_SUBQUERY).get(FIELD_NODE));
            }
            default -> {
                return null;
            }
        }
    }

    public static JsonNode getSelectForTableFunction(JsonNode statementNode) {
        var fromTable = statementNode.get(FIELD_FROM_TABLE);
        var type = fromTable.get(FIELD_TYPE).asText();
        switch (type) {
            case NODE_TYPE_BASE_TABLE -> {
                return statementNode;
            }
            case NODE_TYPE_SUBQUERY -> {
                var subQueryNode = fromTable.get(FIELD_SUBQUERY).get(FIELD_NODE);
                var subQueryNodeType = subQueryNode.get(FIELD_TYPE).asText();
                switch (subQueryNodeType) {
                    case NODE_TYPE_SELECT_NODE -> {
                        return getSelectForTableFunction(subQueryNode);
                    }
                    case NODE_TYPE_SET_OPERATION_NODE -> {
                        if(subQueryNode.get(FIELD_RIGHT).get(FIELD_FROM_TABLE).get(FIELD_TYPE).asText().equals(NODE_TYPE_TABLE_FUNCTION)) {
                            return statementNode;
                        }
                        return null;
                    }
                    default -> {
                        return null;
                    }
                }
            }
            default -> {
                return null;
            }
        }
    }

    public static JsonNode getWhereClauseForBaseTable(JsonNode statementNode) {
        JsonNode selectNode = getSelectForBaseTable(statementNode);
        return selectNode != null ? selectNode.get(FIELD_WHERE_CLAUSE) : null;
    }

    public static JsonNode getWhereClauseForTableFunction(JsonNode statementNode) {
        JsonNode selectNode = getSelectForTableFunction(statementNode);
        return selectNode != null ? selectNode.get(FIELD_WHERE_CLAUSE) : null;
    }

    public static String[][]  getHivePartition(JsonNode tree){
        var select = getFirstStatementNode(tree);
        var tableFunction = Transformations.getTableFunction(select);
        if (tableFunction == null) {
            throw new IllegalStateException("No table function found in tree: " + tree);
        }
        JsonNode childrenNode = tableFunction.get(FIELD_CHILDREN);
        if (childrenNode == null || !childrenNode.isArray()) {
            throw new IllegalStateException("Table function has no children array. Function: " + tableFunction);
        }
        ArrayNode children = (ArrayNode) childrenNode;
        JsonNode partition = null;
        for( var c :  children){
            if(isHivePartition(c)){
                partition = c;
                break;
            }
        }
        if (partition == null) {
            throw new IllegalStateException("No hive partition found in table function children");
        }
        return extractPartition(partition);
    }



    private static void getAllTablesOrPathsFromSelect(JsonNode statement, String catalogName, String schemaName, List<CatalogSchemaTable> collector) {
        var fromNode = statement.get(FIELD_FROM_TABLE);
        var fromTableType = fromNode.get(FIELD_TYPE).asText();
        switch (fromTableType) {
            case NODE_TYPE_BASE_TABLE -> {
                var schemaFromQuery = fromNode.get(FIELD_SCHEMA_NAME).asText();
                var catalogFromQuery = fromNode.get(FIELD_CATALOG_NAME).asText();
                var s = schemaFromQuery == null || schemaFromQuery.isEmpty() ? schemaName : schemaFromQuery;
                var c = catalogFromQuery == null || catalogFromQuery.isEmpty() ? catalogName : catalogFromQuery;
                collector.add(new CatalogSchemaTable(c, s, fromNode.get(FIELD_TABLE_NAME).asText(), TableType.BASE_TABLE));
            }
            case NODE_TYPE_TABLE_FUNCTION -> {
                var function = fromNode.get(FIELD_FUNCTION);
                var functionName = function.get(FIELD_FUNCTION_NAME).asText();
                var functionChildren = function.get(FIELD_CHILDREN);
                if (functionChildren == null || !functionChildren.isArray() || functionChildren.isEmpty()) {
                    throw new IllegalStateException("TABLE_FUNCTION node has no children. Function: " + functionName);
                }
                var firstChild = functionChildren.get(0);
                collector.add(new CatalogSchemaTable(null, null, firstChild.get(FIELD_VALUE).get(FIELD_VALUE).asText(), TableType.TABLE_FUNCTION, functionName));
            }
            case NODE_TYPE_SUBQUERY -> {
                var node = fromNode.get(FIELD_SUBQUERY).get(FIELD_NODE);
                if(node.get(FIELD_TYPE).asText().equals(NODE_TYPE_SET_OPERATION_NODE)) {
                    getAllTablesOrPathsFromSetOperationNode(node, catalogName, schemaName, collector);
                } else {
                    getAllTablesOrPathsFromSelect(fromNode.get(FIELD_SUBQUERY).get(FIELD_NODE), catalogName, schemaName, collector);
                }
            }

            case NODE_TYPE_SET_OPERATION_NODE -> {
                getAllTablesOrPathsFromSelect(fromNode.get(FIELD_LEFT), catalogName, schemaName, collector);
                getAllTablesOrPathsFromSelect(fromNode.get(FIELD_RIGHT), catalogName, schemaName, collector);
            }
        }
    }

    private static void getAllTablesOrPathsFromSetOperationNode(JsonNode setOperation, String catalogName, String schemaName, List<CatalogSchemaTable> collector) {
        getAllTablesOrPathsFromSelect(setOperation.get(FIELD_LEFT), catalogName, schemaName, collector);
        getAllTablesOrPathsFromSelect(setOperation.get(FIELD_RIGHT), catalogName, schemaName, collector);
    }

    private static boolean isHivePartition(JsonNode node) {
        if (node == null) {
            return false;
        }
        JsonNode typeNode = node.get(FIELD_TYPE);
        JsonNode leftNode = node.get(FIELD_LEFT);
        if(typeNode != null  && typeNode.asText().equals(COMPARE_TYPE_EQUAL) && leftNode != null){
            JsonNode names = leftNode.get(FIELD_COLUMN_NAMES);
            if (names != null && names.isArray() && names.size() > 0) {
                return names.get(0).asText().equals("hive_types");
            }
        }
        return false;
    }

    private static String[][] extractPartition(JsonNode node){
        if (node == null) {
            throw new IllegalArgumentException("Partition node cannot be null");
        }
        JsonNode rightNode = node.get(FIELD_RIGHT);
        if (rightNode == null) {
            throw new IllegalStateException("Partition node has no 'right' field. Node: " + node);
        }
        JsonNode childrenNode = rightNode.get(FIELD_CHILDREN);
        if (childrenNode == null || !childrenNode.isArray()) {
            throw new IllegalStateException("Partition right node has no 'children' array. Node: " + rightNode);
        }
        ArrayNode children = (ArrayNode) childrenNode;
        String[][] res = new String[children.size()][];
        int index = 0;
        for(JsonNode  c : children) {
            JsonNode aliasNode = c.get(FIELD_ALIAS);
            JsonNode columnNamesNode = c.get(FIELD_COLUMN_NAMES);
            if (aliasNode == null) {
                throw new IllegalStateException("Partition child has no 'alias' field. Child: " + c);
            }
            if (columnNamesNode == null || !columnNamesNode.isArray() || columnNamesNode.isEmpty()) {
                throw new IllegalStateException("Partition child has no 'column_names' array or it's empty. Child: " + c);
            }
            String alias = aliasNode.asText();
            String columnName = columnNamesNode.get(0).asText();
            res[index++] = new String[]{alias, columnName};
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

    /**
     * Extracts the constant value from a CONSTANT node.
     * <p><b>Note:</b> Currently only supports VARCHAR type. The generic return type is
     * maintained for backward compatibility but will always return String.
     *
     * @param constant the CONSTANT node
     * @param <T> the expected type (currently only String is supported)
     * @return the constant value as a String
     * @throws IllegalStateException if the constant is not VARCHAR type or has unexpected structure
     */
    @SuppressWarnings("unchecked")
    public static <T> T getConstant(JsonNode constant) {
        if (constant == null) {
            throw new IllegalArgumentException("Constant node cannot be null");
        }
        JsonNode classNode = constant.get(FIELD_CLASS);
        if (classNode == null || !CONSTANT_CLASS.equals(classNode.asText())) {
            throw new IllegalStateException("Expected CONSTANT class but got: " + (classNode != null ? classNode.asText() : "null"));
        }

        var valueNode = constant.get(FIELD_VALUE);
        if (valueNode == null) {
            throw new IllegalStateException("CONSTANT node has no value field");
        }

        JsonNode typeNode = valueNode.get(FIELD_TYPE);
        if (typeNode == null) {
            throw new IllegalStateException("CONSTANT value has no type field");
        }

        JsonNode idNode = typeNode.get("id");
        if (idNode == null) {
            throw new IllegalStateException("CONSTANT type has no id field");
        }

        String valueType = idNode.asText();
        switch (valueType) {
            case TYPE_VARCHAR -> {
                return (T) valueNode.get(FIELD_VALUE).asText();
            }
            default -> throw new IllegalStateException(
                    "Unsupported constant type: " + valueType + ". Only VARCHAR is currently supported. Node: " + constant);
        }
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
        if (typeInfo == null || typeInfo.size() < 2) {
            throw new IllegalStateException("MAP type info must have at least 2 child types. Node: " + node);
        }
        var key = getTypeString(typeInfo.get(0).get("second"));    // Fixed: use index 0 for key
        var value = getTypeString(typeInfo.get(1).get("second")); // index 1 for value
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
            case TYPE_STRUCT -> {
                return structCast(jsonNode);
            }
            case TYPE_MAP -> {
                return mapCast(jsonNode);
            }
            case TYPE_LIST -> {
                return listCast(jsonNode);
            }
            case TYPE_DECIMAL -> {
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

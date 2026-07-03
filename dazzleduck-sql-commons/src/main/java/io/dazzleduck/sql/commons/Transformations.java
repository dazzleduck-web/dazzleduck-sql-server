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
import java.util.function.BiFunction;
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
        return isBound(node,
                Set.of(COMPARE_TYPE_LESSTHAN, COMPARE_TYPE_LESSTHANOREQUALTO, COMPARE_TYPE_EQUAL),
                Set.of(COMPARE_TYPE_GREATERTHAN, COMPARE_TYPE_GREATERTHANOREQUALTO, COMPARE_TYPE_EQUAL));
    }

    public static boolean isLowerBound(JsonNode node) {
        return isBound(node,
                Set.of(COMPARE_TYPE_GREATERTHAN, COMPARE_TYPE_GREATERTHANOREQUALTO, COMPARE_TYPE_EQUAL),
                Set.of(COMPARE_TYPE_LESSTHAN, COMPARE_TYPE_LESSTHANOREQUALTO, COMPARE_TYPE_EQUAL));
    }

    private static boolean isBound(JsonNode node, Set<String> forwardTypes, Set<String> reverseTypes) {
        if (node == null) return false;
        JsonNode left  = node.get(FIELD_LEFT);
        JsonNode right = node.get(FIELD_RIGHT);
        JsonNode classNode = node.get(FIELD_CLASS);
        JsonNode typeNode  = node.get(FIELD_TYPE);
        if (classNode == null || typeNode == null) return false;
        String clazz = classNode.asText();
        String type  = typeNode.asText();
        return (IS_REFERENCE.apply(left)  && IS_CONSTANT.apply(right) && clazz.equals(COMPARISON_CLASS) && forwardTypes.contains(type))
            || (IS_REFERENCE.apply(right) && IS_CONSTANT.apply(left)  && clazz.equals(COMPARISON_CLASS) && reverseTypes.contains(type));
    }


    /** if(col=null, true, cast(col as T) OP cast(literal as T)) */
    private static JsonNode constructBoundPredicate(String[] col, JsonNode literal, String datatype,
                                                     BiFunction<JsonNode, JsonNode, JsonNode> comparison) {
        JsonNode refNode = ExpressionFactory.reference(col);
        JsonNode ifCondition = ExpressionFactory.equalExpr(refNode, ExpressionFactory.constant(null));
        JsonNode then = ExpressionFactory.cast(ExpressionFactory.constant("t"), "BOOLEAN");
        JsonNode elseExpr = comparison.apply(
                ExpressionFactory.cast(refNode.deepCopy(), datatype),
                ExpressionFactory.cast(literal, datatype));
        return ExpressionFactory.ifExpr(ifCondition, then, elseExpr);
    }

    /** if(min_col=null, true, cast(min_col as T) &lt;= cast(x as T)) */
    public static JsonNode constructUpperBoundPredicate(String[] minCol, JsonNode literal, String datatype) {
        return constructBoundPredicate(minCol, literal, datatype, ExpressionFactory::lessThanOrEqualExpr);
    }

    /** if(max_col=null, true, cast(max_col as T) &gt;= cast(x as T)) */
    public static JsonNode constructLowerBoundPredicate(String[] maxCol, JsonNode literal, String datatype) {
        return constructBoundPredicate(maxCol, literal, datatype, ExpressionFactory::greaterThanOrEqualExpr);
    }

    public static JsonNode constructEqualPredicate(String[] minCol, String[] maxCol, JsonNode literal, String datatype) {
        JsonNode minRef = ExpressionFactory.reference(minCol);
        JsonNode maxRef = ExpressionFactory.reference(maxCol);
        JsonNode ifCondition = ExpressionFactory.equalExpr(minRef, ExpressionFactory.constant(null));
        JsonNode then = ExpressionFactory.cast(ExpressionFactory.constant("t"), "BOOLEAN");
        JsonNode elseExpr = ExpressionFactory.andFilters(
                ExpressionFactory.lessThanOrEqualExpr(ExpressionFactory.cast(minRef.deepCopy(), datatype), ExpressionFactory.cast(literal, datatype)),
                ExpressionFactory.lessThanOrEqualExpr(ExpressionFactory.cast(literal, datatype), ExpressionFactory.cast(maxRef.deepCopy(), datatype)));
        return ExpressionFactory.ifExpr(ifCondition, then, elseExpr);
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

    public static List<JsonNode> collect(JsonNode tree, Function<JsonNode, Boolean> matchFn) {
        final List<JsonNode> list = new ArrayList<>();
        find(tree, matchFn, list::add);
        return list;
    }

    public static List<JsonNode> collectLiterals(JsonNode tree)                          { return collect(tree, IS_CONSTANT); }
    public static List<JsonNode> collectFunction(JsonNode tree, String functionName)     { return collect(tree, isFunction(functionName)); }
    public static List<JsonNode> collectReferences(JsonNode tree)                        { return collect(tree, IS_REFERENCE); }
    public static List<JsonNode> collectSubQueries(JsonNode tree)                        { return collect(tree, IS_SUBQUERY); }

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

    public static List<JsonNode> collectReferencesWithCast(JsonNode tree)                { return collect(tree, IS_REFERENCE_CAST); }

    /**
     * Rewrites a SQL statement by replacing a single fully-qualified table reference with
     * the {@code __this} placeholder used by the ingestion transformation CTE.
     *
     * <p>Exactly one occurrence of {@code (inputCatalog, inputSchema, inputTable)} must exist
     * as a BASE_TABLE node in the AST. Aliases are preserved. The rewritten SQL is
     * serialised back via {@code json_deserialize_sql}.
     *
     * @param sql          the view body SQL to rewrite
     * @param inputCatalog catalog component of the table to replace
     * @param inputSchema  schema component of the table to replace
     * @param inputTable   table-name component of the table to replace
     * @return rewritten SQL with the matched reference replaced by {@code __this}
     * @throws IllegalArgumentException if the table is not found or appears more than once
     */
    public static String rewriteTableAsThis(String sql, String inputCatalog, String inputSchema, String inputTable)
            throws SQLException, JsonProcessingException {
        JsonNode ast = parseToTree(sql);
        int[] count = {0};
        replaceBaseTableNode(ast, inputCatalog, inputSchema, inputTable, count);
        if (count[0] == 0) {
            throw new IllegalArgumentException(
                    "input_table '%s.%s.%s' not found in view body: %s"
                            .formatted(inputCatalog, inputSchema, inputTable, sql));
        }
        if (count[0] > 1) {
            throw new IllegalArgumentException(
                    "input_table '%s.%s.%s' appears %d times in view body — multiple references not supported: %s"
                            .formatted(inputCatalog, inputSchema, inputTable, count[0], sql));
        }
        return parseToSql(ast);
    }

    private static void replaceBaseTableNode(JsonNode node, String catalog, String schema, String table, int[] count) {
        if (node == null || node instanceof NullNode || !node.isContainerNode()) return;
        if (count[0] > 1) return; // already invalid — no point scanning further
        if (node.isObject()) {
            JsonNode typeNode = node.get(ExpressionConstants.FIELD_TYPE);
            if (typeNode != null && ExpressionConstants.NODE_TYPE_BASE_TABLE.equals(typeNode.asText())) {
                String nodeCatalog = node.get(ExpressionConstants.FIELD_CATALOG_NAME).asText("");
                String nodeSchema  = node.get(ExpressionConstants.FIELD_SCHEMA_NAME).asText("");
                String nodeTable   = node.get(ExpressionConstants.FIELD_TABLE_NAME).asText("");
                if (catalog.equals(nodeCatalog) && schema.equals(nodeSchema) && table.equals(nodeTable)) {
                    count[0]++;
                    ObjectNode obj = (ObjectNode) node;
                    obj.put(ExpressionConstants.FIELD_CATALOG_NAME, "");
                    obj.put(ExpressionConstants.FIELD_SCHEMA_NAME, "");
                    obj.put(ExpressionConstants.FIELD_TABLE_NAME, "__this");
                    return; // don't recurse into the node we just mutated
                }
            }
            node.fields().forEachRemaining(e -> replaceBaseTableNode(e.getValue(), catalog, schema, table, count));
        } else {
            node.forEach(child -> replaceBaseTableNode(child, catalog, schema, table, count));
        }
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

    /**
     * Comprehensive table reference collector for security-critical access control.
     *
     * <p>Unlike {@link #getAllTablesOrPathsFromSelect}, this method walks the <em>entire</em>
     * query AST — FROM clauses, JOIN conditions, CTE bodies, UNION/INTERSECT/EXCEPT branches,
     * and expression-level subqueries (IN, EXISTS, scalar) in WHERE, HAVING, SELECT list,
     * QUALIFY, GROUP BY, and ORDER BY — so that no table reference can be hidden from
     * access control checks.
     *
     * <p>Do NOT use this method for partition pruning or split planning; those callers
     * intentionally see only FROM-clause tables.
     */
    public static List<CatalogSchemaTable> collectAllTableReferences(JsonNode statement,
                                                                      String catalogName,
                                                                      String schemaName) {
        var result = new ArrayList<CatalogSchemaTable>();
        collectAllTableRefsFromStatement(statement, catalogName, schemaName, result, Set.of());
        return result;
    }

    /**
     * CTE-aware recursive collector.
     *
     * <p>{@code visibleCteNames} carries the CTE names that are in scope at the current nesting
     * level. BASE_TABLE references whose name is in this set are CTE references (not real tables)
     * and are skipped — the filter is enforced inside the CTE body instead. This prevents
     * double-counted or incorrectly flagged references when the same query has already been
     * transformed by {@link #injectFilterCtes} (which renames real tables to {@code ___xxx} CTEs).
     */
    private static void collectAllTableRefsFromStatement(JsonNode statement, String catalogName,
                                                          String schemaName,
                                                          List<CatalogSchemaTable> collector,
                                                          Set<String> visibleCteNames) {
        if (statement == null || statement instanceof NullNode) return;
        String type = statement.get(FIELD_TYPE).asText();
        if (NODE_TYPE_SELECT_NODE.equals(type)) {
            // Build the set of CTE names visible in this SELECT scope
            Set<String> localCteNames = new HashSet<>(visibleCteNames);
            JsonNode cteMap = statement.get("cte_map");
            if (cteMap != null) {
                JsonNode map = cteMap.get("map");
                if (map != null && map.isArray()) {
                    for (JsonNode entry : map) {
                        localCteNames.add(entry.get("key").asText());
                        // Walk each CTE body so hidden tables inside them are captured
                        collectAllTableRefsFromStatement(
                                entry.get("value").get("query").get("node"),
                                catalogName, schemaName, collector, localCteNames);
                    }
                }
            }
            // FROM clause — skip references that resolve to local CTEs
            JsonNode fromTable = statement.get(FIELD_FROM_TABLE);
            if (fromTable != null && !(fromTable instanceof NullNode)) {
                collectAllTableRefsFromFromNode(fromTable, catalogName, schemaName, collector, localCteNames);
            }
            // Expression fields (WHERE, HAVING, SELECT list, etc.)
            collectAllTableRefsFromExpressionFields(statement, catalogName, schemaName, collector, localCteNames);
        } else if (NODE_TYPE_SET_OPERATION_NODE.equals(type)) {
            collectAllTableRefsFromStatement(statement.get(FIELD_LEFT), catalogName, schemaName, collector, visibleCteNames);
            collectAllTableRefsFromStatement(statement.get(FIELD_RIGHT), catalogName, schemaName, collector, visibleCteNames);
            collectAllTableRefsFromModifiers(statement.get(FIELD_MODIFIERS), catalogName, schemaName, collector, visibleCteNames);
        } else if (NODE_TYPE_RECURSIVE_CTE_NODE.equals(type)) {
            // WITH RECURSIVE body: recurse into both arms. Keep the CTE's own name visible so the
            // recursive self-reference is treated as a CTE reference, not a hidden base table.
            Set<String> localCteNames = visibleCteNames;
            JsonNode cteName = statement.get(FIELD_CTE_NAME);
            if (cteName != null) {
                localCteNames = new HashSet<>(visibleCteNames);
                localCteNames.add(cteName.asText());
            }
            collectAllTableRefsFromStatement(statement.get(FIELD_LEFT), catalogName, schemaName, collector, localCteNames);
            collectAllTableRefsFromStatement(statement.get(FIELD_RIGHT), catalogName, schemaName, collector, localCteNames);
            collectAllTableRefsFromModifiers(statement.get(FIELD_MODIFIERS), catalogName, schemaName, collector, localCteNames);
        } else {
            // Fail closed: an unrecognized query node type must abort authorization rather than
            // silently hide the tables it contains (which would let them bypass the access check).
            throw new IllegalStateException(
                    "collectAllTableReferences: unsupported query node type '" + type + "'; refusing to "
                            + "authorize a query whose tables cannot all be enumerated");
        }
    }

    /** Walks ORDER BY / LIMIT / OFFSET modifiers for table references inside their subqueries. */
    private static void collectAllTableRefsFromModifiers(JsonNode modifiers, String catalogName,
                                                         String schemaName,
                                                         List<CatalogSchemaTable> collector,
                                                         Set<String> cteNames) {
        if (modifiers == null || !modifiers.isArray()) return;
        for (JsonNode modifier : modifiers) {
            JsonNode orders = modifier.get(FIELD_ORDERS);
            if (orders != null && orders.isArray()) {
                for (JsonNode order : orders) {
                    collectAllTableRefsFromExpression(order.get(FIELD_EXPRESSION), catalogName, schemaName, collector, cteNames);
                }
            }
            collectAllTableRefsFromExpression(modifier.get(FIELD_LIMIT), catalogName, schemaName, collector, cteNames);
            collectAllTableRefsFromExpression(modifier.get(FIELD_OFFSET), catalogName, schemaName, collector, cteNames);
        }
    }

    private static void collectAllTableRefsFromFromNode(JsonNode fromNode, String catalogName,
                                                         String schemaName,
                                                         List<CatalogSchemaTable> collector,
                                                         Set<String> cteNames) {
        if (fromNode == null || fromNode instanceof NullNode) return;
        String type = fromNode.get(FIELD_TYPE).asText();
        switch (type) {
            case NODE_TYPE_BASE_TABLE -> {
                String tableName = fromNode.get(FIELD_TABLE_NAME).asText();
                if (cteNames.contains(tableName)) return; // CTE reference — body already walked
                var schemaFromQuery = fromNode.get(FIELD_SCHEMA_NAME).asText();
                var catalogFromQuery = fromNode.get(FIELD_CATALOG_NAME).asText();
                var s = schemaFromQuery == null || schemaFromQuery.isEmpty() ? schemaName : schemaFromQuery;
                var c = catalogFromQuery == null || catalogFromQuery.isEmpty() ? catalogName : catalogFromQuery;
                collector.add(new CatalogSchemaTable(c, s, tableName, TableType.BASE_TABLE));
            }
            case NODE_TYPE_TABLE_FUNCTION -> {
                var function = fromNode.get(FIELD_FUNCTION);
                var functionName = function.get(FIELD_FUNCTION_NAME).asText();
                var functionChildren = function.get(FIELD_CHILDREN);
                if (functionChildren != null && functionChildren.isArray() && !functionChildren.isEmpty()) {
                    extractPathsFromTableFunctionChild(functionChildren.get(0), functionName, collector);
                }
            }
            case NODE_TYPE_JOIN -> {
                collectAllTableRefsFromFromNode(fromNode.get(FIELD_LEFT), catalogName, schemaName, collector, cteNames);
                collectAllTableRefsFromFromNode(fromNode.get(FIELD_RIGHT), catalogName, schemaName, collector, cteNames);
                collectAllTableRefsFromExpression(fromNode.get("condition"), catalogName, schemaName, collector, cteNames);
            }
            case NODE_TYPE_SUBQUERY -> {
                collectAllTableRefsFromStatement(
                        fromNode.get(FIELD_SUBQUERY).get(FIELD_NODE), catalogName, schemaName, collector, cteNames);
            }
            case NODE_TYPE_PIVOT -> {
                // PIVOT / UNPIVOT: the scanned relation is under `source`; recurse into it so the
                // pivoted table is seen by the access check. Aggregate expressions may hold subqueries.
                collectAllTableRefsFromFromNode(fromNode.get(FIELD_SOURCE), catalogName, schemaName, collector, cteNames);
                collectAllTableRefsFromExpression(fromNode.get(FIELD_AGGREGATES), catalogName, schemaName, collector, cteNames);
            }
            case NODE_TYPE_EXPRESSION_LIST -> {
                // VALUES list: each value expression may embed a subquery over a base table.
                collectAllTableRefsFromExpression(fromNode.get(FIELD_VALUES), catalogName, schemaName, collector, cteNames);
            }
            case NODE_TYPE_EMPTY -> {
                // FROM-less SELECT (e.g. SELECT 1): no relation to enumerate.
            }
            default -> throw new IllegalStateException(
                    "collectAllTableReferences: unsupported FROM node type '" + type + "'; refusing to "
                            + "authorize a query whose tables cannot all be enumerated");
        }
    }

    private static void collectAllTableRefsFromExpressionFields(JsonNode selectNode, String catalogName,
                                                                  String schemaName,
                                                                  List<CatalogSchemaTable> collector,
                                                                  Set<String> cteNames) {
        collectAllTableRefsFromExpression(selectNode.get(FIELD_WHERE_CLAUSE), catalogName, schemaName, collector, cteNames);
        collectAllTableRefsFromExpression(selectNode.get("having"), catalogName, schemaName, collector, cteNames);
        collectAllTableRefsFromExpression(selectNode.get("qualify"), catalogName, schemaName, collector, cteNames);
        collectAllTableRefsFromExpression(selectNode.get("select_list"), catalogName, schemaName, collector, cteNames);
        collectAllTableRefsFromExpression(selectNode.get("group_expressions"), catalogName, schemaName, collector, cteNames);
        // ORDER BY / LIMIT / OFFSET live under modifiers[], not a top-level "order_bys" field.
        collectAllTableRefsFromModifiers(selectNode.get(FIELD_MODIFIERS), catalogName, schemaName, collector, cteNames);
    }

    private static void collectAllTableRefsFromExpression(JsonNode expr, String catalogName,
                                                           String schemaName,
                                                           List<CatalogSchemaTable> collector,
                                                           Set<String> cteNames) {
        if (expr == null || expr instanceof NullNode) return;
        if (expr.isArray()) {
            for (JsonNode element : expr) {
                collectAllTableRefsFromExpression(element, catalogName, schemaName, collector, cteNames);
            }
            return;
        }
        if (!expr.isObject()) return;
        JsonNode classNode = expr.get(FIELD_CLASS);
        if (classNode != null && SUBQUERY_CLASS.equals(classNode.asText())) {
            JsonNode subqueryContent = expr.get(FIELD_SUBQUERY);
            if (subqueryContent != null) {
                JsonNode innerNode = subqueryContent.get(FIELD_NODE);
                if (innerNode != null) {
                    collectAllTableRefsFromStatement(innerNode, catalogName, schemaName, collector, cteNames);
                }
            }
            collectAllTableRefsFromExpression(expr.get(FIELD_CHILD), catalogName, schemaName, collector, cteNames);
            return;
        }
        collectAllTableRefsFromExpression(expr.get(FIELD_LEFT), catalogName, schemaName, collector, cteNames);
        collectAllTableRefsFromExpression(expr.get(FIELD_RIGHT), catalogName, schemaName, collector, cteNames);
        collectAllTableRefsFromExpression(expr.get(FIELD_CHILDREN), catalogName, schemaName, collector, cteNames);
        collectAllTableRefsFromExpression(expr.get(FIELD_CHILD), catalogName, schemaName, collector, cteNames);
        collectAllTableRefsFromExpression(expr.get(FIELD_FILTER), catalogName, schemaName, collector, cteNames);
        collectAllTableRefsFromExpression(expr.get(FIELD_WHEN_EXPR), catalogName, schemaName, collector, cteNames);
        collectAllTableRefsFromExpression(expr.get(FIELD_THEN_EXPR), catalogName, schemaName, collector, cteNames);
        collectAllTableRefsFromExpression(expr.get(FIELD_ELSE_EXPR), catalogName, schemaName, collector, cteNames);
        collectAllTableRefsFromExpression(expr.get(FIELD_CASE_CHECKS), catalogName, schemaName, collector, cteNames);
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

    public static JsonNode getTableFunction(JsonNode tree)       { return getTableFunctionNode(tree, false); }
    public static JsonNode getTableFunctionParent(JsonNode tree) { return getTableFunctionNode(tree, true); }

    private static JsonNode getTableFunctionNode(JsonNode tree, boolean returnParent) {
        var fromNode = tree.get(FIELD_FROM_TABLE);
        var fromTableType = fromNode.get(FIELD_TYPE).asText();
        switch (fromTableType) {
            case NODE_TYPE_BASE_TABLE -> throw new IllegalStateException(
                    "Cannot get table function" + (returnParent ? " parent" : "") +
                    " for BASE_TABLE node. Expected TABLE_FUNCTION or SUBQUERY. Tree: " + tree);
            case NODE_TYPE_TABLE_FUNCTION -> { return returnParent ? fromNode : fromNode.get(FIELD_FUNCTION); }
            case NODE_TYPE_SUBQUERY -> {
                var node = fromNode.get(FIELD_SUBQUERY).get(FIELD_NODE);
                var type = node.get(FIELD_TYPE).asText();
                if (type.equals(NODE_TYPE_SET_OPERATION_NODE)) {
                    return getTableFunctionNode(node.get(FIELD_RIGHT), returnParent);
                } else if (type.equals(NODE_TYPE_SELECT_NODE)) {
                    return getTableFunctionNode(node, returnParent);
                }
                return null;
            }
        }
        return null;
    }

    public static JsonNode getSelectForBaseTable(JsonNode statementNode) {
        if (statementNode == null) {
            return null;
        }
        var fromTable = statementNode.get(FIELD_FROM_TABLE);
        if (fromTable == null) {
            return null;
        }
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
                extractPathsFromTableFunctionChild(firstChild, functionName, collector);
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

    /**
     * Extracts paths from a table function child node.
     * Handles both direct constant values and nested list_value functions.
     */
    private static void extractPathsFromTableFunctionChild(JsonNode child, String functionName, List<CatalogSchemaTable> collector) {
        JsonNode classNode = child.get(FIELD_CLASS);
        if (classNode != null && FUNCTION_CLASS.equals(classNode.asText())) {
            // Child is a function (e.g., list_value)
            JsonNode functionNameNode = child.get(FIELD_FUNCTION_NAME);
            if (functionNameNode != null && "list_value".equals(functionNameNode.asText())) {
                // Extract all paths from list_value children
                JsonNode listChildren = child.get(FIELD_CHILDREN);
                if (listChildren != null && listChildren.isArray()) {
                    for (JsonNode listChild : listChildren) {
                        String path = extractConstantValue(listChild);
                        if (path != null) {
                            collector.add(new CatalogSchemaTable(null, null, path, TableType.TABLE_FUNCTION, functionName));
                        }
                    }
                }
            }
        } else {
            // Direct constant value
            String path = extractConstantValue(child);
            if (path != null) {
                collector.add(new CatalogSchemaTable(null, null, path, TableType.TABLE_FUNCTION, functionName));
            }
        }
    }

    /**
     * Extracts the string value from a constant node.
     */
    private static String extractConstantValue(JsonNode node) {
        JsonNode valueNode = node.get(FIELD_VALUE);
        if (valueNode != null) {
            JsonNode innerValue = valueNode.get(FIELD_VALUE);
            if (innerValue != null) {
                return innerValue.asText();
            }
        }
        return null;
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
    /**
     * Applies LIMIT (and optionally OFFSET) to the outermost SELECT node.
     *
     * <p>Works for all FROM clause types: BASE_TABLE, TABLE_FUNCTION, SUBQUERY,
     * SET_OPERATION_NODE. Previously only BASE_TABLE and simple SUBQUERY were
     * handled; TABLE_FUNCTION queries (e.g. generate_series) were silently skipped.
     */
    public static JsonNode addLimit(JsonNode query, long limit, long offset) {
        if (limit < 0 && offset < 0) {
            return query;
        }
        var select = (ObjectNode) getFirstStatementNode(query);

        ArrayNode modifiers = (ArrayNode) select.get(FIELD_MODIFIERS);
        if (modifiers == null) {
            modifiers = select.putArray(FIELD_MODIFIERS);
        } else {
            for (int i = 0; i < modifiers.size(); i++) {
                if (modifiers.get(i).get(FIELD_TYPE).asText().equals(LIMIT_MODIFIER_TYPE)) {
                    modifiers.remove(i);
                    break;
                }
            }
        }

        modifiers.add(ExpressionFactory.limitModifier(limit, offset));
        return query;
    }

    private static String escapeSpecialChar(String sql) {
        return sql.replaceAll("'", "''");
    }

    /**
     * Injects a row-level security filter into every real base table in the query by
     * prepending a filter CTE for each table and renaming the table references to the CTE.
     *
     * <p>Example — given filter {@code tenant_id = 'abc'}:
     * <pre>SELECT * FROM a JOIN b ON a.id = b.id</pre>
     * becomes:
     * <pre>WITH ___a AS (SELECT * FROM a WHERE tenant_id = 'abc'),
     *      ___b AS (SELECT * FROM b WHERE tenant_id = 'abc')
     * SELECT * FROM ___a a JOIN ___b b ON a.id = b.id</pre>
     *
     * <p>Existing user-defined CTEs are not wrapped at the reference site; instead the
     * filter is injected into their bodies, so they automatically see filtered data.
     */
    public static JsonNode injectFilterCtes(JsonNode query, JsonNode filter) {
        return injectFilterCtes(query, tableName -> filter);
    }

    /**
     * Per-table variant of {@link #injectFilterCtes}: each base table gets its own filter
     * expression looked up by table name from {@code tableFilters}.
     * Keys may be fully-qualified ({@code catalog.schema.table}), partially qualified
     * ({@code schema.table}), or bare table names. Qualified keys are expanded so that every
     * shorter suffix form is also registered (e.g. {@code "memory.main.orders"} additionally
     * registers {@code "main.orders"} and {@code "orders"}), allowing unqualified or partially
     * qualified AST references to match. A fully-qualified reference to a different catalog or
     * schema (e.g. {@code other.main.orders}) will NOT match and receives a {@code false} filter.
     * Tables not present in the map receive a {@code false} filter (no rows returned),
     * so the map must cover every table the query touches.
     */
    public static JsonNode injectFilterCtes(JsonNode query, Map<String, JsonNode> tableFilters) {
        // Normalize: for "catalog.schema.table" or "schema.table" keys, also register every
        // shorter suffix so that unqualified or partially-qualified AST references still match.
        // e.g. "memory.main.orders" → also adds "main.orders" and "orders".
        // The lookup in the private variant uses the fully-qualified dotted key built from the
        // AST node's catalog/schema/table fields, so "other_catalog.main.orders" will NOT match
        // a key added for "memory.main.orders" — only exact or suffix-subset matches apply.
        Map<String, JsonNode> normalized = new LinkedHashMap<>(tableFilters);
        for (Map.Entry<String, JsonNode> e : tableFilters.entrySet()) {
            String[] parts = e.getKey().split("\\.", -1);
            for (int i = 1; i < parts.length; i++) {
                String suffix = String.join(".", Arrays.copyOfRange(parts, i, parts.length));
                normalized.putIfAbsent(suffix, e.getValue());
            }
        }
        return injectFilterCtes(query,
                qualifiedName -> normalized.getOrDefault(qualifiedName, ExpressionFactory.falseExpression()));
    }

    private static JsonNode injectFilterCtes(JsonNode query, Function<String, JsonNode> filterForTable) {
        var result = query.deepCopy();
        var statementNode = (ObjectNode) getFirstStatementNode(result);

        // Ordered map: cteKey → original BASE_TABLE node (schema/catalog preserved for CTE body)
        Map<String, ObjectNode> tablesToWrap = new LinkedHashMap<>();

        // Walk the whole statement — its FROM clause, expression subqueries, and any WITH clause
        // (top-level or nested) — collecting every real base table and rewriting each reference to
        // its filter-CTE name. CTE bodies are handled inside the dispatcher so nesting at any depth
        // is filtered uniformly.
        renameBaseTablesInStatementNode(statementNode, new HashSet<>(), tablesToWrap);

        if (!tablesToWrap.isEmpty()) {
            ArrayNode newMap = JsonNodeFactory.instance.arrayNode();
            for (Map.Entry<String, ObjectNode> e : tablesToWrap.entrySet()) {
                String qualifiedLookupKey = buildQualifiedLookupKey(e.getValue());
                newMap.add(buildFilterCteEntry(e.getKey(), e.getValue(), filterForTable.apply(qualifiedLookupKey)));
            }
            JsonNode existingMap = statementNode.get(FIELD_CTE_MAP).get(FIELD_MAP);
            if (existingMap != null && existingMap.isArray()) {
                for (JsonNode existing : existingMap) {
                    newMap.add(existing);
                }
            }
            ((ObjectNode) statementNode.get(FIELD_CTE_MAP)).set(FIELD_MAP, newMap);
        }

        return result;
    }

    /**
     * Processes a query node's own WITH clause: registers every CTE name it declares (so references
     * to them are treated as CTE references, not base tables) and injects the filter into each CTE
     * body. Returns {@code visibleCteNames} augmented with the names declared here.
     *
     * <p>Each CTE body is walked with its OWN name removed from the visible set: a non-recursive CTE
     * cannot reference itself, so a same-named reference inside the body is the real base table and
     * MUST be filtered. A recursive CTE body ({@code RECURSIVE_CTE_NODE}) re-adds its own name for the
     * self-reference in {@link #renameBaseTablesInStatementNode}.
     */
    private static Set<String> walkNestedCtes(ObjectNode node, Set<String> visibleCteNames,
                                              Map<String, ObjectNode> tablesToWrap) {
        JsonNode cteMapNode = node.get(FIELD_CTE_MAP);
        if (cteMapNode == null) return visibleCteNames;
        JsonNode mapArray = cteMapNode.get(FIELD_MAP);
        if (mapArray == null || !mapArray.isArray() || mapArray.isEmpty()) return visibleCteNames;

        Set<String> withDeclared = new HashSet<>(visibleCteNames);
        for (JsonNode entry : mapArray) {
            withDeclared.add(entry.get(FIELD_KEY).asText());
        }
        for (JsonNode entry : mapArray) {
            String cteName = entry.get(FIELD_KEY).asText();
            Set<String> namesVisibleInBody = new HashSet<>(withDeclared);
            namesVisibleInBody.remove(cteName);
            ObjectNode cteBody = (ObjectNode) entry.get(FIELD_VALUE).get(FIELD_QUERY).get(FIELD_NODE);
            renameBaseTablesInStatementNode(cteBody, namesVisibleInBody, tablesToWrap);
        }
        return withDeclared;
    }

    /**
     * Dispatches a query node (a statement body or CTE body) to the correct handler by type.
     *
     * <p>Fails closed: an unrecognized node type throws rather than silently passing its base tables
     * through unfiltered (which would be an RLS bypass). This mechanism guards row-level security, so
     * any shape it does not understand must abort the query, not leak.
     */
    private static void renameBaseTablesInStatementNode(ObjectNode statementNode, Set<String> visibleCteNames,
                                                         Map<String, ObjectNode> tablesToWrap) {
        String type = statementNode.get(FIELD_TYPE).asText();
        switch (type) {
            case NODE_TYPE_SELECT_NODE -> {
                Set<String> visible = walkNestedCtes(statementNode, visibleCteNames, tablesToWrap);
                renameBaseTablesInSelect(statementNode, visible, tablesToWrap);
            }
            case NODE_TYPE_SET_OPERATION_NODE -> {
                Set<String> visible = walkNestedCtes(statementNode, visibleCteNames, tablesToWrap);
                renameBaseTablesInStatementNode((ObjectNode) statementNode.get(FIELD_LEFT), visible, tablesToWrap);
                renameBaseTablesInStatementNode((ObjectNode) statementNode.get(FIELD_RIGHT), visible, tablesToWrap);
                walkModifiersForSubqueries(statementNode.get(FIELD_MODIFIERS), visible, tablesToWrap);
            }
            case NODE_TYPE_RECURSIVE_CTE_NODE -> {
                Set<String> visible = walkNestedCtes(statementNode, visibleCteNames, tablesToWrap);
                // The recursive self-reference (FROM r inside the CTE body) is a CTE reference, not a
                // base table; keep the CTE's own name visible so it is not wrapped as a phantom table.
                JsonNode cteName = statementNode.get(FIELD_CTE_NAME);
                if (cteName != null) {
                    visible = new HashSet<>(visible);
                    visible.add(cteName.asText());
                }
                renameBaseTablesInStatementNode((ObjectNode) statementNode.get(FIELD_LEFT), visible, tablesToWrap);
                renameBaseTablesInStatementNode((ObjectNode) statementNode.get(FIELD_RIGHT), visible, tablesToWrap);
                walkModifiersForSubqueries(statementNode.get(FIELD_MODIFIERS), visible, tablesToWrap);
            }
            default -> throw new IllegalStateException(
                    "injectFilterCtes: unsupported query node type '" + type + "'; refusing to run to "
                            + "avoid producing an unfiltered (RLS-bypassing) query");
        }
    }

    /**
     * Walks the result modifiers of a SELECT or set-operation node (ORDER BY, LIMIT, OFFSET) for
     * subqueries, so a base table referenced only inside e.g. {@code ORDER BY (SELECT ... FROM t)}
     * is still filtered. DuckDB serializes these under {@code modifiers[]} (ORDER BY expressions live
     * in {@code orders[].expression}), NOT a top-level {@code order_bys} field.
     */
    private static void walkModifiersForSubqueries(JsonNode modifiers, Set<String> visibleCteNames,
                                                    Map<String, ObjectNode> tablesToWrap) {
        if (modifiers == null || !modifiers.isArray()) return;
        for (JsonNode modifier : modifiers) {
            JsonNode orders = modifier.get(FIELD_ORDERS);
            if (orders != null && orders.isArray()) {
                for (JsonNode order : orders) {
                    walkExpressionsForSubqueries(order.get(FIELD_EXPRESSION), visibleCteNames, tablesToWrap);
                }
            }
            walkExpressionsForSubqueries(modifier.get(FIELD_LIMIT), visibleCteNames, tablesToWrap);
            walkExpressionsForSubqueries(modifier.get(FIELD_OFFSET), visibleCteNames, tablesToWrap);
        }
    }

    private static void renameBaseTablesInSelect(ObjectNode selectNode, Set<String> userCteNames,
                                                  Map<String, ObjectNode> tablesToWrap) {
        JsonNode fromTable = selectNode.get(FIELD_FROM_TABLE);
        if (fromTable != null && !(fromTable instanceof NullNode)) {
            renameBaseTablesInFromNode(fromTable, userCteNames, tablesToWrap);
        }
        // Walk all expression-bearing fields for subqueries (WHERE IN, EXISTS, scalar, etc.)
        walkExpressionFieldsInSelect(selectNode, userCteNames, tablesToWrap);
    }

    /**
     * Walks the expression-bearing fields of a SELECT node, recursing into any subquery
     * expressions found (IN, EXISTS, scalar, ANY/ALL).
     */
    private static void walkExpressionFieldsInSelect(ObjectNode selectNode, Set<String> userCteNames,
                                                       Map<String, ObjectNode> tablesToWrap) {
        walkExpressionsForSubqueries(selectNode.get(FIELD_WHERE_CLAUSE), userCteNames, tablesToWrap);
        walkExpressionsForSubqueries(selectNode.get("having"), userCteNames, tablesToWrap);
        walkExpressionsForSubqueries(selectNode.get("qualify"), userCteNames, tablesToWrap);
        walkExpressionsForSubqueries(selectNode.get("select_list"), userCteNames, tablesToWrap);
        walkExpressionsForSubqueries(selectNode.get("group_expressions"), userCteNames, tablesToWrap);
        // ORDER BY / LIMIT / OFFSET live under modifiers[], not a top-level "order_bys" field.
        walkModifiersForSubqueries(selectNode.get(FIELD_MODIFIERS), userCteNames, tablesToWrap);
    }

    /**
     * Recursively walks an expression tree and injects the filter into any subquery expressions
     * (IN/ANY/ALL, EXISTS, scalar subqueries) by renaming their base tables.
     */
    private static void walkExpressionsForSubqueries(JsonNode expr, Set<String> userCteNames,
                                                      Map<String, ObjectNode> tablesToWrap) {
        if (expr == null || expr instanceof NullNode) return;

        if (expr.isArray()) {
            for (JsonNode element : expr) {
                walkExpressionsForSubqueries(element, userCteNames, tablesToWrap);
            }
            return;
        }

        if (!expr.isObject()) return;

        JsonNode classNode = expr.get(FIELD_CLASS);
        if (classNode != null && SUBQUERY_CLASS.equals(classNode.asText())) {
            // Expression-level subquery: IN (SELECT ...), EXISTS (...), scalar (SELECT ...)
            // renameBaseTablesInStatementNode already walks expression fields via renameBaseTablesInSelect
            JsonNode subqueryContent = expr.get(FIELD_SUBQUERY);
            if (subqueryContent != null) {
                ObjectNode innerNode = (ObjectNode) subqueryContent.get(FIELD_NODE);
                if (innerNode != null) {
                    renameBaseTablesInStatementNode(innerNode, userCteNames, tablesToWrap);
                }
            }
            // Walk the `child` field (the expression on the left side of IN/ANY/ALL)
            walkExpressionsForSubqueries(expr.get(FIELD_CHILD), userCteNames, tablesToWrap);
            return;
        }

        // Recurse into known expression child fields for all other expression types
        walkExpressionsForSubqueries(expr.get(FIELD_LEFT), userCteNames, tablesToWrap);
        walkExpressionsForSubqueries(expr.get(FIELD_RIGHT), userCteNames, tablesToWrap);
        walkExpressionsForSubqueries(expr.get(FIELD_CHILDREN), userCteNames, tablesToWrap);
        walkExpressionsForSubqueries(expr.get(FIELD_CHILD), userCteNames, tablesToWrap);
        walkExpressionsForSubqueries(expr.get(FIELD_FILTER), userCteNames, tablesToWrap);
        // CASE WHEN THEN ELSE
        walkExpressionsForSubqueries(expr.get(FIELD_WHEN_EXPR), userCteNames, tablesToWrap);
        walkExpressionsForSubqueries(expr.get(FIELD_THEN_EXPR), userCteNames, tablesToWrap);
        walkExpressionsForSubqueries(expr.get(FIELD_ELSE_EXPR), userCteNames, tablesToWrap);
        walkExpressionsForSubqueries(expr.get(FIELD_CASE_CHECKS), userCteNames, tablesToWrap);
    }

    private static void renameBaseTablesInFromNode(JsonNode fromNode, Set<String> userCteNames,
                                                    Map<String, ObjectNode> tablesToWrap) {
        if (fromNode == null || fromNode instanceof NullNode) return;
        String type = fromNode.get(FIELD_TYPE).asText();
        switch (type) {
            case NODE_TYPE_BASE_TABLE -> {
                String tableName = fromNode.get(FIELD_TABLE_NAME).asText();
                if (userCteNames.contains(tableName)) return; // CTE reference — skip
                String schema = fromNode.has(FIELD_SCHEMA_NAME) ? fromNode.get(FIELD_SCHEMA_NAME).asText() : "";
                String catalog = fromNode.has(FIELD_CATALOG_NAME) ? fromNode.get(FIELD_CATALOG_NAME).asText() : "";
                String cteKey = buildCteKey(tableName, schema, catalog);
                if (!tablesToWrap.containsKey(cteKey)) {
                    tablesToWrap.put(cteKey, (ObjectNode) fromNode.deepCopy());
                }
                ObjectNode mutable = (ObjectNode) fromNode;
                String existingAlias = mutable.has(FIELD_ALIAS) ? mutable.get(FIELD_ALIAS).asText() : "";
                if (existingAlias.isEmpty()) {
                    mutable.put(FIELD_ALIAS, tableName);
                }
                mutable.put(FIELD_TABLE_NAME, cteKey);
                mutable.put(FIELD_SCHEMA_NAME, "");
                mutable.put(FIELD_CATALOG_NAME, "");
            }
            case NODE_TYPE_JOIN -> {
                renameBaseTablesInFromNode(fromNode.get(FIELD_LEFT), userCteNames, tablesToWrap);
                renameBaseTablesInFromNode(fromNode.get(FIELD_RIGHT), userCteNames, tablesToWrap);
                // Walk the join condition for subqueries (rare but possible)
                walkExpressionsForSubqueries(fromNode.get("condition"), userCteNames, tablesToWrap);
            }
            case NODE_TYPE_SUBQUERY -> {
                // The derived-table body may be a SELECT, a set operation, or a recursive CTE;
                // dispatch on its type so nested base tables are filtered too.
                ObjectNode inner = (ObjectNode) fromNode.get(FIELD_SUBQUERY).get(FIELD_NODE);
                renameBaseTablesInStatementNode(inner, userCteNames, tablesToWrap);
            }
            case NODE_TYPE_PIVOT -> {
                // PIVOT / UNPIVOT: the scanned relation is under `source`; recurse into it so the
                // pivoted base table is filtered. Aggregate expressions may hold subqueries.
                renameBaseTablesInFromNode(fromNode.get(FIELD_SOURCE), userCteNames, tablesToWrap);
                walkExpressionsForSubqueries(fromNode.get(FIELD_AGGREGATES), userCteNames, tablesToWrap);
            }
            case NODE_TYPE_EXPRESSION_LIST -> {
                // VALUES list: each value expression may embed a subquery over a base table.
                walkExpressionsForSubqueries(fromNode.get(FIELD_VALUES), userCteNames, tablesToWrap);
            }
            case NODE_TYPE_EMPTY -> {
                // FROM-less SELECT (e.g. SELECT 1): no relation to filter.
            }
            default -> throw new IllegalStateException(
                    "injectFilterCtes: unsupported FROM node type '" + type + "'; refusing to run to "
                            + "avoid producing an unfiltered (RLS-bypassing) query");
        }
    }

    /** Builds the lookup key used to match against the tableFilters map: "catalog.schema.table",
     *  omitting empty parts (e.g. "main.orders" when catalog is absent, "orders" when both absent). */
    private static String buildQualifiedLookupKey(ObjectNode tableNode) {
        String table = tableNode.get(FIELD_TABLE_NAME).asText();
        String schema = tableNode.has(FIELD_SCHEMA_NAME) ? tableNode.get(FIELD_SCHEMA_NAME).asText() : "";
        String catalog = tableNode.has(FIELD_CATALOG_NAME) ? tableNode.get(FIELD_CATALOG_NAME).asText() : "";
        var sb = new StringBuilder();
        if (!catalog.isEmpty()) sb.append(catalog).append(".");
        if (!schema.isEmpty()) sb.append(schema).append(".");
        sb.append(table);
        return sb.toString();
    }

    private static String buildCteKey(String tableName, String schema, String catalog) {
        var key = new StringBuilder("___");
        if (catalog != null && !catalog.isEmpty()) key.append(catalog).append("_");
        if (schema != null && !schema.isEmpty()) key.append(schema).append("_");
        key.append(tableName);
        return key.toString();
    }

    private static ObjectNode buildFilterCteEntry(String cteKey, ObjectNode originalTableNode, JsonNode filter) {
        JsonNodeFactory f = JsonNodeFactory.instance;

        // SELECT * FROM <original_table> WHERE <filter>
        ObjectNode innerSelect = f.objectNode();
        innerSelect.put(FIELD_TYPE, NODE_TYPE_SELECT_NODE);
        innerSelect.putArray(FIELD_MODIFIERS);
        ObjectNode innerCteMap = f.objectNode();
        innerCteMap.putArray("map");
        innerSelect.set("cte_map", innerCteMap);

        ObjectNode star = f.objectNode();
        star.put(FIELD_CLASS, "STAR");
        star.put(FIELD_TYPE, "STAR");
        star.put(FIELD_ALIAS, "");
        star.put(FIELD_QUERY_LOCATION, 0);
        star.put("relation_name", "");
        star.putArray("exclude_list");
        star.putArray("replace_list");
        star.put("columns", false);
        star.putNull("expr");
        star.putArray("qualified_exclude_list");
        star.putArray("rename_list");
        innerSelect.putArray("select_list").add(star);

        ObjectNode fromTable = originalTableNode.deepCopy();
        fromTable.put(FIELD_ALIAS, "");
        innerSelect.set(FIELD_FROM_TABLE, fromTable);
        innerSelect.set(FIELD_WHERE_CLAUSE, filter);
        innerSelect.putArray("group_expressions");
        innerSelect.putArray("group_sets");
        innerSelect.put("aggregate_handling", "STANDARD_HANDLING");
        innerSelect.putNull("having");
        innerSelect.putNull("sample");
        innerSelect.putNull("qualify");

        ObjectNode queryWrapper = f.objectNode();
        queryWrapper.set(FIELD_NODE, innerSelect);
        queryWrapper.putArray("named_param_map");

        ObjectNode cteValue = f.objectNode();
        cteValue.putArray("aliases");
        cteValue.set("query", queryWrapper);
        cteValue.put("materialized", "CTE_MATERIALIZE_DEFAULT");
        cteValue.putArray("key_targets");

        ObjectNode entry = f.objectNode();
        entry.put("key", cteKey);
        entry.set("value", cteValue);
        return entry;
    }
}

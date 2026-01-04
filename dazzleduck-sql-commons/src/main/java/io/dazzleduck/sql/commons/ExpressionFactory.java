package io.dazzleduck.sql.commons;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.List;

import static io.dazzleduck.sql.commons.ExpressionConstants.*;

/**
 * Factory for creating DuckDB SQL Abstract Syntax Tree (AST) expression nodes.
 *
 * <p>This factory provides methods to programmatically construct SQL expression nodes
 * in Jackson JSON format, which can be serialized and executed by DuckDB's
 * {@code json_deserialize_sql} function.
 *
 * <p>The JSON AST structure follows DuckDB's logical plan format, where each node contains:
 * <ul>
 *   <li>{@code class} - The node class (e.g., CONSTANT, COMPARISON, CONJUNCTION)</li>
 *   <li>{@code type} - The specific type within the class (e.g., COMPARE_EQUAL, CONJUNCTION_AND)</li>
 *   <li>Type-specific fields (e.g., {@code left}/{@code right} for comparisons, {@code children} for conjunctions)</li>
 * </ul>
 *
 * <p><b>Usage Example:</b>
 * <pre>{@code
 * // Create: WHERE age > 18 AND city = 'NYC'
 * JsonNode ageRef = ExpressionFactory.reference(new String[]{"age"});
 * JsonNode eighteen = ExpressionFactory.constant(18);
 * JsonNode ageCheck = ExpressionFactory.greaterThanOrEqualExpr(ageRef, eighteen);
 *
 * JsonNode cityRef = ExpressionFactory.reference(new String[]{"city"});
 * JsonNode nyc = ExpressionFactory.constant("NYC");
 * JsonNode cityCheck = ExpressionFactory.equalExpr(cityRef, nyc);
 *
 * JsonNode whereClause = ExpressionFactory.andFilters(ageCheck, cityCheck);
 * }</pre>
 *
 * <p><b>Null Safety:</b> All methods validate their parameters and throw
 * {@link IllegalArgumentException} if null or invalid values are provided.
 *
 * <p><b>Supported Constant Types:</b> String, Integer, Long, Double, Float, Boolean, null
 *
 * @see ExpressionConstants for field and type constant definitions
 * @see Transformations for AST transformation utilities
 */
public class ExpressionFactory {

    /**
     * Creates a column reference expression node.
     *
     * <p>Generates a COLUMN_REF node that references one or more column names.
     * Multi-part names are used for qualified references (e.g., ["schema", "table", "column"]).
     *
     * @param value array of column name parts (e.g., ["age"] or ["users", "age"])
     * @return JsonNode representing the column reference
     * @throws IllegalArgumentException if value is null or empty
     */
    public static JsonNode reference(String[] value) {
        if (value == null) {
            throw new IllegalArgumentException("Column names array cannot be null");
        }
        if (value.length == 0) {
            throw new IllegalArgumentException("Column names array cannot be empty");
        }
        ObjectNode result = withClassType(COLUMN_REF_CLASS, COLUMN_REF_TYPE);
        ArrayNode arrayNode = new ArrayNode(JsonNodeFactory.instance);
        for (String string : value) {
            arrayNode.add(string);
        }
        result.set(FIELD_COLUMN_NAMES, arrayNode);
        return result;
    }

    /**
     * Creates an equality comparison expression (=).
     *
     * <p>Example: {@code age = 18}
     *
     * @param left the left operand
     * @param right the right operand
     * @return JsonNode representing the equality comparison
     * @throws IllegalArgumentException if left or right is null
     */
    public static JsonNode equalExpr(JsonNode left, JsonNode right) {
        if (left == null) {
            throw new IllegalArgumentException("Left operand cannot be null");
        }
        if (right == null) {
            throw new IllegalArgumentException("Right operand cannot be null");
        }
        return comparing(COMPARE_TYPE_EQUAL, left, right);
    }

    /**
     * Creates a less-than-or-equal comparison expression (<=).
     *
     * <p>Example: {@code age <= 65}
     *
     * @param left the left operand
     * @param right the right operand
     * @return JsonNode representing the <= comparison
     * @throws IllegalArgumentException if left or right is null
     */
    public static JsonNode lessThanOrEqualExpr(JsonNode left, JsonNode right) {
        if (left == null) {
            throw new IllegalArgumentException("Left operand cannot be null");
        }
        if (right == null) {
            throw new IllegalArgumentException("Right operand cannot be null");
        }
        return comparing(COMPARE_TYPE_LESSTHANOREQUALTO, left, right);
    }

    /**
     * Creates a greater-than-or-equal comparison expression (>=).
     *
     * <p>Example: {@code age >= 18}
     *
     * @param left the left operand
     * @param right the right operand
     * @return JsonNode representing the >= comparison
     * @throws IllegalArgumentException if left or right is null
     */
    public static JsonNode greaterThanOrEqualExpr(JsonNode left, JsonNode right) {
        if (left == null) {
            throw new IllegalArgumentException("Left operand cannot be null");
        }
        if (right == null) {
            throw new IllegalArgumentException("Right operand cannot be null");
        }
        return comparing(COMPARE_TYPE_GREATERTHANOREQUALTO, left, right);
    }

    /**
     * Creates a type cast expression.
     *
     * <p>Example: {@code CAST(value AS INTEGER)}
     *
     * @param child the expression to cast
     * @param castType the target type (e.g., "INTEGER", "VARCHAR", "BOOLEAN")
     * @return JsonNode representing the cast operation
     * @throws IllegalArgumentException if child is null or castType is null/empty
     */
    public static JsonNode cast(JsonNode child, String castType) {
        if (child == null) {
            throw new IllegalArgumentException("Child node cannot be null");
        }
        if (castType == null || castType.isEmpty()) {
            throw new IllegalArgumentException("Cast type cannot be null or empty");
        }
        ObjectNode result = withClassType(CAST_CLASS, CAST_TYPE_OPERATOR);
        result.set(FIELD_CHILD, child);
        ObjectNode ct = new ObjectNode(JsonNodeFactory.instance);
        ct.put(FIELD_ID, castType);
        ct.set(FIELD_TYPE_INFO, null);
        result.set(FIELD_CAST_TYPE, ct);
        result.put(FIELD_TRY_CAST, false);
        return result;
    }

    /**
     * Creates a CASE check (WHEN-THEN pair) for use in CASE expressions.
     *
     * <p>This is a helper method typically used internally by {@link #ifExpr}.
     *
     * @param when the condition expression
     * @param then the result expression if condition is true
     * @return JsonNode representing the WHEN-THEN pair
     * @throws IllegalArgumentException if when or then is null
     */
    public static JsonNode caseCheck(JsonNode when, JsonNode then) {
        if (when == null) {
            throw new IllegalArgumentException("When expression cannot be null");
        }
        if (then == null) {
            throw new IllegalArgumentException("Then expression cannot be null");
        }
        ObjectNode result = new ObjectNode(JsonNodeFactory.instance);
        result.set(FIELD_WHEN_EXPR, when);
        result.set(FIELD_THEN_EXPR, then);
        return result;
    }

    /**
     * Creates an IF expression (CASE WHEN condition THEN then ELSE elseExpression END).
     *
     * <p>Example: {@code IF(age >= 18, 'adult', 'minor')}
     *
     * @param condition the condition to evaluate
     * @param then the result if condition is true
     * @param elseExpression the result if condition is false
     * @return JsonNode representing the IF expression
     * @throws IllegalArgumentException if any parameter is null
     */
    public static JsonNode ifExpr(JsonNode condition,
                                  JsonNode then,
                                  JsonNode elseExpression) {
        if (condition == null) {
            throw new IllegalArgumentException("Condition cannot be null");
        }
        if (then == null) {
            throw new IllegalArgumentException("Then expression cannot be null");
        }
        if (elseExpression == null) {
            throw new IllegalArgumentException("Else expression cannot be null");
        }
        ObjectNode result = withClassType(CASE_CLASS, CASE_TYPE_EXPR);
        ArrayNode caseChecks = new ArrayNode(JsonNodeFactory.instance);
        caseChecks.add(caseCheck(condition, then));
        result.set(FIELD_CASE_CHECKS, caseChecks);
        result.set(FIELD_ELSE_EXPR, elseExpression);
        return result;
    }

    /**
     * Creates a boolean TRUE constant expression.
     *
     * @return JsonNode representing TRUE
     */
    public static JsonNode trueExpression() {
        return cast(constant(CONSTANT_VALUE_TRUE), TYPE_BOOLEAN);
    }

    /**
     * Creates a boolean FALSE constant expression.
     *
     * @return JsonNode representing FALSE
     */
    public static JsonNode falseExpression() {
        return cast(constant(CONSTANT_VALUE_FALSE), TYPE_BOOLEAN);
    }

    /**
     * Creates a constant value expression.
     *
     * <p><b>Supported types:</b> String, Integer, Long, Double, Float, Boolean, null
     *
     * <p>Examples:
     * <ul>
     *   <li>{@code constant("hello")} - VARCHAR constant</li>
     *   <li>{@code constant(42)} - INTEGER constant</li>
     *   <li>{@code constant(3.14)} - DOUBLE constant</li>
     *   <li>{@code constant(true)} - BOOLEAN constant</li>
     *   <li>{@code constant(null)} - NULL constant</li>
     * </ul>
     *
     * @param value the constant value (can be null)
     * @return JsonNode representing the constant
     * @throws IllegalArgumentException if value type is not supported
     */
    public static JsonNode constant(Object value) {
        ObjectNode result = withClassType(CONSTANT_CLASS, CONSTANT_TYPE);
        JsonNode valueNode = constantValueNode(value);
        result.set(FIELD_VALUE, valueNode);
        return result;
    }

    /**
     * Creates an AND conjunction of two filter expressions.
     *
     * <p>Example: {@code condition1 AND condition2}
     *
     * @param leftFilter the left filter
     * @param rightFilter the right filter
     * @return JsonNode representing the AND conjunction
     * @throws IllegalArgumentException if either filter is null
     * @see #andFilters(JsonNode[]) for joining multiple filters
     */
    public static JsonNode andFilters(JsonNode leftFilter, JsonNode rightFilter) {
        if (leftFilter == null) {
            throw new IllegalArgumentException("Left filter cannot be null");
        }
        if (rightFilter == null) {
            throw new IllegalArgumentException("Right filter cannot be null");
        }
        return andFilters(new JsonNode[]{leftFilter, rightFilter});
    }

    /**
     * Creates an AND conjunction of multiple filter expressions.
     *
     * <p>Example: {@code condition1 AND condition2 AND condition3}
     *
     * @param children array of filter expressions to join with AND
     * @return JsonNode representing the AND conjunction
     * @throws IllegalArgumentException if children is null, empty, or contains null elements
     */
    public static JsonNode andFilters(JsonNode[] children) {
        if (children == null) {
            throw new IllegalArgumentException("Children array cannot be null");
        }
        if (children.length == 0) {
            throw new IllegalArgumentException("Children array cannot be empty - AND conjunction requires at least one child");
        }
        ObjectNode result = withClassType(CONJUNCTION_CLASS, CONJUNCTION_TYPE_AND);
        ArrayNode arrayNode = new ArrayNode(JsonNodeFactory.instance);
        for(var c : children) {
            if (c == null) {
                throw new IllegalArgumentException("Child filter cannot be null");
            }
            arrayNode.add(c);
        }
        result.set(FIELD_CHILDREN, arrayNode);
        return result;
    }

    /**
     * Creates an IN expression with a static list of values.
     *
     * <p>Example: {@code city IN ('NYC', 'LA', 'SF')}
     *
     * @param reference the column reference to check
     * @param elements the list of values to check against
     * @param <T> the type of elements (must be a supported constant type)
     * @return JsonNode representing the IN expression
     * @throws IllegalArgumentException if reference or elements is null, or elements is empty
     */
    public static <T> JsonNode inStaticList(JsonNode reference, List<T> elements) {
        if (reference == null) {
            throw new IllegalArgumentException("Reference cannot be null");
        }
        if (elements == null) {
            throw new IllegalArgumentException("Elements list cannot be null");
        }
        if (elements.isEmpty()) {
            throw new IllegalArgumentException("Elements list cannot be empty - IN clause requires at least one element");
        }
        ObjectNode result = withClassType(OPERATOR_CLASS, COMPARE_IN_TYPE);
        var children = new ArrayNode(JsonNodeFactory.instance);
        children.add(reference);
        for (var e : elements) {
            children.add(constant(e));
        }
        result.set(FIELD_CHILDREN, children);
        return result;
    }

    /**
     * Creates an OR conjunction of two filter expressions.
     *
     * <p>Example: {@code condition1 OR condition2}
     *
     * @param leftFilter the left filter
     * @param rightFilter the right filter
     * @return JsonNode representing the OR conjunction
     * @throws IllegalArgumentException if either filter is null
     */
    public static JsonNode orFilters(JsonNode leftFilter, JsonNode rightFilter) {
        if (leftFilter == null) {
            throw new IllegalArgumentException("Left filter cannot be null");
        }
        if (rightFilter == null) {
            throw new IllegalArgumentException("Right filter cannot be null");
        }
        ObjectNode result = withClassType(CONJUNCTION_CLASS, CONJUNCTION_TYPE_OR);
        ArrayNode arrayNode = new ArrayNode(JsonNodeFactory.instance);
        arrayNode.add(leftFilter);
        arrayNode.add(rightFilter);
        result.set(FIELD_CHILDREN, arrayNode);
        return result;
    }

    /**
     * Creates a function call expression.
     *
     * <p>Example: {@code upper(name)} or {@code concat(first_name, ' ', last_name)}
     *
     * @param name the function name (e.g., "upper", "concat", "count")
     * @param schema the schema name (use empty string for default)
     * @param catalog the catalog name (use empty string for default)
     * @param children the function arguments as an ArrayNode
     * @return JsonNode representing the function call
     * @throws IllegalArgumentException if any parameter is null, or name is empty
     */
    public static JsonNode createFunction(String name, String schema, String catalog, JsonNode children) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Function name cannot be null or empty");
        }
        if (schema == null) {
            throw new IllegalArgumentException("Schema cannot be null (use empty string for default)");
        }
        if (catalog == null) {
            throw new IllegalArgumentException("Catalog cannot be null (use empty string for default)");
        }
        if (children == null) {
            throw new IllegalArgumentException("Children cannot be null (use empty ArrayNode for no arguments)");
        }
        var orderBy = new ObjectNode(JsonNodeFactory.instance);
        orderBy.put(FIELD_TYPE, TYPE_ORDER_MODIFIER);
        orderBy.set(FIELD_ORDERS, new ArrayNode(JsonNodeFactory.instance));
        var input = withClassType(FUNCTION_CLASS, FUNCTION_TYPE);
        input.put(FIELD_FUNCTION_NAME, name);
        input.put(FIELD_SCHEMA_NAME, schema);
        input.put(FIELD_CATALOG_NAME, catalog);
        input.put(FIELD_DISTINCT, false);
        input.put(FIELD_IS_OPERATOR, false);
        input.put(FIELD_EXPORT_STATE, false);
        input.set(FIELD_CHILDREN, children);
        input.set(FIELD_FILTER, null);
        input.set(FIELD_ORDER_BYS, orderBy);
        return input;
    }

    private static JsonNode constantValueNode(Object value) {
        ObjectNode valueNode = new ObjectNode(JsonNodeFactory.instance);
        ObjectNode type = new ObjectNode(JsonNodeFactory.instance);
        valueNode.set(FIELD_TYPE, type);
        type.set(FIELD_TYPE_INFO, null);
        if (value == null) {
            valueNode.put(FIELD_IS_NULL, true);
            type.put(FIELD_ID, TYPE_NULL);
        } else {
            valueNode.put(FIELD_IS_NULL, false);
            if (value instanceof String string) {
                type.put(FIELD_ID, TYPE_VARCHAR);
                valueNode.put(FIELD_VALUE, string);
            } else if (value instanceof Integer i) {
                type.put(FIELD_ID, TYPE_INTEGER);
                valueNode.put(FIELD_VALUE, i);
            } else if (value instanceof Long l) {
                type.put(FIELD_ID, TYPE_BIGINT);
                valueNode.put(FIELD_VALUE, l);
            } else if (value instanceof Double d) {
                type.put(FIELD_ID, TYPE_DOUBLE);
                valueNode.put(FIELD_VALUE, d);
            } else if (value instanceof Float f) {
                type.put(FIELD_ID, TYPE_FLOAT);
                valueNode.put(FIELD_VALUE, f);
            } else if (value instanceof Boolean b) {
                type.put(FIELD_ID, TYPE_BOOLEAN);
                valueNode.put(FIELD_VALUE, b);
            } else {
                throw new IllegalArgumentException(
                    "Unsupported constant type: " + value.getClass().getName() +
                    ". Supported types: String, Integer, Long, Double, Float, Boolean, null");
            }
        }
        return valueNode;
    }

    private static ObjectNode withClassType(String clazz, String type) {
        if (clazz == null) {
            throw new IllegalArgumentException("Class cannot be null");
        }
        if (type == null) {
            throw new IllegalArgumentException("Type cannot be null");
        }
        ObjectNode result = new ObjectNode(JsonNodeFactory.instance);
        result.put(FIELD_CLASS, clazz);
        result.put(FIELD_TYPE, type);
        result.put(FIELD_ALIAS, "");
        result.put(FIELD_QUERY_LOCATION, 0);
        return result;
    }

    private static JsonNode comparing(String type, JsonNode left, JsonNode right) {
        // Null checks are done in the calling methods (equalExpr, etc.)
        ObjectNode node = withClassType(COMPARISON_CLASS, type);
        node.set(FIELD_LEFT, left);
        node.set(FIELD_RIGHT, right);
        return node;
    }
}

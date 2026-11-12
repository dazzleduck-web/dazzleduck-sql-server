package io.dazzleduck.sql.commons;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static io.dazzleduck.sql.commons.ExpressionConstants.*;

/**
 * Reads function and constant nodes in both shapes of DuckDB's {@code json_serialize_sql} output.
 *
 * <p>DuckDB 2.0 changed two expression nodes:
 * <ul>
 *   <li>a function's arguments moved from {@code children: [expr, ...]} to
 *       {@code arguments: [{"name": "", "expression": expr}, ...]};</li>
 *   <li>a constant's typed {@code value: {type: {id, type_info}, is_null, value}} became an untyped
 *       {@code literal: {kind, text}}, with {@code kind} one of {@code STRING}, {@code INTEGER},
 *       {@code NUMERIC}, {@code BOOLEAN} and {@code NULL_LITERAL}.</li>
 * </ul>
 * {@link ExpressionFactory} still builds the 1.5 shape, which DuckDB 2.0's {@code json_deserialize_sql}
 * accepts, so one tree can hold both: a parsed query with a filter or limit built here. Every read of
 * a function's arguments or a constant's value goes through this class.
 */
public final class AstNodes {

    public static final String FIELD_ARGUMENTS = "arguments";
    public static final String FIELD_EXPRESSION = "expression";
    public static final String FIELD_LITERAL = "literal";
    public static final String FIELD_KIND = "kind";
    public static final String FIELD_TEXT = "text";
    public static final String FIELD_QUERY_NODE = "query_node";
    public static final String FIELD_QUALIFIED_NAME = "qualified_name";

    public static final String LITERAL_STRING = "STRING";
    public static final String LITERAL_INTEGER = "INTEGER";
    public static final String LITERAL_NUMERIC = "NUMERIC";
    public static final String LITERAL_BOOLEAN = "BOOLEAN";
    public static final String LITERAL_NULL = "NULL_LITERAL";

    private static final BigInteger INT_MIN = BigInteger.valueOf(Integer.MIN_VALUE);
    private static final BigInteger INT_MAX = BigInteger.valueOf(Integer.MAX_VALUE);
    private static final BigInteger LONG_MIN = BigInteger.valueOf(Long.MIN_VALUE);
    private static final BigInteger LONG_MAX = BigInteger.valueOf(Long.MAX_VALUE);

    private AstNodes() {
    }

    /** A function's argument expressions, in order; empty when it has none. */
    public static List<JsonNode> arguments(JsonNode function) {
        List<JsonNode> result = new ArrayList<>();
        JsonNode arguments = function.get(FIELD_ARGUMENTS);
        if (arguments != null && arguments.isArray()) {
            for (JsonNode argument : arguments) {
                result.add(argument.get(FIELD_EXPRESSION));
            }
            return result;
        }
        JsonNode children = function.get(FIELD_CHILDREN);
        if (children != null && children.isArray()) {
            children.forEach(result::add);
        }
        return result;
    }

    /**
     * Replaces a function's argument expressions, keeping the shape the node has: a 2.0 node keeps
     * {@code arguments} (each argument's name is kept by position, and is empty for an added one), a
     * 1.5 node keeps {@code children}.
     */
    public static void setArguments(ObjectNode function, List<JsonNode> expressions) {
        JsonNode existing = function.get(FIELD_ARGUMENTS);
        if (existing == null || !existing.isArray()) {
            ArrayNode children = JsonNodeFactory.instance.arrayNode();
            expressions.forEach(children::add);
            function.set(FIELD_CHILDREN, children);
            return;
        }
        ArrayNode arguments = JsonNodeFactory.instance.arrayNode();
        for (int i = 0; i < expressions.size(); i++) {
            ObjectNode argument = arguments.addObject();
            JsonNode name = i < existing.size() ? existing.get(i).get("name") : null;
            argument.put("name", name == null ? "" : name.asText());
            argument.set(FIELD_EXPRESSION, expressions.get(i));
        }
        function.set(FIELD_ARGUMENTS, arguments);
    }

    /** True for a CONSTANT node whose value is NULL. */
    public static boolean isNullConstant(JsonNode node) {
        if (node == null || !CONSTANT_CLASS.equals(node.path(FIELD_CLASS).asText())) {
            return false;
        }
        JsonNode literal = node.get(FIELD_LITERAL);
        if (literal != null) {
            return LITERAL_NULL.equals(literal.path(FIELD_KIND).asText());
        }
        return node.path(FIELD_VALUE).path(FIELD_IS_NULL).asBoolean(false);
    }

    /** A CONSTANT node's value as text, or {@code null} for a NULL constant or a node that is not one. */
    public static String constantText(JsonNode node) {
        if (node == null || isNullConstant(node)) {
            return null;
        }
        JsonNode literal = node.get(FIELD_LITERAL);
        if (literal != null) {
            JsonNode text = literal.get(FIELD_TEXT);
            return text == null ? null : text.asText();
        }
        JsonNode value = node.path(FIELD_VALUE).get(FIELD_VALUE);
        return value == null || value.isNull() ? null : value.asText();
    }

    /**
     * A CONSTANT node's DuckDB type id: {@code value.type.id} in the 1.5 shape. A 2.0 literal is
     * untyped, so its type is inferred the way DuckDB binds it: a {@code STRING} is {@code VARCHAR},
     * an {@code INTEGER} is the smallest of {@code INTEGER}, {@code BIGINT} and {@code HUGEINT} that
     * holds it, a {@code NUMERIC} is {@code DOUBLE} with an exponent and {@code DECIMAL} without, and
     * a NULL is {@code NULL}. Returns {@code null} for a node that is not a constant.
     */
    public static String constantTypeId(JsonNode node) {
        if (node == null || !CONSTANT_CLASS.equals(node.path(FIELD_CLASS).asText())) {
            return null;
        }
        JsonNode literal = node.get(FIELD_LITERAL);
        if (literal == null) {
            JsonNode id = node.path(FIELD_VALUE).path(FIELD_TYPE).get(FIELD_ID);
            return id == null ? null : id.asText();
        }
        String text = literal.path(FIELD_TEXT).asText();
        return switch (literal.path(FIELD_KIND).asText()) {
            case LITERAL_STRING -> TYPE_VARCHAR;
            case LITERAL_BOOLEAN -> "BOOLEAN";
            case LITERAL_NULL -> "NULL";
            case LITERAL_INTEGER -> integerTypeId(text);
            case LITERAL_NUMERIC -> text.indexOf('e') >= 0 || text.indexOf('E') >= 0 ? "DOUBLE" : TYPE_DECIMAL;
            default -> null;
        };
    }

    /** A CONSTANT node's value when it is an integer literal that fits a {@code long}, else {@code null}. */
    public static Long integerConstant(JsonNode node) {
        if (node == null || isNullConstant(node)) {
            return null;
        }
        JsonNode literal = node.get(FIELD_LITERAL);
        if (literal != null) {
            if (!LITERAL_INTEGER.equals(literal.path(FIELD_KIND).asText())) {
                return null;
            }
            try {
                return Long.parseLong(integerText(literal.path(FIELD_TEXT).asText()));
            } catch (NumberFormatException e) {
                return null;
            }
        }
        JsonNode value = node.path(FIELD_VALUE).get(FIELD_VALUE);
        return value != null && value.isIntegralNumber() && value.canConvertToLong() ? value.asLong() : null;
    }

    /** DuckDB's type-name aliases, as {@code type_expr} keeps them, to the type id 1.5 serialized. */
    private static final java.util.Map<String, String> TYPE_ALIASES = java.util.Map.ofEntries(
            java.util.Map.entry("TEXT", "VARCHAR"), java.util.Map.entry("STRING", "VARCHAR"),
            java.util.Map.entry("CHAR", "VARCHAR"), java.util.Map.entry("BPCHAR", "VARCHAR"),
            java.util.Map.entry("INT", "INTEGER"), java.util.Map.entry("INT4", "INTEGER"),
            java.util.Map.entry("SIGNED", "INTEGER"), java.util.Map.entry("INT8", "BIGINT"),
            java.util.Map.entry("LONG", "BIGINT"), java.util.Map.entry("INT2", "SMALLINT"),
            java.util.Map.entry("SHORT", "SMALLINT"), java.util.Map.entry("INT1", "TINYINT"),
            java.util.Map.entry("BOOL", "BOOLEAN"), java.util.Map.entry("LOGICAL", "BOOLEAN"),
            java.util.Map.entry("FLOAT4", "FLOAT"), java.util.Map.entry("REAL", "FLOAT"),
            java.util.Map.entry("FLOAT8", "DOUBLE"), java.util.Map.entry("NUMERIC", "DECIMAL"),
            java.util.Map.entry("BYTEA", "BLOB"), java.util.Map.entry("BINARY", "BLOB"),
            java.util.Map.entry("VARBINARY", "BLOB"), java.util.Map.entry("DATETIME", "TIMESTAMP"));

    /**
     * A CAST node's target type in the 1.5 shape, {@code {id, type_info}}: its {@code cast_type} in
     * 1.5, or its 2.0 {@code type_expr} (a {@code TYPE} node holding the type as written) converted:
     * aliases become DuckDB's type ids, and {@code LIST}, {@code ARRAY}, {@code STRUCT}, {@code MAP}
     * and {@code DECIMAL} get the {@code type_info} 1.5 gave them. {@code null} when it has neither.
     */
    public static JsonNode castType(JsonNode cast) {
        JsonNode castType = cast.get("cast_type");
        if (castType != null && !castType.isNull()) {
            return castType;
        }
        JsonNode typeExpr = cast.get("type_expr");
        return typeExpr == null || typeExpr.isNull() ? null : logicalType(typeExpr);
    }

    private static ObjectNode logicalType(JsonNode typeNode) {
        JsonNodeFactory f = JsonNodeFactory.instance;
        String name = typeNode.path("type_name").asText().toUpperCase(java.util.Locale.ROOT);
        String id = TYPE_ALIASES.getOrDefault(name, name);
        List<JsonNode> children = new ArrayList<>();
        typeNode.path(FIELD_CHILDREN).forEach(children::add);
        ObjectNode result = f.objectNode();
        result.put(FIELD_ID, id);
        ObjectNode typeInfo = f.objectNode();
        switch (id) {
            case "LIST" -> typeInfo.set("child_type", logicalType(children.get(0)));
            case "ARRAY" -> {
                typeInfo.set("child_type", logicalType(children.get(0)));
                typeInfo.put("size", children.size() > 1 ? integerConstant(children.get(1)) : 0L);
            }
            case "STRUCT" -> typeInfo.set("child_types", fieldTypes(children, null));
            case "MAP" -> {
                ObjectNode entry = f.objectNode();
                entry.put(FIELD_ID, "STRUCT");
                entry.set(FIELD_TYPE_INFO, f.objectNode().set("child_types",
                        fieldTypes(children, new String[]{"key", "value"})));
                typeInfo.set("child_type", entry);
            }
            case "DECIMAL" -> {
                // DECIMAL is DECIMAL(18,3); DECIMAL(w) is DECIMAL(w,0).
                typeInfo.put("width", children.isEmpty() ? Long.valueOf(18) : integerConstant(children.get(0)));
                typeInfo.put("scale", children.isEmpty() ? Long.valueOf(3)
                        : children.size() < 2 ? Long.valueOf(0) : integerConstant(children.get(1)));
            }
            default -> typeInfo = null;
        }
        result.set(FIELD_TYPE_INFO, typeInfo);
        return result;
    }

    private static ArrayNode fieldTypes(List<JsonNode> children, String[] names) {
        ArrayNode fields = JsonNodeFactory.instance.arrayNode();
        for (int i = 0; i < children.size(); i++) {
            ObjectNode field = fields.addObject();
            field.put("first", names != null ? names[i] : children.get(i).path(FIELD_ALIAS).asText());
            field.set("second", logicalType(children.get(i)));
        }
        return fields;
    }

    /**
     * True for a {@code LIMIT n%} modifier: a {@code LIMIT_PERCENT_MODIFIER} in DuckDB 1.5, a
     * {@code LIMIT_MODIFIER} with {@code limit_type} {@code PERCENTAGE} in 2.0.
     */
    public static boolean isPercentLimit(JsonNode modifier) {
        String type = modifier.path(FIELD_TYPE).asText();
        return LIMIT_PERCENT_MODIFIER_TYPE.equals(type)
                || (LIMIT_MODIFIER_TYPE.equals(type) && "PERCENTAGE".equals(modifier.path("limit_type").asText()));
    }

    /**
     * Drops a node's {@code qualified_name}, after its {@code catalog_name}, {@code schema_name},
     * {@code table_name} or {@code function_name} was changed. DuckDB 2.0 adds {@code qualified_name}
     * to table, function and type nodes, and {@code json_deserialize_sql} renders the name from it
     * when it is present, so a rename that leaves it in place is silently ignored. Without it the
     * name is rendered from the other fields, as in 1.5.
     */
    public static ObjectNode forgetQualifiedName(ObjectNode node) {
        node.remove(FIELD_QUALIFIED_NAME);
        return node;
    }

    /**
     * The body (a query node) of a {@code cte_map.map} entry: {@code value.query_node} in DuckDB 2.0,
     * {@code value.query.node} in 1.5; {@code null} when it has neither.
     */
    public static JsonNode cteBody(JsonNode cteEntry) {
        JsonNode value = cteEntry == null ? null : cteEntry.get(FIELD_VALUE);
        if (value == null) {
            return null;
        }
        JsonNode body = value.get(FIELD_QUERY_NODE);
        if (body != null && !body.isNull()) {
            return body;
        }
        JsonNode query = value.get(FIELD_QUERY);
        return query == null ? null : query.get(FIELD_NODE);
    }

    /**
     * The branches of a set operation node, in order. DuckDB 2.0 makes it n-ary, in {@code children}
     * with {@code left} and {@code right} null; 1.5 nests binary {@code left}/{@code right} nodes.
     */
    public static List<JsonNode> setOperationBranches(JsonNode setOperation) {
        List<JsonNode> result = new ArrayList<>();
        JsonNode children = setOperation.get(FIELD_CHILDREN);
        if (children != null && children.isArray() && !children.isEmpty()) {
            children.forEach(result::add);
            return result;
        }
        for (String side : new String[]{FIELD_LEFT, FIELD_RIGHT}) {
            JsonNode branch = setOperation.get(side);
            if (branch != null && !branch.isNull()) {
                result.add(branch);
            }
        }
        return result;
    }

    /** An INTEGER literal's text as Java parses it: DuckDB accepts digit separators ({@code 1_000}). */
    private static String integerText(String text) {
        return text.indexOf('_') >= 0 ? text.replace("_", "") : text;
    }

    private static String integerTypeId(String text) {
        BigInteger value;
        try {
            value = new BigInteger(integerText(text));
        } catch (NumberFormatException e) {
            return TYPE_DECIMAL;
        }
        if (value.compareTo(INT_MIN) >= 0 && value.compareTo(INT_MAX) <= 0) {
            return TYPE_INTEGER;
        }
        if (value.compareTo(LONG_MIN) >= 0 && value.compareTo(LONG_MAX) <= 0) {
            return "BIGINT";
        }
        return "HUGEINT";
    }
}

package io.dazzleduck.sql.commons;

public class ExpressionConstants {
    public static final String CONSTANT_CLASS = "CONSTANT";
    public static final String CONSTANT_TYPE = "VALUE_CONSTANT";
    public static final String COLUMN_REF_CLASS = "COLUMN_REF";
    public static final String COLUMN_REF_TYPE = "COLUMN_REF";
    public static final String COMPARISON_CLASS = "COMPARISON";
    public static final String OPERATOR_CLASS = "OPERATOR";
    public static final String SUBQUERY_CLASS = "SUBQUERY";

    public static final String SUBQUERY_TYPE = "SUBQUERY";
    public static final String COMPARE_TYPE_EQUAL = "COMPARE_EQUAL";
    public static final String COMPARE_TYPE_LESSTHANOREQUALTO = "COMPARE_LESSTHANOREQUALTO";
    public static final String COMPARE_TYPE_GREATERTHANOREQUALTO = "COMPARE_GREATERTHANOREQUALTO";
    public static final String COMPARE_TYPE_LESSTHAN = "COMPARE_LESSTHAN";
    public static final String COMPARE_TYPE_GREATERTHAN = "COMPARE_GREATERTHAN";
    public static final String COMPARE_IN_TYPE = "COMPARE_IN";
    public static final String CASE_CLASS = "CASE";
    public static final String CASE_TYPE_EXPR = "CASE_EXPR";
    public static final String CAST_CLASS = "CAST";
    public static final String CAST_TYPE_OPERATOR = "OPERATOR_CAST";
    public static final String CONJUNCTION_CLASS = "CONJUNCTION";
    public static final String CONJUNCTION_TYPE_AND = "CONJUNCTION_AND";
    public static final String CONJUNCTION_TYPE_OR = "CONJUNCTION_OR";
    public static final String SELECT_NODE_TYPE = "SELECT_NODE";
    public static final String FUNCTION_CLASS = "FUNCTION";
    public static final String FUNCTION_TYPE = "FUNCTION";

    public static final String TABLE_FUNCTION_TYPE = "TABLE_FUNCTION";
    public static final String BASE_TABLE_TYPE = "BASE_TABLE";

    // Node type constants for AST traversal
    public static final String NODE_TYPE_BASE_TABLE = "BASE_TABLE";
    public static final String NODE_TYPE_TABLE_FUNCTION = "TABLE_FUNCTION";
    public static final String NODE_TYPE_SUBQUERY = "SUBQUERY";
    public static final String NODE_TYPE_SELECT_NODE = "SELECT_NODE";
    public static final String NODE_TYPE_SET_OPERATION_NODE = "SET_OPERATION_NODE";

    // Field name constants for JSON AST nodes
    public static final String FIELD_CLASS = "class";
    public static final String FIELD_TYPE = "type";
    public static final String FIELD_CHILDREN = "children";
    public static final String FIELD_CHILD = "child";
    public static final String FIELD_COLUMN_NAMES = "column_names";
    public static final String FIELD_FUNCTION_NAME = "function_name";
    public static final String FIELD_FUNCTION = "function";
    public static final String FIELD_VALUE = "value";
    public static final String FIELD_WHERE_CLAUSE = "where_clause";
    public static final String FIELD_FROM_TABLE = "from_table";
    public static final String FIELD_TABLE_NAME = "table_name";
    public static final String FIELD_SCHEMA_NAME = "schema_name";
    public static final String FIELD_CATALOG_NAME = "catalog_name";
    public static final String FIELD_LEFT = "left";
    public static final String FIELD_RIGHT = "right";
    public static final String FIELD_STATEMENTS = "statements";
    public static final String FIELD_NODE = "node";
    public static final String FIELD_SUBQUERY = "subquery";
    public static final String FIELD_ALIAS = "alias";
    public static final String FIELD_ID = "id";
    public static final String FIELD_TYPE_INFO = "type_info";
    public static final String FIELD_CAST_TYPE = "cast_type";
    public static final String FIELD_TRY_CAST = "try_cast";
    public static final String FIELD_WHEN_EXPR = "when_expr";
    public static final String FIELD_THEN_EXPR = "then_expr";
    public static final String FIELD_CASE_CHECKS = "case_checks";
    public static final String FIELD_ELSE_EXPR = "else_expr";
    public static final String FIELD_ORDERS = "orders";
    public static final String FIELD_DISTINCT = "distinct";
    public static final String FIELD_IS_OPERATOR = "is_operator";
    public static final String FIELD_EXPORT_STATE = "export_state";
    public static final String FIELD_FILTER = "filter";
    public static final String FIELD_ORDER_BYS = "order_bys";
    public static final String FIELD_IS_NULL = "is_null";
    public static final String FIELD_QUERY_LOCATION = "query_location";

    // Type ID constants for data types
    public static final String TYPE_VARCHAR = "VARCHAR";
    public static final String TYPE_INTEGER = "INTEGER";
    public static final String TYPE_DECIMAL = "DECIMAL";
    public static final String TYPE_STRUCT = "STRUCT";
    public static final String TYPE_MAP = "MAP";
    public static final String TYPE_LIST = "LIST";
    public static final String TYPE_BOOLEAN = "BOOLEAN";
    public static final String TYPE_NULL = "NULL";
    public static final String TYPE_BIGINT = "BIGINT";
    public static final String TYPE_DOUBLE = "DOUBLE";
    public static final String TYPE_FLOAT = "FLOAT";
    public static final String TYPE_ORDER_MODIFIER = "ORDER_MODIFIER";

    // Constant values for boolean expressions
    public static final String CONSTANT_VALUE_TRUE = "t";
    public static final String CONSTANT_VALUE_FALSE = "f";

}

package io.dazzleduck.sql.commons.delta;

import com.fasterxml.jackson.databind.JsonNode;
import io.dazzleduck.sql.commons.AstNodes;
import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.expressions.*;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.util.Arrays;

import java.util.Map;
import java.util.function.Function;


import static io.dazzleduck.sql.commons.delta.DataType.getCastRules;


public class Transformations {

    // TODO
    // 1. Binary Type
    // 2. Decimal Type
    // 3. ArrayType, MapType, StructType, StructField
    // https://github.com/apache/spark/blob/87b9866903baa3b291058b3613f9954ec62c178c/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/Cast.scala#L4
    private static final Map<String, Function<Expression, Object>> parsingFunctions = Map.ofEntries(
            Map.entry(DataType.TINY_INT, expr -> Byte.parseByte(expr.toString())),
            Map.entry(DataType.SMALL_INT, expr -> Short.parseShort(expr.toString())),
            Map.entry(DataType.INT, expr -> Integer.parseInt(expr.toString())),
            Map.entry(DataType.BIG_INT, expr -> Long.parseLong(expr.toString())),
            Map.entry(DataType.FLOAT, expr -> Float.parseFloat(expr.toString())),
            Map.entry(DataType.DOUBLE, expr -> Double.parseDouble(expr.toString())),
            Map.entry(DataType.DECIMAL, expr -> new java.math.BigDecimal(expr.toString())),
            Map.entry(DataType.STRING, expr -> expr.toString()),
            Map.entry(DataType.BINARY, expr -> {
                // Assuming Expression stores a byte[] as its value
                if (expr instanceof Literal) {
                    return ((Literal) expr).getDataType(); // Replace this with the actual method
                }
                throw new IllegalArgumentException("Invalid expression type for BINARY: " + expr);
            }),
            Map.entry(DataType.BOOLEAN, expr -> Boolean.parseBoolean(expr.toString())),
            Map.entry(DataType.TIMESTAMP, expr -> {
                return java.sql.Timestamp.valueOf(expr.toString()).getTime() * 1000; // Convert to microseconds
            }),
            Map.entry(DataType.TIMESTAMP_NTZ, expr -> java.time.LocalDateTime.parse(expr.toString())),
            Map.entry(DataType.DATE, expr -> Date.parse(expr.toString()))

    );

    private static final Map<String, Function<Object, Expression>> castingFunctions = Map.ofEntries(
            Map.entry(DataType.TINY_INT, DataTypeConverter::toTinyInt),
            Map.entry(DataType.SMALL_INT, DataTypeConverter::toSmallInt),
            Map.entry(DataType.INT, DataTypeConverter::toInt),
            Map.entry(DataType.BIG_INT, DataTypeConverter::toBigInt),
            Map.entry(DataType.FLOAT, DataTypeConverter::toFloat),
            Map.entry(DataType.DOUBLE, DataTypeConverter::toDouble),
            Map.entry(DataType.STRING, DataTypeConverter::toString),
            Map.entry(DataType.BINARY, DataTypeConverter::toBinary),
            Map.entry(DataType.BOOLEAN, DataTypeConverter::toBoolean),
            Map.entry(DataType.TIMESTAMP, DataTypeConverter::toTimestamp),
            Map.entry(DataType.TIMESTAMP_NTZ, DataTypeConverter::toTimestamp),
            Map.entry(DataType.DATE, DataTypeConverter::toDate),

            // TODO Add Decimal support
            Map.entry(DataType.DECIMAL, value -> {
                // Default precision and scale
                int defaultPrecision = 10;
                int defaultScale = 0;
                // Call the toDecimal method with default precision and scale
                return DataTypeConverter.toDecimal(value, defaultPrecision, defaultScale);
            })

    );

    /**
     * Perform a two-step cast: parse the source type and then cast to the target type.
     *
     * @param expr       The expression to cast.
     * @param sourceType The source type of the expression (e.g., TINYINT).
     * @param targetType The target type to cast to (e.g., INT).
     * @return The casted expression.
     */
    private static Expression castExpression(Expression expr, String sourceType, String targetType) {
        // Step 1: Parse the source type
        Function<Expression, Object> parseFunction = parsingFunctions.get(sourceType);
        if (parseFunction == null) {
            throw new UnsupportedOperationException("Unsupported source type: " + sourceType);
        }
        Object intermediateValue = parseFunction.apply(expr);

        // Step 2: Cast to the target type
        Function<Object, Expression> castFunction = castingFunctions.get(targetType);
        if (castFunction == null) {
            throw new UnsupportedOperationException("Unsupported target type: " + targetType);
        }
        return castFunction.apply(intermediateValue);
    }


    // Function to check if a type can be cast to another type
    private static boolean canCast(String fromType, String toType) {
        return getCastRules().get(fromType).contains(toType);
    }

    @Evolving
    public static final class Equal extends Predicate {
        public Equal(Expression left, Expression right) {
            super("=", Arrays.asList(left, right));
        }
    }

    @Evolving
    public static final class NotEqual extends Predicate {
        public NotEqual(Expression left, Expression right) {
            super("NOT", Arrays.asList(new Equal(left, right)));
        }
    }

    @Evolving
    public static final class GreaterThan extends Predicate {
        public GreaterThan(Expression left, Expression right) {
            super(">", Arrays.asList(left, right));
        }
    }

    @Evolving
    public static final class GreaterThanOrEqualTo extends Predicate {
        public GreaterThanOrEqualTo(Expression left, Expression right) {
            super(">=", Arrays.asList(left, right));
        }
    }

    @Evolving
    public static final class LessThan extends Predicate {
        public LessThan(Expression left, Expression right) {
            super("<", Arrays.asList(left, right));
        }
    }

    @Evolving
    public static final class CompareLessThanOrEqualTo extends Predicate {
        public CompareLessThanOrEqualTo(Expression left, Expression right) {
            super("<=", Arrays.asList(left, right));
        }
    }

    public static Expression toDeltaPredicate(JsonNode jsonPredicate) throws IOException {
        if(io.dazzleduck.sql.commons.Transformations.IS_CONSTANT.apply(jsonPredicate)) {
            return toLiteral(jsonPredicate);
        } else if (io.dazzleduck.sql.commons.Transformations.IS_REFERENCE.apply(jsonPredicate)) {
            return toReference(jsonPredicate);
        } else if(io.dazzleduck.sql.commons.Transformations.IS_COMPARISON.apply(jsonPredicate)){
            return toComparison(jsonPredicate);
        } else if(io.dazzleduck.sql.commons.Transformations.IS_CONJUNCTION_AND.apply(jsonPredicate) || io.dazzleduck.sql.commons.Transformations.IS_CONJUNCTION_OR.apply(jsonPredicate)  ) {
            return toConjunction(jsonPredicate);
        } else if(io.dazzleduck.sql.commons.Transformations.IS_CAST.apply(jsonPredicate)) {
            return toCast(jsonPredicate);
        }
        throw new UnsupportedOperationException("No transformation supported" + jsonPredicate);
    }

    private static Expression toComparison(JsonNode comparison) throws IOException {
        JsonNode left = comparison.get("left");
        JsonNode right = comparison.get("right");
        Expression leftExpr = toDeltaPredicate(left);
        Expression rightExpr = toDeltaPredicate(right);

        String comparisonType = comparison.get("type").asText();
        return switch (comparisonType) {
            case "COMPARE_EQUAL" -> new Equal(leftExpr, rightExpr);
            case "COMPARE_NOT_EQUAL" -> new NotEqual(leftExpr, rightExpr);
            case "COMPARE_GREATER_THAN" -> new GreaterThan(leftExpr, rightExpr);
            case "COMPARE_GREATERTHANOREQUALTO" -> new GreaterThanOrEqualTo(leftExpr, rightExpr);
            case "COMPARE_LESS_THAN" -> new LessThan(leftExpr, rightExpr);
            case "COMPARE_LESSTHANOREQUALTO"-> new CompareLessThanOrEqualTo(leftExpr, rightExpr);
            default -> throw new UnsupportedOperationException("Unsupported comparison type: " + comparisonType);
        };
    }

    private static Expression toConjunction(JsonNode conjunction) throws IOException {
        JsonNode children = conjunction.get("children");
        String conjunctionType = conjunction.get("type").asText();

        // Ensure `children` exists and has at least two elements
        if (children == null || children.size() < 2) {
            throw new IllegalArgumentException("Conjunction must have at least two children.");
        }

        if ("CONJUNCTION_AND".equals(conjunctionType)) {
            return buildLeftAssociativeAnd(children);
        } else if ("CONJUNCTION_OR".equals(conjunctionType)) {
            return buildLeftAssociativeOr(children);
        } else {
            throw new UnsupportedOperationException("Unsupported conjunction type: " + conjunctionType);
        }
    }

    /**
     * Builds a left-associative nested And expression.
     *
     * Example:
     * For children [A, B, C], it builds: And(And(A, B), C)
     *
     * @param children the JSON array of child expressions
     * @return a nested And expression
     * @throws IOException if an error occurs during parsing
     */
    private static Expression buildLeftAssociativeAnd(JsonNode children) throws IOException {
        Expression current = (Predicate) toDeltaPredicate(children.get(0)); // Start with the first child

        // Iterate through the rest of the children, building nested And expressions
        for (int i = 1; i < children.size(); i++) {
            current = new And((Predicate) current, (Predicate) toDeltaPredicate(children.get(i)));
        }

        return current;
    }

    /**
     * Builds a left-associative nested Or expression.
     *
     * Example:
     * For children [A, B, C], it builds: Or(Or(A, B), C)
     *
     * @param children the JSON array of child expressions
     * @return a nested Or expression
     * @throws IOException if an error occurs during parsing
     */
    private static Expression buildLeftAssociativeOr(JsonNode children) throws IOException {
        Expression current = (Predicate) toDeltaPredicate(children.get(0)); // Start with the first child

        // Iterate through the rest of the children, building nested Or expressions
        for (int i = 1; i < children.size(); i++) {
            current = new Or((Predicate) current, (Predicate) toDeltaPredicate(children.get(i)));
        }

        return current;
    }

    // TODO
    private static Expression toLiteral(JsonNode literal) throws IOException {
        String type = AstNodes.constantTypeId(literal);
        String value = AstNodes.constantText(literal);

        return switch (type) {
            case "INTEGER" -> Literal.ofInt(AstNodes.integerConstant(literal).intValue());
            case "VARCHAR" -> Literal.ofString(value);
            case "BOOLEAN" -> Literal.ofBoolean(Boolean.parseBoolean(value));
            case "DATE" -> Literal.ofDate(Integer.parseInt(value));
            case "TIMESTAMP" -> Literal.ofTimestamp(Long.parseLong(value));
            case "FLOAT" -> Literal.ofFloat(Float.parseFloat(value));
            case "DOUBLE" -> Literal.ofDouble(Double.parseDouble(value));
            case "BLOB" -> Literal.ofBinary(value.getBytes(StandardCharsets.UTF_8));
            default -> throw new UnsupportedOperationException("Unsupported literal type: " + type);
        };
    }

    private static Expression toReference(JsonNode reference) {
        String columnName = reference.get("column_names").get(0).asText();
        return new Column(columnName);
    }

    private static Expression toCast(JsonNode cast) throws IOException {
        JsonNode child = cast.get("child");
        Expression childExpr = toDeltaPredicate(child);
        String sourceType = String.valueOf(((Literal) childExpr).getDataType()).toUpperCase();;
        String castType = AstNodes.castType(cast).get("id").asText().toUpperCase();

        if (!canCast(sourceType, castType)) {
            throw new UnsupportedOperationException("Casting from " + sourceType + " to " + castType + " is not supported");
        }
        return castExpression(childExpr, sourceType, castType);
    }
}
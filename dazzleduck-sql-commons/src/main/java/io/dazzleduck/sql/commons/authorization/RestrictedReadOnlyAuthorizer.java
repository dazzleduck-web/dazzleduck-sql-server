package io.dazzleduck.sql.commons.authorization;

import com.fasterxml.jackson.databind.JsonNode;
import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.ExpressionConstants;
import io.dazzleduck.sql.commons.Transformations;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Authorizer for RESTRICT_READ_ONLY mode: SELECT-only access with a mandatory filter
 * injected into every base table via CTEs.
 *
 * <p>Two filter claim formats are supported in the JWT:
 * <ul>
 *   <li><b>{@code access}</b> — a JSON array of 3-element tuples
 *       {@code [[tableName, projection, filter], ...]}. All three elements are required.
 *       Use {@code "*"} for all columns and {@code "true"} for no row filter.
 *       Column projection other than {@code "*"} is reserved for future implementation.
 *       Takes precedence over {@code filter} when present.</li>
 *   <li><b>{@code filter}</b> — a single SQL expression applied to every table
 *       (e.g. {@code tenant_id = 'abc'}). All tables must expose the filter column.</li>
 * </ul>
 * At least one claim must be present; otherwise the query is rejected.
 */
public class RestrictedReadOnlyAuthorizer implements SqlAuthorizer {

    public static final SqlAuthorizer INSTANCE = new RestrictedReadOnlyAuthorizer();

    private RestrictedReadOnlyAuthorizer() {}

    @Override
    public JsonNode authorize(String user, String database, String schema, JsonNode query,
                              Map<String, String> verifiedClaims) throws UnauthorizedException {
        SelectOnlyAuthorizer.INSTANCE.authorize(user, database, schema, query, verifiedClaims);

        SqlAuthorizer.requireSingleStatement(query, "RESTRICT_READ_ONLY");

        // Block any TABLE_FUNCTION usage (read_parquet, read_json, duckdb_tables,
        // duckdb_secrets, etc.). The CTE-based filter injection only rewrites BASE_TABLE
        // references — TABLE_FUNCTION references would pass through unfiltered and could
        // read external data or leak catalog metadata. RESTRICT_READ_ONLY's claim shape
        // ("table" only) gives users no way to express function access, so reject outright.
        // Scans the raw AST, independently of collectAllTableReferences (which now reports every
        // table function too): the refusal does not depend on that walk being complete.
        String foundFunction = findTableFunctionName(query);
        if (foundFunction != null) {
            throw new UnauthorizedException(
                    "Table functions are not allowed in RESTRICT_READ_ONLY mode (found: "
                            + foundFunction + ")");
        }

        String tableAccessStr = verifiedClaims.get(Headers.HEADER_ACCESS);
        if (tableAccessStr != null && !tableAccessStr.isBlank()) {
            var entries = SqlAuthorizer.parseTableAccess(tableAccessStr);
            Map<String, JsonNode> tableFilters = new LinkedHashMap<>();
            for (var entry : entries) {
                if (!TableAccessEntry.TABLE.equals(entry.type())) {
                    throw new UnauthorizedException(
                            "RESTRICT_READ_ONLY mode only supports type 'table' in access; got: '" + entry.type() + "'");
                }
                // Accept fully-qualified ("catalog.schema.table"), schema-qualified
                // ("schema.table"), or bare ("table") names. Missing prefixes are filled
                // in from the connection's database/schema headers so a bare name is
                // implicitly scoped to the request's catalog/schema — preventing it
                // from matching same-named tables in other catalogs.
                tableFilters.put(SqlAuthorizer.qualifyTableName(entry.name(), database, schema), entry.filter());
            }
            return SqlAuthorizer.addFilterViaCtes(query, tableFilters, database, schema);
        }

        String filterStr = verifiedClaims.get(Headers.HEADER_FILTER);
        if (filterStr == null || filterStr.isBlank()) {
            throw new UnauthorizedException(
                    "RESTRICT_READ_ONLY mode requires a 'access' or 'filter' claim in the JWT");
        }
        String tableStr = verifiedClaims.get(Headers.HEADER_TABLE);
        if (tableStr == null || tableStr.isBlank()) {
            throw new UnauthorizedException(
                    "RESTRICT_READ_ONLY mode: 'filter' claim requires a 'table' claim; " +
                    "use 'access' for multi-table queries");
        }
        // filter + table is shorthand for a single-entry access: [[table, "*", filter]].
        // The key is qualified from the token's database/schema claims when it has them; anything
        // it leaves out injectFilterCtes fills from the connection's database/schema, which is
        // also what it resolves the query's table references against.
        String databaseStr = verifiedClaims.get(Headers.HEADER_DATABASE);
        String schemaStr = verifiedClaims.get(Headers.HEADER_SCHEMA);
        String qualifiedKey = (databaseStr != null && !databaseStr.isEmpty()
                && schemaStr != null && !schemaStr.isEmpty())
                ? databaseStr + "." + schemaStr + "." + tableStr
                : tableStr;
        Map<String, JsonNode> tableFilters = new LinkedHashMap<>();
        tableFilters.put(qualifiedKey, SqlAuthorizer.compileFilterString(filterStr));
        return SqlAuthorizer.addFilterViaCtes(query, tableFilters, database, schema);
    }

    @Override
    public boolean hasWriteAccess(String user, String ingestionQueue, Map<String, String> verifiedClaims) {
        return false;
    }

    /**
     * Recursively scans the AST for any node whose {@code type} field equals
     * {@code "TABLE_FUNCTION"}, returning the function name if found (or {@code null}).
     * Catches all TABLE_FUNCTION usages regardless of argument count or nesting depth
     * (subqueries, JOINs, CTE bodies, WHERE-clause subqueries, etc.).
     */
    private static String findTableFunctionName(JsonNode node) {
        if (node == null || node.isNull()) return null;
        if (node.isArray()) {
            for (JsonNode child : node) {
                String found = findTableFunctionName(child);
                if (found != null) return found;
            }
            return null;
        }
        if (!node.isObject()) return null;
        JsonNode typeNode = node.get(ExpressionConstants.FIELD_TYPE);
        if (typeNode != null && ExpressionConstants.NODE_TYPE_TABLE_FUNCTION.equals(typeNode.asText())) {
            JsonNode function = node.get(ExpressionConstants.FIELD_FUNCTION);
            if (function != null) {
                JsonNode name = function.get(ExpressionConstants.FIELD_FUNCTION_NAME);
                if (name != null) return name.asText();
            }
            return "<unknown>";
        }
        var fields = node.fields();
        while (fields.hasNext()) {
            String found = findTableFunctionName(fields.next().getValue());
            if (found != null) return found;
        }
        return null;
    }
}

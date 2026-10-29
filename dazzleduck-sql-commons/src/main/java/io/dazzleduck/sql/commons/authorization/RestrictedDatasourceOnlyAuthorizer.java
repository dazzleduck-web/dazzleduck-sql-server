package io.dazzleduck.sql.commons.authorization;

import com.fasterxml.jackson.databind.JsonNode;
import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.Transformations;

import java.util.Map;

import static io.dazzleduck.sql.common.Headers.HEADER_TOKEN_REDIRECT;

/**
 * JWT Claim-based Authorizer for SQL query and write access authorization.
 *
 * <h2>Supported JWT Claims for Authorization</h2>
 *
 * <h3>Read Access Claims (for SQL queries)</h3>
 * <ul>
 *   <li><b>database</b> - The database/catalog name for query execution context</li>
 *   <li><b>schema</b> - The schema name for query execution context</li>
 *   <li><b>table</b> - The table name the user is authorized to access (for BASE_TABLE queries)</li>
 *   <li><b>path</b> - The file path prefix the user is authorized to access (for TABLE_FUNCTION queries like read_parquet)</li>
 *   <li><b>function</b> - The table function name the user is authorized to use (e.g., 'read_parquet', 'read_delta')</li>
 *   <li><b>filter</b> - A SQL filter expression that will be automatically applied to all queries (row-level security).
 *       Example: "tenant_id = 'abc'" or "region IN ('us-east', 'us-west')"</li>
 * </ul>
 *
 * <h3>Write Access Claims (for data ingestion)</h3>
 * <ul>
 *   <li><b>access_type</b> - Must be "WRITE" to grant write access</li>
 *   <li><b>path</b> - The file path prefix the user is authorized to write to</li>
 * </ul>
 *
 * <h3>Authorization Logic</h3>
 * <p><b>For Read Access:</b></p>
 * <ul>
 *   <li>Either 'table' or 'path' claim must be present</li>
 *   <li>For BASE_TABLE queries: 'table' claim must match the queried table name</li>
 *   <li>For TABLE_FUNCTION queries: 'path' must be a prefix of the queried path, OR 'function' must match</li>
 *   <li>If 'filter' claim is present, it is applied to every read of the authorized table (each
 *       reference reads it through a filtered CTE), and for a path or function grant to the table
 *       function the filter can be placed on</li>
 * </ul>
 *
 * <p><b>For Write Access:</b></p>
 * <ul>
 *   <li>'access_type' claim must be "WRITE" (case-insensitive)</li>
 *   <li>'path' claim must be present and be a prefix of the target write path</li>
 * </ul>
 *
 * <h3>Path Matching</h3>
 * <p>Path authorization uses prefix matching. If authorized path is "data/tenant1",
 * the user can access "data/tenant1", "data/tenant1/subfolder", "data/tenant1/file.parquet", etc.</p>
 *
 * @see SqlAuthorizer
 * @see AccessType
 */
public class RestrictedDatasourceOnlyAuthorizer implements SqlAuthorizer {

    public static SqlAuthorizer INSTANCE = new RestrictedDatasourceOnlyAuthorizer();

    private RestrictedDatasourceOnlyAuthorizer() {

    }

    @Override
    public JsonNode authorize(String user, String database, String schema, JsonNode query, Map<String, String> verifiedClaims) throws UnauthorizedException {
        // First, so that it also covers the redirect dispatch below: that authorizer reads one
        // statement as well.
        SqlAuthorizer.requireSingleStatement(query, "RESTRICTED");

        // Dispatch to redirect authorizer when the token signals redirect mode
        String tokenType = verifiedClaims.get(Headers.HEADER_TOKEN_TYPE);
        if (HEADER_TOKEN_REDIRECT.equalsIgnoreCase(tokenType)) {
            return RedirectAuthorizer.INSTANCE.authorize(user, database, schema, query, verifiedClaims);
        }

        // --- Inline mode ---
        var catalogSchemaTables =
                Transformations.collectAllTableReferences(Transformations.getFirstStatementNode(query), database, schema);

        if (catalogSchemaTables.isEmpty()) {
            throw new UnauthorizedException("No table or path found in query");
        }

        var updatedQuery = SqlAuthorizer.withUpdatedDatabaseSchema(query, database, schema);

        // access takes precedence: [[type, name, projection, filter]] — exactly one entry
        String tableAccessStr = verifiedClaims.get(Headers.HEADER_ACCESS);
        if (tableAccessStr != null && !tableAccessStr.isBlank()) {
            var tableAccess = SqlAuthorizer.parseTableAccess(tableAccessStr);
            if (tableAccess.size() != 1) {
                throw new UnauthorizedException(
                        "RESTRICTED mode requires exactly one entry in 'access'; got " + tableAccess.size());
            }
            var entry = tableAccess.get(0);
            return switch (entry.type()) {
                case TableAccessEntry.TABLE -> {
                    var authorized = SqlAuthorizer.resolveAuthorizedTable(entry.name(), database, schema);
                    for (var cst : catalogSchemaTables) {
                        if (cst.type() == Transformations.TableType.TABLE_FUNCTION) {
                            throw new UnauthorizedException(
                                    "Access type 'table' does not allow TABLE_FUNCTION queries");
                        }
                        if (!SqlAuthorizer.hasAccessToTable(authorized, cst)) {
                            throw new UnauthorizedException("No access to %s".formatted(cst));
                        }
                    }
                    yield SqlAuthorizer.addFilterToEveryRead(updatedQuery,
                            SqlAuthorizer.qualifyTableName(entry.name(), database, schema), entry.filter(),
                            database, schema);
                }
                case TableAccessEntry.PATH -> {
                    for (var cst : catalogSchemaTables) {
                        if (cst.type() == Transformations.TableType.BASE_TABLE) {
                            throw new UnauthorizedException(
                                    "Access type 'path' does not allow BASE_TABLE queries");
                        }
                        if (!SqlAuthorizer.hasAccessToPath(entry.name(), cst.tableOrPath())) {
                            throw new UnauthorizedException("No access to %s".formatted(cst));
                        }
                    }
                    SqlAuthorizer.requireFilterablePlacement(query, entry.filter());
                    yield SqlAuthorizer.addFilterToTableFunction(updatedQuery, entry.filter());
                }
                case TableAccessEntry.FUNCTION -> {
                    for (var cst : catalogSchemaTables) {
                        if (cst.type() == Transformations.TableType.BASE_TABLE) {
                            throw new UnauthorizedException(
                                    "Access type 'function' does not allow BASE_TABLE queries");
                        }
                        if (!SqlAuthorizer.hasAccessToTableFunction(entry.name(), cst.functionName())) {
                            throw new UnauthorizedException("No access to %s".formatted(cst));
                        }
                    }
                    SqlAuthorizer.requireFilterablePlacement(query, entry.filter());
                    yield SqlAuthorizer.addFilterToTableFunction(updatedQuery, entry.filter());
                }
                default -> throw new UnauthorizedException("Unknown access type: " + entry.type());
            };
        }

        // Fallback: legacy table/path/filter claims
        var path = verifiedClaims.get(Headers.HEADER_PATH);
        var functionName = verifiedClaims.get(Headers.HEADER_FUNCTION);
        var table = verifiedClaims.get(Headers.HEADER_TABLE);
        if (path == null && table == null) {
            throw new UnauthorizedException("No access to %s".formatted(catalogSchemaTables));
        }

        var authorizedTable = table == null ? null
                : SqlAuthorizer.resolveAuthorizedTable(table, database, schema);
        for (var catalogSchemaTable : catalogSchemaTables) {
            if (catalogSchemaTable.type() == Transformations.TableType.TABLE_FUNCTION &&
                    (path == null || !SqlAuthorizer.hasAccessToPath(path, catalogSchemaTable.tableOrPath())) &&
                    !SqlAuthorizer.hasAccessToTableFunction(functionName, catalogSchemaTable.functionName())){
                throw new UnauthorizedException("No access to %s".formatted(catalogSchemaTable));
            }
            if (catalogSchemaTable.type() == Transformations.TableType.BASE_TABLE &&
                    (authorizedTable == null || !SqlAuthorizer.hasAccessToTable(authorizedTable, catalogSchemaTable))){
                throw new UnauthorizedException("No access to %s".formatted(catalogSchemaTable));
            }
        }

        var filter = verifiedClaims.get(Headers.HEADER_FILTER);
        if (filter == null) {
            return updatedQuery;
        }
        JsonNode compiledFilter = SqlAuthorizer.compileFilterString(filter);
        boolean readsTables = catalogSchemaTables.stream()
                .anyMatch(t -> t.type() == Transformations.TableType.BASE_TABLE);
        boolean readsFunctions = catalogSchemaTables.stream()
                .anyMatch(t -> t.type() == Transformations.TableType.TABLE_FUNCTION);
        if (readsTables && readsFunctions) {
            // One filter, one kind of datasource: it cannot be placed for both.
            throw new UnauthorizedException("A filtered grant cannot read both a table and a table function");
        }
        if (readsFunctions) {
            SqlAuthorizer.requireFilterablePlacement(query, compiledFilter);
            return SqlAuthorizer.addFilterToTableFunction(updatedQuery, compiledFilter);
        }
        return SqlAuthorizer.addFilterToEveryRead(updatedQuery,
                SqlAuthorizer.qualifyTableName(table, database, schema), compiledFilter, database, schema);
    }

    @Override
    public boolean hasWriteAccess(String user, String ingestionQueue, Map<String, String> verifiedClaims) {
        var accessType = verifiedClaims.get(Headers.HEADER_ACCESS_TYPE);
        if (!AccessType.WRITE.name().equalsIgnoreCase(accessType)) {
            return false;
        }
        var authorizedIngestionQueue = verifiedClaims.get(Headers.QUERY_PARAMETER_INGESTION_QUEUE);
        if (authorizedIngestionQueue == null) {
            return false;
        }
        return authorizedIngestionQueue.equals(ingestionQueue);
    }
}

package io.dazzleduck.sql.commons.authorization;

import com.fasterxml.jackson.databind.JsonNode;
import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.authorization.UnauthorizedException;
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
 *   <li>If 'filter' claim is present, it is automatically added to the query's WHERE clause</li>
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
public class JwtClaimBasedAuthorizer implements SqlAuthorizer {

    public static SqlAuthorizer INSTANCE = new JwtClaimBasedAuthorizer();

    private JwtClaimBasedAuthorizer() {

    }

    @Override
    public JsonNode authorize(String user, String database, String schema, JsonNode query, Map<String, String> verifiedClaims) throws UnauthorizedException {
        // Dispatch to redirect authorizer when the token signals redirect mode
        String tokenType = verifiedClaims.get(Headers.HEADER_TOKEN_TYPE);
        if (HEADER_TOKEN_REDIRECT.equalsIgnoreCase(tokenType)) {
            return RedirectAuthorizer.INSTANCE.authorize(user, database, schema, query, verifiedClaims);
        }

        // --- Inline mode (original logic) ---
        var catalogSchemaTables =
                Transformations.getAllTablesOrPathsFromSelect(Transformations.getFirstStatementNode(query), database, schema);

        if (catalogSchemaTables.isEmpty()) {
            throw new UnauthorizedException("No table or path found in query");
        }

        var updatedQuery = SqlAuthorizer.withUpdatedDatabaseSchema(query, database, schema);
        var path = verifiedClaims.get(Headers.HEADER_PATH);
        var functionName = verifiedClaims.get(Headers.HEADER_FUNCTION);
        var table = verifiedClaims.get(Headers.HEADER_TABLE);
        if (path == null && table == null) {
            throw new UnauthorizedException("No access to %s".formatted(catalogSchemaTables));
        }

        // Authorize all tables/paths (handles list_value with multiple paths)
        for (var catalogSchemaTable : catalogSchemaTables) {
            // Check function access
            if (catalogSchemaTable.type() == Transformations.TableType.TABLE_FUNCTION &&
                    (path == null || !SqlAuthorizer.hasAccessToPath(path, catalogSchemaTable.tableOrPath())) &&
                    !SqlAuthorizer.hasAccessToTableFunction(functionName, catalogSchemaTable.functionName())){
                throw new UnauthorizedException("No access to %s".formatted(catalogSchemaTable));
            }

            // Check table access
            if (catalogSchemaTable.type() == Transformations.TableType.BASE_TABLE &&
                    (table == null || !SqlAuthorizer.hasAccessToTable(database, schema, table, catalogSchemaTable))){
                throw new UnauthorizedException("No access to %s".formatted(catalogSchemaTable));
            }
        }

        // Use first entry for type determination (all entries should have same type for single table function)
        var catalogSchemaTable = catalogSchemaTables.get(0);
        var filter = verifiedClaims.get(Headers.HEADER_FILTER);
        if (filter == null) {
            return updatedQuery;
        }
        JsonNode compiledFilter = SqlAuthorizer.compileFilterString(filter);
        switch (catalogSchemaTable.type()) {
            case TABLE_FUNCTION -> {
                return SqlAuthorizer.addFilterToTableFunction(updatedQuery, compiledFilter);
            }
            case BASE_TABLE -> {
                return SqlAuthorizer.addFilterToBaseTable(updatedQuery, compiledFilter);
            }
            default -> {
                return null;
            }
        }
    }

    @Override
    public boolean hasWriteAccess(String user, String path, Map<String, String> verifiedClaims) {
        var accessType = verifiedClaims.get(Headers.HEADER_ACCESS_TYPE);
        if (!AccessType.WRITE.name().equalsIgnoreCase(accessType)) {
            return false;
        }
        var authorizedPath = verifiedClaims.get(Headers.HEADER_PATH);
        if (authorizedPath == null) {
            return false;
        }
        return SqlAuthorizer.hasAccessToPath(authorizedPath, path);
    }
}

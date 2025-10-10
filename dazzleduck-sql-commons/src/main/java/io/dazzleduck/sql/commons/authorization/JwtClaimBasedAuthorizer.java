package io.dazzleduck.sql.commons.authorization;

import com.fasterxml.jackson.databind.JsonNode;
import io.dazzleduck.sql.common.auth.UnauthorizedException;
import io.dazzleduck.sql.commons.Transformations;

import java.util.Map;

/**
 * Claim based Authorizer which respect all the data showing up in the jwt  claim
 * Following claims are required
 * filter = 'a > 10'
 * columns = [a, b, c]
 * Path based access
 *  path = '/a/b/c'
 *  function = 'parquet, json, csv'
 * Or Identifier based access
 *  identifier = catalog.schema.table
 */
public class JwtClaimBasedAuthorizer implements SqlAuthorizer {

    public static SqlAuthorizer INSTANCE = new JwtClaimBasedAuthorizer();

    private JwtClaimBasedAuthorizer() {

    }

    @Override
    public JsonNode authorize(String user, String database, String schema, JsonNode query, Map<String, String> verifiedClaims) throws UnauthorizedException {

        var catalogSchemaTables =
                Transformations.getAllTablesOrPathsFromSelect(Transformations.getFirstStatementNode(query), database, schema);

        if (catalogSchemaTables.size() != 1) {
            throw new UnauthorizedException("%s TableOrPath/Path found: Only one table or path is supported".formatted(catalogSchemaTables.size()));
        }
        var catalogSchemaTable = catalogSchemaTables.get(0);
        var path = verifiedClaims.get("path");
        var functionName = verifiedClaims.get("function");
        var table = verifiedClaims.get("table");
        if (path == null && table == null) {
            throw new UnauthorizedException("No access to %s".formatted(catalogSchemaTable));
        }

        // Check function access
        if (catalogSchemaTable.type() == Transformations.TableType.TABLE_FUNCTION &&
                (path == null || !SqlAuthorizer.hasAccessToPath(path, catalogSchemaTable.tableOrPath()))){
                throw new UnauthorizedException("No access to %s".formatted(catalogSchemaTable));
        }


        // Check table access
        if (catalogSchemaTable.type() == Transformations.TableType.BASE_TABLE &&
                (table == null || !SqlAuthorizer.hasAccessToTable(database, schema, table, catalogSchemaTable))){
            throw new UnauthorizedException("No access to %s".formatted(catalogSchemaTable));
        }
        var filter = verifiedClaims.get("filter");
        if (filter == null) {
            return query;
        }
        JsonNode compiledFilter = SqlAuthorizer.compileFilterString(filter);
        switch (catalogSchemaTable.type()) {
            case TABLE_FUNCTION -> {
                return SqlAuthorizer.addFilterToTableFunction(query, compiledFilter);
            }
            case BASE_TABLE -> {
                return SqlAuthorizer.addFilterToBaseTable(query, compiledFilter);
            }
            default -> {
                return null;
            }
        }
    }
}

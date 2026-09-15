package io.dazzleduck.sql.commons.authorization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.dazzleduck.sql.commons.authorization.UnauthorizedException;
import io.dazzleduck.sql.commons.ExpressionConstants;
import io.dazzleduck.sql.commons.ExpressionFactory;
import io.dazzleduck.sql.commons.Transformations;
import java.util.function.Function;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface SqlAuthorizer {

    ObjectMapper TABLE_ACCESS_MAPPER = new ObjectMapper();

    SqlAuthorizer NOOP_AUTHORIZER = NOOPAuthorizer.INSTANCE;

    SqlAuthorizer RESTRICTED_DATASOURCE_AUTHORIZER = RestrictedDatasourceOnlyAuthorizer.INSTANCE;

    SqlAuthorizer SELECT_ONLY_AUTHORIZER = SelectOnlyAuthorizer.INSTANCE;

    SqlAuthorizer RESTRICT_READ_ONLY_AUTHORIZER = RestrictedReadOnlyAuthorizer.INSTANCE;

    static JsonNode addFilterViaCtes(JsonNode query, JsonNode filter) {
        return Transformations.injectFilterCtes(query, filter);
    }

    static JsonNode addFilterViaCtes(JsonNode query, java.util.Map<String, JsonNode> tableFilters) {
        return Transformations.injectFilterCtes(query, tableFilters);
    }

    static JsonNode addFilterToTableFunction(JsonNode query, JsonNode toAdd) {
        var statement = Transformations.getFirstStatementNode(query);
        var selectOrig = Transformations.getSelectForTableFunction(statement);
        var qWhereClause = selectOrig.get(ExpressionConstants.FIELD_WHERE_CLAUSE);

        JsonNode allWhere ;
        if(qWhereClause == null || qWhereClause instanceof NullNode) {
            allWhere = toAdd;
        } else {
            allWhere = ExpressionFactory.andFilters(qWhereClause, toAdd);
        }
        var res = query.deepCopy();
        ObjectNode select = (ObjectNode)Transformations.getSelectForTableFunction(Transformations.getFirstStatementNode(res));
        if(select != null) {
            select.set(ExpressionConstants.FIELD_WHERE_CLAUSE, allWhere);
            return res;
        }
        return null;
    }

    static JsonNode addFilterToBaseTable(JsonNode query, JsonNode toAdd) {
        var statement = Transformations.getFirstStatementNode(query);
        var qWhereClause = Transformations.getWhereClauseForBaseTable(statement);
        JsonNode allWhere ;
        if(qWhereClause == null || qWhereClause instanceof NullNode ) {
            allWhere = toAdd;
        } else {
            allWhere = ExpressionFactory.andFilters(qWhereClause, toAdd);
        }
        var res = query.deepCopy();
        ObjectNode select = (ObjectNode)Transformations.getSelectForBaseTable(Transformations.getFirstStatementNode(res));
        if(select != null) {
            select.set(ExpressionConstants.FIELD_WHERE_CLAUSE, allWhere);
            return res;
        }
        return null;
    }

    Set<String> VALID_ACCESS_TYPES = Set.of(TableAccessEntry.TABLE, TableAccessEntry.PATH, TableAccessEntry.FUNCTION);

    /**
     * Parses the {@code access} claim: {@code [[type, name, projection, filter], ...]}.
     * All four elements are required per entry:
     * <ul>
     *   <li><b>type</b>       — {@code "table"}, {@code "path"}, or {@code "function"}</li>
     *   <li><b>name</b>       — table name, path prefix, or function name</li>
     *   <li><b>projection</b> — must be {@code "*"} (column restriction not yet implemented)</li>
     *   <li><b>filter</b>     — SQL WHERE expression; use {@code "true"} for no row restriction</li>
     * </ul>
     */
    static List<TableAccessEntry> parseTableAccess(String json) throws UnauthorizedException {
        try {
            JsonNode root = TABLE_ACCESS_MAPPER.readTree(json);
            if (!root.isArray()) {
                throw new UnauthorizedException("'access' must be a JSON array of [type, name, projection, filter] tuples");
            }
            List<TableAccessEntry> result = new ArrayList<>();
            for (int i = 0; i < root.size(); i++) {
                JsonNode entry = root.get(i);
                if (!entry.isArray() || entry.size() != 4) {
                    throw new UnauthorizedException(
                            "'access' entry " + i + " must have exactly 4 elements: [type, name, projection, filter]");
                }
                String type       = entry.get(0).asText();
                String name       = entry.get(1).asText();
                String projection = entry.get(2).asText();
                String filter     = entry.get(3).asText();
                if (!VALID_ACCESS_TYPES.contains(type)) {
                    throw new UnauthorizedException(
                            "'access' entry " + i + " has unknown type \"" + type +
                            "\"; must be one of: " + VALID_ACCESS_TYPES);
                }
                if (!"*".equals(projection)) {
                    throw new UnauthorizedException(
                            "Column projection is not yet supported; use \"*\" for entry " + i +
                            " (got: \"" + projection + "\")");
                }
                result.add(new TableAccessEntry(type, name, compileFilterString(filter)));
            }
            return result;
        } catch (UnauthorizedException e) {
            throw e;
        } catch (Exception e) {
            throw new UnauthorizedException("Failed to parse 'access' claim: " + e.getMessage());
        }
    }

    static JsonNode compileFilterString(String stringFilter) {
        var sql = "select * from t where " + stringFilter;
        JsonNode tree;
        try {
            tree = Transformations.parseToTree(sql);
        } catch (SQLException | JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        var statement = Transformations.getFirstStatementNode(tree);
        return Transformations.getWhereClauseForBaseTable(statement);
    }

    JsonNode authorize(String user, String database, String schema, JsonNode query,
                       Map<String, String> verifiedClaims
    ) throws UnauthorizedException;

    default JsonNode authorize(String user, String database, String schema, JsonNode query,
                       Map<String, String> verifiedClaims, long limit, long offset) throws UnauthorizedException {
        var authorized = authorize(user, database, schema, query, verifiedClaims);
        // Offset first: skipping rows of the query's own bounded result reduces what remains,
        // and the cap is then a ceiling on whatever is left.
        return Transformations.capLimit(Transformations.applyOffset(authorized, offset), limit);
    }

    boolean hasWriteAccess(String user, String ingestionQueue, Map<String, String> verifiedClaims);

    static boolean hasAccessToPath(String authorizedPath, String queriedPath) {
        var authorizedSplits = authorizedPath.split("/");
        var queriedSplits = queriedPath.split("/");
        if (queriedSplits.length < authorizedSplits.length) {
            return false;
        }
        for(int i=0 ; i < authorizedSplits.length; i ++){
            if(!authorizedSplits[i].equals(queriedSplits[i])) {
                return false;
            }
        }
        return true;
    }

    static boolean hasAccessToTableFunction(String authorizedFunction, String function) {
        return authorizedFunction!=null && authorizedFunction.equalsIgnoreCase(function);
    }

    /**
     * Resolves an authorized table {@code name} into a fully-qualified
     * {@link Transformations.CatalogSchemaTable}. Missing prefixes are filled from the
     * connection's {@code database}/{@code schema} headers:
     * <ul>
     *   <li>3-part {@code "catalog.schema.table"}: all parts come from the name</li>
     *   <li>2-part {@code "schema.table"}: catalog from {@code database}</li>
     *   <li>1-part {@code "table"}: catalog from {@code database}, schema from {@code schema}</li>
     * </ul>
     */
    static Transformations.CatalogSchemaTable resolveAuthorizedTable(String name, String database, String schema) {
        int firstDot = name.indexOf('.');
        int lastDot  = name.lastIndexOf('.');
        String c, s, t;
        if (firstDot == -1) {
            c = database; s = schema; t = name;
        } else if (firstDot == lastDot) {
            c = database; s = name.substring(0, firstDot); t = name.substring(firstDot + 1);
        } else {
            c = name.substring(0, firstDot); s = name.substring(firstDot + 1, lastDot); t = name.substring(lastDot + 1);
        }
        return new Transformations.CatalogSchemaTable(c, s, t, Transformations.TableType.BASE_TABLE);
    }

    /**
     * Convenience: returns the fully-qualified {@code "catalog.schema.table"} string form
     * of an authorized table name resolved against the connection's database/schema headers.
     * Used by callers (e.g. {@link RestrictedReadOnlyAuthorizer}) that need a string key.
     */
    static String qualifyTableName(String name, String database, String schema) {
        var t = resolveAuthorizedTable(name, database, schema);
        return t.catalog() + "." + t.schema() + "." + t.tableOrPath();
    }

    /**
     * Checks whether the {@code authorized} table grants access to the {@code queried}
     * BASE_TABLE. Both arguments are already-resolved triples — {@code queried} by
     * {@link Transformations#collectAllTableReferences}, and {@code authorized} typically
     * via {@link #resolveAuthorizedTable}.
     */
    static boolean hasAccessToTable(Transformations.CatalogSchemaTable authorized, Transformations.CatalogSchemaTable queried) {
        if (queried.type() != Transformations.TableType.BASE_TABLE) return false;
        return java.util.Objects.equals(authorized.tableOrPath(), queried.tableOrPath())
            && java.util.Objects.equals(authorized.schema(), queried.schema())
            && java.util.Objects.equals(authorized.catalog(), queried.catalog());
    }

    /**
     * Returns a copy of {@code tree} with the catalog and schema names set on the
     * FROM table, but only when the FROM clause is a BASE_TABLE. For TABLE_FUNCTION queries the
     * original tree is returned unchanged.
     */
    static JsonNode withUpdatedDatabaseSchema(JsonNode tree, String database, String schema) {
        var copy = tree.deepCopy();
        Function<JsonNode, JsonNode> identity = t -> t;
        var f = identity.andThen(Transformations::getFirstStatementNode).apply(copy);
        var fromTableNode = f.get(ExpressionConstants.FIELD_FROM_TABLE);
        if (fromTableNode == null || fromTableNode instanceof NullNode) {
            return tree;
        }
        var fromTable = (ObjectNode) fromTableNode;
        var typeNode = fromTable.get(ExpressionConstants.FIELD_TYPE);
        if (typeNode != null && typeNode.asText().equals(ExpressionConstants.BASE_TABLE_TYPE)) {
            var existingCatalog = fromTable.get(ExpressionConstants.FIELD_CATALOG_NAME);
            var existingSchema = fromTable.get(ExpressionConstants.FIELD_SCHEMA_NAME);
            if (existingCatalog == null || existingCatalog.asText().isEmpty()) {
                fromTable.put(ExpressionConstants.FIELD_CATALOG_NAME, database);
            }
            if (existingSchema == null || existingSchema.asText().isEmpty()) {
                fromTable.put(ExpressionConstants.FIELD_SCHEMA_NAME, schema);
            }
            return copy;
        }
        return tree;
    }

}

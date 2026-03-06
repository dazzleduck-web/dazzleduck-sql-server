package io.dazzleduck.sql.commons.authorization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.dazzleduck.sql.commons.authorization.UnauthorizedException;
import io.dazzleduck.sql.commons.ExpressionConstants;
import io.dazzleduck.sql.commons.ExpressionFactory;
import io.dazzleduck.sql.commons.Transformations;
import java.util.function.Function;

import java.sql.SQLException;
import java.util.Map;

public interface SqlAuthorizer {

    SqlAuthorizer NOOP_AUTHORIZER = NOOPAuthorizer.INSTANCE;

    SqlAuthorizer JWT_AUTHORIZER = JwtClaimBasedAuthorizer.INSTANCE;

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

    boolean hasWriteAccess(String user, String path, Map<String, String> verifiedClaims);

    static boolean hasAccessToPath(String authorizedPath, String queriedPath) {
        var authorizedSplits = authorizedPath.split("/");
        var queriedSplits = queriedPath.split("/");
        if (queriedSplits.length < authorizedSplits.length) {
            return false;
        }
        for(int i = 0 ; i < authorizedSplits.length; i ++){
            if(!authorizedSplits[i].equals(queriedSplits[i])) {
                return false;
            }
        }
        return true;
    }

    static boolean hasAccessToTableFunction(String authorizedFunction, String function) {
        return authorizedFunction!=null && authorizedFunction.equalsIgnoreCase(function);
    }

    static boolean hasAccessToTable(String database, String schema, String table, Transformations.CatalogSchemaTable queried) {
        return table.equals(queried.tableOrPath()) &&
                schema.equals(queried.schema()) &&
                database.equals(queried.catalog()) &&
                queried.type() == Transformations.TableType.BASE_TABLE;
    }

    /**
     * Returns a copy of {@code tree} with the catalog and schema names set on the FROM table,
     * but only when the FROM clause is a BASE_TABLE. For TABLE_FUNCTION queries the original
     * tree is returned unchanged.
     */
    static JsonNode withUpdatedDatabaseSchema(JsonNode tree, String database, String schema) {
        var copy = tree.deepCopy();
        Function<JsonNode, JsonNode> identity = t -> t;
        var f = identity.andThen(Transformations::getFirstStatementNode).apply(copy);
        var fromTable = (ObjectNode) f.get(ExpressionConstants.FIELD_FROM_TABLE);
        if (fromTable == null) {
            return tree;
        }
        var typeNode = fromTable.get(ExpressionConstants.FIELD_TYPE);
        if (typeNode != null && typeNode.asText().equals(ExpressionConstants.BASE_TABLE_TYPE)) {
            fromTable.put(ExpressionConstants.FIELD_CATALOG_NAME, database);
            fromTable.put(ExpressionConstants.FIELD_SCHEMA_NAME, schema);
            return copy;
        }
        return tree;
    }

}

package io.dazzleduck.sql.commons.authorization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.dazzleduck.sql.common.auth.UnauthorizedException;
import io.dazzleduck.sql.commons.ExpressionFactory;
import io.dazzleduck.sql.commons.Transformations;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

public interface SqlAuthorizer {

    public static SqlAuthorizer NOOP_AUTHORIZER = NOOPAuthorizer.INSTANCE;

    public static SqlAuthorizer JWT_AUTHORIZER = JwtClaimBasedAuthorizer.INSTANCE;

    static JsonNode addFilterToTableFunction(JsonNode query, JsonNode toAdd) {
        var statement = Transformations.getFirstStatementNode(query);
        var selectOrig = Transformations.getSelectForTableFunction(statement);
        var qWhereClause = selectOrig.get("where_clause");

        JsonNode allWhere ;
        if(qWhereClause == null || qWhereClause instanceof NullNode) {
            allWhere = toAdd;
        } else {
            allWhere = ExpressionFactory.andFilters(qWhereClause, toAdd);
        }
        var res = query.deepCopy();
        ObjectNode select = (ObjectNode)Transformations.getSelectForTableFunction(Transformations.getFirstStatementNode(res));
        if(select != null) {
            select.set("where_clause", allWhere);
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
            select.set("where_clause", allWhere);
            return res;
        }
        return null;
    }

    static void validateForAuthorization(JsonNode jsonNode) throws UnauthorizedException {
        var supportedFromType = Set.of("TABLE_FUNCTION", "BASE_TABLE", "SUBQUERY");
        var supportedTableFunction = Set.of("generate_series", "read_parquet", "read_delta");
        var statements = (ArrayNode) jsonNode.get("statements");
        if (statements.size() != 1) {
            throw new UnauthorizedException("too many statements");
        }
        var statement = statements.get(0);
        var statementNode = statement.get("node");
        //
        var statementNodeType = statementNode.get("type").asText();
        if (!statementNodeType.equals("SELECT_NODE")) {
            throw new UnauthorizedException("Not authorized. Incorrect Type :" + statementNodeType);
        }
        var where = statementNode.get("where_clause");
        var subQueries = Transformations.collectSubQueries(where);
        if (!subQueries.isEmpty()) {
            throw new UnauthorizedException("Sub queries are not supported");
        }
        var selectList = statementNode.get("select_list");
        var groupExpression = statementNode.get("group_expressions");
        var fromTable = statementNode.get("from_table");
        var fromTableType = fromTable.get("type").asText();
        if (!supportedFromType.contains(fromTableType)) {
            throw new UnauthorizedException("Type " + fromTableType + "Not supported");
        }

        var cteMap = statementNode.get("cte_map");
        if (!cteMap.isEmpty()) {
            var cteMapMap = cteMap.get("map");
            if (!cteMapMap.isEmpty()) {
                throw new UnauthorizedException("CTE expression is not supported");
            }
        }

        if (fromTable.get("type").asText().equals("BASE_TABLE")) {
            return;
        }
        var tableFunction = fromTable.get("function");
        var tableFunctionName = tableFunction.get("function_name").asText();
        if (!supportedTableFunction.contains(tableFunctionName.toLowerCase())) {
            throw new UnauthorizedException("Function " + fromTableType + "Not supported");
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

    static boolean hasAccessToPath(String authorizedPath, String queriedPath) {
        var authorizedSplits = authorizedPath.split("/");
        var queriedSplits = queriedPath.split("/");
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
}

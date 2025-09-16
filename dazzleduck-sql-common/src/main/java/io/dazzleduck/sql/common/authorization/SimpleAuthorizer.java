package io.dazzleduck.sql.common.authorization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import io.dazzleduck.sql.commons.ExpressionFactory;
import io.dazzleduck.sql.commons.Transformations;
import io.dazzleduck.sql.common.auth.UnauthorizedException;

import java.io.*;
import java.sql.SQLException;
import java.util.*;

public class SimpleAuthorizer implements SqlAuthorizer {
    private record AccessKey(String user, Transformations.CatalogSchemaTable catalogSchemaTable) { }
    private record AccessValue(JsonNode filter, List<String> columns) { }
    Map<AccessKey, AccessValue> accessMap = new HashMap<>();
    Set<String> superUsers = new HashSet<>();

    public SimpleAuthorizer(Map<String, List<String>> userGroupMapping,
                            List<AccessRow> accessRows) {
        var groupAccessRowMap = new HashMap<String, List<AccessRow>>();
        for (var row : accessRows) {
            groupAccessRowMap.compute(row.group(), (key, oldValue) -> {
                if (oldValue == null) {
                    var l = new ArrayList<AccessRow>();
                    l.add(row);
                    return l;
                } else {
                    oldValue.add(row);
                    return oldValue;
                }
            });
        }

        userGroupMapping.forEach((user, groups) -> {
            var map = new HashMap<Transformations.CatalogSchemaTable, AccessValue>();
            groups.forEach(group -> {
                var _accessRows = groupAccessRowMap.get(group);
                if(_accessRows != null) {
                    _accessRows.forEach(accessRow -> {
                        var key = new Transformations.CatalogSchemaTable(accessRow.database(), accessRow.schema(), accessRow.tableOrPath(), accessRow.tableType(), accessRow.functionName());
                        map.compute(key, (k, oldValue) -> {
                            if (oldValue == null) {
                                return collapse(accessRow);
                            } else {
                                return collapse(accessRow, oldValue);
                            }
                        });

                    });
                }
            });
            map.forEach((key, value) -> accessMap.put(new AccessKey(user, key), value));
        });
    }

    public static SqlAuthorizer load(Config config) throws IOException {
        var userGroupMapping = loadUsrGroupMapping(config);
        var accessRows = loadAccessRows(config);
        return new SimpleAuthorizer(userGroupMapping, accessRows);
    }

    public static List<AccessRow> loadAccessRows(Config config) throws IOException {
        var result = new ArrayList<AccessRow>();
        if (!config.hasPath("access-row-file")) {
            throw new IllegalArgumentException("Missing 'access-row-file' in config");
        }
        String path = config.getString("access-row-file");
        ObjectMapper mapper = new ObjectMapper();
        try (InputStream inputStream = new FileInputStream(path);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty()) {
                    AccessRow accessRow = mapper.readValue(line, AccessRow.class);
                    result.add(accessRow);
                }
            }
        }
        return result;
    }

    public static Map<String, List<String>> loadUsrGroupMapping(Config conf ) throws IOException {
        var res = new HashMap<String, List<String>>();
        var users = conf.getObjectList("users");
        for (var userConfigObject : users) {
            var userConfig = userConfigObject.toConfig();
            var user = userConfig.getString("username");
            var groups = userConfig.getStringList("groups");
            res.put(user, groups);
        }
        return res;
    }

    private AccessValue collapse(AccessRow r1, AccessValue accessValue) {
        return new AccessValue(collapseFilters(r1, accessValue.filter), collapseColumns(r1, accessValue.columns));
    }

    private AccessValue collapse(AccessRow r) {
        return new AccessValue(fromFilterString(r.filter()), r.columns());
    }

    private JsonNode fromFilterString(String stringFilter) {
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

    private List<String> collapseColumns(AccessRow r1, List<String> columns) {
        return List.of();
    }

    private JsonNode collapseFilters(AccessRow r1, JsonNode node) {
        var qnode = fromFilterString(r1.filter());
        return ExpressionFactory.orFilters(qnode, node);
    }

    @Override
    public JsonNode authorize(String user, String database, String schema, JsonNode query) throws UnauthorizedException {
        if (superUsers.contains(user)) {
            return query;
        }
        //validateForAuthorization(query);
        var catalogSchemaTables =
                Transformations.getAllTablesOrPathsFromSelect(Transformations.getFirstStatementNode(query), database, schema);

        if (catalogSchemaTables.size() != 1) {
            throw new UnauthorizedException("%s TableOrPath/Path found: Only one table or path is supported".formatted(catalogSchemaTables.size()));
        }
        var catalogSchemaTable = catalogSchemaTables.get(0);

        var key = new AccessKey(user, catalogSchemaTable);
        var a = accessMap.get(key);
        if (a == null) {
            throw new UnauthorizedException(database + "." + schema);
        }
        var columnAccess = hasAccessToColumns(query, a.columns());
        if (!columnAccess) {
            throw new UnauthorizedException("No access to columns specified columns");
        }

        switch (catalogSchemaTable.type()) {
            case TABLE_FUNCTION -> {
                return addFilterToTableFunction(query, a.filter);
            }
            case BASE_TABLE -> {
                return addFilterToBaseTable(query, a.filter);
            }
            default -> {
                return null;
            }
        }
    }

    public static JsonNode addFilterToTableFunction(JsonNode query, JsonNode toAdd) {
        var statement = Transformations.getFirstStatementNode(query);
        var selectOrig = Transformations.getSelectForTableFunction(statement);
        var qWhereClause = selectOrig.get("where_clause");

        JsonNode allWhere ;
        if(qWhereClause == null || qWhereClause instanceof NullNode ) {
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

    public static JsonNode addFilterToBaseTable(JsonNode query, JsonNode toAdd) {
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

    public static void validateForAuthorization(JsonNode jsonNode) throws UnauthorizedException {
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

    private boolean hasAccessToColumns(JsonNode query, List<String> accessColumn) {
        return true;
    }

    private static List<AccessRow> readAccessRows() throws IOException {
        String resourceName = "simple-access.json";
        ObjectMapper mapper = new ObjectMapper();
        List<AccessRow> result = new ArrayList<>();
        try (InputStream is = SimpleAuthorizer.class.getClassLoader().getResourceAsStream(resourceName)) {
            assert is != null;
            try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
                String line;
                while ((line = br.readLine()) != null) {
                    AccessRow myObject = mapper.readValue(line, AccessRow.class);
                    result.add(myObject);
                }
            }
        }
        return result;
    }
}

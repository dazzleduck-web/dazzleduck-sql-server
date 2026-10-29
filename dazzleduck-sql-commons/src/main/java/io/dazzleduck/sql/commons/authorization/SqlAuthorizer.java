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
import java.util.HashSet;
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

    /**
     * @param database the connection's catalog, and {@code schema} its schema: the defaults every
     *                 key and every table reference is resolved against
     *                 ({@link Transformations#injectFilterCtes(JsonNode, Map, String, String)})
     */
    static JsonNode addFilterViaCtes(JsonNode query, Map<String, JsonNode> tableFilters,
                                     String database, String schema) {
        return Transformations.injectFilterCtes(query, tableFilters, database, schema);
    }

    /**
     * Applies {@code filter} to every read of {@code qualifiedTable} in {@code query} — the FROM
     * clause, joins, subqueries in any clause and CTE bodies, at any depth — by reading the table
     * through a filtered CTE ({@link #addFilterViaCtes}). A filter added only to the outermost
     * SELECT's WHERE, as {@link #addFilterToBaseTable} does, leaves every other read of the same
     * table unfiltered. The caller must already have rejected references to any other table, since
     * a table with no entry in the map is given a {@code false} filter.
     *
     * <p>The filter is ALSO left on the outermost WHERE, where it is redundant for correctness (the
     * CTE has already applied it) but is what partition pruning reads: a split planner prunes from
     * the top-level predicates, so moving the filter into the CTE alone would stop it pruning on
     * the filter's columns and scan every partition. That copy is only added where it is valid —
     * see {@link #pruningHintFilter}.
     */
    static JsonNode addFilterToEveryRead(JsonNode query, String qualifiedTable, JsonNode filter,
                                         String database, String schema) {
        return addFilterToEveryRead(query, Map.of(qualifiedTable, filter), database, schema);
    }

    /**
     * Per-table variant of the above: every read of each table in {@code tableFilters} is rewritten
     * to go through that table's own filtered CTE. A table the query reads but the map does not
     * list is given a {@code false} filter, so the caller must have authorized — and listed — every
     * table the query touches. Keys and references are resolved against {@code database} /
     * {@code schema} before matching, so a bare name means one table and not every table that
     * shares it.
     */
    static JsonNode addFilterToEveryRead(JsonNode query, Map<String, JsonNode> tableFilters,
                                         String database, String schema) {
        JsonNode filtered = addFilterViaCtes(query, tableFilters, database, schema);
        JsonNode pruningHint = pruningHintFilter(query, tableFilters, database, schema);
        if (pruningHint == null) {
            return filtered;
        }
        JsonNode alsoAtTopLevel = addFilterToBaseTable(filtered, pruningHint);
        return alsoAtTopLevel == null ? filtered : alsoAtTopLevel;
    }

    /**
     * The filter to repeat on the outermost SELECT's WHERE so that split planning and partition
     * pruning can still see it, or {@code null} when there is none to place there.
     *
     * <p>The copy is only valid where that SELECT reads a granted table directly, because it then
     * reads that table's filter CTE — {@code SELECT * FROM table WHERE filter} — whose output has
     * every column the filter names. Reading anything else, the filter may not bind: in
     * {@code WITH x AS (SELECT amount FROM orders) SELECT sum(amount) FROM x} a copy of
     * {@code tenant_id = 'abc'} on the outer SELECT has no {@code tenant_id} to compare against and
     * the query fails to plan. The CTE has already made the result correct, so skipping the copy
     * costs pruning and nothing else.
     *
     * <p>Evaluated against the query as it was BEFORE CTE injection, which renames base tables but
     * leaves the shape — and so which SELECT {@link #addFilterToBaseTable} targets — unchanged.
     */
    private static JsonNode pruningHintFilter(JsonNode query, Map<String, JsonNode> tableFilters,
                                              String database, String schema) {
        JsonNode baseTable = baseTableOfOutermostRead(
                Transformations.getFirstStatementNode(query), Set.of());
        if (baseTable == null) {
            return null;
        }
        JsonNode filter = filterForQualifiedName(
                qualifiedNameOf(baseTable, database, schema), tableFilters, database, schema);
        // A table granted without a row restriction carries a `true` filter: nothing to prune on.
        if (filter == null || filter.equals(ExpressionFactory.trueExpression())) {
            return null;
        }
        return filter;
    }

    /**
     * The BASE_TABLE node read by the SELECT that {@link #addFilterToBaseTable} would add a filter
     * to, or {@code null} when that SELECT reads something else: a CTE (whose name is not a table),
     * a set operation, or a table function. Mirrors {@link Transformations#getSelectForBaseTable}'s
     * descent through derived tables, tracking the CTE names in scope on the way down.
     */
    private static JsonNode baseTableOfOutermostRead(JsonNode statement, Set<String> cteNamesInScope) {
        if (statement == null || statement.isNull()) {
            return null;
        }
        Set<String> inScope = new HashSet<>(cteNamesInScope);
        JsonNode cteMap = statement.get(ExpressionConstants.FIELD_CTE_MAP);
        JsonNode map = cteMap == null ? null : cteMap.get(ExpressionConstants.FIELD_MAP);
        if (map != null && map.isArray()) {
            for (JsonNode entry : map) {
                JsonNode key = entry.get(ExpressionConstants.FIELD_KEY);
                if (key != null && !key.isNull()) {
                    inScope.add(Transformations.foldIdentifier(key.asText()));
                }
            }
        }
        JsonNode from = statement.get(ExpressionConstants.FIELD_FROM_TABLE);
        if (from == null || from.isNull()) {
            return null;
        }
        JsonNode type = from.get(ExpressionConstants.FIELD_TYPE);
        if (type == null || type.isNull()) {
            return null;
        }
        switch (type.asText()) {
            case ExpressionConstants.NODE_TYPE_BASE_TABLE -> {
                JsonNode name = from.get(ExpressionConstants.FIELD_TABLE_NAME);
                if (name == null || name.isNull()) {
                    return null;
                }
                // Column aliases (orders AS o(a, b, c)) rename the columns the filter names, so a
                // copy on this SELECT would not bind; the filter CTE alone carries it.
                if (!from.path("column_name_alias").isEmpty()) {
                    return null;
                }
                // A qualified name is always the table, even under a same-named CTE.
                boolean qualified = !from.path(ExpressionConstants.FIELD_SCHEMA_NAME).asText("").isEmpty()
                        || !from.path(ExpressionConstants.FIELD_CATALOG_NAME).asText("").isEmpty();
                return !qualified && inScope.contains(Transformations.foldIdentifier(name.asText())) ? null : from;
            }
            case ExpressionConstants.NODE_TYPE_SUBQUERY -> {
                JsonNode subquery = from.get(ExpressionConstants.FIELD_SUBQUERY);
                return subquery == null ? null
                        : baseTableOfOutermostRead(subquery.get(ExpressionConstants.FIELD_NODE), inScope);
            }
            default -> {
                return null;
            }
        }
    }

    /**
     * {@code "catalog.schema.table"} of a BASE_TABLE node, taking each part the query omits from
     * the connection's {@code database} / {@code schema}, as the filter-CTE lookup does.
     */
    private static String qualifiedNameOf(JsonNode baseTable, String database, String schema) {
        String catalog = textOrEmpty(baseTable, ExpressionConstants.FIELD_CATALOG_NAME);
        String tableSchema = textOrEmpty(baseTable, ExpressionConstants.FIELD_SCHEMA_NAME);
        return (catalog.isEmpty() ? database : catalog) + "."
                + (tableSchema.isEmpty() ? schema : tableSchema) + "."
                + baseTable.get(ExpressionConstants.FIELD_TABLE_NAME).asText();
    }

    private static String textOrEmpty(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? "" : value.asText();
    }

    /**
     * The filter granted for {@code qualifiedName}, resolving each key the same way
     * {@link Transformations#injectFilterCtes(JsonNode, Map, String, String)} does so that this
     * lookup and the CTE it is a hint for cannot disagree about which table a name means.
     */
    private static JsonNode filterForQualifiedName(String qualifiedName, Map<String, JsonNode> tableFilters,
                                                   String database, String schema) {
        for (Map.Entry<String, JsonNode> grant : tableFilters.entrySet()) {
            if (Transformations.qualifiedTableName(grant.getKey(), database, schema).equals(qualifiedName)) {
                return grant.getValue();
            }
        }
        return null;
    }

    /**
     * Refuses a query that carries more than one statement, naming {@code mode} in the message.
     *
     * <p>Authorization reads one statement — {@link Transformations#getFirstStatementNode} — and the
     * filter injection rewrites only that one, but the whole tree is rendered back into the SQL that
     * runs, and DuckDB executes every statement in it and returns the last one's result. A trailing
     * statement would therefore run against tables this authorizer never checked and never filtered,
     * so a mode that restricts which tables may be read, or which rows of them, has to refuse the
     * shape outright.
     */
    static void requireSingleStatement(JsonNode query, String mode) throws UnauthorizedException {
        JsonNode statements = query.get(ExpressionConstants.FIELD_STATEMENTS);
        if (statements != null && statements.isArray() && statements.size() > 1) {
            throw new UnauthorizedException(
                    "Multi-statement queries are not allowed in " + mode + " mode.");
        }
    }

    /**
     * A filter for a path or function grant is applied by {@link #addFilterToTableFunction}, which
     * adds it to the SELECT that {@link Transformations#getSelectForTableFunction} identifies. When
     * there is no such SELECT the filter has nowhere to go, and the old code then returned
     * {@code null} — which the caller dereferenced into a NullPointerException rather than
     * refusing. The query is refused instead.
     *
     * <p>The filter must also bind to the table function's OWN columns, so two shapes the helper
     * accepts are refused here:
     * <ul>
     *   <li>a SELECT over a CTE (a BASE_TABLE reference -- a path or function grant reads no real
     *       table): the CTE defines its columns, so
     *       {@code WITH x AS (SELECT * REPLACE ('abc' AS tenant_id) FROM read_parquet(..))} makes a
     *       {@code tenant_id = 'abc'} filter pass every row;</li>
     *   <li>a positional UNION: its column names come from the user-written left arm, so the
     *       filter's column can name a different column of the file. Only UNION BY NAME -- the shape
     *       the clients and planner generate -- keeps the file's column names;</li>
     *   <li>a union arm that reads a table function but does not project its columns unchanged:
     *       {@code SELECT * REPLACE ('k2' AS key) FROM read_parquet(..)}, a column list, or column
     *       aliases on the function all let the filter's column be the user's;</li>
     *   <li>more than one table-function read: the filter lands on ONE SELECT, so a second read --
     *       in a scalar subquery, a WHERE, a join -- would run unfiltered.</li>
     * </ul>
     * A filter that is the constant {@code true} has nothing to bypass, so only the placement
     * itself is required then.
     */
    static void requireFilterablePlacement(JsonNode query, JsonNode filter) throws UnauthorizedException {
        JsonNode statement = Transformations.getFirstStatementNode(query);
        JsonNode select = Transformations.getSelectForTableFunction(statement);
        if (select == null) {
            throw new UnauthorizedException(
                    "This query shape cannot have a path or function grant's filter applied to it");
        }
        if (isTrueConstant(filter)) {
            return;
        }
        if (countTableFunctions(statement) != 1) {
            throw new UnauthorizedException(
                    "A path or function grant's filter applies to one table-function read; this query has several");
        }
        JsonNode from = select.get(ExpressionConstants.FIELD_FROM_TABLE);
        String fromType = from == null ? null : from.path(ExpressionConstants.FIELD_TYPE).asText(null);
        if (ExpressionConstants.NODE_TYPE_BASE_TABLE.equals(fromType)) {
            throw new UnauthorizedException(
                    "A path or function grant's filter cannot be applied over a CTE, which defines its own columns");
        }
        if (ExpressionConstants.NODE_TYPE_SUBQUERY.equals(fromType)) {
            JsonNode inner = from.path(ExpressionConstants.FIELD_SUBQUERY).path(ExpressionConstants.FIELD_NODE);
            if (ExpressionConstants.NODE_TYPE_SET_OPERATION_NODE.equals(inner.path(ExpressionConstants.FIELD_TYPE).asText(null))) {
                if (!"UNION_BY_NAME".equals(inner.path("setop_type").asText(null))) {
                    throw new UnauthorizedException(
                            "A path or function grant's filter needs UNION BY NAME, so it binds to the file's own columns");
                }
                requireArmsProjectTheFunctionUnchanged(inner);
            }
        }
    }

    /**
     * Every arm of the union that reads the table function must be {@code SELECT * FROM fn(..)}:
     * a bare star (no REPLACE / EXCLUDE / RENAME / COLUMNS) over the function itself, with no column
     * aliases on it. Arms that read no table function (the typed empty VALUES arm) add only rows
     * the user wrote, which the filter still sees.
     */
    private static void requireArmsProjectTheFunctionUnchanged(JsonNode node) throws UnauthorizedException {
        String type = node.path(ExpressionConstants.FIELD_TYPE).asText("");
        if (ExpressionConstants.NODE_TYPE_SET_OPERATION_NODE.equals(type)) {
            requireArmsProjectTheFunctionUnchanged(node.path(ExpressionConstants.FIELD_LEFT));
            requireArmsProjectTheFunctionUnchanged(node.path(ExpressionConstants.FIELD_RIGHT));
            return;
        }
        if (countTableFunctions(node) == 0) {
            return;
        }
        JsonNode from = node.path(ExpressionConstants.FIELD_FROM_TABLE);
        JsonNode selectList = node.path("select_list");
        boolean overTheFunction = ExpressionConstants.NODE_TYPE_SELECT_NODE.equals(type)
                && ExpressionConstants.NODE_TYPE_TABLE_FUNCTION.equals(from.path(ExpressionConstants.FIELD_TYPE).asText(""))
                && from.path("column_name_alias").isEmpty();
        boolean bareStar = selectList.isArray() && selectList.size() == 1 && isBareStar(selectList.get(0));
        if (!overTheFunction || !bareStar) {
            throw new UnauthorizedException("A path or function grant's filter needs the table function's"
                    + " columns unchanged: its union arm must be SELECT * FROM the function, with no"
                    + " REPLACE, EXCLUDE, column list or column aliases");
        }
    }

    private static boolean isBareStar(JsonNode expr) {
        return "STAR".equals(expr.path(ExpressionConstants.FIELD_CLASS).asText(""))
                && expr.path("exclude_list").isEmpty()
                && expr.path("replace_list").isEmpty()
                && expr.path("rename_list").isEmpty()
                && expr.path("qualified_exclude_list").isEmpty()
                && !expr.path("columns").asBoolean(false)
                && (expr.path("expr").isMissingNode() || expr.path("expr").isNull());
    }

    /** TABLE_FUNCTION FROM nodes anywhere under {@code node}. */
    private static int countTableFunctions(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) return 0;
        int n = node.isObject() && !node.has(ExpressionConstants.FIELD_CLASS)
                && ExpressionConstants.NODE_TYPE_TABLE_FUNCTION.equals(node.path(ExpressionConstants.FIELD_TYPE).asText(""))
                ? 1 : 0;
        for (JsonNode child : node) n += countTableFunctions(child);
        return n;
    }

    /**
     * Whether {@code filter} is the constant TRUE, i.e. a grant with no row filter. DuckDB parses
     * the literal {@code true} as {@code CAST('t' AS BOOLEAN)}, so that form counts too.
     */
    private static boolean isTrueConstant(JsonNode filter) {
        if (filter == null) return false;
        JsonNode node = filter;
        if ("CAST".equals(node.path(ExpressionConstants.FIELD_CLASS).asText(""))
                && "BOOLEAN".equals(node.path("cast_type").path("id").asText(""))) {
            node = node.path(ExpressionConstants.FIELD_CHILD);
        }
        if (!"CONSTANT".equals(node.path(ExpressionConstants.FIELD_CLASS).asText(""))) return false;
        JsonNode value = node.path("value").path("value");
        return value.isBoolean() ? value.asBoolean()
                : value.isTextual() && (value.asText().equalsIgnoreCase("t") || value.asText().equalsIgnoreCase("true"));
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
        if (queriedPath == null || Transformations.NON_CONSTANT_PATH.equals(queriedPath)) {
            // Not a path at all (a computed argument): no grant covers it, a grant of "/" included.
            return false;
        }
        for (String segment : queriedPath.split("/")) {
            // The segments are compared as written, so a "." or ".." would let
            // "/allowed/../secret/x.parquet" match the "/allowed" prefix and read outside it.
            if (segment.equals("..") || segment.equals(".")) return false;
        }
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
        return Transformations.qualifiedTableName(name, database, schema);
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

    /** Whether {@code fromTable} (a BASE_TABLE node) names a CTE declared by {@code statement}. */
    private static boolean namesATopLevelCte(JsonNode statement, ObjectNode fromTable) {
        JsonNode name = fromTable.get(ExpressionConstants.FIELD_TABLE_NAME);
        if (name == null || name.isNull()) return false;
        // A schema- or catalog-qualified name is never a CTE reference (as isCteReference): leaving
        // `main.orders` unqualified under a CTE named orders would let a redirect target resolve it
        // against its own default catalog rather than the one authorized.
        for (String part : new String[]{ExpressionConstants.FIELD_SCHEMA_NAME, ExpressionConstants.FIELD_CATALOG_NAME}) {
            JsonNode qualifier = fromTable.get(part);
            if (qualifier != null && !qualifier.isNull() && !qualifier.asText().isEmpty()) return false;
        }
        JsonNode cteMap = statement.get(ExpressionConstants.FIELD_CTE_MAP);
        JsonNode map = cteMap == null ? null : cteMap.get(ExpressionConstants.FIELD_MAP);
        if (map == null || !map.isArray()) return false;
        for (JsonNode entry : map) {
            JsonNode key = entry.get(ExpressionConstants.FIELD_KEY);
            if (key != null && !key.isNull() && Transformations.sameIdentifier(key.asText(), name.asText())) {
                return true;
            }
        }
        return false;
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
            // A FROM that names one of the query's own CTEs is not a table: qualifying it with the
            // connection's catalog/schema would turn `FROM x` into `FROM db.schema.x`, which no
            // longer resolves to the CTE.
            if (namesATopLevelCte(f, fromTable)) {
                return tree;
            }
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

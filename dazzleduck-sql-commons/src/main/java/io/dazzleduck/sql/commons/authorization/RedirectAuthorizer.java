package io.dazzleduck.sql.commons.authorization;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.Transformations;
import io.dazzleduck.sql.commons.ExpressionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.dazzleduck.sql.common.SslUtils;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Handles authorization for tokens with {@code token_type=redirect}.
 *
 * <p>Instead of reading access rules from JWT claims directly, this authorizer calls a
 * remote resolve endpoint that returns the full list of tables and functions the user
 * is allowed to access. The original bearer token is forwarded as the
 * {@code Authorization: Bearer} header so the resolve server can identify the caller.
 *
 * <p>The resolve URL is derived from the server's configured {@code login_url} by
 * replacing the trailing {@code /login} segment with {@code /resolve}.
 */
public class RedirectAuthorizer {

    private static final Logger logger = LoggerFactory.getLogger(RedirectAuthorizer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP_CLIENT = SslUtils.httpClient();
    private static final Duration RESOLVE_RESPONSE_TTL = Duration.ofMinutes(5);

    public static final RedirectAuthorizer INSTANCE = new RedirectAuthorizer();

    private record ResolveResponseCacheEntry(ResolveResponse resolveResponse, Instant resolvedAt) {
        boolean isExpired() {
            return Instant.now().isAfter(resolvedAt.plus(RESOLVE_RESPONSE_TTL));
        }
    }

    // Keyed on user + redirectUrl + bearerToken so that revoking a token or switching
    // to a different redirect URL always bypasses the cache and triggers a fresh resolve call.
    private final Map<String, ResolveResponseCacheEntry> resolveResponseCache = new ConcurrentHashMap<>();

    private static String cacheKey(String user, String redirectUrl, String bearerToken) {
        return user + ":" + redirectUrl + ":" + bearerToken.hashCode();
    }

    /**
     * Authorizes a query by calling the remote resolve endpoint and matching each
     * table or function referenced in the query against the returned access rows.
     * If a matching row has a {@code filter}, it is injected into the query's WHERE clause.
     */
    public JsonNode authorize(String user, String database, String schema,
                              JsonNode query, Map<String, String> verifiedClaims)
            throws UnauthorizedException {

        String bearerToken = verifiedClaims.get(Headers.HEADER_BEARER_TOKEN);
        if (bearerToken == null) {
            throw new UnauthorizedException("No bearer token available for redirect authorization");
        }

        String redirectUrl = verifiedClaims.get(Headers.HEADER_REDIRECT_URL);
        if (redirectUrl == null) {
            throw new UnauthorizedException("No redirect url available for redirect authorization");
        }

        ResolveResponse response = callResolveEndpoint(user, bearerToken, redirectUrl);

        var firstStatement = Transformations.getFirstStatementNode(query);
        // Use comprehensive collector so subquery tables are also access-controlled.
        var catalogSchemaTables = Transformations.collectAllTableReferences(firstStatement, database, schema);

        if (catalogSchemaTables.isEmpty()) {
            throw new UnauthorizedException("No table or path found in query");
        }

        // Authorize each table/function. Each table gets its own resolved filter, applied to every
        // read of it; table functions share one, which can only be placed in one SELECT.
        Map<String, JsonNode> tableFilters = new LinkedHashMap<>();
        boolean anyTableFilter = false;
        String functionFilter = null;
        boolean readsTables = false;
        for (var cst : catalogSchemaTables) {
            ResolveAccessRow matched = findMatchingRow(response, cst);
            if (matched == null) {
                throw new UnauthorizedException("No access to %s".formatted(cst));
            }
            String filter = matched.filter() == null || matched.filter().isEmpty() ? null : matched.filter();
            if (cst.type() == Transformations.TableType.BASE_TABLE) {
                readsTables = true;
                String name = java.util.stream.Stream.of(cst.catalog(), cst.schema(), cst.tableOrPath())
                        .filter(part -> part != null && !part.isEmpty())
                        .collect(java.util.stream.Collectors.joining("."));
                tableFilters.putIfAbsent(SqlAuthorizer.qualifyTableName(name, database, schema),
                        filter == null ? ExpressionFactory.trueExpression() : SqlAuthorizer.compileFilterString(filter));
                anyTableFilter |= filter != null;
            } else if (filter != null) {
                if (functionFilter != null && !functionFilter.equals(filter)) {
                    throw new UnauthorizedException("Table functions with different filters cannot be combined");
                }
                functionFilter = filter;
            }
        }

        var updatedQuery = SqlAuthorizer.withUpdatedDatabaseSchema(query, database, schema);

        if (functionFilter != null) {
            if (readsTables) {
                throw new UnauthorizedException("A filtered table function cannot be combined with a table");
            }
            JsonNode compiledFunctionFilter = SqlAuthorizer.compileFilterString(functionFilter);
            SqlAuthorizer.requireFilterablePlacement(query, compiledFunctionFilter);
            return SqlAuthorizer.addFilterToTableFunction(updatedQuery, compiledFunctionFilter);
        }
        // addFilterToEveryRead, not addFilterViaCtes: a redirect-mode query is split-planned like
        // any other, and the CTEs alone leave the planner no top-level predicate to prune on.
        return anyTableFilter
                ? SqlAuthorizer.addFilterToEveryRead(updatedQuery, tableFilters, database, schema)
                : updatedQuery;
    }

    private ResolveAccessRow findMatchingRow(ResolveResponse response,
                                             Transformations.CatalogSchemaTable cst) {
        List<ResolveAccessRow> candidates = switch (cst.type()) {
            case TABLE_FUNCTION -> response.functions() != null ? response.functions() : List.of();
            case BASE_TABLE -> response.tables() != null ? response.tables() : List.of();
        };

        for (ResolveAccessRow row : candidates) {
            if (isExpired(row)) {
                continue;
            }
            if (cst.type() == Transformations.TableType.BASE_TABLE) {
                logger.debug("RedirectAuthorizer.findMatchingRow: checking cst={} against row=catalog:{} schema:{} table:{}", cst, row.catalog(), row.schema(), row.tableOrPath());
                var authorized = new Transformations.CatalogSchemaTable(
                        row.catalog(), row.schema(), row.tableOrPath(), Transformations.TableType.BASE_TABLE);
                if (SqlAuthorizer.hasAccessToTable(authorized, cst)) {
                    return row;
                }
            } else {
                boolean pathMatch = row.tableOrPath() != null
                        && SqlAuthorizer.hasAccessToPath(row.tableOrPath(), cst.tableOrPath());
                boolean functionMatch = SqlAuthorizer.hasAccessToTableFunction(
                        row.functionName(), cst.functionName());
                if (pathMatch || functionMatch) {
                    return row;
                }
            }
        }
        return null;
    }

    private boolean isExpired(ResolveAccessRow row) {
        if (row.expiration() == null) {
            return false;
        }
        try {
            String datePart = row.expiration().length() >= 10
                    ? row.expiration().substring(0, 10)
                    : row.expiration();
            return LocalDate.now().isAfter(LocalDate.parse(datePart));
        } catch (Exception e) {
            logger.warn("Could not parse expiration '{}', treating as not expired", row.expiration());
            return false;
        }
    }

    private ResolveResponse callResolveEndpoint(String user, String bearerToken, String resolveUrl) throws UnauthorizedException {
        String key = cacheKey(user, resolveUrl, bearerToken);
        ResolveResponseCacheEntry entry = resolveResponseCache.get(key);
        if (entry != null && !entry.isExpired()) {
            logger.debug("RedirectAuthorizer: using cached resolve response for user={}, tables={}", user,
                    entry.resolveResponse().tables() != null ? entry.resolveResponse().tables().stream().map(r -> r.catalog() + "." + r.schema() + "." + r.tableOrPath()).toList() : "null");
            return entry.resolveResponse();
        }
        try {
            logger.debug("RedirectAuthorizer: calling resolve endpoint url={} user={}", resolveUrl, user);
            var request = HttpRequest.newBuilder()
                    .uri(URI.create(resolveUrl))
                    .header("Authorization", "Bearer " + bearerToken)
                    .GET()
                    .build();
            var response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
            logger.debug("RedirectAuthorizer: resolve response status={} body={}", response.statusCode(), response.body());
            if (response.statusCode() != 200) {
                throw new UnauthorizedException(
                        "Resolve endpoint returned status " + response.statusCode());
            }
            ResolveResponse resolved = MAPPER.readValue(response.body(), ResolveResponse.class);
            resolveResponseCache.put(key, new ResolveResponseCacheEntry(resolved, Instant.now()));
            return resolved;
        } catch (UnauthorizedException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Failed to call resolve endpoint at {}: {}", resolveUrl, e.getMessage(), e);
            throw new UnauthorizedException("Failed to resolve access: " + e.getMessage());
        }
    }
}

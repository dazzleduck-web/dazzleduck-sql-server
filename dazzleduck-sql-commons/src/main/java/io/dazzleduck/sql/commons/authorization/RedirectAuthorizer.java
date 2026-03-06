package io.dazzleduck.sql.commons.authorization;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.Transformations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
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
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
    private static final Duration RESOLVE_RESPONSE_TTL = Duration.ofMinutes(5);

    public static final RedirectAuthorizer INSTANCE = new RedirectAuthorizer();

    private record ResolveResponseCacheEntry(ResolveResponse resolveResponse, Instant resolvedAt) {
        boolean isExpired() {
            return Instant.now().isAfter(resolvedAt.plus(RESOLVE_RESPONSE_TTL));
        }
    }

    private final Map<String, ResolveResponseCacheEntry> resolveResponseByUser = new ConcurrentHashMap<>();

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
        var catalogSchemaTables = Transformations.getAllTablesOrPathsFromSelect(firstStatement, database, schema);

        if (catalogSchemaTables.isEmpty()) {
            throw new UnauthorizedException("No table or path found in query");
        }

        // Authorize each table/function and track the first filter to apply
        String firstFilter = null;
        Transformations.TableType firstType = null;

        for (var cst : catalogSchemaTables) {
            ResolveAccessRow matched = findMatchingRow(response, cst, database, schema);
            if (matched == null) {
                throw new UnauthorizedException("No access to %s".formatted(cst));
            }
            if (firstFilter == null && matched.filter() != null && !matched.filter().isEmpty()) {
                firstFilter = matched.filter();
                firstType = cst.type();
            }
        }

        var updatedQuery = SqlAuthorizer.withUpdatedDatabaseSchema(query, database, schema);

        if (firstFilter == null) {
            return updatedQuery;
        }

        JsonNode compiledFilter = SqlAuthorizer.compileFilterString(firstFilter);
        return switch (firstType) {
            case TABLE_FUNCTION -> SqlAuthorizer.addFilterToTableFunction(updatedQuery, compiledFilter);
            case BASE_TABLE -> SqlAuthorizer.addFilterToBaseTable(updatedQuery, compiledFilter);
        };
    }

    private ResolveAccessRow findMatchingRow(ResolveResponse response,
                                             Transformations.CatalogSchemaTable cst,
                                             String database, String schema) {
        List<ResolveAccessRow> candidates = switch (cst.type()) {
            case TABLE_FUNCTION -> response.functions() != null ? response.functions() : List.of();
            case BASE_TABLE -> response.tables() != null ? response.tables() : List.of();
        };

        for (ResolveAccessRow row : candidates) {
            if (isExpired(row)) {
                continue;
            }
            if (cst.type() == Transformations.TableType.BASE_TABLE) {
                if (database.equals(row.catalog())
                        && schema.equals(row.schema())
                        && SqlAuthorizer.hasAccessToTable(database, schema, row.tableOrPath(), cst)) {
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
        ResolveResponseCacheEntry entry = resolveResponseByUser.get(user);
        if (entry != null && !entry.isExpired()) {
            return entry.resolveResponse();
        }
        try {
            var request = HttpRequest.newBuilder()
                    .uri(URI.create(resolveUrl))
                    .header("Authorization", "Bearer " + bearerToken)
                    .GET()
                    .build();
            var response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new UnauthorizedException(
                        "Resolve endpoint returned status " + response.statusCode());
            }
            ResolveResponse resolved = MAPPER.readValue(response.body(), ResolveResponse.class);
            resolveResponseByUser.put(user, new ResolveResponseCacheEntry(resolved, Instant.now()));
            return resolved;
        } catch (UnauthorizedException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Failed to call resolve endpoint at {}: {}", resolveUrl, e.getMessage(), e);
            throw new UnauthorizedException("Failed to resolve access: " + e.getMessage());
        }
    }
}

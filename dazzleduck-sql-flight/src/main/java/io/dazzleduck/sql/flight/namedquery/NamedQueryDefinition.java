package io.dazzleduck.sql.flight.namedquery;

import java.util.Map;

/**
 * DB-facing representation of a named query.
 *
 * <p>Maps directly to a row in the named-query table:
 * <pre>{@code
 *   CREATE TABLE named_queries (
 *       id                     BIGINT PRIMARY KEY,
 *       name                   VARCHAR UNIQUE,
 *       template               VARCHAR,
 *       validators             VARCHAR[],
 *       description            VARCHAR,
 *       parameter_descriptions MAP(VARCHAR, VARCHAR),
 *       preferred_display      VARCHAR,
 *       query_group             VARCHAR DEFAULT 'general'
 *   );
 * }</pre>
 *
 * <p>Column order must match the record component order because
 * {@code ConnectionPool.collectAll} maps by position.
 */
public record NamedQueryDefinition(
        long id,
        String name,
        String template,
        String[] validators,
        String description,
        Map<String, String> parameterDescriptions,
        String preferredDisplay,
        String queryGroup) {
}

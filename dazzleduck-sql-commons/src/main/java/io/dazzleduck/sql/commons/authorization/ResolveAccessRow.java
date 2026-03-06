package io.dazzleduck.sql.commons.authorization;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Represents a single access row returned by the redirect resolve endpoint.
 * Maps to entries in the "tables" or "functions" arrays of the resolve response.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ResolveAccessRow(
        @JsonProperty("database") String catalog,
        String schema,
        String tableOrPath,
        String tableType,
        List<String> columns,
        String filter,
        String functionName,
        String expiration) { }

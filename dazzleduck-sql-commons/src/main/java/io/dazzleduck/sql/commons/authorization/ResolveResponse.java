package io.dazzleduck.sql.commons.authorization;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/**
 * Response from the redirect resolve endpoint ({@code /resolve}).
 * Contains the list of tables and functions the authenticated user is authorized to access.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ResolveResponse(
        List<ResolveAccessRow> tables,
        List<ResolveAccessRow> functions,
        String version) {
}

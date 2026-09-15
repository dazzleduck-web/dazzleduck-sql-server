package io.dazzleduck.sql.commons.authorization;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.sql.common.Headers;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Turns the {@link Headers#CLAIM_SESSION_VARIABLES} JWT claim — a JSON object of string
 * key/values, e.g. {@code {"tenant_id":"acme"}} — into DuckDB {@code SET VARIABLE} statements
 * applied to the per-request connection, so queries and injected row-level-security filters can
 * read them with {@code getvariable('name')}.
 *
 * <p>The claim is trusted only because it arrives inside the server-signed token (the caller must
 * read it from the verified claims, never from a client request header). Even so, names are
 * validated against a conservative identifier and values are rendered as escaped SQL string
 * literals, so a crafted claim cannot inject SQL.
 *
 * <p>All values are applied as VARCHAR literals, so every value must be a JSON string — a bare
 * number or boolean is rejected with a hint to quote it. A filter needing a numeric/temporal
 * comparison casts, e.g. {@code getvariable('n')::INT}.
 */
public final class SessionVariables {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Variable name we accept. Deliberately excludes the double quote, so the name can be rendered
     * as a quoted identifier without any escaping of its own.
     */
    private static final Pattern VALID_NAME = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");

    /** Upper bounds so an oversized claim cannot turn one request into thousands of statements. */
    private static final int MAX_VARIABLES = 64;
    private static final int MAX_VALUE_LENGTH = 4096;

    private SessionVariables() {
    }

    /**
     * Renders {@code SET VARIABLE <name> = '<value>'} for each entry of the JSON object in
     * {@code claimJson}. Returns an empty list when the claim is null, blank, or an empty object.
     * Null-valued entries are skipped ({@code getvariable} of an unset name already returns NULL).
     *
     * <p>The claim value is the JSON <em>text</em>, i.e. a JWT claim whose value is a string such as
     * {@code "{\"tenant_id\":\"acme\"}"} — not a nested JSON object, which
     * {@code JwtClaimsExtractor} cannot read as a String.
     *
     * @throws IllegalArgumentException if the claim is not a JSON object of scalar values, a key is
     *                                  not a valid identifier, a value holds a control character,
     *                                  or the claim exceeds {@link #MAX_VARIABLES} entries or
     *                                  {@link #MAX_VALUE_LENGTH} characters in a value
     */
    public static List<String> toSetStatements(String claimJson) {
        if (claimJson == null || claimJson.isBlank()) {
            return List.of();
        }
        Map<String, Object> vars;
        try {
            vars = MAPPER.readValue(claimJson, new TypeReference<LinkedHashMap<String, Object>>() {
            });
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Invalid " + Headers.CLAIM_SESSION_VARIABLES + " claim: expected a JSON object", e);
        }
        // Parse and enforce the structural rules needed for safe SQL rendering, collecting a
        // name→value map, then hand it to the policy hook before anything is applied.
        Map<String, String> variables = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : vars.entrySet()) {
            String name = entry.getKey();
            if (!VALID_NAME.matcher(name).matches()) {
                throw new IllegalArgumentException("Invalid session variable name: " + name);
            }
            Object value = entry.getValue();
            if (value == null) {
                continue;
            }
            if (value instanceof Map || value instanceof List) {
                throw new IllegalArgumentException(
                        "Session variable '" + name + "' must be a JSON string, not a nested JSON value");
            }
            if (!(value instanceof String)) {
                // Every session variable is a VARCHAR, so numbers and booleans must be quoted in the
                // claim rather than coerced — reject them with a hint instead of guessing intent.
                throw new IllegalArgumentException(
                        "Session variable '" + name + "': " + value + " needs to be inside quotes "
                                + "(write it as \"" + name + "\": \"" + value + "\")");
            }
            String text = (String) value;
            if (text.length() > MAX_VALUE_LENGTH) {
                throw new IllegalArgumentException("Session variable '" + name + "' is too long: "
                        + text.length() + " characters, limit is " + MAX_VALUE_LENGTH);
            }
            // DuckDB truncates a SQL string at an embedded NUL, which turns the rendered statement
            // into an unterminated literal and fails the whole request with an unrelated parser
            // error. Reject control characters here, while the message can still name the variable.
            int control = indexOfControlCharacter(text);
            if (control >= 0) {
                throw new IllegalArgumentException(String.format(
                        "Session variable '%s' contains a control character (U+%04X) at index %d",
                        name, (int) text.charAt(control), control));
            }
            variables.put(name, text);
        }
        if (variables.size() > MAX_VARIABLES) {
            throw new IllegalArgumentException("Too many session variables: " + variables.size()
                    + ", limit is " + MAX_VARIABLES);
        }

        validate(variables);

        List<String> sqls = new ArrayList<>(variables.size());
        variables.forEach((name, value) ->
                sqls.add("SET VARIABLE " + quotedIdentifier(name) + " = " + sqlLiteral(value)));
        return sqls;
    }

    /**
     * Policy hook for the requested session variables, called after structural parsing and before
     * any {@code SET VARIABLE} is rendered. The parser already guarantees valid identifier names and
     * string values (what safe SQL rendering needs); this is where higher-level policy belongs —
     * e.g. an allow-list of variable names, per-tenant value constraints, or required variables.
     *
     * <p>Placeholder: the default accepts every variable. Implement policy checks here and throw
     * {@link IllegalArgumentException} to reject a variable set; the request then fails cleanly.
     *
     * @param variables the requested variables (insertion order preserved), all names valid
     *                  identifiers and all values non-null strings
     */
    static void validate(Map<String, String> variables) {
        // TODO: enforce session-variable policy (allowed names, value format/limits, per-tenant
        // rules). No additional checks yet.
    }

    /** Index of the first ISO control character in {@code value}, or -1 if there is none. */
    private static int indexOfControlCharacter(String value) {
        for (int i = 0; i < value.length(); i++) {
            if (Character.isISOControl(value.charAt(i))) {
                return i;
            }
        }
        return -1;
    }

    /**
     * A double-quoted SQL identifier. {@code VALID_NAME} already rejects the double quote, so there
     * is nothing to escape. Quoting matters because the regex accepts reserved words — bare
     * {@code SET VARIABLE table = 'x'} is a DuckDB parser error, {@code SET VARIABLE "table" = 'x'}
     * is not.
     */
    private static String quotedIdentifier(String name) {
        return "\"" + name + "\"";
    }

    /** A single-quoted SQL string literal with embedded quotes doubled. */
    private static String sqlLiteral(String value) {
        return "'" + value.replace("'", "''") + "'";
    }
}

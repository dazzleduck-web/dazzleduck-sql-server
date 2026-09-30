package io.dazzleduck.sql.compaction;

import java.util.regex.Pattern;

/**
 * Masks credentials in text that is about to leave the process as an exported log record.
 *
 * <p>The startup script ATTACHes catalogs and creates secrets, and DuckDB echoes those statements
 * back in its errors: a failed Postgres or DuckLake-on-Postgres ATTACH reports the whole connection
 * string, password included, and a parser error quotes the statement around the error position.
 * Every raw connection re-runs the script, so these errors can recur on every compaction cycle,
 * not only at startup. The exported log table is readable far more widely than the compactor's
 * config, so nothing that looks like a credential may reach it.
 */
final class LogRedaction {

    static final String MASK = "***";

    // Quoted values of secret-like names: CREATE SECRET options (SECRET, KEY_ID, SESSION_TOKEN,
    // ACCOUNT_KEY, CONNECTION_STRING, ...), SET s3_secret_access_key = '...', password = '...'.
    // An echo may be cut off inside the value, so the closing quote is optional.
    private static final Pattern QUOTED_SECRET_VALUE = Pattern.compile(
            "(?i)\\b(\\w*(?:secret|password|passwd|token|key_id|access_key|account_key|connection_string|credential)\\w*)"
                    + "(\\s*=?\\s*)'(?:[^'\\n]|'')*'?");

    // Unquoted values: libpq keywords ("host=... user=u password=VALUE", also inside a quoted string)
    // and URL query parameters ("?access_key=...", an Azure SAS "&sig=...").
    private static final Pattern UNQUOTED_SECRET_VALUE = Pattern.compile(
            "(?i)\\b(\\w*(?:password|passwd|pwd|secret|token|access_key|account_key|key_id)\\w*|sig)"
                    + "(\\s*=\\s*)(?!')[^\\s'\"&;,)]+");

    // user:password@ in a URI (postgresql://u:p@host/db, s3://key:secret@bucket).
    private static final Pattern URI_USERINFO = Pattern.compile(
            "(?i)\\b([a-z][a-z0-9+.-]*://[^/\\s:@'\"]+:)[^@\\s/'\"]+@");

    private static final Pattern BEARER = Pattern.compile("(?i)\\b(bearer\\s+)[A-Za-z0-9._~+/=-]+");

    // DuckDB parser errors quote the statement ("LINE 1: ..."), cut to a window that can start
    // inside a literal, so quotes cannot be paired reliably; the whole echo is masked. The error's
    // first line ("syntax error at or near ...") is kept.
    private static final Pattern ECHO_LINE = Pattern.compile("(?m)^(LINE \\d+:).*$");

    private LogRedaction() {
    }

    static String redact(String text) {
        if (text == null || text.isEmpty()) {
            return text;
        }
        String out = ECHO_LINE.matcher(text).replaceAll("$1 " + MASK);
        out = QUOTED_SECRET_VALUE.matcher(out).replaceAll("$1$2'" + MASK + "'");
        out = UNQUOTED_SECRET_VALUE.matcher(out).replaceAll("$1$2" + MASK);
        out = URI_USERINFO.matcher(out).replaceAll("$1" + MASK + "@");
        out = BEARER.matcher(out).replaceAll("$1" + MASK);
        return out;
    }
}

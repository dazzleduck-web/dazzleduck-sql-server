package io.dazzleduck.sql.commons.ingestion;

/**
 * Lexical helpers for the text of an ingestion transformation, which is embedded in a larger
 * statement — nested as a subquery, or wrapped by the {@code SELECT *, <expr> FROM __this}
 * shorthand — rather than run on its own.
 *
 * <p>A statement may end with {@code ;} and comments, but embedded text may not: a {@code ;} inside
 * a subquery is a syntax error, and a trailing {@code --} comment swallows whatever is appended
 * after it. {@link #stripTrailing} removes exactly that tail and leaves everything else as the user
 * wrote it. The text is scanned the way DuckDB tokenizes it, so a {@code ;} or comment marker inside
 * a string literal, a quoted identifier or a dollar-quoted string is never mistaken for one.
 *
 * <p>The SQL is deliberately not re-rendered through DuckDB's parser: the renderer rewrites lambdas
 * into the deprecated {@code x -> ...} arrow form, which fails to bind once
 * {@code lambda_syntax = 'DISABLE_SINGLE_ARROW'} is in effect, so a transformation written in the
 * current {@code lambda x: ...} syntax would break on every write.
 */
final class TransformationText {

    private TransformationText() {
    }

    /** End of the last code token, and whether a {@code ;} separates two statements. */
    private record Scan(int codeEnd, boolean multipleStatements) {
    }

    /**
     * {@code sql} without its trailing whitespace, statement terminators and comments, so it can be
     * embedded in a larger statement. Comments elsewhere are kept; they are harmless as long as the
     * text that follows the embedded SQL starts on a new line or the text ends in code, which this
     * guarantees.
     */
    static String stripTrailing(String sql) {
        return sql.substring(0, scan(sql).codeEnd());
    }

    /**
     * Whether {@code sql} holds more than one statement: a {@code ;} outside any literal, quoted
     * identifier or comment that is followed by more code.
     */
    static boolean hasMultipleStatements(String sql) {
        return scan(sql).multipleStatements();
    }

    private static Scan scan(String sql) {
        int n = sql.length();
        int codeEnd = 0;
        boolean terminatorPending = false;
        boolean multiple = false;
        int i = 0;
        while (i < n) {
            char c = sql.charAt(i);
            if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
                int newline = sql.indexOf('\n', i);
                i = newline < 0 ? n : newline + 1;
                continue;
            }
            if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
                i = skipBlockComment(sql, i);
                continue;
            }
            if (Character.isWhitespace(c)) {
                i++;
                continue;
            }
            if (c == ';') {
                terminatorPending = true;
                i++;
                continue;
            }
            // A code token. One after a ';' means the ';' separated two statements.
            multiple |= terminatorPending;
            terminatorPending = false;
            if (c == '\'') {
                i = skipQuoted(sql, i, '\'', isEscapeStringPrefix(sql, i));
            } else if (c == '"') {
                i = skipQuoted(sql, i, '"', false);
            } else if (c == '$') {
                int tagEnd = dollarTagEnd(sql, i);
                i = tagEnd < 0 ? i + 1 : skipDollarQuoted(sql, sql.substring(i, tagEnd), tagEnd);
            } else {
                i++;
            }
            codeEnd = i;
        }
        return new Scan(codeEnd, multiple);
    }

    /** Index after a {@code /* ... *}{@code /} comment starting at {@code start}; they nest in DuckDB. */
    private static int skipBlockComment(String sql, int start) {
        int depth = 0;
        int i = start;
        while (i < sql.length()) {
            if (sql.startsWith("/*", i)) {
                depth++;
                i += 2;
            } else if (sql.startsWith("*/", i)) {
                depth--;
                i += 2;
                if (depth == 0) {
                    return i;
                }
            } else {
                i++;
            }
        }
        return sql.length();
    }

    /**
     * Index after the literal opening at {@code start} with {@code quote}. A doubled quote is an
     * escaped quote; in an {@code E'...'} string a backslash escapes the next character too.
     * Unterminated text runs to the end, where DuckDB reports its own error.
     */
    private static int skipQuoted(String sql, int start, char quote, boolean backslashEscapes) {
        int i = start + 1;
        while (i < sql.length()) {
            char c = sql.charAt(i);
            if (backslashEscapes && c == '\\') {
                i += 2;
            } else if (c == quote) {
                if (i + 1 < sql.length() && sql.charAt(i + 1) == quote) {
                    i += 2;
                } else {
                    return i + 1;
                }
            } else {
                i++;
            }
        }
        return sql.length();
    }

    /** Whether the quote at {@code quoteIndex} opens an {@code E'...'} escape string. */
    private static boolean isEscapeStringPrefix(String sql, int quoteIndex) {
        if (quoteIndex == 0 || Character.toUpperCase(sql.charAt(quoteIndex - 1)) != 'E') {
            return false;
        }
        return quoteIndex == 1 || !isIdentifierChar(sql.charAt(quoteIndex - 2));
    }

    /**
     * End (exclusive) of a dollar-quote tag — {@code $$} or {@code $tag$} — starting at
     * {@code start}, or -1 when the {@code $} starts something else, such as a {@code $1} or
     * {@code $name} parameter.
     */
    private static int dollarTagEnd(String sql, int start) {
        if (start > 0 && isIdentifierChar(sql.charAt(start - 1))) {
            return -1;
        }
        int i = start + 1;
        if (i < sql.length() && Character.isDigit(sql.charAt(i))) {
            return -1;
        }
        while (i < sql.length() && isIdentifierChar(sql.charAt(i))) {
            i++;
        }
        return i < sql.length() && sql.charAt(i) == '$' ? i + 1 : -1;
    }

    /** Index after the dollar-quoted string whose opening {@code tag} ends at {@code bodyStart}. */
    private static int skipDollarQuoted(String sql, String tag, int bodyStart) {
        int close = sql.indexOf(tag, bodyStart);
        return close < 0 ? sql.length() : close + tag.length();
    }

    private static boolean isIdentifierChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }
}

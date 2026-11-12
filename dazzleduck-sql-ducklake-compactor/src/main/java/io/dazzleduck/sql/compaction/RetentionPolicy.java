package io.dazzleduck.sql.compaction;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * One row of the data-retention policy table: rows of {@code catalog.schema.table} whose
 * {@code timeColumn} is older than {@code retention} are deleted.
 *
 * <p>Names are validated as plain identifiers and quoted when they are put into SQL, so a policy
 * row cannot inject SQL. {@code retention} is {@code null} for a table kept forever.
 */
record RetentionPolicy(String catalog, String schema, String table, String timeColumn, Duration retention) {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_$]*");

    /** Retention values that keep a table's rows forever. */
    private static final String UNLIMITED = "unlimited";

    private static final Pattern DURATION = Pattern.compile("(\\d+)\\s*([a-z]+)");

    private static final Map<String, ChronoUnit> UNITS = Map.ofEntries(
            Map.entry("s", ChronoUnit.SECONDS), Map.entry("second", ChronoUnit.SECONDS), Map.entry("seconds", ChronoUnit.SECONDS),
            Map.entry("min", ChronoUnit.MINUTES), Map.entry("mins", ChronoUnit.MINUTES),
            Map.entry("minute", ChronoUnit.MINUTES), Map.entry("minutes", ChronoUnit.MINUTES),
            Map.entry("h", ChronoUnit.HOURS), Map.entry("hour", ChronoUnit.HOURS), Map.entry("hours", ChronoUnit.HOURS),
            Map.entry("d", ChronoUnit.DAYS), Map.entry("day", ChronoUnit.DAYS), Map.entry("days", ChronoUnit.DAYS),
            Map.entry("w", ChronoUnit.WEEKS), Map.entry("week", ChronoUnit.WEEKS), Map.entry("weeks", ChronoUnit.WEEKS));

    /**
     * Parses one policy row.
     *
     * @param tableName  {@code catalog.schema.table}; the catalog says which database's cycle owns it
     * @param timeColumn the column whose value is the age of a row
     * @param retention  a duration such as {@code 30 days} or {@code 12h}, or {@code unlimited}
     * @throws IllegalArgumentException when the row cannot be used
     */
    static RetentionPolicy parse(String tableName, String timeColumn, String retention) {
        if (tableName == null || timeColumn == null || retention == null) {
            throw new IllegalArgumentException("table_name, time_column and retention must all be set");
        }
        String[] parts = tableName.trim().split("\\.", -1);
        if (parts.length != 3) {
            throw new IllegalArgumentException(
                    "table_name must be catalog.schema.table, got '%s'".formatted(tableName));
        }
        for (String part : parts) {
            identifier("table_name", part);
        }
        return new RetentionPolicy(parts[0], parts[1], parts[2], identifier("time_column", timeColumn.trim()),
                parseRetention(retention));
    }

    /**
     * A whole number and a unit of seconds or longer, with the unit spelled out: unlike HOCON
     * durations, a bare number (milliseconds there) and {@code m} (minutes there, months to some) are
     * rejected, since a misread retention deletes data.
     */
    private static Duration parseRetention(String value) {
        String trimmed = value.trim().toLowerCase(Locale.ROOT);
        if (trimmed.equals(UNLIMITED)) {
            return null;
        }
        Matcher matcher = DURATION.matcher(trimmed);
        ChronoUnit unit = matcher.matches() ? UNITS.get(matcher.group(2)) : null;
        if (unit == null) {
            throw new IllegalArgumentException(("retention must be a number and a unit (s, min, h, d, w, or spelled out, "
                    + "such as '30 days'), or 'unlimited', got '%s'").formatted(value));
        }
        long amount;
        try {
            amount = Long.parseLong(matcher.group(1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("retention is too long, got '%s'".formatted(value), e);
        }
        if (amount <= 0) {
            throw new IllegalArgumentException("retention must be positive, got '%s'".formatted(value));
        }
        try {
            return unit.getDuration().multipliedBy(amount);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("retention is too long, got '%s'".formatted(value), e);
        }
    }

    private static String identifier(String field, String value) {
        if (!IDENTIFIER.matcher(value).matches()) {
            throw new IllegalArgumentException(
                    "%s must be a plain identifier, got '%s'".formatted(field, value));
        }
        return value;
    }

    boolean unlimited() {
        return retention == null;
    }

    /** {@code catalog.schema.table}, as it is written in the policy table and in metric tags. */
    String name() {
        return catalog + "." + schema + "." + table;
    }

    String quotedTable() {
        return quote(catalog) + "." + quote(schema) + "." + quote(table);
    }

    String quotedTimeColumn() {
        return quote(timeColumn);
    }

    private static String quote(String identifier) {
        return '"' + identifier + '"';
    }
}

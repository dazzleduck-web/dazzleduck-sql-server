package io.dazzleduck.sql.compaction;

import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;

import java.time.Duration;
import java.util.Locale;
import java.util.Map;
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

    /**
     * Parses one policy row.
     *
     * @param tableName  {@code catalog.schema.table}; the catalog says which database's cycle owns it
     * @param timeColumn the column whose value is the age of a row
     * @param retention  a duration as written in the config file ({@code 30 days}, {@code 12h}), or
     *                   {@code unlimited}
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

    private static Duration parseRetention(String value) {
        String trimmed = value.trim();
        if (trimmed.toLowerCase(Locale.ROOT).equals(UNLIMITED)) {
            return null;
        }
        Duration duration;
        try {
            // The same parser as every other duration of this service, so '30 days' and '12h' both work.
            duration = ConfigFactory.parseMap(Map.of("retention", trimmed)).getDuration("retention");
        } catch (ConfigException e) {
            throw new IllegalArgumentException(
                    "retention must be a duration such as '30 days' or 'unlimited', got '%s'".formatted(value), e);
        }
        if (duration.isNegative() || duration.isZero()) {
            throw new IllegalArgumentException("retention must be positive, got '%s'".formatted(value));
        }
        return duration;
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

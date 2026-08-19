package io.dazzleduck.sql.commons;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.ConfigProvider;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Reads configuration overrides from a two-column key/value relation in the attached database.
 *
 * <pre>{@code
 * config_provider {
 *     class = "io.dazzleduck.sql.commons.TableConfigProvider"
 *     table = "mylake.main.v_config"     # required; table or view
 *     key_column = "config_key"          # default: config_key
 *     value_column = "value"             # default: value
 *     prefix = "compaction."             # optional; see below
 * }
 * }</pre>
 *
 * <h2>Deliberately generic</h2>
 *
 * The relation's name and columns are configuration, not constants. A deployment's config schema is
 * its own business — this class knows only "somewhere there is a key/value relation" — so the same
 * provider serves a registry with extra columns (scopes, TTLs, provenance) by pointing it at a view
 * that projects two of them. Baking one deployment's schema in here would make the next deployment
 * fork the file.
 *
 * <h2>prefix</h2>
 *
 * A shared registry namespaces its keys by owner ({@code compaction.snapshot_retention},
 * {@code aggregator.analytics.cron}). {@code prefix} selects that namespace and strips it, so the
 * remainder lines up with the service's own config paths: with {@code prefix = "compaction."} the
 * row {@code compaction.snapshot_retention} overlays the path {@code snapshot_retention}. Without a
 * prefix every row is taken and its key used as the path verbatim.
 *
 * <p>Keys that are not valid HOCON paths, and rows whose key or value is NULL, are skipped rather
 * than failing the read: an unrelated row in a shared registry must not stop a service from
 * starting. A row whose path the service does not recognise is simply inert — typesafe-config
 * carries it and nothing reads it.
 *
 * <h2>Values are strings</h2>
 *
 * Every value is overlaid as a string, and HOCON converts on demand — {@code getDuration} parses
 * {@code "60 minutes"}, {@code getBytes} parses {@code "8MB"}, {@code getInt} parses {@code "4"}.
 * So a registry stores what an operator would have written in the file, and the service's existing
 * accessors keep working unchanged.
 *
 * <p><b>Scalars only.</b> HOCON will not widen a string to a list or object, so a list-valued key
 * cannot be set this way — reading an overridden path with {@code getStringList} throws
 * {@code WrongType}. That limit falls in a sensible place: the list-shaped settings in practice are
 * the ones naming WHICH databases to open, and a provider reading a table inside one of them cannot
 * be what tells the process where to look. Anything needed to REACH the database belongs in the
 * file that gets the process there; everything read afterwards is a candidate for the table.
 */
public class TableConfigProvider implements ConfigProvider {

    private static final System.Logger LOG = System.getLogger(TableConfigProvider.class.getName());

    /** Relation and column names are interpolated into SQL, so they must be plain identifiers. */
    private static final Pattern IDENTIFIER =
            Pattern.compile("[A-Za-z_][A-Za-z0-9_$]*(\\.[A-Za-z_][A-Za-z0-9_$]*){0,2}");

    private String table;
    private String keyColumn = "config_key";
    private String valueColumn = "value";
    private String prefix = "";

    public TableConfigProvider() {
    }

    public TableConfigProvider(Config config) {
        setConfig(config);
    }

    @Override
    public final void setConfig(Config config) {
        this.table = identifier("table", config.getString("table"));
        if (config.hasPath("key_column")) {
            this.keyColumn = identifier("key_column", config.getString("key_column"));
        }
        if (config.hasPath("value_column")) {
            this.valueColumn = identifier("value_column", config.getString("value_column"));
        }
        if (config.hasPath("prefix")) {
            this.prefix = config.getString("prefix");
        }
    }

    @Override
    public Config overrides(Config serviceConfig) throws SQLException {
        Map<String, Object> values = new LinkedHashMap<>();
        String sql = "SELECT " + keyColumn + ", " + valueColumn + " FROM " + table;
        try (Connection connection = ConnectionPool.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sql)) {
            while (rs.next()) {
                String key = rs.getString(1);
                String value = rs.getString(2);
                if (key == null || value == null) {
                    continue;
                }
                if (!key.startsWith(prefix)) {
                    continue;
                }
                String path = key.substring(prefix.length());
                if (path.isEmpty() || !isPath(path)) {
                    LOG.log(System.Logger.Level.DEBUG,
                            "skipping config row with a key that is not a config path: {0}", key);
                    continue;
                }
                values.put(path, value);
            }
        }
        LOG.log(System.Logger.Level.INFO, "config overrides from {0}: {1}", table, values);
        return ConfigFactory.parseMap(values, table);
    }

    /**
     * A conservative check that the remainder is usable as a HOCON path. Deliberately rejects the
     * quoting-sensitive cases rather than trying to escape them: a shared registry can hold
     * anything, and a key that needs quoting is far more likely to be another service's row than a
     * path this one meant to set.
     */
    private static boolean isPath(String path) {
        if (path.startsWith(".") || path.endsWith(".") || path.contains("..")) {
            return false;
        }
        for (String segment : path.split("\\.", -1)) {
            if (segment.isEmpty() || !IDENTIFIER.matcher(segment).matches()) {
                return false;
            }
        }
        return true;
    }

    private static String identifier(String field, String value) {
        if (value == null || !IDENTIFIER.matcher(value).matches()) {
            throw new IllegalArgumentException(
                    CONFIG_PROVIDER_PREFIX + "." + field + " must be a plain identifier"
                            + " (optionally catalog- and schema-qualified), got: " + value);
        }
        return value;
    }
}

package io.dazzleduck.sql.otel.collector;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.otel.collector.config.CollectorConfig;
import io.dazzleduck.sql.otel.collector.config.CollectorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    static {
        // Netty 4.2 on Java 24+ disables sun.misc.Unsafe unless told otherwise, and Arrow's Netty
        // allocator then fails to initialize. The JVM images pass --sun-misc-unsafe-memory-access=allow,
        // but a native image takes no JVM flags, so ask Netty directly. Runs before any Netty class
        // loads; an explicit -Dio.netty.noUnsafe still wins.
        if (System.getProperty("io.netty.noUnsafe") == null) {
            System.setProperty("io.netty.noUnsafe", "false");
        }
    }

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    static class Args {
        @Parameter(names = {"-c", "--config"}, description = "Path to HOCON configuration file")
        String configFile;

        @Parameter(names = {"-h", "--help"}, help = true, description = "Show usage")
        boolean help;

        @Parameter(names = {"-v", "--version"}, description = "Show version")
        boolean version;
    }

    public static void main(String[] argv) throws Exception {
        Args args = new Args();
        JCommander jc = JCommander.newBuilder().addObject(args).build();
        jc.parse(argv);

        if (args.help) {
            jc.usage();
            return;
        }
        if (args.version) {
            System.out.println("dazzleduck-sql-otel-collector 0.0.26");
            return;
        }

        // Run the startup script before calling toProperties() so that extensions like
        // ducklake and arrow are loaded before DuckLakeIngestionHandler queries DuckLake metadata.
        CollectorConfig config = new CollectorConfig(args.configFile);

        // UTC first, so operator connection_settings and the startup script can still override it.
        applyDuckDbDefaults();

        // DuckDB settings for the ingestion instance. The default (disabling the external-file
        // cache) is declared in reference.conf rather than here, so it is visible to operators
        // and overridable per deployment - the ingest path stages each batch to an Arrow file,
        // reads it once via read_arrow([...]) and then deletes it, so that cache can never hit.
        for (String setting : config.getIngestionConnectionSettings()) {
            ConnectionPool.executeOnSingleton(setting);
        }

        String startupScript = config.getStartupScript();
        if (startupScript != null && !startupScript.isBlank()) {
            log.info("Executing startup script");
            ConnectionPool.executeOnSingleton(startupScript);
        }

        CollectorProperties props = config.toProperties();
        OtelCollectorServer server = new OtelCollectorServer(props);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown signal received");
            server.close();
        }, "shutdown-hook"));

        server.start();
        server.blockUntilShutdown();
    }

    /**
     * Instance-wide DuckDB defaults the collector relies on, applied before {@code connection_settings} and the
     * startup script so either can override them.
     *
     * <p>{@code TimeZone = 'UTC'}: timestamp columns are {@code TIMESTAMP WITH TIME ZONE}, and DuckDB turns an instant
     * into calendar parts in the session's time zone. Transformations such as {@code CAST(timestamp AS DATE)},
     * partition expressions, and DuckLake's time partition transforms would otherwise depend on the host's zone, and a
     * bare literal like {@code '2026-10-18 00:00:00'} would be read in it. {@code SET GLOBAL} so every pooled
     * connection, not only the singleton, starts in UTC.
     */
    static void applyDuckDbDefaults() {
        ConnectionPool.executeOnSingleton("SET GLOBAL TimeZone = 'UTC'");
    }
}

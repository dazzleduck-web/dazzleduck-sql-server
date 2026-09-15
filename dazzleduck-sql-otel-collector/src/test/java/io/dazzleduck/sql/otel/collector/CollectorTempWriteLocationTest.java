package io.dazzleduck.sql.otel.collector;

import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.commons.ingestion.IngestionConfig;
import io.dazzleduck.sql.commons.ingestion.IngestionHandler;
import io.dazzleduck.sql.commons.ingestion.IngestionResult;
import io.dazzleduck.sql.commons.ingestion.PostIngestionTask;
import io.dazzleduck.sql.otel.collector.config.CollectorConfig;
import io.dazzleduck.sql.otel.collector.config.CollectorProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code otel_collector.temp_write_location} — the parent directory for each signal service's Arrow
 * scratch directory. Declared in reference.conf with a {@code ${java.io.tmpdir}"/dazzleduck-writes"}
 * default, so it is always present but never has to be set.
 *
 * <p>Validating and creating that directory is {@link OtelCollectorServer}'s job, done once at
 * startup via {@code ConfigConstants.getTempWriteDir}; those checks are covered by
 * {@code ConfigConstantsTempWriteDirTest} in dazzleduck-sql-common. What is tested here is the
 * config value itself and the per-service scratch directory created beneath it.
 */
class CollectorTempWriteLocationTest {

    private static final IngestionConfig CONFIG = new IngestionConfig(
            1024L, IngestionConfig.DEFAULT_MAX_BUCKET_SIZE, IngestionConfig.DEFAULT_MAX_BATCHES,
            IngestionConfig.DEFAULT_MAX_PENDING_WRITE, Duration.ofSeconds(5),
            IngestionConfig.DEFAULT_CONFIG_REFRESH);

    /** Minimal handler — these tests only construct and close, never submit a batch. */
    private static final IngestionHandler NOOP_HANDLER = new IngestionHandler() {
        @Override public PostIngestionTask createPostIngestionTask(IngestionResult r) { return null; }
        @Override public String getTargetPath(String queueId) { return null; }
        @Override public String[] getPartitionBy(String queueId) { return new String[0]; }
    };

    @Test
    void defaultMatchesTheFlightModule() {
        // reference.conf resolves ${java.io.tmpdir}"/dazzleduck-writes", so an unset key still
        // yields a usable directory rather than a missing-key failure -- and the same directory
        // the flight module defaults to (/tmp/dazzleduck-writes on Linux).
        Path expected = Path.of(System.getProperty("java.io.tmpdir"), "dazzleduck-writes");
        assertEquals(expected, Path.of(new CollectorConfig().getTempWriteLocation()));
    }


    @Test
    void explicitValueOverridesTheDefault(@TempDir Path dir) {
        var config = ConfigFactory.parseString(
                "otel_collector.temp_write_location = \"" + dir.toString().replace("\\", "\\\\") + "\"")
                .withFallback(ConfigFactory.load()).resolve();
        assertEquals(dir.toString(), new CollectorConfig(config).getTempWriteLocation());
    }

    @Test
    void programmaticDefaultsAgreeWithTheDeclaredDefault() {
        // The same default is spelled in reference.conf, CollectorConfig.DEFAULT_TEMP_SUBDIRECTORY
        // and the CollectorProperties field initialiser. Only reference.conf drives production, so
        // the other two could drift silently — every collector test builds CollectorProperties
        // directly and would quietly stage batches somewhere else.
        // Compared as Paths, not strings: HOCON concatenation leaves a redundant separator when
        // java.io.tmpdir ends in one (macOS), which Path.of normalises away. The directory is
        // what has to match, not the spelling.
        assertEquals(Path.of(new CollectorConfig().getTempWriteLocation()),
                Path.of(new CollectorProperties().getTempWriteLocation()),
                "CollectorProperties must default to the same directory as the declared config");
    }

    @Test
    void serverCreatesOneScratchDirectoryPerSignalUnderTheConfiguredPath(@TempDir Path dir)
            throws IOException {
        // Creation is the server's job, not a service constructor's, so it is exercised here.
        var server = new OtelCollectorServer(new CollectorProperties());
        Path logs    = server.createScratchDir(dir, OtelLogService.SCRATCH_PREFIX);
        Path traces  = server.createScratchDir(dir, OtelTraceService.SCRATCH_PREFIX);
        Path metrics = server.createScratchDir(dir, OtelMetricsService.SCRATCH_PREFIX);

        for (Path p : new Path[]{logs, traces, metrics}) {
            assertTrue(Files.isDirectory(p), "not created: " + p);
            assertEquals(dir, p.getParent(), "must sit under temp_write_location: " + p);
        }
        assertTrue(logs.getFileName().toString().startsWith("otel-logs-arrow-"), logs.toString());
        assertTrue(traces.getFileName().toString().startsWith("otel-traces-arrow-"), traces.toString());
        assertTrue(metrics.getFileName().toString().startsWith("otel-metrics-arrow-"), metrics.toString());
        try (var entries = Files.list(dir)) {
            assertEquals(3L, entries.count(), "one directory per signal");
        }
    }

    @Test
    void serviceConstructionDoesNoIo(@TempDir Path dir) {
        // The point of moving creation out: constructing a service must not touch the filesystem,
        // so it cannot fail partway and strand an allocator. The directory need not even exist.
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        var metrics = new OtelCollectorMetrics(new SimpleMeterRegistry());
        Path neverCreated = dir.resolve("not-created");
        try {
            var service = new OtelLogService(neverCreated, NOOP_HANDLER, CONFIG, scheduler, metrics);
            service.close();
            assertTrue(Files.notExists(neverCreated), "the constructor must not create anything");
        } finally {
            scheduler.shutdownNow();
            metrics.close();
        }
    }
}

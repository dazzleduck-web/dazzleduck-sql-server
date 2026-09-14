package io.dazzleduck.sql.otel.collector;

import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.commons.ingestion.IngestionConfig;
import io.dazzleduck.sql.commons.ingestion.IngestionHandler;
import io.dazzleduck.sql.commons.ingestion.IngestionResult;
import io.dazzleduck.sql.commons.ingestion.PostIngestionTask;
import io.dazzleduck.sql.otel.collector.config.CollectorConfig;
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
 * {@code otel_collector.temp_path} — the parent directory for each signal service's Arrow scratch
 * directory. Declared in reference.conf with a {@code ${java.io.tmpdir}} default, so it is always
 * present but never has to be set.
 */
class CollectorTempPathTest {

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
    void defaultsToJavaIoTmpdir() {
        // reference.conf resolves ${java.io.tmpdir}, so an unset temp_path still yields a usable
        // directory rather than a missing-key failure.
        assertEquals(System.getProperty("java.io.tmpdir"), new CollectorConfig().getTempPath());
    }

    @Test
    void explicitValueOverridesTheDefault(@TempDir Path dir) {
        var config = ConfigFactory.parseString(
                "otel_collector.temp_path = \"" + dir.toString().replace("\\", "\\\\") + "\"")
                .withFallback(ConfigFactory.load()).resolve();
        assertEquals(dir.toString(), new CollectorConfig(config).getTempPath());
    }

    @Test
    void scratchDirectoryIsCreatedUnderTheConfiguredPath(@TempDir Path dir) throws IOException {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        var metrics = new OtelCollectorMetrics(new SimpleMeterRegistry());
        try (var base = new OtelServiceBase(dir.toString(), "otel-logs-arrow-",
                NOOP_HANDLER, CONFIG, scheduler, metrics)) {
            try (var children = Files.list(dir)) {
                Path created = children.findFirst().orElseThrow(
                        () -> new AssertionError("no scratch directory created under " + dir));
                assertTrue(created.getFileName().toString().startsWith("otel-logs-arrow-"),
                        "unexpected scratch directory name: " + created);
            }
        } finally {
            scheduler.shutdownNow();
            metrics.close();
        }
    }

    @Test
    void scratchDirectoryIsRemovedOnClose(@TempDir Path dir) throws IOException {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        var metrics = new OtelCollectorMetrics(new SimpleMeterRegistry());
        var base = new OtelServiceBase(dir.toString(), "otel-traces-arrow-",
                NOOP_HANDLER, CONFIG, scheduler, metrics);
        base.close();
        scheduler.shutdownNow();
        metrics.close();
        try (var children = Files.list(dir)) {
            assertEquals(0L, children.count(), "close() must remove its scratch directory");
        }
    }

    @Test
    void missingDirectoryFailsAtStartup(@TempDir Path dir) {
        // A typo'd path must fail while the collector is starting, not on the first export RPC
        // once telemetry is already flowing.
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        var metrics = new OtelCollectorMetrics(new SimpleMeterRegistry());
        try {
            Path missing = dir.resolve("does-not-exist");
            IOException e = assertThrows(IOException.class, () -> new OtelServiceBase(
                    missing.toString(), "otel-logs-arrow-", NOOP_HANDLER, CONFIG, scheduler, metrics));
            assertTrue(e.getMessage().contains("not an existing directory"), e.getMessage());
        } finally {
            scheduler.shutdownNow();
            metrics.close();
        }
    }

    @Test
    void blankValueIsRejected() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        var metrics = new OtelCollectorMetrics(new SimpleMeterRegistry());
        try {
            IOException e = assertThrows(IOException.class, () -> new OtelServiceBase(
                    "  ", "otel-logs-arrow-", NOOP_HANDLER, CONFIG, scheduler, metrics));
            assertTrue(e.getMessage().contains("must not be blank"), e.getMessage());
        } finally {
            scheduler.shutdownNow();
            metrics.close();
        }
    }
}

package io.dazzleduck.sql.micrometer;

import io.dazzleduck.sql.commons.util.TestUtils;
import io.dazzleduck.sql.micrometer.metrics.MetricsRegistryFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class HttpMetricIntegrationTest {

    private static final int PORT = 8081;
    private String warehouse;

    @BeforeAll
    void startServer() throws Exception {
        warehouse = "/tmp/" + java.util.UUID.randomUUID();
        Files.createDirectories(Path.of(warehouse));

        io.dazzleduck.sql.runtime.Main.main(new String[]{
                "--conf", "dazzleduck_server.networking_modes=[http]",
                "--conf", "dazzleduck_server.http.port=" + PORT,
                "--conf", "dazzleduck_server.http.auth=jwt",
                "--conf", "dazzleduck_server.warehouse=" + warehouse,
                "--conf", "dazzleduck_server.ingestion.max_delay_ms=500"
        });


    }

    @Test
    void testMetricsCanPostAndPersist() throws Exception {

        MeterRegistry registry = MetricsRegistryFactory.create();

        try {
            Counter counter = Counter.builder("records.processed")
                            .description("Number of records processed")
                            .register(registry);

            Timer timer = Timer.builder("record.processing.time")
                            .description("Time spent processing records")
                            .register(registry);

            for (int i = 0; i < 10; i++) {
                timer.record(100, TimeUnit.MILLISECONDS);
                counter.increment();
            }

        } finally {
            registry.close();
        }

        Path metricFile = waitForMetricFile(Path.of(warehouse));
        assertNotNull(metricFile, "Metric parquet file was not created");

        TestUtils.isEqual("""
                select 'records.processed' as name,
                       'counter'           as type,
                       10.0                as value,
                       'ap101'             as applicationId,
                       'MyApplication'     as applicationName,
                       'localhost'         as host
                """,
                """
                select name, type, value, applicationId, applicationName, host
                from read_parquet('%s')
                where name = 'records.processed'
                """.formatted(metricFile.toAbsolutePath())
        );
    }

    @AfterAll
    void cleanupWarehouse() throws Exception {
        Path path = Path.of(warehouse);
        if (Files.exists(path)) {
            Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException ignored) {}
                    });
        }
    }

    private Path waitForMetricFile(Path warehouseDir) throws Exception {
        Thread.sleep(400); // > ingestion.max_delay_ms

        try (var files = Files.list(warehouseDir)) {
            return files.filter(Files::isRegularFile)
                    .findFirst()
                    .orElse(null);
        }
    }
}

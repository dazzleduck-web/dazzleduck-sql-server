package io.dazzleduck.sql.micrometer;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.util.ConfigUtils;
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

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class HttpMetricIntegrationTest {

    private Thread serverThread;

    @BeforeAll
    void startServers() throws Exception {
        Config config = ConfigFactory.load().getConfig(ConfigUtils.CONFIG_PATH);

        serverThread = new Thread(() -> {
            try {
                io.dazzleduck.sql.runtime.Main.start(config);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        serverThread.setDaemon(true);
        serverThread.start();

        waitForHttpServer();
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
            Thread.sleep(100);

        } finally {
            registry.close();
        }

        String warehousePath = ConfigUtils.getWarehousePath(ConfigFactory.load().getConfig(ConfigUtils.CONFIG_PATH));
        Path metricFile = waitForMetricFile(warehousePath);

        TestUtils.isEqual("""
                select 'records.processed' as name,
                       'counter'           as type,
                       10.0                as value,
                       'ap101'             as application_id,
                       'MyApplication'     as application_name,
                       'localhost'         as application_host
                """,
                """
                select name, type, value, application_id, application_name, hostapplication_host
                from read_parquet('%s')
                where name = 'records.processed'
                """.formatted(metricFile.toAbsolutePath())
        );
    }

    @AfterAll
    void cleanupWarehouse() throws Exception {
        String warehousePath = ConfigUtils.getWarehousePath(ConfigFactory.load().getConfig(ConfigUtils.CONFIG_PATH));

        Path path = Path.of(warehousePath);
        if (Files.exists(path)) {
            Files.walk(path).sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException ignored) {}
                    });
        }

        Files.createDirectories(path);
    }

    private void waitForHttpServer() throws Exception {
        int port = 8081;
        long deadline = System.currentTimeMillis() + 10_000;

        while (System.currentTimeMillis() < deadline) {
            try (var socket = new java.net.Socket("localhost", port)) {
                return;
            } catch (IOException e) {
                Thread.sleep(100);
            }
        }
        throw new IllegalStateException("HTTP server did not start");
    }

    private Path waitForMetricFile(String warehousePath) throws Exception {

        Path dir = Path.of(warehousePath);
        long deadline = System.currentTimeMillis() + 15_000;

        while (System.currentTimeMillis() < deadline) {
            try (var stream = Files.list(dir)) {
                var file = stream.filter(Files::isRegularFile).findFirst();
                if (file.isPresent()) {
                    return file.get();
                }
            }
            Thread.sleep(200);
        }

        throw new IllegalStateException("No metric file found in " + dir);
    }
}

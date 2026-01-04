package io.dazzleduck.sql.micrometer;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.client.HttpProducer;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.micrometer.service.ArrowMicroMeterRegistry;
import io.dazzleduck.sql.micrometer.util.ArrowFileWriterUtil;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.MockClock;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Disabled
class ArrowMicroMeterRegistryTest {

    private ArrowMicroMeterRegistry registry;
    private MockClock testClock;
    private static final Logger log = LoggerFactory.getLogger(ArrowMicroMeterRegistryTest.class);
    private File tempFile;
    private String application_id;
    private String application_name;
    private String application_host;
    static Schema schema;
    @BeforeEach
    void setup() {

        // Load test metadata EXACTLY like production
        Config dazzConfig = ConfigFactory.load().getConfig("dazzleduck_micrometer");
        application_id = dazzConfig.getString("application_id");
        application_name = dazzConfig.getString("application_name");
        application_host = dazzConfig.getString("application_host");

        testClock = new MockClock();

        schema = new Schema(java.util.List.of(new Field("timestamp", FieldType.nullable(new ArrowType.Utf8()), null)));
        ConnectionPool.execute("load arrow");
        HttpProducer sender = new HttpProducer(
                schema,
                "http://localhost:8080",
                "admin",
                "admin",
                "arrow.outputFile",
                Duration.ofSeconds(6),
                1024,                          // minBatchSize (test)
                2048,                          // maxBatchSize (test)
                Duration.ofSeconds(6),      // maxSendInterval
                3,                          // retryCount
                1000,                       // retryIntervalMillis
                java.util.List.of(),        // transformations
                java.util.List.of(),        // partitionBy
                10_000_000,                 // maxInMemoryBytes
                10_000_000,                 // maxOnDiskBytes
                java.time.Clock.systemUTC()       // or testClock if desired
        );

        registry = new ArrowMicroMeterRegistry(
                sender,
                testClock,
                Duration.ofSeconds(5),      // step interval
                application_id,
                application_name,
                application_host
        );

    }

    @Test
    void counterGaugeTimerTest() throws Exception {
        Counter counter = Counter.builder("demo.counter").description("demo counter").tag("env", "dev").register(registry);
        counter.increment(5.0);

        double[] gaugeState = new double[]{12.5};
        Gauge.builder("demo.gauge", gaugeState, a -> a[0]).description("demo gauge").register(registry);

        Timer timer = Timer.builder("demo.timer").publishPercentiles(0.5, 0.95).description("demo timer").register(registry);
        timer.record(Duration.ofMillis(150));
        timer.record(Duration.ofMillis(300));

        File tempFile = File.createTempFile("test-metrics", ".arrow");
        try {
            ArrowFileWriterUtil.writeMetersToFile(
                    new ArrayList<>(registry.getMeters()),
                    tempFile.getAbsolutePath(),
                    application_id,
                    application_name,
                    application_host
            );

            TestUtils.isEqual(
                    "select unnest(['demo.counter', 'demo.gauge', 'demo.timer.percentile', 'demo.timer.percentile', 'demo.timer']) as name",
                    "select name from read_arrow('%s')".formatted(tempFile.getAbsolutePath())
            );
        } finally {
            tempFile.delete();
        }
    }

    @Test
    void postAllRegistryMetersTest() throws Exception {
        Counter counter = Counter.builder("demo.counter").description("demo counter").tag("env", "dev").register(registry);
        counter.increment(3.0);

        double[] gaugeState = new double[]{42.0};
        Gauge.builder("demo.gauge", gaugeState, a -> a[0]).description("demo gauge").register(registry);

        Timer timer = Timer.builder("demo.timer").publishPercentiles(0.5, 0.95).description("demo timer").register(registry);
        timer.record(Duration.ofMillis(100));
        timer.record(Duration.ofMillis(250));

        DistributionSummary summary = DistributionSummary.builder("demo.summary").description("demo summary").register(registry);
        summary.record(10);
        summary.record(20);

        LongTaskTimer longTaskTimer = LongTaskTimer.builder("demo.longtask").description("long task").register(registry);
        LongTaskTimer.Sample sample = longTaskTimer.start();
        testClock.add(Duration.ofMillis(50));
        sample.stop();

        AtomicLong functionCounterState = new AtomicLong(7);
        FunctionCounter.builder("demo.function.counter", functionCounterState, AtomicLong::doubleValue).description("function counter").register(registry);

        AtomicLong functionTimerCount = new AtomicLong(5);
        AtomicLong functionTimerTime = new AtomicLong(500);

        FunctionTimer.builder("demo.function.timer", functionTimerCount, AtomicLong::longValue,
                        obj -> functionTimerTime.doubleValue(), TimeUnit.MILLISECONDS)
                .description("function timer").register(registry);

        File tempFile = File.createTempFile("test-metrics", ".arrow");
        try {
            ArrowFileWriterUtil.writeMetersToFile(
                    new ArrayList<>(registry.getMeters()),
                    tempFile.getAbsolutePath(),
                    application_id,
                    application_name,
                    application_host
            );

            TestUtils.isEqual(
                    "select unnest(['demo.counter', 'demo.function.counter', 'demo.summary', 'demo.gauge', 'demo.timer.percentile', 'demo.timer.percentile', 'demo.longtask', 'demo.function.timer', 'demo.timer']) as 'name'",
                    "select name from read_arrow('%s')".formatted(tempFile.getAbsolutePath())
            );
        } finally {
            tempFile.delete();
        }
    }

    @Test
    void testFunctionCounterSerialization() throws Exception {
        AtomicLong state = new AtomicLong(0);
        FunctionCounter.builder("test.fn.counter", state, AtomicLong::doubleValue).register(registry);
        state.set(42);

        File tempFile = File.createTempFile("test-metrics-", ".arrow");
        try {
            ArrowFileWriterUtil.writeMetersToFile(
                    new ArrayList<>(registry.getMeters()),
                    tempFile.getAbsolutePath(),
                    application_id,
                    application_name,
                    application_host
            );

            TestUtils.isEqual(
                    "select 'test.fn.counter' as name",
                    "select name from read_arrow('%s')".formatted(tempFile.getAbsolutePath())
            );
        } finally {
            tempFile.delete();
        }
    }

    @Test
    void testFunctionTimerSerialization() throws Exception {
        AtomicLong count = new AtomicLong(5);
        AtomicLong totalTime = new AtomicLong(200);
        FunctionTimer.builder("test.fn.timer", count, AtomicLong::longValue,
                        obj -> totalTime.doubleValue(), TimeUnit.MILLISECONDS)
                .register(registry);

        File tempFile = File.createTempFile("test-metrics-", ".arrow");
        try {
            ArrowFileWriterUtil.writeMetersToFile(
                    new ArrayList<>(registry.getMeters()),
                    tempFile.getAbsolutePath(),
                    application_id,
                    application_name,
                    application_host
            );

            TestUtils.isEqual(
                    "select 'test.fn.timer' as name",
                    "select name from read_arrow('%s')".formatted(tempFile.getAbsolutePath())
            );
        } finally {
            tempFile.delete();
        }
    }

    @Test
    @Order(1)
    void testDistributionSummarySerialization() throws Exception {
        DistributionSummary summary = DistributionSummary.builder("test.summary").register(registry);
        summary.record(5);
        summary.record(15);
        Thread.sleep(100);
        // Advance step window
        testClock.add(Duration.ofSeconds(6));

        tempFile = File.createTempFile("test-metrics-", ".arrow");

        ArrowFileWriterUtil.writeMetersToFile(
                new ArrayList<>(registry.getMeters()),
                tempFile.getAbsolutePath(),
                application_id,
                application_name,
                application_host
        );

        Assertions.assertTrue(tempFile.exists(), "Temp file should exist after serialization");
    }

    @Test
    @Order(2)
    void distributionSummaryTestAssertion() throws SQLException, IOException {
        Assertions.assertNotNull(tempFile, "Temp file should have been created in previous test");

        TestUtils.isEqual(
                "select unnest(['test.summary']) as name",
                "select name from read_arrow('%s')".formatted(tempFile.getAbsolutePath())
        );

        TestUtils.isEqual(
                "select unnest(['distribution_summary']) as type",
                "select type from read_arrow('%s')".formatted(tempFile.getAbsolutePath())
        );

        TestUtils.isEqual(
                "select 2.0 as value, 0.0 as min, 15.0 as max, 10.0 as mean",
                "select value, min, max, mean from read_arrow('%s')".formatted(tempFile.getAbsolutePath())
        );

        TestUtils.isEqual(
                """
                select 'test.summary' as name,
                       'distribution_summary' as type,
                       'ap101' as application_id,
                       'MyApplication' as application_name,
                       'localhost' as application_host
                """,
                "select name, type, application_id, application_name, application_host from read_arrow('%s')"
                        .formatted(tempFile.getAbsolutePath())
        );
    }
}
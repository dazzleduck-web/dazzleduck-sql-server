package io.dazzleduck.sql.micrometer;

import io.dazzleduck.sql.micrometer.config.ArrowRegistryConfig;
import io.dazzleduck.sql.micrometer.service.ArrowMicroMeterRegistry;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.Clock;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class Main {
    private static final String serverUrl = "http://localhost:8080/arrow";

    public static void main(String[] args) throws Exception {
        // configuration (publishes every 10s)
        ArrowRegistryConfig config = new ArrowRegistryConfig() {
            @Override
            public String get(String k) {
                if ("arrow.enabled".equals(k)) return "true";
                return null;
            }

            @Override
            public Duration step() {
                return Duration.ofSeconds(10);
            }
        };

        ArrowMicroMeterRegistry registry = new ArrowMicroMeterRegistry.Builder()
                .config(new ArrowRegistryConfig() {
                    @Override
                    public String get(String key) {
                        if ("arrow.outputFile".equals(key)) {
                            return "D:/Development/micrometer-metries/src/main/resources/metrics.arrow";
                        }
                        return null;
                    }
                    @Override
                    public boolean enabled() { return true; }
                })
                .endpoint(serverUrl)
                .httpTimeout(Duration.ofSeconds(5))
                .clock(Clock.SYSTEM)
                .build();

        // ----------------
        // Create meters
        // ----------------

        // 1. Counter
        Counter counter = Counter.builder("demo.counter")
                .description("demo counter")
                .tag("env", "dev")
                .register(registry);

        // 2. Gauge
        double[] gaugeState = new double[]{12.5};
        Gauge.builder("demo.gauge", gaugeState, a -> a[0])
                .description("demo gauge")
                .tag("val", "write")
                .register(registry);

        // 3. Timer
        Timer timer = Timer.builder("demo.timer")
                .publishPercentiles(0.5, 0.95)
                .description("demo timer")
                .register(registry);

        // 4. DistributionSummary
        DistributionSummary summary = DistributionSummary.builder("demo.summary")
                .description("demo summary")
                .register(registry);

        // 5. LongTaskTimer
        LongTaskTimer longTaskTimer = LongTaskTimer.builder("demo.longtask")
                .description("demo long running task")
                .register(registry);

        // 6. FunctionCounter (wraps an object and derives a count)
        AtomicLong functionCounterState = new AtomicLong(0);
        FunctionCounter.builder("demo.function.counter", functionCounterState,
                        AtomicLong::doubleValue)
                .description("function counter tracking state value")
                .register(registry);

        // 7. FunctionTimer (measures count + total time)
        AtomicLong functionTimerCount = new AtomicLong(0);
        AtomicLong functionTimerTime = new AtomicLong(0);
        FunctionTimer.builder("demo.function.timer",
                        functionTimerCount,
                        AtomicLong::longValue, // count function
                        obj -> functionTimerTime.doubleValue(), // total time function
                        TimeUnit.MILLISECONDS)
                .description("function timer tracking ops and time")
                .register(registry);

        // ----------------
        // Simulate workload for ~30 seconds
        // ----------------
        Random rand = new Random();
        long end = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < end) {
            counter.increment(rand.nextInt(5) + 1);

            gaugeState[0] = rand.nextDouble() * 100;

            timer.record(50 + rand.nextInt(150), TimeUnit.MILLISECONDS);

            summary.record(rand.nextDouble() * 500);

            // simulate long task
            LongTaskTimer.Sample sample = longTaskTimer.start();
            Thread.sleep(rand.nextInt(50));
            sample.stop();

            // function counter/timer updates
            functionCounterState.addAndGet(1);
            functionTimerCount.incrementAndGet();
            functionTimerTime.addAndGet(rand.nextInt(200));

            Thread.sleep(500);
        }

        // graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown: closing registry...");
            try {
                registry.close();
            } catch (Exception ignored) {}
        }));

        System.out.println("App started; metrics will be auto-published by ArrowMicroMeterRegistry every step.");
    }
}

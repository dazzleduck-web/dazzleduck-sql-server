package io.dazzleduck.sql.example;

import io.dazzleduck.sql.logger.ArrowSimpleLogger;
import io.dazzleduck.sql.micrometer.metrics.MetricsRegistryFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SampleApplication {



    public static void main(String[] args) {
        String warehousePath = "/tmp/dazzleduckWarehouse/metric";
        new java.io.File(warehousePath).mkdirs();
        String warehousePath2 = "/tmp/dazzleduckWarehouse/log";
        new java.io.File(warehousePath2).mkdirs();
        MeterRegistry registry = MetricsRegistryFactory.create();
        ArrowSimpleLogger logger = new ArrowSimpleLogger(SampleApplication.class.getName());
        Counter processedCounter = Counter.builder("records.processed")
                .description("Number of records processed")
                .register(registry);

        Timer processingTimer = Timer.builder("record.processing.duration")
                .description("Time spent processing records")
                .register(registry);

        try {
            logger.info("Sample application started");
            simulateWork(processedCounter, processingTimer);
            logger.info("Sample application finished successfully");
        } finally {
            registry.close(); // flush metrics
        }
    }

    private static void simulateWork(
            Counter processedCounter,
            Timer processingTimer
    ) {

        for (int i = 1; i <= 10; i++) {
            final int recordNumber = i;

            processingTimer.record(() -> {
                try {
                 //   logger.info("Processing record {}", recordNumber);
                    // Simulate I/O-bound work
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    //logger.error("Record processing interrupted", e);
                    throw new IllegalStateException("Processing interrupted", e);
                }
            });

            processedCounter.increment();
        }
    }
}

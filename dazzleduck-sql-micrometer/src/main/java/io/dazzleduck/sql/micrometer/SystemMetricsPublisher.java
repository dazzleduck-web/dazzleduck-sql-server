package io.dazzleduck.sql.micrometer;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.system.DiskSpaceMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Binds JVM and OS system metrics to a Micrometer registry.
 *
 * <p>Registers the following metric groups:
 * <ul>
 *   <li>{@code system.cpu.*} — CPU usage via {@link ProcessorMetrics}</li>
 *   <li>{@code jvm.memory.*} — Heap and non-heap memory via {@link JvmMemoryMetrics}</li>
 *   <li>{@code jvm.gc.*} — GC pause count and duration via {@link JvmGcMetrics}</li>
 *   <li>{@code disk.free}, {@code disk.total} — Disk space for the warehouse path</li>
 * </ul>
 *
 * <p>All binders are from {@code micrometer-core} — no additional dependency required.
 */
public final class SystemMetricsPublisher {

    private static final Logger logger = LoggerFactory.getLogger(SystemMetricsPublisher.class);

    private SystemMetricsPublisher() {}

    /**
     * Bind all system metrics to the registry.
     *
     * @param registry      the registry to bind metrics to
     * @param warehousePath path used for disk space metrics; falls back to the JVM working directory
     */
    public static void bind(MeterRegistry registry, String warehousePath) {
        new ProcessorMetrics().bindTo(registry);
        logger.info("Bound CPU metrics (system.cpu.*) to registry");

        new JvmMemoryMetrics().bindTo(registry);
        logger.info("Bound JVM memory metrics (jvm.memory.*) to registry");

        new JvmGcMetrics().bindTo(registry);
        logger.info("Bound JVM GC metrics (jvm.gc.*) to registry");

        File diskPath = resolveDiskPath(warehousePath);
        new DiskSpaceMetrics(diskPath).bindTo(registry);
        logger.info("Bound disk space metrics for path: {}", diskPath.getAbsolutePath());
    }

    private static File resolveDiskPath(String warehousePath) {
        if (warehousePath != null && !warehousePath.isBlank()
                && !warehousePath.startsWith("s3://")
                && !warehousePath.startsWith("s3a://")) {
            File f = new File(warehousePath);
            if (f.exists()) {
                return f;
            }
        }
        return new File(System.getProperty("user.dir"));
    }
}

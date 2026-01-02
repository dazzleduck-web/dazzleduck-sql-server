package io.dazzleduck.sql.micrometer.service;

import io.dazzleduck.sql.common.ingestion.FlightSender;
import io.dazzleduck.sql.common.types.JavaRow;
import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.step.StepMeterRegistry;
import io.micrometer.core.instrument.step.StepRegistryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class ArrowMicroMeterRegistry extends StepMeterRegistry implements AutoCloseable {

    private static final Logger log =
            LoggerFactory.getLogger(ArrowMicroMeterRegistry.class);

    private final FlightSender sender;
    private final String application_id;
    private final String application_name;
    private final String application_host;

    public ArrowMicroMeterRegistry(
            FlightSender sender,
            Clock clock,
            Duration step,
            String applicationId,
            String applicationName,
            String applicationHost
    ) {
        super(new StepRegistryConfig() {
            @Override public String prefix() { return "arrow"; }
            @Override public Duration step() { return step; }
            @Override public String get(String key) { return null; }
        }, clock);

        this.sender = sender;
        this.application_id = applicationId;
        this.application_name = applicationName;
        this.application_host = applicationHost;
    }

    @Override
    protected void publish() {
        for (Meter meter : getMeters()) {
            try {
                sender.addRow(toRow(meter));
            } catch (Exception e) {
                log.warn("Failed to publish meter {}", meter.getId(), e);
            }
        }
    }

    private JavaRow toRow(Meter meter) {
        Meter.Id id = meter.getId();

        Map<String, String> tags = new LinkedHashMap<>();
        for (Tag t : id.getTags()) {
            tags.put(t.getKey(), t.getValue());
        }

        double value = 0, min = 0, max = 0, mean = 0;

        try {
            switch (meter) {
                case Counter c -> value = c.count();
                case Gauge g -> value = g.value();

                case Timer t -> {
                    HistogramSnapshot s = t.takeSnapshot();
                    value = t.count();
                    max = s.max(TimeUnit.SECONDS);
                    mean = s.mean(TimeUnit.SECONDS);
                }

                case DistributionSummary ds -> {
                    value = ds.count();
                    max = ds.max();
                    mean = ds.mean();
                }

                case LongTaskTimer ltt -> {
                    value = ltt.activeTasks();
                    double total = ltt.duration(TimeUnit.SECONDS);
                    max = total;
                    if (value > 0) mean = total / value;
                }

                case FunctionCounter fc -> value = fc.count();

                case FunctionTimer ft -> {
                    value = ft.count();
                    double total = ft.totalTime(TimeUnit.SECONDS);
                    max = total;
                    if (value > 0) mean = total / value;
                }

                default -> {
                    double total = 0;
                    int c = 0;
                    for (Measurement m : meter.measure()) {
                        total += m.getValue();
                        c++;
                    }
                    if (c > 0) {
                        value = total;
                        mean = total / c;
                    }
                }
            }
        } catch (Exception e) {
            log.debug("Metric evaluation error: {}", id.getName(), e);
        }

        return new JavaRow(new Object[]{
                id.getName(),
                id.getType().name().toLowerCase(),
                application_id,
                application_name,
                application_host,
                tags,
                value,
                min,
                max,
                mean
        });
    }

    @Override
    protected TimeUnit getBaseTimeUnit() {
        return TimeUnit.SECONDS;
    }
    @Override
    public void close() {
        try {
            log.info("Closing ArrowMicroMeterRegistry");
            super.close();          // stops scheduler & publish loop
        } catch (Exception e) {
            log.warn("Error while closing StepMeterRegistry", e);
        }

        try {
            sender.close();         // flush + shutdown HttpSender
        } catch (Exception e) {
            log.warn("Error while closing HttpSender", e);
        }
    }

    @Override
    protected DistributionStatisticConfig defaultHistogramConfig() {
        return DistributionStatisticConfig.DEFAULT;
    }
}

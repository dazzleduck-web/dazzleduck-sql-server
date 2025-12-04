package io.dazzleduck.sql.flight.ingestion;

import io.dazzleduck.sql.commons.ingestion.Batch;
import org.apache.arrow.flight.sql.impl.FlightSql;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.function.Function;

public record IngestionParameters(String path,
                                  String format, String[] partitions, String[] transformations,
                                  String[] sortOrder, String producerId, Long producerBatchId,
                                  Map<String, String> parameters) {
    public static final Duration DEFAULT_MAX_DELAY = Duration.ofSeconds(1);

    public static final long DEFAULT_MAX_BUCKET_SIZE = 16 * 1024 * 1024;

    public Batch<String> constructBatch(long size, String tempFile) {
        return new Batch<>(
                sortOrder,
                transformations,
                partitions,
                tempFile,
                producerId,
                producerBatchId,
                size,
                format,
                Instant.now()
        );
    }

    public FlightSql.CommandStatementIngest createCommand() {
        var options = Map.of("path", path(), "partitions", String.join(",", partitions()), "format", format(), "transformations", String.join(",", transformations()), "sort_orders", String.join(",", sortOrder()));
        return FlightSql.CommandStatementIngest.newBuilder().putAllOptions(options).build();
    }

    public String completePath(String warehousePath) {
        return Path.of(warehousePath).resolve(path).toString();
    }
    public static IngestionParameters getIngestionParameters(FlightSql.CommandStatementIngest command){
        Map<String, String > optionMap = command.getOptionsMap();
        String path = optionMap.get("path");
        String format = optionMap.getOrDefault("format", "parquet");

        Function<String, String[]> splitCsv = value -> {
            if (value == null || value.isBlank()) return new String[0];
            return java.util.Arrays.stream(value.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toArray(String[]::new);
        };

        // Optional comma-separated lists
        String[] partitions = splitCsv.apply(optionMap.get("partitions"));
        String[] transformations = splitCsv.apply(optionMap.get("transformations"));
        String[] sortOrder = splitCsv.apply(optionMap.get("sort_orders"));

        String table = optionMap.getOrDefault("table", "");
        return new IngestionParameters(path, format, partitions, transformations, sortOrder, table, 0L, Map.of());
    }
}

package io.dazzleduck.sql.logger.tailing;

import io.dazzleduck.sql.client.HttpSender;
import io.dazzleduck.sql.commons.ConnectionPool;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class LogTailToArrowProcessorTest {

    static final int PORT = 8094;
    static Schema schema;

    @TempDir
    static Path warehouse;
    @TempDir
    Path tempDir;
    static final String APPLICATION_ID = "test-app-id";
    static final String APPLICATION_NAME = "TestApplication";
    static final String APPLICATION_HOST = "localhost";

    @BeforeAll
    static void setup() throws Exception {
        String warehousePath = warehouse.toAbsolutePath().toString().replace("\\", "\\\\");
        io.dazzleduck.sql.runtime.Main.main(new String[]{
                "--conf", "dazzleduck_server.http.port=" + PORT,
                "--conf", "dazzleduck_server.http.auth=jwt",
                "--conf", "dazzleduck_server.warehouse=\"" + warehousePath + "\"",
                "--conf", "dazzleduck_server.ingestion.max_delay_ms=500"
        });

        JsonToArrowConverter converter = new JsonToArrowConverter(APPLICATION_ID, APPLICATION_NAME, APPLICATION_HOST);        schema = converter.getSchema();
        converter.close();
    }

    @BeforeEach
    void setupEach() {
        org.awaitility.Awaitility.setDefaultPollInterval(5, TimeUnit.MILLISECONDS);
        org.awaitility.Awaitility.setDefaultTimeout(3, TimeUnit.SECONDS);
    }

    @AfterEach
    void teardownEach() {
        org.awaitility.Awaitility.reset();
    }

    @Test
    void withSingleFile_endToEndTest() throws Exception {
        // targetDir where dd_uuid.parquet will be created
        String targetDir = "withSingleFileDir";
        // because target directory must exist
        Files.createDirectories(Path.of(warehouse.toString(), targetDir));

        // Create temp log file
        Path logFile = tempDir.resolve("app.log");
        Files.writeString(logFile, """
                {"timestamp":"2024-01-01T10:00:00Z","level":"INFO","thread":"main","logger":"App","message":"Hello"}
                {"timestamp":"2024-01-01T10:00:01Z","level":"WARN","thread":"main","logger":"App","message":"World"}
                """);
        // Create REAL HttpSender
        try (HttpSender sender = new HttpSender(schema, "http://localhost:" + PORT, "admin", "admin", targetDir, Duration.ofSeconds(5), 1, Duration.ofSeconds(1), 3, 1000, java.util.List.of(), java.util.List.of(), 10_000_000, 10_000_000)) {
            JsonToArrowConverter converter = new JsonToArrowConverter(APPLICATION_ID, APPLICATION_NAME, APPLICATION_HOST);
            LogTailToArrowProcessor processor = new LogTailToArrowProcessor(tempDir.toString(), "*.log", converter, sender, 100);
            processor.start();
            // Verify ingestion via DuckDB
            await().ignoreExceptions().untilAsserted(() -> {
                long count = ConnectionPool.collectFirst("select count(*) from read_parquet('%s/%s/*.parquet')".formatted(warehouse, targetDir), Long.class);
                assertEquals(2, count);
            });
            processor.close();
        }
    }

    @Test
    void withMultipleFiles_endToEndTest() throws Exception {
        String targetDir = "withMultipleFileDir";
        Files.createDirectories(Path.of(warehouse.toString(), targetDir));
        // Creating and writing logs in 3 files inside same directory.
        Path logFile1 = tempDir.resolve("first.log");
        Path logFile2 = tempDir.resolve("second.log");
        Path logFile3 = tempDir.resolve("third.log");
        // add application_name , application_id, application_host, application_name -->> in log columns.
        Files.writeString(logFile1, """
                {"timestamp":"2024-01-01T10:00:00Z","level":"INFO","thread":"main","logger":"App","message":"Hello file1"}
                {"timestamp":"2024-01-01T10:00:01Z","level":"WARN","thread":"main","logger":"App","message":"World"}
                """);
        Files.writeString(logFile2, """
                {"timestamp":"2024-01-01T10:00:00Z","level":"INFO","thread":"main","logger":"App","message":"Namaste file2"}
                """);
        Files.writeString(logFile3, """
                {"timestamp":"2024-01-01T10:00:00Z","level":"INFO","thread":"main","logger":"App","message":"Hello"}
                {"timestamp":"2024-01-01T10:00:01Z","level":"WARN","thread":"main","logger":"App","message":"Hii"}
                {"timestamp":"2024-01-01T10:00:01Z","level":"WARN","thread":"main","logger":"App","message":"file3"}
                """);

        try (HttpSender sender = new HttpSender(schema, "http://localhost:" + PORT, "admin", "admin", targetDir, Duration.ofSeconds(5), 1, Duration.ofSeconds(1), 3, 1000, java.util.List.of(), java.util.List.of(), 10_000_000, 10_000_000)) {
            // Start processor
            JsonToArrowConverter converter = new JsonToArrowConverter(APPLICATION_ID, APPLICATION_NAME, APPLICATION_HOST);
            LogTailToArrowProcessor processor = new LogTailToArrowProcessor(tempDir.toString(), "*.log", converter, sender, 100);
            processor.start();
            // Verify ingestion via DuckDB
            await().ignoreExceptions().untilAsserted(() -> {
                long count = ConnectionPool.collectFirst("select count(*) from read_parquet('%s/%s/*.parquet')".formatted(warehouse, targetDir), Long.class);
                assertEquals(6, count);
            });
            processor.close();
        }
    }

    @Test
    void invalidJsonLines_areSkipped_andNotIngested() throws Exception {
        String targetDir = "invalidJsonLinesDir";
        Files.createDirectories(Path.of(warehouse.toString(), targetDir));
        Path logFile = tempDir.resolve("bad.log");
        Files.writeString(logFile, """
                {"timestamp":"2024-01-01","level":"INFO","message":"OK"}
                {this is not json}
                {"timestamp":"2024-01-01","level":"WARN","message":"OK"}
                """);

        try (HttpSender sender = new HttpSender(schema, "http://localhost:" + PORT, "admin", "admin", targetDir, Duration.ofSeconds(5), 1, Duration.ofSeconds(1), 3, 1000, java.util.List.of(), java.util.List.of(), 10_000_000, 10_000_000)) {
            JsonToArrowConverter converter = new JsonToArrowConverter(APPLICATION_ID, APPLICATION_NAME, APPLICATION_HOST);
            LogTailToArrowProcessor processor = new LogTailToArrowProcessor(tempDir.toString(), "*.log", converter, sender, 100);
            processor.start();
            await().ignoreExceptions().untilAsserted(() -> {
                long count = ConnectionPool.collectFirst("select count(*) from read_parquet('%s/%s/*.parquet')".formatted(warehouse, targetDir), Long.class);
                // Only 2 valid JSON rows should be ingested
                assertEquals(2, count);
            });
            processor.close();
        }
    }

    @Test
    void emptyLogFile_doesNotCreateParquet() throws Exception {
        String targetDir = "emptyLogFileDir";
        Files.createDirectories(Path.of(warehouse.toString(), targetDir));
        Path logFile = tempDir.resolve("empty.log");
        Files.createFile(logFile);

        try (HttpSender sender = new HttpSender(schema, "http://localhost:" + PORT, "admin", "admin", targetDir, Duration.ofSeconds(3), 1, Duration.ofSeconds(1), 3, 1000, java.util.List.of(), java.util.List.of(), 10_000_000, 10_000_000)) {
            JsonToArrowConverter converter = new JsonToArrowConverter(APPLICATION_ID, APPLICATION_NAME, APPLICATION_HOST);
            LogTailToArrowProcessor processor = new LogTailToArrowProcessor(tempDir.toString(), "*.log", converter, sender, 100);
            processor.start();
            // Wait briefly and assert file does NOT exist
            await().during(1, TimeUnit.SECONDS).untilAsserted(() -> {
                assertThrows(Exception.class, () -> ConnectionPool.collectFirst("select count(*) from read_parquet('%s/%s/*.parquet')".formatted(warehouse, targetDir), Long.class));
            });
            processor.close();
        }
    }

    @Test
    void missingLogFile_doesNotCrashProcessor() throws Exception {
        String targetDir = "missingLogFileDir";
        Files.createDirectories(Path.of(warehouse.toString(), targetDir));
        tempDir.resolve("missing.log"); // not created

        try (HttpSender sender = new HttpSender(schema, "http://localhost:" + PORT, "admin", "admin", targetDir, Duration.ofSeconds(3), 1, Duration.ofSeconds(1), 3, 1000, java.util.List.of(), java.util.List.of(), 10_000_000, 10_000_000)) {
            JsonToArrowConverter converter = new JsonToArrowConverter(APPLICATION_ID, APPLICATION_NAME, APPLICATION_HOST);
            LogTailToArrowProcessor processor = new LogTailToArrowProcessor(tempDir.toString(), "*.log", converter, sender, 100);
            processor.start();
            // Just ensure no ingestion happens and no crash
            await().during(1, TimeUnit.SECONDS).untilAsserted(() -> assertTrue(processor.isRunning()));
            processor.close();
        }
    }
}

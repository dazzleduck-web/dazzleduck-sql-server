package io.dazzleduck.sql.logger;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.types.JavaRow;
import io.dazzleduck.sql.commons.types.VectorSchemaRootWriter;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.*;
import org.slf4j.Marker;
import org.slf4j.event.Level;
import org.slf4j.event.LoggingEvent;
import org.slf4j.helpers.LegacyAbstractLogger;

import java.io.ByteArrayOutputStream;
import java.io.Serial;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * SLF4J logger that prints to console and asynchronously sends Arrow batches to Flight server.
 */
public class ArrowSimpleLogger extends LegacyAbstractLogger {

    @Serial
    private static final long serialVersionUID = 1L;

    private static final int MAX_BATCH_SIZE = 10;
    private static final RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    // schema stays EXACTLY the same
    private static final Schema schema = new Schema(java.util.List.of(
            new Field("timestamp", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("level", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("logger", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("thread", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("message", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("applicationId", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("applicationName", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("host", FieldType.nullable(new ArrowType.Utf8()), null)
    ));

    // load config ONCE — names unchanged
    private static final Config config = ConfigFactory.load().getConfig("dazzleduck_logger");
    private static final String CONFIG_APPLICATION_ID = config.getString("application_id");
    private static final String CONFIG_APPLICATION_NAME = config.getString("application_name");
    private static final String CONFIG_HOST = config.getString("host");

    private static final DateTimeFormatter TS_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    // -------- instance fields (names unchanged) ----------
    private final String name;
    private final ArrowHttpPoster httpPoster;
    private final Queue<JavaRow> batchBuffer = new ConcurrentLinkedQueue<>();
    final AtomicInteger batchCounter = new AtomicInteger(0);
    final String applicationId;
    final String applicationName;
    final String host;

    public ArrowSimpleLogger(String name) {
        this(name, new ArrowHttpPoster());
    }

    public ArrowSimpleLogger(String name, ArrowHttpPoster sender) {
        this.name = name;
        this.httpPoster = sender;
        this.applicationId = CONFIG_APPLICATION_ID;
        this.applicationName = CONFIG_APPLICATION_NAME;
        this.host = CONFIG_HOST;
    }

    @Override
    protected String getFullyQualifiedCallerName() {
        return name;
    }

    @Override
    protected void handleNormalizedLoggingCall(
            Level level, Marker marker, String messagePattern,
            Object[] args, Throwable throwable) {

        String message = format(messagePattern, args);
        writeArrowAsync(level, message);
    }
    private String format(String pattern, Object[] args) {
        if (args == null || args.length == 0) return pattern;
        String msg = pattern;
        for (Object arg : args)
            msg = msg.replaceFirst("\\{}", arg == null ? "null" : arg.toString());
        return msg;
    }
    /** Collect logs in batches of 10 and send to Flight */
    private void writeArrowAsync(Level level, String message) {
        try {
            JavaRow row = new JavaRow(new Object[]{
                    TS_FORMAT.format(LocalDateTime.now()),
                    level.toString(),
                    name,
                    Thread.currentThread().getName(),
                    message,
                    applicationId,
                    applicationName,
                    host

            });

            batchBuffer.add(row);
            if (batchCounter.incrementAndGet() >= MAX_BATCH_SIZE) {
                flushBatch();
            }

        } catch (Exception e) {
            System.err.println("[ArrowSimpleLogger] enqueue failed: " + e.getMessage());
        }
    }
    /** Serialize batched logs into Arrow and send */
    private void flushBatch() {
        if (batchBuffer.isEmpty()) return;

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
             ByteArrayOutputStream out = new ByteArrayOutputStream();
             ArrowStreamWriter streamWriter = new ArrowStreamWriter(root, null, out)) {

            JavaRow[] rows = batchBuffer.toArray(new JavaRow[0]);
            batchBuffer.clear();
            batchCounter.set(0);

            var writer = VectorSchemaRootWriter.of(schema);
            writer.writeToVector(rows, root);
            root.setRowCount(rows.length);

            streamWriter.start();
            streamWriter.writeBatch();
            streamWriter.end();

            httpPoster.enqueue(out.toByteArray());
        } catch (Exception e) {
            System.err.println("[ArrowSimpleLogger] flushBatch failed: " + e.getMessage());
        }
    }

    public void flush() {
        while (!batchBuffer.isEmpty()) {
            flushBatch();
        }
    }
    /** Close logger and flush remaining logs */
    public void close() {
        flush();
        httpPoster.close();
    }
    /** For SLF4J event forwarding */
    public void log(LoggingEvent event) {
        if (isLevelEnabled(event.getLevel().toInt())) {
            writeArrowAsync(event.getLevel(), event.getMessage());
        }
    }
    // === Log level controls ===
    @Override public boolean isTraceEnabled() { return isLevelEnabled(0); }
    @Override public boolean isDebugEnabled() { return isLevelEnabled(10); }
    @Override public boolean isInfoEnabled()  { return isLevelEnabled(20); }
    @Override public boolean isWarnEnabled()  { return isLevelEnabled(30); }
    @Override public boolean isErrorEnabled() { return isLevelEnabled(40); }

    private boolean isLevelEnabled(int levelInt) {
        int defaultLevel = 20; // INFO
        return levelInt >= defaultLevel;
    }
}

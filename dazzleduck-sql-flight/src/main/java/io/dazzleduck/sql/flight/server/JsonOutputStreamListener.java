package io.dazzleduck.sql.flight.server;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.io.SerializedString;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * A ServerStreamListener that writes Arrow batches as JSON to an OutputStream.
 *
 * <p>Supports three {@link Format}s:
 * <ul>
 *   <li>{@link Format#ARRAY} — a single JSON array of row objects (default).</li>
 *   <li>{@link Format#SINGLE_OBJECT} — the first row only, as a bare object
 *       (errors with {@link NoSuchElementException} if there are no rows).</li>
 *   <li>{@link Format#JSONL} — JSON Lines / NDJSON: one row object per line,
 *       newline-terminated, with no enclosing array.</li>
 * </ul>
 */
public class JsonOutputStreamListener implements FlightProducer.ServerStreamListener {

    /** Output shape written by this listener. */
    public enum Format {
        /** One JSON array of row objects. */
        ARRAY,
        /** Only the first row, as a bare JSON object. */
        SINGLE_OBJECT,
        /** JSON Lines / NDJSON: one newline-terminated object per row. */
        JSONL
    }

    private static final Logger logger = LoggerFactory.getLogger(JsonOutputStreamListener.class);
    private static final JsonFactory JSON_FACTORY = new JsonFactory();
    // JavaTimeModule + ISO output so java.time values (e.g. non-TZ TIMESTAMP -> LocalDateTime),
    // including those nested inside structs/lists, serialize as strings rather than failing.
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final Supplier<OutputStream> outputStreamSupplier;
    private final CompletableFuture<Void> future;
    private final Format format;

    private OutputStream outputStream;
    private JsonGenerator generator;
    private VectorSchemaRoot root;
    private boolean firstRowWritten = false;

    public JsonOutputStreamListener(Supplier<OutputStream> outputStreamSupplier, CompletableFuture<Void> future) {
        this(outputStreamSupplier, future, Format.ARRAY);
    }

    public JsonOutputStreamListener(Supplier<OutputStream> outputStreamSupplier, CompletableFuture<Void> future, boolean includeArrayBrackets) {
        this(outputStreamSupplier, future, includeArrayBrackets ? Format.ARRAY : Format.SINGLE_OBJECT);
    }

    public JsonOutputStreamListener(Supplier<OutputStream> outputStreamSupplier, CompletableFuture<Void> future, Format format) {
        this.outputStreamSupplier = outputStreamSupplier;
        this.future = future;
        this.format = format;
    }

    @Override
    public boolean isCancelled() {
        return future.isCancelled();
    }

    @Override
    public void setOnCancelHandler(Runnable handler) {
        // No-op for HTTP streaming
    }

    @Override
    public boolean isReady() {
        // We are ready if the future is not complete
        return !future.isDone();
    }

    @Override
    public synchronized void start(VectorSchemaRoot root, DictionaryProvider dictionaries, IpcOption option) {
        this.root = root;
        try {
            // ARRAY and JSONL commit the response eagerly so an empty result still
            // produces a valid body ("[]" / empty stream). SINGLE_OBJECT defers until
            // the first row so a missing row can surface as an error status.
            if (format == Format.ARRAY || format == Format.JSONL) {
                ensureGenerator();
            }
            logger.debug("JsonOutputStreamListener started with schema: {}, format: {}",
                    root.getSchema(), format);
        } catch (Exception e) {
            logger.error("Error in start()", e);
            future.completeExceptionally(e);
        }
    }

    private void ensureGenerator() throws IOException {
        if (generator == null) {
            this.outputStream = outputStreamSupplier.get();
            this.generator = JSON_FACTORY.createGenerator(outputStream);
            switch (format) {
                case ARRAY -> generator.writeStartArray();
                // Suppress the default single-space separator between root-level values;
                // JSONL writes its own newline after each object instead.
                case JSONL -> generator.setRootValueSeparator(new SerializedString(""));
                case SINGLE_OBJECT -> { /* bare object, no wrapper */ }
            }
        }
    }

    @Override
    public synchronized void putNext() {
        try {
            ensureGenerator();
            writeRows();
            generator.flush();
        } catch (IOException e) {
            logger.error("Error in putNext()", e);
            future.completeExceptionally(e);
        }
    }

    @Override
    public synchronized void putNext(ArrowBuf metadata) {
        putNext();
    }

    @Override
    public synchronized void putMetadata(ArrowBuf metadata) {
        // No-op
    }

    @Override
    public synchronized void error(Throwable ex) {
        try {
            if (generator != null) {
                generator.close();
            } else if (outputStream != null) {
                outputStream.close();
            }
        } catch (Exception ignored) {
        } finally {
            future.completeExceptionally(ex);
        }
    }

    @Override
    public synchronized void completed() {
        try {
            if (!firstRowWritten && format == Format.SINGLE_OBJECT) {
                throw new NoSuchElementException("No rows found");
            }
            if (generator != null) {
                if (format == Format.ARRAY) {
                    generator.writeEndArray();
                }
                generator.flush();
                generator.close();
            }
            future.complete(null);
        } catch (Exception e) {
            if (!(e instanceof NoSuchElementException)) {
                logger.error("Error in completed()", e);
            }
            future.completeExceptionally(e);
        }
    }

    private void writeRows() throws IOException {
        List<FieldVector> vectors = root.getFieldVectors();
        int rowCount = root.getRowCount();
        for (int row = 0; row < rowCount; row++) {
            if (format == Format.SINGLE_OBJECT && firstRowWritten) {
                break; // Only write the first row in single-object mode
            }
            generator.writeStartObject();
            for (FieldVector vector : vectors) {
                writeField(vector, row);
            }
            generator.writeEndObject();
            if (format == Format.JSONL) {
                generator.writeRaw('\n'); // newline-delimit each row object
            }
            firstRowWritten = true;
        }
    }

    private void writeField(FieldVector vector, int row) throws IOException {
        String name = vector.getName();
        if (vector.isNull(row)) {
            generator.writeNullField(name);
            return;
        }

        switch (vector.getMinorType()) {
            case TINYINT:
                generator.writeNumberField(name, ((TinyIntVector) vector).get(row));
                break;
            case SMALLINT:
                generator.writeNumberField(name, ((SmallIntVector) vector).get(row));
                break;
            case INT:
                generator.writeNumberField(name, ((IntVector) vector).get(row));
                break;
            case BIGINT:
                generator.writeNumberField(name, ((BigIntVector) vector).get(row));
                break;
            case FLOAT4:
                generator.writeNumberField(name, ((Float4Vector) vector).get(row));
                break;
            case FLOAT8:
                generator.writeNumberField(name, ((Float8Vector) vector).get(row));
                break;
            case BIT:
                generator.writeBooleanField(name, ((BitVector) vector).get(row) != 0);
                break;
            case VARCHAR:
                generator.writeStringField(name, ((VarCharVector) vector).getObject(row).toString());
                break;
            case VARBINARY:
                generator.writeBinaryField(name, ((VarBinaryVector) vector).get(row));
                break;

            // Date types
            case DATEDAY:
                generator.writeStringField(name, LocalDate.ofEpochDay(((DateDayVector) vector).get(row)).toString());
                break;
            case DATEMILLI:
                generator.writeStringField(name, LocalDate.ofEpochDay(((DateMilliVector) vector).get(row) / 86_400_000L).toString());
                break;

            // Time types
            case TIMESEC:
                generator.writeStringField(name, LocalTime.ofSecondOfDay(((TimeSecVector) vector).get(row)).toString());
                break;
            case TIMEMILLI:
                generator.writeStringField(name, LocalTime.ofNanoOfDay((long) ((TimeMilliVector) vector).get(row) * 1_000_000L).toString());
                break;
            case TIMEMICRO:
                generator.writeStringField(name, LocalTime.ofNanoOfDay(((TimeMicroVector) vector).get(row) * 1_000L).toString());
                break;
            case TIMENANO:
                generator.writeStringField(name, LocalTime.ofNanoOfDay(((TimeNanoVector) vector).get(row)).toString());
                break;

            // Timezone-aware timestamp types
            case TIMESTAMPSECTZ:
                generator.writeStringField(name, Instant.ofEpochSecond(((TimeStampSecTZVector) vector).get(row)).toString());
                break;
            case TIMESTAMPMILLITZ:
                generator.writeStringField(name, Instant.ofEpochMilli(((TimeStampMilliTZVector) vector).get(row)).toString());
                break;
            case TIMESTAMPMICROTZ: {
                long micros = ((TimeStampMicroTZVector) vector).get(row);
                generator.writeStringField(name, Instant.ofEpochSecond(
                        Math.floorDiv(micros, 1_000_000L),
                        Math.floorMod(micros, 1_000_000L) * 1_000L).toString());
                break;
            }
            case TIMESTAMPNANOTZ: {
                long nanos = ((TimeStampNanoTZVector) vector).get(row);
                generator.writeStringField(name, Instant.ofEpochSecond(
                        Math.floorDiv(nanos, 1_000_000_000L),
                        Math.floorMod(nanos, 1_000_000_000L)).toString());
                break;
            }

            // Fallback for complex types (List, Struct, Map etc)
            default:
                Object value = vector.getObject(row);
                if (value != null) {
                    generator.writeFieldName(name);
                    MAPPER.writeValue(generator, value);
                } else {
                    generator.writeNullField(name);
                }
                break;
        }
    }
}

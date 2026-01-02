package io.dazzleduck.sql.client.tailing;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.sql.client.tailing.model.LogMessage;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts JSON log messages to Apache Arrow format
 */
public final class JsonToArrowConverter implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(JsonToArrowConverter.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final BufferAllocator allocator;
    private final Schema schema;
    private final String application_id;
    private final String application_name;
    private final String application_host;

    public JsonToArrowConverter(String applicationId, String applicationName, String applicationHost) {
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.application_id = applicationId;
        this.application_name = applicationName;
        this.application_host = applicationHost;
        this.schema = createArrowSchema();
    }

    /**
     * Get the Arrow schema for log messages
     */
    public Schema getSchema() {
        return schema;
    }

    /**
     * Convert list of JSON log lines to Arrow VectorSchemaRoot
     * @param jsonLines List of JSON log lines
     * @param fileName Name of the file these logs came from
     */
    public VectorSchemaRoot convertToArrow(List<String> jsonLines, String fileName) {
        if (jsonLines == null || jsonLines.isEmpty()) {
            return null;
        }

        List<LogMessage> logMessages = parseJsonLines(jsonLines);

        if (logMessages.isEmpty()) {
            logger.warn("No valid log messages parsed from {} lines", jsonLines.size());
            return null;
        }

        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.setRowCount(logMessages.size());
        populateVectors(root, logMessages, fileName);
        return root;
    }

    private List<LogMessage> parseJsonLines(List<String> jsonLines) {
        List<LogMessage> logMessages = new ArrayList<>();

        for (String jsonLine : jsonLines) {
            try {
                LogMessage log = MAPPER.readValue(jsonLine, LogMessage.class);
                logMessages.add(log);
            } catch (IOException e) {
                logger.error("Failed to parse JSON line: {}", jsonLine, e);
            }
        }

        return logMessages;
    }

    private Schema createArrowSchema() {
        return new Schema(List.of(
                new Field("timestamp", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("level", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("logger", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("thread", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("message", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("application_id", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("application_name", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("application_host", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("file_name", FieldType.nullable(new ArrowType.Utf8()), null)));
    }

    private void populateVectors(VectorSchemaRoot root, List<LogMessage> logMessages, String fileName) {
        VarCharVector timestampVec = (VarCharVector) root.getVector("timestamp");
        VarCharVector levelVec = (VarCharVector) root.getVector("level");
        VarCharVector loggerVec = (VarCharVector) root.getVector("logger");
        VarCharVector threadVec = (VarCharVector) root.getVector("thread");
        VarCharVector messageVec = (VarCharVector) root.getVector("message");
        VarCharVector applicationIdVec = (VarCharVector) root.getVector("application_id");
        VarCharVector applicationNameVec = (VarCharVector) root.getVector("application_name");
        VarCharVector applicationHostVec = (VarCharVector) root.getVector("application_host");
        VarCharVector fileNameVec = (VarCharVector) root.getVector("file_name");

        for (int i = 0; i < logMessages.size(); i++) {
            LogMessage log = logMessages.get(i);

            setVectorValue(timestampVec, i, log.timestamp());
            setVectorValue(levelVec, i, log.level());
            setVectorValue(loggerVec, i, log.logger());
            setVectorValue(threadVec, i, log.thread());
            setVectorValue(messageVec, i, log.message());
            setVectorValue(applicationIdVec, i, application_id);
            setVectorValue(applicationNameVec, i, application_name);
            setVectorValue(applicationHostVec, i, application_host);
            setVectorValue(fileNameVec, i, fileName);
        }
    }

    private void setVectorValue(VarCharVector vector, int index, String value) {
        if (value != null) {
            vector.setSafe(index, value.getBytes());
        } else {
            vector.setNull(index);
        }
    }

    @Override
    public void close() {
        allocator.close();
    }
}
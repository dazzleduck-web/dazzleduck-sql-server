package io.dazzleduck.sql.flight.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.dazzleduck.sql.common.util.CryptoUtils;
import org.apache.arrow.flight.FlightDescriptor;

import javax.annotation.Nullable;
import java.io.IOException;

public record StatementHandle(String query, long queryId, String producerId, long splitSize,
                              @Nullable String queryChecksum) {

    public StatementHandle(String query, long queryId, String producerId, long splitSize){
        this(query, queryId, producerId, splitSize, null);
    }


    private static final ObjectMapper objectMapper = new ObjectMapper();

    byte[] serialize() {
        try {
            return objectMapper.writeValueAsBytes(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean signatureMismatch(String key) {
        return !CryptoUtils.generateHMACSHA1(key, queryId + ":" + query).equals(queryChecksum);
    }

    public StatementHandle signed(String key) {
        String checksum = CryptoUtils.generateHMACSHA1(key, queryId + ":" + query);
        return new StatementHandle(this.query, this.queryId, this.producerId(), this.splitSize, checksum);
    }
    public static StatementHandle deserialize(byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, StatementHandle.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static StatementHandle deserialize(ByteString bytes) {
        return deserialize(bytes.toByteArray());
    }

    public static StatementHandle fromFlightDescriptor(FlightDescriptor flightDescriptor) {
        return deserialize(flightDescriptor.getCommand());
    }
}

package io.dazzleduck.sql.commons.ingestion;

import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class IngestionConfigTest {

    private static IngestionConfig parse(String hocon) {
        return IngestionConfig.fromConfig(ConfigFactory.parseString(
                "min_bucket_size = 1048576\nmax_delay_ms = 2000\nparquet_compression = snappy\n" + hocon));
    }

    @Test
    void shouldReadConfiguredCodec() {
        assertEquals("zstd", parse("parquet_compression = zstd").parquetCompression());
    }

    @Test
    void shouldNormalizeCase() {
        assertEquals("lz4_raw", parse("parquet_compression = LZ4_RAW").parquetCompression());
    }

    @Test
    void shouldRejectUnsupportedCodec() {
        var e = assertThrows(IllegalArgumentException.class, () -> parse("parquet_compression = lzo"));
        assertTrue(e.getMessage().contains("lzo"), e.getMessage());
        assertTrue(e.getMessage().contains("zstd"), e.getMessage());
    }

    /** Callers that build the record directly leave the COPY option off entirely. */
    @Test
    void shouldLeaveCodecUnsetForProgrammaticCallers() {
        assertNull(new IngestionConfig(1024L, 2048L, 4, 4096L,
                Duration.ofMillis(10), Duration.ofMinutes(1)).parquetCompression());
    }
}

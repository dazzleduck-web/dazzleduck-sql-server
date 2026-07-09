package io.dazzleduck.sql.commons;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Characterizes DuckDB's {@code VARIANT} type as seen through this project's Arrow pipeline.
 *
 * <p>DuckDB (1.5.x, this project uses 1.5.4.0) has a native {@code VARIANT} type that works in SQL
 * and Parquet. However, its Arrow C-Data-interface export does <b>not</b> implement {@code VARIANT}:
 * exporting a {@code VARIANT} column throws
 * {@code Not implemented Error: Unsupported Arrow type VARIANT}. So a {@code VARIANT} value cannot be
 * read over Arrow directly — callers must first project it to {@code VARCHAR}/JSON.
 *
 * <p>On the Arrow side, the Java library gained the {@code arrow.parquet.variant} canonical extension
 * type in <b>arrow-java 19.0.0</b> — the version this project is now on ({@code arrow.version} =
 * 19.0.0, the latest published Java artifact). So the Arrow half of native variant support is in
 * place; the remaining blocker is entirely on DuckDB, which must implement Arrow {@code VARIANT}
 * export before a native variant vector can round-trip.
 *
 * <p>{@link #nativeVariantArrowExportIsUnsupported()} pins that current limitation. When DuckDB adds
 * Arrow {@code VARIANT} export it will start failing — the signal to add native variant-vector
 * handling (the {@code arrow.parquet.variant} extension type is already available).
 */
public class VariantTypeTest {

    /** VARIANT works in DuckDB SQL: cast to VARIANT and inspect with variant_typeof. */
    @Test
    public void variantSqlTypeofWorks() throws Exception {
        try (DuckDBConnection conn = ConnectionPool.getConnection()) {
            assertEquals("INT32",
                    ConnectionPool.collectFirst(conn,
                            "SELECT variant_typeof(42::VARIANT)::VARCHAR", String.class));
            assertEquals("VARCHAR",
                    ConnectionPool.collectFirst(conn,
                            "SELECT variant_typeof('hello'::VARIANT)::VARCHAR", String.class));
        }
    }

    /** A VARIANT projected to text exports over Arrow fine (as Utf8). */
    @Test
    public void variantProjectedToTextExportsOverArrow() throws Exception {
        try (DuckDBConnection conn = ConnectionPool.getConnection();
             BufferAllocator allocator = new RootAllocator();
             ArrowReader reader = ConnectionPool.getReader(conn, allocator,
                     "SELECT (42::VARIANT)::VARCHAR AS v", 100)) {
            var field = reader.getVectorSchemaRoot().getSchema().getFields().get(0);
            assertEquals(ArrowType.Utf8.INSTANCE, field.getType());
            assertTrue(reader.loadNextBatch());
            assertEquals("42", reader.getVectorSchemaRoot().getVector(0).getObject(0).toString());
        }
    }

    /** VARIANT survives a Parquet round-trip (Parquet Variant encoding, not the Arrow interface). */
    @Test
    public void variantRoundTripsThroughParquet() throws Exception {
        try (DuckDBConnection conn = ConnectionPool.getConnection()) {
            String dir = Files.createTempDirectory("variant-parquet-test").toString();
            String file = dir + "/v.parquet";
            ConnectionPool.execute(conn,
                    "COPY (SELECT 42::VARIANT AS v) TO '" + file + "' (FORMAT parquet)");
            assertEquals("INT32",
                    ConnectionPool.collectFirst(conn,
                            "SELECT variant_typeof(v)::VARCHAR FROM read_parquet('" + file + "')", String.class));
        }
    }

    /**
     * Characterization: DuckDB cannot yet export a native VARIANT column over Arrow.
     * If this stops throwing, DuckDB has added Arrow VARIANT support — bump arrow to &ge; 19.0.0.
     */
    @Test
    public void nativeVariantArrowExportIsUnsupported() throws Exception {
        try (DuckDBConnection conn = ConnectionPool.getConnection();
             BufferAllocator allocator = new RootAllocator()) {
            IOException ex = assertThrows(IOException.class, () -> {
                try (ArrowReader reader = ConnectionPool.getReader(conn, allocator,
                        "SELECT 42::VARIANT AS v", 100)) {
                    reader.loadNextBatch();
                }
            });
            // DuckDB's exact wording varies ("Unsupported Arrow type VARIANT" /
            // "Unsupported type in DuckDB -> Arrow Conversion: VARIANT"); assert the essentials.
            String msg = ex.getMessage();
            assertTrue(msg.contains("VARIANT") && msg.contains("Unsupported"),
                    "Expected DuckDB to reject Arrow export of VARIANT, but got: " + msg);
        }
    }
}

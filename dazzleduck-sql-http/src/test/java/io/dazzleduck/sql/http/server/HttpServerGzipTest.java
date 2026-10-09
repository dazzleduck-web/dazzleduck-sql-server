package io.dazzleduck.sql.http.server;

import io.dazzleduck.sql.common.ContentTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Responses are gzip-compressed when, and only when, the client asks with Accept-Encoding: gzip, on
 * HTTP/1.1 and HTTP/2, for every output format; decompressed, they are the uncompressed response.
 */
public class HttpServerGzipTest extends HttpServerTestBase {

    // Many batches (x-dd-fetch-size 1000), so the body is streamed with a flush per batch.
    private static final String QUERY =
            "SELECT i, 'row number ' || i AS s, i * 1.5 AS d FROM range(30000) t(i) ORDER BY i";

    @BeforeAll
    static void setup() throws Exception {
        initWarehouse();
        initClient();
        initPort();
        startServer();
        installArrowExtension();
    }

    @AfterAll
    static void cleanup() throws Exception {
        cleanupWarehouse();
    }

    static Stream<Arguments> formatsAndVersions() {
        return Stream.of(ContentTypes.TEXT_TSV, ContentTypes.APPLICATION_JSONL, "arrow")
                .flatMap(format -> Stream.of(HttpClient.Version.HTTP_1_1, HttpClient.Version.HTTP_2)
                        .map(version -> Arguments.of(format, version)));
    }

    private HttpResponse<byte[]> query(String format, HttpClient.Version version, boolean gzip) throws Exception {
        var uri = URI.create(baseUrl + "/v1/query?q=" + URLEncoder.encode(QUERY, StandardCharsets.UTF_8));
        var builder = authenticatedRequestBuilder(uri).GET().version(version).header("x-dd-fetch-size", "1000");
        if (!format.equals("arrow")) {
            builder.header("Accept", format);
        }
        if (gzip) {
            builder.header("Accept-Encoding", "gzip");
        }
        var response = client.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
        assertEquals(200, response.statusCode(), () -> new String(response.body(), StandardCharsets.UTF_8));
        return response;
    }

    @ParameterizedTest(name = "{0} over {1}")
    @MethodSource("formatsAndVersions")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void gzipOnlyWhenAskedAndLossless(String format, HttpClient.Version version) throws Exception {
        var plain = query(format, version, false);
        assertTrue(plain.headers().firstValue("Content-Encoding").isEmpty(),
                "not asked for, not compressed: " + plain.headers().firstValue("Content-Encoding"));

        var compressed = query(format, version, true);
        assertEquals("gzip", compressed.headers().firstValue("Content-Encoding").orElse(null));
        byte[] decompressed;
        try (var in = new GZIPInputStream(new ByteArrayInputStream(compressed.body()))) {
            decompressed = in.readAllBytes();
        }
        assertArrayEquals(plain.body(), decompressed, "decompressed, the same bytes as uncompressed");
        if (!format.equals("arrow")) {
            // Text compresses several times over; Arrow is already ZSTD-compressed inside.
            assertTrue(compressed.body().length * 3 < plain.body().length,
                    "compressed " + compressed.body().length + " of " + plain.body().length + " bytes");
        }
    }
}

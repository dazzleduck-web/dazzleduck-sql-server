package io.dazzleduck.sql.http.server;

import com.fasterxml.jackson.databind.JsonNode;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.common.ContentTypes;
import io.dazzleduck.sql.http.server.model.QueryRequest;
import io.helidon.http.HeaderValues;
import org.junit.jupiter.api.*;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class HttpServerJsonlTest extends HttpServerTestBase {

    @BeforeAll
    static void setup() throws Exception {
        initWarehouse();
        initClient();
        initPort();
        startServer();
        installArrowExtension();
        ConnectionPool.execute("CREATE TABLE jsonl_test(id INTEGER, name VARCHAR, score INTEGER)");
        ConnectionPool.execute("INSERT INTO jsonl_test VALUES (1, 'Alice', 10), (2, 'Bob', 20), (3, 'Carol', 30)");
    }

    @AfterAll
    static void cleanup() throws Exception {
        ConnectionPool.execute("DROP TABLE IF EXISTS jsonl_test");
        cleanupWarehouse();
    }

    // ==================== BASIC QUERY TESTS ====================

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testQueryJsonlWithGet() throws Exception {
        var query = "SELECT * FROM jsonl_test ORDER BY id";
        var request = authenticatedRequestBuilder(uriForQuery(query))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.APPLICATION_JSONL)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        List<JsonNode> rows = parseJsonl(response.body());
        assertEquals(3, rows.size());
        assertEquals(1, rows.get(0).get("id").asInt());
        assertEquals("Alice", rows.get(0).get("name").asText());
        assertEquals(10, rows.get(0).get("score").asInt());
        assertEquals("Carol", rows.get(2).get("name").asText());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testQueryJsonlWithPost() throws Exception {
        var query = "SELECT * FROM jsonl_test ORDER BY id";
        var body = objectMapper.writeValueAsBytes(new QueryRequest(query));
        var request = authenticatedRequestBuilder(URI.create(baseUrl + "/v1/query"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.APPLICATION_JSONL)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        List<JsonNode> rows = parseJsonl(response.body());
        assertEquals(3, rows.size());
        assertEquals("Bob", rows.get(1).get("name").asText());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testQueryJsonlResponseContentType() throws Exception {
        var request = authenticatedRequestBuilder(uriForQuery("SELECT 1 AS id"))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.APPLICATION_JSONL)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        var contentType = response.headers().firstValue("Content-Type").orElse("");
        assertTrue(contentType.contains("application/jsonl"),
                "Expected JSONL Content-Type but got: " + contentType);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testNdjsonAcceptAlias() throws Exception {
        var request = authenticatedRequestBuilder(uriForQuery("SELECT 42 AS answer"))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.APPLICATION_X_NDJSON)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        List<JsonNode> rows = parseJsonl(response.body());
        assertEquals(1, rows.size());
        assertEquals(42, rows.get(0).get("answer").asInt());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testEachLineIsValidStandaloneJson() throws Exception {
        var query = "SELECT * FROM jsonl_test ORDER BY id";
        var request = authenticatedRequestBuilder(uriForQuery(query))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.APPLICATION_JSONL)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        // Body must NOT be a JSON array — no enclosing brackets.
        assertFalse(response.body().trim().startsWith("["), "JSONL must not be a JSON array");
        for (String line : response.body().split("\n")) {
            if (line.isBlank()) continue;
            var node = objectMapper.readTree(line); // throws if not valid JSON
            assertTrue(node.isObject(), "Each line must be a JSON object: " + line);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testEmptyResultProducesEmptyBody() throws Exception {
        var request = authenticatedRequestBuilder(uriForQuery("SELECT * FROM jsonl_test WHERE id = -999"))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.APPLICATION_JSONL)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        assertEquals("", response.body().trim());
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    public void testQueryMillionRowsSingleColumn() throws Exception {
        var query = "SELECT generate_series FROM generate_series(1, 1000000)";
        var request = authenticatedRequestBuilder(uriForQuery(query))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.APPLICATION_JSONL)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        List<JsonNode> rows = parseJsonl(response.body());
        assertEquals(1000000, rows.size());
        assertEquals(1, rows.get(0).get("generate_series").asInt());
        assertEquals(1000000, rows.get(999999).get("generate_series").asInt());
    }

    // ==================== TYPE FIDELITY TESTS ====================

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testNumericTypesAreJsonNumbers() throws Exception {
        var sql = "SELECT " +
                "CAST(1 AS TINYINT) AS ti, " +
                "CAST(2 AS SMALLINT) AS si, " +
                "CAST(3 AS INTEGER) AS i, " +
                "CAST(4 AS BIGINT) AS bi, " +
                "CAST(1.5 AS FLOAT) AS f, " +
                "CAST(2.5 AS DOUBLE) AS d, " +
                "true AS b";
        var request = authenticatedRequestBuilder(uriForQuery(sql))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.APPLICATION_JSONL)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        JsonNode row = parseJsonl(response.body()).get(0);
        assertTrue(row.get("ti").isNumber());
        assertEquals(1, row.get("ti").asInt());
        assertEquals(4, row.get("bi").asLong());
        assertEquals(1.5, row.get("f").asDouble(), 1e-6);
        assertEquals(2.5, row.get("d").asDouble(), 1e-9);
        assertTrue(row.get("b").isBoolean());
        assertTrue(row.get("b").asBoolean());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testNullValuesAreJsonNull() throws Exception {
        var sql = "SELECT NULL::INTEGER AS i, NULL::VARCHAR AS s, NULL::DATE AS d";
        var request = authenticatedRequestBuilder(uriForQuery(sql))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.APPLICATION_JSONL)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        JsonNode row = parseJsonl(response.body()).get(0);
        assertTrue(row.get("i").isNull());
        assertTrue(row.get("s").isNull());
        assertTrue(row.get("d").isNull());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testTemporalTypesAreIsoStrings() throws Exception {
        var sql = "SELECT CAST('2026-03-12' AS DATE) AS d, " +
                "CAST('2026-03-12 10:30:00' AS TIMESTAMP) AS ts";
        var request = authenticatedRequestBuilder(uriForQuery(sql))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.APPLICATION_JSONL)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        JsonNode row = parseJsonl(response.body()).get(0);
        assertEquals("2026-03-12", row.get("d").asText());
        assertTrue(row.get("ts").asText().startsWith("2026-03-12"), "TIMESTAMP: " + row.get("ts").asText());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testNestedTypesAreRealJson() throws Exception {
        var sql = "SELECT [1, 2, 3] AS arr, {'name': 'Alice', 'age': 30} AS s";
        var request = authenticatedRequestBuilder(uriForQuery(sql))
                .GET()
                .header(HeaderValues.ACCEPT_JSON.name(), ContentTypes.APPLICATION_JSONL)
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        JsonNode row = parseJsonl(response.body()).get(0);
        assertTrue(row.get("arr").isArray(), "List should be a JSON array");
        assertEquals(3, row.get("arr").size());
        assertEquals(2, row.get("arr").get(1).asInt());
        assertTrue(row.get("s").isObject(), "Struct should be a JSON object");
        assertEquals("Alice", row.get("s").get("name").asText());
        assertEquals(30, row.get("s").get("age").asInt());
    }

    // ==================== FORMAT FALLBACK ====================

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testNoAcceptHeaderStillReturnsArrow() throws Exception {
        var request = authenticatedRequestBuilder(uriForQuery("SELECT 1 AS id"))
                .GET()
                .build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        assertFalse(response.headers().firstValue("Content-Type").orElse("").contains("application/jsonl"),
                "No Accept header should not return JSONL Content-Type");
    }

    // ==================== HELPERS ====================

    private List<JsonNode> parseJsonl(String body) throws Exception {
        List<JsonNode> rows = new ArrayList<>();
        for (String line : body.split("\n")) {
            if (line.isBlank()) continue;
            rows.add(objectMapper.readTree(line));
        }
        return rows;
    }

    private URI uriForQuery(String sql) {
        return URI.create(baseUrl + "/v1/query?q=" + URLEncoder.encode(sql, StandardCharsets.UTF_8));
    }
}

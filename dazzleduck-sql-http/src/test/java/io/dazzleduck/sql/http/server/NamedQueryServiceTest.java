package io.dazzleduck.sql.http.server;

import io.dazzleduck.sql.common.ParameterValidationException;
import io.dazzleduck.sql.common.NamedQueryParameterValidator;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.flight.namedquery.NamedQueryRequest;
import io.dazzleduck.sql.commons.util.TestUtils;
import io.dazzleduck.sql.http.server.model.ContentTypes;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for {@link NamedQueryService}.
 *
 * <p>The server is started with {@code named_query_table=named_queries}. Each test that
 * needs a template inserts a row into that table before making the HTTP request.
 */
public class NamedQueryServiceTest extends HttpServerTestBase {

    private static final String NAMED_QUERIES_TABLE = "named_queries";
    private static final String ENDPOINT = "/v1/named-query";

    @BeforeAll
    static void setup() throws Exception {
        initWarehouse();
        initClient();
        initPort();
        startServer(
                "--conf", "dazzleduck_server.named_query_table=" + NAMED_QUERIES_TABLE
        );
        installArrowExtension();

        // Create the named-query table and seed it with templates used by the tests
        ConnectionPool.execute(
                "CREATE TABLE IF NOT EXISTS " + NAMED_QUERIES_TABLE +
                " (id BIGINT PRIMARY KEY, name VARCHAR UNIQUE, template VARCHAR, validators VARCHAR[]," +
                "  description VARCHAR, parameter_descriptions MAP(VARCHAR, VARCHAR), preferred_display VARCHAR, query_group VARCHAR DEFAULT 'general')");
        ConnectionPool.executeBatch(new String[]{
                "DELETE FROM " + NAMED_QUERIES_TABLE,
                "INSERT INTO " + NAMED_QUERIES_TABLE + " VALUES " +
                "(1, 'get_series', 'SELECT * FROM generate_series({{ limit }}) t(v) ORDER BY v'," +
                " NULL, 'Returns the first N integers', MAP {'limit': 'upper bound (exclusive)'}, NULL, 'general')",
                "INSERT INTO " + NAMED_QUERIES_TABLE + " VALUES " +
                "(2, 'filter_series', 'SELECT * FROM generate_series(10) t(v) WHERE v > {{ min }}'," +
                " NULL, 'Filters integers above a threshold', MAP {'min': 'minimum value (exclusive)'}, NULL, 'general')",
                "INSERT INTO " + NAMED_QUERIES_TABLE + " VALUES " +
                "(3, 'validated_query', 'SELECT * FROM generate_series({{ limit }}) t(v)'," +
                " ['" + RejectAllValidatorNamedQuery.class.getName() + "'], 'Always rejected by validator', NULL, NULL, 'general')",
                "INSERT INTO " + NAMED_QUERIES_TABLE + " VALUES " +
                "(4, 'multi_validator_query', 'SELECT * FROM generate_series({{ limit }}) t(v)'," +
                " ['" + RejectAllValidatorNamedQuery.class.getName() + "', '" + AnotherRejectValidatorNamedQuery.class.getName() + "']," +
                " 'Rejected by two validators', NULL, NULL, 'general')",
                "INSERT INTO " + NAMED_QUERIES_TABLE + " VALUES " +
                "(5, 'invalid_sql', 'SELECT * FROM non_existent_table'," +
                " NULL, 'Query with invalid SQL', NULL, NULL, 'general')",
                "INSERT INTO " + NAMED_QUERIES_TABLE + " VALUES " +
                "(6, 'marketing_query', 'SELECT * FROM generate_series({{ limit }}) t(v) ORDER BY v'," +
                " NULL, 'Marketing analytics query', MAP {'limit': 'upper bound (exclusive)'}, NULL, 'marketing')",
                "INSERT INTO " + NAMED_QUERIES_TABLE + " VALUES " +
                "(7, 'siem_query', 'SELECT * FROM generate_series({{ limit }}) t(v) ORDER BY v'," +
                " NULL, 'siem alert query', MAP {'limit': 'upper bound (exclusive)'}, NULL, 'siem')"
        });
    }

    @AfterAll
    static void cleanup() throws Exception {
        ConnectionPool.execute("DROP TABLE IF EXISTS " + NAMED_QUERIES_TABLE);
        cleanupWarehouse();
    }

    // -------------------------------------------------------------------------
    // Happy-path tests
    // -------------------------------------------------------------------------

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testHappyPath_templateRenderedAndExecuted() throws IOException, InterruptedException, SQLException {
        // Template: SELECT * FROM generate_series({{ limit }}) t(v) ORDER BY v
        var namedQuery = NamedQueryRequest.execute("get_series", Map.of("limit", "5"));
        var body = objectMapper.writeValueAsBytes(namedQuery);

        var request = authenticatedRequestBuilder(URI.create(baseUrl + ENDPOINT))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(200, response.statusCode(), "Expected 200 for valid named query");

        try (var allocator = new RootAllocator();
             var reader = new ArrowStreamReader(response.body(), allocator)) {
            String expected = "SELECT * FROM generate_series(5) t(v) ORDER BY v";
            TestUtils.isEqual(expected, allocator, reader);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testHappyPath_multipleParameters() throws IOException, InterruptedException, SQLException {
        // Template: SELECT * FROM generate_series(10) t(v) WHERE v > {{ min }}
        var namedQuery = NamedQueryRequest.execute("filter_series", Map.of("min", "7"));
        var body = objectMapper.writeValueAsBytes(namedQuery);

        var request = authenticatedRequestBuilder(URI.create(baseUrl + ENDPOINT))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        assertEquals(200, response.statusCode(), "Expected 200 for valid named query with parameters");

        try (var allocator = new RootAllocator();
             var reader = new ArrowStreamReader(response.body(), allocator)) {
            String expected = "SELECT * FROM generate_series(10) t(v) WHERE v > 7";
            TestUtils.isEqual(expected, allocator, reader);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testHappyPath_arrowResponseHasCorrectContentType() throws IOException, InterruptedException {
        var namedQuery = NamedQueryRequest.execute("get_series", Map.of("limit", "3"));
        var body = objectMapper.writeValueAsBytes(namedQuery);

        var request = authenticatedRequestBuilder(URI.create(baseUrl + ENDPOINT))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.discarding());
        assertEquals(200, response.statusCode());
        assertTrue(response.headers().firstValue("Content-Type")
                        .orElse("").contains(ContentTypes.APPLICATION_ARROW),
                "Arrow path must set Content-Type: application/vnd.apache.arrow.stream");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testHappyPath_tsvResponseBodyAndContentType() throws IOException, InterruptedException {
        var namedQuery = NamedQueryRequest.execute("get_series", Map.of("limit", "3"));
        var body = objectMapper.writeValueAsBytes(namedQuery);

        var request = authenticatedRequestBuilder(URI.create(baseUrl + ENDPOINT))
                .header("Accept", ContentTypes.TEXT_TSV)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        assertTrue(response.headers().firstValue("Content-Type")
                        .orElse("").contains(ContentTypes.TEXT_TSV),
                "TSV path must set Content-Type: text/tab-separated-values");

        String[] lines = response.body().strip().split("\n");
        assertTrue(lines.length >= 2, "Expected header row + at least one data row, got: " + response.body());
        assertEquals("v", lines[0].strip(), "Header row should be the column name");
        assertEquals("0", lines[1].strip(), "First data row should be 0 (generate_series is 0-based)");
        assertEquals(4, lines.length - 1, "generate_series(3) produces 4 rows: 0..3 inclusive");
    }

    // -------------------------------------------------------------------------
    // Error cases
    // -------------------------------------------------------------------------

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testUnknownNameReturns404() throws IOException, InterruptedException {
        var namedQuery = NamedQueryRequest.execute("no_such_query", Map.of());
        var body = objectMapper.writeValueAsBytes(namedQuery);

        var request = authenticatedRequestBuilder(URI.create(baseUrl + ENDPOINT))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(404, response.statusCode(), "Expected 404 when named query does not exist");
        assertNotNull(response.body());
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testMissingNameReturns400() throws IOException, InterruptedException {
        var namedQuery = new NamedQueryRequest(null, Map.of());
        var body = objectMapper.writeValueAsBytes(namedQuery);

        var request = authenticatedRequestBuilder(URI.create(baseUrl + ENDPOINT))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(400, response.statusCode(), "Expected 400 when name is missing");
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testBlankNameReturns400() throws IOException, InterruptedException {
        var namedQuery = new NamedQueryRequest("   ", Map.of());
        var body = objectMapper.writeValueAsBytes(namedQuery);

        var request = authenticatedRequestBuilder(URI.create(baseUrl + ENDPOINT))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(400, response.statusCode(), "Expected 400 when name is blank");
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testIncorrectQueryReturnsError() throws IOException, InterruptedException {
        var namedQuery = NamedQueryRequest.execute("invalid_sql", Map.of());
        var body = objectMapper.writeValueAsBytes(namedQuery);

        var request = authenticatedRequestBuilder(URI.create(baseUrl + ENDPOINT))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        // Invalid SQL typically results in Internal Server Error or Bad Request depending on mapping
        assertTrue(response.statusCode() >= 400, "Expected error status for invalid SQL");
    }

    // -------------------------------------------------------------------------
    // Validator test — row in DB has a validator class that rejects everything
    // -------------------------------------------------------------------------

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testValidatorBlocksRequest() throws IOException, InterruptedException {
        var namedQuery = NamedQueryRequest.execute("validated_query", Map.of("limit", "5"));
        var body = objectMapper.writeValueAsBytes(namedQuery);

        var request = authenticatedRequestBuilder(URI.create(baseUrl + ENDPOINT))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(400, response.statusCode(), "Validator should cause HTTP 400");
        assertTrue(response.body().contains("Rejected by test validator"),
                "Response body should contain the validator message");
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testAllValidatorsRunAndErrorsAreCombined() throws IOException, InterruptedException {
        var namedQuery = NamedQueryRequest.execute("multi_validator_query", Map.of("limit", "5"));
        var body = objectMapper.writeValueAsBytes(namedQuery);

        var request = authenticatedRequestBuilder(URI.create(baseUrl + ENDPOINT))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(400, response.statusCode(), "All validators should cause HTTP 400");
        assertTrue(response.body().contains("Rejected by test validator"),
                "Response should contain first validator message");
        assertTrue(response.body().contains("Rejected by another validator"),
                "Response should contain second validator message");
    }

    // -------------------------------------------------------------------------
    // List + single-query tests
    // -------------------------------------------------------------------------

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testListReturnsItems() throws IOException, InterruptedException {
        var request = authenticatedRequestBuilder(URI.create(baseUrl + ENDPOINT + "?offset=0&limit=10"))
                .GET()
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), "Expected 200 for list endpoint");

        List<Map<String, Object>> items = objectMapper.readValue(response.body(), new TypeReference<>() {});
        assertEquals(7, items.size(), "All 7 queries fit within limit=10");

        // Sorted by id
        assertEquals(1, items.get(0).get("id"));
        assertEquals("get_series",            items.get(0).get("name"));
        assertEquals(2, items.get(1).get("id"));
        assertEquals("filter_series",         items.get(1).get("name"));
        assertEquals(3, items.get(2).get("id"));
        assertEquals("validated_query",       items.get(2).get("name"));
        assertEquals(4, items.get(3).get("id"));
        assertEquals("multi_validator_query", items.get(3).get("name"));

        // Verify new fields are present in list responses
        assertTrue(items.get(0).containsKey("preferred_display"), "List items must include preferred_display");
        assertTrue(items.get(0).containsKey("query_group"), "List items must include query_group");
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testListPaginationOffsetById() throws IOException, InterruptedException {
        // offset=2 means id > 2
        var request = authenticatedRequestBuilder(URI.create(baseUrl + ENDPOINT + "?offset=2&limit=2"))
                .GET()
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());

        List<Map<String, Object>> items = objectMapper.readValue(response.body(), new TypeReference<>() {});
        assertEquals(2, items.size(), "Offset=2 (id > 2), limit=2 should return id=3 and id=4");
        assertEquals(3, ((Number) items.get(0).get("id")).intValue());
        assertEquals("validated_query",       items.get(0).get("name"));
        assertEquals(4, ((Number) items.get(1).get("id")).intValue());
        assertEquals("multi_validator_query", items.get(1).get("name"));
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testListWithLargeOffsetReturnsEmpty() throws IOException, InterruptedException {
        // offset=100 means id > 100
        var request = authenticatedRequestBuilder(URI.create(baseUrl + ENDPOINT + "?offset=100&limit=10"))
                .GET()
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());

        List<Map<String, Object>> items = objectMapper.readValue(response.body(), new TypeReference<>() {});
        assertTrue(items.isEmpty(), "Expected empty list for offset larger than max id");
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testGetByNameReturnsFullObject() throws IOException, InterruptedException {
        var request = authenticatedRequestBuilder(URI.create(baseUrl + ENDPOINT + "/get_series"))
                .GET()
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), "Expected 200 for named query by name");

        Map<String, Object> info = objectMapper.readValue(response.body(), new TypeReference<>() {});
        assertEquals(1, ((Number) info.get("id")).intValue());
        assertEquals("get_series",           info.get("name"));
        assertEquals("Returns the first N integers", info.get("description"));
        assertTrue(info.containsKey("parameterDescriptions"), "Full object must include parameterDescriptions");
        assertTrue(info.containsKey("validatorDescriptions"), "Full object must include validatorDescriptions");
        assertTrue(info.containsKey("template"), "Full object must include template");
        assertTrue(info.containsKey("preferred_display"), "Full object must include preferred_display");
        assertTrue(info.containsKey("query_group"), "Full object must include query_group");
        assertEquals("general", info.get("query_group"), "Default query_group should be 'general'");
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testGetByNameWithValidatorsReturnsDescriptions() throws IOException, InterruptedException {
        var request = authenticatedRequestBuilder(URI.create(baseUrl + ENDPOINT + "/validated_query"))
                .GET()
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());

        Map<String, Object> info = objectMapper.readValue(response.body(), new TypeReference<>() {});
        @SuppressWarnings("unchecked")
        List<String> validatorClassNames = (List<String>) info.get("validatorDescriptions");
        assertNotNull(validatorClassNames);
        assertEquals(1, validatorClassNames.size());
        assertEquals(RejectAllValidatorNamedQuery.class.getName(), validatorClassNames.get(0));
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testGetByNameUnknownReturns404() throws IOException, InterruptedException {
        var request = authenticatedRequestBuilder(URI.create(baseUrl + ENDPOINT + "/no_such_query"))
                .GET()
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(404, response.statusCode());
    }

    // -------------------------------------------------------------------------
    // Test validator implementations — must be public with a no-arg constructor
    // -------------------------------------------------------------------------

    /** A validator that always rejects — used only in tests. */
    public static class RejectAllValidatorNamedQuery implements NamedQueryParameterValidator {
        @Override
        public void validate(Map<String, String> parameters) throws ParameterValidationException {
            throw new ParameterValidationException("Rejected by test validator");
        }
    }

    /** A second validator that always rejects — used to verify multi-validator error collection. */
    public static class AnotherRejectValidatorNamedQuery implements NamedQueryParameterValidator {
        @Override
        public void validate(Map<String, String> parameters) throws ParameterValidationException {
            throw new ParameterValidationException("Rejected by another validator");
        }
    }
}

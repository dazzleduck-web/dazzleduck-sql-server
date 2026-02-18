package io.dazzleduck.sql.http.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.sql.common.ConfigConstants;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.common.auth.LoginRequest;
import io.dazzleduck.sql.common.auth.LoginResponse;
import io.helidon.http.HeaderValues;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class HttpServerTestBase {
    protected static HttpClient client;
    protected static ObjectMapper objectMapper = new ObjectMapper();
    protected static String warehousePath;

    protected static String ingestionPath;
    protected static int serverPort;
    protected static String baseUrl;

    protected static final String BASE_URL_TEMPLATE = "http://localhost:%s";

    protected static final String LONG_RUNNING_QUERY = "with t as " +
            "(select len(split(concat('abcdefghijklmnopqrstuvwxyz:', generate_series), ':')) as len  from generate_series(1, 1000000000) )" +
            " select count(*) from t where len = 10";

    protected static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    protected static void initWarehouse() {
        warehousePath = "/tmp/" + UUID.randomUUID();
        ingestionPath = warehousePath + "ingestion";
        new File(warehousePath).mkdir();
        new File(ingestionPath).mkdir();
    }

    protected static void initClient() {
        client = HttpClient.newHttpClient();
    }

    protected static void initPort() throws IOException {
        serverPort = findFreePort();
        baseUrl = BASE_URL_TEMPLATE.formatted(serverPort);
    }

    protected static void startServer(String... extraArgs) throws Exception {
        String[] baseArgs = {"--conf", "dazzleduck_server.http.port=%s".formatted(serverPort),
                "--conf", "dazzleduck_server.%s=%s".formatted(ConfigConstants.WAREHOUSE_CONFIG_KEY, warehousePath),
                "--conf", "dazzleduck_server.ingestion_task_factory_provider.class=io.dazzleduck.sql.commons.ingestion.NOOPIngestionTaskFactoryProvider",
                "--conf", "dazzleduck_server.ingestion_task_factory_provider.ingestion_path=%s".formatted(ingestionPath),
                "--conf", "dazzleduck_server.ingestion.max_delay_ms=500"};
        String[] args = new String[baseArgs.length + extraArgs.length];
        System.arraycopy(baseArgs, 0, args, 0, baseArgs.length);
        System.arraycopy(extraArgs, 0, args, baseArgs.length, extraArgs.length);
        Main.main(args);
    }

    protected static void installArrowExtension() {
        String[] sqls = {"INSTALL arrow FROM community", "LOAD arrow"};
        ConnectionPool.executeBatch(sqls);
    }

    protected static void cleanupWarehouse() throws IOException {
        if (warehousePath != null) {
            deleteDirectory(new File(warehousePath));
        }
    }

    protected static void deleteDirectory(File directory) throws IOException {
        if (directory == null || !directory.exists()) {
            return;
        }

        if (directory.isDirectory()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    deleteDirectory(file);
                }
            }
        }

        if (!directory.delete()) {
            throw new IOException("Failed to delete: " + directory.getAbsolutePath());
        }
    }

    protected LoginResponse login() throws IOException, InterruptedException {
        return login(serverPort, new LoginRequest("admin", "admin"));
    }

    protected LoginResponse login(int port) throws IOException, InterruptedException {
        return login(port, new LoginRequest("admin", "admin"));
    }

    protected LoginResponse login(int port, LoginRequest loginRequestPayload) throws IOException, InterruptedException {
        var loginRequest = HttpRequest.newBuilder(URI.create(BASE_URL_TEMPLATE.formatted(port) + "/v1/login"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(objectMapper.writeValueAsBytes(loginRequestPayload)))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values())
                .build();
        var jwtResponse = client.send(loginRequest, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, jwtResponse.statusCode());
        return objectMapper.readValue(jwtResponse.body(), LoginResponse.class);
    }

    protected LoginResponse login(LoginRequest loginRequestPayload) throws IOException, InterruptedException {
        return login(serverPort, loginRequestPayload);
    }

    protected String getJWTToken() throws IOException, InterruptedException {
        var request = HttpRequest.newBuilder(URI.create(baseUrl + "/v1/login"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(objectMapper.writeValueAsBytes(new LoginRequest("admin", "admin", Map.of("org", "123")))))
                .header(HeaderValues.ACCEPT_JSON.name(), HeaderValues.ACCEPT_JSON.values()).build();
        var inputStreamResponse = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, inputStreamResponse.statusCode());
        var object = objectMapper.readValue(inputStreamResponse.body(), LoginResponse.class);
        return object.accessToken();
    }

    protected String urlEncode(String s) {
        return URLEncoder.encode(s, Charset.defaultCharset());
    }

    /**
     * Create an HttpRequest.Builder with JWT authentication header.
     * @param uri the request URI
     * @return HttpRequest.Builder with Authorization header
     * @throws IOException if login fails
     * @throws InterruptedException if login is interrupted
     */
    protected HttpRequest.Builder authenticatedRequestBuilder(URI uri) throws IOException, InterruptedException {
        String token = getJWTToken();
        return HttpRequest.newBuilder(uri)
                .header("Authorization", "Bearer " + token);
    }
}

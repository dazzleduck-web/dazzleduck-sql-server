package io.dazzleduck.sql.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dazzleduck.sql.login.LoginRequest;
import io.dazzleduck.sql.login.LoginResponse;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class HttpProducer extends FlightProducer.AbstractFlightProducer {

    private static final Logger logger = LoggerFactory.getLogger(HttpProducer.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Duration REFRESH_SKEW = Duration.ofSeconds(60);
    private static final Duration DEFAULT_TOKEN_LIFETIME = Duration.ofHours(5);
    private static final int MAX_AUTH_RETRIES = 2;

    private final HttpClient client;
    private final ExecutorService executorService;
    private final String baseUrl;
    private final String username;
    private final String password;
    private final String targetPath;
    private final Duration httpClientTimeout;
    private final long maxMem;
    private final long maxDisk;

    private volatile String jwt = null;
    private volatile Instant jwtExpiry = Instant.EPOCH;

    public HttpProducer(
            Schema schema,
            String baseUrl,
            String username,
            String password,
            String targetPath,
            Duration httpClientTimeout,
            long minBatchSize,
            long maxBatchSize,
            Duration maxSendInterval,
            int retryCount,
            long retryIntervalMillis,
            java.util.List<String> transformations,
            java.util.List<String> partitionBy,
            long maxInMemorySize,
            long maxOnDiskSize
    ) {
        this(schema, baseUrl, username, password, targetPath, httpClientTimeout, minBatchSize, maxBatchSize, maxSendInterval, retryCount, retryIntervalMillis, transformations, partitionBy, maxInMemorySize, maxOnDiskSize, Clock.systemUTC());
    }
    public HttpProducer(
            Schema schema,
            String baseUrl,
            String username,
            String password,
            String targetPath,
            Duration httpClientTimeout,
            long minBatchSize,
            long maxBatchSize,
            Duration maxSendInterval,
            int retryCount,
            long retryIntervalMillis,
            java.util.List<String> transformations,
            java.util.List<String> partitionBy,
            long maxInMemorySize,
            long maxOnDiskSize,
            Clock clock
    ) {
        super(minBatchSize, maxBatchSize, maxSendInterval, schema, clock, retryCount, retryIntervalMillis, transformations, partitionBy);

        // Issue #3: Parameter validation
        Objects.requireNonNull(baseUrl, "baseUrl must not be null");
        Objects.requireNonNull(username, "username must not be null");
        Objects.requireNonNull(password, "password must not be null");
        Objects.requireNonNull(targetPath, "targetPath must not be null");
        Objects.requireNonNull(httpClientTimeout, "httpClientTimeout must not be null");

        if (baseUrl.trim().isEmpty()) {
            throw new IllegalArgumentException("baseUrl must not be empty");
        }
        if (username.trim().isEmpty()) {
            throw new IllegalArgumentException("username must not be empty");
        }
        if (targetPath.trim().isEmpty()) {
            throw new IllegalArgumentException("targetPath must not be empty");
        }
        if (httpClientTimeout.isNegative() || httpClientTimeout.isZero()) {
            throw new IllegalArgumentException("httpClientTimeout must be positive");
        }

        this.baseUrl = baseUrl;
        this.username = username;
        this.password = password;
        this.targetPath = targetPath;
        this.httpClientTimeout = httpClientTimeout;
        this.maxMem = maxInMemorySize;
        this.maxDisk = maxOnDiskSize;

        // Issue #1: Configure HttpClient with proper settings
        this.executorService = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "http-sender-client");
            t.setDaemon(true);
            return t;
        });

        this.client = HttpClient.newBuilder()
                .executor(executorService)
                .connectTimeout(httpClientTimeout)
                .build();

        logger.info("Initializing HttpSender with baseUrl={}, targetPath={}, httpClientTimeout={}",
                    baseUrl, targetPath, httpClientTimeout);
    }

    @Override
    public long getMaxInMemorySize() {
        return maxMem;
    }

    @Override
    public long getMaxOnDiskSize() {
        return maxDisk;
    }

    // Issue #6: Improved error handling with specific exceptions
    private void login() throws IOException, InterruptedException {
        logger.debug("Attempting login to {}", baseUrl);

        final byte[] body;
        try {
            body = mapper.writeValueAsBytes(new LoginRequest(username, password));
        } catch (IOException e) {
            logger.error("Failed to serialize login request", e);
            throw new IOException("Failed to serialize login request", e);
        }

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/login"))
                .timeout(httpClientTimeout)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .build();

        HttpResponse<String> resp;
        try {
            resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        } catch (HttpTimeoutException e) {
            logger.error("Login request timed out after {}", httpClientTimeout, e);
            throw new IOException("Login request timed out", e);
        } catch (IOException e) {
            logger.error("Network error during login to {}", baseUrl, e);
            throw new IOException("Network error during login", e);
        }

        if (resp.statusCode() == 401 || resp.statusCode() == 403) {
            logger.error("Authentication failed with status {}: invalid credentials", resp.statusCode());
            throw new SecurityException("Invalid username or password");
        }

        if (resp.statusCode() != 200) {
            logger.error("Login failed with status {}: {}", resp.statusCode(), resp.body());
            throw new IOException("Login failed with status " + resp.statusCode() + ": " + resp.body());
        }

        try {
            JsonNode json = mapper.readTree(resp.body());
            LoginResponse lr = mapper.treeToValue(json, LoginResponse.class);

            this.jwt = lr.tokenType() + " " + lr.accessToken();

            long expiresIn = json.hasNonNull("expiresIn")
                    ? json.get("expiresIn").asLong()
                    : DEFAULT_TOKEN_LIFETIME.toSeconds();

            this.jwtExpiry = clock.instant().plusSeconds(expiresIn);
            logger.info("Login successful, JWT expires in {} seconds", expiresIn);
        } catch (IOException e) {
            logger.error("Failed to parse login response", e);
            throw new IOException("Failed to parse login response", e);
        }
    }

    // Issue #8: Fixed race condition - synchronized access to jwt
    private String getJwt() throws IOException, InterruptedException {
        // Check outside synchronized block first (performance optimization)
        String currentJwt = jwt;
        Instant currentExpiry = jwtExpiry;

        if (currentJwt == null || clock.instant().isAfter(currentExpiry.minus(REFRESH_SKEW))) {
            synchronized (this) {
                // Re-check inside synchronized block (double-checked locking)
                currentJwt = jwt;
                currentExpiry = jwtExpiry;

                if (currentJwt == null || clock.instant().isAfter(currentExpiry.minus(REFRESH_SKEW))) {
                    logger.debug("JWT refresh needed");
                    login();
                    currentJwt = jwt; // Get the newly refreshed token
                }
            }
        }
        return currentJwt;
    }

    @Override
    protected void doSend(ProducerElement element) throws InterruptedException {
        // Issue #4: Check if thread was interrupted before attempting send
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Thread interrupted before send");
        }

        HttpResponse<String> resp = null;
        int authRetries = 0;

        // Read the element bytes for HTTP sending
        final byte[] payload;
        try (java.io.InputStream in = element.read();
             java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream()) {
            in.transferTo(out);
            payload = out.toByteArray();
        } catch (IOException e) {
            logger.error("Failed to read element data", e);
            throw new RuntimeException("Failed to read element data", e);
        }

        // Issue #7: Specific exception handling
        try {
            while (authRetries <= MAX_AUTH_RETRIES) {
                resp = post(payload);

                if (resp.statusCode() == 401 || resp.statusCode() == 403) {
                    if (authRetries >= MAX_AUTH_RETRIES) {
                        logger.error("Max auth retries ({}) exceeded for {}{}", MAX_AUTH_RETRIES, baseUrl, targetPath);
                        break;
                    }
                    logger.warn("Received auth failure ({}) on attempt {}, invalidating JWT and retrying",
                            resp.statusCode(), authRetries + 1);
                    synchronized (this) {
                        jwt = null;
                        jwtExpiry = Instant.EPOCH;
                    }
                    authRetries++;
                } else {
                    // Non-auth response, break the retry loop
                    break;
                }
            }

            if (resp.statusCode() != 200) {
                logger.error("Ingestion failed with status {} to {}{}", resp.statusCode(), baseUrl, targetPath);
                throw new RuntimeException("Ingestion failed with status " + resp.statusCode() + ": " + resp.body());
            }

            logger.debug("Successfully sent element to {}{}", baseUrl, targetPath);

        } catch (HttpTimeoutException e) {
            logger.error("HTTP request timed out after {} to {}{}", httpClientTimeout, baseUrl, targetPath, e);
            throw new RuntimeException("HTTP request timed out to " + baseUrl + targetPath, e);
        } catch (IOException e) {
            // Check if interrupted during IO
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Thread interrupted during HTTP send");
            }
            logger.error("Network error sending data to {}{}", baseUrl, targetPath, e);
            throw new RuntimeException("Network error sending data to " + baseUrl + targetPath, e);
        } catch (SecurityException e) {
            logger.error("Authentication failed for {}{}", baseUrl, targetPath, e);
            throw new RuntimeException("Authentication failed for " + baseUrl + targetPath, e);
        }
    }

    private HttpResponse<String> post(byte[] payload) throws IOException, InterruptedException {
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/ingest?path=" + targetPath))
                .timeout(httpClientTimeout)
                .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
                .header("Authorization", getJwt())
                .header("Content-Type", "application/vnd.apache.arrow.stream");

        // Add transformations and partitionBy headers if present (URL encoded)
        if (!getTransformations().isEmpty()) {
            String transformationsValue = String.join(",", getTransformations());
            requestBuilder.header(io.dazzleduck.sql.common.Headers.HEADER_DATA_TRANSFORMATION,
                java.net.URLEncoder.encode(transformationsValue, java.nio.charset.StandardCharsets.UTF_8));
        }
        if (!getPartitionBy().isEmpty()) {
            String partitionByValue = String.join(",", getPartitionBy());
            requestBuilder.header(io.dazzleduck.sql.common.Headers.HEADER_DATA_PARTITION,
                java.net.URLEncoder.encode(partitionByValue, java.nio.charset.StandardCharsets.UTF_8));
        }

        HttpRequest req = requestBuilder.build();
        return client.send(req, HttpResponse.BodyHandlers.ofString());
    }

    // Issue #1: Override close() to cleanup HttpClient resources
    @Override
    public void close() {
        Exception superCloseException = null;

        // Close parent resources first
        try {
            super.close();
        } catch (Exception e) {
            superCloseException = e;
            logger.error("Error closing FlightSender resources", e);
        }

        // Shutdown the executor service
        try {
            executorService.shutdown();
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.warn("ExecutorService did not terminate gracefully, forcing shutdown");
                executorService.shutdownNow();
            }
            logger.debug("HttpClient executor service closed successfully");
        } catch (InterruptedException e) {
            logger.warn("Interrupted while waiting for executor service shutdown", e);
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.error("Failed to shutdown executor service", e);
            if (superCloseException != null) {
                superCloseException.addSuppressed(e);
            }
        }

        // Rethrow the first exception if any occurred
        if (superCloseException != null) {
            throw (RuntimeException) superCloseException;
        }
    }
}
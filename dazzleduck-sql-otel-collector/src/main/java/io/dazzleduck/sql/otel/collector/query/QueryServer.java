package io.dazzleduck.sql.otel.collector.query;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.dazzleduck.sql.common.ContentTypes;
import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.io.ResultStreams;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A local SQL endpoint on the collector's own DuckDB instance, for testing: {@code GET
 * /v1/query?q=<sql>} or {@code POST /v1/query} with {@code {"query": "..."}}, answering in the same
 * formats as the main server's {@code /v1/query}, chosen by {@code Accept}: TSV
 * ({@code text/tab-separated-values}), JSON Lines ({@code application/jsonl} or
 * {@code application/x-ndjson}), otherwise an Arrow IPC stream (ZSTD unless
 * {@code x-dd-arrow-compression: none}). Results stream as DuckDB produces them.
 *
 * <p>Only a single SELECT is accepted (checked with DuckDB's parser; multiple statements, DDL, DML,
 * COPY, SET, ATTACH, CALL and EXPLAIN get 400). It runs on its own {@link ConnectionPool}
 * connection inside {@code BEGIN TRANSACTION READ ONLY}, so it sees the catalogs ingestion writes
 * to and cannot write to their tables. That is a guard against mistakes, not a security boundary:
 * a SELECT can still call a table function with side effects, such as DuckLake's maintenance
 * functions. There is no authentication,
 * which is why it binds to localhost by default.
 *
 * <p>{@code query.timeout} bounds query execution, through the driver's
 * {@link Statement#setQueryTimeout}; reading a streamed result is not time-limited, and ends when the
 * client disconnects.
 *
 * <p>Requests a browser makes on a web page's behalf are refused (403): any request with an
 * {@code Origin} header, or with {@code Sec-Fetch-Site} other than {@code none} (a URL typed by the
 * user). Otherwise any page open in the developer's browser could run SQL here, e.g. through an
 * {@code <img>} pointing at {@code /v1/query?q=...}. While bound to a loopback address, the
 * {@code Host} header must also name a loopback host, which stops DNS rebinding. Command-line
 * clients such as curl send none of these headers and are unaffected.
 */
public class QueryServer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(QueryServer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private enum Format { ARROW, TSV, JSONL }

    private final QuerySettings settings;
    private final MeterRegistry registry;
    private final HttpServer server;
    private final ExecutorService requests;
    private final BufferAllocator allocator = new RootAllocator();
    // Statements executing right now, so close() can cancel them.
    private final Set<Statement> running = ConcurrentHashMap.newKeySet();
    private volatile boolean closing;

    public QueryServer(QuerySettings settings, MeterRegistry registry) throws IOException {
        this.settings = settings;
        this.registry = registry;
        this.server = HttpServer.create(new InetSocketAddress(settings.host(), settings.port()), 0);
        this.requests = Executors.newFixedThreadPool(settings.threads(), daemon("otel-query"));
        server.createContext("/v1/query", this::handle);
        server.setExecutor(requests);
    }

    private static java.util.concurrent.ThreadFactory daemon(String name) {
        return r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        };
    }

    public void start() {
        server.start();
        log.info("Query endpoint listening on http://{}:{}/v1/query (read-only, no authentication)",
                settings.host(), getPort());
    }

    public int getPort() {
        return server.getAddress().getPort();
    }

    private void handle(HttpExchange exchange) throws IOException {
        Timer.Sample sample = Timer.start(registry);
        String format = "none";
        String outcome = "error";
        boolean aborted = false;
        try {
            if (closing) {
                outcome = "unavailable";
                sendText(exchange, 503, "Shutting down");
                return;
            }
            String rejection = browserRequestRejection(exchange);
            if (rejection != null) {
                outcome = "forbidden";
                sendText(exchange, 403, rejection);
                return;
            }
            String sql = readQuery(exchange);
            if (sql == null) {
                outcome = "bad_request";
                return;
            }
            Format chosen = format(exchange);
            format = chosen.name().toLowerCase();
            CompressionUtil.CodecType codec = CompressionUtil.CodecType.NO_COMPRESSION;
            if (chosen == Format.ARROW) {
                codec = compression(exchange);
                if (codec == null) {
                    outcome = "bad_request";
                    return;
                }
            }
            String notSingleSelect = notASingleSelect(sql);
            if (notSingleSelect != null) {
                outcome = "bad_request";
                sendText(exchange, 400, notSingleSelect);
                return;
            }
            outcome = run(exchange, sql, chosen, codec);
        } catch (StreamAborted e) {
            // Rethrown without closing the exchange: the JDK server then drops the connection instead
            // of ending the chunked body normally, so the client cannot mistake a partial result for
            // a complete one.
            aborted = true;
            outcome = e.outcome;
            throw e;
        } catch (Exception e) {
            log.error("Query request failed", e);
            sendText(exchange, 500, "Internal error: " + e.getMessage());
        } finally {
            if (!aborted) {
                exchange.close();
            }
            sample.stop(Timer.builder("dazzleduck.otel.query.duration")
                    .description("Local query endpoint requests")
                    .tag("format", format)
                    .tag("outcome", outcome)
                    .register(registry));
        }
    }

    /**
     * Why {@code sql} is not exactly one SELECT, or null when it is. DuckDB's JDBC driver runs every
     * statement in a string (even through prepareStatement), so without this {@code COMMIT; DROP
     * TABLE ...} would end the read-only transaction and write. DuckDB's own parser decides:
     * {@code json_serialize_sql} accepts SELECT, WITH, FROM-first, DESCRIBE, SHOW and SUMMARIZE,
     * and reports an error for anything else (COPY, SET, ATTACH, CALL, EXPLAIN, DDL, DML).
     */
    private static String notASingleSelect(String sql) throws SQLException {
        String serialized;
        try (DuckDBConnection connection = ConnectionPool.getConnection();
             var statement = connection.prepareStatement("SELECT json_serialize_sql(?::VARCHAR)")) {
            statement.setString(1, sql);
            try (var rs = statement.executeQuery()) {
                rs.next();
                serialized = rs.getString(1);
            }
        }
        try {
            JsonNode tree = MAPPER.readTree(serialized);
            if (tree.path("error").asBoolean(false)) {
                return "Only a single SELECT statement is accepted: " + tree.path("error_message").asText();
            }
            int count = tree.path("statements").size();
            if (count != 1) {
                return "Only a single SELECT statement is accepted, got " + count;
            }
            return null;
        } catch (IOException e) {
            return "Could not parse the query: " + e.getMessage();
        }
    }

    /** Why a request looks browser-initiated (see the class Javadoc), or null when it is acceptable. */
    private String browserRequestRejection(HttpExchange exchange) {
        var headers = exchange.getRequestHeaders();
        if (headers.containsKey("Origin")) {
            return "Cross-origin browser requests are not accepted";
        }
        String site = headers.getFirst("Sec-Fetch-Site");
        if (site != null && !site.equalsIgnoreCase("none")) {
            return "Browser requests from a web page are not accepted";
        }
        if (isLoopback(settings.host())) {
            String host = headers.getFirst("Host");
            if (host == null || !isLoopback(hostName(host))) {
                return "Host header must name a loopback host";
            }
        }
        return null;
    }

    /** The host part of a {@code Host} header value: without the port, and without IPv6 brackets. */
    private static String hostName(String hostHeader) {
        String value = hostHeader.trim();
        if (value.startsWith("[")) {
            int end = value.indexOf(']');
            return end > 0 ? value.substring(1, end) : value;
        }
        int colon = value.lastIndexOf(':');
        return colon >= 0 ? value.substring(0, colon) : value;
    }

    private static final java.util.regex.Pattern IPV4_LOOPBACK =
            java.util.regex.Pattern.compile("127\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}");

    /** Exactly localhost, ::1, or an IPv4 literal 127.x.x.x; not a name that merely starts with 127. */
    private static boolean isLoopback(String host) {
        return host.equalsIgnoreCase("localhost") || host.equals("::1") || IPV4_LOOPBACK.matcher(host).matches();
    }

    /** The SQL from {@code ?q=} (GET) or {@code {"query": ...}} (POST); null after sending an error. */
    private String readQuery(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String sql;
        if ("GET".equalsIgnoreCase(method)) {
            sql = queryParameter(exchange.getRequestURI().getRawQuery(), "q");
        } else if ("POST".equalsIgnoreCase(method)) {
            JsonNode body;
            try {
                body = MAPPER.readTree(exchange.getRequestBody());
            } catch (IOException e) {
                sendText(exchange, 400, "Invalid JSON body: " + e.getMessage());
                return null;
            }
            sql = body != null && body.hasNonNull("query") ? body.get("query").asText() : null;
        } else {
            exchange.getResponseHeaders().set("Allow", "GET, POST");
            sendText(exchange, 405, "Use GET ?q=<sql> or POST {\"query\": \"<sql>\"}");
            return null;
        }
        if (sql == null || sql.isBlank()) {
            sendText(exchange, 400, "Missing required parameter: q");
            return null;
        }
        return sql;
    }

    private static String queryParameter(String rawQuery, String name) {
        if (rawQuery == null) {
            return null;
        }
        for (String pair : rawQuery.split("&")) {
            int eq = pair.indexOf('=');
            String key = URLDecoder.decode(eq < 0 ? pair : pair.substring(0, eq), StandardCharsets.UTF_8);
            if (key.equals(name)) {
                return eq < 0 ? "" : URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
            }
        }
        return null;
    }

    private static Format format(HttpExchange exchange) {
        String accept = String.join(",", exchange.getRequestHeaders().getOrDefault("Accept", java.util.List.of()));
        if (accept.contains(ContentTypes.TEXT_TSV)) {
            return Format.TSV;
        }
        if (accept.contains(ContentTypes.APPLICATION_JSONL) || accept.contains(ContentTypes.APPLICATION_X_NDJSON)) {
            return Format.JSONL;
        }
        return Format.ARROW;
    }

    /** Same values as the main server's {@code /v1/query}; null after sending an error. */
    private static CompressionUtil.CodecType compression(HttpExchange exchange) throws IOException {
        String value = exchange.getRequestHeaders().getFirst(Headers.HEADER_ARROW_COMPRESSION);
        if (value == null) {
            return CompressionUtil.CodecType.ZSTD;
        }
        return switch (value.trim().toUpperCase()) {
            case "ZSTD", "ZSTANDARD" -> CompressionUtil.CodecType.ZSTD;
            case "NONE" -> CompressionUtil.CodecType.NO_COMPRESSION;
            default -> {
                sendText(exchange, 400, "Invalid Arrow compression codec: " + value + ". Supported values: zstd, zstandard, none");
                yield null;
            }
        };
    }

    /** Thrown once a streamed response has failed after its headers were sent; see {@link #handle}. */
    private static final class StreamAborted extends RuntimeException {
        final String outcome;

        StreamAborted(String outcome, Throwable cause) {
            super("Query result stream failed after the response started", cause);
            this.outcome = outcome;
        }
    }

    /**
     * Runs the query and streams the result; returns the outcome tag. Before the response headers
     * are sent, a failure becomes a 504 (timeout) or 500. After, the status cannot change, so the
     * connection is aborted instead ({@link StreamAborted}), and the client sees an incomplete
     * response rather than a short one that looks complete.
     *
     * <p>The statement leaves {@link #running} only after its response is sent and its resources
     * are closed, so {@link #close()} waiting for an empty set really waits for the answer.
     */
    private String run(HttpExchange exchange, String sql, Format format,
                       CompressionUtil.CodecType codec) throws IOException {
        boolean headersSent = false;
        Statement registered = null;
        long started = System.nanoTime();
        try {
            try (DuckDBConnection connection = ConnectionPool.getConnection();
                 Statement statement = connection.createStatement();
                 BufferAllocator requestAllocator = allocator.newChildAllocator("query", 0, Long.MAX_VALUE)) {
                registered = statement;
                running.add(statement);
                if (closing) {
                    // close() may have cancelled everything just before this registered; do not start.
                    sendText(exchange, 503, "Shutting down");
                    return "unavailable";
                }
                // The driver cancels execution after this; it does not bound reading a streamed
                // result, which ends when the client disconnects (the next write fails).
                statement.setQueryTimeout(timeoutSeconds());
                statement.execute("BEGIN TRANSACTION READ ONLY");
                if (!statement.execute(sql)) {
                    exchange.sendResponseHeaders(200, -1);
                    headersSent = true;
                    return "ok";
                }
                try (var resultSet = (DuckDBResultSet) statement.getResultSet();
                     ArrowReader reader = (ArrowReader) resultSet.arrowExportStream(requestAllocator, settings.arrowBatchSize())) {
                    exchange.getResponseHeaders().set("Content-Type", switch (format) {
                        case TSV -> ContentTypes.TEXT_TSV_UTF8;
                        case JSONL -> ContentTypes.APPLICATION_JSONL_UTF8;
                        case ARROW -> ContentTypes.APPLICATION_ARROW;
                    });
                    exchange.sendResponseHeaders(200, 0); // chunked: rows stream as they are produced
                    headersSent = true;
                    OutputStream body = exchange.getResponseBody();
                    // The writers close their stream even when they fail; closing the body writes
                    // the end-of-body chunk, so they get a stream whose close() only flushes.
                    OutputStream unclosable = new java.io.FilterOutputStream(body) {
                        @Override
                        public void write(byte[] b, int off, int len) throws IOException {
                            out.write(b, off, len);
                        }

                        @Override
                        public void close() throws IOException {
                            flush();
                        }
                    };
                    switch (format) {
                        case TSV -> ResultStreams.writeTsv(reader, unclosable);
                        case JSONL -> ResultStreams.writeJsonl(reader, unclosable);
                        case ARROW -> ResultStreams.writeArrow(reader, unclosable, codec, CommonsCompressionFactory.INSTANCE);
                    }
                    body.close(); // complete: now end the chunked body
                }
                return "ok";
            }
        } catch (SQLException | IOException | RuntimeException e) {
            // DuckDB reports a query timeout and a cancel from close() the same way; while not
            // closing, an interrupt can only be the timeout. DuckDB 2.0 reports a timeout of a
            // streamed result as "Invalid result set" instead, so a failure once the timeout has
            // passed counts as the timeout too.
            boolean timedOut = !closing && (isInterrupt(e)
                    || System.nanoTime() - started >= TimeUnit.SECONDS.toNanos(timeoutSeconds()));
            String outcome = timedOut ? "timeout" : "error";
            if (headersSent) {
                log.warn("Query result stream failed after the response started; aborting the connection", e);
                throw new StreamAborted(outcome, e);
            }
            if (timedOut) {
                sendText(exchange, 504, "Query timed out after " + settings.timeout());
            } else {
                sendText(exchange, 500, e.getMessage());
            }
            return outcome;
        } finally {
            if (registered != null) {
                running.remove(registered);
            }
        }
    }

    /** JDBC query timeouts are whole seconds; round up so a timeout never fires early. */
    private int timeoutSeconds() {
        return (int) Math.max(1, (settings.timeout().toMillis() + 999) / 1000);
    }

    private static boolean isInterrupt(Throwable e) {
        for (Throwable c = e; c != null; c = c.getCause()) {
            if (c.getMessage() != null && c.getMessage().contains("INTERRUPT Error")) {
                return true;
            }
        }
        return false;
    }

    private static void cancel(Statement statement) {
        try {
            statement.cancel();
        } catch (SQLException e) {
            log.warn("Could not cancel a running query", e);
        }
    }

    private static void sendText(HttpExchange exchange, int status, String message) throws IOException {
        byte[] body = (message == null ? "" : message).getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=UTF-8");
        exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
        if (body.length > 0) {
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        }
    }

    /** Queries executing right now; for tests. */
    int runningQueries() {
        return running.size();
    }

    /**
     * Cancels the queries still running and waits for them to be answered, then stops the server, so
     * none is left running on the shared DuckDB instance while the collector shuts down. The HTTP
     * server stops last: stopping it closes open connections, so cancelled queries could no longer
     * get their error response.
     */
    @Override
    public void close() {
        closing = true; // new requests get 503 from here on
        running.forEach(QueryServer::cancel);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!running.isEmpty() && System.nanoTime() < deadline) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        if (!running.isEmpty()) {
            log.warn("{} queries still running 10s after they were cancelled", running.size());
        }
        server.stop(0);
        requests.shutdown();
        try {
            if (!requests.awaitTermination(5, TimeUnit.SECONDS)) {
                requests.shutdownNow();
            }
        } catch (InterruptedException e) {
            requests.shutdownNow();
            Thread.currentThread().interrupt();
        }
        try {
            allocator.close();
        } catch (RuntimeException e) {
            log.warn("Query allocator closed with memory still in use", e);
        }
    }
}

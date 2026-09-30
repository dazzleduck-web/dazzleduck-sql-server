package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.commons.authorization.SubjectAndVerifiedClaims;
import io.dazzleduck.sql.flight.MicroMeterFlightRecorder;
import io.dazzleduck.sql.flight.context.SyntheticFlightContext;
import io.dazzleduck.sql.flight.server.auth2.AuthUtils;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.*;

import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A query must stop when its caller goes away or cancels it, and a cancel must not close the
 * connection while the streaming thread is still inside DuckDB on it.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(60)
class StreamCancellationTest {

    // Minutes of work before the first row; only finishes quickly if it is interrupted.
    private static final String SLOW_TO_EXECUTE = "SELECT sum(a.range * b.range)::BIGINT FROM range(1000000) a, range(1000000) b";
    // Effectively endless stream of rows.
    private static final String ENDLESS_STREAM = "SELECT * FROM range(10000000000)";

    private BufferAllocator allocator;
    private FlightServer server;
    private FlightSqlClient client;
    private DuckDBFlightSqlProducer producer;

    @BeforeAll
    void setup() throws Exception {
        allocator = new RootAllocator(Long.MAX_VALUE);
        Location location = FlightTestUtils.findNextLocation();
        producer = new DuckDBFlightSqlProducer(
                location, UUID.randomUUID().toString(), "test-secret", allocator,
                System.getProperty("java.io.tmpdir"), AccessMode.COMPLETE, DuckDBFlightSqlProducer.newTempDir(),
                null, Executors.newSingleThreadScheduledExecutor(), Duration.ofMinutes(10), Duration.ZERO,
                Clock.systemDefaultZone(), new MicroMeterFlightRecorder(new SimpleMeterRegistry(), "test"),
                DuckDBFlightSqlProducer.DEFAULT_INGESTION_CONFIG, List.of());
        server = FlightServer.builder(allocator, location, producer)
                .headerAuthenticator(AuthUtils.getTestAuthenticator())
                .build()
                .start();
        client = new FlightSqlClient(FlightClient.builder(allocator, location)
                .intercept(AuthUtils.createClientMiddlewareFactory("admin", "password", Map.of()))
                .build());
    }

    @AfterAll
    void teardown() throws Exception {
        if (client != null) client.close();
        if (server != null) server.close();
        if (allocator != null) allocator.close();
    }

    @AfterEach
    void drain() {
        producer.statementLoadingCache.invalidateAll();
        producer.statementLoadingCache.cleanUp();
    }

    private boolean anyRunning() {
        return producer.statementLoadingCache.asMap().values().stream().anyMatch(StatementContext::running);
    }

    private static void await(BooleanSupplier condition, Duration within, String what) throws InterruptedException {
        long deadline = System.nanoTime() + within.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) fail("Timed out waiting for: " + what);
            Thread.sleep(20);
        }
    }

    private static void closeQuietly(FlightStream stream) {
        try {
            stream.close();
        } catch (Exception ignored) {
            // a cancelled stream may report the cancellation on close
        }
    }

    @Test
    void clientCancelWhileExecutingInterruptsTheQuery() throws Exception {
        FlightStream stream = client.getStream(client.execute(SLOW_TO_EXECUTE).getEndpoints().get(0).getTicket());
        await(this::anyRunning, Duration.ofSeconds(10), "query to start");
        stream.cancel("client went away", null);
        await(() -> producer.statementLoadingCache.size() == 0, Duration.ofSeconds(10), "query to stop");
        closeQuietly(stream);
    }

    @Test
    void clientCancelWhileStreamingStopsTheQuery() throws Exception {
        FlightStream stream = client.getStream(client.execute(ENDLESS_STREAM).getEndpoints().get(0).getTicket());
        assertTrue(stream.next(), "expected a first batch");
        stream.cancel("client went away", null);
        await(() -> producer.statementLoadingCache.size() == 0, Duration.ofSeconds(10), "stream to stop");
        closeQuietly(stream);
    }

    @Test
    void cancelFlightInfoStopsARunningQuery() throws Exception {
        FlightInfo info = client.execute(SLOW_TO_EXECUTE);
        var read = CompletableFuture.runAsync(() -> {
            try (FlightStream s = client.getStream(info.getEndpoints().get(0).getTicket())) {
                while (s.next()) { }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        await(this::anyRunning, Duration.ofSeconds(10), "query to start");
        client.cancelFlightInfo(new CancelFlightInfoRequest(info));
        assertThrows(Exception.class, read::get, "the stream should fail once its query is cancelled");
        await(() -> producer.statementLoadingCache.size() == 0, Duration.ofSeconds(10), "query to be removed");
    }

    @Test
    void cancelDefersClosingTheConnectionUntilTheStreamEnds() throws Exception {
        var connection = ConnectionPool.getConnection();
        Statement statement = connection.createStatement();
        var ctx = new StatementContext<>(connection, statement, "SELECT 1");
        long id = StatementHandle.nextStatementId();
        producer.statementLoadingCache.put(new DuckDBFlightSqlProducer.CacheKey("admin", id), ctx);
        ctx.start(); // a stream thread is using the statement

        var caller = new SyntheticFlightContext(Map.of(), new SubjectAndVerifiedClaims("admin", Map.of()));
        assertTrue(producer.tryCancel(id, caller));
        assertFalse(connection.isClosed(), "cancel closed the connection under a running stream");
        assertFalse(statement.isClosed(), "cancel closed the statement under a running stream");

        ctx.end(); // the stream thread finishes
        assertTrue(connection.isClosed());
        assertTrue(statement.isClosed());
    }

    @Test
    void cancelOfARunningPreparedStatementDefersClosingIt() throws Exception {
        var connection = ConnectionPool.getConnection();
        var statement = connection.prepareStatement("SELECT 1");
        var ctx = new StatementContext<>(connection, statement, "SELECT 1");
        long id = StatementHandle.nextStatementId();
        producer.preparedStatementLoadingCache.put(new DuckDBFlightSqlProducer.CacheKey("admin", id), ctx);
        ctx.start();

        var caller = new SyntheticFlightContext(Map.of(), new SubjectAndVerifiedClaims("admin", Map.of()));
        assertTrue(producer.tryCancel(id, caller));
        assertFalse(statement.isClosed(), "cancel closed the prepared statement under a running stream");
        assertNull(producer.preparedStatementLoadingCache.getIfPresent(new DuckDBFlightSqlProducer.CacheKey("admin", id)));

        ctx.end();
        assertTrue(statement.isClosed());
        assertTrue(connection.isClosed());
    }

    @Test
    void cancelOfAnIdleCursorClosesItAtOnce() throws Exception {
        var connection = ConnectionPool.getConnection();
        var ctx = new StatementContext<>(connection, connection.createStatement(), "SELECT 1");
        long id = StatementHandle.nextStatementId();
        producer.statementLoadingCache.put(new DuckDBFlightSqlProducer.CacheKey("admin", id), ctx);

        var caller = new SyntheticFlightContext(Map.of(), new SubjectAndVerifiedClaims("admin", Map.of()));
        assertTrue(producer.tryCancel(id, caller));
        assertTrue(connection.isClosed());
    }

    /** Records what a stream sends to its client; the client stays connected. */
    private static final class RecordingListener implements FlightProducer.ServerStreamListener {
        volatile Throwable error;
        volatile boolean completed;
        final java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(1);

        @Override public boolean isCancelled() { return false; }
        @Override public void setOnCancelHandler(Runnable handler) { }
        @Override public boolean isReady() { return true; }
        @Override public void start(org.apache.arrow.vector.VectorSchemaRoot root,
                                    org.apache.arrow.vector.dictionary.DictionaryProvider dictionaries,
                                    org.apache.arrow.vector.ipc.message.IpcOption option) { }
        @Override public void putNext() { }
        @Override public void putNext(org.apache.arrow.memory.ArrowBuf metadata) { }
        @Override public void putMetadata(org.apache.arrow.memory.ArrowBuf metadata) { }
        @Override public void error(Throwable ex) { error = ex; done.countDown(); }
        @Override public void completed() { completed = true; done.countDown(); }
    }

    @Test
    void cancellingAQueuedQueryEndsItCleanlyWithoutRunningIt() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        var release = new java.util.concurrent.CountDownLatch(1);
        executor.submit(() -> { release.await(); return null; }); // keeps the stream below queued
        try {
            var connection = ConnectionPool.getConnection();
            Statement statement = connection.createStatement();
            var ctx = new StatementContext<>(connection, statement, "SELECT 1");
            long id = StatementHandle.nextStatementId();
            var key = new DuckDBFlightSqlProducer.CacheKey("admin", id);
            ctx.markClaimed();
            producer.statementLoadingCache.put(key, ctx);
            var executed = new java.util.concurrent.atomic.AtomicBoolean();
            var listener = new RecordingListener();
            ResultSetStreamUtil.streamResultSet(executor, ctx, key, new OptionalResultSetSupplier() {
                        @Override public boolean hasResultSet() { return false; }
                        @Override public org.duckdb.DuckDBResultSet get() { return null; }
                        @Override public void execute() { executed.set(true); }
                    }, allocator, 1024, listener,
                    () -> producer.statementLoadingCache.asMap().remove(key, ctx),
                    new MicroMeterFlightRecorder(new SimpleMeterRegistry(), "test"));

            var caller = new SyntheticFlightContext(Map.of(), new SubjectAndVerifiedClaims("admin", Map.of()));
            assertTrue(producer.tryCancel(id, caller));
            assertFalse(statement.isClosed(), "a queued query must not be closed under its pending task");

            release.countDown();
            assertTrue(listener.done.await(10, java.util.concurrent.TimeUnit.SECONDS));
            assertFalse(executed.get(), "a query cancelled while queued must not run");
            assertInstanceOf(FlightRuntimeException.class, listener.error);
            assertEquals(FlightStatusCode.CANCELLED, ((FlightRuntimeException) listener.error).status().code());
            assertTrue(statement.isClosed(), "closed once its stream ended");
        } finally {
            release.countDown();
            executor.shutdown();
        }
    }

    @Test
    void aLateCancelDoesNotCancelTheNextRunOfAPreparedStatement() throws Exception {
        var connection = ConnectionPool.getConnection();
        var ctx = new StatementContext<>(connection, connection.prepareStatement("SELECT 1"), "SELECT 1");
        try {
            ctx.markClaimed();
            ctx.start();
            ctx.end();       // the first run finished; the prepared statement stays open
            ctx.cancel();    // e.g. a gRPC cancel handler firing after the stream ended
            assertFalse(ctx.isCancelRequested(), "a cancel with no stream to see it must not stick");
            ctx.markClaimed();
            ctx.start();     // the client runs it again
            assertFalse(ctx.isCancelRequested(), "the next run must not be cancelled");
            ctx.end();
        } finally {
            ctx.close();
        }
    }

    @Test
    void aSecondStreamThatCannotStartLeavesTheRunningStreamAlone() throws Exception {
        var connection = ConnectionPool.getConnection();
        var prepared = connection.prepareStatement("SELECT 1");
        var ctx = new StatementContext<>(connection, prepared, "SELECT 1", true);
        var executor = Executors.newSingleThreadExecutor();
        try {
            ctx.markClaimed();
            ctx.start(); // stream A is running on the prepared statement

            // Stream B for the same prepared handle: its start() fails because A holds the context.
            var executed = new java.util.concurrent.atomic.AtomicBoolean();
            var listener = new RecordingListener();
            ctx.markClaimed();
            ResultSetStreamUtil.streamResultSet(executor, ctx, new DuckDBFlightSqlProducer.CacheKey("admin", 1L),
                    new OptionalResultSetSupplier() {
                        @Override public boolean hasResultSet() { return false; }
                        @Override public org.duckdb.DuckDBResultSet get() { return null; }
                        @Override public void execute() { executed.set(true); }
                    }, allocator, 1024, listener, () -> {},
                    new MicroMeterFlightRecorder(new SimpleMeterRegistry(), "test"));
            assertTrue(listener.done.await(10, java.util.concurrent.TimeUnit.SECONDS));
            assertNotNull(listener.error, "B fails to start");
            assertFalse(executed.get(), "B never runs the query");

            assertTrue(ctx.running(), "B's failure must not end A's stream");
            ctx.closeWhenIdle(); // e.g. a cancel or cache invalidation while A is still streaming
            assertFalse(prepared.isClosed(), "the statement must not be closed under A");

            ctx.end(); // A finishes
            assertTrue(prepared.isClosed(), "closed once A ends");
        } finally {
            executor.shutdown();
            ctx.close();
        }
    }
}

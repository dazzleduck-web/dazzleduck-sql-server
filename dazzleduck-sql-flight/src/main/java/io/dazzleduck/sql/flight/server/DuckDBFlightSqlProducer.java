package io.dazzleduck.sql.flight.server;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.protobuf.*;
import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.common.ConfigConstants;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.commons.authorization.SessionVariables;
import io.dazzleduck.sql.commons.authorization.SqlAuthorizer;
import io.dazzleduck.sql.commons.authorization.UnauthorizedException;
import io.dazzleduck.sql.commons.ingestion.*;
import io.dazzleduck.sql.flight.FlightRecorder;
import io.dazzleduck.sql.flight.MicroMeterFlightRecorder;
import io.dazzleduck.sql.flight.SimpleFlightRecorder;
import io.dazzleduck.sql.flight.ingestion.IngestionParameters;
import io.dazzleduck.sql.flight.model.RunningStatementInfo;
import io.dazzleduck.sql.flight.server.auth2.AdvanceServerCallHeaderAuthMiddleware;
import io.dazzleduck.sql.flight.stream.FlightStreamReader;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
import org.apache.arrow.adapter.jdbc.JdbcParameterBinder;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfigBuilder;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlUtils;
import org.apache.arrow.flight.sql.SqlInfoBuilder;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBResultSet;
import org.duckdb.DuckDBResultSetMetaData;
import org.duckdb.StatementReturnType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.*;
import java.util.concurrent.*;

import static com.google.protobuf.Any.pack;
import static com.google.protobuf.ByteString.copyFrom;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Objects.isNull;
import static org.duckdb.DuckDBConnection.DEFAULT_SCHEMA;

/**
 * It's a simple implementation which support most of the construct for reading as well as bulk writing to parquet file.
 * For now only property which is supported is database, schema and fetch size which are supplied as the connection parameter
 * and available in the header. More options will be supported in the future version.
 * Future implementation note for statement we check if its SET or RESET statement and based on that use cookies to set unset the values
 */
public class DuckDBFlightSqlProducer implements FlightSqlHttpProducer, SqlProducerMBean {

    public static final String TEMP_WRITE_FORMAT = "arrow";
    public static final IngestionConfig DEFAULT_INGESTION_CONFIG = new IngestionConfig(1024 * 1024,
            1024 * 1024 * 1024L,
            2048,
            256 * 1024 * 1024L,
            Duration.ofSeconds(2));

    public static AccessMode getAccessMode(com.typesafe.config.Config appConfig) {
        return AccessMode.valueOf(appConfig.getString(ConfigConstants.ACCESS_MODE_KEY).toUpperCase());
    }

    public static Path getTempWriteDir(com.typesafe.config.Config appConfig) throws IOException {
        return ConfigConstants.getTempWriteDir(appConfig);
    }

    @Override
    public long getRunningStatements() {
        return statementLoadingCache.size();
    }

    @Override
    public long getOpenPreparedStatement() {
        return preparedStatementLoadingCache.size();
    }

    @Override
    public long getRunningPreparedStatements() {
        var map = preparedStatementLoadingCache.asMap();
        var size = 0;
        for(var e : map.values()){
            if(e.running()) {
                size +=1;
            }
        }
        return size;
    }

    @Override
    public double getBytesOut() {
        return recorder.getBytesOut();
    }
    @Override
    public long getCompletedStatements() {
        return recorder.getCompletedStatements();
    }

    @Override
    public long getCompletedPreparedStatements() {
        return recorder.getCompletedPreparedStatements();
    }


    @Override
    public long getCancelledStatements() {
        return recorder.getCancelledStatements();
    }

    @Override
    public long getCancelledPreparedStatements() {
        return recorder.getCancelledPreparedStatements();
    }

    @Override
    public List<RunningStatementInfo> getRunningStatementDetails() {
        var result = new ArrayList<RunningStatementInfo>();
        statementLoadingCache.asMap().forEach((key, ctx) -> {
                result.add(
                        new RunningStatementInfo(
                                key.peerIdentity(),                       // user
                                String.valueOf(key.id()),                  // statementId
                                ctx.startTime(),                           // startInstant
                                ctx.getQuery(),                                // query
                                ctx.running(),                                 // action
                                ctx.endTime()                                       // endInstant
                        )
                );
        });

        return result;
    }

    @Override
    public List<RunningStatementInfo> getOpenPreparedStatementDetails() {
        List<RunningStatementInfo> result = new ArrayList<>();

        preparedStatementLoadingCache.asMap().forEach((key, ctx) -> {
            result.add(
                    new RunningStatementInfo(
                            key.peerIdentity(),
                            String.valueOf(key.id()),
                            ctx.startTime(),
                            ctx.getQuery(),
                            ctx.running(),
                            ctx.endTime()
                    )
            );
        });
        return result;
    }

    @Override
    public List<RunningStatementInfo> getRunningBulkIngestDetails() {
        return List.of();
    }

    @Override
    public List<Stats> getIngestionDetails() {
        return ingestionHandler.getQueueStats();
    }

    @Override
    public long getIngestRequests() {
        return recorder.getIngestRequests();
    }

    @Override
    public long getIngestErrors() {
        return recorder.getIngestErrors();
    }

    @Override
    public double getBytesIn() {
        return recorder.getBytesIn();
    }

    @Override
    public Instant getStartTime() {
        return startTime;
    }

    public static FlightRecorder buildRecorder(String producerId) {
        return buildRecorder(producerId, "dazzleduck-sql-server");
    }

    public static FlightRecorder buildRecorder(String producerId, String serviceName) {
        try {
            var registry = new LoggingMeterRegistry();
            setupCommonTags(registry, producerId, serviceName);
            return new MicroMeterFlightRecorder(registry, producerId);
        } catch (Throwable t) {
            return new SimpleFlightRecorder();
        }
    }

    private static void setupCommonTags(io.micrometer.core.instrument.MeterRegistry registry, String producerId, String serviceName) {
        MicroMeterFlightRecorder.setupCommonTags(registry, producerId, serviceName);
    }

    public AccessMode getAccessMode() {
        return accessMode;
    }


    public record DatabaseSchema ( String database, String schema) {}
    public record CacheKey(String peerIdentity, long id){}

    protected static final Calendar DEFAULT_CALENDAR = JdbcToArrowUtils.getUtcCalendar();

    /**
     * Maps a query's JDBC result metadata to its Arrow schema. DuckDB 2.0's driver reports a
     * NULL-typed column ({@code SELECT NULL}, {@code getvariable} of an unset variable) as
     * {@link Types#OTHER} named {@code "NULL"}, which Arrow's default mapping rejects ("Unmapped JDBC
     * type: 1111"). It maps to Arrow's Null type, which is what DuckDB 2.0 exports for the column's
     * data; every other column keeps the default mapping. The config requires an allocator, though
     * building a schema allocates nothing, hence one with no capacity.
     */
    static final JdbcToArrowConfig RESULT_SCHEMA_CONFIG = new JdbcToArrowConfigBuilder(
            new RootAllocator(0), DEFAULT_CALENDAR)
            .setJdbcToArrowTypeConverter(DuckDBFlightSqlProducer::resultArrowType)
            .build();

    static ArrowType resultArrowType(JdbcFieldInfo field) {
        if (field.getJdbcType() == Types.NULL
                || (field.getJdbcType() == Types.OTHER && isNullTypeName(field.getTypeName()))) {
            return ArrowType.Null.INSTANCE;
        }
        return JdbcToArrowUtils.getArrowTypeFromJdbcType(field, DEFAULT_CALENDAR);
    }

    private static boolean isNullTypeName(String typeName) {
        return typeName != null && typeName.replace("\"", "").equalsIgnoreCase("NULL");
    }
    public static final String  DEFAULT_DATABASE = "memory";
    protected final FlightRecorder recorder;
    private final Instant startTime;
    private final AccessMode accessMode;
    private final Set<Integer> supportedSqlInfo;
    // Platform threads that start queries (a stream's execute, createPreparedStatement's prepare):
    // native DuckDB calls must not run on a virtual thread (see StreamExecutors). Also bounds how
    // many queries start at once.
    protected final ExecutorService executorService = StreamExecutors.duckDbPool("duckdb-execute", Runtime.getRuntime().availableProcessors());
    // Platform threads for everything a running stream fetches (each batch) and its cleanup: kept
    // apart from executorService so streams already running never wait behind new long executes.
    protected final ExecutorService fetchExecutorService = StreamExecutors.duckDbPool("duckdb-fetch", Runtime.getRuntime().availableProcessors());
    // Stream tasks on virtual threads, so a client that stops reading parks a virtual thread
    // instead of holding one of the few platform threads.
    protected final StreamExecutors streamExecutors = StreamExecutors.create(executorService, fetchExecutorService);
    private final static Logger logger = LoggerFactory.getLogger(DuckDBFlightSqlProducer.class);
    private Set<Location> dataProcessorLocations = new LinkedHashSet<>();
    private final Location serverLocation;
    private final String producerId;
    protected final String secretKey;

    protected final BufferAllocator allocator;
    private final String warehousePath;
    // Package-private only so tests (ConnectionLeakTest) can inspect it; not for production use.
    final Cache<CacheKey, StatementContext<PreparedStatement>> preparedStatementLoadingCache;
    protected final Cache<CacheKey, StatementContext<Statement>> statementLoadingCache;
    private final SqlAuthorizer sqlAuthorizer;

    private final SqlInfoBuilder sqlInfoBuilder;

    private final IngestionConfig bulkIngestionConfig;
    private final CursorConfig cursorConfig;

    /**
     * Wrapper for ingestion queue with lifecycle tracking metadata.
     * <p>
     * This class tracks queue lifecycle including creation time and the last
     * applied transformation. The transformation is stored in an AtomicReference
     * to ensure thread-safe updates when transformations are refreshed concurrently.
     *
     * @see #getOrCreateIngestionQueue(String)
     */

    private final IngestionHandler ingestionHandler;

    /**
     * This producer's private scratch directory, created under the configured
     * {@code temp_write_location} — never that location itself. Incoming ingestion batches are
     * staged here before the Parquet COPY, and {@link #close()} deletes it.
     *
     * <p>The location is shared: it defaults to {@code ${java.io.tmpdir}/dazzleduck-writes}, the
     * same default the OTel collector uses, so any number of servers and collectors on a host stage
     * into it. Deleting the location on close — which this used to do — removed every other
     * process's in-flight batches with it, failing their ingests with "No files found".
     */
    private final Path tempDir;

    private final ScheduledExecutorService scheduledExecutorService;

    /**
     * Single shared scheduler for every bulk-ingest queue's time-based flush trigger — including all
     * partition children of a {@link PartitionedIngestionQueue}. Daemon-threaded and shut down in
     * {@link #close()}. Previously each queue (and each partition child) created its own
     * {@code Executors.newSingleThreadScheduledExecutor()} that was never shut down, leaking one
     * non-daemon thread per queue — N+1 per partitioned queue — on every queue eviction. Mirrors the
     * OTLP collector, which already shares one flush scheduler across its queues.
     */
    private final ScheduledExecutorService bulkIngestFlushScheduler =
            Executors.newScheduledThreadPool(2, r -> {
                Thread t = new Thread(r, "dd-bulk-ingest-flush");
                t.setDaemon(true);
                return t;
            });

    private final Duration defaultQueryTimeout;

    private final Duration maxQueryTimeout;

    private final Clock clock;


    public static Path newTempDir() {
        var dir = Path.of(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());
        // A producer deletes only its own scratch directory inside this one, never the location
        // itself, so remove the location at exit — deleteOnExit only removes an empty directory.
        dir.toFile().deleteOnExit();
        if (!Files.exists(dir)) {
            try {
                Files.createDirectories(dir);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return dir;
    }

    public DuckDBFlightSqlProducer(Location serverLocation,
                                   String producerId,
                                   String secretKey,
                                   BufferAllocator allocator,
                                   String warehousePath,
                                   AccessMode accessMode,
                                   Path tempDir,
                                   IngestionHandler ingestionHandler,
                                   ScheduledExecutorService scheduledExecutorService,
                                   Duration queryTimeout,
                                   IngestionConfig ingestionConfig) {
        this(serverLocation, producerId, secretKey, allocator, warehousePath, accessMode, tempDir, ingestionHandler,
                scheduledExecutorService, queryTimeout, Duration.ZERO, Clock.systemDefaultZone(),
                buildRecorder(producerId), ingestionConfig, List.of());

    }
    public DuckDBFlightSqlProducer(Location serverLocation,
                                   String producerId,
                                   String secretKey,
                                   BufferAllocator allocator,
                                   String warehousePath,
                                   AccessMode accessMode,
                                   Path tempDir,
                                   IngestionHandler ingestionHandler,
                                   ScheduledExecutorService scheduledExecutorService,
                                   Duration queryTimeout,
                                   Clock clock,
                                   FlightRecorder recorder,
                                   IngestionConfig bulkIngestionConfig) {
        this(serverLocation, producerId, secretKey, allocator, warehousePath, accessMode, tempDir, ingestionHandler,
                scheduledExecutorService, queryTimeout, Duration.ZERO, clock, recorder, bulkIngestionConfig, List.of());
    }

    public DuckDBFlightSqlProducer(Location serverLocation,
                                   String producerId,
                                   String secretKey,
                                   BufferAllocator allocator,
                                   String warehousePath,
                                   AccessMode accessMode,
                                   Path tempDir,
                                   IngestionHandler ingestionHandler,
                                   ScheduledExecutorService scheduledExecutorService,
                                   Duration defaultQueryTimeout,
                                   Duration maxQueryTimeout,
                                   Clock clock,
                                   FlightRecorder recorder,
                                   IngestionConfig bulkIngestionConfig,
                                   List<Location> dataProcessorLocations) {
        this(serverLocation, producerId, secretKey, allocator, warehousePath, accessMode, tempDir, ingestionHandler,
                scheduledExecutorService, defaultQueryTimeout, maxQueryTimeout, clock, recorder,
                bulkIngestionConfig, dataProcessorLocations, CursorConfig.DEFAULT);
    }

    public DuckDBFlightSqlProducer(Location serverLocation,
                                   String producerId,
                                   String secretKey,
                                   BufferAllocator allocator,
                                   String warehousePath,
                                   AccessMode accessMode,
                                   Path tempDir,
                                   IngestionHandler ingestionHandler,
                                   ScheduledExecutorService scheduledExecutorService,
                                   Duration defaultQueryTimeout,
                                   Duration maxQueryTimeout,
                                   Clock clock,
                                   FlightRecorder recorder,
                                   IngestionConfig bulkIngestionConfig,
                                   List<Location> dataProcessorLocations,
                                   CursorConfig cursorConfig) {
        this.startTime = clock.instant();
        this.serverLocation = serverLocation;
        this.dataProcessorLocations.addAll(dataProcessorLocations);
        this.producerId = producerId;
        this.allocator = allocator;
        this.secretKey = secretKey;
        this.accessMode = accessMode;
        this.tempDir = createScratchDir(tempDir);
        this.scheduledExecutorService = scheduledExecutorService;
        this.defaultQueryTimeout = defaultQueryTimeout;
        this.maxQueryTimeout = maxQueryTimeout;
        this.recorder = recorder;
        if (AccessMode.RESTRICTED == accessMode) {
            this.sqlAuthorizer = SqlAuthorizer.RESTRICTED_DATASOURCE_AUTHORIZER;
        } else if (AccessMode.RESTRICT_READ_ONLY == accessMode) {
            this.sqlAuthorizer = SqlAuthorizer.RESTRICT_READ_ONLY_AUTHORIZER;
        } else if (AccessMode.COMPLETE == accessMode) {
            this.sqlAuthorizer = SqlAuthorizer.NOOP_AUTHORIZER;
        } else {
            this.sqlAuthorizer = SqlAuthorizer.SELECT_ONLY_AUTHORIZER;
        }

        this.ingestionHandler = ingestionHandler;
        this.bulkIngestionConfig = bulkIngestionConfig;
        this.cursorConfig = cursorConfig;
        preparedStatementLoadingCache =
                CacheBuilder.newBuilder()
                        .maximumSize(4000)
                        .expireAfterAccess(10, TimeUnit.MINUTES)
                        .removalListener(new StatementRemovalListener<PreparedStatement>())
                        .build();
        // No time- or size-based eviction: an evicted entry is gone from the cache, so a query still
        // running would become impossible to cancel and would drop out of the cursor limits.
        // Abandoned cursors are reaped by reapIdleCursors instead, and enforceCursorLimits caps the
        // total.
        statementLoadingCache =
                CacheBuilder.newBuilder()
                        .removalListener(new StatementRemovalListener<>())
                        .build();
        this.warehousePath = warehousePath;
        this.clock = clock;
        sqlInfoBuilder = new SqlInfoBuilder();
        try (final Connection connection = ConnectionPool.getConnection()) {
            final DatabaseMetaData metaData = connection.getMetaData();

            sqlInfoBuilder
                    .withFlightSqlServerName(metaData.getDatabaseProductName())
                    .withFlightSqlServerVersion(metaData.getDatabaseProductVersion())
                    .withFlightSqlServerArrowVersion(metaData.getDriverVersion())
                    .withFlightSqlServerReadOnly(metaData.isReadOnly())
                    .withFlightSqlServerSql(true)
                    .withFlightSqlServerSubstrait(false)
                    .withFlightSqlServerTransaction(FlightSql.SqlSupportedTransaction.SQL_SUPPORTED_TRANSACTION_NONE)
                    .withSqlIdentifierQuoteChar(metaData.getIdentifierQuoteString())
                    .withSqlDdlCatalog(metaData.supportsCatalogsInDataManipulation())
                    .withSqlDdlSchema(metaData.supportsSchemasInDataManipulation())
                    .withSqlDdlTable(metaData.allTablesAreSelectable())
                    .withSqlIdentifierCase(
                            metaData.storesMixedCaseIdentifiers()
                                    ? FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_CASE_INSENSITIVE
                                    : metaData.storesUpperCaseIdentifiers()
                                    ? FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_UPPERCASE
                                    : metaData.storesLowerCaseIdentifiers()
                                    ? FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_LOWERCASE
                                    : FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_UNKNOWN)
                    .withSqlQuotedIdentifierCase(
                            metaData.storesMixedCaseQuotedIdentifiers()
                                    ? FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_CASE_INSENSITIVE
                                    : metaData.storesUpperCaseQuotedIdentifiers()
                                    ? FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_UPPERCASE
                                    : metaData.storesLowerCaseQuotedIdentifiers()
                                    ? FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_LOWERCASE
                                    : FlightSql.SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_UNKNOWN)
                    .withSqlAllTablesAreSelectable(true)
                    .withSqlNullOrdering(FlightSql.SqlNullOrdering.SQL_NULLS_SORTED_AT_END)
                    .withSqlMaxColumnsInTable(42)
                    .withFlightSqlServerBulkIngestion(true)
                    .withFlightSqlServerBulkIngestionTransaction(false)
                    .withSqlTransactionsSupported(false);

            // Manually track supported SQL info IDs based on what was configured above
            supportedSqlInfo = Set.of(
                    FlightSql.SqlInfo.FLIGHT_SQL_SERVER_NAME_VALUE,
                    FlightSql.SqlInfo.FLIGHT_SQL_SERVER_VERSION_VALUE,
                    FlightSql.SqlInfo.FLIGHT_SQL_SERVER_ARROW_VERSION_VALUE,
                    FlightSql.SqlInfo.FLIGHT_SQL_SERVER_READ_ONLY_VALUE,
                    FlightSql.SqlInfo.FLIGHT_SQL_SERVER_SQL_VALUE,
                    FlightSql.SqlInfo.FLIGHT_SQL_SERVER_SUBSTRAIT_VALUE,
                    FlightSql.SqlInfo.FLIGHT_SQL_SERVER_TRANSACTION_VALUE,
                    FlightSql.SqlInfo.SQL_IDENTIFIER_QUOTE_CHAR_VALUE,
                    FlightSql.SqlInfo.SQL_DDL_CATALOG_VALUE,
                    FlightSql.SqlInfo.SQL_DDL_SCHEMA_VALUE,
                    FlightSql.SqlInfo.SQL_DDL_TABLE_VALUE,
                    FlightSql.SqlInfo.SQL_IDENTIFIER_CASE_VALUE,
                    FlightSql.SqlInfo.SQL_QUOTED_IDENTIFIER_CASE_VALUE,
                    FlightSql.SqlInfo.SQL_ALL_TABLES_ARE_SELECTABLE_VALUE,
                    FlightSql.SqlInfo.SQL_NULL_ORDERING_VALUE,
                    FlightSql.SqlInfo.SQL_MAX_COLUMNS_IN_TABLE_VALUE,
                    FlightSql.SqlInfo.FLIGHT_SQL_SERVER_BULK_INGESTION_VALUE,
                    FlightSql.SqlInfo.FLIGHT_SQL_SERVER_INGEST_TRANSACTIONS_SUPPORTED_VALUE,
                    FlightSql.SqlInfo.SQL_TRANSACTIONS_SUPPORTED_VALUE
            );

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getProducerId() {
        return producerId;
    }

    @Override
    public void createPreparedStatement(FlightSql.ActionCreatePreparedStatementRequest request, final CallContext context, StreamListener<Result> listener) {
        // Running on another thread
        final Connection connection;
        try {
            connection = getConnection(context, accessMode);
        } catch (Throwable t ) {
            ErrorHandling.handleThrowable(listener, t);
            return;
        }

        final String authorizedSql;
        try {
            authorizedSql = transformPreparedStatementQuery(context, connection, request.getQuery());
        } catch (Throwable t) {
            // Not yet owned by a cache entry, so nothing else will close it.
            closeQuietly(connection);
            ErrorHandling.handleThrowable(listener, t);
            return;
        }
        // Bound to the caller but not expiring: the prepared-statement cache bounds its lifetime.
        StatementHandle handle = StatementHandle.newStatementHandle(authorizedSql, producerId, -1)
                .signed(secretKey, context.peerIdentity(), 0);
        var cacheKey = new CacheKey(context.peerIdentity(), handle.queryId());

        Runnable runnable = () -> {
            // This method owns the connection until the context is cached, and closes it on any
            // failure; after, the cache owns it (closePreparedStatement or eviction closes it). The
            // put is the last step before replying: if it came earlier, a later failure (e.g. a
            // result type the Arrow schema conversion cannot map, such as LIST or HUGEINT) would
            // leave a cached statement the client never got a handle for, holding its connection
            // until eviction.
            boolean cached = false;
            try {
                final ByteString serializedHandle =
                        copyFrom(handle.serialize());

                final PreparedStatement preparedStatement =
                        connection.prepareStatement(
                                authorizedSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                final Schema parameterSchema =
                        JdbcToArrowUtils.jdbcToArrowSchema(preparedStatement.getParameterMetaData(), DEFAULT_CALENDAR);

                final DuckDBResultSetMetaData metaData = (DuckDBResultSetMetaData) preparedStatement.getMetaData();
                var builder =
                        FlightSql.ActionCreatePreparedStatementResult.newBuilder()
                                .setParameterSchema(copyFrom(serializeMetadata(parameterSchema)))
                                .setPreparedStatementHandle(serializedHandle);
                ByteString bytes;
                if (isNull(metaData) || metaData.getReturnType() == StatementReturnType.NOTHING) {
                    bytes = ByteString.copyFrom(
                            serializeMetadata(new Schema(List.of())));
                } else {
                    var x = JdbcToArrowUtils.jdbcToArrowSchema(metaData, RESULT_SCHEMA_CONFIG);
                    bytes = ByteString.copyFrom(
                            serializeMetadata(x));
                }
                builder.setDatasetSchema(bytes);
                final FlightSql.ActionCreatePreparedStatementResult result = builder.build();
                preparedStatementLoadingCache.put(
                        cacheKey, new StatementContext<>(connection, preparedStatement, authorizedSql, true));
                cached = true;
                listener.onNext(new Result(pack(result).toByteArray()));
            } catch (Throwable e ) {
                if (!cached) {
                    closeQuietly(connection); // also closes a prepared statement created on it
                }
                ErrorHandling.handleThrowable(listener, e);
                return;
            }
            listener.onCompleted();
        };
        try {
            executorService.submit(runnable);
        } catch (RejectedExecutionException e) {
            // Shutting down: the runnable will never run to take ownership.
            closeQuietly(connection);
            ErrorHandling.handleThrowable(listener, e);
        }
    }

    @Override
    public void closePreparedStatement(FlightSql.ActionClosePreparedStatementRequest request, CallContext context, StreamListener<Result> listener) {
        final StatementHandle statementHandle = StatementHandle.deserialize(request.getPreparedStatementHandle());
        if (invalidHandle(statementHandle, context)) {
            ErrorHandling.handleInvalidHandle(listener);
            return;
        }
        Runnable runnable = () -> {
            try {
                var key = new CacheKey(context.peerIdentity(), statementHandle.queryId());
                preparedStatementLoadingCache.invalidate(key);
            } catch (final Throwable e) {
                ErrorHandling.handleThrowable(listener, e);
                return;
            }
            listener.onCompleted();
        };
        executorService.submit(runnable);
    }


    @Override
    public FlightInfo getFlightInfoPreparedStatement(
            final FlightSql.CommandPreparedStatementQuery command,
            final CallContext context,
            final FlightDescriptor descriptor) {
        StatementHandle statementHandle = StatementHandle.deserialize(command.getPreparedStatementHandle());
        if (invalidHandle(statementHandle, context)) {
            ErrorHandling.handleInvalidHandle();
            return null; // Never reached if handleInvalidHandle throws, but prevents execution if it doesn't
        }
        var key = new CacheKey(context.peerIdentity(), statementHandle.queryId());
        StatementContext<PreparedStatement> statementContext =
                preparedStatementLoadingCache.getIfPresent(key);
        if (statementContext == null) {
            ErrorHandling.handleContextNotFound();
            return null; // Never reached if handleContextNotFound throws, but prevents execution if it doesn't
        }
        return getFlightInfoForSchema(command, descriptor, null);
    }


    /**
     * Template method for getting flight info from a SQL query string.
     * Subclasses can override this method to customize query processing behavior
     * (e.g., adding authorization, parallelization, or query transformation).
     * The default implementation delegates to {@link #getFlightInfoStatement(String, CallContext, FlightDescriptor)}.
     *
     * @param query The SQL query string
     * @param context Per-call context
     * @param descriptor The descriptor identifying the data stream
     * @return FlightInfo metadata about the query result stream
     */
    protected FlightInfo getFlightInfoStatementFromQuery(final String query, final CallContext context, final FlightDescriptor descriptor){
        return getFlightInfoStatement(query, context, descriptor);
    }
    @Override
    public FlightInfo getFlightInfoStatement(
            final FlightSql.CommandStatementQuery request,
            final CallContext context,
            final FlightDescriptor descriptor) {
        String query = request.getQuery();
        return getFlightInfoStatementFromQuery(query, context, descriptor);
    }


    @Override
    public SchemaResult getSchemaStatement(FlightSql.CommandStatementQuery command, CallContext context,
                                           FlightDescriptor descriptor) {
        String query = command.getQuery();
        try (Connection connection = getConnection(context, accessMode);
             PreparedStatement preparedStatement = connection.prepareStatement(
                     query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {

            DuckDBResultSetMetaData metaData = (DuckDBResultSetMetaData) preparedStatement.getMetaData();
            Schema schema;
            if (isNull(metaData) || metaData.getReturnType() == StatementReturnType.NOTHING) {
                schema = new Schema(List.of());
            } else {
                schema = JdbcToArrowUtils.jdbcToArrowSchema(metaData, RESULT_SCHEMA_CONFIG);
            }
            return new SchemaResult(schema);
        } catch (SQLException e) {
            throw CallStatus.INVALID_ARGUMENT
                    .withDescription("Failed to get schema for query: " + e.getMessage())
                    .withCause(e)
                    .toRuntimeException();
        } catch (Exception e) {
            throw CallStatus.INTERNAL
                    .withDescription("Error getting schema: " + e.getMessage())
                    .withCause(e)
                    .toRuntimeException();
        }
    }


    @Override
    public void getStreamPreparedStatement(FlightSql.CommandPreparedStatementQuery command, CallContext context,
                                           ServerStreamListener listener) {

        StatementHandle statementHandle = StatementHandle.deserialize(command.getPreparedStatementHandle());
        if (invalidHandle(statementHandle, context)) {
            ErrorHandling.handleInvalidHandle(listener);
            return;
        }
        var key = new CacheKey(context.peerIdentity(), statementHandle.queryId());
        StatementContext<PreparedStatement> statementContext =
            preparedStatementLoadingCache.getIfPresent(key);
        if (statementContext == null) {
            ErrorHandling.handleContextNotFound();
            return; // Never reached if handleContextNotFound throws, but prevents NPE if it doesn't
        }
        // Validated now (an invalid timeout header fails the call), but applied only when this run
        // starts executing: the statement is shared, and a run rejected because another is still
        // executing must not change that run's timeout.
        final int queryTimeoutSeconds;
        try {
            queryTimeoutSeconds = getEffectiveQueryTimeoutSeconds(context);
        } catch (RuntimeException e) {
            ErrorHandling.handleThrowable(listener, e);
            return;
        }
        PreparedStatement preparedStatement = statementContext.getStatement();
        OptionalResultSetSupplier run = OptionalResultSetSupplier.of(preparedStatement);
        OptionalResultSetSupplier timedRun = new OptionalResultSetSupplier() {
            @Override
            public boolean hasResultSet() {
                return run.hasResultSet();
            }

            @Override
            public DuckDBResultSet get() throws SQLException {
                return run.get();
            }

            @Override
            public void execute() throws SQLException {
                preparedStatement.setQueryTimeout(queryTimeoutSeconds);
                run.execute();
            }
        };
        statementContext.markClaimed(); // like getStreamStatement: a queued run sees a cancel and counts as live
        ResultSetStreamUtil.streamResultSet(streamExecutors, statementContext, key, timedRun,
            allocator, getBatchSize(context),
            listener, () -> {}, recorder);
    }


    @Override
    public void getStreamStatement(
            final FlightSql.TicketStatementQuery ticketStatementQuery,
            final CallContext context,
            final ServerStreamListener listener) {
        StatementHandle statementHandle = StatementHandle.deserialize(ticketStatementQuery.getStatementHandle());
        getStreamStatement(statementHandle, context, listener);
    }

    /**
     * Template method for streaming statement results. Uses {@link #transformQuery} and
     * {@link #createResultSetSupplier} as extension points for subclasses.
     *
     * @param statementHandle The statement handle containing query information
     * @param context Per-call context
     * @param listener An interface for sending data back to the client
     */
    protected void getStreamStatement(
            StatementHandle statementHandle,
            final CallContext context,
            final ServerStreamListener listener) {
        DuckDBConnection connection = null;
        try {
            connection = getConnection(context, getAccessMode());
            String query = statementHandle.query();
            if (statementHandle.queryChecksum() != null
                    && invalidHandle(statementHandle, context)) {
                ErrorHandling.handleInvalidHandle(listener);
                return;
            }
            if (statementHandle.queryChecksum() == null) {
                query = transformQuery(context, connection, query);
            }
            enforceCursorLimits(context.peerIdentity());
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(getEffectiveQueryTimeoutSeconds(context));
            var statementContext = new StatementContext<>(connection, statement, query);
            var key = new CacheKey(context.peerIdentity(), statementHandle.queryId());
            statementContext.markClaimed(); // the stream below will run it: never reaped while queued
            // One stream per ticket at a time: a second one would replace the first's entry (and
            // whichever finished first would remove the other's), leaving a running stream that
            // cancel cannot find and cursor limits do not count.
            if (statementLoadingCache.asMap().putIfAbsent(key, statementContext) != null) {
                statementContext.close(); // closes the new statement and connection
                connection = null;
                throw CallStatus.ALREADY_EXISTS
                        .withDescription("This ticket is already being streamed")
                        .toRuntimeException();
            }
            connection = null; // ownership transferred to StatementContext — do not close here
            ResultSetStreamUtil.streamResultSet(streamExecutors,
                    statementContext,
                    key,
                    createResultSetSupplier(statement, query),
                    allocator,
                    getBatchSize(context),
                    listener,
                    () -> statementLoadingCache.asMap().remove(key, statementContext), recorder);
        } catch (Throwable e) {
            ErrorHandling.handleThrowable(listener, e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception closeEx) {
                    logger.atWarn().setCause(closeEx).log("Failed to close connection after error in getStreamStatement");
                }
            }
        }
    }

    /**
     * Extension point for subclasses to transform or authorize a query before execution.
     * Called only when the statement handle has no pre-computed checksum (i.e., the query
     * was not pre-authorized at planning time).
     * Default implementation returns the query unchanged, but rejects the limit header if present.
     */
    protected String transformQuery(CallContext context, Connection connection, String query)
            throws UnauthorizedException, JsonProcessingException, SQLException {
        if (getLimit(context) > 0) {
            throw CallStatus.INVALID_ARGUMENT
                    .withDescription("Limit header '" + Headers.HEADER_DATA_LIMIT + "' is not supported by this producer")
                    .toRuntimeException();
        }
        return query;
    }

    /**
     * Extension point for subclasses to authorize and rewrite a query at prepared-statement
     * creation time. The returned SQL is stored in the signed handle; execution skips
     * {@link #transformQuery} when a checksum is present, so the rewrite happens exactly once.
     * Default implementation returns the query unchanged.
     */
    protected String transformPreparedStatementQuery(CallContext context, Connection connection, String query)
            throws UnauthorizedException, JsonProcessingException, SQLException {
        return query;
    }

    /**
     * Extension point for subclasses to supply a custom {@link OptionalResultSetSupplier},
     * e.g., one that includes a {@link io.dazzleduck.sql.flight.optimizer.QueryOptimizer}.
     * Default implementation uses no optimizer.
     */
    protected OptionalResultSetSupplier createResultSetSupplier(Statement statement, String query) {
        return OptionalResultSetSupplier.of(statement, query);
    }

    protected static long getLimit(CallContext callContext) {
        return ContextUtils.getValue(callContext, Headers.HEADER_DATA_LIMIT, -1L, Long.class);
    }

    protected static long getOffset(CallContext callContext) {
        return ContextUtils.getValue(callContext, Headers.HEADER_DATA_OFFSET, -1L, Long.class);
    }

    /**
     * Resolves the effective query timeout in seconds for the current request.
     *
     * <p>Resolution order:
     * <ol>
     *   <li>Client-supplied value from the {@value Headers#HEADER_QUERY_TIMEOUT} header (seconds).
     *   <li>Server default ({@code defaultQueryTimeout}) when the client provides no value.
     * </ol>
     *
     * <p>If {@code maxQueryTimeout} is non-zero and the resolved timeout exceeds it, an
     * {@code INVALID_ARGUMENT} Flight error is thrown immediately.
     *
     * @param context the per-call context carrying request headers
     * @return effective timeout in seconds; 0 means no timeout
     * @throws FlightRuntimeException if the requested timeout exceeds the server maximum
     */
    protected int getEffectiveQueryTimeoutSeconds(CallContext context) {
        int requestedSeconds = ContextUtils.getValue(context, Headers.HEADER_QUERY_TIMEOUT, 0, Integer.class);
        if (requestedSeconds < 0) {
            throw CallStatus.INVALID_ARGUMENT
                    .withDescription("Query timeout must be non-negative, got: " + requestedSeconds)
                    .toRuntimeException();
        }
        if (requestedSeconds > 0) {
            // Client explicitly requested a timeout — enforce the server maximum cap.
            if (!maxQueryTimeout.isZero() && requestedSeconds > maxQueryTimeout.toSeconds()) {
                throw CallStatus.INVALID_ARGUMENT
                        .withDescription("Requested query timeout " + requestedSeconds
                                + "s exceeds server maximum of " + maxQueryTimeout.toSeconds() + "s")
                        .toRuntimeException();
            }
            return requestedSeconds;
        }
        // No client-supplied timeout — use the server default (not subject to the max cap).
        return (int) defaultQueryTimeout.toSeconds();
    }


    @Override
    public Runnable acceptPutStatement(FlightSql.CommandStatementUpdate command, CallContext context,
                                       FlightStream flightStream, StreamListener<PutResult> ackStream) {

        final String query = command.getQuery();
        return () -> {
            try (final Connection connection = getConnection(context, accessMode);
                 final Statement statement = connection.createStatement()) {
                statement.execute(query);
                var result =  statement.getUpdateCount();
                final FlightSql.DoPutUpdateResult build =
                        FlightSql.DoPutUpdateResult.newBuilder().setRecordCount(result).build();

                try (final ArrowBuf buffer = allocator.buffer(build.getSerializedSize())) {
                    buffer.writeBytes(build.toByteArray());
                    ackStream.onNext(PutResult.metadata(buffer));
                    ackStream.onCompleted();
                }
            } catch (Throwable throwable) {
                ErrorHandling.handleThrowable(ackStream, throwable);
            }
        };
    }


    @Override
    public Runnable acceptPutPreparedStatementUpdate(FlightSql.CommandPreparedStatementUpdate command,
                                                     CallContext context, FlightStream flightStream,
                                                     StreamListener<PutResult> ackStream) {
        return () -> {
            StatementHandle statementHandle = StatementHandle.deserialize(command.getPreparedStatementHandle());
            if (invalidHandle(statementHandle, context)) {
                ErrorHandling.handleInvalidHandle(ackStream);
                return;
            }
            var key = new CacheKey(context.peerIdentity(),statementHandle.queryId());
            StatementContext<PreparedStatement> statementContext =
                    preparedStatementLoadingCache.getIfPresent(key);
            if (statementContext == null) {
                ErrorHandling.handleContextNotFound(ackStream);
                return;
            }
            final PreparedStatement preparedStatement = statementContext.getStatement();
            // Tracked like a stream, so a concurrent run is rejected and a close waits for it.
            if (!statementContext.tryStart()) {
                ackStream.onError(ErrorHandling.cannotStart(statementContext));
                return;
            }
            try {
                while (flightStream.next()) {
                    final VectorSchemaRoot root = flightStream.getRoot();

                    final int rowCount = root.getRowCount();
                    final int recordCount;

                    if (rowCount == 0) {
                        recordCount = Math.max(0, preparedStatement.executeUpdate());
                    } else {
                        final JdbcParameterBinder binder =
                                JdbcParameterBinder.builder(preparedStatement, root).bindAll().build();
                        while (binder.next()) {
                            preparedStatement.addBatch();
                        }
                        final int[] recordCounts = preparedStatement.executeBatch();
                        recordCount = Arrays.stream(recordCounts).sum();
                    }

                    final FlightSql.DoPutUpdateResult build =
                            FlightSql.DoPutUpdateResult.newBuilder().setRecordCount(recordCount).build();

                    try (final ArrowBuf buffer = allocator.buffer(build.getSerializedSize())) {
                        buffer.writeBytes(build.toByteArray());
                        ackStream.onNext(PutResult.metadata(buffer));
                    }
                }
                ackStream.onCompleted();
            } catch (Throwable e) {
                ErrorHandling.handleThrowable(ackStream, e);
            } finally {
                statementContext.end();
            }
        };
    }


    @Override
    public Runnable acceptPutPreparedStatementQuery(FlightSql.CommandPreparedStatementQuery command,
                                                    CallContext context, FlightStream flightStream,
                                                    StreamListener<PutResult> ackStream) {
       return () -> ErrorHandling.handleUnimplemented(ackStream, "acceptPutPreparedStatementQuery");
    }


    /**
     * Gets an existing ParquetIngestionQueue for the given queue ID, creating it on first call.
     * <p>
     * Refresh of the handler's internal state (target path, transformation, partition columns)
     * is managed lazily inside the {@link IngestionHandler} implementation — callers do not
     * need to supply a refresh delay or clock. The handler's read accessors ({@code getTargetPath},
     * etc.) trigger a refresh automatically when the cached state is stale.
     * <p>
     * Returns {@code null} when the queue's target path is gone (tombstone / deleted mapping).
     *
     * @param queueId unique identifier for the ingestion queue
     * @return the ParquetIngestionQueue for the specified queue, or null if no target path exists
     */
    protected ParquetIngestionQueue getOrCreateIngestionQueue(String queueId) {
        return ingestionHandler.getOrCreateQueue(
                queueId,
                (id, path) -> createQueue(producerId, id, path, ingestionHandler, bulkIngestionConfig, recorder,
                        bulkIngestFlushScheduler),
                new IngestionHandler.QueueEventListener() {
                    @Override public void onCreated(String id)   { recorder.recordQueueCreated(id);   }
                    @Override public void onRefreshed(String id) { recorder.recordQueueRefreshed(id); }
                    @Override public void onDeleted(String id)   {
                        recorder.recordQueueDeleted(id);
                        recorder.unregisterWriteQueue(id); // drop per-queue meters so they don't leak
                    }
                });
    }

    public static ParquetIngestionQueue createQueue(String producerId, String localQueueId, String path, IngestionHandler ingestionHandler,
                                                    IngestionConfig bulkIngestionConfig, FlightRecorder flightRecorder,
                                                    ScheduledExecutorService flushScheduler) {
        int numPartitions = ingestionHandler.getNumPartitions(localQueueId);
        // One shared flush scheduler is used for the queue and, for a partitioned queue, all its
        // partition children — see the bulkIngestFlushScheduler field for why this must not be a
        // per-queue executor.
        ParquetIngestionQueue queue = numPartitions > 1
                ? new PartitionedIngestionQueue(producerId, TEMP_WRITE_FORMAT, path, localQueueId,
                        bulkIngestionConfig.minBucketSize(),
                        bulkIngestionConfig.maxBucketSize(),
                        bulkIngestionConfig.maxBatches(),
                        bulkIngestionConfig.maxPendingWrite(),
                        bulkIngestionConfig.maxDelay(),
                        bulkIngestionConfig.parquetCompression(),
                        ingestionHandler,
                        flushScheduler,
                        Clock.systemDefaultZone(),
                        numPartitions,
                        ingestionHandler.getPartitionExpression(localQueueId))
                : new ParquetIngestionQueue(producerId, TEMP_WRITE_FORMAT, path, localQueueId,
                        bulkIngestionConfig.minBucketSize(),
                        bulkIngestionConfig.maxBucketSize(),
                        bulkIngestionConfig.maxBatches(),
                        bulkIngestionConfig.maxPendingWrite(),
                        bulkIngestionConfig.maxDelay(),
                        bulkIngestionConfig.parquetCompression(),
                        ingestionHandler,
                        flushScheduler,
                        Clock.systemDefaultZone());
        flightRecorder.registerWriteQueue(localQueueId,
                Map.of("write_batches", queue::getTotalWriteBatches,
                        "write_buckets", queue::getTotalWriteBuckets,
                        "bytes_written", queue::getTotalWriteBytes,
                        "failed_batches", queue::getFailedWriteBatches,
                        "failed_buckets", queue::getFailedWriteBuckets,
                        "bytes_failed", queue::getFailedWriteBytes,
                        "producer_id_evictions", queue::getProducerIdEvictions,
                        "data_phase_ms", () -> queue.getDataPhaseNanos() / 1_000_000,
                        "post_ingest_phase_ms", () -> queue.getPostIngestPhaseNanos() / 1_000_000),
                Map.of("pending_batches", queue::getPendingBatches,
                        "pending_buckets", queue::getPendingBuckets),
                Map.of("write_latency", new FlightRecorder.WriteTimerSuppliers(
                        queue::getTotalWriteBuckets,
                        queue::getTimeSpentWriting)));
        return queue;
    }

    @Override
    public Runnable acceptPutStatementBulkIngest(
            FlightSql.CommandStatementIngest command,
            CallContext context,
            FlightStream flightStream,
            StreamListener<PutResult> ackStream) {
        IngestionParameters ingestionParameters = IngestionParameters.getIngestionParameters(command);
        if (!hasWriteAccess(context, ingestionParameters.ingestionQueue(), ackStream)) {
            return () -> {};
        }
        var ingestionQueue = getOrCreateIngestionQueue(ingestionParameters.ingestionQueue());
        if( ingestionQueue == null) {
            return () -> ErrorHandling.handleThrowable(ackStream,
                    new IllegalArgumentException("Ingestion queue '" + ingestionParameters.ingestionQueue() + "' not found. No target path is configured for this queue."));
        }
        return ingestFromReader(FlightStreamReader.of(flightStream, allocator), ingestionQueue, ingestionParameters, ackStream);
    }

    @Override
    public Runnable acceptPutStatementBulkIngest(
            CallContext context,
            IngestionParameters ingestionParameters,
            InputStream inputStream,
            StreamListener<PutResult> ackStream) {
        if (!hasWriteAccess(context, ingestionParameters.ingestionQueue(), ackStream)) {
            return () -> {};
        }
        var ingestionQueue = getOrCreateIngestionQueue(ingestionParameters.ingestionQueue());
        if( ingestionQueue == null) {
            return () -> ErrorHandling.handleThrowable(ackStream,
                    new IllegalArgumentException("Ingestion queue '" + ingestionParameters.ingestionQueue() + "' not found. No target path is configured for this queue."));
        }
        return ingestFromReader(new ArrowStreamReader(inputStream, allocator), ingestionQueue, ingestionParameters, ackStream);
    }

    /**
     * Bulk ingest writes to an ingestion queue without going through SQL, so the query authorizers
     * never see it: every ingest, Flight {@code executeIngest} or HTTP {@code /v1/ingest}, is gated on
     * the authorizer's write check here. COMPLETE allows all writes, RESTRICTED checks the write
     * claim, and READ_ONLY / RESTRICT_READ_ONLY refuse every ingest. (HTTP also checks in its JWT
     * filter; this keeps the rule in force whatever the transport.)
     */
    private boolean hasWriteAccess(CallContext context, String queue, StreamListener<PutResult> ackStream) {
        if (sqlAuthorizer.hasWriteAccess(context.peerIdentity(), queue, getVerifiedClaims(context))) {
            return true;
        }
        ErrorHandling.handleUnauthorized(ackStream, new UnauthorizedException("No write access to ingestion_queue:" + queue));
        return false;
    }

    private Runnable ingestFromReader(
            ArrowReader reader,
            BulkIngestQueue<String, IngestionResult> ingestionQueue,
            IngestionParameters ingestionParameters,
            StreamListener<PutResult> ackStream) {
        return () -> {
            Path tempFile = null;
            try (reader) {
                tempFile = BulkIngestQueue.writeAndValidateTempArrowFile(tempDir, reader);
                long fileSize = Files.size(tempFile);
                recorder.recordIngestReceived(fileSize);
                var batch = ingestionParameters.constructBatch(fileSize, tempFile.toAbsolutePath().toString());
                var result = ingestionQueue.add(batch);
                // add() either rejects the batch (it throws, or returns an already-failed future)
                // or accepts it, and an accepted batch's file belongs to the queue: it deletes it
                // once the batch's bucket is written or abandoned. So a wait that fails from here -
                // this thread interrupted because the client went away, or a timeout - must not
                // delete it: the queue may still read it, in a bucket with other clients' batches,
                // whose writes would then all fail with "No files found".
                if (!result.isCompletedExceptionally()) {
                    tempFile = null;
                }
                result.get(10L, TimeUnit.MINUTES);
                ackStream.onNext(PutResult.empty());
                ackStream.onCompleted();
            } catch (Throwable throwable) {
                if (tempFile != null) {
                    try { Files.deleteIfExists(tempFile); } catch (IOException ignored) {}
                }
                recorder.recordIngestError();
                ErrorHandling.handleThrowable(ackStream, throwable);
            }
        };
    }

    /**
     * Cancels every endpoint of the FlightInfo (a RESTRICTED query can be split across several),
     * replying with a single status: CANCELLED if any of them was still known here, NOT_FOUND if
     * none was. A ticket that cannot be read is INVALID_ARGUMENT.
     */
    @Override
    public void cancelFlightInfo(
            CancelFlightInfoRequest request, CallContext context, StreamListener<CancelStatus> listener) {
        boolean found = false;
        try {
            // Read every ticket first, so a bad request cancels nothing.
            var queryIds = new ArrayList<Long>();
            for (FlightEndpoint endpoint : request.getInfo().getEndpoints()) {
                queryIds.add(queryIdOf(endpoint.getTicket()));
            }
            for (long queryId : queryIds) {
                found |= cancelRunning(new CacheKey(context.peerIdentity(), queryId));
            }
        } catch (FlightRuntimeException e) {
            listener.onError(e);
            return;
        } catch (SQLException e) {
            ErrorHandling.handleSqlException(listener, e);
            return;
        }
        if (!found) {
            ErrorHandling.handleContextNotFound(listener);
            return;
        }
        listener.onNext(CancelStatus.CANCELLED);
        listener.onCompleted();
    }

    private static long queryIdOf(Ticket ticket) {
        try {
            Any command = Any.parseFrom(ticket.getBytes());
            if (command.is(FlightSql.TicketStatementQuery.class)) {
                return StatementHandle.deserialize(command.unpack(FlightSql.TicketStatementQuery.class)
                        .getStatementHandle()).queryId();
            }
            if (command.is(FlightSql.CommandPreparedStatementQuery.class)) {
                return StatementHandle.deserialize(command.unpack(FlightSql.CommandPreparedStatementQuery.class)
                        .getPreparedStatementHandle()).queryId();
            }
            throw CallStatus.INVALID_ARGUMENT
                    .withDescription("Cannot cancel ticket of type " + command.getTypeUrl())
                    .toRuntimeException();
        } catch (InvalidProtocolBufferException | RuntimeException e) {
            if (e instanceof FlightRuntimeException fre) throw fre;
            throw CallStatus.INVALID_ARGUMENT.withDescription("Unreadable ticket").withCause(e).toRuntimeException();
        }
    }

    @Override
    public boolean tryCancel(Long queryId, CallContext context) throws SQLException {
        return cancelRunning(new CacheKey(context.peerIdentity(), queryId));
    }

    /**
     * Cancels the query under {@code key}, returning false if there is nothing to cancel. A cursor is
     * removed (the removal listener closes it once no stream is using it). A prepared statement is
     * removed only if a running execution was interrupted, since DuckDB closes it then; one whose
     * run was still queued, or that is idle (a late cancel), stays usable.
     */
    private boolean cancelRunning(CacheKey key) throws SQLException {
        return cancelIn(statementLoadingCache, key, false) || cancelIn(preparedStatementLoadingCache, key, true);
    }

    private <T extends Statement> boolean cancelIn(Cache<CacheKey, StatementContext<T>> cache, CacheKey key,
                                                   boolean preparedStatements) throws SQLException {
        StatementContext<T> ctx = cache.getIfPresent(key);
        if (ctx == null) {
            return false;
        }
        StatementContext.CancelOutcome outcome;
        try {
            outcome = ctx.cancel();
        } finally {
            if (!preparedStatements) {
                // A cursor serves one stream, so it is removed whatever the cancel found.
                cache.asMap().remove(key, ctx);
            }
        }
        if (preparedStatements) {
            if (outcome == StatementContext.CancelOutcome.NOTHING_RUNNING) {
                // A late cancel (the run already ended): nothing to cancel, and the client's prepared
                // statement stays usable.
                return false;
            }
            if (outcome == StatementContext.CancelOutcome.INTERRUPTED) {
                // DuckDB closes a prepared statement whose execution is interrupted, so it could not
                // run again; removing it gives the next run NOT_FOUND instead of a closed-statement
                // error. A run cancelled while still queued never executed, so its statement stays.
                cache.asMap().remove(key, ctx);
            }
        }
        if (outcome != StatementContext.CancelOutcome.NOTHING_RUNNING) {
            recorder.recordStatementCancel(key, ctx);
        }
        return true;
    }





    @Override
    public FlightInfo getFlightInfoSqlInfo(
            final FlightSql.CommandGetSqlInfo request,
            final CallContext context,
            final FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_SQL_INFO_SCHEMA);
    }

    @Override
    public void getStreamSqlInfo(
            final FlightSql.CommandGetSqlInfo command,
            final CallContext context,
            final ServerStreamListener listener) {
        List<Integer> infoList = command.getInfoList();
        if (infoList.isEmpty()) {
            infoList = supportedSqlInfo.stream().toList();
        }
        sqlInfoBuilder.send(infoList, listener);
    }

    @Override
    public FlightInfo getFlightInfoTypeInfo(FlightSql.CommandGetXdbcTypeInfo request,
                                            CallContext context, FlightDescriptor descriptor) {
        ErrorHandling.throwUnimplemented("getFlightInfoTypeInfo");
        return null;
    }

    @Override
    public void getStreamTypeInfo(FlightSql.CommandGetXdbcTypeInfo request, CallContext context,
                                  ServerStreamListener listener) {
        ErrorHandling.throwUnimplemented("getStreamTypeInfo");
    }

    @Override
    public FlightInfo getFlightInfoCatalogs(
            final FlightSql.CommandGetCatalogs request,
            final CallContext context,
            final FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_CATALOGS_SCHEMA);
    }

    @Override
    public void getStreamCatalogs(final CallContext context, final ServerStreamListener listener) {
        ResultSetStreamUtil.streamResultSet(streamExecutors, DuckDBDatabaseMetadataUtil::getCatalogs, context, accessMode, allocator, listener, recorder);
    }

    @Override
    public FlightInfo getFlightInfoSchemas(FlightSql.CommandGetDbSchemas request, CallContext context,
                                           FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_SCHEMAS_SCHEMA);
    }

    @Override
    public void getStreamSchemas(FlightSql.CommandGetDbSchemas command, CallContext context, ServerStreamListener listener) {
        final String catalog = command.hasCatalog() ? command.getCatalog() : null;
        final String schemaFilterPattern =
                command.hasDbSchemaFilterPattern() ? command.getDbSchemaFilterPattern() : null;
        ResultSetStreamUtil.streamResultSet(streamExecutors, connection ->
                        DuckDBDatabaseMetadataUtil.getSchemas(connection, catalog, schemaFilterPattern),
                context, accessMode, allocator, listener, recorder);
    }

    @Override
    public FlightInfo getFlightInfoTables(
            final FlightSql.CommandGetTables request,
            final CallContext context,
            final FlightDescriptor descriptor) {
        Schema schemaToUse = Schemas.GET_TABLES_SCHEMA;
        if (!request.getIncludeSchema()) {
            schemaToUse = Schemas.GET_TABLES_SCHEMA_NO_SCHEMA;
        }
        return getFlightInfoForSchema(request, descriptor, schemaToUse);
    }

    @Override
    public void getStreamTables(
            final FlightSql.CommandGetTables command,
            final CallContext context,
            final ServerStreamListener listener) {
        final String catalog = command.hasCatalog() ? command.getCatalog() : null;
        final String schemaFilterPattern =
                command.hasDbSchemaFilterPattern() ? command.getDbSchemaFilterPattern() : null;
        final String tableFilterPattern =
                command.hasTableNameFilterPattern() ? command.getTableNameFilterPattern() : null;
        final ProtocolStringList protocolStringList = command.getTableTypesList();
        final int protocolSize = protocolStringList.size();
        final String[] tableTypes =
                protocolSize == 0 ? null : protocolStringList.toArray(new String[protocolSize]);
        ResultSetStreamUtil.streamResultSet(streamExecutors, connection ->
            DuckDBDatabaseMetadataUtil.getTables(connection, catalog, schemaFilterPattern, tableFilterPattern, tableTypes),
                context, accessMode, allocator, listener, recorder);
    }

    @Override
    public FlightInfo getFlightInfoTableTypes(FlightSql.CommandGetTableTypes request, CallContext context,
                                              FlightDescriptor descriptor) {
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_TABLE_TYPES_SCHEMA);
    }


    @Override
    public void getStreamTableTypes(CallContext context, ServerStreamListener listener) {
        ResultSetStreamUtil.streamResultSet(streamExecutors, DuckDBDatabaseMetadataUtil::getTableTypes, context, accessMode, allocator, listener, recorder);
    }

    @Override
    public FlightInfo getFlightInfoPrimaryKeys(FlightSql.CommandGetPrimaryKeys request, CallContext context,
                                               FlightDescriptor descriptor) {
        ErrorHandling.throwUnimplemented("getFlightInfoPrimaryKeys");
        return null;
    }

    @Override
    public void getStreamPrimaryKeys(FlightSql.CommandGetPrimaryKeys command, CallContext context,
                                     ServerStreamListener listener) {
        ErrorHandling.throwUnimplemented(listener, "getStreamPrimaryKeys");
    }


    @Override
    public FlightInfo getFlightInfoExportedKeys(FlightSql.CommandGetExportedKeys request, CallContext context,
                                                FlightDescriptor descriptor) {
        ErrorHandling.throwUnimplemented("getFlightInfoExportedKeys");
        return null;
    }


    @Override
    public FlightInfo getFlightInfoImportedKeys(FlightSql.CommandGetImportedKeys request, CallContext context,
                                                FlightDescriptor descriptor) {
        ErrorHandling.throwUnimplemented("getFlightInfoImportedKeys");
        return null;
    }


    @Override
    public FlightInfo getFlightInfoCrossReference(FlightSql.CommandGetCrossReference request, CallContext context,
                                                  FlightDescriptor descriptor) {
        ErrorHandling.throwUnimplemented("getFlightInfoCrossReference");
        return null;
    }


    @Override
    public void getStreamExportedKeys(FlightSql.CommandGetExportedKeys command, CallContext context,
                                      ServerStreamListener listener) {
        ErrorHandling.throwUnimplemented(listener, "getStreamExportedKeys");
    }


    @Override
    public void getStreamImportedKeys(FlightSql.CommandGetImportedKeys command, CallContext context,
                                      ServerStreamListener listener) {
        ErrorHandling.throwUnimplemented(listener, "getStreamImportedKeys");
    }


    @Override
    public void getStreamCrossReference(FlightSql.CommandGetCrossReference command, CallContext context,
                                        ServerStreamListener listener) {
        ErrorHandling.throwUnimplemented(listener, "getStreamCrossReference");
    }


    @Override
    public void close() {
        ExecutorService streams = streamExecutors.streams();
        streams.shutdown();
        scheduledExecutorService.shutdown();
        try {
            if (!streams.awaitTermination(closeGrace.toMillis(), TimeUnit.MILLISECONDS)) {
                // Thread interrupts do not stop a query inside DuckDB: cancel the statements too,
                // so their streams end and give back their connections and Arrow memory.
                logger.atWarn().log("Queries still running after {}, cancelling them", closeGrace);
                cancelAllRunning();
                streams.shutdownNow();
                if (!streams.awaitTermination(10, TimeUnit.SECONDS)) {
                    logger.atWarn().log("Query streams did not stop within 10 seconds of being cancelled");
                }
            }
            // Streams have stopped (or been given up on): now the DuckDB pools they submitted to.
            for (ExecutorService pool : List.of(executorService, fetchExecutorService)) {
                pool.shutdown();
            }
            for (var pool : Map.of("execute", executorService, "fetch", fetchExecutorService).entrySet()) {
                if (!pool.getValue().awaitTermination(10, TimeUnit.SECONDS)) {
                    StreamExecutors.shutdownNowAndCancel(pool.getValue());
                    if (!pool.getValue().awaitTermination(10, TimeUnit.SECONDS)) {
                        logger.atWarn().log("DuckDB {} calls did not stop within 20 seconds of shutdown", pool.getKey());
                    }
                }
            }
            if (!scheduledExecutorService.awaitTermination(10, TimeUnit.SECONDS)) {
                logger.atWarn().log("ScheduledExecutorService did not terminate in 10 seconds, forcing shutdown");
                scheduledExecutorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            logger.atWarn().setCause(e).log("Interrupted while waiting for executor services to terminate");
            cancelAllRunning();
            streams.shutdownNow();
            StreamExecutors.shutdownNowAndCancel(executorService);
            StreamExecutors.shutdownNowAndCancel(fetchExecutorService);
            scheduledExecutorService.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Each step runs even if an earlier one fails, so one problem does not leak everything after it.
        // Cursors and prepared statements hold DuckDB connections; the removal listener closes each
        // (one still in use by a stream is closed when that stream ends).
        if (streams.isTerminated() && executorService.isTerminated() && fetchExecutorService.isTerminated()) {
            // No stream thread is left, so nothing can be using a context: close them all outright.
            // Deferring would leak those claimed by stream tasks that shutdownNow() dropped before
            // they ran, since no stream would ever end to close them.
            runQuietly("statement contexts", () -> {
                statementLoadingCache.asMap().values().forEach(StatementContext::close);
                preparedStatementLoadingCache.asMap().values().forEach(StatementContext::close);
            });
        }
        runQuietly("statement caches", () -> {
            statementLoadingCache.invalidateAll();
            preparedStatementLoadingCache.invalidateAll();
            statementLoadingCache.cleanUp();
            preparedStatementLoadingCache.cleanUp();
        });
        if (ingestionHandler != null) {
            runQuietly("ingestion queues", ingestionHandler::closeQueues);
        }
        // Shut down after the queues have drained/closed — draining does not depend on the scheduler,
        // but this keeps any in-flight flush trigger valid until the queues are gone.
        runQuietly("ingestion flush scheduler", bulkIngestFlushScheduler::shutdownNow);
        runQuietly("allocator", allocator::close);

        // Only this producer's own scratch directory: the location above it is shared.
        try (var stream = Files.walk(tempDir)) {
            stream.sorted(Comparator.reverseOrder())
                  .forEach(p -> { try { Files.deleteIfExists(p); } catch (IOException ignored) {} });
        } catch (IOException ignored) {}
    }

    /**
     * A uniquely named directory under {@code location}, which is created first if absent. The OTel
     * collector does the same for each of its signal services ({@code OtelCollectorServer
     * #createScratchDir}), so the two never share a directory to clean up.
     */
    private static Path createScratchDir(Path location) {
        try {
            Files.createDirectories(location);
            return Files.createTempDirectory(location, "flight-");
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Cannot create a scratch directory under temp_write_location " + location, e);
        }
    }

    /** This producer's own scratch directory; package-private for tests. */
    Path scratchDir() {
        return tempDir;
    }

    /** How long close() lets running queries finish before cancelling them; shortened by tests. */
    Duration closeGrace = Duration.ofSeconds(30);

    private void cancelAllRunning() {
        for (var cache : List.of(statementLoadingCache.asMap(), preparedStatementLoadingCache.asMap())) {
            for (StatementContext<?> ctx : cache.values()) {
                try {
                    ctx.cancel();
                } catch (Exception e) {
                    logger.atWarn().setCause(e).log("Failed to cancel a running query during shutdown");
                }
            }
        }
    }

    private static void runQuietly(String what, Runnable step) {
        try {
            step.run();
        } catch (Exception e) {
            logger.atWarn().setCause(e).log("Failed to close {} during shutdown", what);
        }
    }

    public SqlAuthorizer getSqlAuthorizer(){
        return sqlAuthorizer;
    }


    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        ErrorHandling.throwUnimplemented(listener, "listFlights");
    }


    /**
     * @return external locations which will be visible to client.
     * This can be the location of the producer it can be overwritten based on external hostname and port
     */
    public Location getServerLocation() {
        return serverLocation;
    }

    public synchronized Set<Location> getDataProcessorLocations(){
        return Set.copyOf(dataProcessorLocations);
    }

    public synchronized void setDataProcessorLocations(Collection<Location> dataProcessorLocations){
        var newLocations = new LinkedHashSet<Location>();
        newLocations.addAll(dataProcessorLocations);
        this.dataProcessorLocations = newLocations;
    }


    /** The cursor limits in effect; for tests. */
    CursorConfig getCursorConfig() {
        return cursorConfig;
    }

    /** Closes {@code connection}, logging instead of throwing: for cleanup on a failure path. */
    private static void closeQuietly(Connection connection) {
        try {
            connection.close();
        } catch (Exception e) {
            logger.atWarn().setCause(e).log("Failed to close connection");
        }
    }

    /**
     * Injects a live cursor entry into the cache on behalf of {@code peerIdentity}.
     * Visible for testing only — do not call from production code.
     */
    void injectTestCursor(String peerIdentity) {
        try {
            var conn = ConnectionPool.getConnection();
            var stmt = conn.createStatement();
            var ctx = new StatementContext<>(conn, stmt, "SELECT 1");
            var key = new CacheKey(peerIdentity, System.nanoTime());
            statementLoadingCache.put(key, ctx);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Enforces cursor limits before a new StatementContext is admitted.
     * Throws a RESOURCE_EXHAUSTED FlightRuntimeException if either the
     * server-wide total or the per-identity cap is exceeded.
     */
    /**
     * Closes cursors nobody will use: not claimed by a stream (or already finished) and idle for
     * longer than the cursor TTL. Runs on each new query, as the cache's own expiry did. A cursor
     * that is queued or running is never reaped, so it stays cancellable and counted in the cursor
     * limits however long its query runs.
     */
    void reapIdleCursors() {
        Duration ttl = Duration.ofMillis(cursorConfig.cursorTtlMs());
        statementLoadingCache.asMap().forEach((key, ctx) -> {
            if (ctx.idleLongerThan(ttl)) {
                statementLoadingCache.asMap().remove(key, ctx); // the removal listener closes it
            }
        });
    }

    private void enforceCursorLimits(String identity) {
        reapIdleCursors();
        long total = statementLoadingCache.size();
        if (total >= cursorConfig.maxCursorsTotal()) {
            throw CallStatus.RESOURCE_EXHAUSTED
                    .withDescription("Server cursor limit reached (%d/%d). Retry later."
                            .formatted(total, cursorConfig.maxCursorsTotal()))
                    .toRuntimeException();
        }
        long perIdentity = statementLoadingCache.asMap().keySet().stream()
                .filter(k -> k.peerIdentity().equals(identity))
                .count();
        if (perIdentity >= cursorConfig.maxCursorsPerIdentity()) {
            throw CallStatus.RESOURCE_EXHAUSTED
                    .withDescription("Too many open cursors for identity '%s' (%d/%d). Close or consume existing queries first."
                            .formatted(identity, perIdentity, cursorConfig.maxCursorsPerIdentity()))
                    .toRuntimeException();
        }
    }

    private static class StatementRemovalListener<T extends Statement>
            implements RemovalListener<CacheKey, StatementContext<T>> {
        @Override
        public void onRemoval(final RemovalNotification<CacheKey, StatementContext<T>> notification) {
            try {
                assert notification.getValue() != null;
                // Never pull the connection out from under a running stream, whether the entry was
                // evicted (TTL/size) or invalidated by a cancel: the stream thread may still be
                // inside DuckDB on it. The stream closes it when it ends instead.
                notification.getValue().closeWhenIdle();
            } catch (final Exception e) {
                logger.atWarn().setCause(e).log("Failed to close statement during cache removal");
            }
        }
    }

    private static ByteBuffer serializeMetadata(final Schema schema) {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            MessageSerializer.serialize(new WriteChannel(Channels.newChannel(outputStream)), schema);
            return ByteBuffer.wrap(outputStream.toByteArray());
        } catch (final IOException e) {
            throw new RuntimeException("Failed to serialize schema", e);
        }
    }

    protected <T extends Message> FlightInfo getFlightInfoForSchema(
            final T request, final FlightDescriptor descriptor, final Schema schema) {
        final Ticket ticket = new Ticket(pack(request).toByteArray());
        var locs = getDataProcessorLocations();
        final List<FlightEndpoint> endpoints = singletonList(new FlightEndpoint(ticket, locs.toArray(new Location[0])));
        return new FlightInfo(schema, descriptor, endpoints, -1, -1);
    }

    protected static DuckDBConnection getConnection(final CallContext context, AccessMode accessMode) throws NoSuchCatalogSchemaError {
        var databaseSchema = getDatabaseSchema(context, accessMode);
        String dbSchema = format("%s.%s", databaseSchema.database, databaseSchema.schema);
        List<String> sqls = new ArrayList<>();
        // Quoted: database and schema come from client headers in every mode except RESTRICTED, and
        // the setup batch runs every statement in a string, so an unquoted value such as
        // "memory.main; DELETE FROM t; USE memory" would run the DELETE before any authorization.
        sqls.add(format("USE %s.%s", quoteIdentifier(databaseSchema.database), quoteIdentifier(databaseSchema.schema)));
        // Session variables are read only from the verified (signed) claims, never from client
        // headers, so they cannot be overridden per-request. Applied as SET VARIABLE so queries and
        // injected RLS filters can read them via getvariable('name'). A malformed claim throws here
        // (before the connection is built) rather than failing silently.
        sqls.addAll(sessionSetupSqls(context));
        try {
            return ConnectionPool.getConnection(sqls.toArray(new String[0]));
        } catch (Exception e ){
            // Only the USE can mean "no such catalog/schema". The batch also carries the session
            // variables now, and reporting a failed SET VARIABLE as a missing schema sends the
            // caller looking in the wrong place.
            throw new NoSuchCatalogSchemaError(dbSchema, e);
        }
    }

    /**
     * The {@code SET VARIABLE} statements for this request's session variables.
     *
     * <p>{@link #getConnection} is not the only connection that evaluates the request's query: split
     * planning prunes partitions on its own connections, and the tree it prunes with already has the
     * row-level-security filter injected. A filter referencing {@code getvariable('x')} evaluates
     * against NULL on a connection that has not run these — pruning away every file and returning an
     * empty result — so every such connection must apply them too.
     */
    protected static List<String> sessionSetupSqls(CallContext context) {
        return SessionVariables.toSetStatements(
                getVerifiedClaims(context).get(Headers.CLAIM_SESSION_VARIABLES));
    }

    /** A SQL identifier in double quotes, with embedded quotes doubled, so it can only name an object. */
    static String quoteIdentifier(String identifier) {
        return '"' + identifier.replace("\"", "\"\"") + '"';
    }

    protected static DatabaseSchema getDatabaseSchema(CallContext context, AccessMode accessMode){
        var verifiedClaims = getVerifiedClaims(context);
        if (accessMode == AccessMode.RESTRICTED) {
            return makeDatabaseSchema(verifiedClaims.get(Headers.HEADER_DATABASE),
                    verifiedClaims.get(Headers.HEADER_SCHEMA));
        }
        CallHeaders headers = context.getMiddleware(FlightConstants.HEADER_KEY).headers();
        return makeDatabaseSchema(headers.get(Headers.HEADER_DATABASE),
                headers.get(Headers.HEADER_SCHEMA));
    }

    private static DatabaseSchema makeDatabaseSchema(String database, String schema) {
        if (schema == null) {
            schema = DEFAULT_SCHEMA;
        }
        if(database == null) {
            database = DEFAULT_DATABASE;
        }
        return new DatabaseSchema(database, schema);
    }

    //TODO Need to provide implementation
    protected static Map<String, String> getVerifiedClaims(CallContext context){
        AdvanceServerCallHeaderAuthMiddleware middleware = context.getMiddleware(AdvanceServerCallHeaderAuthMiddleware.KEY);
        if (middleware == null) {
            return Map.of();
        }
        return middleware.getAuthResultWithClaims().verifiedClaims();
    }

    protected static int getBatchSize(final CallContext context) {
        return ContextUtils.getValue(context, Headers.HEADER_FETCH_SIZE, Headers.DEFAULT_ARROW_FETCH_SIZE, Integer.class);
    }

    protected FlightInfo getFlightInfoStatement(String query,
                                      final CallContext context,
                                      final FlightDescriptor descriptor) {
        try (var connection = getConnection(context, getAccessMode())) {
            query = transformQuery(context, connection, query);
        } catch (UnauthorizedException e) {
            throw CallStatus.UNAUTHORIZED.withCause(e).withDescription(e.getMessage()).toRuntimeException();
        } catch (IllegalArgumentException e) {
            // A rejected request (unsupported LIMIT form, offset past the query's own bound) is a
            // client error. getStreamStatement already maps it via ErrorHandling; without this
            // branch the getFlightInfo half of the same call returned INTERNAL / HTTP 500.
            throw CallStatus.INVALID_ARGUMENT.withCause(e).withDescription(e.getMessage()).toRuntimeException();
        } catch (Exception e){
            throw CallStatus.INTERNAL.withCause(e).withDescription("Failed to transform query: " + e.getMessage()).toRuntimeException();
        }
        StatementHandle handle = newStatementHandle(query, -1, context);
        final ByteString serializedHandle =
                copyFrom(handle.serialize());
        FlightSql.TicketStatementQuery ticket =
                FlightSql.TicketStatementQuery.newBuilder().setStatementHandle(serializedHandle).build();
        return getFlightInfoForSchema(
                ticket, descriptor, null);
    }

    <T extends Message> FlightInfo getFlightInfoForSchema(
            final List<T> requests, final FlightDescriptor descriptor,
            final Schema schema, Collection<Location> locs) {
        var endpoints = requests.stream().map(request -> {
            var ticket = new Ticket(pack(request).toByteArray());
            return new FlightEndpoint(ticket, locs.toArray(new Location[0]));
        }).toList();
        return new FlightInfo(schema, descriptor, endpoints, -1, -1);
    }




    protected static long getSplitSize(CallContext callContext) {
        return ContextUtils.getValue(callContext, Headers.HEADER_SPLIT_SIZE, 0L, Long.class);
    }



    /** How long a statement ticket stays usable after it is issued, unless configured ({@code ticket_ttl_ms}). */
    public static final Duration DEFAULT_TICKET_TTL = Duration.ofHours(1);

    private volatile Duration ticketTtl = DEFAULT_TICKET_TTL;

    /**
     * Sets how long statement tickets issued from now on stay usable. Must be positive: tickets are
     * signed and skip authorization, so they always expire.
     */
    public void setTicketTtl(Duration ticketTtl) {
        this.ticketTtl = requirePositiveTicketTtl(ticketTtl);
    }

    static Duration requirePositiveTicketTtl(Duration ticketTtl) {
        if (ticketTtl == null || ticketTtl.isZero() || ticketTtl.isNegative()) {
            throw new IllegalArgumentException(ConfigConstants.TICKET_TTL_MS_KEY + " must be positive, got " + ticketTtl);
        }
        return ticketTtl;
    }

    public Duration getTicketTtl() {
        return ticketTtl;
    }

    /** A signed statement ticket, usable only by the caller and only until the ticket TTL passes. */
    protected StatementHandle newStatementHandle(String query, long splitSize, CallContext context) {
        return StatementHandle.newStatementHandle(query, producerId, splitSize)
                .signed(secretKey, context.peerIdentity(), clock.millis() + ticketTtl.toMillis());
    }

    /**
     * Whether a signed handle may NOT be used by this caller: a bad signature, issued to another
     * principal, or expired. Signed handles skip authorization, so this is what stops a leaked
     * ticket from being replayed by another user or indefinitely.
     */
    protected boolean invalidHandle(StatementHandle handle, CallContext context) {
        return !handle.validFor(secretKey, context.peerIdentity(), clock.millis());
    }

    protected static <T> T throwNotSupported(String operation) {
        throw new UnsupportedOperationException("Operation not supported: " + operation);
    }

}

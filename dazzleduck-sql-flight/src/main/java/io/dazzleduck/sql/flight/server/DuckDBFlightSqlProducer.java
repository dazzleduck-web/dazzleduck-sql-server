package io.dazzleduck.sql.flight.server;


import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.protobuf.*;
import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.common.auth.UnauthorizedException;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.Transformations;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.commons.authorization.SqlAuthorizer;
import io.dazzleduck.sql.commons.ingestion.*;
import io.dazzleduck.sql.commons.planner.SplitPlanner;
import io.dazzleduck.sql.flight.FlightRecorder;
import io.dazzleduck.sql.flight.MicroMeterFlightRecorder;
import io.dazzleduck.sql.flight.ingestion.IngestionParameters;
import io.dazzleduck.sql.flight.server.auth2.AdvanceServerCallHeaderAuthMiddleware;
import io.dazzleduck.sql.flight.stream.FlightStreamReader;
import io.dazzleduck.sql.micrometer.metrics.MetricsRegistryFactory;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.arrow.adapter.jdbc.JdbcParameterBinder;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.FlightSqlUtils;
import org.apache.arrow.flight.sql.SqlInfoBuilder;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBResultSet;
import org.duckdb.DuckDBResultSetMetaData;
import org.duckdb.StatementReturnType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
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
public class DuckDBFlightSqlProducer implements FlightSqlProducer, AutoCloseable, SimpleBulkIngestConsumer, SqlProducerMBean {

    public static final String TEMP_WRITE_FORMAT = "arrow";
    private final FlightRecorder recorder;
    private final Instant startTime;

    public static AccessMode getAccessMode(com.typesafe.config.Config appConfig) {
        return appConfig.hasPath("access_mode") ? AccessMode.valueOf(appConfig.getString("access_mode").toUpperCase()) : AccessMode.COMPLETE;
    }

    public static Path getTempWriteDir(com.typesafe.config.Config appConfig) throws IOException {
        var tempWriteDir = Path.of(appConfig.getString("temp_write_location"));
        if (!Files.exists(tempWriteDir)) {
            Files.createDirectories(tempWriteDir);
        }
        return tempWriteDir;
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
    public long getCompletedBulkIngest() {
        return recorder.getCompletedBulkIngest();
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
    public double getBytesIn() {
        return recorder.getBytesIn();
    }

    @Override
    public Instant getStartTime() {
        return startTime;
    }

    private static FlightRecorder buildRecorder(String producerId) {
        try {
            MeterRegistry registry = MetricsRegistryFactory.create();
            return new MicroMeterFlightRecorder(registry, producerId);
        } catch (Throwable t) {
            return new NOOPFlightRecorder();
        }
    }


    record DatabaseSchema ( String database, String schema) {}
    record CacheKey(String peerIdentity, long id){}

    protected static final Calendar DEFAULT_CALENDAR = JdbcToArrowUtils.getUtcCalendar();
    public static final String  DEFAULT_DATABASE = "memory";
    private final AccessMode accessMode;
    private final Set<Integer> supportedSqlInfo;
    private final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private final static Logger logger = LoggerFactory.getLogger(DuckDBFlightSqlProducer.class);
    private final Location location;
    private final String producerId;
    private final String secretKey;
    private final BufferAllocator allocator;
    private final String warehousePath;
    private final Cache<CacheKey, StatementContext<PreparedStatement>> preparedStatementLoadingCache;
    private final Cache<CacheKey, StatementContext<Statement>> statementLoadingCache;
    private final SqlAuthorizer sqlAuthorizer;

    private final SqlInfoBuilder sqlInfoBuilder;
    private final ConcurrentHashMap<String, BulkIngestQueue<String, IngestionResult>> ingestionQueueMap =
            new ConcurrentHashMap<>();

    private final PostIngestionTaskFactory postIngestionTaskFactory;

    private final Path tempDir;

    private final ScheduledExecutorService scheduledExecutorService;

    private final Duration queryTimeout;

    private final Clock clock;

    private final long CANCEL_TASK_INTERVAL_SECOND = 10;

    StreamListener<CancelStatus> streamListener = new StreamListener<>() {
        @Override
        public void onNext(CancelStatus val) {

        }

        @Override
        public void onError(Throwable t) {

        }

        @Override
        public void onCompleted() {

        }
    };

    public static DuckDBFlightSqlProducer createProducer(Location location,
                                                          String producerId,
                                                          String secretKey,
                                                          BufferAllocator allocator,
                                                          String warehousePath,
                                                          AccessMode accessMode,
                                                         PostIngestionTaskFactory postIngestionTaskFactory) {
        return new DuckDBFlightSqlProducer(location, producerId, secretKey, allocator, warehousePath, accessMode, newTempDir(), postIngestionTaskFactory, Executors.newSingleThreadScheduledExecutor(), Duration.ofMinutes(2));
    }

    public static Path newTempDir() {
        var dir = Path.of(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());
        if (!Files.exists(dir)) {
            try {
                Files.createDirectories(dir);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return dir;
    }

    public DuckDBFlightSqlProducer(Location location){
        this(location, UUID.randomUUID().toString());
    }

    public DuckDBFlightSqlProducer(Location location, String producerId) {
        this(location, producerId, "change me", new RootAllocator(),  System.getProperty("user.dir") + "/warehouse", AccessMode.COMPLETE, newTempDir()
        , PostIngestionTaskFactoryProvider.NO_OP.getPostIngestionTaskFactory(), Executors.newSingleThreadScheduledExecutor(), Duration.ofMinutes(2));
    }

    public DuckDBFlightSqlProducer(Location location,
                                   String producerId,
                                   String secretKey,
                                   BufferAllocator allocator,
                                   String warehousePath,
                                   AccessMode accessMode,
                                   Path tempDir,
                                   PostIngestionTaskFactory postIngestionTaskFactory,
                                   ScheduledExecutorService scheduledExecutorService,
                                   Duration queryTimeout) {
        this(location, producerId, secretKey, allocator, warehousePath, accessMode, tempDir, postIngestionTaskFactory,
                scheduledExecutorService, queryTimeout, Clock.systemDefaultZone(),
                 new NOOPFlightRecorder());

    }
    public DuckDBFlightSqlProducer(Location location,
                                   String producerId,
                                   String secretKey,
                                   BufferAllocator allocator,
                                   String warehousePath,
                                   AccessMode accessMode,
                                   Path tempDir,
                                   PostIngestionTaskFactory postIngestionTaskFactory,
                                   ScheduledExecutorService scheduledExecutorService,
                                   Duration queryTimeout,
                                   Clock clock,
                                   FlightRecorder recorder) {
        this.startTime = clock.instant();
        this.location = location;
        this.producerId = producerId;
        this.allocator = allocator;
        this.secretKey = secretKey;
        this.accessMode = accessMode;
        this.tempDir = tempDir;
        this.scheduledExecutorService = scheduledExecutorService;
        this.queryTimeout = queryTimeout;
        this.recorder = recorder;
        if(AccessMode.RESTRICTED ==  accessMode) {
            this.sqlAuthorizer = SqlAuthorizer.JWT_AUTHORIZER;
        } else {
            this.sqlAuthorizer = SqlAuthorizer.NOOP_AUTHORIZER;
        }

        this.postIngestionTaskFactory = postIngestionTaskFactory;
        preparedStatementLoadingCache =
                CacheBuilder.newBuilder()
                        .maximumSize(4000)
                        .expireAfterAccess(10, TimeUnit.MINUTES)
                        .removalListener(new StatementRemovalListener<PreparedStatement>())
                        .build();
        statementLoadingCache =
                CacheBuilder.newBuilder()
                        .maximumSize(4000)
                        .expireAfterWrite(10, TimeUnit.MINUTES)
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


            var providerField =  sqlInfoBuilder.getClass().getDeclaredField("providers");
            providerField.setAccessible(true);
            supportedSqlInfo = ((HashMap<Integer, ?>)providerField.get(sqlInfoBuilder)).keySet();
            scheduledExecutorService.scheduleWithFixedDelay(() -> {
                var now = clock.instant();
                preparedStatementLoadingCache.asMap().forEach((key, ctx) -> {
                    if (ctx.startTime().plus(queryTimeout).isBefore(now)) {
                        cancel(key.id, streamListener, key.peerIdentity);
                    }
                });

                statementLoadingCache.asMap().forEach((key, ctx) -> {
                    if (ctx.startTime().plus(queryTimeout).isBefore(now)) {
                        cancel(key.id, streamListener, key.peerIdentity);
                    }
                });
            }, 0, CANCEL_TASK_INTERVAL_SECOND, TimeUnit.SECONDS);
        } catch (SQLException | NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getProducerId() {
        return producerId;
    }

    @Override
    public void createPreparedStatement(FlightSql.ActionCreatePreparedStatementRequest request, final CallContext context, StreamListener<Result> listener) {
        if (checkAccessModeAndRespond(listener)) {
            return;
        }

        // Running on another thread
        final Connection connection;
        try {
            connection = getConnection(context, accessMode);
        } catch (Throwable t ) {
            ErrorHandling.handleThrowable(listener, t);
            return;
        }

        String authorizedSql = request.getQuery();
        StatementHandle handle = newStatementHandle(authorizedSql);
        var cacheKey = new CacheKey(context.peerIdentity(), handle.queryId());

        Runnable runnable = () -> {
            try {
                final ByteString serializedHandle =
                        copyFrom(handle.serialize());
                // Ownership of the connection will be passed to the context. Do NOT close!

                final PreparedStatement preparedStatement =
                        connection.prepareStatement(
                                authorizedSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                final StatementContext<PreparedStatement> preparedStatementContext =
                        new StatementContext<>(preparedStatement, authorizedSql);
                preparedStatementLoadingCache.put(
                        cacheKey, preparedStatementContext);

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
                    var x = JdbcToArrowUtils.jdbcToArrowSchema(metaData, DEFAULT_CALENDAR);
                    bytes = ByteString.copyFrom(
                            serializeMetadata(x));
                }
                builder.setDatasetSchema(bytes);
                final FlightSql.ActionCreatePreparedStatementResult result = builder.build();
                listener.onNext(new Result(pack(result).toByteArray()));
            } catch (Throwable e ) {
                ErrorHandling.handleThrowable(listener, e);
                return;
            }
            listener.onCompleted();
        };
        executorService.submit(runnable);
    }

    @Override
    public void closePreparedStatement(FlightSql.ActionClosePreparedStatementRequest request, CallContext context, StreamListener<Result> listener) {
        if (checkAccessModeAndRespond(listener)) {
            return;
        }
        final StatementHandle statementHandle = StatementHandle.deserialize(request.getPreparedStatementHandle());
        if (statementHandle.signatureMismatch(secretKey)) {
            ErrorHandling.handleSignatureMismatch(listener);
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
        checkAccessModeAndRespond();
        StatementHandle statementHandle = StatementHandle.deserialize(command.getPreparedStatementHandle());
        if (statementHandle.signatureMismatch(secretKey)) {
            ErrorHandling.handleSignatureMismatch();
        }

        var key = new CacheKey(context.peerIdentity(), statementHandle.queryId());
        StatementContext<PreparedStatement> statementContext =
                preparedStatementLoadingCache.getIfPresent(key);
        if (statementContext == null) {
            ErrorHandling.handleContextNotFound();
        }
        return getFlightInfoForSchema(command, descriptor, null);
    }


    public FlightInfo getFlightInfoStatementFromQuery(final String query, final CallContext context, final FlightDescriptor descriptor){
        var parallelize = parallelize(context);
        if (!parallelize && AccessMode.COMPLETE == accessMode) {
            return getFlightInfoStatement(query, context, descriptor);
        }

        JsonNode tree = null;
        try {
            tree = Transformations.parseToTree(query);
            if (tree.get("error").asBoolean()) {
                ErrorHandling.handleQueryCompilationError(tree);
            }
        } catch (Throwable s) {
            ErrorHandling.handleThrowable(s);
        }

        if (AccessMode.RESTRICTED == accessMode) {
            JsonNode restrictedTree = null;
            try {
                restrictedTree = authorize(context, tree);
            } catch (UnauthorizedException e) {
                ErrorHandling.handleUnauthorized(e);
            } catch (Throwable e) {
                ErrorHandling.handleThrowable(e);
            }
            tree = restrictedTree;
        }

        if (parallelize) {
            return getFlightInfoStatementSplittable(tree, context, descriptor);
        } else {
            try {
                var newSql = Transformations.parseToSql(tree);
                return getFlightInfoStatement(newSql, context, descriptor);
            } catch (SQLException e) {
                throw ErrorHandling.handleSqlException(e);
            }
        }
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
        return null;
    }


    @Override
    public void getStreamPreparedStatement(FlightSql.CommandPreparedStatementQuery command, CallContext context,
                                           ServerStreamListener listener) {
       if (checkAccessModeAndRespond(listener)){
           return;
       }

        StatementHandle statementHandle = StatementHandle.deserialize(command.getPreparedStatementHandle());
        if (statementHandle.signatureMismatch(secretKey)) {
            ErrorHandling.handleSignatureMismatch(listener);
        }
        var key = new CacheKey(context.peerIdentity(), statementHandle.queryId());
        StatementContext<PreparedStatement> statementContext =
            preparedStatementLoadingCache.getIfPresent(key);
        if (statementContext == null) {
            ErrorHandling.handleContextNotFound();
        }
        streamResultSet(executorService, statementContext, OptionalResultSetSupplier.of(statementContext.getStatement()),
            allocator, getBatchSize(context),
            listener, () -> {}, recorder);
    }


    @Override
    public void getStreamStatement(
            final FlightSql.TicketStatementQuery ticketStatementQuery,
            final CallContext context,
            final ServerStreamListener listener) {
        StatementHandle statementHandle = StatementHandle.deserialize(ticketStatementQuery.getStatementHandle());
        if (statementHandle.signatureMismatch(secretKey)) {
            ErrorHandling.handleSignatureMismatch(listener);
            return;
        }
        getStreamStatement(statementHandle, context, listener);
    }

    public void getStreamStatement(
            StatementHandle statementHandle,
            final CallContext context,
            final ServerStreamListener listener) {
        try {
            var connection = getConnection(context, accessMode);
            Statement statement = connection.createStatement();
            var statementContext = new StatementContext<>(statement, statementHandle.query());
            var key = new CacheKey(context.peerIdentity(), statementHandle.queryId());
            statementLoadingCache.put(key, statementContext);
            streamResultSet(executorService,
                    statementContext,
                    OptionalResultSetSupplier.of(statement, statementHandle.query()),
                    allocator,
                    getBatchSize(context),
                    listener,
                    () -> statementLoadingCache.invalidate(key), recorder);
        } catch (Throwable e) {
            ErrorHandling.handleThrowable(listener, e);
        }
    }


    @Override
    public Runnable acceptPutStatement(FlightSql.CommandStatementUpdate command, CallContext context,
                                       FlightStream flightStream, StreamListener<PutResult> ackStream) {

        final String query = command.getQuery();
        return () -> {
            if (checkAccessModeAndRespond(ackStream)) {
                ackStream.onError(new UnauthorizedException("access mode not supported"));
                return;
            }
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
            if (checkAccessModeAndRespond(ackStream)) {
                return;
            }
            StatementHandle statementHandle = StatementHandle.deserialize(command.getPreparedStatementHandle());
            if (statementHandle.signatureMismatch(secretKey)) {
                ErrorHandling.handleSignatureMismatch(ackStream);
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
            }
        };
    }


    @Override
    public Runnable acceptPutPreparedStatementQuery(FlightSql.CommandPreparedStatementQuery command,
                                                    CallContext context, FlightStream flightStream,
                                                    StreamListener<PutResult> ackStream) {
       return () -> ErrorHandling.handleUnimplemented(ackStream, "acceptPutPreparedStatementQuery");
    }


    @Override
    public Runnable acceptPutStatementBulkIngest(
            FlightSql.CommandStatementIngest command,
            CallContext context,
            FlightStream flightStream,
            StreamListener<PutResult> ackStream) {
        if (checkAccessModeAndRespond(ackStream)) {
           return () -> ackStream.onError(new UnauthorizedException("Access Mode not support ingestion"));
        }
        IngestionParameters ingestionParameters = IngestionParameters.getIngestionParameters(command);
        FlightStreamReader reader = FlightStreamReader.of(flightStream, allocator);
        return acceptPutStatementBulkIngest(context, ingestionParameters, reader, ackStream);
    }

    @Override
    public Runnable acceptPutStatementBulkIngest(
            CallContext context,
            IngestionParameters ingestionParameters,
            ArrowReader inputReader,
            StreamListener<PutResult> ackStream) {
        return () -> {
            Path tempFile;
            try (inputReader) {
                tempFile = BulkIngestQueue.writeAndValidateTempArrowFile(tempDir, inputReader);
                var batch = ingestionParameters.constructBatch(Files.size(tempFile), tempFile.toAbsolutePath().toString());
                var ingestionQueue = ingestionQueueMap.computeIfAbsent(ingestionParameters.completePath(warehousePath), p -> {
                    return new ParquetIngestionQueue(producerId, TEMP_WRITE_FORMAT, p, p,
                            IngestionParameters.DEFAULT_MAX_BUCKET_SIZE,
                            IngestionParameters.DEFAULT_MAX_DELAY,
                            postIngestionTaskFactory,
                            Executors.newSingleThreadScheduledExecutor(),
                            Clock.systemDefaultZone());
                });
                var result = ingestionQueue.addToQueue(batch);
                result.get();
                ackStream.onNext(PutResult.empty());
                ackStream.onCompleted();
            } catch (Throwable throwable) {
                ErrorHandling.handleThrowable(ackStream, throwable);
            }
        };
    }

    @Override
    public void cancelFlightInfo(
            CancelFlightInfoRequest request, CallContext context, StreamListener<CancelStatus> listener) {
        Ticket ticket = request.getInfo().getEndpoints().get(0).getTicket();
        final Any command;
        try {
            command = Any.parseFrom(ticket.getBytes());
        } catch (InvalidProtocolBufferException e) {
            listener.onError(e);
            return;
        }
        if (command.is(FlightSql.TicketStatementQuery.class)) {
            cancelStatement(
                    FlightSqlUtils.unpackOrThrow(command, FlightSql.TicketStatementQuery.class), context, listener);
        } else if (command.is(FlightSql.CommandPreparedStatementQuery.class)) {
            cancelPreparedStatement(
                    FlightSqlUtils.unpackOrThrow(command, FlightSql.CommandPreparedStatementQuery.class),
                    context,
                    listener);
        }
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
        checkAccessModeAndRespond();
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_CATALOGS_SCHEMA);
    }

    @Override
    public void getStreamCatalogs(final CallContext context, final ServerStreamListener listener) {
        if (checkAccessModeAndRespond(listener)) {
            return;
        }
        streamResultSet(executorService, DuckDBDatabaseMetadataUtil::getCatalogs, context, accessMode, allocator, listener, recorder);
    }

    @Override
    public FlightInfo getFlightInfoSchemas(FlightSql.CommandGetDbSchemas request, CallContext context,
                                           FlightDescriptor descriptor) {
        checkAccessModeAndRespond();
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_SCHEMAS_SCHEMA);
    }

    @Override
    public void getStreamSchemas(FlightSql.CommandGetDbSchemas command, CallContext context, ServerStreamListener listener) {
        if (checkAccessModeAndRespond(listener)) {
            return;
        }
        final String catalog = command.hasCatalog() ? command.getCatalog() : null;
        final String schemaFilterPattern =
                command.hasDbSchemaFilterPattern() ? command.getDbSchemaFilterPattern() : null;
        streamResultSet(executorService, connection ->
                        DuckDBDatabaseMetadataUtil.getSchemas(connection, catalog, schemaFilterPattern),
                context, accessMode, allocator, listener, recorder);
    }

    @Override
    public FlightInfo getFlightInfoTables(
            final FlightSql.CommandGetTables request,
            final CallContext context,
            final FlightDescriptor descriptor) {
        checkAccessModeAndRespond();
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
        if (checkAccessModeAndRespond(listener)) {
            return;
        }

        final String catalog = command.hasCatalog() ? command.getCatalog() : null;
        final String schemaFilterPattern =
                command.hasDbSchemaFilterPattern() ? command.getDbSchemaFilterPattern() : null;
        final String tableFilterPattern =
                command.hasTableNameFilterPattern() ? command.getTableNameFilterPattern() : null;
        final ProtocolStringList protocolStringList = command.getTableTypesList();
        final int protocolSize = protocolStringList.size();
        final String[] tableTypes =
                protocolSize == 0 ? null : protocolStringList.toArray(new String[protocolSize]);
        streamResultSet(executorService, connection ->
            DuckDBDatabaseMetadataUtil.getTables(connection, catalog, schemaFilterPattern, tableFilterPattern, tableTypes),
                context, accessMode, allocator, listener, recorder);
    }

    @Override
    public FlightInfo getFlightInfoTableTypes(FlightSql.CommandGetTableTypes request, CallContext context,
                                              FlightDescriptor descriptor) {
        checkAccessModeAndRespond();
        return getFlightInfoForSchema(request, descriptor, Schemas.GET_TABLE_TYPES_SCHEMA);
    }


    @Override
    public void getStreamTableTypes(CallContext context, ServerStreamListener listener) {
        if (checkAccessModeAndRespond(listener)) {
            return;
        }
        streamResultSet(executorService, DuckDBDatabaseMetadataUtil::getTableTypes, context, accessMode, allocator, listener, recorder);
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
    public void close()  {
        executorService.shutdown();
        allocator.close();

    }


    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        ErrorHandling.throwUnimplemented(listener, "listFlights");
    }


    public Location getLocation(){
        return this.location;
    }

    private void cancelStatement(final FlightSql.TicketStatementQuery ticketStatementQuery,
                                 CallContext context,
                                 StreamListener<CancelStatus> listener) {
        StatementHandle statementHandle = StatementHandle.deserialize(ticketStatementQuery.getStatementHandle());
        cancel(statementHandle.queryId(), listener, context.peerIdentity());
    }


    private void cancelPreparedStatement(FlightSql.CommandPreparedStatementQuery ticketPreparedStatementQuery,
                                         CallContext context,
                                         StreamListener<CancelStatus> listener) {
        final StatementHandle statementHandle = StatementHandle.deserialize(ticketPreparedStatementQuery.getPreparedStatementHandle());
        cancel(statementHandle.queryId(), listener, context.peerIdentity());
    }

    @Override
    public void cancel(Long queryId,
                       StreamListener<CancelStatus> listener,
                       String peerIdentity) {
        var key = new CacheKey(peerIdentity, queryId);
        StatementContext<?> context = getStatementContext(key);

        if (context == null) {
            ErrorHandling.handleContextNotFound(listener);
            return;
        }
        try {
            Statement statement = context.getStatement();
            listener.onNext(CancelStatus.CANCELLING);
            try {
                statement.cancel();
                listener.onNext(CancelStatus.CANCELLED);
            } catch (SQLException e) {
                ErrorHandling.handleSqlException(listener, e);
            }
        } finally {
            listener.onCompleted();
            invalidateCache(key, context);
        }
    }

    private StatementContext<?> getStatementContext(CacheKey key) {
        StatementContext<?> context = statementLoadingCache.getIfPresent(key);
        if (context == null) {
            context = preparedStatementLoadingCache.getIfPresent(key);
        }
        return context;
    }

    private void invalidateCache(CacheKey key, StatementContext<?> context) {
        if (context.getStatement() instanceof PreparedStatement) {
            preparedStatementLoadingCache.invalidate(key);
        } else {
            statementLoadingCache.invalidate(key);
        }
    }


    private static class StatementRemovalListener<T extends Statement>
            implements RemovalListener<CacheKey, StatementContext<T>> {
        @Override
        public void onRemoval(final RemovalNotification<CacheKey, StatementContext<T>> notification) {
            try {
                assert notification.getValue() != null;
                notification.getValue().close();
            } catch (final Exception e) {
                // swallow
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
        // TODO Support multiple endpoints.
        final List<FlightEndpoint> endpoints = singletonList(new FlightEndpoint(ticket, location));
        return new FlightInfo(schema, descriptor, endpoints, -1, -1);
    }

    private static DuckDBConnection getConnection(final CallContext context, AccessMode accessMode) throws NoSuchCatalogSchemaError {
        var databaseSchema = getDatabaseSchema(context, accessMode);
        String dbSchema = format("%s.%s", databaseSchema.database, databaseSchema.schema);
        String[] sqls = {format("USE %s", dbSchema)};
        try {
            return ConnectionPool.getConnection(sqls);
        } catch (Exception e ){
            throw new NoSuchCatalogSchemaError(dbSchema);
        }
    }

    private static DatabaseSchema getDatabaseSchema(CallContext context, AccessMode accessMode){
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
    private static Map<String, String> getVerifiedClaims(CallContext context){
        AdvanceServerCallHeaderAuthMiddleware middleware = context.getMiddleware(AdvanceServerCallHeaderAuthMiddleware.KEY);
        if (middleware == null) {
            return Map.of();
        }
        return middleware.getAuthResultWithClaims().verifiedClaims();
    }

    private static int getBatchSize(final CallContext context) {
        return ContextUtils.getValue(context, Headers.HEADER_FETCH_SIZE, Headers.DEFAULT_ARROW_FETCH_SIZE, Integer.class);
    }

    private interface ResultSetSupplierFromConnection {
        DuckDBResultSet get(DuckDBConnection connection) throws SQLException;
    }

    private interface ResultSetSupplier {
        DuckDBResultSet get() throws SQLException;
    }

    private static void streamResultSet(ExecutorService executorService,
                                        ResultSetSupplierFromConnection supplier,
                                        CallContext context, AccessMode accessMode,
                                        BufferAllocator allocator,
                                        final ServerStreamListener listener, FlightRecorder recorder) {

        streamResultSet(executorService, supplier, context, accessMode, allocator, listener, () -> {}, recorder);
    }
    private static void streamResultSet( ExecutorService executorService,
                                         ResultSetSupplierFromConnection supplier,
                                         CallContext context,
                                         AccessMode  accessMode,
                                         BufferAllocator allocator,
                                         final ServerStreamListener listener,
                                         Runnable finalBlock,
                                         FlightRecorder recorder) {
        try {
            DuckDBConnection connection = getConnection(context, accessMode );
            streamResultSet(executorService,
                    () -> supplier.get(connection),
                    allocator,
                    getBatchSize(context),
                    listener,
                    () -> {
                        try {
                            connection.close();
                        } catch (SQLException e) {
                            logger.atError().setCause(e).log("Error closing connection");
                        }
                        finalBlock.run();
                    }, recorder);
        } catch (Throwable t) {
            ErrorHandling.handleThrowable(listener, t);
        }
    }

    private static void streamResultSet(ExecutorService executorService,
                                        ResultSetSupplier supplier,
                                        BufferAllocator allocator,
                                        final int batchSize,
                                        final ServerStreamListener listener,
                                        Runnable finalBlock,
                                        FlightRecorder recorder) {
        var childAllocator = allocator.newChildAllocator("statement-allocator", 0, allocator.getLimit());
        executorService.submit(() -> {
            var error = false ;
            recorder.startStream(false);
            try (DuckDBResultSet resultSet = supplier.get();
                 ArrowReader reader = (ArrowReader) resultSet.arrowExportStream(childAllocator, batchSize)) {
                listener.start(reader.getVectorSchemaRoot());
                while (reader.loadNextBatch()) {
                    var size = childAllocator.getAllocatedMemory();
                    recorder.recordGetStream(false,
                            size);
                    listener.putNext();
                }
            }  catch (Throwable throwable) {
                recorder.errorStream(false);
                ErrorHandling.handleThrowable(listener, throwable);
            } finally {
                if (!error) {
                    listener.completed();
                }
                recorder.endStream(false);
                finalBlock.run();
            }
        });
    }

    private static <T extends Statement> void streamResultSet(ExecutorService executorService,
                                                              StatementContext<T> statementContext,
                                                              OptionalResultSetSupplier supplier,
                                                              BufferAllocator allocator,
                                                              final int batchSize,
                                                              final ServerStreamListener listener,
                                                              Runnable finalBlock, FlightRecorder recorder) {

        var childAllocator = allocator.newChildAllocator("statement-allocator", 0, allocator.getLimit());
        executorService.submit(() -> {
            var error = false;
            try {
                statementContext.start();
                recorder.startStream(statementContext.isPreparedStatementContext());
                supplier.execute();
                if (supplier.hasResultSet()) {
                    try (DuckDBResultSet resultSet = supplier.get();
                         ArrowReader reader = (ArrowReader) resultSet.arrowExportStream(childAllocator, batchSize)) {
                        listener.start(reader.getVectorSchemaRoot());
                        while (reader.loadNextBatch()) {
                            listener.putNext();
                            var size = childAllocator.getAllocatedMemory();
                            statementContext.bytesOut(size);
                            recorder.recordGetStream(statementContext.isPreparedStatementContext(),
                                    size);
                        }
                    }
                } else {
                    listener.start(new VectorSchemaRoot(List.of()));
                }
            } catch (Throwable throwable) {
                error = true;
                recorder.errorStream(statementContext.isPreparedStatementContext());
                ErrorHandling.handleThrowable(listener, throwable);
            } finally {
                if (!error) {
                    listener.completed();
                }
                statementContext.end();
                recorder.endStream(statementContext.isPreparedStatementContext());
                finalBlock.run();
                childAllocator.close();
            }
        });
    }

    private FlightInfo getFlightInfoStatement(String query,
                                      final CallContext context,
                                      final FlightDescriptor descriptor) {
        StatementHandle handle = newStatementHandle(query);
        final ByteString serializedHandle =
                copyFrom(handle.serialize());
        FlightSql.TicketStatementQuery ticket =
                FlightSql.TicketStatementQuery.newBuilder().setStatementHandle(serializedHandle).build();
        return getFlightInfoForSchema(
                ticket, descriptor, null);
    }

    private FlightInfo getFlightInfoStatementSplittable(JsonNode tree,
            final CallContext context,
            final FlightDescriptor descriptor) {
        try {
            var splitSize = getSplitSize(context);
            var splits = SplitPlanner.getSplitTreeAndSize(tree, splitSize);

            var list = splits.stream().map(split -> {
                try {
                    var sql = Transformations.parseToSql(split.tree());
                    StatementHandle handle = newStatementHandle(sql, split.size());
                    final ByteString serializedHandle =
                            copyFrom(handle.serialize());
                    return FlightSql.TicketStatementQuery.newBuilder().setStatementHandle(serializedHandle).build();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }).toList();
            return getFlightInfoForSchema(list, descriptor, null, getLocation());
        } catch (Throwable throwable) {
            ErrorHandling.handleThrowable(throwable);
            return null;
        }
    }

    private <T extends Message> FlightInfo getFlightInfoForSchema(
            final List<T> requests, final FlightDescriptor descriptor,
            final Schema schema, Location location) {
        var endpoints = requests.stream().map(request -> {
            var ticket = new Ticket(pack(request).toByteArray());
            return new FlightEndpoint(ticket, location);
        }).toList();
        return new FlightInfo(schema, descriptor, endpoints, -1, -1);
    }

    private boolean parallelize(CallContext context) {
        return getSplitSize(context) > 0;
    }


    private static long getSplitSize(CallContext callContext) {
        return ContextUtils.getValue(callContext, Headers.HEADER_SPLIT_SIZE, 0L, Long.class);
    }

    private JsonNode authorize(CallContext callContext, JsonNode sql) throws UnauthorizedException {
        String peerIdentity = callContext.peerIdentity();
        var verifiedClaims = getVerifiedClaims(callContext);
        var databaseSchema = getDatabaseSchema(callContext, accessMode);
        return sqlAuthorizer.authorize(peerIdentity, databaseSchema.database, databaseSchema.schema, sql, verifiedClaims);
    }

    protected StatementHandle newStatementHandle(String query, long splitSize) {
        return StatementHandle.newStatementHandle(query, producerId, splitSize).signed(secretKey);
    }

    protected StatementHandle newStatementHandle(String query) {
        return newStatementHandle(query, -1);
    }

    private FlightRuntimeException handleInconsistentRequest(String s) {
        return CallStatus.INTERNAL.withDescription(s).toRuntimeException();
    }

    private  boolean checkAccessModeAndRespond(ServerStreamListener listener) {
        if (accessMode == AccessMode.RESTRICTED) {
            ErrorHandling.handleUnauthorized(listener, new UnauthorizedException("Close Prepared Statement"));
            return true;
        }
        return false;
    }

    private <T> boolean checkAccessModeAndRespond(StreamListener<T> listener) {
        if (accessMode == AccessMode.RESTRICTED) {
            ErrorHandling.handleUnauthorized(listener, new UnauthorizedException("Close Prepared Statement"));
            return true;
        }
        return false;
    }

    private void checkAccessModeAndRespond() {
        if (accessMode == AccessMode.RESTRICTED) {
            throw ErrorHandling.handleUnauthorized(new UnauthorizedException("Get FlightInfo Prepared Statement"));
        }
    }
}

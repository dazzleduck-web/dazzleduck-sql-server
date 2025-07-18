package io.dazzleduck.sql.flight.server;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.protobuf.*;
import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.common.UnauthorizedException;
import io.dazzleduck.sql.common.authorization.AccessMode;
import io.dazzleduck.sql.common.authorization.NOOPAuthorizer;
import io.dazzleduck.sql.common.FlightStreamReader;
import io.dazzleduck.sql.common.authorization.SqlAuthorizer;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.FileStatus;
import io.dazzleduck.sql.commons.Transformations;
import io.dazzleduck.sql.commons.planner.SplitPlanner;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.FlightSqlUtils;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.protobuf.Any.pack;
import static com.google.protobuf.ByteString.copyFrom;
import static java.util.Collections.singletonList;
import static java.util.Objects.isNull;
import static org.duckdb.DuckDBConnection.DEFAULT_SCHEMA;

/**
 * It's a simple implementation which support most of the construct for reading as well as bulk writing to parquet file.
 * For now only property which is supported is database, schema and fetch size which are supplied as the connection parameter
 * and available in the header. More options will be supported in the future version.
 * Future implementation note for statement we check if its SET or RESET statement and based on that use cookies to set unset the values
 */
public class DuckDBFlightSqlProducer implements FlightSqlProducer, AutoCloseable {
    protected static final Calendar DEFAULT_CALENDAR = JdbcToArrowUtils.getUtcCalendar();
    public static final String  DEFAULT_DATABASE = "memory";
    private final AccessMode accessMode;
    ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
    private final static Logger logger = LoggerFactory.getLogger(DuckDBFlightSqlProducer.class);
    private final Location location;
    private final String producerId;
    private final String secretKey;
    private final BufferAllocator allocator;
    private final String warehousePath;
    final private static AtomicLong sqlIdCounter = new AtomicLong();
    private final Cache<Long, StatementContext<PreparedStatement>> preparedStatementLoadingCache;
    private final Cache<Long, StatementContext<Statement>> statementLoadingCache;
    private final SqlAuthorizer sqlAuthorizer;

    public DuckDBFlightSqlProducer(Location location){
        this(location, UUID.randomUUID().toString());
    }

    public DuckDBFlightSqlProducer(Location location, String producerId) {
        this(location, producerId, "change me", new RootAllocator(),  System.getProperty("user.dir") + "/warehouse", AccessMode.COMPLETE, new NOOPAuthorizer());
    }

    public DuckDBFlightSqlProducer(Location location,
                                   String producerId,
                                   String secretKey,
                                   BufferAllocator allocator,
                                   String warehousePath,
                                   AccessMode accessMode,
                                   SqlAuthorizer sqlAuthorizer) {
        this.location = location;
        this.producerId = producerId;
        this.allocator = allocator;
        this.secretKey = secretKey;
        this.accessMode = accessMode;
        this.sqlAuthorizer = sqlAuthorizer;

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
    }

    @Override
    public void createPreparedStatement(FlightSql.ActionCreatePreparedStatementRequest request, final CallContext context, StreamListener<Result> listener) {
        if (checkAccessModeAndRespond(listener)) {
            return;
        }

        // Running on another thread
        final Connection connection;
        try {
            connection = getConnection(context);
        } catch (NoSuchCatalogSchemaError e) {
            handleNoSuchDBSchema(listener, e);
            return;
        }
        String authorizedSql = request.getQuery();
        StatementHandle handle = newStatementHandle(authorizedSql);
        Future<?> unused =
                executorService.submit(
                        () -> {
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
                                        handle.queryId(), preparedStatementContext);

                                final Schema parameterSchema =
                                        JdbcToArrowUtils.jdbcToArrowSchema(preparedStatement.getParameterMetaData(), DEFAULT_CALENDAR);

                                final ResultSetMetaData metaData = preparedStatement.getMetaData();
                                final ByteString bytes =
                                        isNull(metaData)
                                                ? ByteString.EMPTY
                                                : ByteString.copyFrom(
                                                serializeMetadata(JdbcToArrowUtils.jdbcToArrowSchema(metaData, DEFAULT_CALENDAR)));
                                final FlightSql.ActionCreatePreparedStatementResult result =
                                        FlightSql.ActionCreatePreparedStatementResult.newBuilder()
                                                .setDatasetSchema(bytes)
                                                .setParameterSchema(copyFrom(serializeMetadata(parameterSchema)))
                                                .setPreparedStatementHandle(serializedHandle)
                                                .build();
                                listener.onNext(new Result(pack(result).toByteArray()));
                            } catch (final SQLException e) {
                               handleSqlException(listener, e);
                            }
                            listener.onCompleted();
                        });

    }

    @Override
    public void closePreparedStatement(FlightSql.ActionClosePreparedStatementRequest request, CallContext context, StreamListener<Result> listener) {
        if (checkAccessModeAndRespond(listener)) {
            return;
        }
        final StatementHandle statementHandle = StatementHandle.deserialize(request.getPreparedStatementHandle());
        if (statementHandle.signatureMismatch(secretKey)) {
            handleSignatureMismatch(listener);
            return;
        }
        // Running on another thread
        Future<?> unused =
                executorService.submit(
                        () -> {
                            try {
                                preparedStatementLoadingCache.invalidate(statementHandle.queryId());
                            } catch (final Exception e) {
                                listener.onError(e);
                                return;
                            }
                            listener.onCompleted();
                        });
    }


    @Override
    public FlightInfo getFlightInfoPreparedStatement(
            final FlightSql.CommandPreparedStatementQuery command,
            final CallContext context,
            final FlightDescriptor descriptor) {
        checkAccessModeAndRespond();
        StatementHandle statementHandle = StatementHandle.deserialize(command.getPreparedStatementHandle());
        if (statementHandle.signatureMismatch(secretKey)) {
            handleSignatureMismatch();
        }
        StatementContext<PreparedStatement> statementContext =
                preparedStatementLoadingCache.getIfPresent(statementHandle.queryId());
        if (statementContext == null) {
            handleContextNotFound();
        }
        return getFlightInfoForSchema(command, descriptor, null);
    }

    @Override
    public FlightInfo getFlightInfoStatement(
            final FlightSql.CommandStatementQuery request,
            final CallContext context,
            final FlightDescriptor descriptor) {
        String query = request.getQuery();
        JsonNode tree = null;
        if( parallelize(context) && AccessMode.RESTRICTED != accessMode ) {
            throw handleInconsistentRequest("parallelization only supported in restricted mode");
        }

        if (AccessMode.RESTRICTED == accessMode) {
            try {
                tree = Transformations.parseToTree(query);
                if (tree.get("error").asBoolean()) {
                    handleQueryCompilationError(tree);
                }
            } catch (SQLException s) {
                throw handleSqlException(s);
            } catch (JsonProcessingException e) {
                throw handleJsonProcessingException(e);
            }
        }

        if (AccessMode.RESTRICTED == accessMode) {
            JsonNode newTree = null;
            try {
                newTree = authorize(context, tree);
            } catch (UnauthorizedException e) {
                throw handleUnauthorized(e);
            }

            if (parallelize(context)) {
                return getFlightInfoStatementSplittable(newTree, context, descriptor);
            } else {
                try {
                    var newSql = Transformations.parseToSql(newTree);
                    return getFlightInfoStatement(newSql, context, descriptor);
                } catch (SQLException e) {
                    throw handleSqlException(e);
                }
            }
        } else {
            return getFlightInfoStatement(query, context, descriptor);
        }
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
            handleSignatureMismatch(listener);
        }
        StatementContext<PreparedStatement> statementContext =
                preparedStatementLoadingCache.getIfPresent(statementHandle.queryId());
        if (statementContext == null) {
            handleContextNotFound();
        }

        PreparedStatement statementToUse = statementContext.getStatement();
        try {
            Optional<String> overrideQuery = getOverrideQuery(context, statementContext.getQuery());
            if (overrideQuery.isPresent()) {
                String newQuery = overrideQuery.get();
                DuckDBConnection conn = (DuckDBConnection) statementToUse.getConnection();
                statementToUse = conn.prepareStatement(newQuery);
            }
        } catch (Exception e) {
            logger.warn("Failed to parse HEADER_SPLIT_SIZE schema override: {}", e.getMessage());
        }

        PreparedStatement finalStatementToUse = statementToUse;
        streamResultSet(executorService, () -> (DuckDBResultSet) finalStatementToUse.executeQuery(),
                allocator, getBatchSize(context),
                listener, () -> {});
    }

    @Override
    public void getStreamStatement(
            final FlightSql.TicketStatementQuery ticketStatementQuery,
            final CallContext context,
            final ServerStreamListener listener) {
        StatementHandle statementHandle = StatementHandle.deserialize(ticketStatementQuery.getStatementHandle());
        if(statementHandle.signatureMismatch(secretKey)) {
            handleSignatureMismatch(listener);
            return;
        }
        DuckDBConnection connection;
        try {
            connection = getConnection(context);
        } catch (NoSuchCatalogSchemaError e) {
            handleNoSuchDBSchema(listener, e);
            return;
        }
        Statement statement ;
        String originalQuery = statementHandle.query();
        try {
            statement = connection.createStatement();
        } catch (SQLException e) {
            handleSqlException(listener, e);
            return;
        }

        var statementContext = new StatementContext<>(statement, originalQuery);
        statementLoadingCache.put(statementHandle.queryId(), statementContext);

        // Attempt to override the query
        Optional<String> overrideQuery = Optional.empty();
        try {
            overrideQuery = getOverrideQuery(context, originalQuery);
        } catch (Exception e) {
            logger.warn("Failed to parse HEADER_SPLIT_SIZE schema override: {}", e.getMessage());
        }

        String queryToExecute = overrideQuery.orElse(originalQuery);
        streamResultSet(executorService,
                () -> {
                    statement.execute(queryToExecute);
                    return (DuckDBResultSet) statement.getResultSet();
                },
                allocator,
                getBatchSize(context),
                listener,
                () -> statementLoadingCache.invalidate(statementHandle.queryId()));
    }



    @Override
    public Runnable acceptPutStatement(FlightSql.CommandStatementUpdate command, CallContext context,
                                       FlightStream flightStream, StreamListener<PutResult> ackStream) {
        return null;
    }

    @Override
    public Runnable acceptPutPreparedStatementUpdate(FlightSql.CommandPreparedStatementUpdate command,
                                                     CallContext context, FlightStream flightStream,
                                                     StreamListener<PutResult> ackStream) {
        return null;
    }

    @Override
    public Runnable acceptPutPreparedStatementQuery(FlightSql.CommandPreparedStatementQuery command,
                                                    CallContext context, FlightStream flightStream,
                                                    StreamListener<PutResult> ackStream) {
        return null;
    }

    @Override
    public Runnable acceptPutStatementBulkIngest(
            FlightSql.CommandStatementIngest command,
            CallContext context,
            FlightStream flightStream,
            StreamListener<PutResult> ackStream) {
        if (checkAccessModeAndRespond(ackStream)) {
           return null;
        }
        Map<String, String > optionMap = command.getOptionsMap();
        String path = optionMap.get("path");
        final String completePath = warehousePath + "/" + path;
        String format = optionMap.getOrDefault("format", "parquet");
        String partitionColumnString = optionMap.get("partition");
        List<String> partitionColumns;
        if(partitionColumnString != null) {
            partitionColumns = Arrays.stream(partitionColumnString.split(",")).toList();
        } else {
            partitionColumns = List.of();
        }

        return () -> {
            FlightStreamReader reader = FlightStreamReader.of(flightStream, allocator);
            try {
                ConnectionPool.bulkIngestToFile(reader, allocator, completePath, partitionColumns, format);
                ackStream.onNext(PutResult.empty());
            } catch (SQLException e) {
                ackStream.onError(e);
            } finally {
                ackStream.onCompleted();
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

    private void cancelStatement(final FlightSql.TicketStatementQuery ticketStatementQuery,
                                 CallContext context,
                                 StreamListener<CancelStatus> listener) {
        StatementHandle statementHandle = StatementHandle.deserialize(ticketStatementQuery.getStatementHandle());
        cancel(statementHandle, listener);
    }

    private void cancelPreparedStatement(FlightSql.CommandPreparedStatementQuery ticketPreparedStatementQuery,
                                         CallContext context,
                                         StreamListener<CancelStatus> listener) {
        final StatementHandle statementHandle = StatementHandle.deserialize(ticketPreparedStatementQuery.getPreparedStatementHandle());
        cancel(statementHandle, listener);
    }

    private void cancel(StatementHandle statementHandle,
                        StreamListener<CancelStatus> listener) {
        if (statementHandle.signatureMismatch(secretKey)) {
            handleSignatureMismatch(listener);
            return;
        }
        try {
            StatementContext<Statement> statementContext =
                    statementLoadingCache.getIfPresent(statementHandle.queryId());
            if (statementContext == null) {
                handleContextNotFound(listener);
                return;
            }

            Statement statement = statementContext.getStatement();
            listener.onNext(CancelStatus.CANCELLING);
            try {
                statement.cancel();
            } catch (SQLException e) {
                handleSqlException(listener, e);
            }
            listener.onNext(CancelStatus.CANCELLED);
        } finally {
            listener.onCompleted();
            statementLoadingCache.invalidate(statementHandle.queryId());
        }
    }

    @Override
    public FlightInfo getFlightInfoSqlInfo(FlightSql.CommandGetSqlInfo request,
                                           CallContext context, FlightDescriptor descriptor) {
        request.getInfoList().forEach( r -> {
                    System.out.println (r);
                }
        );
        return null;
    }

    @Override
    public void getStreamSqlInfo(FlightSql.CommandGetSqlInfo command,
                                 CallContext context, ServerStreamListener listener) {
        throwUnimplemented("getStreamSqlInfo");
    }

    @Override
    public FlightInfo getFlightInfoTypeInfo(FlightSql.CommandGetXdbcTypeInfo request,
                                            CallContext context, FlightDescriptor descriptor) {
        return null;
    }

    @Override
    public void getStreamTypeInfo(FlightSql.CommandGetXdbcTypeInfo request, CallContext context,
                                  ServerStreamListener listener) {
        throwUnimplemented("getStreamTypeInfo");
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
        streamResultSet(executorService, DuckDBDatabaseMetadataUtil::getCatalogs, context, allocator, listener);
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
                context, allocator, listener);
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
                context, allocator, listener);
    }

    @Override
    public FlightInfo getFlightInfoTableTypes(FlightSql.CommandGetTableTypes request, CallContext context,
                                              FlightDescriptor descriptor) {
        return null;
    }

    @Override
    public void getStreamTableTypes(CallContext context, ServerStreamListener listener) {
        throwUnimplemented("getStreamTableTypes");
    }

    @Override
    public FlightInfo getFlightInfoPrimaryKeys(FlightSql.CommandGetPrimaryKeys request, CallContext context,
                                               FlightDescriptor descriptor) {
        return null;
    }

    @Override
    public void getStreamPrimaryKeys(FlightSql.CommandGetPrimaryKeys command, CallContext context,
                                     ServerStreamListener listener) {
        throwUnimplemented("getStreamPrimaryKeys");
    }

    @Override
    public FlightInfo getFlightInfoExportedKeys(FlightSql.CommandGetExportedKeys request, CallContext context,
                                                FlightDescriptor descriptor) {
        return null;
    }

    @Override
    public FlightInfo getFlightInfoImportedKeys(FlightSql.CommandGetImportedKeys request, CallContext context,
                                                FlightDescriptor descriptor) {
        return null;
    }

    @Override
    public FlightInfo getFlightInfoCrossReference(FlightSql.CommandGetCrossReference request, CallContext context,
                                                  FlightDescriptor descriptor) {
        return null;
    }

    @Override
    public void getStreamExportedKeys(FlightSql.CommandGetExportedKeys command, CallContext context,
                                      ServerStreamListener listener) {
        throwUnimplemented("getStreamExportedKeys");
    }

    @Override
    public void getStreamImportedKeys(FlightSql.CommandGetImportedKeys command, CallContext context,
                                      ServerStreamListener listener) {
        throwUnimplemented("getStreamImportedKeys");
    }

    @Override
    public void getStreamCrossReference(FlightSql.CommandGetCrossReference command, CallContext context,
                                        ServerStreamListener listener) {
        throwUnimplemented("getStreamCrossReference");
    }

    @Override
    public void close() throws Exception {
        AutoCloseables.close(this.allocator, this.executorService);
    }

    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        throwUnimplemented("listFlights");
    }

    private void throwUnimplemented(String name) {
        logger.info("Unimplemented method error {}", name);
        throw CallStatus.UNIMPLEMENTED.toRuntimeException();
    }

    private static class StatementRemovalListener<T extends Statement>
            implements RemovalListener<Long, StatementContext<T>> {
        @Override
        public void onRemoval(final RemovalNotification<Long, StatementContext<T>> notification) {
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

    public Location getLocation(){
        return this.location;
    }


    private static DuckDBConnection getConnection(final CallContext context) throws NoSuchCatalogSchemaError {
        var databaseSchmema = getDatabaseSchema(context);
        String dbSchema = String.format("%s.%s", databaseSchmema.database, databaseSchmema.schema);
        String[] sqls = {String.format("USE %s", dbSchema)};
        try {
            return ConnectionPool.getConnection(sqls);
        } catch (Exception e ){
            throw new NoSuchCatalogSchemaError(dbSchema);
        }
    }

    record DatabaseSchema ( String database, String schema) {}
    private static DatabaseSchema getDatabaseSchema(CallContext context){
        CallHeaders headers = context.getMiddleware(FlightConstants.HEADER_KEY).headers();
        String database = headers.get(Headers.HEADER_DATABASE);
        String schema = headers.get(Headers.HEADER_SCHEMA);
        if (schema == null) {
            schema = DEFAULT_SCHEMA;
        }
        if(database == null) {
            database = DEFAULT_DATABASE;
        }
        return new DatabaseSchema(database, schema);
    }

    private static int getBatchSize(final CallContext context) {
        return ContextUtils.getValue(context, Headers.HEADER_FETCH_SIZE, Headers.DEFAULT_ARROW_FETCH_SIZE, Integer.class);
    }

    private interface ResultSetSupplierFromConnection {
        DuckDBResultSet get(DuckDBConnection connection) throws SQLException;
    }

    private static void streamResultSet(ExecutorService executorService,
                                        ResultSetSupplierFromConnection supplier,
                                        CallContext context,
                                        BufferAllocator allocator,
                                        final ServerStreamListener listener) {

        streamResultSet(executorService, supplier, context, allocator, listener, () -> {});
    }
    private static void streamResultSet( ExecutorService executorService,
                                         ResultSetSupplierFromConnection supplier,
                                         CallContext context,
                                         BufferAllocator allocator,
                                         final ServerStreamListener listener,
                                         Runnable finalBlock) {
        try {
            DuckDBConnection connection = getConnection(context);
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
                    });
        } catch (NoSuchCatalogSchemaError e) {
            handleNoSuchDBSchema(listener, e);
        }
    }

    private interface ResultSetSupplier {
        DuckDBResultSet get() throws SQLException;
    }

    private static void streamResultSet(ExecutorService executorService,
                                        ResultSetSupplier supplier,
                                        BufferAllocator allocator,
                                        final int batchSize,
                                        final ServerStreamListener listener,
                                        Runnable finalBlock) {
        executorService.submit(() -> {
            var error = false ;
            try (DuckDBResultSet resultSet = supplier.get();
                 ArrowReader reader = (ArrowReader) resultSet.arrowExportStream(allocator, batchSize)) {
                listener.start(reader.getVectorSchemaRoot());
                while (reader.loadNextBatch()) {
                    listener.putNext();
                }
            }  catch (IOException exception) {
                handleIOException(listener, exception);
            } catch (SQLException e) {
                handleSqlException(listener, e);
                error = true;
            } finally {
                if(!error) {
                    listener.completed();
                }
                finalBlock.run();
            }
        });
    }

    private FlightInfo getFlightInfoStatement(String query,
                                              final CallContext context,
                                              final FlightDescriptor descriptor) {

        Optional<String> overrideSchema = Optional.empty();
        try {
            overrideSchema = getOverrideQuery(context, query);
        } catch (SQLException | JsonProcessingException e) {
            throw new RuntimeException(e);
        }

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
            var splitSize = getSplitSize(tree, context);
            var splits = SplitPlanner.getSplits(tree, splitSize);
            var list = splits.stream().map(split -> {
                var copy = tree.deepCopy();
                SplitPlanner.replacePathInFromClause(copy, split.stream().map(FileStatus::fileName).toArray(String[]::new));
                try {
                    var sql = Transformations.parseToSql(copy);
                    StatementHandle handle = newStatementHandle(sql);
                    final ByteString serializedHandle =
                            copyFrom(handle.serialize());
                    return FlightSql.TicketStatementQuery.newBuilder().setStatementHandle(serializedHandle).build();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }).toList();
            return getFlightInfoForSchema(list, descriptor, null, getLocation());
        } catch (SQLException e) {
            throw  handleSqlException(e);
        } catch (IOException e) {
            throw handleIOException(e);
        }
    }

    JsonNode getTableFunction(JsonNode tree){
        var fromTable = Transformations.getFirstStatementNode(tree).get("from_table");
        return  fromTable.get("function");
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
        return ContextUtils.getValue(context, Headers.HEADER_PARALLELIZE, false, Boolean.class);
    }



    private static boolean hasAggregation(JsonNode jsonNode) {
        return false;
    }

    private static long getSplitSize(JsonNode jsonNode, CallContext callContext) {
        return ContextUtils.getValue(callContext, Headers.HEADER_SPLIT_SIZE, Headers.DEFAULT_SPLIT_SIZE, Long.class);
    }

    private JsonNode authorize(CallContext callContext, JsonNode sql) throws UnauthorizedException {
        String peerIdentity = callContext.peerIdentity();
        var databaseSchema = getDatabaseSchema(callContext);
        return sqlAuthorizer.authorize(peerIdentity, databaseSchema.database, databaseSchema.schema, sql);
    }

    protected StatementHandle newStatementHandle(String query) {
        return new StatementHandle(query, sqlIdCounter.incrementAndGet(), producerId).signed(secretKey);
    }

    private static void handleNoSuchDBSchema(ServerStreamListener listener, NoSuchCatalogSchemaError exception){
        listener.error(FlightRuntimeExceptionFactory.of(new CallStatus(FlightStatusCode.INVALID_ARGUMENT, null, exception.getMessage(), null)));
        listener.completed();
    }

    private static void handleNoSuchDBSchema(StreamListener<Result> listener, NoSuchCatalogSchemaError exception){
        listener.onError(FlightRuntimeExceptionFactory.of(new CallStatus(FlightStatusCode.INVALID_ARGUMENT, null, exception.getMessage(), null)));
        listener.onCompleted();
    }

    private static void handleContextNotFound() {
        throw FlightRuntimeExceptionFactory.of(CallStatus.NOT_FOUND);
    }

    private static <T> void handleSignatureMismatch(StreamListener<T> listener) {
        listener.onError(FlightRuntimeExceptionFactory.of(
                new CallStatus(CallStatus.UNAUTHORIZED.code(), null, "Signature in the handle do not match", null)));
    }

    private static void handleSignatureMismatch() {
        throw FlightRuntimeExceptionFactory.of(
                new CallStatus(CallStatus.UNAUTHORIZED.code(), null, "Signature in the handle do not match", null));
    }

    private static void handleSignatureMismatch(ServerStreamListener listener) {
        listener.error(FlightRuntimeExceptionFactory.of(
                new CallStatus(CallStatus.UNAUTHORIZED.code(), null, "Signature in the handle do not match", null)));
    }

    private static void handleContextNotFound(StreamListener<CancelStatus> listener) {
        listener.onError(FlightRuntimeExceptionFactory.of(CallStatus.NOT_FOUND));
    }

    private static<T> void handleUnauthorized(StreamListener<T> listener, UnauthorizedException unauthorizedException) {
        var callStatus = new CallStatus(CallStatus.UNAUTHORIZED.code(), null, unauthorizedException.getMessage(), null);
        listener.onError(FlightRuntimeExceptionFactory.of(callStatus));
    }

    private static void handleUnauthorized(ServerStreamListener listener, UnauthorizedException unauthorizedException) {
        var callStatus = new CallStatus(CallStatus.UNAUTHORIZED.code(), null, unauthorizedException.getMessage(), null);
        listener.error(FlightRuntimeExceptionFactory.of(callStatus));
    }

    private static FlightRuntimeException handleUnauthorized(UnauthorizedException unauthorizedException) {
        return FlightRuntimeExceptionFactory.of(new CallStatus(CallStatus.UNAUTHORIZED.code(), null, "Unauthorized access to tableOrPath or columns", null));
    }

    private static void handleSqlException(ServerStreamListener listener, SQLException e) {
        var exception = CallStatus.INTERNAL
                .withDescription(e.getMessage())
                .toRuntimeException();
        listener.error(exception);
    }

    private static<T> void handleSqlException(StreamListener<T> listener, SQLException e) {
        var exception = CallStatus.INTERNAL
                .withDescription(e.getMessage())
                .toRuntimeException();
        listener.onError(exception);
    }

    private static void handleIOException(ServerStreamListener listener, IOException e) {
        var exception = CallStatus.INTERNAL
                .withDescription(e.getMessage())
                .toRuntimeException();
        listener.error(exception);
    }

    private FlightRuntimeException handleSqlException( SQLException e) {
        return CallStatus.INTERNAL
                .withDescription(e.getMessage())
                .toRuntimeException();
    }

    private static FlightRuntimeException handleIOException(IOException e) {
        throw CallStatus.INTERNAL
                .withDescription(e.getMessage())
                .toRuntimeException();
    }

    private static FlightRuntimeException handleJsonProcessingException(JsonProcessingException e) {
        return  CallStatus.INTERNAL.withDescription(e.getMessage()).toRuntimeException();
    }

    private static void handleQueryCompilationError(JsonNode tree) {
        throw CallStatus.INTERNAL.withDescription(tree.get("error_message").asText()).toRuntimeException();
    }

    private FlightRuntimeException handleInconsistentRequest(String s) {
        return CallStatus.INTERNAL.withDescription(s).toRuntimeException();
    }

    private  boolean checkAccessModeAndRespond(ServerStreamListener listener) {
        if (accessMode == AccessMode.RESTRICTED) {
            handleUnauthorized(listener, new UnauthorizedException("Close Prepared Statement"));
            return true;
        }
        return false;
    }

    private <T> boolean checkAccessModeAndRespond(StreamListener<T> listener) {
        if (accessMode == AccessMode.RESTRICTED) {
            handleUnauthorized(listener, new UnauthorizedException("Close Prepared Statement"));
            return true;
        }
        return false;
    }

    private void checkAccessModeAndRespond() {
        if (accessMode == AccessMode.RESTRICTED) {
            throw handleUnauthorized(new UnauthorizedException("Get FlightInfo Prepared Statement"));
        }
    }

    private static Optional<String> getOverrideQuery(CallContext context, String query)
            throws SQLException, JsonProcessingException {
        if (context == null || context.getMiddleware(FlightConstants.HEADER_KEY) == null) {
            return Optional.empty();
        }
        CallHeaders headers = context.getMiddleware(FlightConstants.HEADER_KEY).headers();
        if (headers == null) {
            return Optional.empty();
        }
        String encodedSchema = headers.get(Headers.HEADER_DATA_SCHEMA);
        if (encodedSchema == null || encodedSchema.isBlank()) {
            return Optional.empty();
        }
        String decodedSchema = URLDecoder.decode(encodedSchema, StandardCharsets.UTF_8);
        System.out.println("Final Query Will be: -> " + "select " + Transformations.getCast(decodedSchema) + " where false " + query + ";");
        return Optional.of("select " + Transformations.getCast(decodedSchema) + " where false " + query + ";");
    }
}

package io.dazzleduck.sql.flight.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.ByteString;
import io.dazzleduck.sql.commons.authorization.UnauthorizedException;
import io.dazzleduck.sql.commons.Transformations;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.commons.ingestion.IngestionHandler;
import io.dazzleduck.sql.flight.ingestion.IngestionParameters;
import io.dazzleduck.sql.commons.planner.SplitPlanner;
import io.dazzleduck.sql.flight.FlightRecorder;
import io.dazzleduck.sql.flight.optimizer.QueryOptimizer;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.protobuf.ByteString.copyFrom;

public class RestrictedFlightSqlProducer extends DuckDBFlightSqlProducer {

    private final QueryOptimizer queryOptimizer;
    public RestrictedFlightSqlProducer(Location serverLocation, String producerId, String secretKey, BufferAllocator allocator, String warehousePath, Path tempDir, IngestionHandler postIngestionHandler, ScheduledExecutorService scheduledExecutorService, Duration queryTimeout, Clock clock, FlightRecorder recorder, QueryOptimizer queryOptimizer, IngestionConfig ingestionConfig) {
        this(serverLocation, producerId, secretKey, allocator, warehousePath, tempDir, postIngestionHandler, scheduledExecutorService, queryTimeout, clock, recorder, queryOptimizer, ingestionConfig, List.of());
    }

    public RestrictedFlightSqlProducer(Location serverLocation, String producerId, String secretKey, BufferAllocator allocator, String warehousePath, Path tempDir, IngestionHandler postIngestionHandler, ScheduledExecutorService scheduledExecutorService, Duration queryTimeout, Clock clock, FlightRecorder recorder, QueryOptimizer queryOptimizer, IngestionConfig ingestionConfig, List<Location> dataProcessorLocations) {
        super(serverLocation, producerId, secretKey, allocator, warehousePath, AccessMode.RESTRICTED, tempDir, postIngestionHandler, scheduledExecutorService, queryTimeout, clock, recorder, ingestionConfig, dataProcessorLocations);
        this.queryOptimizer = queryOptimizer;
    }

    public static <T> T throwNotSupported(String operation) {
        throw new UnsupportedOperationException("Operation not supported :" + operation);
    }

    @Override
    public final FlightInfo getFlightInfoPreparedStatement(
            final FlightSql.CommandPreparedStatementQuery command,
            final CallContext context,
            final FlightDescriptor descriptor) {
        return throwNotSupported("Prepared statements are not supported in restricted mode");
    }

    /**
     * Evaluate a Substrait plan.
     *
     * @param command    The Substrait plan.
     * @param context    Per-call context.
     * @param descriptor The descriptor identifying the data stream.
     * @return Metadata about the stream.
     */
    public final FlightInfo getFlightInfoSubstraitPlan(
            FlightSql.CommandStatementSubstraitPlan command, CallContext context, FlightDescriptor descriptor) {
        return throwNotSupported("Substrait plans are not supported in restricted mode");
    }


    /**
     * Get the result schema for a SQL query.
     *
     * @param command    The SQL query.
     * @param context    Per-call context.
     * @param descriptor The descriptor identifying the data stream.
     * @return the schema of the result set.
     */
    public final SchemaResult getSchemaStatement(
            FlightSql.CommandStatementQuery command, CallContext context, FlightDescriptor descriptor) {
        return throwNotSupported("Schema queries for statements are not supported in restricted mode");
    }

    /**
     * Get the schema of the result set of a prepared statement.
     *
     * @param command    The prepared statement handle.
     * @param context    Per-call context.
     * @param descriptor The descriptor identifying the data stream.
     * @return the schema of the result set.
     */
    public final SchemaResult getSchemaPreparedStatement(
            FlightSql.CommandPreparedStatementQuery command, CallContext context, FlightDescriptor descriptor) {
        return throwNotSupported("Schema queries for prepared statements are not supported in restricted mode");
    }

    /**
     * Get the result schema for a Substrait plan.
     *
     * @param command    The Substrait plan.
     * @param context    Per-call context.
     * @param descriptor The descriptor identifying the data stream.
     * @return Schema for the stream.
     */
    public final SchemaResult getSchemaSubstraitPlan(
            FlightSql.CommandStatementSubstraitPlan command, CallContext context, FlightDescriptor descriptor) {
        return throwNotSupported("Substrait plan schema queries are not supported in restricted mode");
    }

    /**
     * Returns data for a particular prepared statement query instance.
     *
     * @param command  The prepared statement to generate the data stream.
     * @param context  Per-call context.
     * @param listener An interface for sending data back to the client.
     */
    public final void getStreamPreparedStatement(
            FlightSql.CommandPreparedStatementQuery command, CallContext context, ServerStreamListener listener) {
        throwNotSupported("Streaming prepared statements are not supported in restricted mode");
    }

    /**
     * Accepts uploaded data for a particular SQL query based data stream.
     *
     * <p>`PutResult`s must be in the form of a {@link FlightSql.DoPutUpdateResult}.
     *
     * @param command      The sql command to generate the data stream.
     * @param context      Per-call context.
     * @param flightStream The data stream being uploaded.
     * @param ackStream    The result data stream.
     * @return A runnable to process the stream.
     */
    public final Runnable acceptPutStatement(
            FlightSql.CommandStatementUpdate command,
            CallContext context,
            FlightStream flightStream,
            StreamListener<PutResult> ackStream) {
        return throwNotSupported("Update statements are not supported in restricted mode");
    }

    @Override
    public Runnable acceptPutStatementBulkIngest(
            FlightSql.CommandStatementIngest command,
            CallContext context,
            FlightStream flightStream,
            StreamListener<PutResult> ackStream) {
        var queue = IngestionParameters.getIngestionParameters(command).ingestionQueue();
        var user = context.peerIdentity();
        var verifiedClaims = getVerifiedClaims(context);
        if (!getSqlAuthorizer().hasWriteAccess(user, queue, verifiedClaims)) {
            ErrorHandling.handleUnauthorized(ackStream,
                    new UnauthorizedException("No write access to ingestion_queue:" + queue));
            return () -> {};
        }
        return super.acceptPutStatementBulkIngest(command, context, flightStream, ackStream);
    }

    /**
     * Handle a Substrait plan with uploaded data.
     *
     * @param command      The Substrait plan to evaluate.
     * @param context      Per-call context.
     * @param flightStream The data stream being uploaded.
     * @param ackStream    The result data stream.
     * @return A runnable to process the stream.
     */
    public final Runnable acceptPutSubstraitPlan(
            FlightSql.CommandStatementSubstraitPlan command,
            CallContext context,
            FlightStream flightStream,
            StreamListener<PutResult> ackStream) {
        return throwNotSupported("Substrait plans are not supported in restricted mode");
    }

    /**
     * Accepts uploaded data for a particular prepared statement data stream.
     *
     * <p>`PutResult`s must be in the form of a {@link FlightSql.DoPutUpdateResult}.
     *
     * @param command      The prepared statement to generate the data stream.
     * @param context      Per-call context.
     * @param flightStream The data stream being uploaded.
     * @param ackStream    The result data stream.
     * @return A runnable to process the stream.
     */
    public final Runnable acceptPutPreparedStatementUpdate(
            FlightSql.CommandPreparedStatementUpdate command,
            CallContext context,
            FlightStream flightStream,
            StreamListener<PutResult> ackStream) {
        return throwNotSupported("Prepared statement updates are not supported in restricted mode");
    }

    /**
     * Accepts uploaded parameter values for a particular prepared statement query.
     *
     * @param command      The prepared statement the parameter values will bind to.
     * @param context      Per-call context.
     * @param flightStream The data stream being uploaded.
     * @param ackStream    The result data stream.
     * @return A runnable to process the stream.
     */
    public final Runnable acceptPutPreparedStatementQuery(
            FlightSql.CommandPreparedStatementQuery command,
            CallContext context,
            FlightStream flightStream,
            StreamListener<PutResult> ackStream) {
        return throwNotSupported("Prepared statement queries with parameters are not supported in restricted mode");
    }

    /**
     * Returns the SQL Info of the server by returning a {@link FlightSql.CommandGetSqlInfo} in a {@link
     * Result}.
     *
     * @param request    request filter parameters.
     * @param context    Per-call context.
     * @param descriptor The descriptor identifying the data stream.
     * @return Metadata about the stream.
     */
    public final FlightInfo getFlightInfoSqlInfo(
            FlightSql.CommandGetSqlInfo request, CallContext context, FlightDescriptor descriptor) {
        return throwNotSupported("SQL info queries are not supported in restricted mode");
    }

    /**
     * Returns data for SQL info based data stream.
     *
     * @param command  The command to generate the data stream.
     * @param context  Per-call context.
     * @param listener An interface for sending data back to the client.
     */
    public final void getStreamSqlInfo(
            FlightSql.CommandGetSqlInfo command, CallContext context, ServerStreamListener listener) {
        throwNotSupported("SQL info streaming is not supported in restricted mode");
    }

    /**
     * Returns a description of all the data types supported by source.
     *
     * @param request    request filter parameters.
     * @param descriptor The descriptor identifying the data stream.
     * @return Metadata about the stream.
     */
    public final FlightInfo getFlightInfoTypeInfo(
            FlightSql.CommandGetXdbcTypeInfo request, CallContext context, FlightDescriptor descriptor) {
        return throwNotSupported("Type info queries are not supported in restricted mode");
    }

    /**
     * Returns data for type info based data stream.
     *
     * @param context  Per-call context.
     * @param listener An interface for sending data back to the client.
     */
    @Override
    public final void getStreamTypeInfo(
            FlightSql.CommandGetXdbcTypeInfo request, CallContext context, ServerStreamListener listener) {
        throwNotSupported("Type info streaming is not supported in restricted mode");
    }

    /**
     * Returns the available catalogs by returning a stream of {@link FlightSql.CommandGetCatalogs} objects in
     * {@link Result} objects.
     *
     * @param request    request filter parameters.
     * @param context    Per-call context.
     * @param descriptor The descriptor identifying the data stream.
     * @return Metadata about the stream.
     */
    public final FlightInfo getFlightInfoCatalogs(
            FlightSql.CommandGetCatalogs request, CallContext context, FlightDescriptor descriptor) {
        return throwNotSupported("Catalog queries are not supported in restricted mode");
    }

    /**
     * Returns data for catalogs based data stream.
     *
     * @param context  Per-call context.
     * @param listener An interface for sending data back to the client.
     */
    public final void getStreamCatalogs(CallContext context, ServerStreamListener listener) {
        throwNotSupported("Catalog streaming is not supported in restricted mode");
    }

    /**
     * Returns the available schemas by returning a stream of {@link FlightSql.CommandGetDbSchemas} objects in
     * {@link Result} objects.
     *
     * @param request    request filter parameters.
     * @param context    Per-call context.
     * @param descriptor The descriptor identifying the data stream.
     * @return Metadata about the stream.
     */
    public final FlightInfo getFlightInfoSchemas(
            FlightSql.CommandGetDbSchemas request, CallContext context, FlightDescriptor descriptor) {
        return throwNotSupported("Schema queries are not supported in restricted mode");
    }

    /**
     * Returns data for schemas based data stream.
     *
     * @param command  The command to generate the data stream.
     * @param context  Per-call context.
     * @param listener An interface for sending data back to the client.
     */
    public final void getStreamSchemas(
            FlightSql.CommandGetDbSchemas command, CallContext context, ServerStreamListener listener) {
        throwNotSupported("Schema streaming is not supported in restricted mode");
    }

    /**
     * Returns the available tables by returning a stream of {@link FlightSql.CommandGetTables} objects in
     * {@link Result} objects.
     *
     * @param request    request filter parameters.
     * @param context    Per-call context.
     * @param descriptor The descriptor identifying the data stream.
     * @return Metadata about the stream.
     */
    public final FlightInfo getFlightInfoTables(
            FlightSql.CommandGetTables request, CallContext context, FlightDescriptor descriptor) {
        return throwNotSupported("Table queries are not supported in restricted mode");
    }

    /**
     * Returns data for tables based data stream.
     *
     * @param command  The command to generate the data stream.
     * @param context  Per-call context.
     * @param listener An interface for sending data back to the client.
     */
    public final void getStreamTables(
            FlightSql.CommandGetTables command, CallContext context, ServerStreamListener listener) {
        throwNotSupported("Table streaming is not supported in restricted mode");
    }

    /**
     * Returns the available table types by returning a stream of {@link FlightSql.CommandGetTableTypes} objects
     * in {@link Result} objects.
     *
     * @param context    Per-call context.
     * @param descriptor The descriptor identifying the data stream.
     * @return Metadata about the stream.
     */
    public final FlightInfo getFlightInfoTableTypes(
            FlightSql.CommandGetTableTypes request, CallContext context, FlightDescriptor descriptor) {
        return throwNotSupported("Table type queries are not supported in restricted mode");
    }

    /**
     * Returns data for table types based data stream.
     *
     * @param context  Per-call context.
     * @param listener An interface for sending data back to the client.
     */
    public final void getStreamTableTypes(CallContext context, ServerStreamListener listener) {
        throwNotSupported("Table type streaming is not supported in restricted mode");
    }

    /**
     * Returns the available primary keys by returning a stream of {@link FlightSql.CommandGetPrimaryKeys}
     * objects in {@link Result} objects.
     *
     * @param request    request filter parameters.
     * @param context    Per-call context.
     * @param descriptor The descriptor identifying the data stream.
     * @return Metadata about the stream.
     */
    public final FlightInfo getFlightInfoPrimaryKeys(
            FlightSql.CommandGetPrimaryKeys request, CallContext context, FlightDescriptor descriptor) {
        return throwNotSupported("Primary key queries are not supported in restricted mode");
    }

    /**
     * Returns data for primary keys based data stream.
     *
     * @param command  The command to generate the data stream.
     * @param context  Per-call context.
     * @param listener An interface for sending data back to the client.
     */
    public final void getStreamPrimaryKeys(
            FlightSql.CommandGetPrimaryKeys command, CallContext context, ServerStreamListener listener) {
        throwNotSupported("Primary key streaming is not supported in restricted mode");
    }

    /**
     * Retrieves a description of the foreign key columns that reference the given table's primary key
     * columns {@link FlightSql.CommandGetExportedKeys} objects in {@link Result} objects.
     *
     * @param request    request filter parameters.
     * @param context    Per-call context.
     * @param descriptor The descriptor identifying the data stream.
     * @return Metadata about the stream.
     */
    public final FlightInfo getFlightInfoExportedKeys(
            FlightSql.CommandGetExportedKeys request, CallContext context, FlightDescriptor descriptor) {
        return throwNotSupported("Exported key queries are not supported in restricted mode");
    }

    /**
     * Retrieves a description of the primary key columns that are referenced by given table's foreign
     * key columns {@link FlightSql.CommandGetImportedKeys} objects in {@link Result} objects.
     *
     * @param request    request filter parameters.
     * @param context    Per-call context.
     * @param descriptor The descriptor identifying the data stream.
     * @return Metadata about the stream.
     */
    public final FlightInfo getFlightInfoImportedKeys(
            FlightSql.CommandGetImportedKeys request, CallContext context, FlightDescriptor descriptor) {
        return throwNotSupported("Imported key queries are not supported in restricted mode");
    }

    /**
     * Retrieve a description of the foreign key columns that reference the given table's primary key
     * columns {@link FlightSql.CommandGetCrossReference} objects in {@link Result} objects.
     *
     * @param request    request filter parameters.
     * @param context    Per-call context.
     * @param descriptor The descriptor identifying the data stream.
     * @return Metadata about the stream.
     */
    public final FlightInfo getFlightInfoCrossReference(
            FlightSql.CommandGetCrossReference request, CallContext context, FlightDescriptor descriptor) {
        return throwNotSupported("Cross reference queries are not supported in restricted mode");
    }

    /**
     * Returns data for foreign keys based data stream.
     *
     * @param command  The command to generate the data stream.
     * @param context  Per-call context.
     * @param listener An interface for sending data back to the client.
     */
    public final void getStreamExportedKeys(
            FlightSql.CommandGetExportedKeys command, CallContext context, ServerStreamListener listener) {
        throwNotSupported("Exported key streaming is not supported in restricted mode");
    }

    /**
     * Returns data for foreign keys based data stream.
     *
     * @param command  The command to generate the data stream.
     * @param context  Per-call context.
     * @param listener An interface for sending data back to the client.
     */
    public final void getStreamImportedKeys(
            FlightSql.CommandGetImportedKeys command, CallContext context, ServerStreamListener listener) {
        throwNotSupported("Imported key streaming is not supported in restricted mode");
    }

    /**
     * Returns data for cross reference based data stream.
     *
     * @param command  The command to generate the data stream.
     * @param context  Per-call context.
     * @param listener An interface for sending data back to the client.
     */
    public final void getStreamCrossReference(
            FlightSql.CommandGetCrossReference command, CallContext context, ServerStreamListener listener) {
        throwNotSupported("Cross reference streaming is not supported in restricted mode");
    }

    /**
     * Renew the duration of the given endpoint.
     *
     * @param request  The endpoint to renew.
     * @param context  Per-call context.
     * @param listener An interface for sending data back to the client.
     */
    public final void renewFlightEndpoint(
            RenewFlightEndpointRequest request,
            CallContext context,
            StreamListener<FlightEndpoint> listener) {
        throwNotSupported("Renewing flight endpoints is not supported in restricted mode");
    }

    protected final boolean parallelize(CallContext context) {
        return getSplitSize(context) > 0;
    }

    @Override
    protected FlightInfo getFlightInfoStatementFromQuery(final String query, final CallContext context, final FlightDescriptor descriptor) {
        var parallelize = parallelize(context);
        JsonNode tree = null;
        try {
            tree = Transformations.parseToTree(query);
            if (tree.get("error").asBoolean()) {
                ErrorHandling.handleQueryCompilationError(tree);
            }
        } catch (Throwable s) {
            ErrorHandling.handleThrowable(s);
        }

        try {
            tree = authorize(context, tree);
        } catch (UnauthorizedException e) {
            ErrorHandling.handleUnauthorized(e);
        } catch (Throwable e) {
            ErrorHandling.handleThrowable(e);
        }


        if (parallelize) {
            return getFlightInfoStatementSplittable(tree, context, descriptor);
        } else {
            try {
                var newSql = Transformations.parseToSql(tree);
                return super.getFlightInfoStatement(newSql, context, descriptor);
            } catch (SQLException e) {
                throw ErrorHandling.handleSqlException(e);
            }
        }
    }

    private JsonNode authorize(CallContext callContext, JsonNode sql) throws UnauthorizedException {
        var sqlAuthorizer = getSqlAuthorizer();
        String peerIdentity = callContext.peerIdentity();
        var verifiedClaims = getVerifiedClaims(callContext);
        var databaseSchema = getDatabaseSchema(callContext, AccessMode.RESTRICTED);
        return sqlAuthorizer.authorize(peerIdentity, databaseSchema.database(), databaseSchema.schema(), sql, verifiedClaims);
    }

    private String authorize(CallContext callContext, String sql, Connection connection)
            throws UnauthorizedException, JsonProcessingException, SQLException {
        var authorizedTree = authorizeTree(callContext, sql, connection);
        return Transformations.parseToSql(connection, authorizedTree);
    }

    private JsonNode authorizeTree(CallContext callContext, String sql, Connection connection)
            throws UnauthorizedException, JsonProcessingException, SQLException {
        var tree = Transformations.parseToTree(connection, sql);
        return authorize(callContext, tree);
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
            return getFlightInfoForSchema(list, descriptor, null, getDataProcessorLocations());
        } catch (Throwable throwable) {
            ErrorHandling.handleThrowable(throwable);
            return null;
        }
    }

    @Override
    protected void getStreamStatement(
            StatementHandle statementHandle,
            final CallContext context,
            final ServerStreamListener listener) {
        try {
            var connection = getConnection(context, AccessMode.RESTRICTED);
            String query = statementHandle.query();
            if (statementHandle.queryChecksum() != null
                    && statementHandle.signatureMismatch(secretKey)) {
                ErrorHandling.handleSignatureMismatch(listener);
                return;
            }

            if (statementHandle.queryChecksum() == null) {
                query = authorize(context, query, connection);
            }
            Statement statement = connection.createStatement();
            var statementContext = new StatementContext<>(connection, statement, query);
            var key = new CacheKey(context.peerIdentity(), statementHandle.queryId());
            statementLoadingCache.put(key, statementContext);
            ResultSetStreamUtil.streamResultSet(executorService,
                    statementContext,
                    key,
                    OptionalResultSetSupplier.of(statement, query, queryOptimizer),
                    allocator,
                    getBatchSize(context),
                    listener,
                    () -> statementLoadingCache.invalidate(key), recorder);
        } catch (Throwable e) {
            ErrorHandling.handleThrowable(listener, e);
        }
    }
}

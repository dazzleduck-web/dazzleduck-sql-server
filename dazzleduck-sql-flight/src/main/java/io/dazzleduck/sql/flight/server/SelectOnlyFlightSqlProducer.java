package io.dazzleduck.sql.flight.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import io.dazzleduck.sql.commons.Transformations;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.commons.authorization.UnauthorizedException;
import io.dazzleduck.sql.commons.ingestion.IngestionHandler;
import io.dazzleduck.sql.flight.FlightRecorder;
import io.dazzleduck.sql.flight.ingestion.IngestionParameters;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

public class SelectOnlyFlightSqlProducer extends DuckDBFlightSqlProducer {
    public SelectOnlyFlightSqlProducer(Location serverLocation, String producerId, String secretKey, BufferAllocator allocator, String warehousePath, AccessMode accessMode, Path tempDir, IngestionHandler postIngestionHandler, ScheduledExecutorService scheduledExecutorService, Duration queryTimeout, Duration maxQueryTimeout, Clock clock, FlightRecorder recorder, IngestionConfig ingestionConfig, List<Location> dataProcessorLocations) {
        super(serverLocation, producerId, secretKey, allocator, warehousePath, accessMode, tempDir, postIngestionHandler, scheduledExecutorService, queryTimeout, maxQueryTimeout, clock, recorder, ingestionConfig, dataProcessorLocations);
    }

    private static final java.util.regex.Pattern EXPLAIN_PATTERN = java.util.regex.Pattern.compile("^\\s*(EXPLAIN\\s+(ANALYZE\\s+)?)", java.util.regex.Pattern.CASE_INSENSITIVE);

    @Override
    protected String transformQuery(CallContext context, Connection connection, String query)
            throws UnauthorizedException, JsonProcessingException, SQLException {
        // For EXPLAIN queries, strip EXPLAIN part before parsing to AST
        // DuckDB's json_serialize_sql() cannot serialize EXPLAIN statements, so we need to handle them separately
        String queryToParse = query;
        String explainPrefix = null;

        var matcher = EXPLAIN_PATTERN.matcher(query);
        if (matcher.find()) {
            // Extract the EXPLAIN prefix (e.g., "EXPLAIN " or "EXPLAIN ANALYZE ")
            explainPrefix = matcher.group(0);
            // Strip the EXPLAIN prefix to get the inner query
            queryToParse = query.substring(matcher.end()).trim();
        }

        // Parse and authorize the query (without EXPLAIN)
        var tree = Transformations.parseToTree(connection, queryToParse);
        long limit = getLimit(context);
        long offset = getOffset(context);
        var authorized = authorize(context, tree, limit, offset);
        var result = Transformations.parseToSql(connection, authorized);

        // Reconstruct EXPLAIN prefix if it was present
        if (explainPrefix != null) {
            result = explainPrefix + result;
        }

        return result;
    }

    private JsonNode authorize(CallContext context, JsonNode tree, long limit, long offset) throws UnauthorizedException {
        var authorizer = getSqlAuthorizer();
        var claims = getVerifiedClaims(context);
        var databaseSchema = getDatabaseSchema(context, getAccessMode());
        return authorizer.authorize(context.peerIdentity(), databaseSchema.database(), databaseSchema.schema(), tree, claims, limit, offset);
    }

    @Override
    protected String transformPreparedStatementQuery(CallContext context, Connection connection, String query)
            throws UnauthorizedException, JsonProcessingException, SQLException {
        var tree = Transformations.parseToTree(connection, query);
        var authorizer = getSqlAuthorizer();
        var claims = getVerifiedClaims(context);
        var databaseSchema = getDatabaseSchema(context, getAccessMode());
        var authorized = authorizer.authorize(
                context.peerIdentity(), databaseSchema.database(), databaseSchema.schema(), tree, claims);
        return Transformations.parseToSql(connection, authorized);
    }

    /**
     * Update statements bypass {@link #transformQuery}, the only place SELECT-only is enforced, so
     * READ_ONLY and RESTRICT_READ_ONLY must refuse them outright: otherwise {@code executeUpdate}
     * over Flight SQL runs any SQL (DROP, DELETE, COPY ... TO) unchecked.
     */
    @Override
    public Runnable acceptPutStatement(
            FlightSql.CommandStatementUpdate command,
            CallContext context, FlightStream flightStream,
            StreamListener<PutResult> ackStream) {
        return throwNotSupported("acceptPutStatement");
    }

    @Override
    public Runnable acceptPutPreparedStatementUpdate(
            FlightSql.CommandPreparedStatementUpdate command,
            CallContext context, FlightStream flightStream,
            StreamListener<PutResult> ackStream) {
        return throwNotSupported("acceptPutPreparedStatementUpdate");
    }

    /**
     * Flight bulk ingest writes to an ingestion queue without going through SQL, so it is gated on
     * the authorizer's write check, as HTTP {@code /v1/ingest} already is in its JWT filter. The
     * READ_ONLY and RESTRICT_READ_ONLY authorizers grant no write access, so this refuses every
     * ingest in both modes.
     */
    @Override
    public Runnable acceptPutStatementBulkIngest(
            FlightSql.CommandStatementIngest command,
            CallContext context,
            FlightStream flightStream,
            StreamListener<PutResult> ackStream) {
        var queue = IngestionParameters.getIngestionParameters(command).ingestionQueue();
        if (!getSqlAuthorizer().hasWriteAccess(context.peerIdentity(), queue, getVerifiedClaims(context))) {
            ErrorHandling.handleUnauthorized(ackStream,
                    new UnauthorizedException("No write access to ingestion_queue:" + queue));
            return () -> {};
        }
        return super.acceptPutStatementBulkIngest(command, context, flightStream, ackStream);
    }
}

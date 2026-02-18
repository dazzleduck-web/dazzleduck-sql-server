package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.flight.ingestion.IngestionParameters;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.vector.compression.CompressionUtil;

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Adaptor interface that extends {@link FlightProducer} to provide HTTP-specific
 * functionality for the DazzleDuck SQL server.
 *
 * <p>This interface bridges the Arrow Flight SQL protocol with HTTP-based operations,
 * enabling both gRPC and REST API access to the same underlying query engine.
 */
public interface HttpFlightAdaptor {

    /**
     * Accepts a bulk ingestion request via HTTP, processing Arrow data from an input stream.
     *
     * @param context the call context containing peer identity and credentials
     * @param ingestionParameters parameters specifying the ingestion target and options
     * @param inputStream the input stream containing Arrow-formatted data
     * @param ackStream listener for acknowledgment results
     * @return a Runnable that performs the ingestion when executed
     */
    Runnable acceptPutStatementBulkIngest(
            FlightProducer.CallContext context,
            IngestionParameters ingestionParameters,
            InputStream inputStream,
            FlightProducer.StreamListener<PutResult> ackStream);

    /**
     * Returns the unique identifier for this producer instance.
     *
     * @return the producer ID
     */
    String getProducerId();

    /**
     * Attempts to cancel a running query without reporting status.
     *
     * <p>This is a best-effort, fire-and-forget cancellation method designed for
     * high-availability environments where cancellation requests are broadcast
     * to multiple nodes when the actual executor is not known.
     *
     * <p>This method:
     * <ul>
     *   <li>Does not require a listener - no status feedback is provided</li>
     *   <li>Silently returns if the query is not found on this node</li>
     *   <li>Silently ignores any errors during cancellation</li>
     *   <li>Still records cancellation metrics if the query is found</li>
     * </ul>
     *
     * <p><b>Usage in HA environments:</b> When a client requests cancellation but
     * the routing layer doesn't know which node is executing the query, it can
     * broadcast {@code tryCancel} to all nodes. Only the node actually running
     * the query will perform the cancellation; others will silently ignore the request.
     *
     * @param queryId the unique identifier of the query to cancel
     * @param context the call context containing peer identity
     */
    boolean tryCancel(Long queryId,
                      FlightProducer.CallContext context) throws SQLException;

    /**
     * Gets the stream for a statement query ticket.
     * This method provides a typed API for HTTP services to retrieve query results.
     *
     * @param ticket the statement query ticket
     * @param context the call context
     * @param listener the stream listener to receive results
     */
    void getStreamStatement(FlightSql.TicketStatementQuery ticket,
                            FlightProducer.CallContext context,
                            FlightProducer.ServerStreamListener listener);

    /**
     * Gets flight info for a statement query command.
     * This method provides a typed API for HTTP services to get query planning info.
     *
     * @param command the statement query command
     * @param context the call context
     * @param descriptor the flight descriptor
     * @return the flight info containing endpoints and schema
     */
    FlightInfo getFlightInfoStatement(FlightSql.CommandStatementQuery command,
                                      FlightProducer.CallContext context,
                                      FlightDescriptor descriptor);

    /**
     * Gets the stream for a statement query ticket, writing directly to an OutputStream.
     *
     * <p>The default implementation delegates to {@link #getStreamStatement} using a
     * {@link DirectOutputStreamListener} that writes Arrow IPC format to the output stream.
     *
     * <p>The outputStreamSupplier is used instead of a direct OutputStream to defer
     * obtaining the stream until data is actually ready to write. This allows HTTP
     * error responses to set proper status codes before the response is committed.
     *
     * @param ticket the statement query ticket
     * @param context the call context
     * @param outputStreamSupplier supplier that provides the output stream when data is ready to write
     * @return a CompletableFuture that completes when streaming is done, or exceptionally on error
     */
    default CompletableFuture<Void> getStreamStatementDirect(
            FlightSql.TicketStatementQuery ticket,
            FlightProducer.CallContext context,
            Supplier<OutputStream> outputStreamSupplier) {

        return getStreamStatementDirect(ticket, context, outputStreamSupplier, CompressionUtil.CodecType.ZSTD);
    }

    /**
     * Gets the stream for a statement query ticket, writing directly to an OutputStream,
     * with configurable compression.
     *
     * <p>The default implementation delegates to {@link #getStreamStatement} using a
     * {@link DirectOutputStreamListener} that writes Arrow IPC format to the output stream.
     *
     * <p>The outputStreamSupplier is used instead of a direct OutputStream to defer
     * obtaining the stream until data is actually ready to write. This allows HTTP
     * error responses to set proper status codes before the response is committed.
     *
     * @param ticket the statement query ticket
     * @param context the call context
     * @param outputStreamSupplier supplier that provides the output stream when data is ready to write
     * @param compressionCodec the compression codec to use for Arrow IPC (e.g., ZSTD, LZ4, NONE)
     * @return a CompletableFuture that completes when streaming is done, or exceptionally on error
     */
    default CompletableFuture<Void> getStreamStatementDirect(
            FlightSql.TicketStatementQuery ticket,
            FlightProducer.CallContext context,
            Supplier<OutputStream> outputStreamSupplier,
            CompressionUtil.CodecType compressionCodec) {

        CompletableFuture<Void> future = new CompletableFuture<>();
        // Use lazy initialization - only get the outputStream when start() is called
        // This allows errors before start() to be handled without committing the HTTP response
        DirectOutputStreamListener listener = new DirectOutputStreamListener(outputStreamSupplier, future, compressionCodec);

        // getStreamStatement is typically async - it starts streaming and returns
        getStreamStatement(ticket, context, listener);

        return future;
    }
}

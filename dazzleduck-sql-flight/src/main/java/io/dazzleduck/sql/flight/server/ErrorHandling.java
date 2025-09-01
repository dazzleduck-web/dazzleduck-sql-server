package io.dazzleduck.sql.flight.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.InvalidProtocolBufferException;
import io.dazzleduck.sql.common.auth.UnauthorizedException;
import org.apache.arrow.flight.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;

public class ErrorHandling {

    private final static Logger logger = LoggerFactory.getLogger(ErrorHandling.class);
    public static void handleThrowable(Throwable t) {
        if (t instanceof NoSuchCatalogSchemaError e) {
            handleNoSuchDBSchema(e);
        } else if (t instanceof SQLSyntaxErrorException e) {
            handleSQLSyntaxErrorException(e);
        } else if (t instanceof SQLException s) {
            handleSqlException(s);
        } else if (t instanceof IOException io) {
            handleIOException(io);
        } else if (t instanceof Exception e) {
            var exception = CallStatus.INTERNAL
                    .withDescription(e.getMessage())
                    .toRuntimeException();
            logger.atError().setCause(e).log("Error processing");
            throw exception;
        } else  {
            var exception = CallStatus.INTERNAL
                    .withDescription(t.getMessage())
                    .toRuntimeException();
            logger.atError().setCause(t).log("Error processing");
            throw exception;
        }
    }

    public static void handleThrowable(FlightProducer.ServerStreamListener listener, Throwable t) {
        if (t instanceof NoRegisterExecutorException e) {
            handleNoRegisterExecutor(listener, e);
        } else if (t instanceof NotImplemented e) {
            handleUnimplemented(listener, e);
        } else if (t instanceof InvalidProtocolBufferException e) {
            var ex = FlightRuntimeExceptionFactory.of(new CallStatus(CallStatus.INTERNAL.code(), null, e.getMessage(), null));
            listener.error(ex);
        } else if (t instanceof URISyntaxException e) {
            listener.error(
                    CallStatus.INTERNAL
                            .withDescription(e.getMessage())
                            .withCause(e)
                            .toRuntimeException());
        } else if (t instanceof NoSuchCatalogSchemaError e) {
            handleNoSuchDBSchema(listener, e);
        } else if (t instanceof SQLSyntaxErrorException e) {
            handleSQLSyntaxErrorException(listener, e);
        } else if (t instanceof SQLException s) {
            handleSqlException(listener, s);
        } else if (t instanceof IOException io) {
            handleIOException(listener, io);
        } else if (t instanceof Exception e) {
            handleException(listener, e);
        } else {
            var exception = CallStatus.INTERNAL
                    .withDescription(t.getMessage())
                    .toRuntimeException();
            logger.atError().setCause(t).log("Error processing");
            listener.error(exception);
        }
    }

    public static  <T> void handleThrowable(FlightProducer.StreamListener<T> listener, Throwable t) {
        if (t instanceof NoRegisterExecutorException e) {
            handleNoRegisterExecutor(listener, e);
        } else if (t instanceof NotImplemented e) {
            handleUnimplemented(listener, e);
        } else if (t instanceof InvalidProtocolBufferException e) {
            var ex = FlightRuntimeExceptionFactory.of(new CallStatus(CallStatus.INTERNAL.code(), null, e.getMessage(), null));
            listener.onError(ex);
        } else if (t instanceof IOException e) {
            listener.onError(
                    CallStatus.INTERNAL
                            .withDescription(e.getMessage())
                            .withCause(e)
                            .toRuntimeException());
        } else if (t instanceof URISyntaxException e) {
            listener.onError(
                    CallStatus.INTERNAL
                            .withDescription(e.getMessage())
                            .withCause(e)
                            .toRuntimeException());
        } else if (t instanceof NoSuchCatalogSchemaError e) {
            handleNoSuchDBSchema(listener, e);
        } else if (t instanceof SQLSyntaxErrorException e) {
            handleSQLSyntaxErrorException(listener, e);
        } else if (t instanceof SQLException s) {
            handleSqlException(listener, s);
        } else if (t instanceof Exception e) {
            var exception = CallStatus.INTERNAL
                    .withDescription(e.getMessage())
                    .toRuntimeException();
            logger.atError().setCause(e).log("Error processing");
            listener.onError(exception);
        } else {
            var exception = CallStatus.INTERNAL
                    .withDescription(t.getMessage())
                    .toRuntimeException();
            logger.atError().setCause(t).log("Error processing");
            listener.onError(exception);
        }
    }

    public static void handleUnimplemented(FlightProducer.StreamListener<?> ackStream, String method) {
        ackStream.onError(FlightRuntimeExceptionFactory.of(new CallStatus(CallStatus.UNIMPLEMENTED.code(), null, method, null)));
    }

    private static void handleNoSuchDBSchema(FlightProducer.ServerStreamListener listener, NoSuchCatalogSchemaError exception){
        listener.error(FlightRuntimeExceptionFactory.of(new CallStatus(FlightStatusCode.INVALID_ARGUMENT, null, exception.getMessage(), null)));
        listener.completed();
    }

    private static void handleNoSuchDBSchema(NoSuchCatalogSchemaError exception){
        throw FlightRuntimeExceptionFactory.of(new CallStatus(FlightStatusCode.INVALID_ARGUMENT, null, exception.getMessage(), null));
    }

    private static void handleNoSuchDBSchema(FlightProducer.StreamListener<?> listener, NoSuchCatalogSchemaError exception){
        listener.onError(FlightRuntimeExceptionFactory.of(new CallStatus(FlightStatusCode.INVALID_ARGUMENT, null, exception.getMessage(), null)));
        listener.onCompleted();
    }

    static void handleContextNotFound() {
        throw FlightRuntimeExceptionFactory.of(CallStatus.NOT_FOUND);
    }

    static <T> void handleSignatureMismatch(FlightProducer.StreamListener<T> listener) {
        listener.onError(FlightRuntimeExceptionFactory.of(
                new CallStatus(CallStatus.UNAUTHORIZED.code(), null, "Signature in the handle do not match", null)));
    }

    static void handleSignatureMismatch() {
        throw FlightRuntimeExceptionFactory.of(
                new CallStatus(CallStatus.UNAUTHORIZED.code(), null, "Signature in the handle do not match", null));
    }

    static void handleSignatureMismatch(FlightProducer.ServerStreamListener listener) {
        listener.error(FlightRuntimeExceptionFactory.of(
                new CallStatus(CallStatus.UNAUTHORIZED.code(), null, "Signature in the handle do not match", null)));
    }

    static void handleContextNotFound(FlightProducer.StreamListener<?> listener) {
        listener.onError(FlightRuntimeExceptionFactory.of(CallStatus.NOT_FOUND));
    }

    static<T> void handleUnauthorized(FlightProducer.StreamListener<T> listener, UnauthorizedException unauthorizedException) {
        var callStatus = new CallStatus(CallStatus.UNAUTHORIZED.code(), null, unauthorizedException.getMessage(), null);
        listener.onError(FlightRuntimeExceptionFactory.of(callStatus));
    }

    static void handleUnauthorized(FlightProducer.ServerStreamListener listener, UnauthorizedException unauthorizedException) {
        var callStatus = new CallStatus(CallStatus.UNAUTHORIZED.code(), null, unauthorizedException.getMessage(), null);
        listener.error(FlightRuntimeExceptionFactory.of(callStatus));
    }

    static FlightRuntimeException handleUnauthorized(UnauthorizedException unauthorizedException) {
        throw  FlightRuntimeExceptionFactory.of(new CallStatus(CallStatus.UNAUTHORIZED.code(), null, unauthorizedException.getMessage(), null));
    }

    private static void handleSqlException(FlightProducer.ServerStreamListener listener, SQLException e) {
        var exception = CallStatus.INTERNAL
                .withDescription(e.getMessage())
                .toRuntimeException();
        listener.error(exception);
    }

    static<T> void handleSqlException(FlightProducer.StreamListener<T> listener, SQLException e) {
        var exception = CallStatus.INTERNAL
                .withDescription(e.getMessage())
                .toRuntimeException();
        listener.onError(exception);
    }

    static<T> void handleSQLSyntaxErrorException(FlightProducer.StreamListener<T> listener, SQLSyntaxErrorException e) {
        var exception = CallStatus.INTERNAL
                .withDescription(e.getMessage())
                .toRuntimeException();
        listener.onError(exception);
    }

    private static void handleSQLSyntaxErrorException(SQLSyntaxErrorException e) {
        throw CallStatus.INTERNAL
                .withDescription(e.getMessage())
                .toRuntimeException();
    }

    private static void handleSQLSyntaxErrorException(FlightProducer.ServerStreamListener listener, SQLSyntaxErrorException e) {
        var exception = CallStatus.INTERNAL
                .withDescription(e.getMessage())
                .toRuntimeException();
        listener.error(exception);
    }

    private static void handleException(FlightProducer.ServerStreamListener listener, Exception e) {
        var exception = CallStatus.INTERNAL
                .withDescription(e.getMessage())
                .toRuntimeException();
        logger.atError().setCause(e).log("Error processing");
        listener.error(exception);
    }

    private static void handleIOException(FlightProducer.ServerStreamListener listener, IOException e) {
        var exception = CallStatus.INTERNAL
                .withDescription(e.getMessage())
                .toRuntimeException();
        listener.error(exception);
    }

    static void handleIOException(FlightProducer.StreamListener<?> listener, IOException e) {
        var exception = CallStatus.INTERNAL
                .withDescription(e.getMessage())
                .toRuntimeException();
        listener.onError(exception);
    }

    static FlightRuntimeException handleSqlException(SQLException e) {
        throw  CallStatus.INTERNAL
                .withDescription(e.getMessage())
                .toRuntimeException();
    }

    private static FlightRuntimeException handleIOException(IOException e) {
        throw CallStatus.INTERNAL
                .withDescription(e.getMessage())
                .toRuntimeException();
    }

    static FlightRuntimeException handleJsonProcessingException(JsonProcessingException e) {
        return  CallStatus.INTERNAL.withDescription(e.getMessage()).toRuntimeException();
    }

    static void handleQueryCompilationError(JsonNode tree) {
        throw CallStatus.INTERNAL.withDescription(tree.get("error_message").asText()).toRuntimeException();
    }



    public static void throwUnimplemented(FlightProducer.ServerStreamListener listener, String method) {
        var exception = FlightRuntimeExceptionFactory.of(new CallStatus(CallStatus.UNIMPLEMENTED.code(),
                null,  method, null));
        listener.error(exception);
    }

    public static <T> void throwUnimplemented(FlightProducer.StreamListener<T> listener, String method) {
        var exception = FlightRuntimeExceptionFactory.of(new CallStatus(CallStatus.UNIMPLEMENTED.code(),
                null,  method, null));
        listener.onError(exception);
    }

    public static void throwUnimplemented(String name) {
        logger.info("Unimplemented method error {}", name);
        throw CallStatus.UNIMPLEMENTED.toRuntimeException();
    }

    private static void handleNoRegisterExecutor(FlightProducer.ServerStreamListener listener,
                                                 NoRegisterExecutorException exception) {
        listener.error(FlightRuntimeExceptionFactory.of(new CallStatus(FlightStatusCode.NOT_FOUND, null, "No registered executor for cluster " + exception.clusterId(), null)));
    }

    private static <T> void  handleNoRegisterExecutor(FlightProducer.StreamListener<T> listener,
                                                      NoRegisterExecutorException exception) {
        listener.onError(FlightRuntimeExceptionFactory.of(new CallStatus(FlightStatusCode.NOT_FOUND, null, "No registered executor for cluster " + exception.clusterId(), null)));
    }

    private static void handleUnimplemented(FlightProducer.ServerStreamListener listener, NotImplemented unImplemented) {
        listener.error(FlightRuntimeExceptionFactory.of(new CallStatus(FlightStatusCode.UNIMPLEMENTED, null, "Not implemented " + unImplemented.getMessage(), null)));
    }

    private static <T> void handleUnimplemented(FlightProducer.StreamListener<T> listener, NotImplemented unImplemented) {
        listener.onError(FlightRuntimeExceptionFactory.of(new CallStatus(FlightStatusCode.UNIMPLEMENTED, null, "Not implemented " + unImplemented.getMessage(), null)));
    }
}

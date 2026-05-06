package io.dazzleduck.sql.flight.namedquery;

import com.google.protobuf.ByteString;
import com.hubspot.jinjava.Jinjava;
import io.dazzleduck.sql.common.NamedQueryParameterValidator;
import io.dazzleduck.sql.common.ParameterValidationException;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.flight.server.HttpFlightAdaptor;
import io.dazzleduck.sql.flight.server.StatementHandle;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultNamedQueryServiceAdaptor implements NamedQueryServiceAdaptor {

    private static final Logger logger = LoggerFactory.getLogger(DefaultNamedQueryServiceAdaptor.class);
    private static final int MAX_VALIDATOR_CACHE_SIZE = 500;
    private static final String LOAD_FROM_DB_QUERY = "SELECT id, name, template, validators, description, parameter_descriptions, preferred_display, query_group FROM %s WHERE name = '%s'";

    private final String name;
    private final HttpFlightAdaptor httpFlightAdaptor;
    private final Jinjava jinjava;

    /** Validator instance cache: fully-qualified class name → singleton instance. */
    private final ConcurrentHashMap<String, NamedQueryParameterValidator> validatorCache = new ConcurrentHashMap<>();

    public DefaultNamedQueryServiceAdaptor(String name,
                                           HttpFlightAdaptor httpFlightAdaptor) {
        this.name = name;
        this.httpFlightAdaptor = httpFlightAdaptor;
        this.jinjava = new Jinjava();
    }

    @Override
    public String getTemplateTable(FlightProducer.CallContext callContext) {
        return name;
    }

    @Override
    public HttpFlightAdaptor getHttpFlightAdaptor() {
        return httpFlightAdaptor;
    }

    private NamedQueryDefinition loadFromDb(String name) throws SQLException, NamedQueryServiceAdaptor.TemplateNotFoundException {
        String safeName = name.replace("'", "''");
        String sql = LOAD_FROM_DB_QUERY.formatted(this.name, safeName);
        try (DuckDBConnection connection = ConnectionPool.getConnection()) {
            var iter = ConnectionPool.collectAll(connection, sql, NamedQueryDefinition.class).iterator();
            if (!iter.hasNext()) {
                throw new NamedQueryServiceAdaptor.TemplateNotFoundException("Named query not found: " + name);
            }
            return iter.next();
        }
    }

    @Override
    public void getStreamNamedQuery(String name, Map<String, String> parameters,
                                    FlightProducer.CallContext context,
                                    FlightProducer.ServerStreamListener listener) {
        try {
            NamedQueryDefinition queryTemplate = loadFromDb(name);
            runValidators(queryTemplate, parameters);
            Map<String, Object> jinjavaContext = new HashMap<>(parameters != null ? parameters : Map.of());
            String sql = jinjava.render(queryTemplate.template(), jinjavaContext);
            logger.debug("Rendered SQL for named query '{}': {}", name, sql);
            var id = StatementHandle.nextStatementId();
            var statementHandle = StatementHandle.newStatementHandle(id, sql, httpFlightAdaptor.getProducerId(), -1);
            var ticket = FlightSql.TicketStatementQuery.newBuilder()
                    .setStatementHandle(ByteString.copyFrom(MAPPER.writeValueAsBytes(statementHandle)))
                    .build();
            httpFlightAdaptor.getStreamStatement(ticket, context, listener);
        } catch (NamedQueryServiceAdaptor.TemplateNotFoundException e) {
            listener.error(org.apache.arrow.flight.CallStatus.NOT_FOUND
                    .withDescription(e.getMessage()).toRuntimeException());
        } catch (io.dazzleduck.sql.common.ParameterValidationException e) {
            listener.error(org.apache.arrow.flight.CallStatus.INVALID_ARGUMENT
                    .withDescription(e.getMessage()).toRuntimeException());
        } catch (Exception e) {
            listener.error(e);
        }
    }

    private void runValidators(NamedQueryDefinition queryTemplate, Map<String, String> parameters)
            throws ParameterValidationException {
        String[] validatorClassNames = queryTemplate.validators();
        if (validatorClassNames == null || validatorClassNames.length == 0) {
            return;
        }
        Map<String, String> safeParams = parameters != null ? parameters : Map.of();
        List<String> errors = new ArrayList<>();
        for (String className : validatorClassNames) {
            try {
                getOrCreateValidator(className).validate(safeParams);
            } catch (ParameterValidationException e) {
                errors.add(e.getMessage());
            } catch (RuntimeException e) {
                errors.add("Validator configuration error: " + className + " — " + e.getMessage());
            }
        }
        if (!errors.isEmpty()) {
            throw new ParameterValidationException(String.join("; ", errors));
        }
    }

    private NamedQueryParameterValidator getOrCreateValidator(String className) {
        NamedQueryParameterValidator cached = validatorCache.get(className);
        if (cached != null) {
            return cached;
        }
        NamedQueryParameterValidator instance = instantiateValidator(className);
        if (validatorCache.size() < MAX_VALIDATOR_CACHE_SIZE) {
            NamedQueryParameterValidator existing = validatorCache.putIfAbsent(className, instance);
            return existing != null ? existing : instance;
        }
        return instance;
    }

    private NamedQueryParameterValidator instantiateValidator(String className) {
        try {
            return (NamedQueryParameterValidator) Class.forName(className)
                    .getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate validator: " + className, e);
        }
    }
}

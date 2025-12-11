package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.flight.optimizer.QueryOptimizer;
import org.duckdb.DuckDBResultSet;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public interface OptionalResultSetSupplier {
    boolean hasResultSet();

    DuckDBResultSet get() throws SQLException;

    void execute() throws SQLException;

    static OptionalResultSetSupplier of(final Statement statement, String query, QueryOptimizer queryOptimizer) {
        return new OptionalResultSetSupplier() {
            boolean hasResultSet;

            @Override
            public boolean hasResultSet() {
                return hasResultSet;
            }

            @Override
            public DuckDBResultSet get() throws SQLException {
                return (DuckDBResultSet) statement.getResultSet();
            }

            @Override
            public void execute() throws SQLException {
                var optimizedQuery = queryOptimizer.optimize(query);
                hasResultSet = statement.execute(optimizedQuery);
            }
        };
    }

    static OptionalResultSetSupplier of(PreparedStatement preparedStatement) {
        return new OptionalResultSetSupplier() {
            boolean hasResultSet;

            @Override
            public boolean hasResultSet() {
                return hasResultSet;
            }

            @Override
            public DuckDBResultSet get() throws SQLException {
                return (DuckDBResultSet) preparedStatement.getResultSet();
            }

            @Override
            public void execute() throws SQLException {
                hasResultSet = preparedStatement.execute();
            }
        };
    }
}

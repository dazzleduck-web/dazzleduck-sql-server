package io.dazzleduck.sql.flight.server;

import org.duckdb.DuckDBResultSet;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public interface OptionalResultSetSupplier {
    boolean hasResultSet();

    DuckDBResultSet get() throws SQLException;

    void execute() throws SQLException;

    static OptionalResultSetSupplier of(final StatementContext<Statement> statementContext, String query) {
        return new OptionalResultSetSupplier() {
            boolean hasResultSet;

            @Override
            public boolean hasResultSet() {
                return hasResultSet;
            }

            @Override
            public DuckDBResultSet get() throws SQLException {
                return (DuckDBResultSet) statementContext.getStatement().getResultSet();
            }

            @Override
            public void execute() throws SQLException {
                statementContext.start();
                hasResultSet = statementContext.getStatement().execute(query);
            }
        };
    }

    static OptionalResultSetSupplier of(StatementContext<PreparedStatement> preparedStatementStatementContext) {
        return new OptionalResultSetSupplier() {
            boolean hasResultSet;

            @Override
            public boolean hasResultSet() {
                return hasResultSet;
            }

            @Override
            public DuckDBResultSet get() throws SQLException {
                return (DuckDBResultSet) preparedStatementStatementContext.getStatement().getResultSet();
            }

            @Override
            public void execute() throws SQLException {
                preparedStatementStatementContext.start();
                hasResultSet = preparedStatementStatementContext.getStatement().execute();
            }
        };
    }
}

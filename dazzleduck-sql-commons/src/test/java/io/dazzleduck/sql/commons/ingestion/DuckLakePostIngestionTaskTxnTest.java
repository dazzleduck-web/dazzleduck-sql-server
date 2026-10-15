package io.dazzleduck.sql.commons.ingestion;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@link DuckLakePostIngestionTask#executeInTxn} decides what the queue may do with a batch's files:
 * {@link FilesNotRegisteredException} means nothing was registered (delete them, retry the batch);
 * a plain {@link SQLException} from the commit means the outcome is unknown (keep them, never retry).
 * So the exception it throws must be the one that decided the outcome, whatever fails after it.
 * A stub connection makes each step fail on demand.
 */
class DuckLakePostIngestionTaskTxnTest {

    /** Records calls; throws from the named steps. */
    private static final class Stub {
        final List<String> calls = new ArrayList<>();
        boolean failStatement, failCommit, failRollback, failRestoreAutoCommit;
        boolean autoCommit = true;

        Connection connection() {
            return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Connection.class},
                    (proxy, method, args) -> switch (method.getName()) {
                        case "getAutoCommit" -> autoCommit;
                        case "setAutoCommit" -> {
                            boolean value = (Boolean) args[0];
                            calls.add("setAutoCommit(" + value + ")");
                            if (value && failRestoreAutoCommit) throw new SQLException("autocommit failed");
                            autoCommit = value;
                            yield null;
                        }
                        case "createStatement" -> statement();
                        case "commit" -> {
                            calls.add("commit");
                            if (failCommit) throw new SQLException("commit failed");
                            yield null;
                        }
                        case "rollback" -> {
                            calls.add("rollback");
                            if (failRollback) throw new SQLException("rollback failed");
                            yield null;
                        }
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
        }

        private Statement statement() {
            return (Statement) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Statement.class},
                    (proxy, method, args) -> switch (method.getName()) {
                        case "addBatch", "close" -> null;
                        case "executeBatch" -> {
                            calls.add("executeBatch");
                            if (failStatement) throw new SQLException("invalid partition value");
                            yield new int[0];
                        }
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
        }
    }

    private static final List<String> FILES = List.of("/data/f1.parquet");

    @Test
    void successCommitsAndRestoresAutoCommit() throws Exception {
        var stub = new Stub();
        DuckLakePostIngestionTask.executeInTxn(stub.connection(), List.of("q"), FILES);
        assertEquals(List.of("setAutoCommit(false)", "executeBatch", "commit", "setAutoCommit(true)"), stub.calls);
    }

    @Test
    void aRejectedStatementIsFilesNotRegisteredEvenWhenItsRollbackFails() {
        var stub = new Stub();
        stub.failStatement = true;
        stub.failRollback = true;
        var e = assertThrows(FilesNotRegisteredException.class,
                () -> DuckLakePostIngestionTask.executeInTxn(stub.connection(), List.of("q"), FILES));
        assertEquals(FILES, e.files());
        assertEquals("invalid partition value", e.getCause().getMessage(), "the rejection reason is kept");
        assertEquals("rollback failed", e.getSuppressed()[0].getMessage());
        assertFalse(stub.calls.contains("commit"));
    }

    @Test
    void aFailedCommitIsRolledBackAndReportedAsItselfWhenRestoringAutoCommitFails() {
        var stub = new Stub();
        stub.failCommit = true;
        stub.failRestoreAutoCommit = true;
        var e = assertThrows(SQLException.class,
                () -> DuckLakePostIngestionTask.executeInTxn(stub.connection(), List.of("q"), FILES));
        assertEquals("commit failed", e.getMessage(), "the commit error, not the auto-commit one, is thrown");
        assertEquals("autocommit failed", e.getSuppressed()[0].getMessage());
        assertEquals(List.of("setAutoCommit(false)", "executeBatch", "commit", "rollback", "setAutoCommit(true)"), stub.calls);
    }

    @Test
    void aFailedCommitKeepsItsErrorWhenTheRollbackFailsToo() {
        var stub = new Stub();
        stub.failCommit = true;
        stub.failRollback = true;
        var e = assertThrows(SQLException.class,
                () -> DuckLakePostIngestionTask.executeInTxn(stub.connection(), List.of("q"), FILES));
        assertEquals("commit failed", e.getMessage());
        assertEquals("rollback failed", e.getSuppressed()[0].getMessage());
    }

    @Test
    void failingToRestoreAutoCommitAfterASuccessIsReported() {
        var stub = new Stub();
        stub.failRestoreAutoCommit = true;
        var e = assertThrows(SQLException.class,
                () -> DuckLakePostIngestionTask.executeInTxn(stub.connection(), List.of("q"), FILES));
        assertEquals("autocommit failed", e.getMessage());
    }
}

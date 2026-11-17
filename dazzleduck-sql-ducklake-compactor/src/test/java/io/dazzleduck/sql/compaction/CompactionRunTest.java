package io.dazzleduck.sql.compaction;

import org.junit.jupiter.api.Test;

import static io.dazzleduck.sql.compaction.CompactionRun.FailureClass.*;
import static org.junit.jupiter.api.Assertions.*;

class CompactionRunTest {

    @Test
    void classifiesCommitTimeout() {
        assertEquals(COMMIT_TIMEOUT, CompactionRun.classify(
                new RuntimeException("Failed to execute query \"ROLLBACK\"")));
        assertEquals(COMMIT_TIMEOUT, CompactionRun.classify(
                new RuntimeException("connection died: idle_in_transaction_session_timeout exceeded")));
    }

    @Test
    void classifiesTheOtherFamilies() {
        assertEquals(TRANSACTION_CONFLICT, CompactionRun.classify(new RuntimeException("Transaction conflict on table x")));
        assertEquals(OUT_OF_MEMORY, CompactionRun.classify(new RuntimeException("Out of Memory Error: failed to allocate")));
        assertEquals(SPILL_EXCEEDED, CompactionRun.classify(new RuntimeException("failed to offload data block to disk")));
        assertEquals(CATALOG_UNAVAILABLE, CompactionRun.classify(new RuntimeException("Connection refused to catalog")));
        assertEquals(OTHER, CompactionRun.classify(new RuntimeException("something unexpected")));
    }

    @Test
    void classifyWalksTheCauseChain() {
        Throwable t = new RuntimeException("wrapper", new IllegalStateException("Out of Memory Error"));
        assertEquals(OUT_OF_MEMORY, CompactionRun.classify(t));
    }

    @Test
    void truncatesLongErrorToRootCause() {
        String longMsg = "x".repeat(500);
        Throwable t = new RuntimeException("outer", new RuntimeException(longMsg));
        String truncated = CompactionRun.truncateError(t);
        assertTrue(truncated.length() <= 201, "should be ~200 chars + ellipsis");
        assertTrue(truncated.startsWith("x"));
        assertTrue(truncated.endsWith("…"));
    }

    @Test
    void classifiesPlanningErrors() {
        assertEquals(PLANNING, CompactionRun.classify(new RuntimeException("Binder Error: table x does not exist")));
        assertEquals(PLANNING, CompactionRun.classify(new RuntimeException(
                "IO Error: Failed to get compaction file list from DuckLake: no such file")));
    }

    @Test
    void filesRetiredIsWhatTheMergeRetiredNetOfWhatItWrote() {
        assertNull(TestRuns.run(1, 0, 1, 0, null, null, CompactionRun.Outcome.EMPTY, NONE).filesRetired());
        assertEquals(9L, TestRuns.run(1, 0, 1, 0, 10L, 1L, CompactionRun.Outcome.SUCCESS, NONE).filesRetired());
    }
}

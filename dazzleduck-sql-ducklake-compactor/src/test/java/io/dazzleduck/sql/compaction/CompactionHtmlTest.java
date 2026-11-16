package io.dazzleduck.sql.compaction;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CompactionHtmlTest {

    @Test
    void emptyLogRendersPlaceholderPage() {
        String html = CompactionHtml.renderPage(new CompactionRunLog(50), 5);
        assertTrue(html.startsWith("<!DOCTYPE html>"));
        assertTrue(html.contains("http-equiv=\"refresh\" content=\"5\""));
        assertTrue(html.contains("No table calls recorded yet"));
    }

    @Test
    void rendersCardWithOutcomeBadgeAndSparklines() {
        CompactionRunLog log = new CompactionRunLog(50);
        log.record(TestRuns.run(1, 0, 2, 5000, 10L, 1L, CompactionRun.Outcome.SUCCESS, CompactionRun.FailureClass.NONE));
        log.record(TestRuns.run(2, 10, 13, 5000, 10L, 1L, CompactionRun.Outcome.SUCCESS, CompactionRun.FailureClass.NONE));
        String html = CompactionHtml.renderPage(log, 5);

        assertTrue(html.contains("b-success"), "outcome badge");
        assertTrue(html.contains("tier"), "tier name");
        assertTrue(html.contains("<svg"), "sparklines rendered");
        assertTrue(html.contains("throughput"));
        assertTrue(html.contains("main.events"), "the table the latest call merged");
        assertTrue(html.contains("4 of 12"), "the call's cap against the static cap");
        assertTrue(html.contains("headroom"));
    }

    @Test
    void failedCardShowsClassChipAndError() {
        CompactionRunLog log = new CompactionRunLog(50);
        log.record(TestRuns.run(1, 0, 1, 100, 10L, 1L, CompactionRun.Outcome.FAILED, CompactionRun.FailureClass.OUT_OF_MEMORY));
        String html = CompactionHtml.renderPage(log, 5);
        assertTrue(html.contains("b-failed"));
        assertTrue(html.contains("OUT_OF_MEMORY"));
    }

    @Test
    void escapesHtmlInErrorMessage() {
        CompactionRunLog log = new CompactionRunLog(50);
        CompactionRun base = TestRuns.run(1, 0, 1, 100, null, null,
                CompactionRun.Outcome.FAILED, CompactionRun.FailureClass.OTHER);
        CompactionRun run = new CompactionRun(1, "db", "tier", "main.<b>t</b>", base.startedAt(), base.endedAt(),
                100, null, null, 4, 12, 0, null, null, 5, 5,
                CompactionRun.Outcome.FAILED, CompactionRun.FailureClass.OTHER, "<script>alert(1)</script>",
                -1, -1, -1, -1, -1, TestRuns.TIMEOUT_MS);
        log.record(run);
        String html = CompactionHtml.renderPage(log, 5);
        assertFalse(html.contains("<script>alert(1)</script>"));
        assertFalse(html.contains("<b>t</b>"), "the table name is escaped too");
        assertTrue(html.contains("&lt;script&gt;"));
    }
}

package io.dazzleduck.sql.compaction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class HealthServerTest {

    private HealthServer server;
    private final HttpClient client = HttpClient.newHttpClient();

    @AfterEach
    void tearDown() {
        if (server != null) server.close();
    }

    // Tier name "tier" and database "db" match TestRuns.run(...), so the /health per-tier lookup finds
    // the recorded run and attaches its telemetry.
    private static CompactionStats statsWithTier() {
        var loop = new CompactionStats.TierStatus("RUNNING", null, Instant.now(), 7, 2, 1, List.of("main.bad"),
                List.of(), List.of(), List.of("main.huge"), List.of("main.tiny"), 600_000, 4, 1024,
                List.of(new CompactionStats.TableStatus("main.tiny", 98_000, 490_000_000, 2_400, 1, false,
                        Instant.parse("2026-11-16T19:28:33Z"), "SUCCESS", Instant.parse("2026-11-16T19:28:33Z"), 0,
                        null, 6, null, List.of("stale", "reduced"))));
        var ds = new CompactionStats.DatabaseStats(
                Map.of("tier", 1L), 0, 5, 2, Instant.now(), Map.of("tier", loop), Map.of("tier", 3L), 3, 42);
        return new CompactionStats(Instant.now(), Map.of("db", ds), List.of("db tier 'tier' backs off \"main.bad\""),
                new CompactionStats.SpillStatus("/tmp/ducklake-compactor", 20_000_000_000L, 13_600_000_000L, 1_024,
                        19_000_000_000L));
    }

    private String get(String path) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(
                URI.create("http://localhost:" + serverPort() + path)).GET().build();
        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode());
        return resp.body();
    }

    private int serverPort;
    private int serverPort() { return serverPort; }

    @Test
    void healthJsonCarriesPerTierTelemetryWhenRunsExist() throws Exception {
        CompactionRunLog log = new CompactionRunLog(50);
        log.record(TestRuns.run(1, 0, 2, 5000, 10L, 1L, CompactionRun.Outcome.SUCCESS, CompactionRun.FailureClass.NONE));
        server = new HealthServer(0, HealthServerTest::statsWithTier, log);
        server.start();
        serverPort = portOf(server);

        String health = get("/health");
        assertTrue(health.contains("\"status\": \"UP\""));
        assertTrue(health.contains("\"telemetry\""), "per-tier telemetry section present");
        assertTrue(health.contains("\"spill\": {\"directory\": \"/tmp/ducklake-compactor\", \"budgetBytes\": 20000000000,"
                + " \"allocatedBytes\": 13600000000, \"usedBytes\": 1024, \"freeBytes\": 19000000000}"), health);
        assertTrue(health.contains("\"throughputFilesPerSec\""), "derived aggregates present");
        assertTrue(health.contains("\"table\": \"main.events\""), "the latest call's table");
        assertTrue(health.contains("\"state\": \"RUNNING\""), "the tier loop's state");
        assertTrue(health.contains("\"carryOver\": 1"));
        assertTrue(health.contains("\"backoffs\": [\"main.bad\"]"));
        assertTrue(health.contains("\"duckdbMemoryBytes\": 1024"));
        assertTrue(health.contains("\"oversized\": [\"main.huge\"]"));
        assertTrue(health.contains("\"reduced\": [\"main.tiny\"]"), "tables merged with a lowered target");
        assertTrue(health.contains("{\"table\": \"main.tiny\", \"filesWaiting\": 98000, \"bytesWaiting\": 490000000,"
                + " \"oldestWaitSeconds\": 2400, \"busyPartitions\": 1, \"sorted\": false,"
                + " \"lastCall\": \"2026-11-16T19:28:33Z\", \"lastOutcome\": \"SUCCESS\","
                + " \"lastSuccess\": \"2026-11-16T19:28:33Z\", \"consecutiveFailures\": 0, \"backoffUntil\": null,"
                + " \"capLimit\": 6, \"groupFilesLimit\": null, \"flags\": [\"stale\", \"reduced\"]}"), health);
        assertTrue(health.contains("\"unexpiredSnapshots\": 42"));
        assertTrue(health.contains("\"alerts\": [\"db tier 'tier' backs off \\\"main.bad\\\"\"]"),
                "alerts are listed, with quotes escaped: " + health);
        assertTrue(health.contains("\"outcome\": \"SUCCESS\""));
        assertTrue(health.contains("\"totalFilesRewritten\": 2"), "files rewritten by housekeeping reported");
    }

    @Test
    void uiServesHtmlDashboard() throws Exception {
        CompactionRunLog log = new CompactionRunLog(50);
        log.record(TestRuns.run(1, 0, 2, 5000, 10L, 1L, CompactionRun.Outcome.SUCCESS, CompactionRun.FailureClass.NONE));
        server = new HealthServer(0, HealthServerTest::statsWithTier, log);
        server.start();
        serverPort = portOf(server);

        String ui = get("/ui");
        assertTrue(ui.startsWith("<!DOCTYPE html>"));
        assertTrue(ui.contains("DuckLake Compaction Telemetry"));
    }

    @Test
    void telemetryIsNullPerTierBeforeAnyRun() throws Exception {
        CompactionRunLog log = new CompactionRunLog(50); // empty
        server = new HealthServer(0, HealthServerTest::statsWithTier, log);
        server.start();
        serverPort = portOf(server);

        String health = get("/health");
        assertTrue(health.contains("\"telemetry\": null"), "no run yet -> telemetry null, not an error");
    }

    /** The HttpServer picked an ephemeral port (0); read it back via reflection-free accessor. */
    private static int portOf(HealthServer server) {
        return server.boundPort();
    }
}

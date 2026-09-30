package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.ConnectionPool;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for RESTRICT_READ_ONLY mode.
 *
 * Verifies that:
 * - SELECT queries execute with the JWT filter automatically applied
 * - Missing filter claim is rejected at the server
 * - Non-SELECT operations are blocked
 * - Prepared-statement and schema-probe entry points are blocked
 */
public class RestrictedReadOnlyFlightSqlTest {

    static final String TEST_CATALOG = "memory";
    static final String TEST_SCHEMA = "main";
    static final String TEST_USER = "admin";

    // Server where JWT carries a filter claim (tenant_id = 'abc')
    private static ServerClient filteredClient;
    // Server where JWT carries NO filter claim — requests must be rejected
    private static ServerClient noFilterClient;
    private static Location filteredLocation;
    private static Location noFilterLocation;

    @BeforeAll
    static void setup() throws Exception {
        ConnectionPool.executeBatch(new String[]{
                // rro_orders uses 'owner_id' as its RLS column
                "CREATE TABLE rro_orders (id INT, owner_id VARCHAR, amount INT)",
                "INSERT INTO rro_orders VALUES (1,'alice',100),(2,'bob',200),(3,'alice',300),(4,'alice',400)",
                // rro_items uses 'region' as its RLS column — intentionally different column name
                "CREATE TABLE rro_items (order_id INT, region VARCHAR, name VARCHAR)",
                // item 4 is 'eu', not 'us' — cross-column cross-value row for the JOIN proof
                "INSERT INTO rro_items VALUES (1,'us','widget'),(2,'eu','gadget'),(3,'us','thing'),(4,'eu','cross')"
        });

        var utils = FlightTestUtils.createForDatabaseSchema(TEST_USER, "password", TEST_CATALOG, TEST_SCHEMA);

        // Per-table access rules using [[table, projection, filter], ...] format.
        // The two tables have DIFFERENT filter columns, proving independent per-arm injection.
        // projection="*" means all columns; "true" means no row restriction.
        String tableAccess = "[[\"table\",\"rro_orders\",\"*\",\"owner_id = 'alice'\"],[\"table\",\"rro_items\",\"*\",\"region = 'us'\"]]";

        filteredLocation = FlightTestUtils.findNextLocation();
        filteredClient = utils.createRestrictReadOnlyServerClient(
                filteredLocation,
                Map.of(Headers.HEADER_ACCESS, tableAccess));

        noFilterLocation = FlightTestUtils.findNextLocation();
        noFilterClient = utils.createRestrictReadOnlyServerClient(noFilterLocation, Map.of());
    }

    @AfterAll
    static void cleanup() throws Exception {
        if (filteredClient != null) filteredClient.close();
        if (noFilterClient != null) noFilterClient.close();
        ConnectionPool.executeBatch(new String[]{
                "DROP TABLE IF EXISTS rro_orders",
                "DROP TABLE IF EXISTS rro_items"
        });
    }

    // ── Filter enforcement ────────────────────────────────────────────────────

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void filterApplied_singleTable() throws Exception {
        // orders: alice rows are ids 1,3,4
        String expected = "SELECT id FROM rro_orders WHERE owner_id = 'alice' ORDER BY id";
        FlightTestUtils.testQuery(expected, "SELECT id FROM rro_orders ORDER BY id",
                filteredClient.flightSqlClient(), filteredClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void filterApplied_crossJoinBothArms() throws Exception {
        // Tables have DIFFERENT RLS columns: rro_orders.owner_id vs rro_items.region.
        // A single shared filter expression could not cover both — this proves
        // each arm gets its own independent filter.
        //
        //   alice orders (ids 1,3,4) = 3 rows  ← filtered by owner_id = 'alice'
        //   us    items  (ids 1,3)   = 2 rows  ← filtered by region   = 'us'
        //   CROSS JOIN = 3 × 2 = 6 rows
        //
        //   only orders filtered  → 3 × 4 = 12  ✗
        //   only items  filtered  → 4 × 2 =  8  ✗
        //   no filtering          → 4 × 4 = 16  ✗
        String expected = "SELECT o.id, i.order_id FROM rro_orders o CROSS JOIN rro_items i " +
                          "WHERE o.owner_id = 'alice' AND i.region = 'us' " +
                          "ORDER BY o.id, i.order_id";
        FlightTestUtils.testQuery(expected,
                "SELECT o.id, i.order_id FROM rro_orders o CROSS JOIN rro_items i " +
                "ORDER BY o.id, i.order_id",
                filteredClient.flightSqlClient(), filteredClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void filterApplied_existingWhereClausePreserved() throws Exception {
        // User's WHERE clause AND the injected filter must both apply
        String expected = "SELECT id FROM rro_orders WHERE owner_id = 'alice' AND amount > 200 ORDER BY id";
        FlightTestUtils.testQuery(expected,
                "SELECT id FROM rro_orders WHERE amount > 200 ORDER BY id",
                filteredClient.flightSqlClient(), filteredClient.clientAllocator());
    }

    // ── Missing filter claim rejected ─────────────────────────────────────────

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void noFilterClaim_queryRejected() {
        assertThrows(FlightRuntimeException.class,
                () -> noFilterClient.flightSqlClient().execute("SELECT * FROM rro_orders"));
    }

    // ── SELECT-only enforcement ───────────────────────────────────────────────

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void selectAllowed() throws Exception {
        FlightTestUtils.testQuerySuccess("SELECT 1",
                filteredClient.flightSqlClient(), filteredClient.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void explainAllowed() throws Exception {
        FlightTestUtils.testQuerySuccess("EXPLAIN SELECT * FROM rro_orders",
                filteredClient.flightSqlClient(), filteredClient.clientAllocator());
    }

    // ── Prepared statement: SELECT allowed, filter injected at create time ───────

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void preparedStatement_selectFiltered() throws Exception {
        // Filter injected at createPreparedStatement; execution must return only alice's rows
        String expected = "SELECT id FROM rro_orders WHERE owner_id = 'alice' ORDER BY id";
        try (var ps = filteredClient.flightSqlClient().prepare("SELECT id FROM rro_orders ORDER BY id")) {
            FlightTestUtils.testStream(expected,
                    () -> filteredClient.flightSqlClient()
                            .getStream(ps.execute().getEndpoints().get(0).getTicket()),
                    filteredClient.clientAllocator());
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void preparedStatement_insertRejected() {
        assertThrows(FlightRuntimeException.class,
                () -> filteredClient.flightSqlClient().prepare(
                        "INSERT INTO rro_orders VALUES (99, 'alice', 999)"));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void preparedStatement_dmlUpdateBlocked() throws Exception {
        // acceptPutPreparedStatementUpdate must be rejected even for a valid prepared SELECT handle
        try (var ps = filteredClient.flightSqlClient().prepare("SELECT id FROM rro_orders")) {
            assertThrows(FlightRuntimeException.class,
                    () -> ps.executeUpdate());
        }
    }

    // ── Raw-SQL schema probe still blocked ────────────────────────────────────

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void getExecuteSchema_blocked() {
        assertThrows(FlightRuntimeException.class,
                () -> filteredClient.flightSqlClient().getExecuteSchema("SELECT * FROM rro_orders"));
    }

    @Test
    void executeUpdateIsBlocked() throws Exception {
        // executeUpdate goes through acceptPutStatement, which bypasses the SELECT-only query path.
        assertThrows(FlightRuntimeException.class, () ->
                noFilterClient.flightSqlClient().executeUpdate("DELETE FROM rro_orders"));
        assertEquals(4L, ConnectionPool.collectFirst("SELECT count(*) FROM rro_orders", Long.class),
                "no rows were deleted");
    }

    @Test
    void bulkIngestIsBlocked() throws Exception {
        // executeIngest writes to an ingestion queue without going through SQL, so the
        // SELECT-only check never sees it; it must be refused by the write-access check.
        var refused = assertThrows(FlightRuntimeException.class, () -> FlightTestUtils.bulkIngest(noFilterClient, "rro_queue"));
        assertTrue(refused.getMessage().contains("No write access"), "refused by authorization: " + refused.getMessage());
    }
}

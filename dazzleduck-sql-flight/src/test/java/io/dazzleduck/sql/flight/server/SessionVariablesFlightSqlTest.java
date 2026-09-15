package io.dazzleduck.sql.flight.server;

import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.ConnectionPool;
import org.apache.arrow.flight.Location;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Integration test for the {@code x-dd-variables} JWT claim: variables travel in the signed
 * token, are applied to the per-request connection as {@code SET VARIABLE}, and are readable via
 * {@code getvariable()} in both plain queries and injected RLS filters.
 */
public class SessionVariablesFlightSqlTest {

    static final String TEST_CATALOG = "memory";
    static final String TEST_SCHEMA = "main";
    static final String TEST_USER = "admin";

    private static ServerClient client;

    @BeforeAll
    static void setup() throws Exception {
        ConnectionPool.executeBatch(new String[]{
                "CREATE TABLE sv_orders (id INT, owner_id VARCHAR, amount INT)",
                "INSERT INTO sv_orders VALUES (1,'alice',100),(2,'bob',200),(3,'alice',300)"
        });

        var utils = FlightTestUtils.createForDatabaseSchema(TEST_USER, "password", TEST_CATALOG, TEST_SCHEMA);

        // The RLS filter references the session variable instead of a baked-in literal — the
        // concrete tenant value travels as data in x-dd-variables.
        String tableAccess = "[[\"table\",\"sv_orders\",\"*\",\"owner_id = getvariable('owner')\"]]";
        String variables = "{\"owner\":\"alice\"}";

        Location location = FlightTestUtils.findNextLocation();
        client = utils.createRestrictReadOnlyServerClient(location,
                Map.of(Headers.HEADER_ACCESS, tableAccess,
                        Headers.CLAIM_SESSION_VARIABLES, variables));
    }

    @AfterAll
    static void cleanup() throws Exception {
        if (client != null) client.close();
        ConnectionPool.executeBatch(new String[]{"DROP TABLE IF EXISTS sv_orders"});
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void variableReadableViaGetvariable() throws Exception {
        // The connection built for the request has SET VARIABLE owner = 'alice' from the token.
        FlightTestUtils.testQuery("SELECT 'alice' AS owner",
                "SELECT getvariable('owner') AS owner",
                client.flightSqlClient(), client.clientAllocator());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void rlsFilterUsesSessionVariable() throws Exception {
        // Injected filter owner_id = getvariable('owner') → only alice's rows (ids 1,3).
        FlightTestUtils.testQuery(
                "SELECT id FROM sv_orders WHERE owner_id = 'alice' ORDER BY id",
                "SELECT id FROM sv_orders ORDER BY id",
                client.flightSqlClient(), client.clientAllocator());
    }
}

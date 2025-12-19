package io.dazzleduck.sql.client;

import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.common.types.JavaRow;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.runtime.Main;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.*;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class GrpcFlightSenderTest {

    private static final String HOST = "localhost";
    private static final int PORT = 55620;

    private static final String USER = "admin";
    private static final String PASSWORD = "admin";
    private static final String CATALOG = "test_catalog";
    private static final String SCHEMA = "test_schema";

    private RootAllocator allocator;
    private FlightSqlClient client;
    private String warehouse;

    @BeforeAll
    void setup() throws Exception {
        allocator = new RootAllocator(Long.MAX_VALUE);
        warehouse = Files.createTempDirectory("grpc-test").toString();
        Main.main(new String[]{
                "--conf", "dazzleduck_server.flight_sql.port=" + PORT,
                "--conf", "dazzleduck_server.flight_sql.host=localhost",
                "--conf", "dazzleduck_server.use_encryption=false",
                "--conf", "dazzleduck_server.warehouse=\"" + warehouse.replace("\\", "\\\\") + "\"",
        });


        ConnectionPool.executeBatch(new String[]{
                "INSTALL arrow FROM community",
                "LOAD arrow",
                "ATTACH ':memory:' AS test_catalog",
                "CREATE SCHEMA test_catalog.test_schema"
        });

        client = new FlightSqlClient(
                FlightClient.builder(
                        allocator,
                        Location.forGrpcInsecure(HOST, PORT))
                        .intercept(io.dazzleduck.sql.flight.server.auth2.AuthUtils
                        .createClientMiddlewareFactory(
                                        USER,
                                        PASSWORD,
                                        Map.of(
                                                Headers.HEADER_DATABASE, CATALOG,
                                                Headers.HEADER_SCHEMA, SCHEMA
                                        )
                                )
                ).build()
        );
    }

    @Test
    void ingestAndVerifyValues() throws Exception {
        Schema schema = new Schema(List.of(new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)));
        String path = warehouse + "/names.parquet";

        try (GrpcFlightSender sender = new GrpcFlightSender(
                schema,
                1024,
                Duration.ofSeconds(1),
                Clock.systemUTC(),
                5_000_000,
                20_000_000,
                allocator,
                Location.forGrpcInsecure(HOST, PORT),
                USER,
                PASSWORD,
                CATALOG,
                SCHEMA,
                Map.of("path", path)
        )) {
            sender.addRow(new JavaRow(new Object[]{"Aman"}));
            sender.addRow(new JavaRow(new Object[]{"Yash"}));
            sender.addRow(new JavaRow(new Object[]{"Sid"}));
        }

        FlightInfo flightInfo = client.execute("SELECT name FROM '" + path + "' ORDER BY name");
        List<String> values = new java.util.ArrayList<>();

        try (FlightStream stream = client.getStream(flightInfo.getEndpoints().get(0).getTicket())) {

            while (stream.next()) {
                var root = stream.getRoot();
                var nameVector = (org.apache.arrow.vector.VarCharVector)
                        root.getVector("name");

                for (int i = 0; i < root.getRowCount(); i++) {
                    values.add(nameVector.getObject(i).toString());
                }
            }
        }
        assertEquals(List.of("Aman", "Sid", "Yash"), values);
    }
}

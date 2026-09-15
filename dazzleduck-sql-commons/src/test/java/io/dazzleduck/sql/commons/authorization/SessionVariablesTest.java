package io.dazzleduck.sql.commons.authorization;

import io.dazzleduck.sql.commons.ConnectionPool;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SessionVariablesTest {

    @Test
    void rendersOneSetStatementPerEntryInOrder() {
        List<String> sqls = SessionVariables.toSetStatements(
                "{\"tenant_id\":\"acme\",\"region\":\"us-east\"}");
        assertEquals(List.of(
                "SET VARIABLE \"tenant_id\" = 'acme'",
                "SET VARIABLE \"region\" = 'us-east'"), sqls);
    }

    @Test
    void nullBlankAndEmptyObjectYieldNoStatements() {
        assertTrue(SessionVariables.toSetStatements(null).isEmpty());
        assertTrue(SessionVariables.toSetStatements("   ").isEmpty());
        assertTrue(SessionVariables.toSetStatements("{}").isEmpty());
    }

    @Test
    void numberValueIsRejectedWithQuoteHint() {
        var ex = assertThrows(IllegalArgumentException.class,
                () -> SessionVariables.toSetStatements("{\"var1\":42}"));
        assertTrue(ex.getMessage().contains("needs to be inside quotes"), ex.getMessage());
        assertTrue(ex.getMessage().contains("42"), ex.getMessage());
    }

    @Test
    void booleanValueIsRejectedWithQuoteHint() {
        var ex = assertThrows(IllegalArgumentException.class,
                () -> SessionVariables.toSetStatements("{\"active\":true}"));
        assertTrue(ex.getMessage().contains("needs to be inside quotes"), ex.getMessage());
    }

    @Test
    void quotedNumberIsAcceptedAsAString() {
        assertEquals(List.of("SET VARIABLE \"var1\" = '42'"),
                SessionVariables.toSetStatements("{\"var1\":\"42\"}"));
    }

    @Test
    void nullValuedEntriesAreSkipped() {
        assertTrue(SessionVariables.toSetStatements("{\"unset\":null}").isEmpty());
    }

    @Test
    void singleQuotesInValueAreDoubled() {
        List<String> sqls = SessionVariables.toSetStatements("{\"name\":\"O'Brien\"}");
        assertEquals(List.of("SET VARIABLE \"name\" = 'O''Brien'"), sqls);
    }

    @Test
    void sqlInjectionInValueIsNeutralizedIntoASingleLiteral() {
        // A crafted value must stay entirely inside one quoted literal, never break out into SQL.
        List<String> sqls = SessionVariables.toSetStatements(
                "{\"tenant_id\":\"x'; DROP TABLE orders; --\"}");
        assertEquals(List.of("SET VARIABLE \"tenant_id\" = 'x''; DROP TABLE orders; --'"), sqls);
    }

    @Test
    void invalidVariableNameIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> SessionVariables.toSetStatements("{\"tenant id\":\"acme\"}"));
        assertThrows(IllegalArgumentException.class,
                () -> SessionVariables.toSetStatements("{\"1tenant\":\"acme\"}"));
        assertThrows(IllegalArgumentException.class,
                () -> SessionVariables.toSetStatements("{\"drop;table\":\"x\"}"));
    }

    @Test
    void nestedJsonValueIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> SessionVariables.toSetStatements("{\"a\":{\"b\":1}}"));
        assertThrows(IllegalArgumentException.class,
                () -> SessionVariables.toSetStatements("{\"a\":[1,2]}"));
    }

    @Test
    void malformedJsonIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> SessionVariables.toSetStatements("not json"));
        assertThrows(IllegalArgumentException.class,
                () -> SessionVariables.toSetStatements("[\"a\",\"b\"]"));
    }

    @Test
    void reservedWordNamesAreRenderedAsQuotedIdentifiers() throws Exception {
        // The name regex accepts reserved words, so the identifier has to be quoted: bare
        // `SET VARIABLE table = 'x'` is a DuckDB parser error.
        for (String reserved : List.of("table", "select", "order", "from", "group", "default")) {
            var sqls = SessionVariables.toSetStatements("{\"" + reserved + "\":\"x\"}");
            assertEquals(List.of("SET VARIABLE \"" + reserved + "\" = 'x'"), sqls);
            assertExecutable(sqls, reserved, "x");
        }
    }

    @Test
    void controlCharacterInValueIsRejected() throws Exception {
        // DuckDB truncates a SQL string at an embedded NUL, which would turn the rendered statement
        // into an unterminated literal and fail the request with an unrelated parser error.
        // Build the NUL from a char rather than writing a unicode escape in the source: that
        // escape sequence does not survive being copied through tools that normalise it.
        var ex = assertThrows(IllegalArgumentException.class,
                () -> SessionVariables.toSetStatements(claimWithValue("x" + (char) 0 + "y")));
        assertTrue(ex.getMessage().contains("control character"), ex.getMessage());
        assertThrows(IllegalArgumentException.class,
                () -> SessionVariables.toSetStatements(claimWithValue("line1" + (char) 10 + "line2")));
    }

    /** {@code {"a":"<value>"}}, with value escaped by Jackson rather than written by hand. */
    private static String claimWithValue(String value) throws Exception {
        return new com.fasterxml.jackson.databind.ObjectMapper()
                .writeValueAsString(java.util.Map.of("a", value));
    }

    @Test
    void oversizedClaimsAreRejected() {
        var manyVars = new StringBuilder("{");
        for (int i = 0; i < 65; i++) {
            manyVars.append(i > 0 ? "," : "").append("\"v").append(i).append("\":\"x\"");
        }
        var tooMany = assertThrows(IllegalArgumentException.class,
                () -> SessionVariables.toSetStatements(manyVars.append("}").toString()));
        assertTrue(tooMany.getMessage().contains("Too many session variables"), tooMany.getMessage());

        var tooLong = assertThrows(IllegalArgumentException.class,
                () -> SessionVariables.toSetStatements("{\"a\":\"" + "x".repeat(4097) + "\"}"));
        assertTrue(tooLong.getMessage().contains("too long"), tooLong.getMessage());
    }

    @Test
    void renderedStatementsRunOnDuckDbAndCannotInjectSql() throws Exception {
        // Asserting the rendered string is not the same as proving it is safe: DuckDB JDBC executes
        // multiple statements in one execute(), so the escaping is what stands between the claim and
        // arbitrary SQL. Run the payloads and check nothing but the variable changed.
        try (var connection = ConnectionPool.getConnection();
             Statement setup = connection.createStatement()) {
            setup.execute("CREATE OR REPLACE TEMP TABLE injection_canary(x INT)");
            for (String payload : List.of("x'; INSERT INTO injection_canary VALUES (1); --",
                    "back\\slash'; INSERT INTO injection_canary VALUES (2); --",
                    "O'Brien",
                    "a' || (SELECT 'b') || '")) {
                var sqls = SessionVariables.toSetStatements(
                        "{\"v\":" + new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(payload) + "}");
                for (String sql : sqls) {
                    try (Statement st = connection.createStatement()) {
                        st.execute(sql);
                    }
                }
                try (Statement st = connection.createStatement();
                     var rs = st.executeQuery("SELECT getvariable('v')")) {
                    assertTrue(rs.next());
                    assertEquals(payload, rs.getString(1), "value must round-trip verbatim");
                }
            }
            try (Statement st = connection.createStatement();
                 var rs = st.executeQuery("SELECT count(*) FROM injection_canary")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1), "no payload may execute as SQL");
            }
        }
    }

    private static void assertExecutable(List<String> sqls, String name, String expected) throws SQLException {
        try (var connection = ConnectionPool.getConnection()) {
            for (String sql : sqls) {
                try (Statement st = connection.createStatement()) {
                    st.execute(sql);
                }
            }
            try (Statement st = connection.createStatement();
                 var rs = st.executeQuery("SELECT getvariable('" + name + "')")) {
                assertTrue(rs.next());
                assertEquals(expected, rs.getString(1));
            }
        }
    }
}

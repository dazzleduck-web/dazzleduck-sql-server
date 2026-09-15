package io.dazzleduck.sql.commons.authorization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.dazzleduck.sql.commons.ConnectionPool;
import io.dazzleduck.sql.commons.ExpressionConstants;
import io.dazzleduck.sql.commons.Transformations;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class SqlAuthorizerLimitTest {

    private static final SqlAuthorizer PASSTHROUGH = new SqlAuthorizer() {
        @Override
        public JsonNode authorize(String user, String database, String schema,
                                  JsonNode query, Map<String, String> verifiedClaims) {
            return query;
        }
        @Override
        public boolean hasWriteAccess(String user, String ingestionQueue,
                                      Map<String, String> verifiedClaims) {
            return false;
        }
    };

    private static long extractLimit(JsonNode authorized) {
        JsonNode statement = authorized.get(ExpressionConstants.FIELD_STATEMENTS)
                .get(0).get(ExpressionConstants.FIELD_NODE);
        ArrayNode modifiers = (ArrayNode) statement.get(ExpressionConstants.FIELD_MODIFIERS);
        for (JsonNode modifier : modifiers) {
            if (modifier.get(ExpressionConstants.FIELD_TYPE).asText()
                    .equals(ExpressionConstants.LIMIT_MODIFIER_TYPE)) {
                return modifier.get(ExpressionConstants.FIELD_LIMIT)
                        .get(ExpressionConstants.FIELD_VALUE)
                        .get(ExpressionConstants.FIELD_VALUE).asLong();
            }
        }
        throw new AssertionError("No LIMIT_MODIFIER found in: " + authorized);
    }

    @Test
    public void testAddLimit_baseTable() throws Exception {
        JsonNode query = Transformations.parseToTree("SELECT * FROM t");
        JsonNode authorized = PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 100L, -1);
        assertEquals(100L, extractLimit(authorized));
    }

    @Test
    public void testAddLimit_tableFunction() throws Exception {
        // generate_series is a TABLE_FUNCTION — previously addLimit returned the query unchanged
        JsonNode query = Transformations.parseToTree("SELECT * FROM generate_series(1, 1000)");
        JsonNode authorized = PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 3L, -1);
        assertEquals(3L, extractLimit(authorized));
    }

    @Test
    public void testAddLimit_subquery() throws Exception {
        JsonNode query = Transformations.parseToTree("SELECT * FROM (SELECT * FROM t) sub");
        JsonNode authorized = PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 5L, -1);
        assertEquals(5L, extractLimit(authorized));
    }

    @Test
    public void testAddLimit_noLimit_noOffset_unchanged() throws Exception {
        JsonNode query = Transformations.parseToTree("SELECT * FROM t");
        JsonNode authorized = PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), -1L, -1L);
        // No LIMIT modifier should be added
        JsonNode statement = authorized.get(ExpressionConstants.FIELD_STATEMENTS)
                .get(0).get(ExpressionConstants.FIELD_NODE);
        ArrayNode modifiers = (ArrayNode) statement.get(ExpressionConstants.FIELD_MODIFIERS);
        boolean hasLimit = modifiers != null && modifiers.size() > 0;
        assertEquals(false, hasLimit, "Expected no LIMIT modifier when limit=-1 and offset=-1");
    }

    // ── limit-as-ceiling regression tests ────────────────────────────────────────────────────
    // Guard for the "top users showed 16 rows under LIMIT 10" bug: the RESTRICT_READ_ONLY
    // authorizer applied the engine page cap (duckdb.max-rows) as an assignment, discarding a
    // named-query template's own LIMIT N so every user rendered.

    private static java.util.List<String> modifierTypes(JsonNode authorized) {
        JsonNode statement = authorized.get(ExpressionConstants.FIELD_STATEMENTS)
                .get(0).get(ExpressionConstants.FIELD_NODE);
        ArrayNode modifiers = (ArrayNode) statement.get(ExpressionConstants.FIELD_MODIFIERS);
        var out = new java.util.ArrayList<String>();
        if (modifiers != null) {
            for (JsonNode m : modifiers) {
                out.add(m.get(ExpressionConstants.FIELD_TYPE).asText());
            }
        }
        return out;
    }

    @Test
    public void testCap_preservesTighterTemplateLimit() throws Exception {
        // The template asks for 10; the engine cap is 1000. The template must win.
        JsonNode query = Transformations.parseToTree(
                "SELECT user_id, count(*) AS n FROM ai_txn GROUP BY user_id ORDER BY n DESC LIMIT 10");
        JsonNode authorized = PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 1000L, 0L);
        assertEquals(10L, extractLimit(authorized),
                "an engine page cap must not widen the query's own LIMIT 10");
        assertEquals(true, modifierTypes(authorized).contains("ORDER_MODIFIER"),
                "ORDER BY must survive limit capping - otherwise 'top N' loses its ordering");
    }

    @Test
    public void testCap_appliedWhenQueryLimitExceedsCap() throws Exception {
        JsonNode query = Transformations.parseToTree("SELECT * FROM t LIMIT 5000");
        JsonNode authorized = PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 1000L, 0L);
        assertEquals(1000L, extractLimit(authorized), "the cap must still bound a larger LIMIT");
    }

    @Test
    public void testCap_equalToQueryLimit() throws Exception {
        JsonNode query = Transformations.parseToTree("SELECT * FROM t LIMIT 1000");
        JsonNode authorized = PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 1000L, 0L);
        assertEquals(1000L, extractLimit(authorized));
    }

    /** The first row of the round-tripped query - over range(100) this is the effective OFFSET. */
    private static long firstValueOf(JsonNode authorized) throws Exception {
        String sql = Transformations.parseToSql(authorized);
        return ConnectionPool.collectFirst(
                "SELECT coalesce(min(range), -1) FROM (" + sql + ")", Long.class);
    }

    /** Round-trips the authorized AST back to SQL and returns the rows it actually produces. */
    private static long rowsOf(JsonNode authorized) throws Exception {
        String sql = Transformations.parseToSql(authorized);
        return ConnectionPool.collectFirst("SELECT count(*) FROM (" + sql + ")", Long.class);
    }



    @Test
    public void testCap_limitNullIsBounded() throws Exception {
        // Bare LIMIT NULL reads as *unlimited* in SQL. least() skips NULLs, so least(7, NULL) is 7
        // and the cap now binds where it previously could not.
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100) LIMIT NULL");
        JsonNode authorized = PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 7L, 0L);
        assertEquals(7L, rowsOf(authorized), "LIMIT NULL must be bounded by the cap");
    }


    @Test
    public void testCap_literalPathStaysFolded() throws Exception {
        // Both sides literal - folded in Java so the emitted SQL stays a plain LIMIT 10.
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100) LIMIT 10");
        JsonNode authorized = PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 1000L, 0L);
        String sql = Transformations.parseToSql(authorized);
        assertEquals(false, sql.toLowerCase().contains("least"),
                "a literal LIMIT must not be wrapped in least(): " + sql);
        assertEquals(10L, rowsOf(authorized));
    }

    @Test
    public void testNoCap_preservesExistingLimit() throws Exception {
        // limit < 0 means "no cap"; an OFFSET-only rewrite must not clobber the query's LIMIT.
        JsonNode query = Transformations.parseToTree("SELECT * FROM t LIMIT 10");
        JsonNode authorized = PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), -1L, 0L);
        assertEquals(10L, extractLimit(authorized));
    }

    @Test
    public void testCap_singleLimitModifierAfterCapping() throws Exception {
        JsonNode query = Transformations.parseToTree("SELECT * FROM t LIMIT 10");
        JsonNode authorized = PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 1000L, 0L);
        long limitModifiers = modifierTypes(authorized).stream()
                .filter(t -> t.equals(ExpressionConstants.LIMIT_MODIFIER_TYPE)).count();
        assertEquals(1L, limitModifiers, "capping must not leave a duplicate LIMIT modifier");
    }

    // ── offset composition ───────────────────────────────────────────────────────────────────
    // The cap path passes offset=0, which previously *replaced* a template's own OFFSET.

    @Test
    public void testOffset_templateOffsetSurvivesTheCapPath() throws Exception {
        // range(100) OFFSET 5 LIMIT 10 -> rows 6..15. The cap must not reset the offset to 0.
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100) LIMIT 10 OFFSET 5");
        JsonNode authorized = PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 1000L, 0L);
        assertEquals(10L, rowsOf(authorized));
        assertEquals(5L, firstValueOf(authorized), "the template's OFFSET 5 must survive");
    }

    @Test
    public void testOffset_requestOffsetAddsToTemplateOffset() throws Exception {
        // A request offset paginates within the query's result, which already starts after 5.
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100) OFFSET 5");
        JsonNode authorized = PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 1000L, 10L);
        assertEquals(15L, firstValueOf(authorized), "OFFSET 5 + request 10 must compose to 15");
    }

    @Test
    public void testOffset_nonLiteralOffsetComposesViaAdd() throws Exception {
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100) OFFSET 2+3");
        JsonNode authorized = PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 1000L, 10L);
        assertEquals(15L, firstValueOf(authorized), "add(2+3, 10) must compose to 15");
    }

    @Test
    public void testOffset_onlyOffsetRequestedIsNotLimitMinusOne() throws Exception {
        // limit=-1 with offset>=0 used to emit "LIMIT -1", a DuckDB binder error
        // ("LIMIT/OFFSET cannot be negative"). It must serialize as an OFFSET-only modifier.
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100)");
        JsonNode authorized = PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), -1L, 5L);
        String sql = Transformations.parseToSql(authorized);
        assertEquals(false, sql.contains("-1"), "must not emit LIMIT -1: " + sql);
        assertEquals(95L, rowsOf(authorized), "OFFSET 5 over range(100) leaves 95 rows");
    }

    @Test
    public void testOffset_absentOffsetStillOmitted() throws Exception {
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100)");
        JsonNode authorized = PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 10L, -1L);
        assertEquals(10L, rowsOf(authorized));
        assertEquals(0L, firstValueOf(authorized), "no offset requested and none in the query");
    }

    @Test
    public void testOffset_nullOffsetDoesNotSwallowTheRequestOffset() throws Exception {
        // OFFSET NULL is not a literal, so it took the add() path: add(NULL, 5) is NULL, which
        // DuckDB reads as OFFSET 0 - a paginating client got page 1 back for every page.
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100) OFFSET NULL");
        JsonNode authorized = PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 3L, 5L);
        assertEquals(5L, firstValueOf(authorized),
                "coalesce(NULL, 0) + 5 must honour the requested offset");
        assertEquals(3L, rowsOf(authorized), "the cap must still apply");
    }

    @Test
    public void testOffset_emptySubqueryOffsetDoesNotSwallowTheRequestOffset() throws Exception {
        // Same shape via a scalar subquery that evaluates to NULL.
        JsonNode query = Transformations.parseToTree(
                "SELECT * FROM range(100) OFFSET (SELECT max(x) FROM (SELECT 1 AS x) WHERE false)");
        JsonNode authorized = PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 3L, 5L);
        assertEquals(5L, firstValueOf(authorized));
    }

    // ── offset past the query's own LIMIT is rejected, not served as an empty page ────────────

    @Test
    public void testOffset_exactlyAtTemplateLimitIsAnEmptyPage() throws Exception {
        // offset == own LIMIT is the end of the result, not an error: a client paging until it
        // sees a short page must be able to request that page and get zero rows back. Rejecting
        // it ended every such scan in a server error.
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100) LIMIT 10");
        JsonNode authorized = PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 1000L, 10L);
        assertEquals(0L, rowsOf(authorized), "the page at the end of the result must be empty");
    }

    @Test
    public void testOffset_pageUntilShortPageTerminates() throws Exception {
        // The loop the previous contract broke: cap 10 over a template LIMIT 100 yields ten full
        // pages, then an empty one. No page may raise.
        long total = 0;
        for (int offset = 0; offset <= 100; offset += 10) {
            JsonNode q = Transformations.parseToTree("SELECT * FROM range(1000) LIMIT 100");
            final int off = offset;
            JsonNode authorized = assertDoesNotThrow(() -> PASSTHROUGH.authorize(
                    "user", "db", "schema", q, Map.of(), 10L, (long) off),
                    "offset " + offset + " must not raise");
            total += rowsOf(authorized);
        }
        assertEquals(100L, total, "ten pages of 10 then an empty page");
    }

    @Test
    public void testOffset_pastTemplateLimitIsRejected() throws Exception {
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100) LIMIT 10");
        assertThrows(IllegalArgumentException.class, () ->
                PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 1000L, 25L));
    }

    @Test
    public void testOffset_lastRowWithinTemplateLimitIsAllowed() throws Exception {
        // offset 9 against LIMIT 10 is the final row - valid, must not be rejected.
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100) LIMIT 10");
        JsonNode authorized = PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 1000L, 9L);
        assertEquals(1L, rowsOf(authorized), "offset 9 of LIMIT 10 must return the last row");
        assertEquals(9L, firstValueOf(authorized));
    }

    @Test
    public void testOffset_paginatingWithinALargerTemplateLimitIsAllowed() throws Exception {
        // The cap is a page size, not the result size: paging inside the query's own 100-row
        // bound with a 10-row cap is legitimate and must not be rejected.
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(1000) LIMIT 100");
        JsonNode authorized = PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 10L, 10L);
        assertEquals(10L, rowsOf(authorized));
        assertEquals(10L, firstValueOf(authorized));
    }

    @Test
    public void testOffset_noTemplateLimitIsNeverRejected() throws Exception {
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100)");
        assertDoesNotThrow(() ->
                PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 1000L, 50L));
    }




    @Test
    public void testOffset_templateOffsetAndRequestOffsetWithOwnLimit() throws Exception {
        // LIMIT 10 OFFSET 5 -> rows 5..14. Request offset 3 -> rows 8..14, i.e. 7 rows.
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100) LIMIT 10 OFFSET 5");
        JsonNode authorized = PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 1000L, 3L);
        assertEquals(7L, rowsOf(authorized), "10 own rows minus 3 skipped leaves 7");
        assertEquals(8L, firstValueOf(authorized), "own OFFSET 5 + request 3");
    }

    // ── the split: each method touches only its own field of the shared LIMIT_MODIFIER ───────

    private static JsonNode limitModifier(JsonNode tree) {
        JsonNode statement = tree.get(ExpressionConstants.FIELD_STATEMENTS)
                .get(0).get(ExpressionConstants.FIELD_NODE);
        ArrayNode modifiers = (ArrayNode) statement.get(ExpressionConstants.FIELD_MODIFIERS);
        for (JsonNode m : modifiers) {
            if (m.get(ExpressionConstants.FIELD_TYPE).asText()
                    .equals(ExpressionConstants.LIMIT_MODIFIER_TYPE)) {
                return m;
            }
        }
        throw new AssertionError("no LIMIT_MODIFIER in " + tree);
    }

    @Test
    public void testCapLimit_doesNotTouchTheOffset() throws Exception {
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100) LIMIT 50 OFFSET 5");
        JsonNode out = Transformations.capLimit(query, 10L);
        assertEquals(10L, extractLimit(out), "cap must bound the limit");
        assertEquals(5L, limitModifier(out).get(ExpressionConstants.FIELD_OFFSET)
                .get(ExpressionConstants.FIELD_VALUE)
                .get(ExpressionConstants.FIELD_VALUE).asLong(),
                "capLimit must leave OFFSET 5 untouched");
    }

    @Test
    public void testApplyOffset_doesNotInventACap() throws Exception {
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100)");
        JsonNode out = Transformations.applyOffset(query, 5L);
        assertEquals(true, limitModifier(out).get(ExpressionConstants.FIELD_LIMIT).isNull(),
                "applyOffset must leave the limit as JSON null, not fabricate one");
        assertEquals(95L, rowsOf(out), "OFFSET 5 over range(100) leaves 95 rows, uncapped");
    }

    @Test
    public void testCapLimit_negativeCapIsANoOp() throws Exception {
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100) LIMIT 7");
        JsonNode out = Transformations.capLimit(query, -1L);
        assertEquals(7L, extractLimit(out));
        assertEquals(7L, rowsOf(out));
    }

    @Test
    public void testApplyOffset_negativeOffsetIsANoOp() throws Exception {
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100) LIMIT 7");
        JsonNode out = Transformations.applyOffset(query, -1L);
        assertEquals(7L, rowsOf(out));
        assertEquals(0L, firstValueOf(out), "no offset must be applied");
    }

    @Test
    public void testAddLimit_matchesApplyOffsetThenCapLimit() throws Exception {
        // The delegate must equal the hand-composed pair, in that order.
        String sql = "SELECT * FROM range(100) LIMIT 20 OFFSET 4";
        JsonNode viaDelegate = Transformations.addLimit(Transformations.parseToTree(sql), 5L, 3L);
        JsonNode viaSplit = Transformations.capLimit(
                Transformations.applyOffset(Transformations.parseToTree(sql), 3L), 5L);
        assertEquals(Transformations.parseToSql(viaSplit), Transformations.parseToSql(viaDelegate));
        assertEquals(5L, rowsOf(viaDelegate), "min(cap 5, 20 own - 3 skipped)");
        assertEquals(7L, firstValueOf(viaDelegate), "own OFFSET 4 + request 3");
    }

    @Test
    public void testApplyOffset_rejectionIsRaisedByTheOffsetMethod() throws Exception {
        // The guard belongs to offset semantics, so applyOffset alone must raise it. 11 is
        // strictly past the query's own LIMIT 10; 10 itself is the allowed empty page.
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100) LIMIT 10");
        assertThrows(IllegalArgumentException.class, () -> Transformations.applyOffset(query, 11L));
    }

    // ── LIMIT n% is rejected when a cap or offset applies ────────────────────────────────────

    @Test
    public void testPercentLimit_isRejectedUnderACap() throws Exception {
        // Previously produced LIMIT (3) % LIMIT 5 -> Parser Error. Now a clear 400.
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100) LIMIT 3%");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 5L, 0L));
        assertEquals(true, e.getMessage().contains("'LIMIT n%' is not supported"),
                "message must name the unsupported construct: " + e.getMessage());
    }

    @Test
    public void testPercentLimit_isRejectedByCapLimitDirectly() throws Exception {
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100) LIMIT 3%");
        assertThrows(IllegalArgumentException.class, () -> Transformations.capLimit(query, 5L));
    }

    @Test
    public void testPercentLimit_isRejectedByApplyOffsetDirectly() throws Exception {
        // An offset alone would also append a second LIMIT modifier and corrupt the SQL.
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100) LIMIT 3%");
        assertThrows(IllegalArgumentException.class, () -> Transformations.applyOffset(query, 5L));
    }

    @Test
    public void testPercentLimit_isLeftAloneWhenNoCapApplies() throws Exception {
        // No cap, no offset -> nothing is being applied, so the query must pass through and run.
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100) LIMIT 3%");
        JsonNode out = assertDoesNotThrow(() -> Transformations.capLimit(query, -1L));
        assertEquals(3L, rowsOf(out), "3% of 100 rows must still work uncapped");
    }

    @Test
    public void testPercentLimit_neverEmitsUnparseableSql() throws Exception {
        // Guard the specific corruption: two LIMIT modifiers on one SELECT.
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100) LIMIT 3%");
        try {
            PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 5L, 0L);
        } catch (IllegalArgumentException expected) {
            return; // rejected before any SQL could be produced
        }
        throw new AssertionError("a percent limit under a cap must be rejected, not serialized");
    }

    @Test
    public void testCap_limitNullWithAnOffsetStaysUnlimited() throws Exception {
        // LIMIT NULL means *unlimited*, so there is nothing for the offset to subtract from.
        // It parses as a CONSTANT whose value is NULL (not JSON null), so it previously took the
        // arithmetic path: greatest(subtract(NULL, 5), 0) collapsed to LIMIT 0 - an empty page
        // for a query that asked for every row.
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100) LIMIT NULL");
        JsonNode authorized = PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 3L, 5L);
        assertEquals(3L, rowsOf(authorized), "cap 3 over an unlimited query must yield 3 rows");
        assertEquals(5L, firstValueOf(authorized), "the request offset must still apply");
    }

    @Test
    public void testCap_limitNullWithOffsetAndNoCap() throws Exception {
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100) LIMIT NULL");
        JsonNode authorized = PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), -1L, 5L);
        assertEquals(95L, rowsOf(authorized), "unlimited minus 5 skipped rows leaves 95");
    }

    // ── a non-literal LIMIT is rejected when a cap or offset applies ─────────────────────────
    // Bounding one meant reproducing SQL's NULL / unlimited semantics in AST arithmetic, which
    // produced a new edge case per limit form. Rejecting tells the caller the query cannot be
    // paginated instead of handing back a plausible wrong page.

    @Test
    public void testNonLiteralLimit_expressionIsRejectedUnderACap() throws Exception {
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100) LIMIT 1+1");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 1000L, 0L));
        assertEquals(true, e.getMessage().contains("must be an integer literal"),
                "message must say what is required: " + e.getMessage());
    }

    @Test
    public void testNonLiteralLimit_scalarSubqueryIsRejected() throws Exception {
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100) LIMIT (SELECT 3)");
        assertThrows(IllegalArgumentException.class, () ->
                PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 1000L, 0L));
    }

    @Test
    public void testNonLiteralLimit_rejectedByCapLimitAndApplyOffsetAlike() throws Exception {
        JsonNode q1 = Transformations.parseToTree("SELECT * FROM range(100) LIMIT 5+5");
        assertThrows(IllegalArgumentException.class, () -> Transformations.capLimit(q1, 1000L));
        JsonNode q2 = Transformations.parseToTree("SELECT * FROM range(100) LIMIT 5+5");
        assertThrows(IllegalArgumentException.class, () -> Transformations.applyOffset(q2, 7L));
    }

    @Test
    public void testNonLiteralLimit_overrunNoLongerSilentlyEmpty() throws Exception {
        // Closes the inconsistency: LIMIT 5+5 OFFSET 20 used to return a silent empty page while
        // LIMIT 10 OFFSET 20 raised. Both now raise.
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100) LIMIT 5+5");
        assertThrows(IllegalArgumentException.class, () ->
                PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 1000L, 20L));
    }

    @Test
    public void testNonLiteralLimit_isLeftAloneWhenNothingApplies() throws Exception {
        // No cap, no offset -> nothing is being bounded, so the query must pass through and run.
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100) LIMIT 5+5");
        JsonNode out = assertDoesNotThrow(() -> Transformations.capLimit(query, -1L));
        assertEquals(10L, rowsOf(out), "LIMIT 5+5 must still work when uncapped");
    }

    @Test
    public void testNonLiteralLimit_unlimitedFormsRemainAllowed() throws Exception {
        // An absent LIMIT and an explicit LIMIT NULL both mean "every row" - exactly the case the
        // cap handles, so there is no arithmetic to get wrong and no reason to reject.
        JsonNode absent = Transformations.parseToTree("SELECT * FROM range(100)");
        assertEquals(3L, rowsOf(PASSTHROUGH.authorize(
                "user", "db", "schema", absent, Map.of(), 3L, 5L)));
        JsonNode explicitNull = Transformations.parseToTree("SELECT * FROM range(100) LIMIT NULL");
        assertEquals(3L, rowsOf(PASSTHROUGH.authorize(
                "user", "db", "schema", explicitNull, Map.of(), 3L, 5L)));
    }

    @Test
    public void testNonLiteralLimit_emittedSqlIsAlwaysAPlainLimit() throws Exception {
        // With non-literals rejected, no least/greatest/subtract can reach the SQL.
        for (String sql : new String[]{
                "SELECT * FROM range(100) LIMIT 50 OFFSET 5",
                "SELECT * FROM range(100) LIMIT NULL",
                "SELECT * FROM range(100)"}) {
            String out = Transformations.parseToSql(PASSTHROUGH.authorize(
                    "user", "db", "schema", Transformations.parseToTree(sql), Map.of(), 10L, 2L));
            String lower = out.toLowerCase();
            assertEquals(false,
                    lower.contains("least") || lower.contains("greatest") || lower.contains("subtract"),
                    "no limit arithmetic must survive: " + out);
        }
    }

    /** Non-literal OFFSET composition is retained - adding offsets has no unlimited semantics. */
    @Test
    public void testNonLiteralOffset_stillComposes() throws Exception {
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100) OFFSET 2+3");
        JsonNode authorized = PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 1000L, 10L);
        assertEquals(15L, firstValueOf(authorized), "add(coalesce(2+3,0), 10) must be 15");
    }

    // ── code-review findings on PR #409 ──────────────────────────────────────────────────────

    @Test
    public void testDecimalLimit_isNotReadAsItsUnscaledValue() throws Exception {
        // DuckDB serializes DECIMAL as an unscaled integer plus a scale, so LIMIT 10.9 arrives as
        // 109. Reading that as a literal emitted LIMIT 109 - widening the query's own bound by
        // 10^scale, the exact bug this class exists to prevent. It must be rejected instead.
        for (String limit : new String[]{"10.9", "10.0", "0.5"}) {
            JsonNode query = Transformations.parseToTree("SELECT * FROM range(100) LIMIT " + limit);
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                    PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 1000L, -1L),
                    "LIMIT " + limit + " must not be treated as an integer literal");
            assertEquals(true, e.getMessage().contains("integer literal"), e.getMessage());
        }
    }

    @Test
    public void testDecimalOffset_isNotReadAsItsUnscaledValue() throws Exception {
        // Same trap on the offset side: OFFSET 5.0 arrives as 50, so composing gave OFFSET 60
        // instead of 15. Non-literal offsets compose at execution time, so the result is correct
        // rather than rejected.
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100) OFFSET 5.0");
        JsonNode authorized = PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 1000L, 10L);
        assertEquals(15L, firstValueOf(authorized), "5.0 + 10 must be 15, not 60");
    }

    @Test
    public void testDoubleLimit_isRejected() throws Exception {
        // LIMIT 1e2 serializes as a DOUBLE whose value is 100.0.
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100) LIMIT 1e2");
        assertThrows(IllegalArgumentException.class, () ->
                PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 10L, -1L));
    }

    @Test
    public void testLargeIntegerLimit_isStillAccepted() throws Exception {
        // BIGINT-typed literals must keep working - the type check must not be over-tight.
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100) LIMIT 3000000000");
        JsonNode authorized = PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 7L, -1L);
        assertEquals(7L, rowsOf(authorized), "a BIGINT literal must be capped, not rejected");
    }

    @Test
    public void testZeroOffset_isATrueNoOp() throws Exception {
        // The cap path passes offset = 0. That must not fabricate an OFFSET 0, and must not make
        // applyOffset reject constructs an uncapped read is documented to leave alone.
        JsonNode plain = Transformations.parseToTree("SELECT * FROM range(100)");
        assertEquals("SELECT * FROM \"range\"(100)",
                Transformations.parseToSql(Transformations.applyOffset(plain, 0L)),
                "offset 0 must not add an OFFSET clause");

        for (String sql : new String[]{"SELECT * FROM range(100) LIMIT 3%",
                                       "SELECT * FROM range(100) LIMIT 5+5"}) {
            JsonNode q = Transformations.parseToTree(sql);
            assertDoesNotThrow(() -> Transformations.applyOffset(q, 0L),
                    "offset 0 bounds nothing, so it must not reject: " + sql);
        }
    }

    @Test
    public void testNegativeLimit_reportsTheLimitNotTheOffset() throws Exception {
        // DuckDB folds LIMIT -1 into a constant, so it used to be reported as
        // "'offset' 3 is at or past the query's own LIMIT -1" - misdirecting to the offset.
        JsonNode query = Transformations.parseToTree("SELECT * FROM range(100) LIMIT -1");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                PASSTHROUGH.authorize("user", "db", "schema", query, Map.of(), 10L, 3L));
        assertEquals(true, e.getMessage().contains("must not be negative"),
                "message must point at the LIMIT: " + e.getMessage());
    }
}

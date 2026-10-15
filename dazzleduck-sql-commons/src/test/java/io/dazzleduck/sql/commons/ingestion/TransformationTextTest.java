package io.dazzleduck.sql.commons.ingestion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransformationTextTest {

    @ParameterizedTest
    @CsvSource(delimiter = '|', quoteCharacter = '`', value = {
            // input | expected
            "SELECT a FROM __this|SELECT a FROM __this",
            "SELECT a FROM __this;|SELECT a FROM __this",
            "`SELECT a FROM __this ;; \n `|SELECT a FROM __this",
            "SELECT a FROM __this -- note|SELECT a FROM __this",
            "SELECT a FROM __this; -- note|SELECT a FROM __this",
            "`SELECT a FROM __this;\n-- note\n`|SELECT a FROM __this",
            "SELECT a FROM __this; /* c */|SELECT a FROM __this",
            "SELECT a FROM __this /* outer /* nested */ still */ ;|SELECT a FROM __this",
            "SELECT a FROM __this -- a comment; with a semicolon|SELECT a FROM __this",
            // Markers inside literals and quoted identifiers are not terminators or comments.
            "SELECT ';--' AS x FROM __this|SELECT ';--' AS x FROM __this",
            "SELECT 'it''s; -- x' AS x FROM __this;|SELECT 'it''s; -- x' AS x FROM __this",
            "SELECT E'a\\';--' AS x FROM __this; -- note|SELECT E'a\\';--' AS x FROM __this",
            "SELECT \"we--ird;\" FROM __this; -- note|SELECT \"we--ird;\" FROM __this",
            "SELECT $$x;--y$$ AS x FROM __this;|SELECT $$x;--y$$ AS x FROM __this",
            "SELECT $tag$p;--q$tag$ AS x FROM __this -- c|SELECT $tag$p;--q$tag$ AS x FROM __this",
            // A $1 parameter is not a dollar quote.
            "SELECT $1 AS x FROM __this;|SELECT $1 AS x FROM __this",
            // Shorthand expressions are trimmed the same way.
            "upper(level) AS lvl -- note|upper(level) AS lvl",
            "upper(level) AS lvl;|upper(level) AS lvl",
    })
    void stripTrailingRemovesOnlyTheTail(String input, String expected) {
        assertEquals(expected, TransformationText.stripTrailing(input));
    }

    @Test
    void stripTrailingKeepsCommentsBeforeTheLastToken() {
        String sql = "-- parse once\nWITH t AS (SELECT * FROM __this) /* stage */ SELECT * FROM t -- end";
        assertEquals("-- parse once\nWITH t AS (SELECT * FROM __this) /* stage */ SELECT * FROM t",
                TransformationText.stripTrailing(sql));
    }

    @Test
    void stripTrailingKeepsTheUsersSqlVerbatim() {
        // No re-rendering: modern lambda syntax and list comprehensions survive unchanged.
        String sql = "SELECT list_transform(l, lambda y: y + 1) AS l2, [x * 2 FOR x IN l] AS l3 FROM __this; -- c";
        assertEquals("SELECT list_transform(l, lambda y: y + 1) AS l2, [x * 2 FOR x IN l] AS l3 FROM __this",
                TransformationText.stripTrailing(sql));
    }

    @Test
    void stripTrailingOfOnlyCommentsAndTerminatorsIsEmpty() {
        assertEquals("", TransformationText.stripTrailing(" ; -- nothing here\n /* c */ "));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT 1 FROM __this; SELECT 2",
            "SELECT 1 FROM __this;\nDROP TABLE t",
            "SELECT 1 FROM __this /* c */ ; -- x\n SELECT 2",
    })
    void detectsMultipleStatements(String sql) {
        assertTrue(TransformationText.hasMultipleStatements(sql), sql);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT a FROM __this",
            "SELECT a FROM __this;",
            "SELECT a FROM __this; -- note; SELECT 2",
            "SELECT a FROM __this; /* ; SELECT 2 */",
            "SELECT 'a; SELECT 2' FROM __this",
            "SELECT $$; SELECT 2$$ FROM __this",
            "SELECT \"x; SELECT 2\" FROM __this",
    })
    void singleStatementsAreNotFlagged(String sql) {
        assertFalse(TransformationText.hasMultipleStatements(sql), sql);
    }
}

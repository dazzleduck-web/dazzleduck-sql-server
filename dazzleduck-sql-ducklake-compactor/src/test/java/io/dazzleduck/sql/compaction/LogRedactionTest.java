package io.dazzleduck.sql.compaction;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** The error texts are what DuckDB 1.5.5.1 actually reports for these statements. */
class LogRedactionTest {

    private static void assertMasked(String text, String... secrets) {
        String redacted = LogRedaction.redact(text);
        for (String secret : secrets) {
            assertFalse(redacted.contains(secret), () -> "'" + secret + "' survived: " + redacted);
        }
        assertTrue(redacted.contains(LogRedaction.MASK), redacted);
    }

    @Test
    void postgresAttachFailureEchoesTheConnectionString() {
        assertMasked("IO Error: Unable to connect to Postgres at \"host=127.0.0.1 port=1 dbname=x user=u"
                + " password=FAKE_PG_PASSWORD\": connection to server at \"127.0.0.1\", port 1 failed",
                "FAKE_PG_PASSWORD");
    }

    @Test
    void duckLakeOnPostgresAttachFailureEchoesTheConnectionString() {
        assertMasked("IO Error: Failed to attach DuckLake MetaData \"__ducklake_metadata_lake\" at path +"
                + " \"postgres:host=127.0.0.1 port=1 dbname=x user=u password=FAKE_PG_PASSWORD\"Unable to"
                + " connect to Postgres at \"host=127.0.0.1 port=1 dbname=x user=u password=FAKE_PG_PASSWORD\"",
                "FAKE_PG_PASSWORD");
    }

    @Test
    void wrappedStartupStatementIsMasked() {
        // ConnectionPool.executeOnSingleton puts the failing statement itself in the message.
        assertMasked("Failed to execute on singleton connection: CREATE SECRET s1 (TYPE s3,"
                        + " KEY_ID 'AKIAFAKEKEYID0000', SECRET 'FAKE_S3_SECRET_VALUE', SESSION_TOKEN 'FAKE_SESSION')",
                "AKIAFAKEKEYID0000", "FAKE_S3_SECRET_VALUE", "FAKE_SESSION");
        assertMasked("Failed to execute on singleton connection: ATTACH 'ducklake:postgres:dbname=lake"
                + " host=db user=svc password=FAKE_PG_PASSWORD' AS lake", "FAKE_PG_PASSWORD");
    }

    @Test
    void parserErrorEchoIsMaskedEvenWhenItCutsAPairInHalf() {
        assertMasked("Parser Error: syntax error at or near \"ENDPOINT\"\n\nLINE 1: ...ID0000',"
                        + " SECRET 'FAKE_S3_SECRET_VALUE', REGION 'us-east-1' ENDPOINT 'x')",
                "ID0000", "FAKE_S3_SECRET_VALUE");
        // cut off inside the value, with no closing quote
        assertMasked("LINE 1: CREATE SECRET (TYPE s3, SECRET 'FAKE_S3_SEC...", "FAKE_S3_SEC");
    }

    @Test
    void settingsUriUserinfoAndBearerTokensAreMasked() {
        assertMasked("SET s3_secret_access_key = 'FAKE_SETTING_SECRET'", "FAKE_SETTING_SECRET");
        assertMasked("postgresql://svc:FAKE_URI_PASSWORD@db:5432/lake", "FAKE_URI_PASSWORD");
        assertMasked("Authorization: Bearer eyFAKE.JWT.TOKEN", "eyFAKE.JWT.TOKEN");
        assertMasked("password='FAKE QUOTED PASSWORD'", "FAKE QUOTED PASSWORD");
        assertMasked("s3://bucket/data?access_key=FAKE_QUERY_KEY&region=us-east-1", "FAKE_QUERY_KEY");
        assertMasked("https://acct.blob.core.windows.net/c?sv=2022-11-02&sig=FAKE_SAS_SIG", "FAKE_SAS_SIG");
    }

    @Test
    void ordinaryLinesAreUnchanged() {
        for (String line : new String[]{
                "Tier 'minor' compaction cycle failed for lake [lake.main.events] — scheduler will continue",
                "Compaction service started for 1 database(s), 2 tier(s) (minor, major)",
                "Exporting logs at INFO and above to http://collector:4317 as service 'ducklake-compactor'",
                "Secret with name 'my_s3' not found", // names a secret, contains none
        }) {
            assertEquals(line, LogRedaction.redact(line));
        }
        assertNull(LogRedaction.redact(null));
    }
}

package io.dazzleduck.sql.otel.collector;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.StartupScriptProvider;
import io.dazzleduck.sql.otel.collector.config.CollectorConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Settings whose silent fallback would leave the collector running but doing something other than
 * what was configured: a broken ingestion provider, startup script or login URL fails startup
 * instead. Absent settings keep their defaults.
 */
class CollectorConfigFailFastTest {

    private static CollectorConfig config(String hocon) {
        return new CollectorConfig(ConfigFactory.parseString(hocon));
    }

    /** The bundled reference.conf underneath, as a real deployment starts with. */
    private static CollectorConfig withReferenceConf(String hocon) {
        return new CollectorConfig(ConfigFactory.parseString(hocon).withFallback(ConfigFactory.load()).resolve());
    }

    @Test
    void theBundledDefaultConfigStillStarts() {
        // reference.conf declares an ingestion_task_factory_provider block with no class and no
        // ingestion_path; that must mean plain Parquet, not a startup failure.
        var props = withReferenceConf("").toProperties();
        assertNotNull(props.getIngestionHandler());
        assertNull(props.getLoginUrl());
        assertTrue(props.getStartupScript().contains("LOAD arrow"), props.getStartupScript());
    }

    // --- ingestion_task_factory_provider ---------------------------------------------------

    @Test
    void anAbsentIngestionProviderStillMeansPlainParquet() {
        assertNotNull(config("otel_collector { grpc_port = 4317 }").getIngestionHandler());
    }

    @Test
    void anIngestionProviderThatCannotLoadFailsInsteadOfFallingBackToNoop() {
        var e = assertThrows(IllegalStateException.class, () -> config("""
                otel_collector.ingestion_task_factory_provider {
                    class = "io.dazzleduck.does.not.Exist"
                }
                """).getIngestionHandler());
        assertTrue(e.getMessage().contains("ingestion_task_factory_provider"), e.getMessage());
    }

    // --- startup_script_provider -----------------------------------------------------------

    @Test
    void theDeprecatedStartupScriptKeyIsUsedWhenTheProviderBlockIsAbsent() {
        assertEquals("LOAD arrow;", config("otel_collector.startup_script = \"LOAD arrow;\"").getStartupScript());
    }

    @Test
    void theDefaultScriptIsUsedWhenNothingIsConfigured() {
        assertEquals("INSTALL arrow FROM community; LOAD arrow;", config("otel_collector {}").getStartupScript());
    }

    @Test
    void inlineContentIsReturned() {
        assertTrue(config("otel_collector.startup_script_provider.content = \"SELECT 1;\"")
                .getStartupScript().contains("SELECT 1;"));
    }

    @Test
    void aScriptLocationThatIsNotAFileFails(@TempDir Path dir) {
        String missing = dir.resolve("missing.sql").toString().replace("\\", "\\\\");
        var e = assertThrows(IllegalArgumentException.class, () -> config(
                "otel_collector.startup_script_provider.script_location = \"" + missing + "\"").getStartupScript());
        assertTrue(e.getMessage().contains("script_location"), e.getMessage());
    }

    @Test
    void aScriptReferencingAnUndefinedEnvironmentVariableFails(@TempDir Path dir) throws Exception {
        Path script = Files.writeString(dir.resolve("startup.sql"),
                "ATTACH '${DAZZLEDUCK_TEST_UNDEFINED_VARIABLE_1234}' AS lake;");
        String location = script.toString().replace("\\", "\\\\");
        assertThrows(IllegalArgumentException.class, () -> config(
                "otel_collector.startup_script_provider.script_location = \"" + location + "\"").getStartupScript());
    }

    /** Stands in for a provider that resolves script_location itself, e.g. from S3. */
    public static class RemoteScriptProvider implements StartupScriptProvider {
        private Config config;

        @Override
        public void setConfig(Config config) {
            this.config = config;
        }

        @Override
        public String getStartupScript() {
            return "-- from " + config.getString("script_location");
        }
    }

    @Test
    void aCustomProviderClassMayUseANonLocalScriptLocation() {
        String script = withReferenceConf(
                "otel_collector.startup_script_provider { class = \"" + RemoteScriptProvider.class.getName()
                        + "\", script_location = \"s3://bucket/startup.sql\" }").getStartupScript();
        assertEquals("-- from s3://bucket/startup.sql", script);
    }

    @Test
    void aStartupScriptProviderClassThatCannotLoadFails() {
        var e = assertThrows(IllegalStateException.class, () -> config(
                "otel_collector.startup_script_provider.class = \"io.dazzleduck.does.not.Exist\"").getStartupScript());
        assertTrue(e.getMessage().contains("startup_script_provider"), e.getMessage());
    }

    // --- login_url -------------------------------------------------------------------------

    @Test
    void anAbsentLoginUrlMeansLocalUsers() {
        assertNull(config("otel_collector {}").getLoginUrl());
    }

    @Test
    void aValidLoginUrlIsReturned() {
        assertEquals("https://login.example.com/v1/login",
                config("otel_collector.login_url = \"https://login.example.com/v1/login\"").getLoginUrl());
    }

    @Test
    void aLoginUrlThatIsNotAnAbsoluteHttpUrlFails() {
        for (String bad : new String[]{"", "   ", "login.example.com/v1/login", "ftp://login.example.com", "http://"}) {
            assertThrows(IllegalArgumentException.class,
                    () -> config("otel_collector.login_url = \"" + bad + "\"").getLoginUrl(), "'" + bad + "'");
        }
    }

    @Test
    void aLoginUrlThatIsNotAStringFails() {
        assertThrows(ConfigException.WrongType.class,
                () -> config("otel_collector.login_url { host = x }").getLoginUrl());
    }

    @Test
    void startupFailsThroughToProperties() {
        // toProperties() is what the server reads at startup, so the failure must surface there.
        assertThrows(IllegalArgumentException.class,
                () -> config("otel_collector.login_url = \"not a url\"").toProperties());
    }

    // --- attribute_types -------------------------------------------------------------------

    /**
     * Whether a stream records anything is set per queue, by its {@code attribute_types_table}
     * parameter — not here. This block only has the global off switch and the memory cap.
     */
    @Test
    void theAttributeTypeBlockDefaultsToAvailableWithTheStandardCap() {
        var settings = withReferenceConf("").getAttributeTypeSettings();
        assertTrue(settings.enabled());
        assertEquals(100_000, settings.maxEntries());
    }

    @Test
    void anAbsentAttributeTypesBlockKeepsThoseDefaults() {
        var settings = config("otel_collector { grpc_port = 4317 }").getAttributeTypeSettings();
        assertTrue(settings.enabled());
        assertEquals(100_000, settings.maxEntries());
    }

    @Test
    void theAttributeTypeRegistryCanBeTurnedOffEverywhereAtOnce() {
        assertFalse(config("otel_collector.attribute_types.enabled = false")
                .getAttributeTypeSettings().enabled());
    }

    @Test
    void aNonPositiveMaxEntriesFails() {
        for (String bad : new String[]{"0", "-1"}) {
            var e = assertThrows(IllegalArgumentException.class, () -> config("""
                    otel_collector.attribute_types.max_entries = %s
                    """.formatted(bad)).getAttributeTypeSettings(), bad);
            assertTrue(e.getMessage().contains("max_entries"), e.getMessage());
        }
    }

    @Test
    void anAttributeTypesValueOfTheWrongTypeFails() {
        assertThrows(ConfigException.WrongType.class, () -> config("""
                otel_collector.attribute_types.max_entries = "lots"
                """).getAttributeTypeSettings());
    }

    @Test
    void anInvalidAttributeTypesBlockFailsThroughToProperties() {
        assertThrows(IllegalArgumentException.class,
                () -> config("otel_collector.attribute_types.max_entries = 0").toProperties());
    }
}

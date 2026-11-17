package io.dazzleduck.sql.compaction;

import org.junit.jupiter.api.Assumptions;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * The DuckLake extension this module ships ({@code ducklake.patch.release}), for tests that need its
 * fixes: a local build given as {@code -Dducklake.extension.path} (the file must be named
 * {@code ducklake.duckdb_extension}), or the pinned release, downloaded once per platform into
 * {@code target/ducklake-patch}. Load it with {@code allow_unsigned_extensions} set as a connection
 * property, as {@link RawConnections} does.
 */
final class PatchedDuckLake {

    private PatchedDuckLake() {
    }

    /**
     * Skips the tests locally, as when offline, but fails them in CI (which sets {@code CI}): there a
     * skip would let a regression to stock DuckLake pass unnoticed.
     */
    private static void cannotDownload(String what) {
        String message = "could not download the patched DuckLake extension " + what;
        if (System.getenv("CI") != null) {
            fail(message);
        }
        Assumptions.abort(message);
    }

    /** The patched extension for this platform: a local build, or the pinned release, downloaded once. */
    static Path extension() throws Exception {
        String local = System.getProperty("ducklake.extension.path", "");
        if (!local.isBlank()) {
            return Path.of(local);
        }
        Properties pins = new Properties();
        try (InputStream in = PatchedDuckLake.class.getResourceAsStream("/ducklake-patch-version.properties")) {
            pins.load(in);
        }
        String release = pins.getProperty("ducklake.patch.release");
        String platform;
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("PRAGMA platform")) {
            rs.next();
            platform = rs.getString(1);
        }
        Path target = Path.of("target", "ducklake-patch", release, platform, "ducklake.duckdb_extension");
        if (Files.exists(target)) {
            return target.toAbsolutePath();
        }
        URI uri = URI.create("https://github.com/dazzleduck-web/ducklake/releases/download/%s/ducklake-%s.duckdb_extension"
                .formatted(release, platform));
        Files.createDirectories(target.getParent());
        Path partial = target.resolveSibling(target.getFileName() + ".part");
        try {
            HttpClient client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL)
                    .connectTimeout(Duration.ofSeconds(20)).build();
            HttpResponse<Path> response = client.send(
                    HttpRequest.newBuilder(uri).timeout(Duration.ofMinutes(5)).build(),
                    HttpResponse.BodyHandlers.ofFile(partial));
            if (response.statusCode() != 200) {
                Files.deleteIfExists(partial);
                cannotDownload(uri + ": HTTP " + response.statusCode());
            }
        } catch (IOException e) {
            Files.deleteIfExists(partial);
            cannotDownload(uri + ": " + e);
        }
        Files.move(partial, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        return target.toAbsolutePath();
    }
}

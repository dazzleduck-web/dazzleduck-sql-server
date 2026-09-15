package io.dazzleduck.sql.common;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ConfigConstants#getTempWriteDir(String)} — the Arrow staging directory shared by the
 * flight server and the OTel collector. Validation lives here rather than in either module so the
 * two cannot drift apart.
 */
class ConfigConstantsTempWriteDirTest {

    @Test
    void createsAMissingDirectory(@TempDir Path dir) throws IOException {
        Path nested = dir.resolve("a").resolve("b");
        assertEquals(nested, ConfigConstants.getTempWriteDir(nested.toString()));
        assertTrue(Files.isDirectory(nested), "a missing temp_write_location must be created");
    }

    @Test
    void acceptsAnExistingWritableDirectory(@TempDir Path dir) throws IOException {
        assertEquals(dir, ConfigConstants.getTempWriteDir(dir.toString()));
    }

    @Test
    void rejectsABlankValue() {
        for (String value : new String[]{null, "", "   "}) {
            IOException e = assertThrows(IOException.class,
                    () -> ConfigConstants.getTempWriteDir(value));
            assertTrue(e.getMessage().contains("must not be blank"), e.getMessage());
        }
    }

    @Test
    void rejectsAPathThatIsAnExistingFile(@TempDir Path dir) throws IOException {
        Path file = Files.createFile(dir.resolve("not-a-directory"));
        IOException e = assertThrows(IOException.class,
                () -> ConfigConstants.getTempWriteDir(file.toString()));
        assertTrue(e.getMessage().contains("is not a directory"), e.getMessage());
    }

    @Test
    void messageNamesTheConfigKey(@TempDir Path dir) throws IOException {
        Path file = Files.createFile(dir.resolve("f"));
        IOException e = assertThrows(IOException.class,
                () -> ConfigConstants.getTempWriteDir(file.toString()));
        assertTrue(e.getMessage().startsWith(ConfigConstants.TEMP_WRITE_LOCATION_KEY),
                "the message must name the key an operator has to fix: " + e.getMessage());
    }
}

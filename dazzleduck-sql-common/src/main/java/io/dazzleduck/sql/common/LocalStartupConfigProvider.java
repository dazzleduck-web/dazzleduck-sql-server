package io.dazzleduck.sql.common;

import com.typesafe.config.Config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class LocalStartupConfigProvider implements StartupScriptProvider {

    public static final String SCRIPT_LOCATION_KEY = "script-location";
    Config config;
    public String getStartupScript() throws IOException {
        String startUpFile = config.hasPathOrNull(SCRIPT_LOCATION_KEY) ? config.getString(SCRIPT_LOCATION_KEY) : null;
        if (startUpFile != null) {
            Path path = Paths.get(startUpFile);
            if (Files.isRegularFile(path)) {
                return Files.readString(path).trim();
            }
        }
        return null;
    }

    @Override
    public void loadInner(Config config) {
        this.config = config;
    }
}

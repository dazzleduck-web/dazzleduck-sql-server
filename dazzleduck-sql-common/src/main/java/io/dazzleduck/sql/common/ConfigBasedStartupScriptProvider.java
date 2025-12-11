package io.dazzleduck.sql.common;

import com.typesafe.config.Config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


public class ConfigBasedStartupScriptProvider implements StartupScriptProvider {

    private static String CONTENT_KEY = "content";

    public static final String SCRIPT_LOCATION_KEY = "script_location";
    private Config config;

    public String getStartupScript() throws IOException {
        StringBuilder sb = new StringBuilder();
        if (config.hasPath(CONTENT_KEY)) {
            sb.append(config.getString(CONTENT_KEY));
            sb.append("\n");
        }

        String startUpFile = config.hasPathOrNull(SCRIPT_LOCATION_KEY) ? config.getString(SCRIPT_LOCATION_KEY) : null;
        if (startUpFile != null) {
            Path path = Paths.get(startUpFile);
            if (Files.isRegularFile(path)) {
                sb.append(Files.readString(path).trim());
                sb.append("\n");
            }
        }
        return sb.toString();
    }


    @Override
    public void setConfig(Config config) {
        this.config = config;
    }
}

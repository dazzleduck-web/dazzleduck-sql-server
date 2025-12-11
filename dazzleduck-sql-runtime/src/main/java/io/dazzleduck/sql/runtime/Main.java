package io.dazzleduck.sql.runtime;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.util.ConfigUtils;
import io.dazzleduck.sql.commons.ConnectionPool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.dazzleduck.sql.common.util.ConfigUtils.CONFIG_PATH;

public class Main {
    public static void main(String[] args) throws Exception {
        var commandLineConfig = ConfigUtils.loadCommandLineConfig(args).config();
        var config = commandLineConfig.withFallback(ConfigFactory.load()).getConfig(CONFIG_PATH);
        start(config);
    }

    public static void start(Config config) throws Exception {
        String warehousePath = ConfigUtils.getWarehousePath(config);
        createWarehouse(warehousePath);
        var networkingMode = config.getStringList("networking_modes");
        if (networkingMode.contains("http")) {
            io.dazzleduck.sql.http.server.Main.start(config);
        }
        if (networkingMode.contains("flight-sql")) {
            io.dazzleduck.sql.flight.server.Main.start(config);
        }
    }


    private static void createWarehouse(String path) throws IOException {
        var p = Path.of(path);
        if(!Files.exists(p)) {
            Files.createDirectories(p);
        }
        System.out.println("Warehouse Path :" + p);
    }
}

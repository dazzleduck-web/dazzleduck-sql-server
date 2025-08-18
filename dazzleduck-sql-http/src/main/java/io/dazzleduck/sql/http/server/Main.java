
package io.dazzleduck.sql.http.server;


import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.auth.Validator;
import io.helidon.config.Config;
import io.helidon.logging.common.LogConfig;
import io.helidon.webserver.WebServer;
import org.apache.arrow.memory.RootAllocator;

import javax.crypto.SecretKey;
import io.jsonwebtoken.security.Keys;
import java.util.Base64;
import java.security.NoSuchAlgorithmException;


/**
 * The application main class.
 */
public class Main {


    private static final String CONFIG_PATH = "dazzleduck-http-server";

    /**
     * Cannot be instantiated.
     */
    private Main() {
    }



    /**
     * Application main entry point.
     * @param args command line arguments.
     */
    public static void main(String[] args) throws NoSuchAlgorithmException {
        
        // load logging configuration
        LogConfig.configureRuntime();

        // initialize global config from default configuration
        Config helidonConfig = Config.create();
        var commandlineConfig = io.dazzleduck.sql.common.util.ConfigUtils.loadCommandLineConfig(args).config();
        var appConfig = commandlineConfig.withFallback(ConfigFactory.load().getConfig(Main.CONFIG_PATH));
        var port = Integer.parseInt(appConfig.getString("port"));
        var auth = appConfig.hasPath("auth") ? appConfig.getString("auth") : null;
        String warehousePath = appConfig.hasPath("warehousePath") ?
                appConfig.getString("warehousePath") : System.getProperty("user.dir") + "/warehouse";
        String secretKeyStr = appConfig.hasPath("secretKey")
                ? appConfig.getString("secretKey")
                : "change_me";  // fallback
        byte[] decodedKey = Base64.getDecoder().decode(secretKeyStr);
        SecretKey secretKey = Keys.hmacShaKeyFor(decodedKey);
        var allocator = new RootAllocator();
        String location = "http://localhost:" + port;
        WebServer server = WebServer.builder()
                .config(helidonConfig.get("flight-sql"))
                .routing(routing -> {
                    var b = routing.register("/query", new QueryService(allocator))
                            .register("/login", new LoginService())
                            .register("/plan", new PlaningService(location, allocator))
                            .register("/ingest", new IngestionService(warehousePath, allocator));
                    if ("jwt".equals(auth)) {
                        b.addFilter(new JwtAuthenticationFilter("/query", appConfig, secretKey));
                    }
                })
                .port(port)
                .build()
                .start();

        var builder = new StringBuilder();
        String url = "http://localhost:" + server.port();
        String msg = "WEB server is up! " + url;
        builder.append(msg);
        System.out.println(builder.toString());
    }
}
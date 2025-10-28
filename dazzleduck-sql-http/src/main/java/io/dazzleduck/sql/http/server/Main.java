
package io.dazzleduck.sql.http.server;


import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.auth.Validator;
import io.dazzleduck.sql.common.util.ConfigUtils;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.flight.server.DuckDBFlightSqlProducer;
import io.dazzleduck.sql.login.LoginService;
import io.helidon.config.Config;
import io.helidon.cors.CrossOriginConfig;
import io.helidon.logging.common.LogConfig;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.cors.CorsSupport;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.RootAllocator;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static io.dazzleduck.sql.common.util.ConfigUtils.CONFIG_PATH;


/**
 * The application main class.
 */
public class Main {


    /**
     * Cannot be instantiated.
     */
    private Main() {
    }



    /**
     * Application main entry point.
     * @param args command line arguments.
     */
    public static void main(String[] args) throws Exception {
        
        // load logging configuration
        LogConfig.configureRuntime();

        // initialize global config from default configuration
        Config helidonConfig = Config.create();
        var commandlineConfig = io.dazzleduck.sql.common.util.ConfigUtils.loadCommandLineConfig(args).config();
        var appConfig = commandlineConfig.withFallback(ConfigFactory.load().getConfig(CONFIG_PATH));
        var httpConfig =  appConfig.getConfig("http");
        var port = httpConfig.getInt(ConfigUtils.PORT_KEY);
        var host = httpConfig.getString(ConfigUtils.HOST_KEY);
        var auth = httpConfig.hasPath(ConfigUtils.AUTHENTICATION_KEY) ? httpConfig.getString(ConfigUtils.AUTHENTICATION_KEY) : "none";
        String warehousePath = ConfigUtils.getWarehousePath(appConfig);
        String base64SecretKey = appConfig.getString(ConfigUtils.SECRET_KEY_KEY);
        var secretKey = Validator.fromBase64String(base64SecretKey);
        var allocator = new RootAllocator();
        String location = "http://%s:%s".formatted(host, port);
        var tempWriteDir = Path.of(appConfig.getString("temp-write-location"));
        if (!Files.exists(tempWriteDir)) {
            Files.createDirectories(tempWriteDir);
        }
        AccessMode accessMode = appConfig.hasPath("accessMode") ? AccessMode.valueOf(appConfig.getString("accessMode").toUpperCase()) : AccessMode.COMPLETE;
        if (Files.exists(tempWriteDir)) {
            Files.createDirectories(tempWriteDir);
        }
        var jwtExpiration = appConfig.getDuration("jwt.token.expiration");
        var cors = CorsSupport.builder()
                .addCrossOrigin(CrossOriginConfig.builder()
                        .allowOrigins("http://localhost:5173")
                        .allowMethods("GET", "POST")
                        .allowHeaders("Content-Type", "Authorization")
                        .build())
                .build();

        var producerId = UUID.randomUUID().toString();
        var producer = DuckDBFlightSqlProducer.createProducer(Location.forGrpcInsecure(host, port), producerId, base64SecretKey, allocator, warehousePath, accessMode);
        WebServer server = WebServer.builder()
                .config(helidonConfig.get("dazzleduck-server"))
                .config(helidonConfig.get("flight-sql"))
                .routing(routing -> {
                    routing.register(cors);
                    var b = routing.register("/query", new QueryService(producer, accessMode,base64SecretKey))
                            .register("/login", new LoginService(appConfig, secretKey, jwtExpiration))
                            .register("/plan", new PlaningService(producer, location, allocator, accessMode))
                            .register("/ingest", new IngestionService(producer, warehousePath, allocator));
                    if ("jwt".equals(auth)) {
                        b.addFilter(new JwtAuthenticationFilter("/query", appConfig, secretKey));
                    }
                })
                .port(port)
                .host(host)
                .build()
                .start();
        String url = "http://localhost:" + server.port();
        System.out.println("Http Server is up: Listening on URL: " + url);
    }
}
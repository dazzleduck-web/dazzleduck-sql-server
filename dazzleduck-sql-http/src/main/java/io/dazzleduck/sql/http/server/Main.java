
package io.dazzleduck.sql.http.server;


import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.ConfigBasedProvider;
import io.dazzleduck.sql.common.auth.Validator;
import io.dazzleduck.sql.common.util.ConfigUtils;
import io.dazzleduck.sql.commons.authorization.AccessMode;
import io.dazzleduck.sql.commons.ingestion.PostIngestionTaskFactoryProvider;
import io.dazzleduck.sql.flight.optimizer.QueryOptimizerProvider;
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
import java.util.List;
import java.util.UUID;

import static io.dazzleduck.sql.common.util.ConfigUtils.CONFIG_PATH;


/**
 * The application main class.
 */
public class Main {





    /**
     * Application main entry point.
     * @param args command line arguments.
     */
    public static void main(String[] args) throws Exception {
        var commandlineConfig = io.dazzleduck.sql.common.util.ConfigUtils.loadCommandLineConfig(args).config();
        var appConfig = commandlineConfig.withFallback(ConfigFactory.load()).getConfig(CONFIG_PATH);
        start(appConfig);
    }

    public static void start(com.typesafe.config.Config appConfig) throws Exception {
        LogConfig.configureRuntime();
        Config helidonConfig = Config.create();
        var httpConfig =  appConfig.getConfig("http");
        var port = httpConfig.getInt(ConfigUtils.PORT_KEY);
        var host = httpConfig.getString(ConfigUtils.HOST_KEY);
        var auth = httpConfig.hasPath(ConfigUtils.AUTHENTICATION_KEY) ? httpConfig.getString(ConfigUtils.AUTHENTICATION_KEY) : "none";
        String warehousePath = ConfigUtils.getWarehousePath(appConfig);
        String base64SecretKey = appConfig.getString(ConfigUtils.SECRET_KEY_KEY);
        var secretKey = Validator.fromBase64String(base64SecretKey);
        var allocator = new RootAllocator();
        String location = "http://%s:%s".formatted(host, port);
        var tempWriteDir = DuckDBFlightSqlProducer.getTempWriteDir(appConfig);
        AccessMode accessMode = DuckDBFlightSqlProducer.getAccessMode(appConfig);
        if (Files.exists(tempWriteDir)) {
            Files.createDirectories(tempWriteDir);
        }
        var jwtExpiration = appConfig.getDuration("jwt_token.expiration");
        var cors = CorsSupport.builder()
                .addCrossOrigin(CrossOriginConfig.builder()
                        .allowOrigins(appConfig.hasPath("allow-origin") ? appConfig.getString("allow-origin") : "*")
                        .allowMethods("GET", "POST")
                        .allowHeaders("Content-Type", "Authorization")
                        .build())
                .build();

        var producerId = UUID.randomUUID().toString();
        PostIngestionTaskFactoryProvider provider = ConfigBasedProvider.load(appConfig,
                PostIngestionTaskFactoryProvider.POST_INGESTION_CONFIG_PREFIX,
                PostIngestionTaskFactoryProvider.NO_OP);
        var factory = provider.getPostIngestionTaskFactory();
        QueryOptimizerProvider optimizer = ConfigBasedProvider.load(appConfig, QueryOptimizerProvider.QUERY_OPTIMIZER_PROVIDER_CONFIG_PREFIX,
                QueryOptimizerProvider.NOOPOptimizerProvider);
        var producer = DuckDBFlightSqlProducer.createProducer(Location.forGrpcInsecure(host, port), producerId,
                base64SecretKey, allocator, warehousePath, accessMode, factory, optimizer.getOptimizer());
        WebServer server = WebServer.builder()
                .config(helidonConfig.get("dazzleduck_server"))
                .config(helidonConfig.get("flight_sql"))
                .routing(routing -> {
                    routing.register(cors);
                    var b = routing.register("/query", new QueryService(producer, accessMode))
                            .register("/login", new LoginService(appConfig, secretKey, jwtExpiration))
                            .register("/plan", new PlaningService(producer, location, allocator, accessMode))
                            .register("/cancel", new CancelService(producer, accessMode))
                            .register("/ingest", new IngestionService(producer, warehousePath, allocator))
                            .register("/ui", new UIService(producer));
                    if ("jwt".equals(auth)) {
                        b.addFilter(new JwtAuthenticationFilter(List.of("/query", "/plan", "/ingest", "/cancel"), appConfig, secretKey));
                    }
                })
                .port(port)
                .host(host)
                .build()
                .start();
        var http = server.hasTls() ? "https" : "http";
        String url = "%s://%s:%s".formatted(http, host, port);
        System.out.println("Http Server is up: Listening on URL: " + url);
    }
}
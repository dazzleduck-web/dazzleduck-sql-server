package io.dazzleduck.sql.login;

import com.typesafe.config.ConfigFactory;
import io.dazzleduck.sql.common.auth.Validator;
import io.dazzleduck.sql.common.util.ConfigUtils;
import io.helidon.config.Config;
import io.helidon.logging.common.LogConfig;
import io.helidon.webserver.WebServer;


public class Main {

    public static final String CONFIG_PATH = "dazzleduck-login-service";
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
        var secretKey = Validator.fromBase64String(appConfig.getString(ConfigUtils.SECRET_KEY_KEY));
        var jwtExpiration = appConfig.getDuration("jwt.token.expiration");
        WebServer server = WebServer.builder()
                .config(helidonConfig.get("dazzleduck-server"))
                .config(helidonConfig.get("flight-sql"))
                .routing(routing -> {
                    var b = routing
                            .register("/login", new LoginService(appConfig, secretKey, jwtExpiration));
                })
                .port(port)
                .host(host)
                .build()
                .start();
        String url = "http://localhost:" + server.port();
        System.out.println("Http Server is up: Listening on URL: " + url);
    }
}

package io.dazzleduck.sql.common.authorization;

import com.typesafe.config.Config;

import java.io.IOException;

public class LocalAuthorizationConfigProvider implements AuthorizationProvider {

    public static final String AUTHORIZATION_KEY = "access";
    Config config;
    public SqlAuthorizer getAuthorization() throws IOException {
        var conf = config.hasPathOrNull(AUTHORIZATION_KEY) ? config.getConfig(AUTHORIZATION_KEY) : null;
        if (conf != null) {
            return SimpleAuthorizer.load(config);
        }
        return null;
    }

    @Override
    public void loadInner(Config config) {
        this.config = config;
    }
}

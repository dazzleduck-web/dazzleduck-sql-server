package io.dazzleduck.sql.common.authorization;

import com.typesafe.config.Config;

import java.io.IOException;

public interface AuthorizationProvider {
    String AUTHORIZATION_CONFIG_PROVIDER_CLASS_KEY = "provider-class";
    String AUTHORIZATION_CONFIG_PREFIX = "authorization";
    void loadInner(Config config);

    SqlAuthorizer getAuthorization() throws IOException, InterruptedException;

    static AuthorizationProvider load(Config config) throws Exception {
        if( config.hasPath(AUTHORIZATION_CONFIG_PREFIX)) {
            var innerConfig = config.getConfig(AUTHORIZATION_CONFIG_PREFIX);
            var clazz =  innerConfig.getString(AUTHORIZATION_CONFIG_PROVIDER_CLASS_KEY);
            var constructor = Class.forName(clazz).getConstructor();
            var object = (AuthorizationProvider) constructor.newInstance();
            var loadMethod = Class.forName(clazz).getMethod("loadInner", Config.class);
            loadMethod.invoke(object, innerConfig);
            return object;
        } else {
            return new NoOpConfigProvider();
        }
    }

    public static class NoOpConfigProvider implements AuthorizationProvider {
        public SqlAuthorizer getAuthorization() throws IOException {
            return new NOOPAuthorizer();
        }

        @Override
        public void loadInner(Config config) {

        }
    }
}

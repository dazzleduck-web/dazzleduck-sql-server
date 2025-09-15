package io.dazzleduck.sql.common;

import com.typesafe.config.Config;
import io.dazzleduck.sql.common.authorization.NOOPAuthorizer;
import io.dazzleduck.sql.common.authorization.SqlAuthorizer;

public interface AuthorizerProvider extends ConfigBasedProvider {
    String AUTHORIZER_CONFIG_PREFIX = "authorizer";
    public static SqlAuthorizer load(Config config) throws Exception {
        return ConfigBasedProvider.load(config, AUTHORIZER_CONFIG_PREFIX, new NOOPAuthorizer());
    }
}

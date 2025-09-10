package io.dazzleduck.sql.common;

import com.typesafe.config.Config;

import java.io.IOException;

public interface StartupScriptProvider {

    String STARTUP_SCRIPT_CONFIG_PROVIDER_CLASS_KEY = "provider-class";
    String STARTUP_SCRIPT_CONFIG_PREFIX = "startup-script";
    void loadInner(Config config);

    String getStartupScript() throws IOException, InterruptedException;

    static StartupScriptProvider load(Config config) throws Exception {
        if( config.hasPath(STARTUP_SCRIPT_CONFIG_PREFIX)) {
            var innerConfig = config.getConfig(STARTUP_SCRIPT_CONFIG_PREFIX);
            var clazz =  innerConfig.getString(STARTUP_SCRIPT_CONFIG_PROVIDER_CLASS_KEY);
            var constructor = Class.forName(clazz).getConstructor();
            var object = (StartupScriptProvider) constructor.newInstance();
            var loadMethod = Class.forName(clazz).getMethod("loadInner", Config.class);
            loadMethod.invoke(object, innerConfig);
            return object;
        } else {
            return new NoOpConfigProvider();
        }
    }

    public static class NoOpConfigProvider implements StartupScriptProvider {
        public String getStartupScript() throws IOException {
            return null;
        }

        @Override
        public void loadInner(Config config) {

        }
    }
}

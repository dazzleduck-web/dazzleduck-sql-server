package io.dazzleduck.sql.common;

import com.typesafe.config.Config;

import java.io.IOException;

public interface StartupScriptProvider extends ConfigBasedProvider {

    String STARTUP_SCRIPT_CONFIG_PREFIX = "startup-script";
    void setConfig(Config config);

    String getStartupScript()  throws IOException;

    static StartupScriptProvider load(Config config) throws Exception {
        return ConfigBasedProvider.load(config, STARTUP_SCRIPT_CONFIG_PREFIX,
                new NoOpConfigProvider());
    }


    class NoOpConfigProvider implements StartupScriptProvider {
        public String getStartupScript() throws IOException {
            return null;
        }

        @Override
        public void setConfig(Config config) {

        }
    }
}

package io.dazzleduck.sql.flight.optimizer;

import com.typesafe.config.Config;
import io.dazzleduck.sql.common.ConfigBasedProvider;

public class QueryOptimizerProvider implements ConfigBasedProvider {

    public final static String QUERY_OPTIMIZER_PROVIDER_CONFIG_PREFIX = "query_optimizer_provider";


    public QueryOptimizer getOptimizer() {
        return QueryOptimizer.NOOP_QUERY_OPTIMIZER;
    }

    public static QueryOptimizerProvider NOOPOptimizerProvider = new QueryOptimizerProvider() {

        @Override
        public void setConfig(Config config) {

        }
    };

    @Override
    public void setConfig(Config config) {

    }
}

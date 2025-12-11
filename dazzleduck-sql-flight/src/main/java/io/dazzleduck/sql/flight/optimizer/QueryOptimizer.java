package io.dazzleduck.sql.flight.optimizer;

import com.typesafe.config.Config;
import io.dazzleduck.sql.common.ConfigBasedProvider;

/**
 * Interface to perform last step Query optimization such as reading from index before reading actual data
 */
public interface QueryOptimizer extends ConfigBasedProvider {


    String optimize(String query);

    QueryOptimizer NOOP_QUERY_OPTIMIZER = new QueryOptimizer() {
        @Override
        public String optimize(String query) {
            return query;
        }

        @Override
        public void setConfig(Config config) {

        }
    };
}

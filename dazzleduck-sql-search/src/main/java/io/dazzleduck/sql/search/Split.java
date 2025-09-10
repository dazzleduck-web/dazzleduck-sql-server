package io.dazzleduck.sql.search;

import org.apache.arrow.vector.ipc.ArrowReader;

public interface Split {
    /**
     * @param sql        Sql to process the data
     * @param indexFiles index files
     * @return ArrowReader which can be used to read the data
     */
    ArrowReader readResult(String sql,
                           String[] indexFiles);
}


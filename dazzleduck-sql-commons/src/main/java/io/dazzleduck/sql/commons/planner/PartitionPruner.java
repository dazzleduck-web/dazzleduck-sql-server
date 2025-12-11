package io.dazzleduck.sql.commons.planner;

import com.fasterxml.jackson.databind.JsonNode;
import com.typesafe.config.Config;
import io.dazzleduck.sql.common.ConfigBasedProvider;
import io.dazzleduck.sql.commons.FileStatus;

import java.util.List;
import java.util.Map;

public interface PartitionPruner {
    List<FileStatus> pruneFiles(JsonNode sqlTree, long maxSplitSize, Map<String, String> properties);
}


class HivePartitionPruner implements PartitionPruner, ConfigBasedProvider {
    @Override
    public List<FileStatus> pruneFiles(JsonNode sqlTree, long maxSplitSize, Map<String, String> properties) {
        return null;
    }

    @Override
    public void setConfig(Config config) {

    }
}

class DeltaLakePartitionPruner implements PartitionPruner, ConfigBasedProvider {
    @Override
    public List<FileStatus> pruneFiles(JsonNode sqlTree, long maxSplitSize, Map<String, String> properties) {
        return null;
    }

    @Override
    public void setConfig(Config config) {

    }
}

class DuckLakePartitionPruner implements PartitionPruner, ConfigBasedProvider {

    @Override
    public List<FileStatus> pruneFiles(JsonNode sqlTree, long maxSplitSize, Map<String, String> properties) {
        return null;
    }

    @Override
    public void setConfig(Config config) {

    }
}


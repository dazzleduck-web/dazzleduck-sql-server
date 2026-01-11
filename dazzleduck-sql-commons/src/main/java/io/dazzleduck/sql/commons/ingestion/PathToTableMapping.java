package io.dazzleduck.sql.commons.ingestion;

public record PathToTableMapping(String basePath, String tableName, String schemaName) {

    public boolean matches(String queueName) {
        return normalize(queueName).equals(normalize(basePath));
    }

    private static String normalize(String path) {
        if (path == null) return "";
        path = path.replace("\\", "/");
        int idx = path.lastIndexOf('/');
        if (idx >= 0) path = path.substring(idx + 1);
        return path.trim();
    }
}

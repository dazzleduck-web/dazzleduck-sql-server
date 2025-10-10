package io.dazzleduck.sql.commons.authorization;

import io.dazzleduck.sql.commons.Transformations;

import java.util.List;

public record AccessRow(String group,
                        String database,
                        String schema,
                        String tableOrPath,
                        Transformations.TableType tableType,
                        List<String> columns,
                        String filter,
                        java.sql.Date expiration,
                        String functionName) {
}

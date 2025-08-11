package io.dazzleduck.sql.http.server;

import java.util.List;

public record LoginObject(String username, String password, Long orgId, List<String> clusterId) {

    public LoginObject(String username, String password) {
        this(username, password, 0L, List.of());
    }

    public LoginObject(String username, String password, Long orgId, List<String> clusterId) {
        this.username = username;
        this.password = password;
        this.orgId = orgId != null ? orgId : 0L;
        this.clusterId = clusterId != null ? clusterId : List.of();
    }
}

package io.dazzleduck.sql.http.server;

public record LoginObject(String username, String password, Long orgId) {

    public LoginObject {
        if (orgId == null) {
            orgId = 0L;
        }
    }

    public LoginObject(String username, String password) {
        this(username, password, 0L);
    }
}

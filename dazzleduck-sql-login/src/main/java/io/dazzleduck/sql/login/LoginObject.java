package io.dazzleduck.sql.login;

import java.util.Map;

public record LoginObject(String username, String password, Map<String, String> claims) {

    public LoginObject {
        if (claims == null) {
            claims = Map.of();
        }
    }

    public LoginObject(String username, String password) {
        this(username, password, Map.of());
    }
}

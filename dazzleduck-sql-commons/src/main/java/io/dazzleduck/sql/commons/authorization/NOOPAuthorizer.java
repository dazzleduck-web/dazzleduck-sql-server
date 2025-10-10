package io.dazzleduck.sql.commons.authorization;

import com.fasterxml.jackson.databind.JsonNode;
import io.dazzleduck.sql.common.auth.UnauthorizedException;

import java.util.Map;

public class NOOPAuthorizer implements SqlAuthorizer {

    public static SqlAuthorizer INSTANCE = new NOOPAuthorizer();
    private NOOPAuthorizer() {

    }
    @Override
    public JsonNode authorize(String user, String database, String schema, JsonNode query,
                              Map<String, String> verifiedClaims) throws UnauthorizedException {
        return query;
    }
}

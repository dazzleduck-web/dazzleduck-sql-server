package io.dazzleduck.sql.otel.collector.auth;

import io.dazzleduck.sql.common.Headers;
import io.dazzleduck.sql.commons.auth.Validator;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JwtClaimsFilterTest {

    private static final SecretKey KEY = newKey();

    private static SecretKey newKey() {
        try {
            return Validator.generateRandoSecretKey();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Claims parse(String token) {
        return Jwts.parser().verifyWith(KEY).build().parseSignedClaims(token).getPayload();
    }

    @Test
    void filterClaims_dropsRegisteredClaims_keepsAndStringifiesTheRest() {
        String token = Jwts.builder()
                .subject("admin")
                .issuer("test-issuer")
                .id("token-1")
                .issuedAt(new Date())
                .expiration(new Date(System.currentTimeMillis() + 60_000))
                .claim(Headers.CLAIM_INGESTION_QUEUE, "logs")
                .claim("org_id", "acme")
                .claim("retries", 3)
                .signWith(KEY)
                .compact();

        assertEquals(Map.of(
                        Claims.SUBJECT, "admin",
                        Headers.CLAIM_INGESTION_QUEUE, "logs",
                        "org_id", "acme",
                        "retries", "3"),
                JwtServerInterceptor.filterClaims(parse(token)));
    }

    @Test
    void filterClaims_onlyRegisteredClaims_yieldsEmptyMap() {
        String token = Jwts.builder()
                .expiration(new Date(System.currentTimeMillis() + 60_000))
                .signWith(KEY)
                .compact();

        assertTrue(JwtServerInterceptor.filterClaims(parse(token)).isEmpty());
    }
}

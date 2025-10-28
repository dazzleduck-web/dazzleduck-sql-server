package io.dazzleduck.sql.commons.authorization;

import java.util.Map;

public record SubjectAndVerifiedClaims(String subject, Map<String, String> verifiedClaims){ }
package io.dazzleduck.sql.commons;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;

/**
 * Limitation :
 * - Does not work with CTE. It can be easily extended to work with CTE
 */
public class Fingerprint {
    static ObjectMapper objectMapper = new ObjectMapper();

    private static Logger logger = LoggerFactory.getLogger(Fingerprint.class);

    public static void main(String[] args) throws SQLException, IOException, NoSuchAlgorithmException {
        //String sql1 = "select  2.0, 3, 'one', 4.0, 5 , true from t where num > 1 and num >= 1 and str = 'str1' and date in ('2014-01-01') and if(min_a=null, true, min_a <= cast(x as int))";
        String sql1 = "select * from t where num1 > 1 and num2 < 1.0 and str = 'str1'";
        String sql2 = "select * from t where num1 > 2 and num2 <h 2.0 and str = 'str2'";

        sql1 = "SELECT c1, c2 FROM t WHERE c3 = 1 AND c4 = 'str1'";
        sql2 = "SELECT c1, c2 FROM t WHERE c3 = 2 AND c4 = 'str2'";
        String[] sqls = {sql1, sql2};
        String[] res = new String[sqls.length];
        for (int i = 0; i < sqls.length; i++) {
            res[i] = generate(sqls[i]);
        }
        for (int i = 0 ; i < res.length; i ++) {
            String r = res[i];
            String sql = sqls[i];
            System.out.printf("SQL: %s, fingerprint: %s%n", sql, r);
        }
    }

    public static String generate(String sql) throws IOException, SQLException {
        JsonNode tree = Transformations.parseToTree(sql);
        if (tree.get("error").asBoolean()) {
            throw new SQLException("error parsing sql");
        }
        JsonNode fingerPrintedNode = getFingerprintedTree(tree);
        String jsonString = fingerPrintedNode.toString();
        String sqlToExecute = String.format(Transformations.JSON_DESERIALIZE_SQL, jsonString);
        logger.atInfo().log("Fingerprint SQL{}", ConnectionPool.collectFirst(sqlToExecute, String.class));
        return generateSHA256(fingerPrintedNode.toString());
    }

    private static String generateSHA256(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            return bytesToHex(hashBytes);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not found.", e);
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();
        for (byte b : bytes) {
            String hex = String.format("%02x", b);
            hexString.append(hex);
        }
        return hexString.toString();
    }

    /**
     * Replaces every constant with a placeholder, except inside a type: DuckDB 2.0 serializes a
     * cast's type parameters ({@code DECIMAL(10,2)}, {@code VARCHAR(10)}) as constants, and they are
     * part of the query's shape, not its values. A TYPE node is matched and returned unchanged.
     */
    private static JsonNode getFingerprintedTree(JsonNode tree) throws JsonProcessingException {
        return Transformations.transform(tree,
                n -> IS_TYPE.apply(n) || Transformations.IS_CONSTANT.apply(n),
                n -> IS_TYPE.apply(n) ? n : Transformations.REPLACE_CONSTANT.apply(n));
    }

    private static final java.util.function.Function<JsonNode, Boolean> IS_TYPE = Transformations.isClass("TYPE");
}

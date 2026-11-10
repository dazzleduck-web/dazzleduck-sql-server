package bench;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/** Counts the HTTP requests DuckDB makes for one COPY of a generated table to S3. Usage: HttpCount rows endpoint */
public class HttpCount {
    public static void main(String[] args) throws Exception {
        int rows = Integer.parseInt(args[0]);
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement st = c.createStatement()) {
            st.execute("INSTALL httpfs; LOAD httpfs;");
            st.execute(("CREATE SECRET minio (TYPE s3, KEY_ID 'bench', SECRET 'benchbench', ENDPOINT '%s',"
                    + " URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')").formatted(args[1]));
            st.execute("CREATE TABLE t AS SELECT range AS id, md5(range::VARCHAR) || md5((range+1)::VARCHAR) AS s,"
                    + " 'svc-' || (range % 50) AS svc FROM range(" + rows + ")");
            st.execute("CALL enable_logging('HTTP')");
            long t0 = System.nanoTime();
            st.execute("COPY t TO 's3://bench/httpcount/" + System.nanoTime() + ".parquet' (FORMAT parquet)");
            long ms = (System.nanoTime() - t0) / 1_000_000;
            st.execute("CALL disable_logging()");
            try (ResultSet rs = st.executeQuery("SELECT request.type AS method, count(*) AS n FROM duckdb_logs_parsed('HTTP')"
                    + " GROUP BY ALL ORDER BY ALL")) {
                StringBuilder sb = new StringBuilder();
                while (rs.next()) sb.append(rs.getString(1)).append('=').append(rs.getLong(2)).append(' ');
                try (ResultSet v = c.createStatement().executeQuery("SELECT version()")) {
                    v.next();
                    System.out.printf("DuckDB %s, %,d rows: %d ms, HTTP requests: %s%n", v.getString(1), rows, ms, sb);
                }
            }
        }
    }
}

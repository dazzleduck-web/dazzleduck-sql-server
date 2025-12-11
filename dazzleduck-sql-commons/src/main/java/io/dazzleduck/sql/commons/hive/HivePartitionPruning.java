package io.dazzleduck.sql.commons.hive;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.dazzleduck.sql.commons.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.reader.VarCharReader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;
import org.duckdb.DuckDBConnection;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class HivePartitionPruning extends PartitionPruning {

    private static final String READ_PARTITION_BLOB_SQL = "SELECT filename, size, epoch_ms(last_modified) as last_modified, list_transform(parse_path(substring(filename, len('%s') + 2))[1:%s], x -> split_part(x, '=', 2)) as partitions " +
            "FROM read_blob('%s')";
    private static final String PARTITION_SQL = "WITH A AS (SELECT * FROM %s)," +
            " B AS (SELECT filename, size, last_modified, %s FROM A )" +
            " SELECT * FROM B where %s";
    private static final String READ_BLOB_NO_PARTITION_SQL = "SELECT filename, size, epoch_ms(last_modified) as last_modified FROM read_blob('%s')";

    public static final Field UNSCAPE_PARTITION_FIELD =
            new Field("unescaped_partitions", FieldType.notNullable(new ArrowType.List()),
                    List.of(new Field("children", FieldType.notNullable(new ArrowType.Utf8()), null)));

    public static final MappedReader.Function UNESCAPE_FN = (sources, target) -> {
        ListVector resultVector = (ListVector) target;
        ListVector f = (ListVector) sources.get(0);
        UnionListReader reader = f.getReader();
        UnionListWriter writer = resultVector.getWriter();
        for (int i = 0; i < f.getValueCount(); i++) {
            reader.setPosition(i);
            writer.startList();
            writer.setPosition(i);
            while (reader.next()) {
                VarCharReader reader1 = reader.reader();
                Text text = reader1.readText();
                String res = HivePartitionPruning.unescapePathName(text.toString());
                writer.writeVarChar(new Text(res));
            }
            writer.endList();
        }
    };

    public static String getPartitionSql(String[][] dataTypes,
                                            String tempTableName,
                                            String filter) {
        String filterToApply = filter;
        if(filter ==null || filter.isBlank()) {
            filterToApply = "true";
        }
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < dataTypes.length; i++) {
            String[] ss = dataTypes[i];
            String cast = String.format("cast(unescaped_partitions[%s] as %s) as %s", i + 1, ss[1], ss[0]);
            stringBuilder.append(cast);
            stringBuilder.append(",");
        }
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        return String.format(PARTITION_SQL, tempTableName, stringBuilder, filterToApply);
    }

    public static void main(String[] args) throws SQLException, JsonProcessingException {
        String sql0 = "select * from t";
        String sql1 = "select * from t where c1 = 10 and p1 = 50";
        String sql2 = "select * from t where true and p1 = 50";
        String sql4 = "select * from t where c1 = 10 or p1 = 50";
        String sql5 = "select * from t where c1 = 10";
        for (String sql : List.of(sql0, sql1, sql2, sql4, sql5)) {
            try (Connection connection = ConnectionPool.getConnection()) {
                String transformed = doQueryTransformation(connection, sql, Set.of("p1"));
                System.out.printf("SQL : %s, ", sql);
                System.out.printf("Transformed SQL : %s\n", transformed);
            }
        }
    }

    private static String doQueryTransformation(Connection connection, String sql, Set<String> partitionColumns) throws SQLException, JsonProcessingException {
        JsonNode tree = Transformations.parseToTree(connection, sql);
        JsonNode newTree = Transformations.transform(tree, Transformations.IS_SELECT,
                Transformations.removeNonPartitionColumnsPredicatesInQuery(partitionColumns));
        return Transformations.parseToSql(connection, newTree);
    }

    /**
     * @param basePath path can be relative to absolute
     * @param filterExpression filterExpression which include partition as well as no partition columns. Function will remove the filters which are not applicable
     * @param partitionDataTypes in order to cast the value to specific type
     * @return list of files and size of those files
     * @throws SQLException
     * @throws IOException  This is slightly complicated because we do not have function in duckdb which unescape hive path and java based udf are still not supported
     *                      its divided into 3 steps
     *                      1. Run the query on duckdb to get filename, size, partitions from duckdb.
     *                      Schema of read data is &lt;filename string, size bigint, partitions array&lt;string&gt;&gt;
     *                      2. collect the data from step 1 and unescape the partitions in java context using UNESCAPE_FN.
     *                      Schema at the end &lt;filename string, size bigint, partitions &lt;array&lt;string&gt;, unescape_partitions&lt;array%lt;string&gt;&gt;
     *                      3. Serialize the data in step 2 as temp table and run the pruning sql
     *                      Final Sql looks something like `select size, filename, cast(unescape_partitions[1] as date) as dt, ....from temp table where dt = ?
     *                      4. Remove all the filterExpression which do not have partition columns
     */
    public static List<FileStatus> pruneFiles(String basePath,
                                              String filterExpression,
                                              String[][] partitionDataTypes) throws SQLException, IOException {
        if (partitionDataTypes == null || partitionDataTypes.length == 0) {
            return pruneFilesNoPartition(basePath);
        }
        String firstSql = getQueryString(basePath, partitionDataTypes.length);
        String tempTableName = "connection_temp_table_" + System.currentTimeMillis();
        List<FileStatus> result = new ArrayList<>();
        try (DuckDBConnection readConnection = ConnectionPool.getConnection()) {
            String partitionSql = HivePartitionPruning.getPartitionSql(partitionDataTypes, tempTableName, filterExpression);
            String transformed = doQueryTransformation(readConnection, partitionSql,
                    Arrays.stream(partitionDataTypes).map(ss -> ss[0]).collect(Collectors.toSet()));
            try (DuckDBConnection writeConnection = ConnectionPool.getConnection();
                 BufferAllocator allocator = new RootAllocator();
                 ArrowReader reader1 = ConnectionPool.getReader(readConnection, allocator, firstSql, 1000);
                 Closeable ignored = ConnectionPool.createTempTableWithMap(writeConnection, allocator, reader1,
                         UNESCAPE_FN, List.of("partitions"), UNSCAPE_PARTITION_FIELD, tempTableName);
                 ArrowReader reader2 = ConnectionPool.getReader(writeConnection, allocator, transformed, 100)) {
                while (reader2.loadNextBatch()) {
                    VectorSchemaRoot root = reader2.getVectorSchemaRoot();
                    VarCharVector filename = (VarCharVector) root.getVector("filename");
                    BigIntVector size = (BigIntVector) root.getVector("size");
                    BigIntVector lastModifier = (BigIntVector) root.getVector("last_modified");
                    for (int i = 0; i < root.getRowCount(); i++) {
                        result.add(new FileStatus(new String(filename.get(i)), size.get(i), lastModifier.get(i)));
                    }
                }
            }
            return result;
        }
    }


    public static List<FileStatus> pruneFiles(String basePath,
                                              JsonNode tree,
                                              String[][] partitionDataTypes) throws SQLException, IOException {
        return pruneFiles(basePath, getFilterSql(tree), partitionDataTypes);
    }
    /**
     * Retrieves all files from a specified not partitioned directory path and returns their names and sizes.
     *
     * @param basePath The directory path where files are stored, expected to contain .parquet files.
     * @return A list of FileNameAndSize objects, each containing the name and size of a file.
     * @throws SQLException If there is an SQL error during the query execution.
     * @throws IOException If there is an I/O error during the handling of file data.
     */
    private static List<FileStatus> pruneFilesNoPartition(String basePath) throws SQLException, IOException {
        String sql = String.format(READ_BLOB_NO_PARTITION_SQL, basePath + "/*.parquet");
        List<FileStatus> result = new ArrayList<>();
        try(DuckDBConnection connection = ConnectionPool.getConnection();
            BufferAllocator allocator = new RootAllocator();
            ArrowReader reader = ConnectionPool.getReader(connection, allocator, sql, 1000)){
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                VarCharVector filename = (VarCharVector) root.getVector("filename");
                BigIntVector size = (BigIntVector) root.getVector("size");
                BigIntVector lastModifier = (BigIntVector) root.getVector("last_modified");
                for (int i = 0; i < root.getRowCount(); i++) {
                    result.add(new FileStatus(new String(filename.get(i)), size.get(i), lastModifier.get(i)));
                }
            }
        }
        return result;
    }

    /**
     * Generates a SQL query to read partition metadata (size and partition info) from files in a specified path.
     * This query is used to initially extract size(int64), filename(varchar), and partition information(varchar[]) before further processing.
     *
     * @param basePath The base directory containing partitioned parquet files.
     * @param partitionsLen The number of partition levels in the directory structure.
     * @return A SQL query string to read partition data.
     */
    protected static String getQueryString(String basePath,
                                         int partitionsLen) {
        String readBlobPath = getReadBlobPath(basePath, partitionsLen, "parquet");
        var cBasePath = (basePath.contains("/*")) ? basePath.substring(0, basePath.indexOf("/*")) : basePath;
        return String.format(READ_PARTITION_BLOB_SQL, cBasePath, partitionsLen, readBlobPath);
    }

    private static String getReadBlobPath(String basePath, int partitionsLen, String format) {
        if (basePath.contains("/*")) {
            return basePath;
        }
        return basePath +
                "/*".repeat(Math.max(0, partitionsLen)) +
                "/*." +
                format;
    }

    public static String unescapePathName(String path) {
        StringBuilder sb = new StringBuilder();
        var i = 0;

        while (i < path.length()) {
            var c = path.charAt(i);
            if (c == '%' && i + 2 < path.length()) {
                int code;
                try {
                    code = Integer.parseInt(path.substring(i + 1, i + 3), 16);
                } catch (Exception e) {
                    code = -1;
                }
                if (code >= 0) {
                    sb.append((char) code);
                    i += 3;
                } else {
                    sb.append(c);
                    i += 1;
                }
            } else {
                sb.append(c);
                i += 1;
            }
        }
        return sb.toString();
    }

    private static String getFilterSql(JsonNode tree) throws SQLException, JsonProcessingException {
        var statement = io.dazzleduck.sql.commons.Transformations.getFirstStatementNode(tree);
        var filter = Transformations.getWhereClauseForTableFunction(statement);
        var t = Transformations.parseToTree("select * from t");
        var firstStatement = (ObjectNode)Transformations.getFirstStatementNode(t);
        firstStatement.set("where_clause", filter);
        var newSql = Transformations.parseToSql(t);
        var index = newSql.indexOf("WHERE");
        if(index < 0) {
            return "";
        } else {
            return newSql.substring(index + "WHERE".length());
        }
    }
}



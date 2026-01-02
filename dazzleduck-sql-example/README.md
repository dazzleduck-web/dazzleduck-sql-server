# DazzleDuck SQL – Simple Example

This project contains a minimal example of using DazzleDuck SQL to collect application logs and store them as Arrow/Parquet files.

What This Example Does
- Starts a local DazzleDuck SQL HTTP server
- Runs a sample Java application
- Writes application logs to Parquet files in a local warehouse


## SampleApplication

- Generates simple log messages
- Uses ArrowSimpleLogger for demonstration
- Explicitly flushes logs using logger.close()


## How to Run

Setup the environment by export maven options
  ```bash
  export MAVEN_OPTS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
  ```
Start the server
```bash
cd dazzleduck-sql-runtime
mvn exec:java \
  -Dexec.mainClass=io.dazzleduck.sql.runtime.Main \
  -Dexec.args="--conf dazzleduck_server.networking_modes=[http] \
               --conf dazzleduck_server.http.port=8081 \
               --conf dazzleduck_server.warehouse=/tmp/dazzleduckWarehouse \
               --conf dazzleduck_server.ingestion.max_delay_ms=500"

```

Run the application
```bash
mvn exec:java -Dexec.mainClass=io.dazzleduck.sql.example.SampleApplication
```
Check the logs and metrics file
- Parquet files are created in tmp\dazzleduckWarehouse

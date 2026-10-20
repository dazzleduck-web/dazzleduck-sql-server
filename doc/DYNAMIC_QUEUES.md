# Dynamic Ingestion Queue Management

The otel-collector supports a **dynamic queue registry** backed by a SQLite database.
Queues can be added, updated, or deleted at runtime by any process that writes to the SQLite file —
no collector restart is required.

## How It Works

```
External process                    SQLite file                  otel-collector
────────────────────────────   ──────────────────────────   ────────────────────────────
INSERT / UPDATE / DELETE row    ingestion_queues table   →   background thread detects the
+ bump schema_version       →   schema_version bumped         schema_version change, reloads
(same transaction)              in the same transaction       registry within
                                                              config_load_interval_ms
```

1. The collector opens the SQLite file read-only via DuckDB's SQLite extension at startup and
   holds that one connection open for its lifetime.
2. A background thread polls the **`schema_version.version`** counter every
   `config_load_interval_ms` milliseconds (default 5 000 ms), through that persistent connection.
3. When the version changes the thread re-reads the entire `ingestion_queues` table and applies
   the diff to the in-memory registry.
4. The hot path (every incoming OTLP RPC) reads only from the in-memory registry — no SQLite
   I/O on the critical path.

> **Important:** change detection is driven by the `schema_version` counter, **not** by file
> mtime. Every external writer **must** bump `schema_version` in the *same transaction* as its
> data change (see the examples below) or the collector will not notice the change. This is
> deliberate: a long-lived process polling file mtime on a FUSE-backed bind mount (e.g. Docker
> Desktop for Mac / virtiofs) can see a stale cached mtime indefinitely, whereas reading the
> counter through the open connection forces a real read every time.

## Enabling the Dynamic Provider

In your HOCON config, set the provider class and point it at the SQLite file:

```hocon
otel_collector {
    ingestion_task_factory_provider {
        class                   = "io.dazzleduck.sql.commons.ingestion.DynamicDuckLakeIngestionTaskFactoryProvider"
        db_path                 = "/var/data/ingestion-queues.db"
        config_load_interval_ms = 5000   # optional, default 5000
    }
}
```

The SQLite file is created and the schema is initialised automatically on first startup
if the file does not exist.

## SQLite Schema

```sql
CREATE TABLE ingestion_queues (
    ingestion_queue  TEXT PRIMARY KEY,  -- queue ID used in JWT x-dd-ingestion-queue claim
    catalog          TEXT NOT NULL,     -- DuckLake catalog name
    schema_name      TEXT NOT NULL,     -- DuckLake schema (e.g. "main")
    table_name       TEXT NOT NULL,     -- DuckLake table name
    transformation   TEXT,             -- optional SQL expressions added as SELECT *, <expr>
    view_name        TEXT,             -- optional view name (reserved for future use)
    input_table      TEXT,             -- optional source table override
    input_schema     TEXT,             -- DuckDB column-def fragment for the __this input columns;
                                       --   used only by manage_tables (see below)
    partition_by     TEXT,             -- reserved; not yet applied at runtime
    min_bucket_size  INTEGER,          -- flush threshold in bytes (overrides global setting)
    max_delay_ms     INTEGER,          -- flush interval in ms (overrides global setting)
    num_partitions       INTEGER,      -- split into N hash-routed sub-queues (NULL/1 = off)
    partition_expression TEXT,         -- required when num_partitions > 1
    variables_view       TEXT,         -- full catalog.schema.table path of a key/value relation
                                       --   holding this queue's session variables (see below)
    variables_key_column TEXT,         -- name column in that relation (default: key)
    variables_value_column TEXT,       -- value column in that relation (default: value)
    variables_expiration_column TEXT   -- optional timestamp column; an expired row stops applying
);

-- Single-row counter the collector polls for change detection. Writers bump it (in the same
-- transaction as any change to ingestion_queues) so the collector reloads. Created automatically.
CREATE TABLE schema_version (
    id      INTEGER PRIMARY KEY CHECK (id = 1),
    version INTEGER NOT NULL DEFAULT 0
);
```

> The write location and partition columns are **not** stored in the registry — they are derived
> from the DuckLake table's own metadata (the table's `data_path` and partition spec).

### Session variables

A queue's transformation can read values with `getvariable('name')` instead of carrying them as
literals. The registry does not store the values, only the `catalog.schema.table` path of a
key/value relation holding them, so changing a value is a write to that relation and takes effect
on the next refresh — no `schema_version` bump, no restart.

That relation can live in this same SQLite file. The registry's own attachment is private to the
loader, so the ingestion connection needs its own, from the startup script:

```sql
-- startup script, on the ingestion connection
INSTALL sqlite; LOAD sqlite;
ATTACH '/var/data/ingestion-queues.db' AS reg (TYPE sqlite);
```

```sql
-- in the registry file
CREATE TABLE queue_vars(key TEXT, value TEXT, expires_at TEXT);
INSERT INTO queue_vars VALUES ('env', 'prod', NULL);

BEGIN;
UPDATE ingestion_queues
   SET variables_view = 'reg.main.queue_vars', variables_expiration_column = 'expires_at'
 WHERE ingestion_queue = 'logs';
UPDATE schema_version SET version = version + 1 WHERE id = 1;
COMMIT;
```

SQLite has no timestamp type, so an expiration is ISO text; it is cast for the comparison, and text
that will not cast is reported as an error rather than read as "never expires". A row whose
expiration has passed stops being applied, so the transformation reads `NULL` for that name.

## Target Table and Output Location

The output location is the DuckLake table's own `data_path` (read from catalog metadata), so the
**table must exist** before a queue can resolve its target — unless `manage_tables` is enabled (see
[Managing Tables](#managing-tables-manage_tables)), in which case the collector creates and evolves
the table for you.

When you provision tables yourself, the table's data path must be reachable by the collector:

- **Local paths:** the DuckLake `DATA_PATH` directory must exist — DuckDB's `COPY` will not create a
  missing parent directory.
- **Object stores:** the bucket must already exist and the collector must be configured with
  credentials for it — e.g. an httpfs S3 secret in the `startup_script` (`CREATE SECRET … (TYPE s3, …)`).

There is also **no default or fallback queue**. A queue is routable only while it is present in
`ingestion_queues`; an empty registry means every request is rejected. Requests whose
`x-dd-ingestion-queue` claim names an unregistered (or deleted) queue get `INVALID_ARGUMENT`.

## Managing Tables (`manage_tables`)

By default the operator must create each DuckLake table before its queue can be used. When
`manage_tables` is enabled, the collector instead creates and evolves the table to match the queue's
transformation output:

```hocon
otel_collector {
    ingestion_task_factory_provider {
        class                   = "io.dazzleduck.sql.commons.ingestion.DynamicDuckLakeIngestionTaskFactoryProvider"
        db_path                 = "/var/data/ingestion-queues.db"
        config_load_interval_ms = 5000
        manage_tables           = true   # optional, default false
    }
}
```

How the columns are derived: for each managed queue the collector materialises the row's
`input_schema` (a DuckDB column-definition fragment, e.g.
`timestamp TIMESTAMPTZ, severity_text VARCHAR, body VARCHAR`) as the `__this` input, applies the
queue's `transformation`, and describes the result — exactly the schema real ingestion would produce.
So a row must set `input_schema` to be managed (rows without one are skipped).

The collector writes every timestamp column as `TIMESTAMPTZ` (a UTC instant), so declare timestamp
columns in `input_schema` as `TIMESTAMPTZ`, not `TIMESTAMP`. DuckLake registers a file only if its
column types match the table's, so a table created with a `TIMESTAMP` column rejects every batch.

Reconciliation runs whenever the registry reloads (i.e. on each `schema_version` bump):

- **table absent** → `CREATE TABLE` with the derived columns, in order;
- **table present** → `ALTER TABLE ADD COLUMN` for each derived column missing (appended in derived
  order) and `DROP COLUMN` for each existing column no longer produced.

Constraints: columns are only **added or removed** — existing columns are never re-typed or
re-ordered (a DuckDB `ALTER` limitation) — and **tables are never dropped**. Deleting a queue only
stops ingestion; its table and data are left intact.

So correcting `input_schema` from `TIMESTAMP` to `TIMESTAMPTZ` does not fix a table that already
exists; point the queue at a new table instead (see "Timestamps" in the collector README).

## Queue Lifecycle Operations

Any process with write access to the SQLite file can perform the following operations.
Each change must bump `schema_version` **in the same transaction**, and is then picked up within
one `config_load_interval_ms` interval. (Ensure the target table exists, or enable `manage_tables`.)

### Add a queue

```sql
BEGIN;
INSERT INTO ingestion_queues
    (ingestion_queue, catalog, schema_name, table_name, input_schema)
VALUES
    ('app-logs', 'my_catalog', 'main', 'app_logs',
     'timestamp TIMESTAMPTZ, severity_number INTEGER, severity_text VARCHAR, body VARCHAR');
UPDATE schema_version SET version = version + 1 WHERE id = 1;
COMMIT;
```

Once the collector picks up the change, any OTLP request whose JWT contains
`x-dd-ingestion-queue: app-logs` will start writing to the new queue.
The `ParquetIngestionQueue` is created lazily on the first incoming message.

### Update a queue (e.g. change transformation)

```sql
BEGIN;
UPDATE ingestion_queues
SET transformation = 'CAST(timestamp AS DATE) AS date, severity_text AS level'
WHERE ingestion_queue = 'app-logs';
UPDATE schema_version SET version = version + 1 WHERE id = 1;
COMMIT;
```

The updated `transformation` SQL is applied on the next batch flush — in-flight batches
complete with the old transformation first.

### Delete a queue

```sql
BEGIN;
DELETE FROM ingestion_queues WHERE ingestion_queue = 'app-logs';
UPDATE schema_version SET version = version + 1 WHERE id = 1;
COMMIT;
```

The collector closes the queue (flushing any pending buffered data to Parquet), stops
accepting new messages for that queue ID, and unregisters the queue's metrics. Subsequent
requests referencing the deleted queue ID are rejected with `INVALID_ARGUMENT`.

## Concurrency Notes

- **One writer at a time**: SQLite does not support concurrent writers. If multiple external
  processes need to modify the registry, serialize their writes or use a single coordinator.
- **Collector is read-only**: The collector never writes to the SQLite file — it is always
  opened with `READ_ONLY true`. External writers can update the file at any time without
  coordination with the collector.
- **Atomic visibility**: The collector reloads the full table on each detected change, so a
  batch of inserts/updates committed in a single SQLite transaction is always seen atomically.

## Updating the Registry from DuckDB

DuckDB's SQLite extension can be used as a convenient admin interface:

```sql
-- Attach the registry (read-write)
ATTACH '/var/data/ingestion-queues.db' AS reg (TYPE sqlite);

-- List current queues
SELECT * FROM reg.ingestion_queues;

-- Add a queue (bump schema_version so the collector reloads)
BEGIN;
INSERT INTO reg.ingestion_queues
    (ingestion_queue, catalog, schema_name, table_name, input_schema)
VALUES ('audit-logs', 'my_catalog', 'main', 'audit_logs', 'timestamp TIMESTAMPTZ, body VARCHAR');
UPDATE reg.schema_version SET version = version + 1 WHERE id = 1;
COMMIT;

-- Update transformation
BEGIN;
UPDATE reg.ingestion_queues
SET transformation = 'CAST(timestamp AS DATE) AS date'
WHERE ingestion_queue = 'audit-logs';
UPDATE reg.schema_version SET version = version + 1 WHERE id = 1;
COMMIT;

-- Remove a queue
BEGIN;
DELETE FROM reg.ingestion_queues WHERE ingestion_queue = 'audit-logs';
UPDATE reg.schema_version SET version = version + 1 WHERE id = 1;
COMMIT;

DETACH reg;
```

## Updating the Registry from Python

```python
import sqlite3

con = sqlite3.connect("/var/data/ingestion-queues.db")

def bump(con):
    """Signal the collector to reload — call inside the same transaction as the change."""
    con.execute("UPDATE schema_version SET version = version + 1 WHERE id = 1")

# Add a queue (the connection's implicit transaction commits on con.commit())
con.execute("""
    INSERT INTO ingestion_queues (ingestion_queue, catalog, schema_name, table_name, input_schema)
    VALUES (?, ?, ?, ?, ?)
""", ("audit-logs", "my_catalog", "main", "audit_logs", "timestamp TIMESTAMPTZ, body VARCHAR"))
bump(con)
con.commit()

# Update transformation
con.execute("""
    UPDATE ingestion_queues SET transformation = ? WHERE ingestion_queue = ?
""", ("CAST(timestamp AS DATE) AS date", "audit-logs"))
bump(con)
con.commit()

# Delete a queue
con.execute("DELETE FROM ingestion_queues WHERE ingestion_queue = ?", ("audit-logs",))
bump(con)
con.commit()

con.close()
```

## Limitations

- `partition_by` is stored in the schema but is **not applied at runtime** — the collector
  always uses an empty partition list regardless of this column's value.
- `min_bucket_size` and `max_delay_ms` per-queue overrides are stored but not yet wired
  into the queue creation path — the global values from `ingestion` config are used.
- Reload depends on writers bumping `schema_version` in the same transaction as their change. A
  write that updates `ingestion_queues` without bumping `schema_version` will **not** be detected.
- The target table's data path is never created by the collector — provision the table (and its
  object-store bucket / local directory) ahead of time, or enable `manage_tables`. See
  *Target Table and Output Location*.
- With `manage_tables`, only columns are added/removed: existing columns are never re-typed or
  re-ordered (a DuckDB `ALTER` limitation), and tables are never dropped.

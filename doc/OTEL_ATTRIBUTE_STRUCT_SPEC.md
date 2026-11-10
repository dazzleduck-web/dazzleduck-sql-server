# Otel Attributes as One Struct — Design Specification

Status: **proposed.** Task 3 of `doc/DUCKDB_2_UPGRADE_PLAN.md`. The behaviour below was checked on
DuckDB 1.5.5 with stock DuckLake and on DuckDB 2.0.0 with DuckLake `417a9117`; both gave the same
results.

## Summary

The otel-collector writes each OTLP attribute set as six maps, one per value type. Today they are
six top-level columns. This spec makes them **one column per attribute set**, a struct with one map
per value type:

```sql
attributes STRUCT(
    string   MAP(VARCHAR, VARCHAR),
    "int"    MAP(VARCHAR, BIGINT),
    "double" MAP(VARCHAR, DOUBLE),
    bool     MAP(VARCHAR, BOOLEAN),
    bytes    MAP(VARCHAR, BLOB),
    "json"   MAP(VARCHAR, JSON)
)
```

The same type is used for `resource_attributes` and for the `attributes` of span events and links.
What goes into each map does not change.

## Today

Since #506 an attribute set is spread across the table:

- `attributes`, the original `MAP(VARCHAR, VARCHAR)`, stays at its old position, so that column
  indices and queries that predate the typed maps keep working.
- `attributes_int`, `attributes_double`, `attributes_bool`, `attributes_bytes` and
  `attributes_json` are appended at the end of the table, followed by the same five for
  `resource_attributes`.
- Span events and links carry the same six fields inside their structs.

So one attribute set is six columns in two places, and every table has ten extra top-level columns.

## Proposed Schema

| Signal | Columns today | Columns with the struct |
|---|---|---|
| Logs | 23 | 13 |
| Traces | 26 | 16 |
| Metrics | 29 | 19 |

`claims` (with `extract_claims = true`) is not an attribute set and stays a plain
`MAP(VARCHAR, VARCHAR)`, last.

**Positions.** `attributes` and `resource_attributes` keep the positions the string maps have today.
Nothing is appended after the signal's own columns.

**Field names** are the value types: `string`, `int`, `double`, `bool`, `bytes`, `json`. Three are
type keywords, so DDL quotes them (`"int"`, `"double"`, `"json"`), as DuckDB itself prints them. A
query does not need to:

```sql
SELECT * FROM logs WHERE attributes.int['http.status_code'] >= 500;
SELECT resource_attributes.string['service.name'], count(*) FROM spans GROUP BY 1;
SELECT attributes.json['tags'] ->> '$[0]' FROM spans;
SELECT e.attributes.string['exception.type'] FROM spans, unnest(events) AS u(e);
```

**Values are unchanged.** Each attribute is routed by its `AnyValue` kind as today. With
`otel_collector.attributes.string_copy` (default `true`) the `string` map also holds every
non-string value as text, so `attributes.string['k']` finds a key whatever its type. An attribute
with no value set is kept in `string` as `''`.

**Nulls.** A log record, span or data point always has a non-null struct, and each map is empty
when the record has no attribute of that type, as the six columns are today. A span event or link
with no attributes has a null struct, where today all six fields are null.

## Cost

- **Storage:** none to speak of. One million rows with realistic attribute maps came to 45,022,554
  bytes as six flat columns and 45,028,439 bytes as a struct (+0.01%). The Parquet leaf columns are
  the same; the struct adds one definition level.
- **Reads:** a query reads only the fields it uses. A DuckLake scan of `sum(attributes.int['k'])`
  projects `attributes.int` alone (`Projections: attributes.int` in `EXPLAIN ANALYZE`), so a struct
  costs nothing over separate columns for a query that uses one type.
- **Writes:** the same buffers as today plus one validity buffer per attribute set per batch. The
  writer is the collector's hottest loop, so the change is measured with the collector CPU benchmark
  before it merges (see Test Plan).

## Writer Changes

All in `dazzleduck-sql-otel-collector`:

- `AttributeType` gives each value type its struct field name (`string`, `int`, ...) in place of a
  column suffix.
- `OtelSchemaFields` builds the struct field; `typedAttributeFields` and `withTypedAttributes` go.
  The log, trace and metric schemas and the event and link structs use it.
- `AttributeMapsWriter` takes the struct vector and fills its six child maps, buffering a batch and
  loading each buffer in one copy as today. It also builds the struct's validity buffer: all valid
  for a top-level set, and null for an event or link without attributes, which `writeNull` already
  marks.
- `COL_TYPED_ATTRIBUTES` and `COL_TYPED_RESOURCE_ATTRIBUTES` go; `COL_ATTRIBUTES` and
  `COL_RESOURCE_ATTRIBUTES` name the struct columns.
- README (schema tables and the Typed attributes section) and the comment in `reference.conf`.

## Existing Tables

A DuckLake table created for the flat layout cannot take the new files, and DuckLake cannot alter a
`MAP` column into a `STRUCT`.

**It fails loudly.** The ingestion path registers files with `ducklake_add_data_files`, passing
`ignore_extra_columns => true` and `allow_missing => true`. A struct file meeting a flat table is
rejected, not partly loaded:

```
Invalid Input Error: Failed to map column "attributes" from file "..." to the column in table "flat"
* Expected type "MAP(VARCHAR, VARCHAR)" but found type "STRUCT(string MAP(VARCHAR, VARCHAR), ...)"
```

The second line is DuckDB 2.0's; 1.5 prints only the first. The batch fails and nothing is lost,
but every batch of that queue fails the same way.

**A startup check.** So that this shows at startup instead of on the first batch, a queue runs its
transformation over an empty batch of the collector's schema and compares the result's columns with
its table, failing startup with the table, the column and both types. The sidecar spec
(`doc/SIDECAR_VIEWS_SPEC.md`) needs the same check for its tables; it is one shared check, not two.

An existing deployment then has three ways forward.

### Keep the flat tables

A queue transformation rewrites the struct back into the old columns before the file is written.
Checked: the transformed file registers into a flat table and queries return the same rows.

Logs and metrics:

```sql
SELECT * EXCLUDE (attributes, resource_attributes),
       attributes.string AS attributes,
       attributes.int    AS attributes_int,
       attributes.double AS attributes_double,
       attributes.bool   AS attributes_bool,
       attributes.bytes  AS attributes_bytes,
       attributes.json   AS attributes_json,
       resource_attributes.string AS resource_attributes,
       resource_attributes.int    AS resource_attributes_int,
       resource_attributes.double AS resource_attributes_double,
       resource_attributes.bool   AS resource_attributes_bool,
       resource_attributes.bytes  AS resource_attributes_bytes,
       resource_attributes.json   AS resource_attributes_json
FROM __this
```

Traces add the same for `events` and `links`, whose attributes sit inside lists:

```sql
list_transform(events, lambda e: struct_pack(
    name := e.name, time_ms := e.time_ms,
    attributes := e.attributes.string, attributes_int := e.attributes.int,
    attributes_double := e.attributes.double, attributes_bool := e.attributes.bool,
    attributes_bytes := e.attributes.bytes, attributes_json := e.attributes.json)) AS events
```

Matching is by name, so the column order of the result does not matter. The transformation costs a
projection per batch.

### Move to new tables

Create new tables with the struct layout and point the queues at them. A view keeps the old data
queryable with the new shape:

```sql
CREATE VIEW logs_all AS
SELECT * EXCLUDE (attributes, attributes_int, attributes_double, attributes_bool, attributes_bytes,
                  attributes_json, resource_attributes, resource_attributes_int,
                  resource_attributes_double, resource_attributes_bool, resource_attributes_bytes,
                  resource_attributes_json),
       {'string': attributes, 'int': attributes_int, 'double': attributes_double,
        'bool': attributes_bool, 'bytes': attributes_bytes, 'json': attributes_json} AS attributes,
       {'string': resource_attributes, 'int': resource_attributes_int,
        'double': resource_attributes_double, 'bool': resource_attributes_bool,
        'bytes': resource_attributes_bytes, 'json': resource_attributes_json} AS resource_attributes
FROM logs_old
UNION ALL BY NAME
SELECT * FROM logs;
```

Checked with a flat and a struct table: the view returns both tables' rows and
`attributes.int['k']` works across them.

### New deployments

Create the tables with the struct columns. Nothing else changes.

## Everything That Names the Columns

| Where | Change |
|---|---|
| Queue transformations | References to `attributes['k']` or `attributes_int['k']` become `attributes.string['k']` and `attributes.int['k']` |
| Partitioning | A partition expression over an attribute changes the same way |
| Row filters | `x-dd-access` filters and RESTRICT_READ_ONLY filter CTEs that test an attribute |
| Dashboards and saved queries | Same rewrite; the `logs_all` view above serves them during a move |
| `doc/ATTRIBUTE_TYPE_REGISTRY_SPEC.md` | Its queries over the six maps become queries over the struct's fields |
| `doc/SIDECAR_VIEWS_SPEC.md` | Its attribute examples |

## Adding a Value Type Later

A new OTLP value type becomes a new struct field, which DuckLake handles as a schema change. Checked
on both versions:

```sql
ALTER TABLE logs ADD COLUMN attributes.extra MAP(VARCHAR, VARCHAR);
```

Existing files read the new field as NULL. With the flat layout the same change is a new top-level
column per attribute set.

## Rejected Alternatives

**A layout setting in the collector (`flat` or `struct`).** It would keep existing tables working
without a transformation, but the collector would carry two schemas, two writer paths and two sets
of tests indefinitely. The transformation above gives the same files from one writer, and is
configuration an operator can remove once the tables move.

**One map with a union value, `MAP(VARCHAR, UNION(...))`.** One lookup per key, but every read
pays for `union_extract`, a query cannot read only one value type, and a filter on a typed value is
written against the union rather than a plain type.

**Leave the layout as it is.** It works, but the ten appended columns are the cost of adding the
typed maps without breaking queries, and that cost grows with every new value type. The DuckDB 2.0
upgrade is a point where consumers expect changes; doing it then makes one break instead of two.

## Test Plan

1. Schema: each signal's schema has the struct at the old string map's position, the column counts
   above, and the six fields in order (replaces `SchemaColumnIndexTest`'s typed-column checks).
2. Values: every `AnyValue` kind lands in its field, with and without `string_copy`; an attribute
   with no value is `''` in `string` (adapts `OtelCollectorTypedAttributesTest`).
3. Nulls: an event or link without attributes has a null struct; a record without attributes has a
   non-null struct of empty maps.
4. Large batches: alignment of the struct and its children with the other columns across buffer
   growth (adapts `LargeBatchAlignmentTest`).
5. End to end into DuckLake on DuckDB 2.0: a struct table, a flat table with the transformation,
   and the startup check failing on a flat table without it.
6. Collector CPU: steady-rate A/B against the current writer; no regression beyond noise.

## Open Questions

1. **Empty maps or null fields.** A record with no `int` attribute gets an empty `int` map, as
   today. Null fields would save a little space and make `attributes.int IS NULL` meaningful, at the
   cost of a second way to say "none".
2. **A shipped transformation.** Whether the collector should ship the flattening transformations
   (one per signal) so an operator names one instead of pasting the SQL above.
3. **Release.** The change breaks existing tables. Whether it waits for the release with the DuckDB
   2.0 bump, so the release notes cover both at once.

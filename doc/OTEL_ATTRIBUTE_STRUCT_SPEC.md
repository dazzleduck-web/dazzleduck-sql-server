# Otel Attributes as One Struct — Design Specification

Status: **proposed.** Task 3 of `doc/DUCKDB_2_UPGRADE_PLAN.md`; it ships after the DuckDB 2.0
upgrade (Task 2), in the same release. The behaviour below was checked on
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

**Never null.** OTLP has no null attribute set. `LogRecord`, `Span`, `Span.Event`, `Span.Link`
and `Resource` all declare their attributes as `repeated KeyValue`, so "no attributes" is an empty
list, never an absent one. The one part that can be absent is the `Resource` message itself, which
also means no resource attributes. So the struct is never null, and each map is empty when there is
no attribute of that type.

This changes span events and links. Today an event or link with no attributes has all six fields
null; with the struct it has a non-null struct of empty maps, like every other attribute set, so a
query tests `cardinality(e.attributes.string) = 0` rather than `IS NULL`. A null typed field would
invent a state OTLP does not have, and give two ways of saying "none".

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
  loading each buffer in one copy as today. The struct's validity buffer is all valid. `writeNull`
  goes, and `SpanListWriter` writes an event or link without attributes as empty maps.
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

**A startup check.** So that a mismatch shows at startup instead of on the first batch, a queue runs
its projection and transformation over an empty batch of the collector's schema and compares the
result's columns with its table, failing startup with the table, the column and both types. It
extends the check that already warns, when a queue's state is first built, about a pass-through
table missing the `claims` column. The sidecar spec (`doc/SIDECAR_VIEWS_SPEC.md`) needs the same
check for its tables; it is one shared check, not two.

An existing deployment then has three ways forward.

### Keep the flat tables: `attribute_layout`

The collector ships the flattening, so an existing table keeps working with no SQL and, by default,
no configuration. A queue mapping takes:

```hocon
{ ingestion_queue = "logs", catalog = "my_catalog", schema = "main", table = "logs",
  attribute_layout = auto }    # auto | struct | flat
```

- `struct` writes the struct columns.
- `flat` applies the collector's shipped projection for the queue's signal, which writes today's six
  columns per attribute set.
- `auto`, the default, follows the target table: `flat` when its `attributes` column is a `MAP`,
  `struct` otherwise, including a table that does not exist yet and plain Parquet output. An
  existing deployment therefore keeps writing its tables after the upgrade without a change.

The projection runs on the batch before the queue's own transformation, which reads its output as
`__this`. A transformation or view written against the flat columns, such as one deriving
`attributes['tenant']`, keeps working unchanged.

There is still one writer: the collector always builds the struct, and `flat` is a projection over
it. Checked: the projected file registers into a flat table and queries return the same rows. The
one difference from today is the change above: an event or link without attributes has empty maps
in the flat layout too, not nulls.

The shipped projection for logs and metrics:

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

For traces it adds the same for `events` and `links`, whose attributes sit inside lists:

```sql
list_transform(events, lambda e: struct_pack(
    name := e.name, time_ms := e.time_ms,
    attributes := e.attributes.string, attributes_int := e.attributes.int,
    attributes_double := e.attributes.double, attributes_bool := e.attributes.bool,
    attributes_bytes := e.attributes.bytes, attributes_json := e.attributes.json)) AS events
```

Matching is by name, so the column order of the result does not matter. It costs a projection per
batch.

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

**A second writer for the flat layout.** Keeping today's writer next to the new one would avoid the
projection, but the collector would carry two writer paths and two sets of writer tests
indefinitely. The shipped projection gives the same files from one writer.

**Leaving the flattening to each operator's transformation.** Every deployment with existing tables
would paste the same SQL, and would have to fold it into any transformation it already has. Shipping
it, applied before the queue's transformation, makes the upgrade need nothing.

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
3. Never null: a record, event or link without attributes has a non-null struct of empty maps, and
   so does a resource that is absent from the request.
4. Large batches: alignment of the struct and its children with the other columns across buffer
   growth (adapts `LargeBatchAlignmentTest`).
5. `attribute_layout`: `auto` picks `flat` for a table whose `attributes` is a `MAP` and `struct`
   otherwise; `flat` output registers into a flat table; a queue transformation written against the
   flat columns runs over it unchanged.
6. End to end into DuckLake on DuckDB 2.0: a struct table, a flat table under `auto`, and the
   startup check failing on a flat table with `attribute_layout = struct`.
7. Collector CPU: steady-rate A/B against the current writer, for `struct` and for `flat`; no
   regression beyond noise.

## Decisions

1. **No nulls.** OTLP has no null attribute set (see *Never null*), so the struct is never null and
   a type with no attribute is an empty map.
2. **The flattening ships with the collector**, as `attribute_layout`, defaulting to `auto`.
3. **Release.** The DuckDB 2.0 upgrade goes first; this ships after it, in the same release, so the
   release notes cover both at once.

## Open Questions

1. **How long `flat` is supported.** Whether it is deprecated after a set number of releases, or
   kept as long as tables in that layout exist.

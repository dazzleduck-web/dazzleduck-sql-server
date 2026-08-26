# DazzleDuck SQL Search

Inverted-index construction for full-text search over Parquet files, built entirely as DuckDB
SQL plus Arrow round-trips (no external search engine at runtime).

**Status: indexing only.** The query side (`Plan`, `Split`) consists of interfaces with no
implementations yet — this module currently builds indexes but does not serve searches.

## How Indexing Works

`Indexer.create(sourcePrefix, sourceFiles, sourceFields, timeField, tokenizationFunctions, indexFile)`:

1. Reads the source Parquet files with `read_parquet(..., filename = true)`, carrying
   `file_row_number` and `filename` through the pipeline
2. Tokenizes the requested VARCHAR columns in Java (via a `MappedReader` function from
   `dazzleduck-sql-commons`) into an Arrow struct of token lists
3. UNPIVOTs / UNNESTs / GROUPs the tokens in DuckDB into a posting-list Parquet file:
   token, column, source file, and the row numbers containing the token

Posting lists for very frequent tokens (more than 1000 rows in a file) store `NULL` row
numbers — for those tokens the index degrades to a file-level filter rather than row-level.

## Key Types

| Type | Purpose |
|------|---------|
| `Indexer` | Static entry point and SQL construction (`create`, `constructInputSql`, `constructWriteSql`) |
| `TokenizationFunction` | `String` to `String[]` tokenizer applied per column |
| `Plan` / `Split` | Query-side SPI — **not yet implemented** |

## Requirements

- Java 21
- Depends on `dazzleduck-sql-commons` (`ConnectionPool`, `MappedReader`)

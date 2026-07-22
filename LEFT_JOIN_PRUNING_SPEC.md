# Unused LEFT JOIN Elimination — Design Specification

## Overview

Given an **outer query that selects from a view**, rewrite it so that
`LEFT JOIN`s inside the view whose columns the outer query never references are
removed — producing a semantically equivalent but cheaper query.

Two chained optimizations run: **projection pushdown** (the outer query prunes
the view's `SELECT` list) followed by **join elimination** (a table that then
contributes no referenced column is dropped).

**Status:** specification + scaffolded failing tests. Implementation is a no-op
stub (`Transformations.pruneUnusedLeftJoins`) pending the work described here.

---

## Worked Example

```sql
-- view
CREATE VIEW fv AS
SELECT f.*, a.a_name, b.b_name
FROM   f
LEFT JOIN a ON f.a_id = a.a_id
LEFT JOIN b ON f.b_id = b.b_id;

-- outer query (input)
SELECT f_col, a_name FROM fv WHERE f_col > 10;

-- output
SELECT f_col, a_name
FROM (
  SELECT f.*, a.a_name          -- b_name projection dropped (projection pushdown)
  FROM   f
  LEFT JOIN a ON f.a_id = a.a_id -- LEFT JOIN b eliminated (join elimination)
) AS fv
WHERE f_col > 10;
```

`F` is the fact table (preserved, left side). `A` and `B` are dimension tables.
Because only `a_name` is used, the join to `B` is dead and is removed.

---

## API

```java
/**
 * Prune LEFT JOINs from an inlined view whose columns the outer query never references.
 * Optimization only — returns the outer AST structurally unchanged on any uncertainty.
 *
 * @param outerSqlAst parsed outer query (a statement wrapper) selecting from the view
 * @param viewBodyAst parsed view body (the F/A/B join SELECT) — the caller fetched and parsed it
 * @return rewritten outer AST with the view inlined as a pruned subquery, or outerSqlAst unchanged
 */
public static JsonNode pruneUnusedLeftJoins(JsonNode outerSqlAst, JsonNode viewBodyAst)
```

- **AST in, AST out** (`JsonNode`). The caller owns `parseToTree` / `parseToSql`;
  this function never touches SQL strings and takes no DuckDB connection.
- **Which base table is the view?** The caller passes no view name, so v1 relies
  on the precondition that the outer `FROM` contains **exactly one `BASE_TABLE`** —
  that reference is replaced by `viewBodyAst`.
- **No `conn` in v1.** The transform is pure AST. The deferred join-key uniqueness
  verification will need a `Connection`; it is intentionally left off the v1
  signature so no unused parameter is carried until then.
- **Contract shift:** because `viewBodyAst` is supplied directly, the function
  cannot confirm the single outer base table is actually a view — it trusts the
  caller that `viewBodyAst` is the body for that reference.

---

## Soundness Preconditions (per candidate LEFT JOIN)

A `JOIN` node is eliminable only when **all** of these hold:

1. `join_type == "LEFT"` and the candidate table is the **`right`** arm (the
   null-supplying side). The preserved `left` / fact side is never dropped.
2. The `right` arm is a **`BASE_TABLE`**. (v1 restriction; subquery / nested-join
   right arms are out of scope.)
3. **No column of the right arm is referenced** anywhere in the *retained* tree —
   the view's (pruned) `select_list`, `where_clause`, `group_expressions`,
   `having`, `qualify`, modifiers (ORDER BY / etc.), **and the `ON` conditions of
   joins that remain**.
4. The `ON` `condition` is an **equi-join** — `COMPARE_EQUAL`, or a
   `CONJUNCTION_AND` of them (`ref_type == REGULAR`, `using_columns` empty). This
   is a *shape* guard only.

### Trusted assumption (stated loudly)

Per the design decision, v1 **trusts** that each dimension table is unique on its
join key and performs **no** row-multiplication check. If a dimension is *not*
unique on the join key, a `LEFT JOIN` multiplies fact rows, and eliminating it
changes row count / aggregate results even though no dimension column is selected.
**This is the single biggest correctness caveat** and is documented on the API.

Scope decisions locked for v1:

| Decision | Choice |
|---|---|
| Row-multiplication safety | Trust join-key uniqueness (no verification) |
| Join types handled | `LEFT` only |
| Input / output | Outer query over a view; inline the view and prune |

---

## Algorithm

```
pruneUnusedLeftJoins(outerSqlAst, viewBodyAst):
  outer      = getFirstStatementNode(outerSqlAst)
  viewRef    = the single BASE_TABLE in outer FROM            // v1: exactly one
  if none    -> return outerSqlAst unchanged

  usedViewCols = column refs in outer qualified to viewRef (alias-aware)
  if bare STAR over the view -> return outerSqlAst unchanged   // all columns used

  body = deep-copy(getFirstStatementNode(viewBodyAst))         // never mutate caller's node

  (a) projection pushdown:
      prune body.select_list to entries whose output name in usedViewCols

  (b) join elimination to a fixpoint:
      repeat until no change:
        counts = one walk of the body counting references per table/alias
        for each JOIN node (bottom-up) meeting preconditions above,
            where every reference to the right arm comes from its own ON condition:
          replace the JOIN node with its `left` arm
      -- one count-walk per ROUND (not per candidate): O(rounds x body size),
      -- rounds bounded by the longest transitive elimination chain. Stale counts
      -- within a round only over-count, so a join is never wrongly freed.

  (c) re-inline:
      replace viewRef in outer with (serialized body) AS <viewRef alias/name>

  return outerSqlAst   // mutated copy; same JsonNode kind in and out
```

### Notes

- **Fixpoint (b) is required.** Dropping `B` removes the `f.b_id` / `A` references
  in its `ON` clause, which can make `A` droppable on the next round.
- **Column ownership.** Each `COLUMN_REF.column_names` is `["alias","col"]` or
  `["col"]`; resolve the qualifier against join-arm aliases / table names.
  **Self-joins** are disambiguated by alias, not table name.

---

## Fail-safe Rule

The transformation is an **optimization, never a semantic change**. On *any*
uncertainty it returns the input AST unchanged (degrading to a no-op — the
fail-closed discipline of commit #358, but non-throwing here). When nothing is
optimized, the **same instance** passed in is returned, so callers can detect a
no-op by reference identity. Explicit bail-outs:

- **Any STAR in the outer query** — bare (`*`) *or table-qualified* (`fv.*`,
  `v.*`): both expand to view columns that cannot be enumerated at the AST level.
- **Unqualified** column reference that cannot be attributed to a single arm.
- `USING(...)` join (null `condition`, populated `using_columns`).
- `right` arm not a base table; non-equi `ON` condition.
- More than one `BASE_TABLE` in the outer `FROM`.
- **Outer `FROM` resolves to a WITH-declared CTE** — a local CTE shadows any
  catalog view of the same name (`WITH fv AS (…) SELECT … FROM fv`), so the
  reference is the CTE, not the view; inlining the view body there would change
  results.

### Scope-aware variant — view referenced inside a CTE

`pruneUnusedLeftJoins(outerSqlAst, viewName, viewBodyAst)` extends the above to the
case where the view is referenced inside **CTE bodies** and/or at the outer top-level
`FROM`, e.g. `WITH a AS (SELECT a_name FROM fv) SELECT * FROM a`. Passing `viewName`
lets references be located by name. Every eligible reference is pruned
**independently within its own scope** — inlining is local and semantics-preserving,
so any subset of references may be optimized:

- **Top-level** — outer `FROM` is the view. Usage is collected from the outer SELECT
  scope **excluding CTE bodies** (a CTE cannot reference the outer `FROM`'s columns),
  so sibling CTE usage does not pollute top-level pruning.
- **CTE-nested** — the view is in the `FROM` of one or more CTE bodies. Column usage
  and the STAR check are evaluated **within each CTE body scope**, so an outer
  `SELECT *` *over the CTE* (which expands to CTE columns, not view columns) does not
  block the optimization. The pruned view body is inlined in place of the reference
  inside each CTE body; the CTE's own `SELECT` list is untouched.

A scope whose view columns cannot be enumerated (a bare or qualified STAR **in that
scope**) is skipped individually; other references are still pruned.

`viewName` may be **qualified** (`s2.fv`, `cat.s2.fv`); qualification must match the
reference at the same level. An unqualified name matches only unqualified references —
a qualified reference (`s2.fv`) may be a *different*, same-named view. A qualified name
matches only identically-qualified references — an unqualified reference resolves via
the search path, which is invisible at the AST level. Qualified references can never
resolve to a CTE, so qualified names skip the CTE-shadow bail below.

Additional bail-out for this variant: an **unqualified** view name rebound by a CTE
anywhere in the outer `WITH` (sibling/outer shadow) is a full no-op.

Additional conservative rules:

- **Projection pushdown is skipped** (join elimination still applies) when the
  view body has `DISTINCT`, `GROUP BY` / grouping sets, or any other modifier —
  under `DISTINCT` the select list *is* the dedup key, and positional references
  (`GROUP BY 1`, `ORDER BY 1`) would silently re-point if entries were dropped.
- **Multi-part column references** (`s.field`) are ambiguous at parse time
  between `table.column` and `struct_column.field`; *every* part is recorded as
  a potentially-used column name so a struct column projected by the view is
  never pruned away (keeps too much, never too little).

---

## AST Reference (DuckDB `json_serialize_sql`)

`LEFT JOIN` node shape (verified against DuckDB 1.x):

```json
{
  "type": "JOIN",
  "left":  { "type": "BASE_TABLE", "table_name": "f", ... },
  "right": { "type": "BASE_TABLE", "table_name": "b", ... },
  "condition": {
    "class": "COMPARISON", "type": "COMPARE_EQUAL",
    "left":  { "class": "COLUMN_REF", "column_names": ["f","b_id"] },
    "right": { "class": "COLUMN_REF", "column_names": ["b","b_id"] }
  },
  "join_type": "LEFT",
  "ref_type": "REGULAR",
  "using_columns": []
}
```

`left` / `right` are themselves FROM-nodes, so joins form a left-deep / nested
tree: `((F LJ A) LJ B)` — the outermost `JOIN` has `left = (F LJ A)`, `right = B`.
Dropping `B` replaces the outer node with its `left`. Dropping the middle `A`
requires recursing into the left arm and replacing `(F LJ A)` with `F`.

---

## Code Touch-points

| Concern | Reuse |
|---|---|
| First SELECT node | `Transformations.getFirstStatementNode` |
| Column refs | `Transformations.collectReferences`, `getReferenceName` |
| FROM/JOIN walk template | `collectAllTableRefsFromFromNode` JOIN case; `renameBaseTablesInFromNode` |
| New constants | add `join_type`, `ref_type`, `condition`, `using_columns` to `ExpressionConstants` (today raw literals / absent) |
| New logic | `Transformations.pruneUnusedLeftJoins` + JOIN-replace / wrap-as-subquery helpers alongside `injectFilterCtes` |

View-body fetching (`duckdb_views()`, SQL slicing) is **the caller's job** and is
not part of this function.

---

## Test Plan (JUnit 5, `dazzleduck-sql-commons`)

Scaffolded in `PruneUnusedLeftJoinsTest`:

- Only `a_name` used → `B` dropped, `A` kept (1 join remains).
- Both dims used → no change (2 joins).
- Neither dim used → both dropped (0 joins).
- Transitive: `A` referenced only by `B`'s `ON` clause; drop `B` → drop `A`
  (fixpoint). *(scaffold, `@Disabled` until implemented.)*
- Fail-safe: bare `*` over view; `USING` join; multi-table outer FROM → input
  returned unchanged.
- Round-trip equivalence: the pruned output re-parses and, on a seeded dataset
  containing a fact row with an **unmatched** dimension key, returns rows
  identical to the original outer query against the real view — the true
  equivalence check that `LEFT` semantics are preserved.

---

## Open Questions

1. **Output form** — inline the pruned view as a subquery (as shown), or emit a
   rewritten `CREATE VIEW`? Inline is self-contained; a rewritten view helps if
   reused.
2. **Scope of v1 input** — hard-require exactly one view in the outer FROM, or
   handle several?
3. **Actual view vs any subquery** — the same machinery works if the outer query
   already contains the join tree as an inline subquery (no catalog lookup).
   Cover that in v1, or view-reference only?

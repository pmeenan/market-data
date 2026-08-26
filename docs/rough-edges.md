# Rough edges — findings log

Tiingo, DuckDB, polars, and platform bugs, quirks, surprising limits,
performance cliffs, and missing capabilities encountered while building
market-data. Log the ones that burned real debugging time and will bite
again — this is a save-future-you log, not a compliance artifact.

**Before adding:** grep for the API/library involved to avoid duplicates.
**Before debugging weirdness:** check here first — it may be known.

A good entry says what environment it happened in and what was observed vs.
expected; include a reproduction when it's cheap to capture.

Format:

```
## RE-NNN: Title  (YYYY-MM-DD, status: open | fixed-upstream | worked-around | wontfix)
Environment / Repro or measurement / Observed / Expected / Impact / Links
```

Newest first. RE-numbers are never reused.

---

## RE-001: DuckDB→polars conversion requires pyarrow  (2026-08-26, status: worked-around)

**Environment:** Python 3.12, duckdb 1.x, polars 1.x, Linux.

**Observed:** `con.pl()` raised `ModuleNotFoundError: No module named
'pyarrow'` even though polars itself no longer depends on pyarrow — DuckDB's
polars bridge goes through Arrow.

**Expected:** duckdb + polars installed ⇒ `.pl()` works.

**Impact:** `pyarrow` is a required dependency in pyproject.toml solely for
this bridge; don't remove it when pruning deps.

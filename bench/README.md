# bench/ — Leveled FTS benchmarks

Two suites live here:

1. **`fts_bench.py`** — the original orchestrator: prepares a Wikipedia corpus
   (`.bz2` → TSV), drives SQLite FTS5 (`sqlite_fts_bench.c`) and Leveled
   (`leveled_fts_bench.erl`) over shared query sets, and compares (`prepare`,
   `sqlite`, `leveled`, `compare`, `run`, `sweep`, `ops-equivalence`, …).
   Unranked (`rank=none`, `ORDER BY key`) with a 2x default threshold.

2. **Equivalence + 5x latency gate** (added 2026-07) — extends both engine
   benches with a **rank** mode (BM25) and Leveled **uncached/amortized**
   measurement regimes, and adds a scale-ladder driver and a gate script.
   Report: [`../docs/fts_sqlite_gate.md`](../docs/fts_sqlite_gate.md).

## The gate suite

| file | role |
|---|---|
| `fts_gate_queries.txt` | curated 13-query set covering every shape (term, phrase, column, AND/OR/NOT, prefix, NEAR, nomatch) in the grammar the differential contracts prove identical |
| `run_ladder.py` | slices the shared TSV by text size, runs both engines × rank × regime per rung, cleans up per rung |
| `check_gate.py` | classifies shapes, computes `leveled_median/sqlite_median` per `(scale, shape, rank)`, enforces the 5x rule, checks equivalence; exits nonzero on breach or unranked mismatch |

### New engine-bench flags

- `sqlite_fts_bench.c --rank none|bm25` — `bm25` runs `ORDER BY rank, key` and
  emits a `query_scores` TSV row (JSON `%.17g` scores). Schema otherwise
  unchanged.
- `leveled_fts_bench.erl --rank none|bm25` — threads `rank => bm25` into
  `book_ftssearch`; emits ordered keys + a `query_scores` row.
- `leveled_fts_bench.erl --uncached` — before each timed run, re-puts one
  existing document (idempotent for results) to advance the FTS write sequence
  and bust the per-sequence **result**, **batch-list**, and **corpus-stats**
  caches, so each timed query does full posting work (page/directory caches
  stay warm). Write-per-query worst case; the gate basis. Without it Leveled
  serves every repeat from its result cache in ~microseconds.
- `leveled_fts_bench.erl --amortized` — bump once per timed run, then run an
  untimed absorber query at the new sequence to re-derive the batch-list and
  (ranked) corpus-stats caches; the timed query is then stats-warm but
  result-cache-cold. Steady-state cost of a *novel* query between writes (the
  fair production number for read-mostly workloads).

### Run it

```bash
# 0. SQLite amalgamation + FTS5 CLI under /Users/dvse/repos/sqlite (once):
#    make sqlite3.c sqlite3            # needs tcl 8.6 (e.g. brew tcl-tk)
#    cc -O2 -DSQLITE_ENABLE_FTS5 -I. shell.c sqlite3.c -lz -lpthread -lm -o sqlite3

# 1. Corpus (cached):
python3 fts_bench.py prepare --corpus-dir bench_corpus/wikipedia \
  --tsv /tmp/leveled_fts_bench/docs.tsv --meta /tmp/leveled_fts_bench/corpus_meta.json

# 2. Ladder (uncached = gate basis; amortized = production number):
python3 run_ladder.py --regime uncached --reuse-sqlite
python3 run_ladder.py --regime amortized --reuse-sqlite
#    default rungs: s100m (~100 MB), s1g (~1 GB), full (~5.3 GB)
#    FTS_KEEP=1 keeps per-rung stores/dbs for debugging

# 3. Gate + markdown tables + JSON summary:
python3 check_gate.py --results-dir /tmp/leveled_fts_bench/results/gate \
  --scales s100m,s1g,full --leveled-regime uncached --threshold 5.0 \
  --markdown /tmp/gate_tables.md --summary /tmp/gate_summary.json
#    --leveled-regime warm  gates the result-cache-hot numbers instead
```

Build time and on-disk size are reported by the gate but never affect its exit
code. See the report for the equivalence verdict and the cost dissection.

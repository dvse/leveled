# Consolidated Leveled benchmark suite

`bench.py` is the single entrypoint for every benchmark in this directory.
It builds and drives the current Leveled path and registered comparison
targets, normalizes their results, applies gates, and writes the same two artifacts for every
subcommand: `<name>.json` and `<name>.md`.

`BAKEOFF.md` is retained only as historical design evidence. It is not an
entrypoint. The deleted `leveled_fts_ops_bench` is not preserved because it
called the removed `book_ftssearch` API; its useful coverage is now in `fts`.

## Inventory

| File | Purpose |
|---|---|
| `bench.py` | Corpus preparation, target registry, 25/50/100% ladders, driver builds, normalization, 2x gate math, and JSON/Markdown rendering |
| `leveled_bench.erl` | One subcommand-driven driver for FTS-2, secondary indexes, batched heads, and compression |
| `sqlite_bench.c` | One subcommand-driven SQLite driver using FTS5 and current B-tree paths |
| `README.md` | This front door and report-format contract |
| `BAKEOFF.md` | Historical FTS layout decision; not executable |

No wrapper scripts, cache-busting regimes, generated Python cache, or drivers
for deleted APIs belong in `bench/`.

## Prerequisites

- OTP/Erlang and this checkout's `rebar3`.
- Python 3.11 or later (standard library only).
- A C11 compiler, SQLite 3 with FTS5 development headers, and OpenSSL
  development headers. On this host the link flags are `-lsqlite3 -lcrypto`.
- For the governing FTS run, the extracted corpus JSONL at
  `/home/dvse/bench3/fts_arena_20260801/corpus/full.jsonl`. A JSONL row must
  provide `text_b64` (or plain `content`) and should provide `chunk_id`, `udi`,
  and `content_version`.

The orchestrator compiles both drivers into the requested output directory;
it does not create build products in `bench/`.

## Subcommands

### `fts`

The primary comparison runs the embedded 29-production public grammar catalog
and the standing 34-query grid. It prepares deterministic 25/50/100% corpus
rungs and reports both rank modes.

The gated default is `--store-config live`: native store and ledger
compression, 8,000-row penciller cache, 256-slot SST geometry, 24-file merge
fan-in, 2,500-row read cache, and row-addressable SST block version 2. The
`legacy_uncompressed` configuration is diagnostic only and cannot silently
stand in for the governing live shape.

Leveled regimes are:

- `served` (primary): replays the production `search_text` option shape
  literally: `columns=[content]`, `offset=0`, `page_only=true`, exact document
  identity tie fields `[udi,path,ipath_vec,content_version]`, unresolved hits,
  exact count, and no terms or positions. The bounded address page is then
  passed to `hydrate_page/3` before snippet text is fetched.
- `bare` (secondary): the same query without a tie field.
- `dirty_tail`: re-derives and re-puts one document between timed queries,
  honestly exercising FTS-2's mutable delta tail.

Every timed case returns a final page with a nonempty snippet for every row.
Leveled's phase split uses only public calls: `search` performs match, rank,
page, exact document-grouped count, and address retrieval with the literal
production map; `hydrate_page` resolves exactly those page addresses, and
`text_blocks_batch` fetches their snippet text. Non-primary diagnostic regimes
retain `posting_read` hydration.
SQLite reports its query-without-snippet wall and its complete grouped
`snippet()` wall; its hydration column is their nonnegative difference.
SQLite's complete wall is its gate wall, while Leveled's gate wall is the sum
of its directly measured phases.

Exact totals, sorted full-set SHA-256 values, returned counts, snippet
presence, and snippet-to-hit content verification are cross-checked for each
supported target pair. The primary served ratio must be at most 2x the engine
named by `--gate-target` (default `sqlite`); other names in `--targets` are
reported side by side without gating. Breaches or equivalence failures make
the command exit nonzero. Use `--report-only` only when intentionally
collecting a failing diagnostic.

From the repository root:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 bench/bench.py fts \
  --corpus /home/dvse/bench3/fts_arena_20260801/corpus/full.jsonl \
  --output-dir /tmp/leveled-bench/fts \
  --rungs 25,50,100 --ranks none,bm25 \
  --regimes served,bare,dirty_tail --limits 20,200 \
  --targets sqlite --gate-target sqlite \
  --query-groups grammar,grid --warmups 3 --runs 7
```

Add `standing` to `--query-groups` when a separately labelled copy of the
17-query public serving set is useful. It overlaps the grid by design.

### `index`

This is the retained index-operations comparison. Leveled writes current
secondary-index specs and executes `book_indexfold`; SQLite writes an indexed
B-tree column and executes the same integer range.

```bash
PYTHONDONTWRITEBYTECODE=1 python3 bench/bench.py index \
  --output-dir /tmp/leveled-bench/index --count 10000
```

### `heads`

This is the batched-head successor to the old mget benchmark. Leveled measures
`book_headonly_many` directly; SQLite measures the equivalent prepared point
reads at the same batch sizes.

```bash
PYTHONDONTWRITEBYTECODE=1 python3 bench/bench.py heads \
  --output-dir /tmp/leveled-bench/heads --count 10000 \
  --batches 1,20,200,1000
```

### `compression`

This replaces the old Erlang module plus shell matrix. It reports Leveled
`none` and `native` storage side by side with SQLite's uncompressed baseline.

```bash
PYTHONDONTWRITEBYTECODE=1 python3 bench/bench.py compression \
  --output-dir /tmp/leveled-bench/compression --count 10000 \
  --bytes 1024 --methods none,native
```

## Stable report format

Every `<subcommand>.json` has this top-level contract:

```json
{
  "schema_version": 1,
  "subcommand": "fts",
  "generated_at": "2026-08-02T00:00:00+00:00",
  "configuration": {},
  "rows": []
}
```

`schema_version` changes if a field is removed or its meaning changes.
Durations are integer microseconds (`*_us`), sizes are bytes (`*_bytes`), and
FTS regime/phase labels are the names documented above. FTS additionally has
`comparisons`, one row per Leveled/target pair, and `gate`, the supported
subset for the configured gate target. Comparison rows contain `supported`,
`equivalent`, `ratio`, `gated`, and `pass`; unsupported productions are marked
and never silently skipped. Raw `driver_runs` retain ingest/consolidation/
open/storage attribution.

The Markdown artifact renders the same normalized rows. Its FTS columns are:

| Column | Meaning |
|---|---|
| `rung` | Percent of corpus rows ingested (25, 50, or 100) |
| `engine` | `leveled` or `sqlite` |
| `config` | Explicit store configuration (`live` for the governing Leveled run) |
| `rank` | `none` or `bm25` |
| `regime` | `served`, `bare`, or `dirty_tail` |
| `limit` | Requested page size |
| `total` | Exact grouped document count |
| `returned` / `snippets` | Page rows and nonempty snippet rows |
| `retrieval us` | Match/rank/page/candidate-key phase |
| `hydration us` | Resolved row and snippet phase |
| `combined us` | Primary with-snippets wall |
| `marginal 20->200 us/row` | `(combined@200 - combined@20) / extra returned rows` |

A worked FTS row looks like:

```text
| 100 | leveled | bm25 | served | 20 | grid:invoice | 359 | 20 | 20 | 920 | 185 | 1105 | 0.41 |
```

The other subcommands use the identical envelope and put their compact driver
objects in `rows`. For example:

```json
[
  {"engine":"sqlite","subcommand":"index","count":10000,"query_us":42},
  {"engine":"leveled","subcommand":"index","count":10000,"query_us":310}
]
```

Driver CLIs are deliberately uniform (`<driver> <subcommand> --flag value`)
and each emits exactly one JSON object on standard output. Direct driver use
is supported for diagnosis, but only `bench.py` owns normalized reports and
gate decisions.

## Comparison-driver contract

The FTS orchestrator is target-pluggable. `COMPARISON_DRIVERS` in `bench.py`
is the registry; adding a target consists of one driver source/binary and one
registry entry containing its generic compile argv, artifact name, store flag,
and store suffix. Rung preparation, query catalogs, invocation, normalization,
pairwise equivalence, unsupported-row handling, rendering, and gate selection
do not contain target-specific branches.

A comparison binary accepts this exact core invocation (its registry-selected
store flag may be `--db`, `--root`, or another private path flag):

```text
<binary> fts <store-flag> PATH --tsv CORPUS.tsv --queries QUERIES.tsv \
  --rank none|bm25 --limit N --runs N --warmups N
```

`CORPUS.tsv` has four tab-separated ASCII fields with no header:

```text
key_b64  udi_b64  decimal_content_version  utf8_content_b64
```

`QUERIES.tsv` also has four tab-separated ASCII fields with no header:

```text
label  group  leveled_query_b64  comparison_query_b64
```

Labels and groups are UTF-8-safe identifiers without tabs. Query and corpus
payloads use standard padded Base64. A comparison driver consumes query column
4 and owns translation into its engine grammar; Leveled consumes column 3.
The catalog normalizes separator concatenation to explicit `+`, NEAR spellings
to the comparison function form, and phrase-last-prefix spacing. Identities
are decoded `udi` bytes, sorted bytewise and newline-framed before SHA-256.
Totals are distinct grouped identities, not raw chunk rows.

The driver writes diagnostic text only to stderr and exactly one compact JSON
object, terminated by a newline, to stdout. Its envelope is:

```json
{
  "schema_version": 1,
  "engine": "target-registry-name",
  "subcommand": "fts",
  "rank": "none",
  "regime": "served",
  "limit": 20,
  "documents": 20395,
  "text_bytes": 123456,
  "ingest": {},
  "store_bytes": 123456,
  "cases": []
}
```

Each case supplies `label`, `group`, `query`, `supported`, `limit`, `total`,
`returned`, `snippet_rows`, `snippet_content_verified`, `result_sha256`,
`retrieval_us`, `hydration_us`, and `combined_us`. `supported` may be omitted
only when true. An unsupported production is still emitted in catalog order
with `supported=false`; timing/count/hash fields may be null. SQLite supports
all current catalog rows. A future Xapian driver must mark unsupported any
production it cannot express exactly—particularly FTS5 column-set or negative-
column forms if its field processor has no equivalent—instead of approximating
or dropping it.

Snippet semantics are per returned grouped identity, on every query. A
nonempty rendered snippet increments `snippet_rows`; it increments
`snippet_content_verified` only when obtained from the matched content for
that identity. Markup and ellipsis are engine-owned and are not compared
byte-for-byte. `retrieval_us` is the median match/rank/page wall without
snippet materialization. `hydration_us` is the separately measured resolved-
row/snippet phase, or the nonnegative difference between complete and
retrieval walls when only complete-query timing is exposed. `combined_us` is
the median complete with-snippets wall and the sole gate wall. Durations are
integer microseconds.

Ranks, limits, labels, rung percentages, and target names are joined exactly.
For each supported pair, equivalence requires identical exact total and
full-set SHA-256 plus `snippet_rows == snippet_content_verified == returned`
on both engines. Ratio is `leveled.combined_us / target.combined_us`. Only
`--gate-target` receives `pass`; other targets remain side-by-side comparison
rows. The gate target must be present in `--targets`.

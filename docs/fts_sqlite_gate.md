# Leveled FTS vs SQLite FTS5 — equivalence + 5x latency gate (2026-07-09)

A dated benchmark run comparing Leveled's native full-text search against
SQLite FTS5 over a shared ~5.3 GB English-Wikipedia corpus, with a hard
performance gate: **Leveled median query latency must be within 5x of SQLite,
per query shape, per rank mode, per scale.** Result equivalence (unranked
match sets and ranked BM25 order + scores) is checked alongside.

This extends the existing `bench/` harness (`fts_bench.py`,
`sqlite_fts_bench.c`, `leveled_fts_bench.erl`); see [FTS.md](FTS.md) for the
engine design. It records what changed, how it was measured, and the verdict.
No production code was changed — only the two engine benches (a new `--rank`
mode and, for Leveled, the `--uncached` / `--amortized` measurement regimes)
and new gate/ladder scripts.

## Provenance

| Component | Value |
|---|---|
| Leveled repo HEAD | `8b80c1f` (working tree has the bench-only edits below) |
| SQLite | 3.54.0 (source `d688d15`), amalgamation built with `-DSQLITE_ENABLE_FTS5`, FTS5 confirmed |
| Erlang/OTP | OTP 29, ERTS 17.0.2 |
| Corpus | `enwiki-latest-pages-articles{9,10,11}` (contiguous page-id ranges p2936261–p6899366) |
| Corpus size | 502,378 docs, 5,329,432,231 bytes of title+body text (TSV 7.1 GB) |
| Host | Apple Silicon, macOS 24.6 |

Corpus is deterministic: `fts_bench.py prepare` streams the `.bz2` dumps in
fixed order, keeps `ns0` non-redirect pages, assigns ordinal keys
`doc-%012d`, and base64-encodes title/body into a TSV. Ladder rungs are
prefix slices of that TSV by cumulative text bytes, so every engine and rank
mode at a given scale indexes the identical first-N documents.

## Tokenizer / prefix parity (the equivalence precondition)

Both engines are configured with the byte-for-byte equivalent tokenizer and
prefix settings, and the small-scale differential contracts in
`test/end_to_end/fts_SUITE.erl` prove those settings produce identical
tokenization and BM25:

| Setting | SQLite FTS5 (DDL) | Leveled (`fts_indexes`) | Contract evidence |
|---|---|---|---|
| tokenizer | `tokenize='unicode61 …'` | `tokenizer => unicode61` (default) | `unicode61_supported_parity_corpus_contract` |
| diacritics | `remove_diacritics 2` | `remove_diacritics => 2` | `unicode61_supported_parity_corpus_contract`, `sqlite_supported_ast_differential_contract` |
| prefixes | `prefix='5 11'` | `prefixes => [5, 11]` | prefix range-scan == `prefix='…'` (FTS.md) |
| columns | `key UNINDEXED, title, body` | `columns => [title(path [1]), body(path [2])]` | — |
| BM25 | `bm25(docs)` via `ORDER BY rank` | `rank => bm25` | `bm25_rank_sqlite_differential_contract` |

The FTS5 DDL used here is exactly
`CREATE VIRTUAL TABLE docs USING fts5(key UNINDEXED, title, body,
tokenize='unicode61 remove_diacritics 2', prefix='5 11')`
(`sqlite_fts_bench.c`), matching `leveled_fts_bench.erl`'s index schema. The
`remove_diacritics 2` value (full Unicode diacritic folding, not just Latin-1)
is the one the parity-corpus contract proves equivalent to Leveled's
`remove_diacritics => 2`.

**At scale this parity holds exactly for match sets, not for the raw token
stream.** Across every query and every scale, the unranked full-match-set hash
(`full_keys_sha256`) is byte-identical between engines (see Equivalence
below). But the corpus-wide token streams are *not* identical: the two
tokenizers disagree on CJK segments embedded in English articles (0.025% of
tokens on the measured slice; dissected below). The tested query set is
Latin-script, so those tokens are never queried and match parity is unaffected
— a query for an affected CJK token *would* return different match sets.

## What was added (bench-only)

- `sqlite_fts_bench.c`: `--rank none|bm25`. `bm25` runs
  `SELECT key, rank FROM docs WHERE docs MATCH ? ORDER BY rank, key LIMIT ?`
  and emits an extra `query_scores` row (JSON array of `%.17g` scores). The
  existing result-TSV schema is unchanged; the extra row is ignored by
  `fts_bench.py`'s parsers.
- `leveled_fts_bench.erl`: `--rank none|bm25` (threads `rank => bm25` into
  `book_ftssearch`, emits ordered keys + a `query_scores` row of BM25 scores),
  and the `--uncached` / `--amortized` regimes (see Methodology).
- `bench/check_gate.py`: the gate — classifies query shapes, computes
  per-`(scale, shape, rank)` `leveled_median / sqlite_median`, enforces the 5x
  rule, and checks equivalence. Exits nonzero on any latency breach or unranked
  mismatch. Build time and store size are reported, never gated.
- `bench/run_ladder.py`: drives the scale ladder (slice → sqlite → leveled ×
  rank × regime → cleanup).
- `bench/fts_gate_queries.txt`: the query set (below).

## Query set and shapes

13 queries in the grammar the differential contracts prove identical between
engines (plain terms with implicit AND, `OR`/`NOT`, `"phrases"`, `col:` filters,
`word*` prefixes, `NEAR`). Shapes are classified structurally by `check_gate.py`:

`history`, `london`, `research`, `istanbul` (terms of decreasing frequency);
`"new york"`, `"united states"` (phrase); `title:history` (column);
`science AND research` (bool_and); `war OR peace` (bool_or);
`science NOT history` (bool_not); `comput*` (prefix);
`NEAR(history culture, 10)` (near); `zzmissingfts0` (nomatch).

## Methodology — three measurement regimes

SQLite has no result cache: every `MATCH` re-executes, so a warm-page repeated
query measures real query work. **Leveled keeps per-write-sequence ETS caches**
(FTS.md): a *result* cache (whole query answers), a *batch-list* cache (the
directory-row enumeration a term lookup starts from), and a *corpus-stats*
cache (the O(N) doc-marker fold BM25 needs). All three are keyed by the FTS
write sequence, which changes on every write. Under the harness's
warmup-then-repeat discipline, Leveled serves every timed run from the result
cache in ~2–6 µs regardless of corpus size — which measures the cache, not the
query engine. Three regimes are therefore reported, bracketing production:

- **warm** — the harness default (warmup then repeat the identical query).
  Leveled hits its result cache; this is its steady-state *repeated*-query
  latency, and it is trivially within 5x (thousands of times faster). Best
  case: an exactly-repeated query between writes.
- **uncached** (`--uncached`) — before **each timed run** Leveled re-puts one
  existing document (idempotent for results: same key, same content, same
  token count → identical match set, count, and BM25 scores), advancing the
  FTS write sequence and invalidating all three per-sequence caches. Each
  timed query then pays snapshot setup, the batch-list re-enumeration, the
  corpus-stats fold (ranked), and full posting work. **Worst case: a store
  that takes a write before every query** — every per-write cost lands on the
  query that follows it.
- **amortized** (`--amortized`) — before each timed run: bump the sequence
  once, then run an untimed *absorber* query (a distinct nomatch term) at the
  new sequence, which re-derives the per-sequence batch-list and (when ranked)
  corpus-stats caches. The timed query is then **stats-warm but
  result-cache-cold**: the steady-state cost of a *novel* query arriving
  between writes. This is the fair "production" number for read-mostly
  workloads; the per-write costs are paid once per write, not per query.

The immutable per-batch page/directory caches stay warm in all regimes,
mirroring SQLite's warm OS page cache. All regimes preserve equivalence
(verified: uncached/amortized leveled return identical keys/scores to warm).
Determinism: fixed corpus, fixed query file, no RNG, warmup then N measured
runs, medians compared. Runs/warmup per rung: s100m 25/5, s1g 15/3, full 7/2
(fewer at full scale where each uncached query is expensive).

**The 5x gate is evaluated on the uncached regime** (the most adversarial
defensible reading of "query latency"); the amortized tables show how much of
each breach is per-write fixed cost rather than per-query engine work.

## Results

### Build and store size (reported, not gated)

| scale | docs | text | build: sqlite | build: leveled | ratio | size: sqlite | size: leveled | ratio |
|---|---|---|---|---|---|---|---|---|
| s100m | 9,638 | 105 MB | 3.2 s | 7.1–7.6 s | 2.2–2.4x | 190 MB | 329 MB | 1.73x |
| s1g | 98,355 | 1.07 GB | 39.2 s | 77.6–77.9 s | ~2.0x | 1.90 GB | 2.33 GB | 1.23x |
| full | 502,378 | 5.33 GB | 221–222 s | 405–425 s | 1.8–1.9x | 9.36 GB | 11.62 GB | 1.24x |

Leveled's build is a consistent ~2x SQLite's and its store ~1.2x at scale
(the store also contains the canonical objects; SQLite's DB here contains the
source text inside the FTS table as well).

### THE GATE — uncached regime (write-before-every-query worst case)

Worst ratio per shape (`leveled_median / sqlite_median`; threshold 5.0x;
**BREACH** marks any query of the shape over 5x):

| shape | s100m none | s100m bm25 | s1g none | s1g bm25 | full none | full bm25 |
|---|---|---|---|---|---|---|
| term | 30.6x B | 36.4x B | 39.8x B | 52.6x B | 43.9x B | 52.0x B |
| phrase | 3.7x | 4.9x | 4.4x | 6.3x B | 12.8x B | 12.0x B |
| prefix | 13.5x B | 15.4x B | 7.6x B | 9.8x B | 44.1x B | 53.5x B |
| bool_and | 12.7x B | 15.2x B | 8.1x B | 12.2x B | 43.7x B | 56.4x B |
| bool_or | 6.0x B | 5.7x B | 4.0x | 4.1x | 23.6x B | 28.1x B |
| bool_not | 20.1x B | 27.2x B | 15.8x B | 34.0x B | 75.1x B | 87.4x B |
| column | 53.3x B | 46.3x B | 56.0x B | 73.2x B | 63.2x B | 75.2x B |
| near | 77.9x B | 92.2x B | 56.2x B | 56.0x B | 154.3x B | 181.7x B |
| nomatch | 102.9x B | 214.7x B | 548.2x B | 1275.3x B | 51,969x B | 77,431x B |

**Uncached gate verdict: FAILED — 50 of 78 cells breach.** The worst
*absolute* numbers at full scale: `war OR peace` bm25 11.5 s (sqlite 0.41 s),
`nomatch` bm25 5.1 s (sqlite 66 µs). But note the shape of the failure: the
hottest term (`history`, 210k hits) is 2.8–2.9x — *inside* the gate at every
scale — while the emptiest query breaches by four orders of magnitude. The
breaches grow as selectivity shrinks: this is a fixed-cost profile, not a
posting-traversal profile (dissected below).

Within-gate cells (uncached): hot terms (`history` 1.6–2.9x, `london`
2.9–4.2x, `research` 3.0–5.0x), phrases at s100m/s1g (3.2–4.9x), and
`war OR peace` at s1g (4.0–4.1x).

### Amortized regime (novel query between writes — the production number)

Same shapes, leveled amortized vs the same SQLite baselines:

Worst ratio per shape (same SQLite baselines; threshold 5.0x):

| shape | s100m none | s100m bm25 | s1g none | s1g bm25 | full none | full bm25 |
|---|---|---|---|---|---|---|
| term | 27.7x B | 31.4x B | 37.8x B | 33.6x B | 42.4x B | 38.4x B |
| phrase | 3.9x | 4.8x | 5.1x B | 5.9x B | 25.7x B | 31.7x B |
| prefix | 13.5x B | 14.6x B | 8.0x B | 6.8x B | 53.2x B | 65.1x B |
| bool_and | 12.1x B | 14.1x B | 7.7x B | 9.7x B | 53.0x B | 67.7x B |
| bool_or | 5.6x B | 5.4x B | 3.9x | 3.2x | 31.1x B | 37.9x B |
| bool_not | 20.9x B | 26.9x B | 15.5x B | 29.9x B | 92.2x B | 101.6x B |
| column | 31.7x B | 53.6x B | 33.2x B | 41.3x B | 52.9x B | 53.9x B |
| near | 72.7x B | 93.6x B | 73.3x B | 48.5x B | 195.8x B | 222.4x B |
| nomatch | 79.5x B | 137.4x B | 295.5x B | 327.6x B | 1,874x B | 1,967x B |

**Amortized gate verdict: FAILED — 52 of 78 cells breach.** Amortization
collapses the *fixed floors* (full-scale nomatch: 5.1 s → 130 ms; `istanbul`
946 → 699 ms; `history` 2.16 → 1.84 s = **2.4x, in-gate**), but two things
keep the regime out of gate: (a) the per-novel-query snapshot floor itself
(129 ms at full scale vs SQLite's µs–ms selective queries), and (b) rare-term
per-batch posting scatter (below), which amortization cannot touch.

**Observed anomaly, reported as-is:** at full scale, several multi-leg
queries measured *slower* amortized than uncached (`war OR peace` bm25
11.5 → 15.6 s, `"united states"` bm25 6.8 → 16.5 s), which should be
impossible from the cache model (amortized does strictly less work). Single
terms improved as predicted at every scale, and the same anomaly appeared
mildly for NEAR at s1g (301 → 393 ms unranked). Suspected mechanisms — worker
heap/GC pressure from the untimed absorber's 502k-marker fold garbage landing
in the timed window, and/or store-layout variance (the amortized full store
was built in a separate run, partly overlapping unrelated I/O) — were not
isolated; treat per-query amortized numbers at full scale as ±2x, and rely on
the nomatch-based decomposition (clean, reproduced across scales) for the
fixed-cost story.

### Warm regime (repeated query between writes)

Every repeated query is served from Leveled's result cache in **2–6 µs flat**,
independent of scale, hit count, or rank mode — thousands of times faster
than SQLite re-executing the same MATCH. All 52 measured warm cells pass the
gate with ratios of 0.00x–0.31x. (SQLite has no equivalent cache; this
comparison is real but measures the cache, not the engine.)

## Equivalence

Comparison over all 78 `(scale × query × rank)` cells (`check_gate.py`):

- **Unranked (`rank=none`): EXACT in 38 of 39 cells.** Identical top-100 key
  lists, identical total match counts, identical full-match-set hashes. The
  single exception is `NEAR(history culture, 10)` at full scale: leveled
  matches 5,479 documents, SQLite 5,478 — dissected below; it is a
  position-drift consequence of the CJK tokenizer divergence, not a NEAR
  logic bug.

- **Ranked (`rank=bm25`): order and scores DIVERGE on real text**, beyond the
  1e-6 relative tolerance (36 of 39 ranked cells; only the three zero-hit
  nomatch rows are exact). This is a genuine large-corpus finding — the
  small-scale differential contract and an 8-document smoke both match to
  ~1e-16, but real articles expose the gap.

### Dissection — root cause 1: CJK tokenizer drift → BM25 statistics drift

The BM25 *formula* is identical (`leveled_fts.erl` `bm25_score/5`, lines
1908–1938, vs FTS5's `bm25()`): k1=1.2, b=0.75,
`idf = ln((N−n+0.5)/(n+0.5))` clamped to 1e-6, same length-normalized term
weight. The divergence is in the **statistics** fed to it. Proven on the
879-doc dissection slice (8 MB of the corpus) with `fts5vocab`
instance tables on the SQLite side and leveled's own `derive_doc` token
counter (compiled `+export_all`) on the other:

1. **Hand-computing the FTS5 formula from vocab-extracted stats (tf, df, dl,
   avgdl) reproduces SQLite's reported ranks to 10+ digits** — formula and
   stats extraction are validated.
2. **Leveled's implied length ratio `L' = dl'/avgdl'` (inverted from its
   reported scores) is exactly `1.00025178 × L_sqlite` for *every* doc across
   multiple terms** — a constant, meaning pure `avgdl` drift for docs whose
   own `dl` agrees.
3. **The drift is real token-count divergence**: leveled's total token count
   is 1,171,655 vs FTS5's 1,171,950 — delta **295 tokens (0.025%)**, and
   1,171,950 / 1,171,655 = 1.00025178 exactly, closing the loop.
4. **The 295 tokens live in 22 of 879 docs** (per-doc deltas −4 … +152),
   every one containing CJK text. Token-stream diffs identify three concrete
   behaviors: FTS5 keeps precomposed Hangul tokens (`광운대`, `광운대학교`,
   …) where leveled emits decomposed Jamo (`가` as U+1100 U+1161); FTS5
   tokenizes Han-ideograph runs (`中國輕工業出版社`) where leveled treats Han
   ideographs as separators and drops them; and leveled merges combining
   voicing marks with adjacent digits (`ロボカップ2017`) where FTS5 splits.
   Latin-script text tokenizes identically — which is why match parity holds
   on the Latin query set.

Downstream sizes: docs with agreeing `dl` shift uniformly by ~2.5e-4
(single-term/phrase worst cases); the 22 CJK docs shift by up to ~2.4e-2 —
the worst non-NEAR divergences at the dissection scale (`comput*` 2.4e-2 and
`science AND research` 2.0e-2) are **both the same Hangul-heavy document**.

Ruled out by experiment: **batching** (identical divergence at batch=200,
5 batches, and batch=2000, 1 batch, over the same docs) and **emission
precision** (both sides emit 17 significant digits; diffs are 1e-4…1).

### Dissection — root cause 2: NEAR scoring semantics (the 1.045 case)

The worst ranked divergence in the whole run — rel score diff **1.045** — is
`NEAR(history culture, 10)` [bm25] at s100m. Mechanism, proven exactly on the
dissection slice (6 NEAR-matched docs):

- **Leveled** scores each NEAR member with **all** its instances in the doc:
  `scoring_phrases` flattens the NEAR group into independent term leaves
  (`leveled_fts.erl:1880–1881`) and `leaf_tf` counts unconstrained
  `term_positions`. Reconstructing scores with (all-instance tf, unfiltered
  member df, leveled stats) reproduces leveled's reported ranks on **6/6**
  docs.
- **FTS5** scores each member with only its **NEAR-satisfying** instances
  (the position lists are trimmed by the NEAR constraint before
  `xInstCount`), while `idf` still uses the unfiltered member df.
  Reconstructing with (NEAR-filtered tf, unfiltered df, FTS5 stats)
  reproduces SQLite's ranks on **6/6** docs, and pins FTS5's window for
  single-token two-phrase groups at `|p−q| ≤ N+1` (a doc with an exactly-11
  gap is matched and counted; 12 is not).
- Example magnitude: a doc with 9 `history` + 3 `culture` instances of which
  only 1+1 are within the window scores tf=(9,3) under leveled vs tf=(1,1)
  under FTS5 — a 66% score difference on that doc.

### Dissection — the single unranked mismatch (NEAR at full scale)

At full scale `NEAR(history culture, 10)` matches 5,479 docs under leveled vs
5,478 under SQLite. Exhaustive boundary accounting over the full corpus
(via `fts5vocab` positions):

- 34,064 docs contain both terms in the same column; 5,301 have a SQLite
  min-gap in (11, 60] (could flip in only), and 692 have a min-gap ≤ 11
  (could flip out). Recomputing every candidate's min-gap under leveled's
  tokenizer: **exactly one flips in, zero flip out** — 5,478 + 1 − 0 = 5,479.
- The flipped doc is `doc-000000472949` (*Gongfu tea*). Between
  `culture`@938 and `history`@952, FTS5's token stream contains a Chinese
  citation — `南強`, `烏龍茶`, `中國輕工業出版社` — as **3 tokens** (gap 14,
  outside the ≤ 11 window). Leveled's stream contains the identical
  surrounding tokens with the three Han-script tokens **absent** (gap 11,
  inside the window): leveled's unicode61 classifies Han ideographs as
  separators where FTS5's treats them as token characters.

So the one unranked mismatch is not a NEAR-logic bug: both engines apply the
same window to their own position streams, and the streams differ by root
cause 1 (CJK tokenization). Every non-NEAR query shape is position-independent
at match time, which is why only NEAR exposes the drift unranked.

### Are the order differences ties or real re-ranking?

For every ranked `top_keys_differ` cell at every scale:

- **All non-NEAR cases are pure reorderings of the same 100-key set**, with
  maximum position shift 1–5 — adjacent near-ties flipped by the ~1e-4 score
  drift — plus two 2-key membership swaps at the exact 100-cutoff boundary
  (`war OR peace` at s100m, `title:history` at full).
- **NEAR is genuine re-ranking**: the top-100 sets differ by 126 keys at s1g
  and 158 keys at full scale (symmetric difference), with position shifts up
  to ~90 — the different tf semantics produce a materially different ranking,
  not tie noise.

**Verdict:** Leveled's BM25 is FTS5-parity for the formula, and for scores
wherever the token statistics agree (all-ASCII/Latin corpora — hence the
small-scale contracts pass at ~1e-16). On corpora containing CJK segments the
tokenizer divergence shifts `avgdl`/`dl` and, through position drift, can even
move a NEAR match across the window boundary. NEAR *scoring* is semantically
different from FTS5 regardless of script. If FTS5-exact ranked parity is a
goal, the two concrete work items are (a) unicode61 Hangul/voicing-mark
tokenization parity, and (b) NEAR-constrained instance counts in
`scoring_phrases`/`leaf_tf`.

## Cost dissection — fixed vs per-posting cost

The `nomatch` query (a term with no postings) isolates the fixed costs
exactly: it pays everything *except* posting traversal and scoring. Comparing
regimes then splits the fixed costs into per-novel-query and per-write parts
(all medians, ms):

| fixed component | measured as | s100m (9.6k docs) | s1g (98k docs) | full (502k docs) |
|---|---|---|---|---|
| snapshot + parse (per **novel query**) | amortized `nomatch` [none] | 3.10 | 13.30 | 129.3 |
| batch-list rediscovery (per **write**) | uncached − amortized `nomatch` [none] | 0.91 | 11.37 | **3,456.6** |
| ranked residual (per novel **ranked** query) | amortized `nomatch` [bm25 − none] | 2.26 | 6.03 | 0.5 |
| **corpus-stats fold** (per **write**, ranked) | uncached − amortized `nomatch` [bm25], minus batch-list | 2.10 | 44.54 | **1,524.1** |

At full scale the write-adjacent fixed costs dominate everything selective: a
single write invalidates the per-sequence caches, and the next query pays
~3.5 s re-enumerating 2,512 batch directories plus (ranked) ~1.5 s re-folding
502k doc markers on a store too large for comfortable page-cache residency.
Amortized, the same nomatch query costs 129 ms — still ~2,000x SQLite's 66 µs
(the per-novel-query snapshot floor itself grows with store size: 3.1 → 13.3
→ 129 ms), but 27x below the uncached floor. The corpus-stats fold measured
two independent ways agrees to 0.5 ms (uncached Δ 1,524.6 vs
amortized-decomposed 1,524.1).

### The stats-fold cost curve (the O(N) marker fold BM25 depends on)

Upper-bound curve straight from the uncached deltas
(`nomatch` bm25 − none — the fold plus the small ranked residual):

| scale | docs | uncached Δ (ms) | per-doc |
|---|---|---|---|
| s100m | 9,638 | 4.36 | 0.45 µs |
| s1g | 98,355 | 50.57 | 0.51 µs |
| full | 502,378 | 1,524.58 | **3.03 µs** |

Roughly linear to ~100k docs, then ~6x superlinear per-doc at full scale — the
11.6 GB store no longer fits the OS page cache and the marker fold becomes
I/O-bound. Every ranked query after a write pays this fold once (then it is
cached per write sequence); at full scale that is ~1.5 s of the uncached
ranked floor. This is the direct measurement motivating a windowed/async
corpus-stats maintenance lever: BM25 needs only `{N, TotalLen}`, which could
be maintained incrementally per write instead of re-folded.

The unranked full-scale floor is similar in kind: uncached `nomatch` [none] is
3,585.9 ms at full scale (vs 24.7 ms at s1g) — snapshot setup plus the
per-write batch-list rediscovery over 2,512 batch-directory rows on a
cold-page store.

### Where ranked query time goes (hot vs selective)

From the s1g amortized (stats-warm, result-cold) bm25 medians vs uncached:

| query | sqlite | leveled uncached | leveled amortized | amortized ratio |
|---|---|---|---|---|
| `history` (41.7k hits) | 131.3 | 344.8 | 287.6 | 2.2x |
| `war OR peace` (19.1k) | 72.6 | 296.4 | 232.9 | 3.2x |
| `comput*` (5.5k) | 22.8 | 223.2 | 154.2 | 6.8x |
| `istanbul` (686) | 3.0 | 160.3 | 102.4 | 33.6x |
| `zzmissingfts0` (0) | 0.06 | 75.2 | 19.3 | 328x |

Reading, in three parts:

1. **BM25 scoring is nearly free** — ranked ≈ unranked + the fixed ranked
   costs, at every scale and selectivity (e.g. `istanbul` s1g amortized:
   101.1 ms unranked vs 102.4 ms ranked).
2. **Hot-term posting traversal is competitive**: amortized ratios 2.2–3.2x
   on the hottest terms — inside the gate. Dense postings amortize the
   ~8 KB per-batch pages.
3. **Rare/mid-selectivity terms pay per-batch scatter**: `istanbul` (686
   hits) costs ~100 ms amortized ≈ 0.15 ms/hit — its hits spread across
   hundreds of write batches, and the engine point-reads roughly one posting
   page per batch containing the term, where FTS5 reads one merged per-term
   doclist. Together with the per-novel-query snapshot floor (13 ms at s1g)
   and the per-write folds, this is what breaches the gate on selective
   queries: partly amortizable fixed cost, partly the per-batch posting
   layout (no segment merge — the documented FTS.md tradeoff).

## Validity caveats

- **Regime interpretation is load-bearing.** The gate is evaluated on the
  uncached (write-before-every-query) regime; the warm regime trivially
  passes; the amortized regime is the production-shaped number. Presenting
  only one of these would misstate the engine either way.
- **Uncached over-counts amortizable cost**: the batch-list and corpus-stats
  folds are per-write, and the snapshot is per-novel-query; a read-heavy
  steady state pays them far less often than the uncached regime charges.
  The decomposition tables separate these so the numbers can be read either
  way. Conversely the amortized regime under-counts a write-heavy store.
- **Unranked equivalence is exact on the tested Latin-script query set**;
  the corpus-wide token streams differ on CJK segments (0.025% of tokens),
  and queries against affected tokens would differ — including, at full
  scale, one NEAR match flipped by position drift.
- **Ranked BM25 scores are not bit-exact** on real text (dissected above);
  the unranked path — the one production `search_content` relies on — is
  exact modulo the CJK caveat.
- **Queries run against freshly built stores** (close + reopen, but no settle
  time), on an 11.6 GB store whose SST files exceed comfortable OS page-cache
  residency at full scale — the superlinear full-scale fixed costs partly
  reflect cold-page I/O, which also matches a realistic cold-ish store.
- **Fewer measured runs at full scale** (7 runs / 2 warmups vs 25/5 at
  s100m); medians are stable but tails are under-sampled.
- The full-scale *amortized* leveled store build overlapped an unrelated
  SQLite rebuild (dissection tooling) for part of its load phase; load times
  are therefore cited from the uncontended uncached runs. Timed query
  sections did not overlap.
- SQLite pragmas (`journal_mode=OFF`, `synchronous=OFF`, `cache_size=-200000`)
  and Leveled (`sync_strategy=none`, native compression) are the harness
  defaults; both favor throughput and are held constant across the ladder.

## Reproducing

```bash
# 1. Corpus (once; caches to /tmp/leveled_fts_bench/docs.tsv)
python3 bench/fts_bench.py prepare --corpus-dir bench_corpus/wikipedia \
  --tsv /tmp/leveled_fts_bench/docs.tsv --meta /tmp/leveled_fts_bench/corpus_meta.json

# 2. Ladder — gate basis (uncached) and production number (amortized)
python3 bench/run_ladder.py --regime uncached --reuse-sqlite
python3 bench/run_ladder.py --regime amortized --reuse-sqlite

# 3. Gate + tables (exit nonzero on breach / unranked mismatch)
python3 bench/check_gate.py --results-dir /tmp/leveled_fts_bench/results/gate \
  --scales s100m,s1g,full --leveled-regime uncached --threshold 5.0 \
  --markdown /tmp/gate_tables.md --summary /tmp/gate_summary.json
# and the same with --leveled-regime amortized / warm for the other regimes
```

The SQLite amalgamation must exist under `--sqlite-source` (default
`/Users/dvse/repos/sqlite`); build it once with `make sqlite3.c sqlite3`
(needs tcl 8.6, e.g. Homebrew `tcl-tk`) then an FTS5 CLI with
`cc -O2 -DSQLITE_ENABLE_FTS5 -I. shell.c sqlite3.c -lz -lpthread -lm -o sqlite3`.

The equivalence dissection used `fts5vocab('docs','instance')` tables on the
SQLite side (exact per-doc `tf`/`dl`/positions) and, on the Leveled side, the
bench-identical token pipeline (`normalise_indexes` → `extract_fields` →
`build_column_terms`) invoked from throwaway modules against a
`+export_all`-compiled `leveled_fts` — no production changes.

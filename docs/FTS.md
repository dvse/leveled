# FTS — full-text search as a pure client library

Status: design authority (2026-07-12). This document and NATIVE_CAS.md
are the only fork documents beyond stock leveled. Supersedes the hooked
FTS engine design (history in git; audit evidence in audit/).

## 1. Principle: zero hooks

`leveled_fts` is a client library. It consumes ONLY the public store
surface — `book_mput`/`book_casmput` (NATIVE_CAS.md), `book_sqn`,
`book_headonly`, head folds, snapshots — and the store carries no FTS
configuration, no FTS state, and no FTS code paths. Anything the old
engine hooked into the Bookie is expressed as data:

| old engine mechanism | library expression |
|---|---|
| write-path augmentation hooks | postings are object-spec rows committed in the caller's own mput/casmput batch |
| fts_seq version clock + marker liveness | gone — per-doc rows are OVERWRITTEN on reindex; leveled's own last-write-wins is the liveness rule |
| bookie-owned shard cache, write-through advance | library-owned cache validated by per-shard EPOCH ROWS (§4) |
| consolidation handler + concurrency machinery | `book_casmput` conditioned on epoch rows (§5) |
| `fts_indexes` start option | schema is a value owned by the caller |
| forged-carrier validation | moot — postings are ordinary rows, there is no privileged channel |

Consequences: the library runs against any bookie (one per resource
group, hash-sharded fleets — topology is the caller's business, one
library instance per bookie); postings can join an ash_leveled OCC
transaction batch, giving TRANSACTIONAL index updates; and the audit's
cache-freshness and two-clock defect classes are structurally absent.

## 2. Data model

All FTS state lives in head rows (`?HEAD_TAG` object specs) under the
index's bucket. For index `I`:

- **Posting rows** — one row per (shard, doc):
  `{I, ShardId, <<"d:", DocKey>>}` → encoded postings of DocKey's
  tokens that hash into ShardId (per column: token → positions,
  occurrence count). Shard = hash class of the token space, fixed per
  index (default 256).
- **Doc manifest** — one row per doc: `{I, <<"doc">>, DocKey}` →
  `{version, current doc id, touched shard list, doc length, schema
  fingerprint}`. Needed to
  remove/update a doc's rows without re-deriving the old version, and
  it is the "indexed" fact for pipeline idempotency.
- **Doc-id rows** — one row per live doc version:
  `{I, <<"id">>, <<DocId:64>>}` → `{DocKey, Version}`. This is the
  lookup direction used at serve; a missing row means the id is retired.
  IDs are content-derived representation state, not a domain version
  clock.
- **Epoch rows** — one row per shard: `{I, ShardId, <<"epoch">>}` →
  small counter value. Rewritten in EVERY batch that touches the shard;
  its SQN is the condition consolidation's casmput commits against (§5).
- **Tail summary** — one small row per shard:
  `{I, ShardId, <<"tailsum">>}` → token bloom + doc count of the
  shard's unconsolidated tail, rewritten with every tail write. Lets a
  query decide with ONE point read whether the tail can contain its
  tokens; a selective or missing term never folds anything.
- **Token pages (consolidated)** — the at-rest read format, chosen for
  stock leveled's strengths (bloom-guarded point reads; ordered-key
  range folds; journal for big values). Format v8 page subkeys include
  the page's first and last docids. Posting payloads are docid-ordered
  ~2KB chunks: each header carries first/last docid and a conservative
  one-byte BM25 bound; the first docid is absolute at the chunk boundary
  and later docids are delta varints. The page-0 header carries document
  frequency, collection frequency, and the term bound. Exact positions
  use distinct position-plane subkey rows and are read only for
  positional evaluation or final winners; they never occupy scan-hot
  boolean rows. Position lists larger than one page entry are split into
  independently decodable bounded chunks, preserving the position-length
  safety contract without truncating true counts. Pages stay below the
  ordinary 32KB value limit. If a hot term's cross-page boundary index no
  longer fits in page 0, it spills into bounded, separately addressed rows
  committed in the same batch; the page reader reassembles those rows only
  when it sees the overflow marker. A term query is one or two point/range
  reads; a
  missing term is a bloom miss; prefix expansion is a bounded ordered
  range fold. Consolidation (§5) is the INVERTER: it folds the
  doc-major tail into token pages. Doc-major posting rows exist only in
  the unconsolidated tail — they are what makes a doc's write atomic
  and overwrite-live — and the tail masks consolidated pages for
  updated/removed docs (segment semantics). Version-stamp admission
  applies to the tail; consolidation bakes only live versions into
  pages. Readers negotiate v7 and v8; new consolidation writes v8 and a
  v7 index remains readable without in-place migration.
- **Stats row** — `{I, <<"stats">>}` → doc count and total length for
  BM25, updated in the same batches.

Liveness needs no queue or version-clock machinery: reindexing a doc overwrites its posting
rows and manifest in one batch; deleting removes them. There is no
domain sequence to allocate, stamp, or reseed — the audit's L2-F3 class
cannot be expressed.

**Doc-version stamp.** Every posting row and the manifest of one derive
batch share an 8-byte content-derived stamp (pure, idempotent). A doc id
names exactly one `(DocKey, Version)` and the same id is used in every
shard, so states read at different instants cannot assemble two versions
into one match. Re-derive removes the old id row in the same batch that
writes the new manifest, tail, and id row. Old page entries may remain
physically present until consolidation, but final point resolution drops
them because their id row is gone.

## 3. Writing

`derive(Schema, DocKey, Object)` is a pure function (tokenise, group by
shard, encode) producing the object specs above. The write integration
point-reads the doc manifest first and calls `update/4` when it exists, so
the returned batch deletes the retired id row and installs the new one.
The caller commits
them with its own `book_mput`/`book_casmput` — typically in the same
batch as the fact that owns the text (transactional plane) or, for
journal-bodied bulk objects, ordered AFTER the bulk write so the
manifest row is the authoritative "indexed" fact (NATIVE_CAS.md §2).
An update uses the manifest to compute removed shards and retirement ids.

Pure derivation gives each version a collision-resistant id that is
immediately searchable and remains its page id after maintenance. A shard
consolidation uses the ids already carried by its authoritative tail; the
epoch CAS rejects a rebuild if any document touching the shard raced the
fold. Consolidation therefore performs no per-document manifest reads, id
assignment, or id-row retirement, and there is never a corpus manifest fold
or an in-memory reverse dictionary.

Derivation is embarrassingly parallel across docs/workers; the store
sees only ordinary mput batches.

## 4. Reading: store-direct, no library caches

### Query syntax

The canonical proximity spelling is `left NEAR,n right`, where `n` is
the maximum distance from 0 through 64. For example,
`"point duty" NEAR,20 "cross claim"` keeps each quoted phrase as one
operand. `left NEAR right` uses the documented default distance of 10.

Common unambiguous variants are accepted and normalized by the query
lexer: `NEAR(left right, n)`, `NEAR(left, right, n)`, and
`left NEAR/n right`. An explicit comma-distance also makes case
variants such as `Near,3` and `near,3` unambiguous; bare lowercase
`near` stays a prose term. `AND`, `OR`, and `NOT` remain uppercase-only
operators; their lowercase spellings are ordinary search terms so prose
queries keep their meaning.

The library maintains NO caches (settled 2026-07-12): every query reads the
store directly, and the only warmth is leveled's own ledger/page caches.
Query flow per token: point-read the token's pages
(`{I, <<"t:", Token>>, PageNo}` — a missing term is a bloom miss inside
leveled, tens of µs) and point-read the token's shard `tailsum`; only
when the tail bloom admits the token, fold that shard's (small)
doc-major tail. Prefix expansion is a bounded ordered range fold over
the token-page key space plus the tailsum check. Page evaluation and BM25
ranking remain docid-keyed; doc length and counts come from the page bytes.
Only the final bounded window point-reads id rows and substitutes DocKeys.
Missing rows are skipped and later ranked candidates backfill the limit.

Correctness without cache admission:

- Reads are snapshot-consistent per read by leveled itself; there is no
  cache to admit, so the audit's stamp-trust/fill-race classes have no
  carrier.
- The tail MASKS consolidated pages per doc (segment semantics): an
  updated or removed doc's tail entry supersedes its baked page
  entries, so single-token queries never serve superseded versions.
- Multi-token assembly uses version-specific global doc ids. Different
  versions never merge across tokens, shards, or planes; a retired old id
  drops at final resolution — the legal concurrent outcome.

## 5. Consolidation

A maintenance fold (caller-scheduled, per shard): read the shard's
doc-major tail from a snapshot, merge it into token pages (the inverter),
then commit `pages + tail removals + tailsum` via `book_casmput` conditioned
only on the shard epoch. A write that raced
the fold fails the condition and the shard is retried later —
linearizable consolidation from the public CAS. Queries see either the
pre- or post-consolidation row set, both complete. Because reads are
store-direct, consolidation frequency is the read-performance knob:
the tail is the only part of a query that is not a point read.

## 6. Boundaries and parity (the audit's Law 5, built in)

- Accept ⊆ encode: the posting codec exports its format capacities
  (column count ≤ 255, token/position byte bounds); schema validation
  consumes them and every fixed-width encode site guards loudly.
- Ranking inputs are consistent: occurrence counts are persisted beside
  (capped) positions; BM25 TF and doc length come from the same counts.
- One token-boundary definition shared by the fast and unicode
  tokenizer paths; malformed UTF-8 is a token boundary;
  `remove_diacritics` modes 0/1/2 follow SQLite unicode61 semantics.
- The standing differential gate is the generated SQLite FTS5 oracle
  corpus (test/fts_sqlite_oracle_corpus.eterm — 82 cases / 267
  oracle-verified queries incl. the surprising behaviours: truncated
  4-byte sequences fusing tokens, NUL as separator, script-dependent
  diacritic folding) run as a CT suite.

## 7. Sharded / multi-bookie deployment

One library instance per bookie; postings colocate with their docs'
bookie. A partition-scoped search hits one instance (no fan-out); a
global search fans out and score-merges, aggregating per-instance stats
rows. See the ash_leveled target spec for routing and topology.

## 8. Verification gates

1. Posting codec round-trip + capacity-guard eunit; derive/query
   property tests against a naive in-memory oracle.
2. SQLite FTS5 oracle corpus CT suite green.
3. Consolidation race test: concurrent writes during consolidation
   never lose a doc (epoch condition observed to fail and retry).
4. Cache admission test: a raced cold fill is discarded (epoch
   mismatch), never installed.
5. audit/ repro dispositions recorded in audit/REVIEW.md.
6. vfs e2e search + indexing benchmarks within gate targets.

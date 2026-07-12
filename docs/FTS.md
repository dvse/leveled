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
  `{touched shard list, doc length, schema fingerprint}`. Needed to
  remove/update a doc's rows without re-deriving the old version, and
  it is the "indexed" fact for pipeline idempotency.
- **Epoch rows** — one row per shard: `{I, ShardId, <<"epoch">>}` →
  small counter value. Rewritten in EVERY batch that touches the shard;
  its SQN is the shard's version token (§4).
- **Token pages (consolidated)** — the at-rest read format, chosen for
  stock leveled's strengths (bloom-guarded point reads; ordered-key
  range folds; journal for big values): one row per (token, page),
  `{I, <<"t:", Token>>, PageNo}` → merged postings for that token slice
  (docs, true counts, capped positions), pages bounded (~8–64KB) so SST
  merges stay cheap; oversized hot tokens overflow to journal-bodied
  pages behind ledger stubs. A term query is one or two point reads; a
  missing term is a bloom miss; prefix expansion is a bounded ordered
  range fold. Consolidation (§5) is the INVERTER: it folds the
  doc-major tail into token pages. Doc-major posting rows exist only in
  the unconsolidated tail — they are what makes a doc's write atomic
  and overwrite-live — and the tail masks consolidated pages for
  updated/removed docs (segment semantics). Version-stamp admission
  applies to the tail; consolidation bakes only live versions into
  pages.
- **Stats row** — `{I, <<"stats">>}` → doc count and total length for
  BM25, updated in the same batches.

Liveness needs no machinery: reindexing a doc overwrites its posting
rows and manifest in one batch; deleting removes them. There is no
sequence to allocate, stamp, or reseed — the audit's L2-F3 class cannot
be expressed.

**Doc-version stamp.** Every posting row and the manifest of one derive
batch share an 8-byte content-derived stamp (pure, idempotent). The
search merge keys each shard's contribution by stamp and admits ONLY
the contribution matching the CURRENT manifest's stamp — so shard
states read at different instants can never assemble two document
versions into one match (cross-shard read skew), and a query concurrent
with an update degrades to a legal pre- or post-state outcome, never a
chimera. Consolidated bases carry the stamp per document.

## 3. Writing

`derive(Schema, DocKey, Object)` is a pure function (tokenise, group by
shard, encode) producing the object specs above. The caller commits
them with its own `book_mput`/`book_casmput` — typically in the same
batch as the fact that owns the text (transactional plane) or, for
journal-bodied bulk objects, ordered AFTER the bulk write so the
manifest row is the authoritative "indexed" fact (NATIVE_CAS.md §2).
An update first reads the doc manifest to compute removed shards.

Derivation is embarrassingly parallel across docs/workers; the store
sees only ordinary mput batches.

## 4. Reading and the cache admission law

Query flow: parse → per-shard reads → merge → BM25 rank (scores use the
TRUE occurrence counts persisted in posting rows — the positions cap
never feeds ranking).

A per-shard read: `book_sqn` of the shard's epoch row (one cheap head
read) → if a cached shard state carries the same epoch SQN, serve it;
otherwise fold the shard's rows (base + per-doc postings) from a
snapshot, then re-read the epoch SQN and install the rebuilt state ONLY
if it is unchanged (discard on mismatch — correctness over warmth).

The law: no cache entry is served or installed without a token equal to
the CURRENT epoch-row SQN, and the epoch row is bumped atomically with
every write it describes. Stamp-trust, fill races, and repair
invalidation (audit L5-F2/F3/F4) are unrepresentable. The same rule
keys the query/result cache.

## 5. Consolidation

A maintenance fold (caller-scheduled, per shard): read the shard's
per-doc rows from a snapshot, merge into a base row, then commit
`base' + removals of merged per-doc rows` via `book_casmput` CONDITIONED
on `{epoch row, {sqn, ObservedSQN}}`. A write that raced the fold fails
the condition and the shard is retried later — linearizable
consolidation from the public CAS, with no special concurrency
machinery. Queries see either the pre- or post-consolidation row set,
both complete.

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

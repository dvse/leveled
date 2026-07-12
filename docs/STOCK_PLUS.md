# STOCK-PLUS — engine specification

Status: DESIGN AUTHORITY (2026-07-12). Supersedes SQN_ENGINE_REDESIGN.md
and the write-path portions of TARGET_API.md. Implemented forward as new
commits on the current HEAD — not a history revert.

## Verdict this spec encodes

The 5-layer correctness audit (leveled_audit/REVIEW.md, QUALITY_SYNTHESIS.md)
showed that every severe defect in the fork lives in the durable-but-
unpublished window the caller-side write protocol created. Decomposing what
that protocol bought: augmentation CPU parallelism (retainable as pure
caller-side preparation), nothing for journal throughput (appends serialize
at the Inker either way), and only ms-scale read-latency isolation. The
window is therefore removed, not fenced: writes return to the stock model —
append + absorb inside the serialized Bookie callback — and the total order
(mailbox order = journal order = visibility order = replay order) becomes an
emergent property again instead of an engineering project.

Target state: **stock leveled semantics + the minimal transactional protocol
Ash requires + FTS with the cache/boundary law fixes + pure caller-side
preparation for the write CPU.**

## 1. Protocol surface

### 1.1 book_mput — atomic plural put (mode-dispatched)

- `book_mput(Pid, Entries)` / `book_mput(Pid, Entries, TTLorOpts)`.
- head_only mode: UNCHANGED upstream semantics (Entries = object_specs,
  third argument is TTL `infinity | integer()`).
- standard mode: Entries are `{put, Bucket, Key, Object, IndexSpecs, Tag,
  TTL}` / `{delete, Bucket, Key, Tag}`; third argument is an options map
  `#{sync => boolean()}` (type-disambiguated from TTL). All entries are
  written to the journal under ONE SQN via ink_batchput: after a crash
  either every entry is recoverable or none is. Per-entry semantics equal N
  book_puts. Returns `ok | pause | {error, term()}`.
- Executes entirely inside the Bookie callback (normalise → validate →
  append → ledger insert → FTS cache advance → reply). No publish protocol.

### 1.2 book_casmput — conditional atomic plural put

- `book_casmput(Pid, Entries, Conditions)` /
  `book_casmput(Pid, Entries, Conditions, #{sync => boolean()})`.
- Conditions: `[{Bucket, Key, Tag, absent | present | {sqn, SQN}}]`,
  evaluable against keys inside or outside the write set.
- Evaluated and committed inside ONE Bookie callback: linearizable by
  construction (the serialized Bookie is the commit point; there are no
  unpublished lower SQNs to race — the audit's L3 class is structurally
  impossible).
- All-or-nothing: on any failed condition NOTHING is written (no journal
  append) and the reply is `{error, {precondition_failed, Failures}}` with
  every failed condition and its observed state.
- This is the transaction commit primitive (§4).

### 1.3 Version reads

- `book_get_sqn/3,4`, `book_head_sqn/3,4` (existing, audited clean): return
  `{ok, Value|Head, SQN}` / `not_found`. SQN is the version token for
  optimistic concurrency and `{sqn, N}` conditions; `not_found` observations
  pair with `absent` conditions.

### 1.4 Read plurals + value cache (kept, read-side only)

- `book_mget`, `book_mhead`, fetchspec reads: kept as-is (planner perf;
  audited clean), with the L1-F1 fix: `mhead`/`head`/`head_sqn` guards
  derive from ONE head-capability predicate (`head_lookup == true`) so the
  mode matrix cannot diverge.
- Caller-side value cache: kept (SQN-keyed entries are provably fresh),
  with the L1-F2 lifecycle fix: persistent_term registration only after
  failure-prone init steps, dead-pid sweep at valuecache_init, erase in
  terminate.
- `book_mhead` documents its deliberate omission of the journal_notfound
  probe (audit L1 suspicion → documented behaviour).

### 1.5 Deleted surfaces (compile errors after the cut)

- `write_refs` / process-dictionary ref caching, `put_caller_side*`,
  `{publish, ...}`, `{publish_fts, ...}`, `{fts_put_intent}`,
  `mput_caller_side/mput_write_and_publish/publish_fts_changes`.
- The absorption engine (absorb_write/apply_write/drain_pending/
  publish_frontier/publish_pending/gap timer) and the Inker void machinery
  (ink_void/ink_getvoids/journal_voids) — no gaps can exist, so neither can
  their fences. The pmem `{sqn_order_violation, ...}` descriptive error
  stays (better diagnostics for an invariant stock upholds).
- `book_mput_std`/`book_mput_std_direct`/`book_casmput_std`-style names:
  renamed to the §1.1/§1.2 protocol names. `book_put_direct` folds into
  `book_put` (there is only the direct path). Singular `book_casput` is
  removed; a singular CAS is a casmput of one.
- The fts_seq counter and its resync/error paths (§2).

## 2. One clock (Law 1, carried over)

The journal SQN is the only sequence space. Required for §3 (prepared
augmentation needs no pre-append allocation) and kills the audit's L2-F3
class (counter drift/reuse across restart):

- Fresh posting-delta frames are seq-free at rest: a frame's liveness stamp
  IS its ledger row's SQN (leveled rows carry their SQN intrinsically).
- The doc marker stores only doc metadata (e.g. doc_length); its version IS
  its row SQN. Marker and frames commit in the same batch (same SQN), so
  liveness is `frame row SQN == marker row SQN` — exact, no allocation.
- Consolidated bases persist per-frame ORIGIN SQNs explicitly (known at
  consolidation time; same slots as today with unambiguous semantics).
- FTS query/cache sequence = the Bookie's current SQN. Restart seeding =
  journal high-water mark, correct by construction.

## 3. Prepared caller-side augmentation (the retained CPU win)

Tokenisation + posting encoding remain parallel WITHOUT a write protocol:

- `leveled_fts` exposes pure preparation: derive/encode an entry's postings
  against the static schema set, producing seq-free prepared postings (§2).
- `book_mput`/`book_casmput` accept entries in two forms: raw objects (the
  Bookie augments in-callback — correctness baseline and differential
  oracle) and `{prepared, Entry, PreparedPostings}` (caller supplies the
  encoded postings; the Bookie validates structurally — schema fingerprint,
  column bounds, frame decode sanity — before accepting; forged or
  schema-stale preparations are rejected loudly, extending the existing
  forged-spec rejection guarantee to the prepared form).
- The Bookie's residual per-batch work: validation, one inker append,
  ledger insert, FTS cache advance. Reads queue behind appends (ms-scale
  under indexing load); accepted, and the future lever for it is sharding
  by entity, not protocol.

## 4. Transactions — minimal capability set for Ash `:transact`

ash_leveled MUST claim `can?(_, :transact) -> true`. The store needs NO new
primitives beyond §1: transactions are an OCC layer in ash_leveled composed
from casmput + version reads (+ optional snapshots).

- `transaction(resource, fun, timeout, reason)`: establish a process-scoped
  context {depth, write_stage, read_set, fences}; run fun.
  - Reads inside: overlay write_stage over store reads (read-your-writes);
    every point read records `{key, sqn | absent}` into read_set.
  - Writes inside: staged into write_stage (with their FTS preparation);
    the store is untouched.
  - Commit (outermost only): ONE `book_casmput(staged_entries,
    read_set_conditions ++ fence_conditions)`. Success → all effects
    visible atomically, notifications released. `precondition_failed` →
    bounded retry of fun (fresh context), then a concurrency error mapping
    to Ash's stale/conflict errors.
  - `rollback/2`: drop context, nothing to undo (no store effects exist).
    Exceptions and timeouts likewise cannot leak partial state.
  - Nesting: depth counter; inner "transactions" join the outer context;
    inner rollback propagates outward. `in_transaction?/1` = context
    presence.
- Isolation contract: no dirty reads (staging), serializable over the
  observed point-read footprint (OCC validation at the serialized commit
  point — concurrent conflicting commits cannot both pass). Range/query
  reads inside transactions are NOT footprint-validated (phantom caveat);
  query-dependent transactions opt into per-resource FENCE KEYS: every
  write batch touching resource R also puts `fence(R)` (a normal key), and
  the transaction adds `{fence(R), {sqn, ObservedSQN}}` to its conditions —
  coarse-grained serializability per resource using only §1 primitives.
- Liveness: no locks anywhere → no deadlocks; timeout applies to fun
  execution only; contention cost is retries, paid by the conflicting
  transaction.
- Identity semantics improve: reservation + record commit in ONE casmput →
  crash-orphaned reservations are impossible for single-action writes; the
  existing reclaim path remains for multi-action workflows only.

## 5. FTS law fixes (Laws 4–5, unchanged from the redesign)

Cache admission (Law 4) — no cache row without a token derived from the
durable clock (now simply the SQN):
- Per-shard write epoch bumped by EVERY write touching the shard including
  the row-absent case; cold fills record the epoch before folding and
  install only if unchanged (kills L5-F3 permanent staleness and the
  stamp-trust class).
- FTS result cache keyed on the invocation-time store SQN; runner closures
  resolve their cache key at INVOCATION (kills L5-F4); no repair paths
  exist to invalidate (no gaps).
- Consolidated-base fetch failure falls back to a direct read (parity with
  the public GET path; audit L5 suspicion).

Boundaries (Law 5) — accept ⊆ encode, oracle-enforced parity:
- Codec exports format capacities (≤255 columns, key/token/position byte
  bounds); schema validation consumes them; every fixed-width encode site
  guards loudly.
- True occurrence count persisted alongside capped positions; BM25 TF and
  doc_length draw from the same count (restores the FTS.md:251 contract).
- One token-boundary definition shared by fast and unicode tokenizer paths
  (malformed UTF-8 is a boundary); SQLite-semantics remove_diacritics
  modes 1/2; the generated SQLite FTS5 oracle corpus
  (test/fts_sqlite_oracle_corpus.eterm, 82 cases/267 oracle-verified
  queries) runs as a standing differential CT suite.

## 6. Verification gates

1. leveled eunit + CT green; the L3 CAS contract matrix and all L4/L5
   positive-control scripts still pass against the new protocol names.
2. Audit repro disposition: every repro either flips to non-reproduction or
   is N/A because its API no longer exists (recorded per-repro in
   leveled_audit/REVIEW.md).
3. Restart-equivalence and concurrent-writer invariant tests retained in
   eunit using only public API (the Phase A protocol tests are recast).
4. ash_leveled adapted (call sites + OCC transaction layer + fences);
   its suite green; Ash transaction semantics covered by tests (commit,
   rollback, nesting, retry-on-conflict, RYW, notification timing).
5. vfs e2e benchmarks: indexing and search within noise of the pre-cut
   baseline (prepared augmentation keeps the CPU parallelism; the gate
   numbers must show it).
6. Two rounds of adversarial Codex review of the full surface.

## 7. Name/compat map

| before | after |
|---|---|
| book_mput_std/2,3 | book_mput/2,3 (standard mode) |
| book_mput_std_direct | (gone — book_mput IS direct) |
| book_mput/2,3 (head_only) | unchanged |
| book_casmput (direct+caller variants) | book_casmput/3,4 (single direct form) |
| book_casput | removed (casmput of one) |
| book_put_direct | book_put (single path) |
| write_refs / publish / publish_fts / fts_put_intent | removed |
| absorption engine + journal voids | removed |
| TARGET_API.md §3 (three-phase writes) | superseded by this document |

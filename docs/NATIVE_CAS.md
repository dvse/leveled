# NATIVE CAS — leveled's transactional write protocol

Status: design authority (2026-07-12). This document and FTS.md are the
only fork documents beyond stock leveled; it consolidates and supersedes
TARGET_API.md, the earlier NATIVE_CAS.md, STANDARD_BATCHPUT.md,
STOCK_PLUS.md and SQN_ENGINE_REDESIGN.md (history in git). The audit
evidence behind its design verdicts lives in audit/.

## 1. Purpose and scope

This fork is upstream leveled plus the SMALLEST patch that turns the
store into a transactional substrate: atomic multi-key commits,
conditional commits, and version reads. Nothing here is specific to any
consumer; the reference consumer is ash_leveled, which builds full
`Ash.DataLayer.transaction` support (§6) from these primitives without
any further store changes. Full-text search is a separate, orthogonal
addition documented in FTS.md.

The patch inventory over upstream, in full:

| unit | kind | section |
|---|---|---|
| `ink_batchput` + standard-mode `book_mput` | transactional core | §3 |
| `book_casmput` | transactional core | §4 |
| `book_get_sqn`, `book_head_sqn` | transactional core | §5 |
| `book_mget`, `book_mhead`, fetchspec reads | read performance | §7 |
| SQN-keyed caller-side value cache | read performance | §7 |
| FTS engine (leveled_fts) + write/search integration | search | FTS.md |

Everything else is upstream behaviour.

## 2. Design principle: one serialized order

The Bookie stays serialized, as upstream: mailbox order = journal order =
visibility order = replay order. Every write executes entirely inside one
Bookie callback (validate → journal append → ledger insert → reply), so:

- an acknowledged write is durable, visible, and restart-stable at the
  moment of its reply;
- live state is always exactly the state journal replay rebuilds;
- a conditional write's evaluation and commit share one callback, which
  makes it linearizable with no further machinery (§4).

This fork previously moved journal writes into caller processes for
throughput. A five-layer adversarial audit (audit/REVIEW.md) traced every
severe defect to the durable-but-unpublished window that protocol
created, and its throughput decomposition showed the window bought almost
nothing: journal appends serialize at the Inker either way, and the real
CPU win — FTS augmentation — is a pure function that can run caller-side
as mere PREPARATION feeding ordinary direct calls (FTS.md). The window is
therefore removed, not fenced. The scaling lever beyond one serialized
engine is running more engines (sharding by entity), not weakening the
engine's order.

## 3. book_mput — atomic plural put

`book_mput(Pid, Entries)` · `book_mput(Pid, Entries, Third)`

The client passes `Third` through verbatim; the mode-guarded server
handlers interpret it:

- **head_only mode** — unchanged upstream semantics: Entries are
  object_specs, `Third` is a TTL (`infinity | integer()`).
- **standard mode** — Entries are
  `{put, Bucket, Key, Object, IndexSpecs, Tag, TTL}` or
  `{delete, Bucket, Key, Tag}`; `Third` is `#{sync => boolean()}`
  (default `#{}`).

Standard-mode contract:

- **Atomic durability.** All entries are appended to the journal under
  ONE SQN. After a crash, either every entry is recoverable or none is.
- **Per-entry equivalence.** Each entry behaves exactly as the
  corresponding `book_put`/`book_delete` would: same validation (forged
  internal index specs rejected), same FTS augmentation for indexed
  buckets, same TTL and tombstone semantics.
- **Backpressure.** `pause` means the batch was accepted and the caller
  should back off, exactly as for `book_put`.
- Returns `ok | pause | {error, term()}`.

## 4. book_casmput — conditional atomic plural put

`book_casmput(Pid, Entries, Conditions)` ·
`book_casmput(Pid, Entries, Conditions, #{sync => boolean()})`

Conditions are `[{Bucket, Key, Tag, Condition}]` with

    Condition :: absent | present | {sqn, non_neg_integer()}

- Condition keys may be inside or outside the write set, in any bucket
  or tag.
- Duplicate condition keys and unrecognised condition terms are
  validation errors (`{error, ...}` without evaluation).

**Whole-batch contract.** ALL conditions are evaluated against current
store state before ANY entry is accepted. On any failure nothing is
written — no journal append, journal SQN unchanged — and the reply is
`{error, {precondition_failed, Failures}}`, listing every failed
condition with its observed state. On success the batch commits
atomically under one SQN, exactly as `book_mput`.

**Lifecycle semantics.** `present` sees live objects only: tombstoned
and TTL-expired objects are `absent`. `{sqn, N}` matches the current
object's SQN exactly, and fails against absent, tombstoned, or expired
objects.

**Linearizability.** Evaluation and commit run in ONE serialized Bookie
callback; the callback is the commit point and no other write can
interleave. There is no caller-side write path, so no
journal-durable-but-invisible state can race the evaluation (the audit's
L3-F1 class is structurally impossible).

A single conditional put is a casmput of one entry; no separate singular
API exists.

## 5. Version reads

`book_get_sqn/3,4` and `book_head_sqn/3,4` return
`{ok, Value | Head, SQN} | not_found`. The SQN is the store's version
token: monotonic, allocated by the journal, never reused. A successful
read pairs with a later `{sqn, N}` condition; a `not_found` observation
pairs with `absent`. Together with §4 this is the complete
optimistic-concurrency surface.

## 6. Building transactions on the protocol

The protocol above is sufficient for full interactive transactions via
optimistic concurrency control, implemented entirely in the consumer.
The reference implementation is ash_leveled's `Ash.DataLayer.transaction`
support (`can?(_, :transact) -> true`); any consumer can follow the same
recipe:

**Context.** `transaction(fun)` establishes a process-scoped context
`{depth, write_stage, read_set, fences}` and runs `fun`.

**Reads** overlay `write_stage` over store reads (read-your-writes).
Every point read records `{key, sqn | absent}` in `read_set`.

**Writes** stage into `write_stage` — with their FTS preparation — and
do not touch the store.

**Commit** (outermost only) is ONE call:

    book_casmput(staged_entries, read_set_conditions ++ fence_conditions)

Success makes all effects visible atomically (release notifications
here). `precondition_failed` means a conflicting commit interleaved:
retry `fun` with a fresh context a bounded number of times, then surface
a concurrency error (Ash: stale/conflict).

**Rollback** drops the context; no store effects exist to undo, so
exceptions and timeouts cannot leak partial state. **Nesting** is a
depth counter — inner transactions join the outer context and inner
rollback propagates. `in_transaction?` is context presence.

**Isolation contract.** No dirty reads (staging). Serializable over the
observed point-read footprint: validation runs at the serialized commit
point, so two conflicting commits cannot both pass. Range and query
reads are NOT footprint-validated — the phantom caveat. Query-dependent
transactions opt into per-resource **fence keys**: every write batch
touching resource R also puts `fence(R)` (an ordinary key), and the
transaction adds `{fence(R), {sqn, Observed}}` to its conditions,
buying coarse per-resource serializability from the same primitives.

**Liveness.** No locks anywhere, so no deadlocks; contention costs
retries, paid by the conflicting transaction.

**Identity pattern.** Uniqueness reservations commit in the same
casmput as the record that claims them, so crash-orphaned reservations
are impossible for single-action writes; reclaim logic remains only for
multi-action workflows.

## 7. Read-side additions (non-transactional)

Performance additions used by the ash_leveled planner; none participate
in the write protocol:

- `book_mget`, `book_mhead`, and fetchspec reads — plural reads that
  plan in the Bookie and fetch in the caller. `book_mhead` resolves
  purely from ledger state and deliberately omits the singular head's
  frequency-gated journal_notfound probe.
- The caller-side value cache (opt-in, `{value_cache_size, Bytes}`):
  entries are keyed `{LedgerKey, SQN}`, so a hit is provably current —
  an overwrite allocates a new SQN and old entries become unreachable.

## 8. Name/compat map

| before | after |
|---|---|
| book_mput_std/2,3 | book_mput/2,3 (standard mode) |
| book_mput_std_direct/3 | book_mput (single direct path) |
| book_casmput/4 (boolean) | book_casmput/3,4 (opts map) |
| book_casput/9 | removed — casmput of one entry |
| book_put_direct/8 | book_put/8 (single direct path) |
| write_refs / {publish} / {publish_fts} / {fts_put_intent} | removed |
| absorption frontier / journal voids | removed (nothing to fence) |

## 9. Verification gates

1. leveled eunit + CT green; the audit's CAS contract matrix and FTS
   positive-control scripts pass against the protocol names.
2. Every audit repro (audit/) flips to non-reproduction or is N/A with
   its API removed — dispositions recorded in audit/REVIEW.md.
3. Restart-equivalence and concurrent-writer invariant tests live in
   eunit using the public API only.
4. ash_leveled suite green with the OCC transaction layer; transaction
   tests cover commit, rollback, nesting, retry-on-conflict,
   read-your-writes, fences, and notification timing.
5. vfs e2e benchmarks (indexing + search) within noise of the pre-cut
   baseline.
6. Two rounds of adversarial Codex review.

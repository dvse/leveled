# ASH — the minimum upstream patch for full Ash transactions

Status: DESIGN AUTHORITY (2026-07-12). This document and FTS.md are the
ONLY fork documents beyond stock leveled. It consolidates and supersedes
TARGET_API.md, NATIVE_CAS.md, STANDARD_BATCHPUT.md, STOCK_PLUS.md and
SQN_ENGINE_REDESIGN.md (all deleted; history in git). Audit evidence
behind the design verdicts lives in audit/.

## Principle

Stock leveled semantics, plus the SMALLEST patch that makes ash_leveled a
full Ash data layer — including `Ash.DataLayer.transaction`. The Bookie
stays serialized: mailbox order = journal order = visibility order =
replay order, as upstream. No caller-side write protocol exists; the
5-layer audit (audit/REVIEW.md) proved that window was the source of every
severe defect class, and it is removed rather than fenced. The write-CPU
win that protocol chased is kept as pure caller-side PREPARATION (FTS
augmentation, see FTS.md) feeding ordinary direct calls.

## Patch inventory vs upstream

Transactional core (required for Ash):

1. **Atomic plural put** — `ink_batchput` (one SQN for N standard
   objects) + standard-mode `book_mput`.
2. **Conditional atomic plural put** — `book_casmput` (the transaction
   commit primitive).
3. **Version reads** — `book_get_sqn`, `book_head_sqn` (SQN as version
   token; `not_found` pairs with `absent` conditions).

Non-transactional additions (perf, used by the ash_leveled planner):

4. Read plurals: `book_mget`, `book_mhead`, fetchspec reads.
5. Caller-side value cache (SQN-keyed, opt-in via `value_cache_size`).
6. The FTS engine (leveled_fts) and its write/search integration — see
   FTS.md, including the prepared-augmentation form of mput/casmput
   entries and the seq-free frame format (one clock: the journal SQN).

Everything else is upstream behaviour.

## book_mput — atomic plural put

- `book_mput(Pid, Entries)` / `book_mput(Pid, Entries, Third)`; the
  message carries `Third` verbatim and the mode-guarded handlers
  interpret it.
- head_only mode: UNCHANGED upstream semantics (Entries = object_specs;
  `Third` = TTL `infinity | integer()`).
- standard mode: Entries are
  `{put, Bucket, Key, Object, IndexSpecs, Tag, TTL}` |
  `{delete, Bucket, Key, Tag}`; `Third` is `#{sync => boolean()}`
  (default `#{}`). All entries commit under ONE journal SQN: after a
  crash either every entry is recoverable or none. Per-entry semantics
  equal N book_puts; `pause` carries the usual backpressure meaning.
- Runs entirely inside the Bookie callback: normalise → validate (public
  index specs; forged FTS carriers rejected) → FTS augmentation →
  journal append → ledger insert → FTS cache advance → reply.

## book_casmput — conditional atomic plural put

- `book_casmput(Pid, Entries, Conditions)` /
  `book_casmput(Pid, Entries, Conditions, #{sync => boolean()})`.
- Conditions: `[{Bucket, Key, Tag, absent | present | {sqn, SQN}}]`.
  Condition keys may be inside or outside the write set, in any bucket
  or tag. Duplicate condition keys and unknown condition terms are
  rejected as validation errors.
- Whole-batch contract: ALL conditions are evaluated against the current
  store state before ANY entry is accepted. On failure nothing is
  written (no journal append, journal SQN unchanged) and the reply is
  `{error, {precondition_failed, Failures}}`, where Failures lists every
  failed condition with its observed state. On success the batch commits
  exactly as book_mput.
- Linearizability: evaluation and commit happen in ONE serialized Bookie
  callback — the callback IS the commit point, and no other write can
  interleave. (There is no caller-side write path, so no
  journal-durable-but-invisible state can race the condition check;
  audit finding L3-F1 is structurally impossible.)
- Condition semantics against object lifecycle: `present` sees live
  objects only (tombstoned and TTL-expired objects are absent);
  `{sqn, N}` matches the current object's SQN exactly and fails against
  absent/tombstoned/expired objects.

## Transactions — Ash `:transact` capability set

ash_leveled claims `can?(_, :transact) -> true`, implemented as an OCC
layer that consumes ONLY the primitives above (no store changes):

- `transaction(resource, fun, timeout, reason)` establishes a
  process-scoped context `{depth, write_stage, read_set, fences}`:
  - Reads overlay `write_stage` over store reads — read-your-writes.
    Every point read records `{key, sqn | absent}` into `read_set`.
  - Writes stage into `write_stage` (with their FTS preparation); the
    store is untouched until commit.
  - Commit (outermost only): ONE
    `book_casmput(staged_entries, read_set_conditions ++ fences)`.
    Success → all effects visible atomically; Ash notifications release
    after commit. `precondition_failed` → bounded retry of fun with a
    fresh context, then a concurrency error mapped to Ash stale/conflict
    errors.
  - `rollback/2`: drop the context — no store effects exist to undo.
    Exceptions and timeouts cannot leak partial state.
  - Nesting: depth counter; inner transactions join the outer context;
    inner rollback propagates. `in_transaction?/1` = context presence.
- Isolation contract: no dirty reads (staging); serializable over the
  observed point-read footprint — the OCC validation runs at the
  serialized commit point, so two conflicting commits cannot both pass.
  Range/query reads inside transactions are NOT footprint-validated
  (documented phantom caveat); query-dependent transactions opt into
  per-resource FENCE KEYS: every write batch touching resource R also
  puts `fence(R)` (an ordinary key), and the transaction conditions on
  `{fence(R), {sqn, Observed}}` — coarse per-resource serializability
  from the same primitives.
- Liveness: no locks → no deadlocks; contention costs retries only.
- Identity semantics: reservation + record commit in ONE casmput, so
  crash-orphaned reservations are impossible for single-action writes;
  reclaim remains only for multi-action workflows.

## Name/compat map (old fork names → this spec)

| before | after |
|---|---|
| book_mput_std/2,3 | book_mput/2,3 (standard mode) |
| book_mput_std_direct/3 | book_mput (single direct path) |
| book_casmput/4 (boolean) | book_casmput/3,4 (opts map) |
| book_casput/9 | removed — casmput of one |
| book_put_direct/8 | book_put/8 (single direct path) |
| write_refs / {publish} / {publish_fts} / {fts_put_intent} | removed |
| absorption frontier / journal voids | removed (nothing to fence) |

## Verification gates

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

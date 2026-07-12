# NATIVE CAS — leveled's transactional write protocol

Status: design authority (2026-07-12). This document and FTS.md are the
only fork documents beyond stock leveled. Audit evidence behind the
design verdicts lives in audit/; superseded design documents are in git
history.

## 1. Purpose

The smallest patch that turns stock leveled into a transactional
substrate. Upstream already contains the hard part: `book_mput` commits
N object-spec rows through ONE journal record — atomic and torn-safe by
CDB's existing CRC recovery, replayed and compacted by existing code
(introduced upstream in 2018 for head_only stores). The fork adds
exactly three things:

1. `book_mput` is allowed in standard-mode stores (one guard removed);
2. a conditional form of it: `book_casmput`;
3. a read-correctness fix: `?HEAD_TAG` rows resolve purely from ledger
   state (they are `no_lookup` changes journaled under `?DUMMY`, so
   reads must bypass the penciller L0 index and the journal probe in
   every store mode).

Everything else — version reads, head reads, folds, snapshots — is
upstream, untouched. Full-text search consumes ONLY this public surface
(FTS.md): the store carries zero FTS hooks, zero FTS configuration, and
no knowledge that FTS exists.

## 2. The two planes

**Transactional plane — head rows.** Transactional state (records,
uniqueness reservations, fences, join rows, projections) is stored as
object-spec rows: `{add | remove, Bucket, Key, SubKey, Value}`, values
living in the ledger under `?HEAD_TAG`. A commit writes any number of
rows in one `book_mput`/`book_casmput` call = one journal record = one
SQN. Values ride the penciller, so this plane is for row-sized values,
not blobs.

**Bulk plane — standard objects.** Large-bodied objects (documents,
extracted text) remain ordinary `book_put` standard objects: per-record
atomic, journal-bodied, non-transactional. The indexing pipeline lives
here and needs no transactions. Cross-plane consistency is by ordering:
write the bulk object first, then commit the head rows that reference
it (the head-row commit is the authoritative fact; a crash between the
two leaves an unreferenced bulk object for idempotent redo).

Consumers scale by running MORE bookies (per resource group, or hash-
sharded), not by weakening one bookie's serialization: a transaction's
write-set must map to one bookie, and the reference topology lives in
the ash_leveled target spec.

## 3. The patch

### 3.1 book_mput in standard mode

Upstream semantics, unchanged: `book_mput(Pid, ObjectSpecs[, TTL])`,
object_specs as upstream defines them, one `INKT_MPUT` journal record,
ledger rows via `gen_headspec`. The only change is removing the
`head_only == true` guard: the flow (ink_mput → preparefor_ledgercache →
ledger insert → reply) is already mode-agnostic, as are replay and
compaction of `INKT_MPUT` records.

### 3.2 book_casmput — the commit primitive

`book_casmput(Pid, ObjectSpecs, Conditions)` ·
`book_casmput(Pid, ObjectSpecs, Conditions, TTL)`

    Conditions :: [{Bucket, Key, SubKey, absent | present | {sqn, SQN}}]

Evaluate all conditions against current head state; if every condition
holds, execute exactly the `book_mput` flow on ObjectSpecs; otherwise
write nothing (no journal append) and reply
`{error, {precondition_failed, Failures}}`, where Failures lists each
failed condition with its observed state (`absent | {present, SQN}`).

- Condition keys address head rows (the transactional plane) and may be
  inside or outside the write set.
- Lifecycle: `present` sees live rows only — a tombstoned (`remove`d)
  or TTL-expired row is `absent`; `{sqn, N}` matches a live row's SQN
  exactly.
- Linearizability: evaluation and commit share one serialized Bookie
  callback — the callback is the commit point, and there is no other
  write path that could make durable-but-invisible state race the
  evaluation.
- Requires `head_lookup` (standard stores, and head_only stores started
  `with_lookup`).

### 3.3 Version reads (upstream)

`book_sqn/3,4` returns a live row's SQN — the version token; pairs with
`{sqn, N}`. `not_found` pairs with `absent`. `book_headonly/4` /
`book_head/3,4` read row values. Nothing added.

## 4. Building transactions (the OCC recipe)

Full interactive transactions are an optimistic-concurrency layer in
the consumer; the reference implementation is ash_leveled's
`Ash.DataLayer.transaction` (`can?(_, :transact) -> true`). No store
support beyond §3 exists or is needed.

- `transaction(fun)` opens a process-scoped context
  `{depth, write_stage, read_set}`.
- Reads overlay `write_stage` (read-your-writes); every point read
  records `{row, sqn | absent}` in `read_set`.
- Writes stage object-spec rows; the store is untouched.
- Commit (outermost only) is one call:
  `book_casmput(staged_specs, read_set_conditions)`. Success = all
  effects visible atomically; release notifications after it.
  `precondition_failed` = a conflicting commit interleaved: retry `fun`
  with a fresh context (bounded), then surface a concurrency error.
- Rollback drops the context; nothing to undo. Nesting is a depth
  counter; inner rollback propagates.
- Isolation: no dirty reads; serializable over the observed point-read
  footprint (validation happens at the serialized commit point).
  Range/query reads are not footprint-validated — the phantom caveat.
  Query-dependent transactions add a fence: every commit touching
  resource R also writes row `fence(R)`, and the transaction conditions
  on `{fence(R), {sqn, Observed}}`.
- No locks, so no deadlocks; contention costs retries only.
- Uniqueness reservations commit in the same call as the record
  claiming them — crash-orphaned reservations cannot exist for
  single-action writes.

## 5. What was deliberately not built

The previous fork implemented multi-record standard-object batches
(`book_mput_std`), a caller-side three-phase write protocol, batch
key-change wrappers with torn-tail replay detection, and a CAS variant
over journal-bodied objects. The audit (audit/REVIEW.md,
audit/QUALITY_SYNTHESIS.md) traced every severe defect to that
machinery's ordering windows, and the two-plane model makes it
unnecessary: transactional state is row-sized and belongs in head rows,
where upstream's single-record commit already provides atomicity; bulk
objects don't need transactions. A transaction that must atomically
rewrite several large journal-bodied objects is out of scope by design
— store references to bulk objects in head rows and commit the
references.

## 6. Verification gates

1. leveled eunit + CT green on the patched surface (casmput condition
   matrix: absent/present/sqn against live/tomb/expired/missing rows,
   whole-batch rejection with unchanged journal SQN, restart
   equivalence, concurrent single-winner).
2. Audit repro dispositions recorded in audit/REVIEW.md (each repro
   flips to non-reproduction or is N/A with its API removed).
3. ash_leveled suite green on the OCC layer (commit, rollback, nesting,
   retry-on-conflict, read-your-writes, fences, notification timing).
4. vfs e2e benchmarks (indexing + search) within noise of the pre-cut
   baseline.
5. Two rounds of adversarial Codex review.

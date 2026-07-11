# Leveled Target Public API

Status: TARGET SPECIFICATION (settled 2026-07-11). Landed so far:
fetchspec `book_mget`/`book_get` (§3.1, snapshot-free reads);
`book_mhead` (§3.1); `book_mput_std`/`book_casmput` surface (§3.2/§3.3
naming + contract over the existing batch engine — the
RESOLVE→IO→PUBLISH write engine is migration step 2; `book_mput/3` for
standard stores awaits absorbing the head_only arity). Deprecated
aliases `book_batchput`/`book_casbatchput` retained until ash_leveled
migrates. This document defines
the complete public Bookie surface in its target state, the uniform
execution protocol underneath every operation, the guarantee each
operation carries, and the migration/retirement plan for the current
surface. Implementation lands incrementally; each landed piece must meet
the per-primitive gates in §7 before the corresponding legacy name
retires.

## 1. The one architectural rule

**The Bookie process performs only in-memory state transitions. All disk
IO executes in caller processes.**

Every data operation decomposes into the same three phases:

| Phase | Executes in | Reads | Writes |
|---|---|---|---|
| RESOLVE | Bookie call (in-memory, µs) | head lookup → fetch spec(s) | SQN allocation + write intent |
| IO | caller process | journal pread(s) | journal append (group-committed per file) + sync policy |
| PUBLISH | Bookie (in-memory) | — | SQN-ordered ledger absorb; CAS conditions evaluated here |

Rationale (all measured, see ash_leveled campaign RESULTS): singleton-
resident IO serializes every concurrent caller behind every other
caller's disk time; ledger snapshots clone the write-heavy ledger cache
and must never be paid per point operation; the transaction writer lock
buys atomicity and must never be used as a batching vehicle.

## 2. Naming convention

Standard KV idiom. Singular and plural forms are semantic pairs: the
plural (`m*`) form has **identical per-key semantics** to N invocations
of the singular, differential-pinned by test, plus explicitly documented
batch-level properties. No `batch*` names in the target surface.

## 3. Data plane

### 3.1 Reads

```erlang
book_get(Pid, Bucket, Key)                    %% Tag = ?STD_TAG
book_get(Pid, Bucket, Key, Tag)
  -> {ok, Object} | not_found

book_mget(Pid, Bucket, Keys)
book_mget(Pid, Bucket, Keys, Tag)
  -> [{Key, {ok, Object} | not_found}]        %% input order; duplicates preserved

book_head(Pid, Bucket, Key [, Tag])           -> {ok, Head} | not_found
book_mhead(Pid, Bucket, Keys [, Tag])         -> [{Key, {ok, Head} | not_found}]   %% NEW
book_headonly(Pid, Bucket, Key, SubKey)       -> as today (head_only stores)

book_sqn/3,4, book_head_sqn/3,4, book_get_sqn/3,4   %% unchanged utilities

book_get_direct(Pid, Bucket, Key, Tag)        %% the serial in-Bookie oracle
```

Semantics:
- `book_get`: RESOLVE returns a fetch spec ({fetch, LedgerKey, SQN} or
  not_found) from an inline head resolution — no snapshot; IO is a
  single-pair `ink_mget` pread in the caller. Linearization point is the
  RESOLVE call, identical to the historical in-Bookie get.
- `book_mget`: one RESOLVE call returns N fetch specs (inline, no
  snapshot, no ledger-cache clone); one batched caller-side journal read
  serves all values. Strictly cheaper than N `book_get`s for any N ≥ 2.
  No cross-key consistency is claimed: each key's head is resolved
  at the same call, which is exactly the guarantee N sequential gets
  would give, no stronger.
- `book_mhead`: the head-only plural; pure RESOLVE, no IO phase.
- **Race contract (all reads)**: a journal re-organisation between
  RESOLVE and IO (file close/truncation by compaction) surfaces as a
  caller-side crash or as `not_present` after a positive head. Both fall
  back transparently to `book_get_direct` for the affected key(s).
  Degradation is a retried read — never a wrong answer, never a false
  `not_found`.
- `book_get_direct` remains public: the differential oracle, the race
  fallback target, and the escape hatch for callers that require the
  read fully serialized through the Bookie.

### 3.2 Writes

```erlang
book_put(Pid, Bucket, Key, Object, IndexSpecs)              %% Tag = ?STD_TAG
book_put(Pid, Bucket, Key, Object, IndexSpecs, Tag)
book_put(Pid, Bucket, Key, Object, IndexSpecs, Tag, TTL, DataSync)
  -> ok | {error, term()}

book_mput(Pid, Entries)
book_mput(Pid, Entries, DataSync)
  -> ok | {error, term()}
  %% Entry :: {Bucket, Key, Object | delete, IndexSpecs, Tag}
  %%        | {Bucket, Key, Object | delete, IndexSpecs, Tag, TTL}

book_delete(Pid, Bucket, Key, IndexSpecs)     %% sugar: tombstone put
book_tempput/7                                %% sugar: put with TTL (kept)
```

Semantics:
- `book_put`: RESOLVE allocates the SQN and registers a write intent;
  the caller appends the encoded journal record (group-commit per
  journal file; the append-offset assignment is the only serial step and
  is an in-memory operation feeding vectored writes); PUBLISH absorbs
  the journal ref into the ledger in SQN order. `ok` is returned only
  after the configured sync policy is satisfied — the durability
  contract is unchanged from today, only the executing process moves.
- `book_mput`: per-entry semantics identical to N `book_put`s, PLUS one
  batch-level guarantee that falls out of the single group append:
  **atomic durability** — after a crash, either every entry in the batch
  is recoverable from the journal or none is. **No isolation is
  claimed**: concurrent readers may observe the batch partially
  published. `book_mput` is thereby the commit primitive for the
  ash_leveled deferred-batch transaction (which supplies isolation via
  its writer lock).
- Ordering: PUBLISH is SQN-ordered with a reorder buffer in the Bookie;
  the ledger never observes a gap while an earlier SQN is unpublished.
- Crash windows (both recoverable with existing journal semantics):
  allocated-but-never-appended SQN = detectable hole on journal scan
  (records are length-prefixed; torn/absent writes surface), recovery
  skips it; appended-but-unpublished = replays into the ledger on open.
- Read-your-writes: PUBLISH completes before `ok` (or RESOLVE consults
  the pending-publish buffer) — a caller that writes then reads sees its
  write.

### 3.3 Conditional writes

```erlang
book_casput(Pid, Bucket, Key, Object, IndexSpecs, Tag, Condition [, TTL, DataSync])
  -> ok | {error, {condition_failed, Current}}

book_casmput(Pid, Entries, Conditions [, DataSync])
  -> [{Key, ok | {error, {condition_failed, Current}}}]
```

Conditions (current-SQN match, absence, attribute predicates — the
existing condition vocabulary) are evaluated at PUBLISH, in-memory, per
entry. A failed condition rejects that entry's publish; its already-
appended journal record becomes compaction garbage (cheap, bounded by
the batch). `book_casmput` reports per-key outcomes; there is no
cross-entry atomicity — that is the transaction layer's job.

### 3.4 Stable-view family (folds and queries)

Unchanged in shape — these are the legitimate snapshot users (iteration
requires a stable view) and already execute caller-side via
`{async, Runner}`:

```erlang
book_returnfolder/2, book_indexfold/5, book_multiindexfold/5,
book_headfold/6,7,9, book_objectfold/4,5,6, book_keylist/3..6,
book_bucketlist/4, book_journalfold/4,
book_snapshot/4                               %% composition primitive
book_ftssearch/5, book_ftsconsolidate/4      %% FTS query surface
```

The snapshot-based multi-get (`{mget, ...}` internal clause) is retained
only as the internal building block for fold hydration where a caller
explicitly composes against one snapshot; it is not part of the public
KV surface.

## 4. Control plane (unchanged)

```erlang
book_start/1,4, book_plainstart/1, book_close/1, book_destroy/1
book_compactjournal/2, book_islastcompactionpending/1,
book_lastcompactionresult/1, book_trimjournal/1
book_hotbackup/1, book_isempty/2, book_status/1, book_headstatus/1,
book_journalsqn/1, book_returnactors/1
book_loglevel/2, book_addlogs/2, book_removelogs/2, book_logsettings/1
```

## 5. Retirements

| Current | Disposition |
|---|---|
| `book_batchput/2,3` | superseded by `book_mput` (rename + protocol); removed after ash_leveled migrates |
| `book_casbatchput/3,4` | superseded by `book_casmput`; same path |
| current `book_mput/2,3` (existing semantics) | absorbed into the new `book_mput` contract above |
| snapshot-based public mget path | internal only (fold hydration) |

## 6. Consumer mapping (ash_leveled)

| Seam | Primitive |
|---|---|
| PointGet | `book_get` (oracle: `book_get_direct`) |
| BatchGet | `book_mget` (chunk size bounded by journal-read locality, not snapshot amortization) |
| Write single / bulk | `book_put` / `book_mput` |
| CAS single / bulk | `book_casput` / `book_casmput` |
| Transactions commit | writer lock (atomicity) + `book_mput` (atomic durability) |
| Head plans | `book_head` / `book_mhead` |
| Folds / FTS | unchanged |

## 7. Per-primitive acceptance gates

Every landed primitive must pass, before its legacy counterpart retires:

1. **Differential test** against its serial in-Bookie oracle across the
   canonical shape set: live values spanning journal generations,
   overwrites, tombstones, TTL-expired entries, absent keys, duplicate
   keys in one call, and post-restart journal serving (pattern:
   `get_runner_differential_test_`, `mget_fetchspec_differential_test_`).
2. **Crash-property test** killing between each phase pair
   (RESOLVE→IO, IO→PUBLISH) and asserting the recovery invariants of
   §3.2 — no lost acked write, no phantom unacked write, no ledger gap.
3. **Concurrency scaling guard**: N-way concurrent execution of the
   primitive must land well under N× the serial cost (architecture
   bound, deliberately loose; pattern: `get_concurrent_scaling_test_`).
4. Full eunit + downstream ash_leveled suite including the
   ETS-equivalence oracle.

## 8. Explicit non-goals

- Cross-key read isolation on `mget`/`mput` (transactions own isolation).
- Batching through the transaction writer lock (measured 3× regression;
  the lock is for atomicity only).
- Per-point-operation snapshots (measured 3–37× regressions; snapshots
  are for stable-view iteration only).
- Any operation whose Bookie-resident work is not O(in-memory-small).

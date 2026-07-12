# Layer 3 native CAS correctness audit

Target: `leveled` at HEAD `9d21968` on `codex/standard-mode-batchput`.

## Findings

### 1. Data-integrity: CAS can commit across an unpublished lower SQN, and restart reverses the acknowledged winner

**Invariant violated.** The three-phase write engine must absorb ledger state in SQN order, and reopening after an abrupt stop must be observationally equivalent to the state exposed after acknowledged writes. A journal-durable but unpublished caller-side write must not let CAS evaluate an older head and commit a higher SQN that is ordered differently in memory and during journal replay.

**Reproduction.** Run from this directory:

```sh
./repro_cas_frontier_restart.escript
```

The probe primes a caller's cached Inker reference, suspends only the Bookie, queues CAS first, then lets the ordinary caller-side `book_put` append and queue PUBLISH. On resume, the CAS condition executes while the lower-SQN put is already journal-durable but not in the ledger cache. Both operations return `ok`.

Exact output:

```text
seed_sqn=1 cas=ok put=ok before_restart={ok,caller_put_value,3}
after_restart={ok,cas_value,4}
BUG: acknowledged state changed across restart; pre-restart SQN 3 was replaced by replayed SQN 4
```

The result reproduced identically in three consecutive runs.

**Expected versus actual.** Given the two `ok` replies, reopening must preserve `before_restart={ok,caller_put_value,3}`. Instead, pre-restart reads return the caller-side put at SQN 3, while journal replay returns the CAS value at SQN 4. The store therefore silently changes an acknowledged current value across restart.

**Cause.** CAS checks only the current ledger cache/Penciller head at [`src/leveled_bookie.erl:2267`](/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl:2267); it has no barrier against a lower SQN already appended by caller-side `book_put` but not yet published. CAS then appends its batch and inserts its higher-SQN changes into the visible ledger cache before calling the frontier tracker at [`src/leveled_bookie.erl:3963`](/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl:3963), [`src/leveled_bookie.erl:3986`](/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl:3986), and [`src/leveled_bookie.erl:3999`](/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl:3999). When the lower-SQN PUBLISH is handled, it likewise inserts its changes before frontier processing at [`src/leveled_bookie.erl:2328`](/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl:2328) and [`src/leveled_bookie.erl:2334`](/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl:2334). The insertion itself is an unconditional ETS overwrite at [`src/leveled_bookie.erl:4830`](/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl:4830).

Consequently, `absorb_sqn`/`maybe_gated_push` protects the cache-to-Penciller watermark, but does not gate ledger-cache visibility or overwrite order. SQN 4 is inserted first; late PUBLISH inserts SQN 3 over it. Replay applies the durable records in SQN order and produces the opposite result.

**Remediation direction.** CAS needs a publication barrier or registered per-key write intents so its condition cannot linearize across relevant lower-SQN durable/unpublished writes. Ledger-cache changes also need to become visible in frontier order rather than being inserted before `absorb_sqn`; changing insertion order alone is insufficient because a CAS that checked the pre-SQN-3 head must not later commit as though it checked after SQN 3.

## Contract and coverage notes

The standalone [`cas_contract_matrix.escript`](./cas_contract_matrix.escript) passed. It covered:

- `absent`, `present`, and `{sqn, N}`, including missing, tombstoned, TTL-expired, and live TTL state, plus invalid condition terms;
- whole-batch rejection, duplicate write keys, duplicate/unsupported condition diagnostics, and exact `{error, {precondition_failed, Failures}}` structure;
- conditions on keys outside the write set, in another bucket, and under another application tag;
- condition resolution from a fresh ledger cache and after reopen through persisted ledger/Penciller state;
- rejection journal invariants by comparing `book_journalsqn/1` before and after failure;
- abrupt termination while a 50,000-condition rejected call was executing, followed by reopen: the journal SQN stayed unchanged and the proposed key remained absent;
- value-cache interaction: a failed CAS retained the prior head/value, while a successful CAS allocated a new SQN and subsequent reads could not hit the old `{LedgerKey, SQN}` entry as current;
- a 32-writer overlapping two-key CAS workload against a sequential single-winner oracle, including an external guard condition and equality of both final keys before and after restart;
- head-only mode, which rejects CAS with `{unsupported_message, casbatchput}` as a standard-mode-only operation.

`docs/TARGET_API.md:171` still advertises singular `book_casput` failure as `{error, {condition_failed, Current}}`, while the revised whole-batch text at line 193, `docs/NATIVE_CAS.md:60`, the implementation's `book_casput -> book_casmput` routing, and observed behavior all use `{error, {precondition_failed, Failures}}`. I treated the stable batch-shaped result as the settled runtime contract and this as a documentation inconsistency, not a second engine finding.

I did not independently rerun FTS-indexed CAS, journal partial-tail injection, hot backup, or retain/recalc compaction campaigns; those paths were source-reviewed only. No additional unreproduced correctness suspicions remain in the requested CAS surface.

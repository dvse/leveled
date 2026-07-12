# Patch-quality synthesis — what the 5-layer audit says about our fork

Scope: all leveled changes from upstream baseline 7f08bba to HEAD 9d21968
(TARGET_API three-phase caller-side writes, native CAS, FTS engine, value
cache, head-only extensions). 13 confirmed findings + 2 validated
suspicions, every one reproduced by me personally (see REVIEW.md).

## The headline

The patches are strong on single-threaded functional correctness and weak
on cross-process interleavings and restart equivalence. Nearly every
confirmed defect lives at a seam the performance campaign created — where
work was moved out of the Bookie's serialization point (caller-side
writes) or where a cache was added in front of a recomputation (FTS
stamps, result cache, runner cache). The data structures, codecs, query
semantics, and CAS contract logic largely survived adversarial
differential testing; the coordination around them did not.

Evidence for the strong half: L4's positive controls all pass (position/
frame/varint boundaries, 32-stream merges, delta round-trips, forged-spec
rejection on all eight write surfaces); L5's differentials found no
consolidation result-inequality, no bloom false negatives, deterministic
tie ordering; L3's contract matrix passes fully, including a 32-writer
oracle and a 50K-condition abrupt-stop probe.

## Core problem areas

### 1. One ordering primitive, five unguarded consumers
The absorption frontier was designed to gate exactly one thing — the
cache→Penciller push watermark. Everything else that used to inherit
ordering for free from the old "all writes serialize through the Bookie"
design silently assumed the frontier protected them too:

- ledger-cache same-key visibility follows insertion order, replay
  follows SQN order (L2-F2, L3-F1);
- CAS evaluates conditions with no barrier against journal-durable
  unpublished lower SQNs (L3-F1);
- FTS shard-cache advances buffer behind gaps while the twin ledger rows
  are already visible (L4-F2/L5-F2);
- the gap-timeout skip lets the persisted watermark pass a live writer,
  then the late publish is silently absorbed and acked (L2-F1 — the
  audit's one true data-loss defect);
- pmem's `MinSQN >= LedgerSQN` if_clause was an unchecked assumption one
  late publish away from crashing the store (L2-F4).

The old design gave total order as an emergent property (mailbox order =
journal order = visibility order). The migration deleted the property and
rebuilt it for only one consumer. This is the root of 5 of the 6 most
severe findings.

### 2. `ok` is not yet a restart-stable promise
Three findings are the same invariant broken three ways: state after an
acknowledged write must survive an abrupt stop unchanged. An acked write
can vanish (L2-F1), an acked winner can reverse (L3-F1), an acked FTS
write can be invisible while its ledger twin is visible (L4/L5-F2).
Restart equivalence was never made a standing tested invariant of the new
write path — the audits manufactured those tests and immediately found
all three.

### 3. Caches treat monotone stamps as completeness proofs
A repeated conceptual flaw, not an accident — four instances of the same
class: `Stamp =< QuerySeq` accepted as proof a shard row is complete
(L5-F3: a raced cold fill installs a stale row *permanently*, since
row-absent advances no-op); frontier repair rewrites cache contents
without bumping the generation the result cache keys on (L5-F2b);
runner result-cache keys resolved at construction, not invocation
(L5-F4); BM25 reads TF from capped positions while doc_length keeps the
true count (L5-F5 — violating the contract we ourselves wrote in
FTS.md:251). Notably the value cache got this right (SQN-keyed, provably
fresh); the discipline was not propagated to the FTS caches.

### 4. Two clocks where there should be one
`fts_seq` is an in-memory counter allocated at intent time, stamped into
durable frames, and re-seeded from a *different* clock (journal SQN) at
restart — so an abandoned intent lets a post-restart write reuse a
durably-stamped sequence and resurrect superseded terms (L2-F3). Any
derived clock that can drift from the durable clock eventually lies.

### 5. Accept-time validation weaker than format capacity
A 256-column schema is accepted, the write is acked, and the persisted
payload is structurally unreadable at rest (L4/L5-F1): schema validation
doesn't know the format's 8-bit widths and the encode sites have no
guards. The codebase already contains the correct pattern (the 6c9fb2a
frame guards); it was not applied uniformly.

### 6. Parity claims asserted, not enforced
SQLite FTS5 is the campaign's correctness oracle, but parity was enforced
by comment rather than test at the edges: `remove_diacritics` modes 1 and
2 silently share one implementation that matches neither (L4-F3); the
unicode fallback tokenizer concatenates across malformed UTF-8 while the
fast path and SQLite split — directly under a comment claiming the paths
are byte-identical (L4-F4).

### 7. Failure paths and mode matrices under-tested
New API surface got happy-path coverage; the edges didn't: valuecache
persistent_term registered before failure-prone init steps leaks on init
failure/kill (L1-F2); `mhead` guards on `head_only == false` while its
singular twin guards on `head_lookup == true` (L1-F1); the consolidated-
base fetch degrades to not_found without the retry the public GET path
has (L5-susp).

## Structural response (beyond the 16-item fix list)

1. Write down the three-phase protocol's invariants explicitly: what is
   guaranteed at each phase boundary and which consumer may rely on what.
   Every finding in area 1 is a consumer relying on an unwritten
   guarantee.
2. Make restart equivalence a standing harness: interleave acked
   operations, kill, replay, compare — the audits' repros become the
   seed corpus.
3. One clock: derived sequence spaces must be recoverable from the
   durable clock, or be the durable clock.
4. Cache admission rule: no cache row is served without a validity token
   derived from the durable clock (epoch/SQN); a self-reported stamp is
   never a completeness proof.
5. Oracle at the boundary: SQLite differential tests belong at the edge
   cases (invalid UTF-8, diacritic modes, cap boundaries), not only on
   the happy corpus.

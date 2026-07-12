# SQN Engine Redesign — one clock, one gate, one pipeline

Status: SUPERSEDED by docs/STOCK_PLUS.md (2026-07-12). The caller-side
write protocol this document fences is REMOVED instead: writes return to
stock in-Bookie semantics and the ordering failure classes die with the
window itself. Law 1 (one clock) and Laws 4–5 (cache admission, boundary
guards, tokenizer parity) carry over into STOCK_PLUS.md §2/§5. Kept for
design history only.

Original status line: DESIGN AUTHORITY for the post-audit rebuild (2026-07-12).
Supersedes the write/visibility/FTS-sequencing portions of TARGET_API.md;
TARGET_API.md will be updated to match as phases land. Motivated by the
5-layer correctness audit (leveled_audit/REVIEW.md) and its synthesis
(leveled_audit/QUALITY_SYNTHESIS.md): the confirmed defects are not 16
independent bugs but 7 problem classes, of which the 4 severe ones share
one root — ordering and visibility invariants that the caller-side
migration deleted and only partially rebuilt.

## Design goals

1. Eliminate the problem *classes*, not instances.
2. Maximally unify: every write path shares one absorption pipeline; every
   sequence-like value is the journal SQN; every cache obeys one admission
   law.
3. No happy-path performance regression. (The design removes one mailbox
   round-trip from FTS writes and adds zero work to gap-free operation.)

## The five laws

### Law 1 — One clock: the journal SQN is the only sequence space

`fts_seq` (the in-memory counter allocated by `{fts_put_intent}`) is
deleted. Everything that needs a version, stamp, generation, or epoch uses
journal SQNs:

- A posting frame's liveness stamp is the SQN of the write that produced
  it. Fresh-delta journal payloads are seq-free; the ledger-change
  derivation (caller post-append, and replay — both of which hold the
  record's SQN) injects it. Consolidated bases persist per-frame *origin*
  SQNs explicitly (the same slots the format has today, with unambiguous
  semantics).
- The doc marker references the SQN of its current version. Marker and
  frames ride the same batch, so equality is exact.
- Restart seeding is the journal high-water mark — now correct by
  construction, because no sequence can exist above it. (Kills L2-F3:
  there is no counter to drift, no abandoned reservation to collide.)
- `{fts_put_intent}` is deleted entirely: its only dynamic reply was the
  seq. FTS writers use the static `{write_refs}` like plain writers. One
  fewer Bookie round-trip per FTS batch.

### Law 2 — One gate: visibility is the contiguous SQN prefix

The absorption engine becomes the *only* way any write becomes visible,
and it applies writes strictly in SQN order:

- Every write path produces `{absorb, SQN, Changes, Advance, ReplyTo}`.
  Nothing inserts into the ledger cache on arrival. Changes are held in
  the pending tree and applied only when their SQN joins the contiguous
  frontier (`drain` applies pending SQNs in order).
- The reply to the writer is issued at application time. `ok` therefore
  means: durable, visible, restart-stable, and ordered after every lower
  acknowledged SQN. In gap-free operation the arriving SQN is contiguous
  and the reply happens in the same callback as today — zero added
  latency. Only a real gap defers acks, bounded by the gap timeout.
- Live state is always a *prefix* of replay state. Restart equivalence
  holds by construction. (Kills L2-F2/L3-F1 visibility reversal and the
  entire live-vs-replay divergence class.)
- The Penciller push watermark needs no separate gating: the ledger cache
  never contains an unabsorbed SQN, so `MinSQN >= LedgerSQN` holds
  invariantly. `leveled_pmem:add_to_cache` gets an explicit descriptive
  error clause for the impossible case rather than an `if_clause`.

Gap policy (abandoned writers):

- A gap older than the timeout is *voided*: the Inker appends the SQN to a
  small fsynced side-file (`journal_voids`) BEFORE the frontier skips it.
  Replay loads the void set and excludes those records; compaction scores
  voided records as non-current. A voided SQN was never acknowledged (its
  publish was never processed), so excluding it is always permitted.
- A publish arriving for a voided/skipped SQN gets a loud
  `{error, publish_expired}`; the caller-side wrappers fall back to the
  direct path, re-appending at a fresh SQN. No silent absorption, no
  silent loss. (Kills L2-F1 data loss and L2-F4 crash: an expired publish
  inserts nothing.)

### Law 3 — Deterministic CAS: conditions ride the log and are evaluated at the gate

CAS becomes a deterministic state-machine command:

- `casbatchput` normalises, optimistically pre-checks (fail-fast without a
  journal write — pure optimisation), then appends a journal record that
  carries the batch AND its conditions, and hands `{absorb, SQN, ...}` to
  the engine like every other write.
- The *authoritative* condition evaluation happens at absorption, when the
  state is exactly the complete prefix of SQNs below the CAS record. Pass:
  apply changes, reply `ok`. Fail: apply nothing, reply
  `{error, {precondition_failed, Failures}}`.
- Replay re-evaluates the persisted conditions at the same point in the
  same order against the same prefix (voids excluded on both sides), so
  live and recovered verdicts are identical by construction. (Kills
  L3-F1: there is no window in which a CAS can linearize across an unseen
  lower SQN — the evaluation point IS its SQN position.)

### Law 4 — One cache-admission law: no row without a frontier-derived token

A cache row may be served only with a validity token derived from the
absorbed frontier, never from a self-reported stamp:

- Write-through bumps happen at absorption (the single visibility site),
  including the row-absent case: an absorbed write touching an uncached
  shard bumps that shard's epoch row so a concurrent cold fill cannot
  install a pre-write snapshot. Cold fills record the epoch before
  folding and install only if it is unchanged. (Kills L5-F3
  permanently-stale fills and L4/L5-F2a stamp trust.)
- The FTS result cache keys on the absorbed frontier at *invocation* time
  (Law 1 makes the frontier the FTS clock). Frontier voids need no repair
  invalidation: a voided SQN was never acknowledged and never applied, so
  caches that exclude it are correct. (Kills L5-F2b and L5-F4.)
- The value cache already follows this law (SQN-keyed); its lifecycle is
  corrected under Law 5.

### Law 5 — Boundaries are derived, guarded, and oracle-tested

- Accept ⊆ encode: format capacities (column count ≤ 255, key/token/
  position byte bounds) are exported by the codec and consumed by schema
  validation; every fixed-width encode site guards loudly
  (`{frame_field_overflow, ...}` family). (Kills L4/L5-F1.)
- Ranking inputs are consistent: the true occurrence count is persisted
  alongside capped positions; BM25 TF and doc_length come from the same
  count. (Kills L5-F5, restores the FTS.md:251 contract.)
- Tokenizer parity is enforced, not asserted: one token-boundary
  definition shared by the fast and unicode paths (malformed UTF-8 is a
  boundary), SQLite-semantics `remove_diacritics` modes 1/2, and a
  standing SQLite FTS5 differential CT suite over the boundary corpus.
  (Kills L4-F3/L4-F4.)
- Mode matrix by construction: head-capability guards derive from one
  predicate so `mhead`/`head`/`head_sqn` cannot diverge (L1-F1); the
  value cache registers its persistent_term only after failure-prone init
  and sweeps dead-pid entries at startup (L1-F2); the consolidated-base
  fetch gets the same direct-read fallback as the public GET path
  (L5 suspicion); `book_mhead` documents its deliberate omission of the
  journal_notfound probe; TARGET_API.md's CAS failure shape is corrected
  to `{error, {precondition_failed, Failures}}`.

## Unified write pipeline (all paths)

```
caller-side put:    write_refs → ink_put (caller IO) → {publish, SQN, Changes}
caller-side batch:  write_refs → derive (seq-free) → ink_batchput → {publish_fts, SQN, Changes, Advance}
direct put/batch:   handle_call → ink append → internal absorb event
CAS:                handle_call → pre-check → ink append (with conditions) → internal absorb event
head_only mput:     handle_call → ink_mput → internal absorb event

            ALL of the above feed:

absorb(SQN, Changes, Advance, Validation, ReplyTo)
  → pending tree → drain in SQN order:
      validate (CAS only; deterministic, replay-identical)
      → addto_ledgercache (single insert site)
      → apply FTS advance + bump shard epochs (single cache-write site)
      → reply(ReplyTo)
  frontier := last drained SQN     %% THE clock for caches, queries, FTS
  gap timeout → void (durable) → skip → late publish ⇒ publish_expired
```

Properties: visibility order == SQN order == replay order; one insert
site; one cache-bump site; one clock; acks are restart-stable; CAS is
deterministic. Each property holds by construction, not by audit.

## Phasing (each phase lands with tests green)

- **A. Absorption engine**: pending-tree unification of publish /
  publish_fts / direct / mput; deferred replies; void file + expired
  publishes; pmem descriptive error.
- **B. Deterministic CAS**: conditions in the journal record; absorb-time
  validation; replay re-evaluation.
- **C. One clock**: delete fts_seq + {fts_put_intent}; SQN stamping via
  change derivation; consolidated-base origin SQNs; replay derivation.
- **D. Cache admission law**: shard epochs (incl. row-absent), result
  cache on invocation-time frontier, cold-fill epoch validation.
- **E. Boundary guards**: codec-exported capacities, schema bound, encode
  guards, true occurrence counts for BM25.
- **F. Tokenizer parity**: shared boundary definition, diacritics modes,
  SQLite differential CT suite.
- **G. Mode matrix + lifecycle**: head-capability predicate, valuecache
  lifecycle, base-fetch fallback, documentation corrections.

Verification gates for the whole redesign: every audit repro in
leveled_audit/ must flip to non-reproduction; the full eunit + CT suites
pass; the audit's positive controls still pass; indexing/search benchmarks
show no regression (the FTS intent-call removal should show a small
improvement); then a fresh 5-layer adversarial audit round runs against
the rebuilt tree.

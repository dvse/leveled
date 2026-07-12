# Leveled write-path reliability regression review

Target: `/Users/dvse/projects/agents/leveled` at the checked-out HEAD. The target tree was read only. Reproductions use the supplied prebuilt beams and keep all data under this audit directory.

## 1. The five-second gap timeout can advance a persisted watermark past a live writer and lose its acknowledged write on restart

Severity: **data-integrity**.

Reproduction: `./repro_persisted_watermark_loses_late_publish.escript` (exit 0).

Invariant: the persisted ledger watermark must never advance past a journal sequence that can still be acknowledged and published by a live writer. After `PUBLISH` returns `ok`, an abrupt restart must recover the same current value.

Expected versus actual:

```text
delayed durable SQN: 1
persisted ledger watermark before late PUBLISH: 835
late PUBLISH returned: ok
live read before abrupt stop: {ok,delayed_value}
expected after restart:      {ok,delayed_value}
actual after restart:        not_found
```

Mechanism: the plain caller-side path has no Bookie-side `put_intent` registration. It obtains static write references and appends directly through `ink_put`, then contacts the Bookie only at `PUBLISH` ([leveled_bookie.erl:618](/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl:618), [leveled_bookie.erl:648](/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl:648)). A later publish opens a gap, and after 5 seconds `maybe_gated_push/2` assumes the missing sequence was abandoned and jumps the frontier to the smallest pending sequence ([leveled_bookie.erl:134](/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl:134), [leveled_bookie.erl:3864](/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl:3864), [leveled_bookie.erl:3878](/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl:3878)).

When the still-live writer eventually publishes its lower sequence, the Bookie first inserts its ledger row and can return `ok`, but `absorb_sqn/3` treats the sequence as already covered because it is below the frontier ([leveled_bookie.erl:2321](/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl:2321), [leveled_bookie.erl:3822](/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl:3822)). After an abrupt stop, startup reads the persisted ledger sequence and replays the journal only from `LedgerSQN + 1`, permanently skipping the acknowledged lower record ([leveled_bookie.erl:3040](/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl:3040), [leveled_bookie.erl:3055](/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl:3055)).

## 2. Reordered plain caller-side publishes overwrite the current same-key ledger row with an older journal sequence

Severity: **wrong-result**.

Reproduction: `./repro_reordered_publish_reverts.escript` (exit 0). This uses only public `book_put` calls for the two writers and does not use CAS.

Invariant: concurrent writes to one key must leave the live ledger at the same journal-sequence winner that replay will rebuild. Publishing a lower sequence after a higher sequence must not make the lower value current.

Expected versus actual:

```text
newer writer returned ok and read: {ok,second}
after delayed older writer returned ok: {ok,slow_first}
expected after restart (journal SQN order): {ok,second}
actual after restart:                       {ok,second}
```

Mechanism: `handle_call({publish, ...})` inserts the arriving changes into the ledger-cache ETS table before it passes the sequence through the frontier ([leveled_bookie.erl:2321](/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl:2321)). `addto_ledgercache/2` uses unconditional `ets:insert`, so a later-arriving lower sequence replaces the already-inserted higher same-key row ([leveled_bookie.erl:4829](/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl:4829)). The pending tree stores only sequence-to-FTS-advance metadata, not the ledger changes; when the missing lower sequence arrives, `drain_pending/1` advances through the higher sequence but cannot reapply its overwritten row ([leveled_bookie.erl:3825](/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl:3825), [leveled_bookie.erl:3837](/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl:3837)). Journal replay remains sequence ordered and restores `second`, exposing the live/restart disagreement.

The batch caller path has the same ordering exposure: `book_mput_std` writes through caller-side `ink_batchput` and publishes through `publish_fts` ([leveled_bookie.erl:925](/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl:925)); that clause also inserts every ledger change before calling `absorb_sqn/3` ([leveled_bookie.erl:2360](/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl:2360)). The reproduction directly proves the single-put path; batch same-key reordering was code-inspected but not separately reproduced.

## 3. Restart can reuse an FTS write sequence after an abandoned intent, leaving superseded terms live

Severity: **wrong-result**.

Reproduction: `./repro_fts_abandoned_intent_seq_reuse.escript` (exit 0).

Invariant: every durable FTS version of a document must have a sequence distinct from every earlier durable version. Updating a document from `alpha` to `beta` must make the old `alpha` posting non-current before and after restart.

Expected versus actual:

```text
abandoned FTS sequence: 1
journal SQN after first durable write: 1
canonical object after update: {ok,#{body => <<"beta">>}}
expected alpha hits after update: []
actual alpha hits after update:   [<<"doc">>]
expected beta hits after update: [<<"doc">>]
actual beta hits after update:   [<<"doc">>]
```

Mechanism: `{fts_put_intent}` increments an in-memory counter before any journal append ([leveled_bookie.erl:2348](/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl:2348)). If that intent is abandoned, the counter is ahead of the journal sequence. Startup nevertheless seeds `fts_seq` solely from the journal high-water mark ([leveled_bookie.erl:2087](/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl:2087)). The next intent after restart can therefore reuse the sequence stamped into the previous durable document's marker and posting frames. FTS derives the marker from that sequence ([leveled_fts.erl:397](/Users/dvse/projects/agents/leveled/src/leveled_fts.erl:397)), and query liveness accepts a posting exactly when its frame sequence equals the marker sequence ([leveled_fts.erl:1344](/Users/dvse/projects/agents/leveled/src/leveled_fts.erl:1344)). Both old `alpha` and new `beta` frames consequently look current.

## 4. A late lower publish after the timeout can revert the live value and crash the Penciller on a later cache push

Severity: **process-failure** (preceded by a transient wrong current value).

Reproduction: `./repro_gap_timeout_watermark_loss.escript` (exit 0).

Invariant: the timeout must not classify a slow but live writer as abandoned. A lower sequence that publishes after the frontier has advanced must neither overwrite a higher current row nor enter a cache range below the Penciller watermark.

Expected versus actual:

```text
delayed SQN: 1
Penciller push watermark after timeout skip: 283
newer write returned ok and initially read: {ok,second}
after slow live caller finally publishes: {ok,first}
Bookie/Penciller failure observed: {down,{if_clause,...leveled_pmem.erl line 142...}}
expected after restart (SQN order/direct oracle): {ok,second}
actual after restart:                         {ok,second}
```

Mechanism: the timeout force-advances the frontier as described in finding 1. The late lower publish is still unconditionally inserted into the ledger cache before `absorb_sqn/3` ignores its already-covered sequence ([leveled_bookie.erl:2328](/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl:2328), [leveled_bookie.erl:3822](/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl:3822)). A later push carries a `MinSQN` below the Penciller's existing `LedgerSQN`. `leveled_pmem:add_to_cache/5` has only the `MinSQN >= LedgerSQN` branch, so the violated ordering assumption raises `if_clause` ([leveled_pmem.erl:137](/Users/dvse/projects/agents/leveled/src/leveled_pmem.erl:137), [leveled_penciller.erl:776](/Users/dvse/projects/agents/leveled/src/leveled_penciller.erl:776)).

## Inherited reproduction status

| Reproduction | Result in this review |
|---|---|
| `repro_reordered_publish_reverts.escript` | Reproduced, exit 0. Live value reverted to the lower sequence; restart restored the higher sequence. |
| `repro_gap_timeout_watermark_loss.escript` | Reproduced, exit 0. Frontier/persisted watermark passed SQN 1, late publish reverted the live value, and a later push crashed the Bookie/Penciller. |
| `repro_compaction_drops_late_publish.escript` | **Did not reproduce**, exit 1. The supplied file was not executable and referenced `book_returnactors/1`, which HEAD source exports but the supplied beam does not. It was made runnable with the equivalent `return_actors` server call and executable bit. Its run reported persisted watermark `0`, compaction `{done,0}`, and `{ok,delayed_value}` both live and after restart. |
| `repro_fts_abandoned_intent_seq_reuse.escript` | Reproduced, exit 0. Both the superseded and current term matched after restart/update. |

## Unreproduced compaction concern

The source mechanism remains plausible but is not a finding. Compaction takes a ledger snapshot and a persisted-sequence cutoff ([leveled_bookie.erl:2697](/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl:2697), [leveled_inker.erl:514](/Users/dvse/projects/agents/leveled/src/leveled_inker.erl:514)). For a record at or below that cutoff, `to_retain/5` consults the snapshot and, under `retain`, converts a non-current full journal entry to key deltas, removing its body ([leveled_iclerk.erl:1018](/Users/dvse/projects/agents/leveled/src/leveled_iclerk.erl:1018)). A timeout-skipped, not-yet-published record could meet that classification after the watermark advances. However, the inherited probe's actual compaction selected no file, so it does not prove that interaction.

## Coverage notes

- Audited the plain caller write from `write_refs` through caller `ink_put`, pure ledger preparation, and `{publish, SQN, Changes}`. There is no plain `{put_intent, ...}` RESOLVE clause at HEAD; only the static `{write_refs}` call precedes `ink_put`. This absence is what prevents the gap timer from distinguishing a live delayed caller from an abandoned write.
- Audited `publish_frontier`, `publish_pending`, `publish_gap_since`, `absorb_sqns/2`, `absorb_sqn/3`, `drain_pending/1`, and `maybe_gated_push/2`, including cache insertion relative to the gate. Findings 1, 2, and 4 reproduce failures in this machinery.
- Audited process-dictionary caching of `write_refs`. Cached references are accepted only while the cached Inker PID is alive, otherwise refreshed; caught races erase the entry and use the direct path ([leveled_bookie.erl:797](/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl:797)). No independent stale-reference failure was reproduced.
- Audited `book_put_direct` and `book_mput_std_direct` as serialized reference paths. Direct-only calls cannot reorder their journal append and cache insertion because both occur in one Bookie callback. The caller-side `book_mput_std` uses the same unsafe cache-before-frontier order as single puts; no separate batch reordering reproduction was added.
- Audited `book_tempput` and `book_delete`. They are thin wrappers around `book_put` ([leveled_bookie.erl:526](/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl:526), [leveled_bookie.erl:1059](/Users/dvse/projects/agents/leveled/src/leveled_bookie.erl:1059)), so they inherit the caller-side failures. `book_put_direct` remains the direct reference API.
- Audited `ink_batchput` and CDB batch commit. The Inker assigns one sequence and calls `cdb_mput/3` once ([leveled_inker.erl:1345](/Users/dvse/projects/agents/leveled/src/leveled_inker.erl:1345), [leveled_inker.erl:1394](/Users/dvse/projects/agents/leveled/src/leveled_inker.erl:1394)). CDB constructs and size-checks the complete binary before one `file:pwrite`, then performs at most one `datasync` for the batch ([leveled_cdb.erl:1273](/Users/dvse/projects/agents/leveled/src/leveled_cdb.erl:1273), [leveled_cdb.erl:684](/Users/dvse/projects/agents/leveled/src/leveled_cdb.erl:684)). No additional group-commit durability failure was reproduced.
- Audited FTS caller intent, augmentation, marker stamping, publish, restart seeding, and query liveness. Finding 3 is the reproduced failure.
- Did not run `rebar3` or mutate/build inside the target tree. No full upstream/downstream suite was run. The compaction interaction and a standalone concurrent same-key `book_mput_std` reproduction remain unproven coverage gaps.

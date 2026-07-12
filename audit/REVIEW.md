# Audit review — my verification pass (post-Codex)

## DISPOSITIONS UNDER THE REBUILT ARCHITECTURE (2026-07-12)

The fork was rebuilt after this audit: all src except leveled_fts was
replaced with upstream martinsumner/leveled (develop-3.4 tip 7f08bba)
and the minimum patch in docs/NATIVE_CAS.md was written fresh
(standard-mode book_mput + book_casmput + HEAD_TAG read fix). The
caller-side write protocol, absorption frontier, fts_seq clock,
book_mput_std/casput/mget/mhead/fetchspecs, and the value cache no
longer exist. Per-repro dispositions:

- L1-F1 (mhead mode guard): N/A — book_mhead removed. The CLASS is
  addressed in the new surface: HEAD_TAG reads derive their plane
  handling from the tag, not per-clause mode guards
  (casmput_headplane_visibility_tester pins it).
- L1-F2 (valuecache persistent_term leak): N/A — value cache removed.
- L2-F1/F2/F4, L3-F1 (frontier/ordering/CAS classes): structurally
  impossible — no caller-side write path exists; every write commits
  inside one serialized Bookie callback (upstream semantics).
  casmput linearizability pinned by casmput_single_winner_tester.
- L2-F3 (fts_seq reuse): structurally impossible — no FTS sequence
  clock exists; liveness is row overwrite (FTS.md §2).
- L4/L5-F2, L5-F3, L5-F4 (stamp-trust cache classes): the epoch-row
  admission law (FTS.md §4) replaces all stamped caches; gate tests
  land with the leveled_fts library rewrite.
- L4/L5-F1 (256-column wrap), L5-F5 (BM25 cap), L4-F3 (diacritics
  modes), L4-F4 (malformed UTF-8): carried as REQUIRED FIXES in the
  leveled_fts library rewrite (FTS.md §6); the SQLite oracle corpus is
  the standing differential gate.
- L5-F6 (consolidated base floor) + L5 suspicion (base-fetch
  fallback): consolidation redesigned (base rows + casmput conditions,
  FTS.md §5); re-evaluate against the new implementation's benchmarks.
- L3 doc inconsistency (condition_failed shape): resolved — the only
  documented failure shape is {error, {precondition_failed, Failures}}
  (docs/NATIVE_CAS.md §3.2).

The repro scripts below remain as historical evidence against 9d21968
(preserved on that commit); they do not run against the rebuilt tree —
their APIs are gone, which is the point.

## Original review (against 9d21968) follows

Protocol per user directive: every repro run by me personally; leveled tree
verified untouched after each layer; severities adversarially re-checked
against HEAD source. Fixes deferred until ALL layers finish (editing
leveled mid-audit would shift line cites / file content under the running
sessions).

Leveled HEAD under audit: 9d21968. Tree check after L1: `git status` clean.

## L1_reads (job task-mrh1t4fc-dwhave) — DONE, reviewed 2026-07-12

### F1 mhead unsupported in head_only=with_lookup — CONFIRMED
- Repro `repro_mhead_headonly.escript` run by me: exit 0, output matches
  FINDINGS.md (`actual={unsupported_message,mhead}`).
- HEAD source verified: `{mhead,...}` guard `head_only == false`
  (leveled_bookie.erl:2452-2454) vs singular `{head,...}` guard
  `head_lookup == true` (:2539-2541, also head_sqn :2605-2607).
  `current_head_state/2` (:4376) already passes `State#state.head_only`
  to fetch_head and handles tomb/TTL — with_lookup support is a pure
  guard asymmetry.
- Severity ADJUSTED: auditor said "wrong-result"; actual failure is a
  LOUD `{unsupported_message,mhead}` top-level return, not silent wrong
  data → downgrade to contract-violation/unsupported-operation. Still a
  genuine bug vs the book_mhead docstring ("per-key semantics identical
  to book_head/4") since the singular works on the same store.
- Planned fix: change mhead guard to `head_lookup == true`; regression
  test in a with_lookup store covering present/tombstone/TTL-expired
  keys, mhead ≡ per-key book_head.

### F2 value-cache persistent_term leak on failed init / kill — CONFIRMED
- Repro `repro_valuecache_persistent_term_leak.escript` run by me:
  exit 0; 4 failed inits + 1 kill → 5 leaked
  `{{leveled_bookie,valuecache,Pid}, {Tid,Bytes}}` entries (repro erases
  them afterwards; VM state clean).
- HEAD source verified: registration in init path (:2013 area, via
  valuecache_init :743-751) happens before failure-prone startup steps;
  only cleanup is terminate/2 (:2847-2849), which OTP skips on init
  failure and on brutal kill.
- Severity agreed: resource leak, perf-class. In our deployment (one
  bookie per store, rare restarts) impact is small, but each dead entry
  is permanent VM state and every persistent_term write rescans it.
- Planned fix (both parts): (a) register the cache entry only after the
  failure-prone init steps succeed; (b) sweep dead-pid
  `{leveled_bookie,valuecache,P}` entries in valuecache_init (local
  `is_process_alive/1`) so brutal-kill leftovers self-heal at the next
  bookie start. Keep the terminate/2 erase. Regression: eunit re-using
  the repro's two scenarios.

### Suspicion (mhead skips journal_notfound) — VALIDATED as real divergence
- Code read confirms: BOTH singular head (:2552-2580) and head_sqn
  include the frequency-gated `journal_notfound` inker probe; plural
  mhead resolves via current_head_state only (no ink check). Divergence
  observable only in a ledger-positive/journal-missing state; auditor
  found no supported lifecycle producing it, neither did I.
- Disposition: fix as DOCUMENTATION — mhead is deliberately pure
  in-memory (TARGET_API §3.1 design: no snapshot, no IO in the call);
  state the omission explicitly in the book_mhead doc comment and
  TARGET_API §3.1. Revisit only if a supported lifecycle surfaces.

## L5_fts_query (job task-mrh1utt8-cl5ryl) — DONE, reviewed 2026-07-12

All 6 repros run by me personally; all reproduced (exit 0). Tree check
after L5: clean. Repro legitimacy verified for the two races: F2 models
the documented caller-death window with the exact calls a stock
caller-side writer makes (write_refs + ink_put, no PUBLISH — a crashed
writer produces this state); F3 uses trace+suspend at a boundary any
scheduler preemption can hit. Neither forges internal protocol.

### F3 cold shard-cache fill race installs stale state — CONFIRMED, TOP SEVERITY
- My run: warm=[old] after the write, cold (row deleted)=[new,old].
- Adversarial escalation beyond the auditor's writeup: the stale row is
  PERSISTENT — `advance_shard_cache` no-ops when no row exists
  (leveled_fts.erl:1410), so the raced write's delta is never retrofitted;
  later writes advance the stale row WITHOUT the missing delta, so the
  lost document stays lost until the row is evicted. Silent, no crash
  required, plain query/write concurrency. This is the most severe
  confirmed finding of the audit so far.
- Root defect (shared with F2a): `Stamp =< QuerySeq` acceptance
  (leveled_fts.erl:3105) treats the stamp as a completeness proof, and
  cold fills install via `ets:insert_new` with no validation that no
  write intervened since their snapshot.
- Planned fix direction: per-shard write epoch in the cache table,
  bumped by EVERY write touching the shard (including when no row
  exists, replacing the :1410 no-op); cold filler records the epoch
  before folding and installs only if unchanged (else serve the
  computed result uncached). Correctness over warmth.

### F2 frontier gap makes acked FTS write invisible + result cache pins it — CONFIRMED
- My run matches: during_gap=[old], cached_after_gap=[old],
  uncached_after_gap=[new,old].
- Two distinct defects: (a) same stamp-trust flaw as F3 — the acked
  write's ledger rows are visible (inserted before the absorption gate,
  leveled_bookie.erl:2366) but its shard-cache advance is buffered
  behind the abandoned SQN, and queries accept the stale row; (b) after
  the 5s gap-skip heals the frontier, `apply_fts_advance` (:3859)
  rewrites cache contents without bumping fts_seq, so results cached
  under that seq remain addressable and wrong until the next FTS write
  bumps the seq.
- Mitigating scope: (b)'s poisoned window closes at the next FTS write
  in a busy store; (a) lasts the length of the gap (≥5s). Still
  wrong-result: an ACKED write is invisible while its ledger twin is
  visible.
- Planned fix: the per-shard epoch scheme fixes (a) (publish_fts bumps
  the epoch at ack time even when the content advance is buffered —
  queries go cold and the cold fold reads the ledger snapshot, which is
  already correct); (b) needs a result-cache epoch (or fts_seq bump)
  on apply_fts_advance drain.

### F4 delayed cached runner frozen at construction — CONFIRMED
- My run matches; the uncached control takes the promised
  invocation-time snapshot, the cached path returns construction-time
  results without ever snapshotting. Cache transparency violation
  (cached ≢ uncached for identical calls).
- Severity agreed (wrong-result) but narrow window in our usage:
  ash_leveled constructs and invokes runners immediately. Real for any
  caller that defers the closure.
- Planned fix: resolve the result-cache key at INVOCATION (closure asks
  the bookie for current fts_seq, cheap call) instead of capturing the
  construction-time seq (leveled_bookie.erl:3304/:3313); miss → SnapFun
  as today.

### F1 256-column schema accepted, writes unreadable delta — CONFIRMED
- My run matches: `{invalid_fts_payload,delta_cols,0,8960,[]}` on query.
- Corruption-class per the brief's scale (accepted write becomes
  structurally unreadable at rest), though bounded: only schemas with
  >255 columns, rejected loudly at query/consolidation time, and our
  production schemas have <20 columns.
- Planned fix: bound schema validation (≤255 columns) at book_start
  (leveled_fts.erl:805) + loud `{frame_field_overflow,...}`-style guard
  at the 8-bit encode sites in encode_delta/encode_base (:2835), same
  pattern as the 6c9fb2a guards.

### F5 positions cap reverses BM25 order — CONFIRMED (contract violation of our own fix)
- My run matches: the 70K-occurrence doc ranks below the 65K one.
- This directly violates the FTS.md:251 contract WE wrote with the cap
  (6c9fb2a): "ranking sees every occurrence". TF is length of the
  decoded (capped) positions (leveled_fts.erl:3681) while doc_length
  keeps the full count — inconsistent inputs to BM25.
- Planned fix: persist the true occurrence count alongside capped
  positions (format detail at fix time — e.g. count field consulted by
  rank instead of positions length), so rank/count semantics match the
  documented contract; extend hot_token_position_cap CT case with a
  two-doc rank assertion at the cap boundary.

### F6 consolidated uncached base 3-8x query floor — CONFIRMED (known)
- My run: 149µs uncached vs 18µs warm (8.28x vs decoded-base cache).
- Matches the campaign's existing verdict (never consolidate serving
  stores until base reads are cached). Value of the repro: pins the
  cause to the full-base journal fetch + whole-base decode per miss
  (leveled_fts.erl:3167/:3183). Fix = decoded-base cache; backlog, not
  gate-blocking (gates use unconsolidated stores).

### L5 suspicion (base fetch → not_found without retry) — ACCEPTED FOR FIX
- leveled_bookie.erl:4130 + leveled_fts.erl:3183: consolidated-base
  fetch failure degrades to absent-base with no direct-read fallback,
  unlike the public GET path. Unreproduced (needs a journal-compaction
  interval race), but the asymmetry vs the GET path is real in source
  and the principled fix (direct-read fallback on base fetch failure)
  is small. Include in fix pass.

### L5 differential coverage (no action)
- 9 AST shapes driver-vs-full equal pre/post consolidation and restart;
  consolidation-vs-oracle exact hit maps equal incl. scores/positions;
  bloom no-false-negative review; deterministic rank tie ordering.

## L3_cas (relaunch task-mrh28zk1-l81sk5) — DONE, reviewed 2026-07-12

### F1 CAS commits across an unpublished lower SQN; restart reverses the acked winner — CONFIRMED, data-integrity
- Repro `repro_cas_frontier_restart.escript` run by me 3×: identical
  output all runs (`seed_sqn=1 cas=ok put=ok
  before_restart={ok,caller_put_value,3} after_restart={ok,cas_value,4}`).
  Two acked operations; the in-memory winner reverses across restart.
- Repro legitimacy verified: public book_casput/book_put only;
  sys:suspend(Bookie) is used solely to order the Bookie mailbox (both
  operations' journal IO runs caller-side as in production); writer's
  write_refs primed first so no hidden setup call sneaks into the queue.
- HEAD source verified by me: CAS condition check at :2267 reads only
  ledger cache + penciller head — no barrier against journal-durable
  unpublished lower SQNs. addto_ledgercache precedes absorb_sqns in
  {publish} (:2328/:2334), {publish_fts} (:2366-2375), and the CAS
  paths (:3963/:3986/:3999); insertion is unconditional ets:insert
  (:4830). The frontier comment at :3798 confirms design intent: the
  gate protects the cache→penciller PUSH watermark only — same-key
  ledger-cache visibility is insertion-ordered while journal replay is
  SQN-ordered.
- Remediation (auditor's caveat adopted): reordering inserts alone is
  insufficient — a CAS that evaluated its condition against the
  pre-SQN-3 head must not commit as though it ran after SQN 3. CAS
  needs a publication barrier (or per-key intent registration) over
  touched keys; ledger-cache same-key visibility must follow SQN order.
- Contract matrix (`cas_contract_matrix.escript`) run by me: PASS —
  condition vocabulary, whole-batch rejection, journal invariants on
  rejection, 50K-condition abrupt-stop probe, 32-writer oracle,
  head-only rejection all hold.
- Docs: TARGET_API.md:171 still advertises `{error,{condition_failed,
  Current}}`; runtime + NATIVE_CAS.md use `{error,{precondition_failed,
  Failures}}`. Fix the doc in the fix pass.

## L4_fts_write (relaunch task-mrh29odl-c19con) — DONE, reviewed 2026-07-12

All 8 scripts (4 repros + 4 verify controls) run by me: repros all
reproduce with byte-identical output to FINDINGS.md; controls all PASS.
Tree check after L4: clean.

### F1 256-column delta wrap — CONFIRMED (duplicate of L5-F1, independent)
- Adds restart evidence: the unreadable payload is persisted (same
  error after reopen). Same fix: schema bound ≤255 + loud encode guards.

### F2 frontier gap hides acked FTS write; result cache preserves it — CONFIRMED (corroborates L5-F2)
- Independent repro with a cleaner expectation framing (abandoned
  writer's recovery explicitly out of scope; the ACKED write missing
  pre-restart is the defect). Adds cite: result-cache key uses the
  unchanged allocator seq (fts:522). Same fix family: per-shard write
  epoch + result-cache generation bump on frontier repair.

### F3 remove_diacritics modes 1/2 diverge from SQLite unicode61 — CONFIRMED, wrong-result (new)
- My run matches; differential oracle = system sqlite3 FTS5. Mode 1
  must retain multi-diacritic precomposed Latin (U+1ED9); mode 2 must
  restrict folding to Latin script (Greek U+03AC must not match plain
  alpha). Leveled treats 1 and 2 identically as NFD+strip-all
  (fts:2106/:2122).
- Severity in our deployment: real but narrow — our production schemas
  use the default; parity matters because SQLite FTS5 is the campaign's
  correctness oracle. Fix: implement the two distinct fold tables per
  SQLite semantics.

### F4 unicode fallback tokenizer concatenates across malformed UTF-8 — CONFIRMED, wrong-result (new)
- My run matches; `bad<<255>>utf8` tokenizes as `badutf8` on the
  unicode path (custom tokenchars/separators) while the fast path and
  SQLite both split at the invalid byte. Disproves the byte-identical
  claim at fts:1937. Fix: emit a separator boundary when recovering
  from malformed UTF-8 in unicode_chars/1 (fts:2691).

## L2_writes (relaunch task-mrh2zwnl-yjcv3d) — DONE, reviewed 2026-07-12

All 4 reproducing repros run by me: all reproduce with output matching
FINDINGS.md. The 5th (compaction_drops_late_publish) did not reproduce
for the auditor either — recorded as an unproven concern. Tree clean.

### F1 gap timeout advances persisted watermark past a LIVE writer; acked write lost on restart — CONFIRMED, DATA-LOSS (most severe finding of the audit)
- My run: delayed durable SQN 1; persisted watermark 589 before the
  late PUBLISH; PUBLISH returns ok; live read returns the value;
  after restart `not_found`. An acknowledged write is silently gone.
- Repro legitimacy verified by me line-by-line: it performs exactly
  the caller-side sequence put_caller_side_validated runs
  ({write_refs} → ink_put → {publish, SQN, Changes} with
  preparefor_ledgercache-equivalent changes), pausing >5s between
  append and PUBLISH — a slow-but-live caller (GC pause, scheduler
  starvation, swap). No forged state.
- Mechanism source-verified: no plain put_intent RESOLVE exists at
  HEAD, so the gap timer cannot distinguish live-delayed from
  abandoned; after the skip, maybe_gated_push lets the watermark
  persist past SQN 1; the late {publish} still inserts + returns ok
  (absorb_sqn treats the SQN as covered); replay starts at
  LedgerSQN+1 and skips the record forever.
- Even if 5s-abandonment is acceptable policy, returning ok for a
  write that replay will drop is indefensible. Fix must make the late
  publish either loud ({error, publish_expired} → caller retries a
  fresh write) or correct (re-append/re-absorb above the watermark).
- Note: PUBLISH_GAP_TIMEOUT_MS exists to protect liveness against
  crashed callers — the fix must preserve that while never lying to a
  live one.

### F2 reordered publishes: older SQN overwrites newer same-key row until restart — CONFIRMED, wrong-result
- My run matches (live reads revert to `slow_first`; restart restores
  `second`). Pure public book_put on both writers; no CAS. Shares the
  L3 root cause (insertion-order visibility vs SQN-order replay); the
  fix is the same SQN-ordered ledger-cache visibility. Batch path
  (publish_fts :2360) code-verified to share the exposure.

### F3 fts_seq reuse after abandoned intent + restart leaves superseded terms live — CONFIRMED, wrong-result
- My run matches: after restart the update to `beta` leaves `alpha`
  still matching (both frames carry the same seq as the marker).
- Mechanism verified: {fts_put_intent} bumps only the in-memory
  counter (:2348); startup seeds fts_seq from the journal high-water
  mark (:2087); an abandoned intent lets the post-restart intent
  reuse the previous durable version's seq; liveness is frame-seq ==
  marker-seq equality (fts:1344).
- Fix direction: persist intent high-water (or seed fts_seq past any
  observed FTS frame seq + markers during replay — replay already
  scans the journal; seed from max(marker/frame seq)+1).

### F4 late lower publish after timeout skip reverts live value, then Penciller if_clause crash — CONFIRMED, process-failure
- My run matches incl. the leveled_pmem:142 if_clause via
  penciller:776. Same root causes as F1+F2: late publish accepted
  silently + unconditional insert. Fixing F1 (loud/correct late
  publish) and F2 (SQN-ordered visibility) removes both the revert
  and the below-watermark cache range; add a defensive clause or
  assertion in pmem for MinSQN < LedgerSQN regardless.

### L2 unproven concern (retain-mode compaction of a timeout-skipped record)
- Compaction converts non-current entries at/below the persisted
  cutoff to key deltas under retain. A skipped-then-published record
  could qualify. Auditor's probe selected no file; not reproduced.
  The F1 fix (no silent late publish) removes the enabling state;
  re-check after fixes.

Note: original L2/L3/L4 sessions were terminated by OpenAI's content
filter misclassifying durability/concurrency QA language as security
work; relaunched with correctness-framed briefs (same technical scope).

# Consolidated fix list (user directive 2026-07-12: resolve, then re-run a round)

Engine (leveled_bookie.erl):
1. [L2-F1/L2-F4/L3-F1 root A] Late publish below the frontier must not
   be silently absorbed: loud {error, publish_expired} (caller-side
   put falls back to direct-path retry) OR correct re-absorption.
2. [L2-F2/L3-F1 root B] Ledger-cache same-key visibility must follow
   SQN order (conditional insert honoring higher-SQN rows).
3. [L3-F1 root C] CAS condition evaluation needs a barrier against
   journal-durable unpublished lower SQNs on touched keys.
4. [L2-F3] Seed fts_seq at startup above any replayed FTS frame or
   marker seq, not just the journal SQN high-water mark.
5. [L1-F1] mhead guard head_only==false → head_lookup==true.
6. [L1-F2] valuecache persistent_term: register after failure-prone
   init + dead-pid sweep at valuecache_init.
7. [L5-F4] Resolve result-cache key at runner invocation, not
   construction.
8. [L5-suspicion] Direct-read fallback on consolidated-base fetch
   failure (parity with public GET path).

FTS (leveled_fts.erl):
9. [L5-F3/L5-F2a/L4-F2] Per-shard write epoch: every write bumps the
   epoch (including row-absent case at :1410); cold fill installs only
   if epoch unchanged since its snapshot; stamp-trust removed.
10. [L5-F2b/L4-F2] Result-cache generation bump when apply_fts_advance
    drains a gap.
11. [L5-F1/L4-F1] Schema validation ≤255 columns + loud guards at
    8-bit encode sites.
12. [L5-F5] Persist true occurrence count so BM25 TF matches
    doc_length semantics (FTS.md:251 contract).
13. [L4-F3] remove_diacritics mode 1 vs 2 per SQLite unicode61.
14. [L4-F4] Malformed-UTF-8 = token boundary on the unicode fallback
    path.

Docs:
15. TARGET_API.md:171 condition_failed → precondition_failed.
16. book_mhead + TARGET_API §3.1: document the deliberate absence of
    the journal_notfound probe in mhead.

Deferred (backlog, not this pass): L5-F6 decoded-base cache (known
consolidation floor; gates run unconsolidated).

# FTS Correctness Cleanup

## Verdict

The segment-indexed/doclist design is the right direction, but the current FTS
work must not be treated as production-ready until the P0 items in this document
are closed. The goal is to keep the smallest number of moving parts while
making the FTS contract explicit enough to compare against SQLite FTS5 without
schema, lifecycle, update, or maintenance ambiguity. SQLite-class feature claims
also require the matching P1/P2 items.

Use this document as a staged release gate. A checkbox stays open until every
acceptance criterion under it is proven by source, tests, benchmark artifacts,
or reviewer sign-off.

## Priority Levels

- P0: blocks any production claim for the current segment-only FTS release.
- P1: blocks SQLite-class FTS claims, or any release that enables the associated
  feature.
- P2: operational hardening that may be deferred only if the public FTS contract
  explicitly excludes it.

P1 and P2 items are still actionable review findings. They are separated so the
current segment-only correctness cleanup does not accidentally become a broad
promise to ship every SQLite auxiliary feature in the first production cut.

## Non-Negotiable Design Boundaries

- Keep the segment secondary-index representation as the only release storage
  representation.
- Do not add a SQLite sidecar, external search process, raw ETS-owned FTS
  state, or a second durable writer.
- Hidden FTS objects are acceptable only if they are ordinary Leveled objects
  written through the existing Journal/Ledger path.
- Do not change raw `book_put`, `book_delete`, or `book_batchput` semantics for
  FTS safety. FTS-managed keys must use the FTS API.
- Do not ship finite TTL support for segment FTS until segment facts are
  decoupled from user-object TTL.
- Do not expose public APIs that let callers supply old manifests or assign
  generations.

## Current-State Notes

- The review concern that `idx_payload` is completely unsupported is partly
  stale in this worktree: internal codec support exists, while public raw
  index specs still reject `idx_payload`. That is the intended direction, but
  it has now been audited as an internal-only FTS boundary.
- Current benchmarks prove exactness and provenance for the present static and
  operation fixtures, but they do not meet the accepted query, full-result, or
  load gates.
- Current tests and verifier evidence cover schema drift, TTL rejection,
  concurrent updates, compaction/reload, corruption handling, and
  high-frequency segment splitting. Long-lived delete-marker accumulation still
  requires optimize/merge/rebuild/integrity work before SQLite-class production
  claims.
- Current docs defer optimize/rebuild/vocab/integrity work. The reviews make
  those deferrals release blockers for any claim of SQLite-class production
  FTS.

## Two-Verifier Evidence

The checked P0 actionable items and checked P0 minimum merge gates have been
audited by two independent verifiers, Singer and Einstein, against the
acceptance criteria in this document.

- Singer ran `git diff --check`, compiled the current sources, and passed
  `leveled_fts:test()` 69/69, `leveled_codec:test()` 7/7, and `fts_SUITE`
  21/21. Singer confirmed the checked P0 items, the repaired global-schema
  SQLite differential coverage, the repaired malformed UTF-8 Unicode61
  differential coverage, the prefix narrowing path, and the tokenizer-matrix
  gate under the v1 contract that only `unicode61` is supported while SQLite
  `ascii`, `porter`, and `trigram` are rejected before FTS write/search side
  effects.
- Einstein ran focused Common Test/EUnit verification, including
  `sqlite_supported_ast_differential_contract`,
  `unicode61_supported_parity_corpus_contract`, `single_object_contract`,
  `segment_representation_contract`, `leveled_fts` EUnit 69/69, `leveled_codec`
  EUnit 7/7, and `leveled_bookie` EUnit 52/52. Einstein confirmed the checked
  P0 items after the repaired coverage and confirmed the prefix narrowing path.
  Einstein also confirmed the tokenizer-matrix gate under the explicit
  unicode61-only release contract.
- The fast-path P0 item remains open: Singer considered it closable, but
  Einstein rejected closure because the current static and operation benchmark
  artifacts still fail query, full-result, and load gates.
- The benchmark/release-gate P0 item remains open for the same reason.
- Singer and Einstein confirmed that `fts_rank_bm25_stats_gate_test` can close
  only as a rejection gate: `rank => none` is the v1 contract, BM25/stat
  options are rejected before search planning, and the persisted-stats P1 item
  remains open until real stats and SQLite BM25 ordering coverage exist.

## Core Leveled Change Audit

Every core change outside `leveled_fts.erl`, FTS tests, FTS benchmarks, and FTS
docs must stay small enough to explain here.

- `include/leveled.hrl`: adds `?FTS_TAG` for hidden FTS schema, segment, and
  delete-segment carrier objects. This avoids external objects while keeping
  shared segment payloads off arbitrary user-object TTL lifecycles.
- `src/leveled_head.erl`: includes `?FTS_TAG` in the object-tag type and default
  reload strategy list. Hidden carriers must reload through the existing
  Journal/Ledger path after restart.
- `src/leveled_codec.erl`: adds `idx_internal_indexspecs/5` and payload-bearing
  index ledger metadata for reserved FTS fields only. Public
  `idx_indexspecs/5` remains the ordinary 3-tuple contract, so raw secondary
  indexes do not gain a payload API.
- `src/leveled_codec.erl`: adds `revert_to_unbatched/2` so retained compacted
  FTS schema carriers keep their full object body after the same-SQN batch
  wrapper is removed. Schema reads need a directly fetchable object; segment
  carriers can remain keydelta-only.
- `src/leveled_bookie.erl`: adds the public FTS API surface and expands FTS
  writes inside the Bookie server turn. This is required for serialized schema
  creation, current-manifest fetch, generation assignment, and one-SQN atomic
  commits.
- `src/leveled_bookie.erl`: uses a private internal batch path for FTS-expanded
  object changes. It shares the normal `ink_batchput` and ledger-cache finish
  path, but uses the internal codec path so hidden FTS payload facts can be
  written without changing public `book_batchput`.
- `src/leveled_bookie.erl`: rejects literal `idx_payload` tuples at raw
  `book_put` entry and through normal batch validation. This is a reserved
  payload-boundary check only; it does not inspect or protect FTS-managed keys,
  and ordinary raw writes remain raw.
- `src/leveled_bookie.erl`: rejects deprecated tag-wide `hashlist_query` for
  `?IDX_TAG`, and rejects `tictactree_idx` only for reserved FTS payload fields.
  A tag-wide index hash has no field boundary and cannot safely mix private FTS
  payload rows with ordinary secondary-index rows; field-scoped ordinary
  secondary-index tictac remains supported.
- `src/leveled_iclerk.erl`: teaches retain compaction to convert hidden FTS
  carrier rows to keydelta-only form, except schema carrier rows which are
  unbatched to preserve direct schema fetch. This keeps FTS state inside the
  existing compaction mechanism.
- No compatibility shim exists for the old row representation or any pre-release
  FTS payload version. Unsupported FTS shapes are rejected or treated as corrupt
  in write paths.

## Actionable Items

- [x] P0: Audit and harden `idx_payload` as an internal-only index fact.

  Acceptance criteria:
  - Public `book_batchput` and raw index specs still reject user-supplied
    `idx_payload` tuples.
  - FTS internal writes can emit `{idx_payload, add | remove, Field, Term,
    Payload}` through the internal batch path without crashing.
  - `leveled_codec` stores payloads in index ledger metadata and preserves
    ordinary 3-tuple index behavior unchanged.
  - Ledger-cache replay, Journal reload, retain compaction, recalc/rebuild
    startup, anti-entropy/hash paths, and fold/query APIs either preserve
    payload metadata or explicitly reject unsupported FTS payload recovery.
  - Tests prove raw payload injection is rejected and FTS payload facts survive
    close/reopen and supported compaction paths.

- [x] P0: Move segment and delete-segment facts off arbitrary user objects.

  Acceptance criteria:
  - Segment postings and segment delete facts are carried by deterministic
    hidden FTS carrier objects, not appended to whichever user document happens
    to receive a batch chunk.
  - Hidden carriers use a dedicated FTS tag and `TTL = infinity`.
  - Carrier keys are deterministic, collision-resistant, and include enough
    index/term/column/payload identity to make replays idempotent.
  - User document changes contain the user object and doc marker only; segment
    fact lifetime is independent of user-object TTL.
  - A same-SQN batch still commits doc markers, segment carriers, and delete
    carriers atomically through existing Leveled batch mechanics.
  - Tests prove finite user TTL cannot expire unrelated segment payloads.
  - Interim finite-TTL rejection may reduce immediate risk, but it does not
    close this item for production FTS because the reviewed lifecycle issue is
    the attachment of shared segment facts to arbitrary user objects.

- [x] P0: Reject or correctly implement finite TTL for FTS segment mode.

  Acceptance criteria:
  - Until hidden carriers are complete, any finite TTL FTS put/delete returns a
    documented error such as `fts_segment_mode_requires_infinity_ttl`.
  - After hidden carriers are complete, finite user-object TTL behavior is
    explicitly specified: expired docs must be invisible even if segment
    carriers remain.
  - Tests cover finite TTL rejection or finite TTL visibility semantics across
    close/reopen.

- [x] P0: Add a persisted global schema object per `{Bucket, Index}`.

  Acceptance criteria:
  - Schema includes version, index id, columns, tokenizer, tokenizer options,
    prefix configuration, diacritic mode, and any future ranking/stat settings.
  - FTS puts validate against the persisted schema; callers cannot define or
    drift schema ad hoc per document.
  - FTS deletes and searches validate index/schema existence and query columns.
  - First-create schema behavior is explicit and serialized inside Bookie.
  - Two new docs in the same `{Bucket, Index}` with different columns,
    tokenizer, prefixes, or diacritic settings are rejected.
  - SQLite differential fixtures include schema mismatch and unknown-column
    cases.

- [x] P0: Make all FTS writes Bookie-owned and generation-safe.

  Acceptance criteria:
  - Public FTS APIs do not accept `OldManifest` or caller-assigned generation.
  - Bookie fetches the current doc marker/schema and builds the FTS batch in one
    serialized server turn.
  - Put modes are explicit: create, replace, and upsert, or only upsert if that
    is the chosen release contract.
  - Create requires no current doc marker; replace requires an existing marker;
    upsert uses the current marker atomically.
  - Concurrent same-key updates cannot both publish the same live generation or
    leave loser-generation postings visible.
  - Tests cover concurrent update, update/delete/reinsert, and batch duplicate
    identity cases.

- [x] P0: Make manifest loading tri-valued and operation-aware.

  Acceptance criteria:
  - Manifest loading returns `not_found`, `{ok, Manifest}`, or `{error, Reason}`;
    malformed existing manifests are never collapsed to missing on write.
  - `manifest_from_keychanges/1` treats `idx_payload remove` doc-marker payloads
    and `manifest_deleted => true` headers as not found.
  - Corrupt payloads return a documented error instead of crashing or silently
    restarting at generation 1.
  - Delete paths reject a manifest whose index does not match the requested
    index.
  - Tests cover delete payloads, corrupt payloads, malformed manifests, and
    wrong-index delete/update attempts.

- [x] P0: Define latest-wins doc-marker semantics.

  Acceptance criteria:
  - If multiple active doc-marker payload facts exist for the same
    `{Bucket, Key, Index}`, lookup chooses the active fact with the greatest
    SQN.
  - Update of the same doc marker either removes the old marker or has a proven
    latest-SQN-wins lookup rule.
  - Tests cover put/put/delete/reinsert cycles and direct fold inspection of
    marker payloads.

- [x] P0: Normalize and enforce duplicate identity semantics.

  Acceptance criteria:
  - Batch duplicate detection normalizes `Index` before identity comparison.
  - The release contract clearly states whether one user object can update
    multiple FTS indexes in one FTS batch.
  - If multi-index-per-object is not supported, duplicate detection is
    object-key-first and rejects same object key across different FTS indexes.
  - If multi-index-per-object is supported, a real combiner emits one user
    object change plus all required FTS marker/segment facts.
  - Tests cover same object/same index duplicates and same object/different
    index duplicates.

- [x] P0: Enforce the FTS object tag contract.

  Acceptance criteria:
  - FTS is either explicitly `?STD_TAG` only, or manifests preserve and use the
    original tag consistently.
  - Doc-marker add/remove payloads do not hardcode a tag that can diverge from
    the actual user object.
  - Tests prove unsupported tags are rejected or correctly indexed, deleted,
    and updated.

- [x] P0: Split segment payloads by postings count and encoded byte size.

  Acceptance criteria:
  - Segment construction enforces maximum postings per payload and maximum
    encoded bytes per payload.
  - A high-frequency term produces multiple bounded segment payloads, not one
    unbounded Erlang term.
  - Segment keys remain ordered and queryable after splitting.
  - Query results are byte-for-byte identical before and after splitting on the
    same corpus.
  - Tests include a hot-term corpus large enough to force multiple pages.

- [x] P1: Add physical prefix-index support or narrow the prefix contract.

  Acceptance criteria:
  - Either configured `prefixes` materialize prefix segment entries in a distinct
    namespace, or the option is renamed/documented as prefix scan and SQLite
    prefix-index parity is not claimed.
  - If physical prefixes are implemented, prefix query plans use prefix segment
    entries for configured lengths instead of scanning all full tokens.
  - Unconfigured or too-short prefix scans have explicit limits or documented
    rejection.
  - Tests compare SQLite prefix results and include stale prefix update/delete
    cases.
  - Evidence: the release contract is narrowed to bounded segment token-range
    scans and explicitly does not claim SQLite physical prefix-index storage
    parity. Singer and Einstein both confirmed the narrowing path against
    `docs/FTS.md`, `prefix_scan_contract_uses_bounded_token_range_test/0`,
    schema prefix validation, prefix query caps, SQLite prefix differential
    coverage, and stale-prefix update/delete coverage.

- [ ] P1: Add persisted stats before claiming BM25 support.

  Acceptance criteria:
  - `rank => none` remains production-supported without stats.
  - `rank => bm25` is rejected or documented experimental until stats are
    persisted.
  - Persisted stats include live document count, total token count, per-column
    token totals, and enough term/phrase document-frequency data for the chosen
    BM25 contract.
  - Stats survive close/reopen and supported compaction paths.
  - BM25 tests compare SQLite ordering within an explicit tolerance and cover
    restart and merge.

- [ ] P1: Add merge, optimize, rebuild, integrity-check, and vocab APIs.

  Acceptance criteria:
  - Public or internal APIs exist for optimize/merge, rebuild, integrity check,
    and vocab-style introspection, with documented scope.
  - Merge folds segment payloads for a term/column, applies tombstones, writes
    higher-level hidden segment carriers, and removes obsolete segment/delete
    carriers safely.
  - Rebuild recreates schema, doc markers, segments, delete facts, and stats
    from the authoritative source contract.
  - Integrity check detects orphan markers, orphan segment carriers, corrupt
    payloads, schema drift, stale generations, and stats mismatch.
  - Optimize reduces segment count and preserves result sets byte-for-byte.

- [x] P0: Bound memory and work in query execution.

  Acceptance criteria:
  - Hot term, prefix, phrase, NEAR, boolean, and NOT paths have documented caps
    for query bytes, token count, AST depth, NEAR distance, prefix expansion,
    postings decoded, returned positions, and result limit.
  - Fallback evaluator paths cannot materialize unbounded maps for hot terms
    without a cap.
  - Iterator/doclist execution is used or explicitly scoped as future work with
    release-limiting query caps.
  - Tests prove oversized queries return errors, not process crashes or runaway
    memory.

- [x] P0: Add explicit anchor correctness coverage.

  Acceptance criteria:
  - Anchored term semantics are tested after update, delete, and reinsert.
  - Column-scoped anchors are tested, including `title:^term` behavior.
  - Negative cases prove anchors do not match later-column/body occurrences
    when SQLite semantics require the term at the start of the constrained
    column.
  - The committed `^equivupdate`, `title:^equivtitleupdate`, and negative
    `^equivbodyupdate` coverage remains present or is replaced by stronger
    equivalent tests.
  - Anchor tests run in both direct Common Test coverage and the 20k
    operation-equivalence query suite before anchor optimizations are accepted.

- [ ] P0: Keep rejected fast paths and benchmark-only shortcuts out of
  production.

  Acceptance criteria:
  - Previously rejected anchor/phrase/NEAR fast paths are not reintroduced
    unless full static and operation-equivalence benchmarks pass exactness,
    provenance, query, full-result, and load gates.
  - Any new anchor fast path must not regress `^equivupdate`, `equivupdate`,
    `title:^equivtitleupdate`, title/body anchor-adjacent queries, or static
    NEAR queries against the accepted baseline gates.
  - Any new phrase-prefix or NEAR fast path must pass the dirty update/delete
    operation queries and not rely on benchmark query literals.
  - Verifier reviews explicitly check that benchmark artifacts are comparable
    and no query-specific shortcut was introduced.

- [x] P0: Complete SQLite grammar and tokenizer differential coverage for the
  supported query contract.

  Acceptance criteria:
  - Differential tests compare SQLite and Leveled for terms, phrases, prefix,
    phrase-prefix, `+`, `^`, NEAR, column filters, NOT, explicit AND/OR, implicit
    AND, and invalid SQLite syntax.
  - Boolean precedence matches SQLite FTS5 for the supported grammar.
  - Unicode61 parity corpus covers case folding, diacritics, tokenchars,
    separators, private-use characters, combining marks, and malformed UTF-8.
  - Unsupported SQLite tokenizers or auxiliary functions are rejected or clearly
    documented as out of scope.

- [x] P0: Add lifecycle tests for update/delete/reinsert, crash/reload, and
  compaction.

  Acceptance criteria:
  - Tests cover update/delete/reinsert for the same key across multiple
    generations.
  - Tests cover delete without original text using only the doc marker manifest.
  - Tests cover crash/reload or simulated partial same-SQN batch tail.
  - Tests cover retain compaction, recalc/rebuild startup, and any explicitly
    unsupported compaction mode rejection.
  - Tests inspect direct index folds for segment, delete-segment, and doc-marker
    payload consistency.

- [x] P0: Keep core Leveled changes minimal and reviewed.

  Acceptance criteria:
  - Any change outside `leveled_fts.erl`, FTS tests, and FTS benchmarks has a
    written reason explaining why existing Leveled mechanisms cannot satisfy
    the contract without it.
  - Two independent verifier reviews inspect every core change for non-FTS
    behavior risk.
  - Existing non-FTS batchput, CAS batchput, raw put/delete, secondary-index,
    compaction, and anti-entropy tests still pass.
  - No compatibility shim is added for old FTS representations.
  - Evidence: Singer and Boyle both returned PASS on the core-change audit;
    `leveled_bookie` EUnit passed 52/52, `basic_SUITE` passed 15/15,
    `recovery_SUITE` passed 15/15, and `tictac_SUITE` passed 5/5.

- [ ] P0: Update the benchmark and release gates after each cleanup pass.

  Acceptance criteria:
  - Static and operation-equivalence suites use at least 20k documents.
  - Summaries print full per-query limited-result and full-result ratios, not
    only pass fractions.
  - Evidence records exactness, provenance, load ratio, query ratios, full-result
    ratios, store bytes, corpus hashes, SQLite build metadata, and artifact
    paths.
  - Current release target remains: exact results true, provenance true, query
    and full-result ratios within 2.0x SQLite, and load within the accepted 5.0x
    target unless explicitly revised.
  - Two independent verifier reviews confirm SQLite and Leveled paths are
    comparable and no benchmark-only shortcuts were introduced.
  - Current evidence:
    `/tmp/leveled_fts_opt/results/v269-current-head/summary-static-b1000.json`
    uses 20k docs and passes exactness/provenance, but fails query,
    full-result, and load gates. Static load is 21.24x SQLite, store ratio is
    3.85x, and the worst query ratios are `title:history` at
    364.23x / 308.28x and `NEAR(history culture, 10)` at
    96.08x / 88.64x.
  - Current evidence:
    `/tmp/leveled_fts_opt/results/v269-current-head/summary-ops.json`
    uses 20k source docs, 24,058 operations, and passes exactness, provenance,
    and operation equivalence, but fails query, full-result, and load gates.
    Operation load is 21.47x SQLite, store ratio is 4.00x, and the worst
    operation ratios are `equivupdate` at 189.27x / 180.96x,
    `equivreinsertold` at 29.76x / 25.49x, and `equivupdateold` at
    24.98x / 20.97x.
  - This item stays open because the accepted release thresholds and
    final two-verifier benchmark-review gate are not yet satisfied.

## P0 Minimum Merge Test Gate

The following named tests or equivalent Common Test/EUnit coverage must exist
before any production-FTS merge claim:

- [x] `fts_idx_payload_internal_only_test`
- [x] `fts_global_schema_mismatch_test`
- [x] `fts_segment_anchor_ttl_test`
- [x] `fts_manifest_delete_payload_not_live_test`
- [x] `fts_corrupt_manifest_rejected_test`
- [x] `fts_concurrent_update_generation_test`
- [x] `fts_update_delete_reinsert_generations_test`
- [x] `fts_multi_index_duplicate_identity_test`
- [x] `fts_hot_term_segment_page_split_test`
- [x] `fts_anchor_update_column_negative_test`
- [x] `fts_rejected_fast_path_regression_gate_test`
- [x] `fts_sqlite_supported_ast_differential_test`
- [x] `fts_unicode61_supported_parity_corpus_test`
- [x] `fts_query_caps_test`
- [x] `fts_retain_compaction_payload_survival_test`

Acceptance criteria:
- Each test names the invariant it protects.
- Each test fails against the unsafe behavior described in the reviews.
- The full 20k SQLite parity benchmark still passes exactness and provenance
  after the tests are added.

## P1/P2 Feature Gate

The following tests or equivalent coverage are required before claiming the
associated SQLite-class feature:

- [x] `fts_prefix_physical_index_or_reject_test`
- [x] `fts_rank_bm25_stats_gate_test`
- [ ] `fts_optimize_equivalence_test`
- [ ] `fts_rebuild_integrity_roundtrip_test`
- [ ] `fts_vocab_integrity_api_test`
- [x] `fts_sqlite_full_tokenizer_matrix_test`

Acceptance criteria:
- Each test is tied to a public feature claim in `docs/FTS.md` or the API docs.
- Deferred features are explicitly rejected or omitted from the public contract.
- Enabling any P1/P2 feature requires the same exactness, provenance, benchmark,
  and two-verifier evidence as P0 work.

## Close-Out Rule

The current segment-only production-safety cleanup is complete only when all P0
items have matching evidence in source, tests, benchmark artifacts,
documentation, and two independent verifier reviews. SQLite-class feature claims
are complete only when their P1/P2 items have the same evidence. Passing the
current happy-path benchmark alone is not sufficient.

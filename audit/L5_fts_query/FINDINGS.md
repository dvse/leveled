# Layer 5 audit: FTS query and consolidation

Target: `/Users/dvse/projects/agents/leveled` at
`9d219684786c98cddc13d78132f3bc633335e887` (baseline
`7f08bba703c9c4f635da0f53e0f18bae0df59dbb`). The target tree was treated as
read-only. All commands below run against its prebuilt beams and create data
only below this audit directory.

## Findings

### 1. A 256-column schema is accepted, then writes an unreadable FTS delta

Severity: **corruption** (persisted FTS payload is structurally unreadable;
queries fail).

Invariant violated: every schema accepted by `book_start/1` must have a
round-trippable posting encoding. The public schema contract states no column
count limit.

Expected: after storing one document with `needle` in each accepted column, a
query restricted to `c0` returns key `k`.

Actual:

```text
columns=256
expected={ok,[<<"k">>]}
actual={error,{invalid_fts_payload,delta_cols,0,8960,[]}}
```

Cause: schema validation checks only non-empty/unique columns and does not
bound their count (`src/leveled_fts.erl:805`). `encode_delta/1` stores both the
column count and each column id in 8 bits (`src/leveled_fts.erl:2835`), so 256
wraps the count to zero while leaving 8,960 trailing bytes. The strict decoder
then rejects those bytes (`src/leveled_fts.erl:2851`). The same 8-bit shapes
also exist in `encode_base/1`.

Reproduction:

```sh
./repro_column_count_wrap.escript
```

File: `repro_column_count_wrap.escript`

### 2. An abandoned earlier publish lets an acknowledged FTS write become invisible, and the result cache preserves the miss after the frontier heals

Severity: **wrong-result**.

Invariant violated: an acknowledged FTS write must be visible to a later
query; an FTS cache stamp is a completeness claim. The documented
IO-before-PUBLISH caller-death window must not make a later acknowledged write
invisible.

The repro writes SQN 2 to the inker and deliberately omits PUBLISH (the
documented unacked-write window), then performs and receives `ok` for an FTS
write at the next SQN.

Expected: `alpha` returns `[new, old]` both during the gap and after the
five-second abandoned-gap skip.

Actual:

```text
abandoned_sqn=2
expected=[<<"new">>,<<"old">>]
during_gap=[<<"old">>]
cached_after_gap=[<<"old">>]
uncached_after_gap=[<<"new">>,<<"old">>]
```

Cause: `{publish_fts,...}` inserts the later write's ledger changes before it
calls the absorption gate (`src/leveled_bookie.erl:2366`), but `absorb_sqn/3`
buffers the corresponding cache advance behind the missing SQN
(`src/leveled_bookie.erl:3825`). A query therefore sees the new ledger rows
while accepting old shard state under `Stamp =< Seq`
(`src/leveled_fts.erl:3105`). When the frontier later applies the buffered
advance, `apply_fts_advance/3` changes cache contents but not the already
allocated FTS sequence (`src/leveled_bookie.erl:3859`), so the wrong result
cached under that sequence remains addressable.

Reproduction:

```sh
./repro_frontier_gap_cache_staleness.escript
```

File: `repro_frontier_gap_cache_staleness.escript`

### 3. A cold shard-cache fill racing a write can install stale state after the writer has finished

Severity: **wrong-result**.

Invariant violated: warm and cold queries at the same completed-write frontier
must return the same result, and a cache row accepted as complete must include
every earlier write touching that shard.

The repro deterministically suspends a cold query inside
`load_shard_state/2`, completes a same-shard write, then lets the old query
install its snapshot into the cache.

Expected: both warm and cold `alpha` queries return `[new, old]`.

Actual:

```text
stale_cache_stamp=1
expected=[<<"new">>,<<"old">>]
warm_actual=[<<"old">>]
cold_actual=[<<"new">>,<<"old">>]
```

Cause: on a cache miss the query folds first and later uses `ets:insert_new/2`
(`src/leveled_fts.erl:3107`). If a writer runs between those operations, its
`advance_shard_cache/3` sees no row and intentionally does nothing
(`src/leveled_fts.erl:1410`). The old query then inserts the stale row, and all
later queries accept it because the read predicate is `Stamp =< QuerySeq`
instead of proof that every touching write after `Stamp` was applied
(`src/leveled_fts.erl:3105`). Removing that row immediately restores the
correct result.

Reproduction:

```sh
./repro_shard_cache_fill_race.escript
```

File: `repro_shard_cache_fill_race.escript`

### 4. A delayed cached FTS runner returns the construction-time result instead of taking its invocation-time snapshot

Severity: **wrong-result**.

Invariant violated: `book_returnfolder/2` states that calling the returned
folder runs the fold over a snapshot (`src/leveled_bookie.erl:1298`). A cached
and uncached runner invoked at the same time must therefore view the same
store state.

Expected: a runner obtained before the `new` write but invoked after it returns
`[new, old]`.

Actual:

```text
expected_at_runner_invocation=[<<"new">>,<<"old">>]
delayed_cached=[<<"old">>]
delayed_uncached_control=[<<"new">>,<<"old">>]
fresh=[<<"new">>,<<"old">>]
```

Cause: `get_runner/2` captures `{Cache, State#state.fts_seq}` while constructing
the closure (`src/leveled_bookie.erl:3304`) and performs the result-cache lookup
only when that closure is later invoked (`src/leveled_bookie.erl:3313`). A hit
returns without calling `SnapFun`, freezing the view at runner construction;
the uncached path takes the promised invocation-time snapshot.

Reproduction:

```sh
./repro_delayed_runner_result_cache.escript
```

File: `repro_delayed_runner_result_cache.escript`

### 5. The positions cap can reverse BM25 ordering even though the contract says ranking counts every occurrence

Severity: **wrong-result** (rank and score).

Invariant violated: `docs/FTS.md:251` says the cap drops trailing positions
only and that ranking sees every occurrence.

The repro indexes two all-`z` documents with 65,000 and 70,000 occurrences.
With the implementation's BM25 formula and full term frequency, the 70,000
document scores slightly higher.

Expected vs actual:

```text
expected_order=[<<"b_70000">>,<<"a_65000">>]
actual_order=[<<"a_65000">>,<<"b_70000">>]
expected_full_tf_scores=[{a_65000,2.199960513529244e-6},
                         {b_70000,2.199961238778174e-6}]
actual_hits=[{<<"a_65000">>,2.1999605135292443e-6,65000},
             {<<"b_70000">>,2.199958591648027e-6,70000}]
```

Cause: `encode_positions/1` truncates the persisted position list at 65,525
bytes (`src/leveled_fts.erl:3941`), while BM25 term frequency is computed as
the length of that decoded list (`src/leveled_fts.erl:3681`). `doc_length`
still contains the full count, so the longer document receives more length
penalty without its matching full TF, reversing the order.

Reproduction:

```sh
./repro_bm25_position_cap.escript
```

File: `repro_bm25_position_cap.escript`

### 6. Known issue quantified: an uncached consolidated base adds a 5–10x query floor

Severity: **perf**.

Invariant violated: consolidation should not materially degrade query service
for a selective term. This is the requested quantification of the known
consolidated-store slowdown.

The repro creates a 1,060,070-byte, two-column base in one shard. The query
uses one tiny column while the other column supplies the bulk of the base. A
typical isolated run on this host was:

```text
docs=20000 runs=31 base_bytes=1060070
unconsolidated_warm_median_us=17
consolidated_uncached_median_us=143
consolidated_warm_median_us=17
uncached_vs_unconsolidated=8.41x
uncached_vs_decoded_base_cache=8.41x
```

Scheduler noise can raise the 17 µs baseline, so the script reports both
ratios; the uncached-vs-decoded-base comparison consistently isolates the
roughly 8x base-read penalty.

Cause: a base-cache miss fetches the reserved base object and decodes the
entire base (`src/leveled_fts.erl:3167`, `src/leveled_fts.erl:3183`). Only
after that full journal read and multi-column decode does the query select its
one column and binary-search the token. The delta representation is already in
the warm shard-state row, and the decoded-base cache removes the regression,
pinning the extra journal fetch/full-base decode as the cause.

Reproduction:

```sh
./repro_consolidated_uncached_slow.escript
```

File: `repro_consolidated_uncached_slow.escript`

## Confirmed non-findings and differential coverage

- `check_driver_vs_full.escript` forced positional full loading by adding a
  phrase leaf under `rank => bm25`, then compared its match keys with the
  unranked driver path. Nine AST shapes covering AND, OR, NOT, phrase, prefix,
  column restrictions, nested boolean logic, and NEAR agreed before
  consolidation, after consolidation, and after restart.
- `check_consolidation_concurrency.escript` held a consolidation worker after
  its snapshot, completed updates, deletes, and inserts, then allowed the old
  derivation to apply. Exact hit maps (including order, BM25 scores,
  positions, doc lengths, limits and offsets) matched an unconsolidated oracle,
  then remained equal after both sides consolidated, after re-consolidation,
  and after restart.
- Source review found the base/delta merge consumes only the exact folded
  delta sequences, retains later deltas via `Seq > ConsSeq`, and commits base,
  summary, and removals in one batch. The adversarial differential above did
  not find a consolidation result-equality defect.
- Driver costs count stale frames and can choose a suboptimal driver, but the
  selected legs remain a candidate superset; the forced-full differential did
  not find a missed match.
- Bloom construction and lookup use the same bit count and four identical
  `phash2`-derived positions. Empty blooms conservatively load a non-empty
  base. No false-negative path was found.
- Ranked results use `{rank, key}` as the total ordering before applying
  offset/limit, so rank ties are deterministic and no additional tie-window
  drop was found.
- Parser/evaluator review and local SQLite FTS5 spot checks covered NOT
  precedence, implicit AND, two- and three-leg NEAR boundary distances, empty
  phrases, phrase prefixes, anchors, and negative column restrictions. No new
  match-set discrepancy was reproduced in those supported shapes.

## Suspicion (unreproduced)

The consolidated-base fetch path converts a snapshot head whose journal body
cannot be fetched into `not_found` (`src/leveled_bookie.erl:4130`), and
`fetch_shard_base/2` treats that as an absent base (`src/leveled_fts.erl:3183`).
Unlike the public caller-side GET path, this path has no direct-read retry. A
journal reorganisation race may therefore be able to create transient false
negatives for consolidated terms. I did not obtain a deterministic prebuilt-
beam reproduction, so this is not claimed as a finding.

## Coverage limits

I did not rerun the repository's full Common Test/EUnit suite or mutate its
read-only `_build`. I did not repeat the 5.3 GB SQLite benchmark, exhaustively
enumerate Unicode tokenizer parity, inject CRC-corrupt base objects, or force a
journal file-close race at the exact consolidated-base fetch. The out-of-tree
checks used small adversarial corpora, prebuilt production beams, real
restarts, real journal writes, and deterministic Erlang tracing for the two
cache/consolidation interleavings.

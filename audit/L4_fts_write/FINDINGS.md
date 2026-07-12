# Layer 4 — FTS write-path correctness review

Target: `/Users/dvse/projects/agents/leveled` at
`9d219684786c98cddc13d78132f3bc633335e887`.

The leveled checkout was treated as read-only. Reproductions use the prebuilt
ebins; the codec/merge and sequence-interleaving positive controls compile the
unchanged source with `export_all` outside the target tree so private codec
functions can be exercised directly.

## Findings

### 1. Data-integrity — 256 populated columns wrap the delta column count and persist an undecodable posting payload

Invariant violated: every accepted FTS schema and acknowledged write must
produce a self-decodable posting delta. If the on-disk format cannot represent a
schema, configuration or the write must be rejected before the journal commit.

Expected: a 256-column schema is rejected as unencodable (or the delta format
encodes all 256 columns without truncation).

Actual: `book_start` returns `{ok, Pid}` and `book_put` returns `ok`. The
canonical object remains readable, but every search touching the delta returns
`{error,{invalid_fts_payload,delta_cols,0,8960,[]}}`. The same error occurs after
restart, proving that the malformed secondary-index payload was persisted.

Cause: schema normalization checks only non-empty/duplicate columns and has no
format-width bound (`src/leveled_fts.erl:781`, `src/leveled_fts.erl:805`).
`encode_delta/1` writes `length(ColStreams)` into eight bits, so 256 truncates to
zero (`src/leveled_fts.erl:2835`). The decoder correctly rejects a zero-column
header followed by 8,960 trailing bytes (`src/leveled_fts.erl:2851`). Column IDs
are also eight-bit fields at `src/leveled_fts.erl:2841`.

Reproduction:

```text
$ ./repro_delta_column_count_overflow.escript
expected=start_or_write_rejected_for_unencodable_schema
actual_start={ok,<0.83.0>}
actual_put=ok
actual_search={error,{invalid_fts_payload,delta_cols,0,8960,[]}}
actual_search_after_restart={error,{invalid_fts_payload,delta_cols,0,8960,[]}}
```

Suggested correction: reject schemas with more than 255 columns during
normalization and add explicit range guards in `encode_delta/1`, or version and
widen both persisted eight-bit fields.

### 2. Wrong-result — an SQN gap lets a warm shard cache omit a later acknowledged FTS write; the result cache preserves the omission after frontier repair

Invariant violated: after an FTS write returns `ok`, a later search must include
that write. A shard-cache stamp is a completeness claim and must never validate
cache state that omits ledger-visible posting rows. Restart must not be required
to make an acknowledged document searchable.

Expected after the second writer is acknowledged: searching `common` returns
`[<<"acked">>, <<"seed">>]`. The abandoned writer is unacknowledged and its
pre-restart visibility is deliberately not part of the expectation.

Actual: after an earlier writer appends journal SQN 2 and dies before
`publish_fts`, the acknowledged SQN 3 writer returns `ok`, but a warm-cache
search returns only `[<<"seed">>]`. After the five-second frontier timeout and
a non-FTS write drives frontier advancement, the default result cache still
returns only `seed`; the same query with `result_cache => false` immediately
returns `acked` and `seed`. Restart returns `abandoned`, `acked`, and `seed` (the
unacknowledged `abandoned` recovery is permitted, but `acked` being absent
before restart is not).

Cause: `{publish_fts,...}` installs the higher-SQN ledger changes immediately
(`src/leveled_bookie.erl:2366`) but defers its FTS cache advance through
`absorb_sqn/3` (`src/leveled_bookie.erl:2375`). A gap stores that advance in
`publish_pending` (`src/leveled_bookie.erl:3825`). During the gap, queries use
the allocator's `fts_seq` as their cache sequence (`src/leveled_bookie.erl:3307`)
and trust an older shard entry whenever `Stamp =< Seq`
(`src/leveled_fts.erl:3099`). Once the incomplete result is cached, its key also
uses the unchanged allocator sequence (`src/leveled_fts.erl:522`), so applying
the delayed advance does not invalidate it.

Reproduction:

```text
$ ./repro_frontier_cache_gap.escript
expected_after_acked=[<<"acked">>,<<"seed">>]
actual_during_gap=[<<"seed">>]
actual_after_frontier_cached=[<<"seed">>]
actual_after_frontier_uncached=[<<"acked">>,<<"seed">>]
abandoned_journal_sqn=2
actual_after_restart=[<<"abandoned">>,<<"acked">>,<<"seed">>]
```

Suggested correction: either keep higher-SQN ledger changes invisible until
their FTS advance can be applied, or invalidate every touched shard immediately
when the higher-SQN changes become ledger-visible. Frontier repair must also
advance/invalidate the result-cache generation.

### 3. Wrong-result — `remove_diacritics` modes do not implement SQLite unicode61 semantics

Invariant violated: the accepted `remove_diacritics => 1 | 2` settings and the
documented unicode61 parity must yield SQLite-compatible tokens.

Expected from SQLite FTS5:

- Mode 1 retains U+1ED9 (a precomposed Latin character with multiple
  diacritics), so a document containing only U+1ED9 does not match `o`.
- Mode 2 removes diacritics from Latin characters, not arbitrary scripts, so
  Greek U+03AC does not match unaccented U+03B1.

Actual: both Leveled searches return `[<<"k">>]`, while the local SQLite FTS5
oracle returns zero matches in both cases.

Cause: values 1 and 2 are both accepted (`src/leveled_fts.erl:1834`), but
`normalise_token/2` sends every nonzero value through the same implementation
(`src/leveled_fts.erl:2106`). `strip_diacritics/1` applies NFD and the combining
mark mask to all scripts (`src/leveled_fts.erl:2122`), losing both SQLite's mode
1 exception and its Latin-script boundary.

Reproduction (requires the system `/usr/bin/sqlite3`, used as the differential
oracle):

```text
$ ./repro_remove_diacritics_mode1.escript
expected_sqlite_counts_mode1_o_mode2_greek_alpha="0\n0"
actual_leveled_mode1_o_keys=[<<"k">>]
actual_leveled_mode2_greek_alpha_keys=[<<"k">>]
```

Suggested correction: implement SQLite's distinct mode-1/mode-2 fold tables
instead of treating both modes as generic Unicode decomposition plus mark
removal.

### 4. Wrong-result — the Unicode fallback concatenates token runs across malformed UTF-8

Invariant violated: invalid UTF-8 must be a token boundary, as it is in SQLite
unicode61 and in Leveled's fast tokenizer. Enabling a custom tokenizer option
must not change the terms on either side of an invalid byte.

Expected for `<<"bad",255,"utf8">>` with `tokenchars => <<"_">>`: SQLite
matches `bad` and `utf8` and does not match `badutf8` (counts `1,1,0`).

Actual: Leveled does not match `bad` or `utf8`, and incorrectly matches
`badutf8` to the document.

Cause: a non-empty custom `tokenchars` or `separators` setting routes writes to
`tokenize_unicode/2` (`src/leveled_fts.erl:1911`). `unicode_chars/1` discards the
bad byte and concatenates the valid prefix and suffix before tokenization
(`src/leveled_fts.erl:2691`). In contrast, the fast path flushes the current
token on a bad byte (`src/leveled_fts.erl:1963`). This also disproves the
byte-identical-path claim at `src/leveled_fts.erl:1937` for malformed input.

Reproduction (also uses `/usr/bin/sqlite3` as the differential oracle):

```text
$ ./repro_unicode_fallback_invalid_utf8.escript
expected_sqlite_counts_bad_utf8_badutf8="1\n1\n0"
actual_leveled_keys=#{utf8 => [],bad => [],badutf8 => [<<"k">>]}
```

Suggested correction: preserve an explicit separator boundary when recovering
from malformed UTF-8 instead of concatenating `Good` with the recursively
decoded suffix.

## Positive controls and coverage

All of these runnable controls pass:

- `verify_codec_and_merge_boundaries.escript`: exact 65,525-byte position
  encoding, cap truncation, a 65,534-byte worst-case final varint, clean
  multi-frame walking, empty positions, a 65,535-byte key, loud rejection at
  65,536 bytes, a 65,535-byte token, verbatim empty/exact/oversized behavior,
  same-token merging across 32 streams, empty runs/frames, single-entry streams,
  delta round trips, valid-UTF-8 fast/Unicode tokenizer parity, and reserved
  delta carriers under `STD_TAG` for a nonstandard document tag.
- `verify_fts_seq_interleaving.escript`: holds five caller-side intents,
  interleaves five direct writers, verifies the fresh sequence does not collide,
  publishes the held writers last, and proves all 11 postings plus a write after
  an intent-only leak are searchable before and after restart. This exercises
  the four `fts_seq = max(SQN, current)` branches at
  `src/leveled_bookie.erl:3941`, `:3947`, `:4005`, and `:4011`.
- `verify_write_replay_and_liveness.escript`: caller/direct singleton and batch
  writes, overwrite and delete marker joins, consolidation, and exact
  search-result equality across restart. LIVE frames remain and obsolete/DEAD
  frames are absent.
- `verify_public_fts_spec_rejection.escript`: forged `fts_term` payload specs are
  rejected as `{error,invalid_index_specs}` by `book_put`, `book_put_direct`,
  `book_tempput`, `book_delete`, caller/direct `book_mput_std`, `book_casput`,
  and `book_casmput`.

The review also traced `derive_doc`, field/path extraction, column construction,
grouping, position/frame/entry/delta codecs, stream heap merging, marker-cache
updates, caller-side intent/journal/publish flow, delta carrier key construction,
reserved-object suppression, cache advances, and startup/replay sequence seeding.

Not exhaustively covered: differential tokenization of every Unicode scalar,
multi-gigabyte 32-bit stream-length boundaries, OS-level torn writes/power loss,
and corpus-scale performance. No unreproduced correctness suspicions are being
reported.

## Final verification commands

```text
./repro_frontier_cache_gap.escript
./repro_delta_column_count_overflow.escript
./repro_remove_diacritics_mode1.escript
./repro_unicode_fallback_invalid_utf8.escript
./verify_codec_and_merge_boundaries.escript
./verify_fts_seq_interleaving.escript
./verify_write_replay_and_liveness.escript
./verify_public_fts_spec_rejection.escript
```

All eight exit with status 0 on the audited HEAD.

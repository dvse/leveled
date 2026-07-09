# Full-Text Search

Leveled full-text search is automatic maintenance of ordinary secondary index
rows on the canonical object.

There are no FTS-owned objects, metadata buckets, segment objects, tombstone
objects, manifests, or sidecar files. The object stored under `{Bucket, Key,
Tag}` remains the only source object. FTS adds and removes secondary index rows
inside the same Bookie write that stores or deletes that object.

## Configure An Index

Configure FTS at `book_start/1` with `fts_indexes`.

```erlang
{ok, Bookie} = leveled_bookie:book_start([
    {root_path, RootPath},
    {fts_indexes, [
        #{
            bucket => <<"openai_responses">>,
            tag => ?STD_TAG,
            index => <<"responses_v1">>,
            columns => [
                #{name => prompt, path => [request, input]},
                #{name => answer, path => [response, output_text]}
            ],
            tokenizer => unicode61,
            remove_diacritics => 2,
            prefixes => [5, 11]
        }
    ]}
]).
```

Each definition applies only to objects whose bucket and tag match. The `index`
name is the query-time name. Columns are logical FTS columns; each column path
extracts text from the original object. Besides `tokenizer`,
`remove_diacritics`, and `prefixes`, the definition accepts the SQLite-style
tokenizer settings `tokenchars`, `separators`, and `stopwords`.

Path elements currently select through maps, tuples, and lists:

- map keys may be atoms or binaries;
- tuple and list positions are positive integers;
- missing paths produce empty text for that column.

Use a new `index` name when changing columns, paths, tokenizer options, or
prefix settings.

For tenant-prefixed buckets, one schema can cover every tenant by replacing
`bucket` with `bucket_prefix`:

```erlang
#{
    bucket_prefix => <<"tenant-">>,
    index => <<"main">>,
    columns => [...]
}
```

The schema matches every binary bucket sharing the prefix, while postings,
markers, and caches remain per actual bucket, so tenants stay isolated at
both write and query time (`book_ftssearch` still addresses one bucket).
Two definitions sharing an index name may not overlap: identical buckets,
a prefix covering an exact bucket, or nested prefixes are rejected at
`book_start` as `ambiguous_fts_schema`.

## Changefeed

The journal doubles as a change log. `book_journalfold/4` folds it in order
of receipt from a cursor SQN, emitting puts and deletes (including
superseded versions), and `book_journalsqn/1` returns the current
high-water mark:

```erlang
{ok, NowSQN} = leveled_bookie:book_journalsqn(Bookie),
{async, Feed} =
    leveled_bookie:book_journalfold(
        Bookie,
        ?STD_TAG,
        Cursor,
        {fun(Bucket, Key, SQN, Change, Acc) ->
             %% Change is {put, Object} or delete
             [{Bucket, Key, SQN, Change} | Acc]
         end,
         []}
    ),
Events = lists:reverse(Feed()).
```

The fold is bounded by the journal SQN at snapshot time; resume from the
highest SQN seen plus one. Journal compaction can remove superseded entries
from older parts of the journal, so a consumer keeping a durable cursor
should not lag indefinitely behind the compaction horizon.

## Write Objects

After configuration, write objects normally.

```erlang
Original = #{
    request => #{input => <<"How do I search saved responses?">>},
    response => #{output_text => <<"Use an automatically maintained FTS index.">>}
},

ok = leveled_bookie:book_put(
    Bookie,
    <<"openai_responses">>,
    <<"resp_001">>,
    Original,
    [{add, <<"session_id_bin">>, <<"sess_42">>}],
    ?STD_TAG,
    infinity,
    false
).
```

The caller supplies only normal application index specs. Leveled derives FTS
posting specs before the journal commit and writes the object plus postings
under one SQN.

Batches and deletes are also normal:

```erlang
ok = leveled_bookie:book_batchput(Bookie, [
    {put, <<"openai_responses">>, <<"resp_002">>, Obj2, [], ?STD_TAG, infinity},
    {delete, <<"openai_responses">>, <<"resp_003">>, [], ?STD_TAG, infinity}
]).
```

Updates and deletes never read the previous object or its postings. An update
writes the new postings as fresh pages and replaces the document's marker row;
a delete writes a tombstone for the marker. Postings from earlier writes are
filtered out at query time by the marker comparison described below, so no
old rows need to be found or removed at write time.

## Storage Shape

Postings live in a fixed, order-preserving TOKEN GRID: token space is
partitioned into shards by token prefix (1,024 ranges over the first two
token bytes), and every posting fact belongs to exactly one shard
regardless of when it was written. Per configured index
(`{Bucket, Index, Tag}`):

- Every indexed document carries a marker row
  `{{fts_doc, Index, Tag}, doc, Key} -> <<WriteSeq:64, DocLength:32>>`
  (unchanged from the first design: liveness is a join against markers).
- Each write batch emits, per touched shard, one small DELTA row —
  token-sorted entries whose doc frames are stamped with the write
  sequence:
  `{{fts_term, Index, Tag}, <<1, ShardId:16, WriteSeq:64>>, DeltaCarrier}`.
  Delta rows ride a per-shard reserved carrier key (`$fts_d$...`), so
  their exact row identity is known later.
- Each shard has at most one BASE: the consolidated posting set for the
  shard's token range, stored as an ordinary OBJECT under a reserved key
  (`$fts$<Index><ShardId>`) — the value lives in the journal (sequential
  writes, fetched by SQN, reclaimed natively by journal compaction when
  superseded). The base binary carries an internal probe directory for
  token binary search.
- Each shard has one SUMMARY row
  `{{fts_term, Index, Tag}, <<0, ShardId:16>>, BaseKey}` holding the
  consolidated-through sequence and a token bloom for the whole shard.
  Summaries supersede in place (same key), so the LSM collects old ones
  natively.

Reserved `$fts`-prefixed keys are engine-internal: the write path never
derives postings from them, and application folds should skip them.

Updates and deletes never read or rewrite earlier postings: a new write
stores fresh delta entries and replaces the document's marker (deletes
remove it). A posting frame is live only if its stamped sequence equals
the document's current marker sequence, so superseded postings are
invisible to queries wherever they sit — in deltas or in a base.

## Consolidation

`book_ftsconsolidate/4` folds a shard's deltas into its base: read base
plus deltas, heap-merge token-sorted streams, drop dead frames against
the markers (the only cross-key join, done here and at query time), and
commit — new base object, new summary, and REMOVALS of the consumed
delta rows — in one atomic batch per shard group. Everything the
operation supersedes is collected by machinery that already exists: the
journal compactor reclaims old base values, the LSM merge collects
replaced summaries and removed delta rows. There are no batch
directories, alias lists, batch discovery, or compaction sequences: a
shard is self-contained, ~megabytes, consolidates in milliseconds,
independently and in parallel — maintenance is continuous and
incremental rather than an operation with a size.

## Query Execution

A token maps to exactly one shard, so a term read is: shard summary
(bloom answers absent tokens without touching postings), base entry by
binary search, plus a scan of the shard's deltas — one contiguous key
neighbourhood per term BY CONSTRUCTION, regardless of write history.
The scatter that motivated batch compaction in the first design cannot
occur. Prefix terms cover a contiguous shard range. Doc frames filter
against the markers (write-through cached), then evaluation, ranking,
and the per-sequence result cache proceed exactly as before.

Per store instance the engine keeps write-through ETS caches of shard
state (summary + pending deltas, advanced by the write path under the
same stamp discipline as the marker cache) and decoded bases
(invalidated by consolidation), so warm reads touch no folds at all.

## Query

Use `book_ftssearch/5`.

```erlang
{async, Run} = leveled_bookie:book_ftssearch(
    Bookie,
    <<"openai_responses">>,
    <<"responses_v1">>,
    <<"answer:search AND prompt:responses">>,
    #{columns => [prompt, answer], limit => 20, rank => none}
),

{ok, Hits} = Run().
```

Hits are returned in key order when `rank => none`:

```erlang
[
    #{key => <<"resp_001">>, rank => 0.0, score => 0.0, doc_length => 17}
]
```

Supported query features:

- terms: `search`
- column filters: `answer:search`
- phrases: `"saved responses"`
- boolean operators: `AND`, `OR`, `NOT`
- prefix terms: `comput*`
- phrase prefix suffixes: `"new yo"*`
- NEAR: `NEAR(history culture, 10)` (nesting NEAR inside NEAR is rejected)
- anchor: `^hello`, or inside a column filter as `prompt:^hello`
  (`^prompt:hello` is a parse error)
- negated column filters: `-prompt:hello`, `-{prompt answer}:hello`

Search options: `limit` (default 10000, max 20000), `offset` (window capped at
20000), `return_positions`, `columns`, `rank => none` (the only supported
value), and `result => summary` (returns `#{total_count, full_keys_sha256}`
instead of the hit list). Query size is capped at 4096 bytes and 128 tokens.

Phrase and NEAR evaluation uses posting payload positions. It does not load
candidate objects just to compare positions.

Prefix terms are capped at 64 bytes and answered by reading the pages whose
directory token ranges cover the prefix. The schema `prefixes` setting is a
contract declaration -- search options naming different `prefixes` are
rejected as a contract change -- it is not required for prefix queries and
does not create a separate prefix store.

## Retrieving Originals

FTS search returns keys. Retrieve the canonical object normally:

```erlang
{ok, Original} = leveled_bookie:book_get(
    Bookie,
    <<"openai_responses">>,
    <<"resp_001">>,
    ?STD_TAG
).
```

For OpenAI API sessions or responses, store the original response as the normal
Leveled object and configure columns over the fields you want searchable. FTS
does not replace the object model and does not require a second per-response
index write.

## Existing Data

FTS maintenance applies to writes made after the index is configured. Existing
objects are not automatically backfilled by the current implementation. To index
existing data, scan the bucket and rewrite each object through the normal write
path with the same value and application index specs.

The rewrite still uses normal Bookie writes; it does not create a separate FTS
store.

## Tradeoffs

This design keeps Leveled's storage model simple:

- no hidden objects;
- no segment store;
- no explicit per-document FTS maintenance API for normal use;
- one Bookie commit for object and postings;
- phrase and NEAR without object reads.

Compared with SQLite FTS5: both pack postings and both run their own
segment maintenance above the host storage engine, but Leveled's grid
delegates the heavy mechanics to native machinery — journal compaction
reclaims superseded bases, LSM merge collects summaries and removed
deltas, and same-key supersession replaces alias bookkeeping. The
remaining engine-specific work is the marker join (per-document
liveness) and the shard consolidation merge itself.

## Benchmarks

- **2026-07-09** — [Leveled FTS vs SQLite FTS5, equivalence + 5x latency gate
  over a 5.3 GB Wikipedia corpus](fts_sqlite_gate.md). Summary: unranked match
  sets are identical in 38/39 cells (the one exception is a NEAR match flipped
  by CJK tokenizer position drift); ranked BM25 order/scores diverge on real
  text — dissected to (1) unicode61 CJK tokenization differences (Han runs
  dropped, Hangul decomposed) shifting `dl`/`avgdl` by ~2.5e-4, and (2) NEAR
  members scored with all instances where FTS5 uses NEAR-filtered counts.
  Write-adjacent (uncached) latency breaches the 5x gate on selective and
  ranked queries via the per-query snapshot floor, the per-write batch-list
  rediscovery, and the O(N) `corpus_stats` fold — while hot-term posting
  traversal is 2–4x and BM25 scoring itself is nearly free.

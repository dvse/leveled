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
extracts text from the original object.

Path elements currently select through maps, tuples, and lists:

- map keys may be atoms or binaries;
- tuple and list positions are positive integers;
- missing paths produce empty text for that column.

Use a new `index` name when changing columns, paths, tokenizer options, or
prefix settings.

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

On update or delete, Bookie reads the current object, derives its old FTS rows,
and appends the needed `{remove, Field, Term}` specs. New or changed postings
are appended as `{add_payload, Field, Term, Payload}` specs.

## Storage Shape

FTS postings are ordinary secondary index rows:

```erlang
{Field, Term, Key} -> Payload
```

The index field is:

```erlang
{fts_term, Index, Column}
```

For each `{Column, Token, Key}` row, the payload is a packed binary containing:

```erlang
#{
    v => 1,
    doc_len => DocLength,
    col_len => ColumnLength,
    tf => TermFrequency,
    pos => DeltaEncodedPositions
}
```

The logical map above is documentation only. The ledger metadata slot stores a
binary payload, not an Erlang map.

Each indexed object also has a standard doc marker row:

```erlang
{{fts_doc, Index}, doc, Key} -> Payload
```

That row is still an ordinary secondary index row attached to the canonical
object. It is not a hidden object.

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
- NEAR: `NEAR(history culture, 10)`
- anchor: `^prompt:hello`

Phrase and NEAR evaluation uses posting payload positions. It does not load
candidate objects just to compare positions.

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

It is not SQLite FTS5. SQLite stores packed cross-document doclists and skip
structures. Leveled stores one secondary index row per `{column, token, object}`
posting, so hot terms and broad boolean queries require more ledger iteration.

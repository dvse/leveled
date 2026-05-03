# Standard-Mode Batch Put

Status: draft proposal

## Problem

`book_put` supports standard-mode objects with values in the Inker journal and
metadata/index entries in the Penciller ledger. `book_mput` supports batches
only for `head_only` mode, where values live in the ledger. Applications that
need to make several standard objects and their secondary indexes visible as one
unit cannot use repeated `book_put` calls without exposing intermediate states.

## Proposed API

Add `leveled_bookie:book_batchput/2` and `book_batchput/3` for standard-mode
bookies only.

`book_batchput/2` accepts a non-empty list of batch object specs:

```erlang
{put, Bucket, Key, Object, IndexSpecs, Tag, TTL}
{delete, Bucket, Key, IndexSpecs, Tag, TTL}
```

`book_batchput/3` adds a batch-level `DataSync` boolean. The sync flag applies
to the whole CDB multi-put.

Return values match existing leveled write semantics:

- `ok` means the batch was accepted and is visible to later Bookie requests.
- `pause` also means the batch was accepted and is visible; callers should
  back off before sending more writes.
- `{error, Reason}` means the batch was rejected before ledger-cache visibility.

The public API intentionally does not return the assigned SQN. The Inker uses a
shared SQN internally so the Bookie can build normal ledger-cache metadata, but
callers should treat SQNs as storage internals rather than application-visible
patch sequence numbers.

## Why Not Overload `book_mput`

`book_mput` already has a specific head-only contract. Reusing that name for
standard-mode values would mix two storage models that leveled deliberately
keeps separate:

- head-only batches store values in the ledger;
- standard objects store values in the journal and metadata/indexes in the
  ledger;
- upstream startup options document that head-only and normal object storage
  should not be mixed in one bookie.

A new API keeps the contracts visible to callers and preserves existing
`book_mput` behavior.

## Internal Shape

The Inker allocates one SQN for the batch, converts every object to its normal
standard journal key/value, and writes all journal records with one
`leveled_cdb:cdb_mput/3` call.

Each record uses the existing fetchable shape:

```erlang
{SQN, ?INKT_STND, LedgerKey} -> JournalValue
```

Deletes use the existing tombstone encoding through the normal `delete` object
value. Ledger metadata for each object points at the shared SQN, while GET keeps
fetching by `{SQN, LedgerKey}`.

Each batch journal value also carries the expected batch object count in its
key-change payload. The payload is unwrapped before normal ledger/index rows are
generated. During startup replay, if only a prefix of a same-SQN batch is present
at the active CDB tail, leveled drops that incomplete batch from ledger rebuild.
The CDB startup path then truncates the unreadable tail to the last valid record.

## Compatibility Risks To Test

- Manifest `last_key` and `journal_sqn` handling when several journal keys
  share one SQN.
- Reload after the journal write lands but before the ledger cache is pushed.
- Journal compaction under `retain`, `recovr`, and any supported `recalc`
  strategy.
- Snapshot visibility before and after a batch commit.
- Index fold visibility for add, remove, and add/remove specs in one batch.
- Hot backup compatibility.
- CDB partial-tail behavior for multi-record writes. The chosen behavior is to
  ignore an incomplete same-SQN batch during startup replay and continue from the
  last complete committed batch.

Batch values use the same standard journal encoding as `book_put`, with only a
small wrapper around the key-change payload to record the expected batch count.
That wrapper is removed before ledger rebuild and before `retain` key-delta
compaction. Existing reload strategies therefore keep their standard meaning:
`retain` keeps enough key deltas to rebuild indexes after journal compaction,
`recovr` keeps its existing recovery contract, and app-defined `recalc` tags can
recompute indexes from app metadata during reload.

Policy: app-defined `recalc` tags are supported for batch writes. The test suite
includes a custom tag with override metadata extraction and index diffing, writes
and updates that tag through `book_batchput`, compacts the journal, removes the
ledger, restarts, and verifies the recalculated secondary indexes.

## Non-Goals

- Cross-bookie transactions.
- Changing head-only `book_mput`.
- Inferring old index removals from stored values. Callers remain responsible
  for passing correct `IndexSpecs`.

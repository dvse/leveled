# Standard-Mode Batch Put PR Notes

## Summary

This change adds `leveled_bookie:book_batchput/2` and
`leveled_bookie:book_batchput/3`, a standard-mode batch write API for atomically
publishing multiple standard objects and their secondary-index changes through
one Bookie request.

The API is storage-engine generic: callers provide standard object puts/deletes
with bucket, key, tag, TTL, and per-object index specs. Leveled writes each
object to the Inker journal using the normal standard-object journal shape, then
inserts all derived object/index rows into the ledger cache before acknowledging
the caller.

## Motivation

Repeated `book_put` calls are insufficient for workloads that need a set of
standard objects to become visible as one logical update. A caller that writes
two indexed standard objects with two separate `book_put` calls can expose the
first object and its index entry to a later read or index fold before the second
object is visible. That creates an intermediate state the caller cannot
distinguish from a committed partial update.

`book_mput` already provides a batch shape for head-only mode, but head-only and
standard mode have different storage contracts. Head-only batches store object
values in the ledger. Standard objects store values in the journal and store
metadata/index rows in the ledger. Reusing `book_mput` would blur those existing
contracts, so this patch adds a separate standard-mode API.

## Public API

```erlang
book_batchput(Bookie, BatchSpecs) -> ok | pause | {error, Reason}.
book_batchput(Bookie, BatchSpecs, DataSync) -> ok | pause | {error, Reason}.
```

`BatchSpecs` is a non-empty list of:

```erlang
{put, Bucket, Key, Object, IndexSpecs, Tag, TTL}
{delete, Bucket, Key, IndexSpecs, Tag, TTL}
```

`DataSync` is a batch-level boolean passed to the journal write.

Return semantics match existing write semantics:

- `ok` means the batch was accepted and is visible to later Bookie requests.
- `pause` means the batch was accepted and is visible, and the caller should
  slow future writes.
- `{error, Reason}` means the batch was rejected before journal or ledger-cache
  visibility.

The public API intentionally does not return the assigned SQN. The SQN remains
an internal journal/ledger coordinate used by the Inker and Bookie; applications
that need patch ordering should persist their own sequence metadata.

The API is rejected in head-only mode and does not change `book_mput`.

## Internal Design

- The Bookie validates the complete batch before journal write.
- Duplicate normalized primary keys are rejected.
- `?HEAD_TAG` and malformed index specs are rejected.
- The Inker allocates one SQN for the whole batch.
- Every object is written as a fetchable standard journal entry:

```erlang
{SQN, ?INKT_STND, LedgerKey} -> JournalValue
```

- The CDB writer appends the complete list through one `cdb_mput` call.
- The Inker returns per-object journal sizes so the Bookie can build normal
  metadata/index ledger rows.
- The Bookie acknowledges only after all derived ledger rows have been inserted
  into the ledger cache.

For startup replay, batch journal values include a small key-change wrapper with
the expected batch object count. If startup sees only a prefix of a same-SQN
batch at an active CDB tail, the replay path ignores that incomplete batch. The
wrapper is removed before normal ledger rebuild and before retain key-delta
compaction.

## Compatibility

This patch is intended to preserve existing behavior:

- `book_put`, `book_delete`, `book_mput`, `book_get`, `book_head`, and
  `book_indexfold` keep their existing contracts.
- `book_mput` remains the head-only batch API.
- Standard-mode batch deletes use the existing tombstone object encoding.
- `pause` keeps existing slow-offer semantics.
- Existing reload strategies keep their standard meanings.

## Test Coverage

Focused tests added in `leveled_bookie` cover:

- standard put/delete fetch and head behavior;
- head-only rejection;
- batch validation;
- active-journal roll and too-large rejection;
- indexfold visibility for add, remove, and add-plus-remove specs;
- crash/restart after journal write and before ledger persistence;
- incomplete CDB tail recovery;
- same-SQN manifest and next-SQN behavior;
- journal compaction under `retain`;
- `recovr` reload and app-defined `recalc`;
- snapshots and folds, including early fold termination;
- hot backup from a batch-written journal-only backup;
- hot backup after replacement/delete batches and retain compaction, restored
  from journal without a ledger;
- 500-object batch reload from journal, verifying every value and secondary
  index;
- `sqn_order` objectfold over multiple same-SQN batch records;
- pause semantics.

Hot backup preserves leveled's existing safety boundary: indexed data is unsafe
with `recovr` because that strategy may discard the key-change history needed to
rebuild a lost ledger. The batch API is covered for the standard `retain` path,
which is the intended HyperBob usage.

Policy note: app-defined `recalc` tags are supported for standard batch writes.
Coverage includes a custom tag with override metadata extraction and index
diffing, batch-written updates, journal compaction, ledger rebuild, restart, and
post-reload secondary-index assertions.

## Non-Goals

- Cross-bookie transactions.
- Changing head-only storage.
- Inferring old index removals from object values.
- Introducing a merge or increment operator.

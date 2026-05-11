# Native CAS APIs

Leveled exposes same-Bookie compare-and-set helpers for callers that need
optimistic atomic writes without adding an external lock service.

## APIs

```erlang
book_get_sqn(Pid, Bucket, Key) -> {ok, Object, SQN} | not_found.
book_get_sqn(Pid, Bucket, Key, Tag) -> {ok, Object, SQN} | not_found.

book_head_sqn(Pid, Bucket, Key) -> {ok, Head, SQN} | not_found.
book_head_sqn(Pid, Bucket, Key, Tag) -> {ok, Head, SQN} | not_found.

book_casput(
    Pid,
    Bucket,
    Key,
    Object,
    IndexSpecs,
    Tag,
    TTL,
    DataSync,
    Condition
) -> ok | pause | {error, term()}.

book_casbatchput(Pid, BatchSpecs, Conditions) ->
    ok | pause | {error, term()}.

book_casbatchput(Pid, BatchSpecs, Conditions, DataSync) ->
    ok | pause | {error, term()}.
```

`Condition` is one of:

```erlang
absent
present
{sqn, SQN}
```

Batch conditions use:

```erlang
{Bucket, Key, Tag, Condition}
```

## Semantics

- Preconditions are evaluated inside the Bookie `gen_server` before any write
  in the CAS batch is accepted.
- `present` and `{sqn, SQN}` match only an active, unexpired object for the
  same bucket, key, and tag. Missing, tombstoned, and TTL-expired objects are
  not active.
- `absent` matches missing, tombstoned, or TTL-expired objects.
- On precondition failure the return value is
  `{error, {precondition_failed, Failures}}`, and no object, tombstone, head,
  or secondary-index changes from the proposed batch are published.
- A `pause` return has the same meaning as `book_put/8` and `book_batchput/3`:
  the write was accepted, and callers should back off before sending more
  writes.
- Conditions may refer to keys outside the write set. This supports reservation
  records and multi-key invariants inside one Bookie.
- Duplicate preconditions for the same `{Bucket, Key, Tag}` are rejected before
  execution as structured precondition diagnostics.
- Invalid condition terms return `{error, invalid_cas_condition}` before any
  write.
- Unsupported condition tags are rejected as structured precondition diagnostics.
- CAS is same-Bookie only. It is not a distributed transaction protocol and it
  does not provide cross-Bookie atomicity.

## Compatibility Risks

- CAS support must be feature-detected by callers. Older Leveled builds do not
  export these functions, and adapters must keep atomic capabilities disabled
  when any required CAS or SQN-read export is absent.
- The CAS APIs are standard-mode APIs. They are not a compatibility promise for
  head-only stores or mixed head-only/object stores.
- CAS relies on the current ledger-key, tag, tombstone, TTL, and SQN semantics.
  Applications that change key/tag encoding or metadata extraction functions
  across upgrades must preserve those semantics for existing objects.
- `pause` is still an accepted-write signal. Retrying a paused CAS operation as
  though it failed can duplicate intended application effects unless the caller
  rereads state and re-evaluates preconditions.
- CAS batches are atomic only inside one Bookie process. Cross-Bookie resources,
  distributed locks, external reservation stores, and side effects need their
  own reconciliation and failure handling.

## Durability, Replay, and Maintenance

- Accepted `book_casput` and `book_casbatchput` calls use the same standard
  journal write path as `book_put` and `book_batchput`.
- A CAS batch is written under one internal SQN. Restart/replay must therefore
  recover the full batch or reject the incomplete same-SQN tail; replay must
  never publish only a prefix of an accepted CAS batch.
- Restart after accepted CAS writes preserves body values, HEAD metadata, index
  rows, tombstones, TTL visibility, and SQN continuity.
- Hot backup, journal compaction, retain reload, and recalc reload treat CAS
  writes as ordinary standard-mode writes once accepted. Stale-rejected CAS
  attempts do not create journal objects, heads, tombstones, or index deltas.
- CAS does not add a new journal encoding. Existing `book_get`, `book_head`,
  key folds, index folds, object folds, backup readers, and reload paths must be
  able to read CAS-created, CAS-updated, and CAS-deleted objects without calling
  CAS-specific APIs.

## Examples

Put if absent:

```erlang
ok = leveled_bookie:book_casput(
    Bookie,
    <<"orders">>,
    <<"tenant-1/order-1">>,
    OrderObject,
    IndexSpecs,
    order_v1,
    infinity,
    false,
    absent
).
```

Replace if the object is still at the SQN just read:

```erlang
{ok, OldObject, SQN} =
    leveled_bookie:book_get_sqn(Bookie, <<"orders">>, <<"tenant-1/order-1">>, order_v1),

NewObject = update_order(OldObject),

ok = leveled_bookie:book_casput(
    Bookie,
    <<"orders">>,
    <<"tenant-1/order-1">>,
    NewObject,
    NewIndexSpecs,
    order_v1,
    infinity,
    false,
    {sqn, SQN}
).
```

Commit a multi-key write only if the object and a reservation record are both
unchanged:

```erlang
ok = leveled_bookie:book_casbatchput(
    Bookie,
    [
        {put, <<"orders">>, <<"tenant-1/order-1">>, NewOrder, OrderIndexes, order_v1, infinity},
        {put, <<"reservations">>, <<"tenant-1/external-id-7">>, Reservation, [], reservation_v1, infinity}
    ],
    [
        {<<"orders">>, <<"tenant-1/order-1">>, order_v1, {sqn, OrderSQN}},
        {<<"reservations">>, <<"tenant-1/external-id-7">>, reservation_v1, absent}
    ]
).
```

If another write wins first, the batch returns
`{error, {precondition_failed, Failures}}` and neither the order nor the
reservation is published.

The focused `leveled_bookie` EUnit suite covers SQN reads, CAS create/update,
stale precondition rejection without index leakage, CAS batch all-or-nothing
publication, validation failures, pause-after-acceptance behavior, TTL/tombstone
as absent semantics, concurrent single-winner behavior, crash/restart replay,
partial-tail rejection, hot backup, retain/recalc compaction reload, and normal
reader compatibility after CAS writes.

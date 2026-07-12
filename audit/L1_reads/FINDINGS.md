# Layer 1 correctness audit: caller-side READ engine

Target: `/Users/dvse/projects/agents/leveled` at HEAD, compared with upstream `7f08bba703c9c4f635da0f53e0f18bae0df59dbb`.

## Findings

### 1. `book_mhead` breaks singular/plural semantics in `head_only=with_lookup` stores

- **Severity:** wrong-result
- **Invariant:** TARGET_API §2 says a plural operation has identical per-key semantics to its singular operation, and `book_mhead/4` itself documents that its results are identical to `book_head/4`. A `head_only=with_lookup` store supports singular HEAD reads, so the corresponding plural HEAD must return an ordered result list with the same value.
- **Expected:** after storing one head-only value, `book_head(Bookie, Bucket, {Key, SubKey}, h)` returns `{ok, Value}` and `book_mhead(Bookie, Bucket, [{Key, SubKey}], h)` returns `[{{Key, SubKey}, {ok, Value}}]`.
- **Actual:** singular HEAD returns `{ok, stored_head_value}`, but plural HEAD returns the top-level tuple `{unsupported_message,mhead}`.
- **Offending code:** `src/leveled_bookie.erl:2452-2454` restricts the `{mhead,...}` handle clause to `State#state.head_only == false`, even though the existing singular HEAD clause supports `head_only=with_lookup`. The public plural contract is at `src/leveled_bookie.erl:1256-1262`.
- **Reproduction:** `./repro_mhead_headonly.escript`

Observed output:

```text
expected=[{{<<"key">>,<<"subkey">>},{ok,stored_head_value}}]
actual={unsupported_message,mhead}
REPRODUCED: plural HEAD is unsupported although singular HEAD succeeds
```

### 2. Value-cache `persistent_term` registrations leak on failed init and untrappable death

- **Severity:** perf (unbounded resource leak; repeated failures retain global persistent terms for dead PIDs)
- **Invariant:** a Bookie that never successfully starts, or one whose ETS cache owner has died, must not leave a permanent global cache registration. Restart/failure cycles must not monotonically grow `persistent_term` state.
- **Expected:** the count of `{{leveled_bookie,valuecache,Pid}, ...}` terms returns to its starting value after each failed initialization or dead Bookie.
- **Actual:** four failed initializations leave four registrations, and killing one successfully initialized Bookie leaves a fifth. Their ETS tables are already gone, so the entries cannot provide cache service and only accumulate stale metadata.
- **Offending code:** `src/leveled_bookie.erl:743-751` registers the cache globally; `src/leveled_bookie.erl:2013` does so before multiple failure-prone startup steps; cleanup exists only in `terminate/2` at `src/leveled_bookie.erl:2847-2849`. OTP does not invoke that callback when `init/1` fails or when the process receives an untrappable `kill` exit.
- **Reproduction:** `./repro_valuecache_persistent_term_leak.escript`

Observed output:

```text
failed_start_results=[error,error,error,error]
kill_cleanup_result=leaked
cache_terms_before=0 after=5 leaked=5
REPRODUCED: failed init and untrappable death leak persistent_term entries
```

The repro copies and compiles the HEAD `leveled_bookie.erl` into `/tmp`; the supplied prebuilt Bookie beam predates commit `9d21968`, which introduced the value cache.

## Suspicion (unreproduced)

`book_mhead` resolves only `current_head_state/2`, while singular `book_head` also performs its historical probabilistic `journal_notfound/4` check (`src/leveled_bookie.erl:2549-2581`). Thus a ledger-positive/journal-missing inconsistency can make plural HEAD return a positive result when singular HEAD returns `not_found`. I did not find a supported lifecycle that leaves this state, so this is not claimed as a finding.

## Coverage

Audited the public `book_get`, `book_mget`, `book_mhead`, `book_get_direct`, and `book_get_sqn` paths; the `{get_fetchspec,...}`, `{mget_fetchspecs,...}`, `{mhead,...}`, direct `{get,...}`, and snapshot hydration clauses; `zip_mget_fetchspecs`; cache init/lookup/insert/bounding and persistent-term lifetime; `ink_mget` grouping/worker/result ordering; and immutable/writer/delete-pending CDB batch reads and pread parsing.

Additional differential probes (not findings) covered:

- 403 CDB queries across writer, immutable-reader, and delete-pending states, including missing and duplicate keys and values of 1, 8,190, 9,000, and 20,000 bytes; every `cdb_mget` result matched per-key `cdb_get`.
- 153 Bookie query entries across 150 objects and multiple journal generations, including reversed order, duplicates, an absent key, tombstone, expired TTL, mixed cache hits/misses, overwrite, delete/recreate, and restart; every `book_mget` result matched ordered per-key `book_get_direct` results.
- A warm cached TTL value expired to `not_found` identically to direct GET.
- 24 concurrent readers over 300 8-KiB values left the cache at 185,656 bytes under a 262,144-byte budget.
- Duplicate keys in one standard batch are rejected with `{error,{duplicate_key,...}}`; ordinary delete/recreate allocates a new SQN. I found no supported same-`{LedgerKey,SQN}` value reuse or stale cache window.

I did not deterministically force the exact live journal-compaction close/truncate interval between RESOLVE and caller pread. The `not_present` and exception fallback code was inspected, and ordinary CDB state transitions were exercised, but that precise scheduler race remains untested. I also did not perform destructive on-disk corruption injection beyond read-only code inspection.

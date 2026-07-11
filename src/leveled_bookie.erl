%% -------- Overview ---------
%%
%% Leveled is based on the LSM-tree similar to leveldb, except that:
%% - Keys, Metadata and Values are not persisted together - the Keys and
%% Metadata are kept in a tree-based ledger, whereas the values are stored
%% only in a sequential Journal.
%% - Different file formats are used for Journal (based on DJ Bernstein
%% constant database), and the ledger (based on sst)
%% - It is not intended to be general purpose, but be primarily suited for
%% use as a Riak backend in specific circumstances (relatively large values,
%% and frequent use of iterators)
%% - The Journal is an extended nursery log in leveldb terms.  It is keyed
%% on the sequence number of the write
%% - The ledger is a merge tree, where the key is the actual object key, and
%% the value is the metadata of the object including the sequence number
%%
%%
%% -------- Actors ---------
%%
%% The store is fronted by a Bookie, who takes support from different actors:
%% - An Inker who persists new data into the journal, and returns items from
%% the journal based on sequence number
%% - A Penciller who periodically redraws the ledger, that associates keys with
%% sequence numbers and other metadata, as well as secondary keys (for index
%% queries)
%% - One or more Clerks, who may be used by either the inker or the penciller
%% to fulfill background tasks
%%
%% Both the Inker and the Penciller maintain a manifest of the files which
%% represent the current state of the Journal and the Ledger repsectively.
%% For the Inker the manifest maps ranges of sequence numbers to cdb files.
%% For the Penciller the manifest maps key ranges to files at each level of
%% the Ledger.
%%

-module(leveled_bookie).

-behaviour(gen_server).

-include("leveled.hrl").

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3,
    book_start/1,
    book_start/4,
    book_plainstart/1,
    book_put/5,
    book_put/6,
    book_put/8,
    book_tempput/7,
    book_batchput/2,
    book_batchput/3,
    book_ftssearch/5,
    book_ftsconsolidate/4,
    book_casput/9,
    book_casbatchput/3,
    book_casbatchput/4,
    book_mput/2,
    book_mput/3,
    book_delete/4,
    book_get/3,
    book_get/4,
    book_mget/3,
    book_mget/4,
    book_mhead/3,
    book_mhead/4,
    book_mput_std/2,
    book_mput_std/3,
    book_casmput/3,
    book_casmput/4,
    book_get_direct/4,
    book_put_direct/8,
    book_get_sqn/3,
    book_get_sqn/4,
    book_head/3,
    book_head/4,
    book_head_sqn/3,
    book_head_sqn/4,
    book_sqn/3,
    book_sqn/4,
    book_headonly/4,
    book_snapshot/4,
    book_compactjournal/2,
    book_islastcompactionpending/1,
    book_lastcompactionresult/1,
    book_trimjournal/1,
    book_hotbackup/1,
    book_close/1,
    book_destroy/1,
    book_isempty/2,
    book_logsettings/1,
    book_loglevel/2,
    book_addlogs/2,
    book_removelogs/2,
    book_headstatus/1,
    book_status/1
]).

%% folding API
-export([
    book_returnfolder/2,
    book_journalfold/4,
    book_journalsqn/1,
    book_indexfold/5,
    book_multiindexfold/5,
    book_bucketlist/4,
    book_keylist/3,
    book_keylist/4,
    book_keylist/5,
    book_keylist/6,
    book_objectfold/4,
    book_objectfold/5,
    book_objectfold/6,
    book_headfold/6,
    book_headfold/7,
    book_headfold/9
]).

-export([
    empty_ledgercache/0,
    snapshot_store/7,
    fetch_value/2,
    journal_notfound/4
]).

-ifdef(TEST).
-export([book_returnactors/1]).
-endif.

% Dummy key used for mput operations
-define(DUMMY, dummy).
-define(PUBLISH_GAP_TIMEOUT_MS, 5000).

-define(OPTION_DEFAULTS, [
    {root_path, undefined},
    {snapshot_bookie, undefined},
    {cache_size, ?CACHE_SIZE},
    {cache_multiple, ?MAX_CACHE_MULTTIPLE},
    {max_journalsize, 1000000000},
    {max_journalobjectcount, 200000},
    {max_sstslots, 256},
    {max_mergebelow, 24},
    {sync_strategy, ?DEFAULT_SYNC_STRATEGY},
    {head_only, false},
    {waste_retention_period, undefined},
    {max_run_length, undefined},
    {singlefile_compactionpercentage, 30.0},
    {maxrunlength_compactionpercentage, 70.0},
    {journalcompaction_scoreonein, 1},
    {reload_strategy, []},
    {max_pencillercachesize, ?MAX_PCL_CACHE_SIZE},
    {ledger_preloadpagecache_level, ?SST_PAGECACHELEVEL_LOOKUP},
    {compression_method, ?COMPRESSION_METHOD},
    {ledger_compression, as_store},
    {block_version, 1},
    {compression_point, ?COMPRESSION_POINT},
    {compression_level, ?COMPRESSION_LEVEL},
    {log_level, ?LOG_LEVEL},
    {forced_logs, []},
    {database_id, ?DEFAULT_DBID},
    {override_functions, []},
    {snapshot_timeout_short, ?SNAPTIMEOUT_SHORT},
    {snapshot_timeout_long, ?SNAPTIMEOUT_LONG},
    {stats_percentage, ?DEFAULT_STATS_PERC},
    {stats_logfrequency, element(1, leveled_monitor:get_defaults())},
    {monitor_loglist, element(2, leveled_monitor:get_defaults())},
    {fts_indexes, []}
]).

-record(ledger_cache, {
    mem :: ets:tab(),
    loader = leveled_tree:empty(?CACHE_TYPE) ::
        tuple() | empty_cache,
    load_queue = [] :: list(),
    index = leveled_pmem:new_index(),
    min_sqn = infinity :: integer() | infinity,
    max_sqn = 0 :: integer()
}).

-record(state, {
    inker :: pid() | null,
    penciller :: pid() | undefined,
    cache_size :: pos_integer() | undefined,
    cache_multiple :: pos_integer() | undefined,
    ledger_cache = #ledger_cache{} :: ledger_cache(),
    is_snapshot :: boolean() | undefined,
    slow_offer = false :: boolean(),
    head_only = false :: boolean(),
    head_lookup = true :: boolean(),
    fts_indexes = [] :: list(),
    %% caller-side write publish gate (TARGET_API §3.2): the ledger-cache
    %% push watermark must never pass an allocated-but-unabsorbed SQN, or
    %% journal replay after a crash would skip an acked write. Absorptions
    %% advance a contiguous frontier; out-of-order publishes buffer.
    publish_frontier = 0 :: non_neg_integer(),
    publish_pending = gb_trees:empty() :: gb_trees:tree(),
    publish_gap_since = undefined :: undefined | erlang:timestamp(),
    fts_seq = 0 :: non_neg_integer(),
    fts_dir_cache :: ets:tid() | undefined,
    ink_checking = ?MAX_KEYCHECK_FREQUENCY :: integer(),
    bookie_monref :: reference() | undefined,
    monitor = {no_monitor, 0} :: leveled_monitor:monitor()
}).

-type book_state() :: #state{}.
-type sync_mode() :: sync | none | riak_sync.
-type ledger_cache() :: #ledger_cache{}.

-type open_options() ::
    %% For full description of options see ../docs/STARTUP_OPTIONS.md
    [
        {root_path, string() | undefined}
        % Folder to be used as the root path for storing all the database
        % information.  Should be undefined is snapshot_bookie is a pid()
        % TODO: Some sort of split root path to allow for mixed classes of
        % storage (e.g. like eleveldb tiered storage - only with
        % separation between ledger and non-current journal)
        | {snapshot_bookie, undefined | pid()}
        % Is the bookie being started required to a be a snapshot of an
        % existing bookie, rather than a new bookie.  The bookie to be
        % snapped should have its pid passed as the startup option in this
        % case
        | {cache_size, pos_integer()}
        % The size of the Bookie's memory, the cache of the recent
        % additions to the ledger.  Defaults to ?CACHE_SIZE, plus some
        % randomised jitter (randomised jitter will still be added to
        % configured values)
        % The minimum value is 100 - any lower value will be ignored
        | {cache_multiple, pos_integer()}
        % A multiple of the cache size beyond which the cache should not
        % grow even if the penciller is busy.  A pasue will be returned for
        % every PUT when this multiple of the cache_size is reached
        | {max_journalsize, pos_integer()}
        % The maximum size of a journal file in bytes.  The absolute
        % maximum must be 4GB due to 4 byte file pointers being used
        | {max_journalobjectcount, pos_integer()}
        % The maximum size of the journal by count of the objects.  The
        % journal must remain within the limit set by both this figures and
        % the max_journalsize
        | {max_sstslots, pos_integer()}
        % The maximum number of slots in a SST file.  All testing is done
        % at a size of 256 (except for Quickcheck tests}, altering this
        % value is not recommended
        | {max_mergeblow, pos_integer() | infinity}
        % The maximum number of files for a single file to be merged into
        % within the ledger.  If less than this, the merge will continue
        % without a maximum.  If this or more overlapping below, only up
        % to max_mergebelow div 2 additions should be created (the merge
        % should be partial)
        | {sync_strategy, sync_mode()}
        % Should be sync if it is necessary to flush to disk after every
        % write, or none if not (allow the OS to schecdule).  This has a
        % significant impact on performance which can be mitigated
        % partially in hardware (e.g through use of FBWC).
        % riak_sync is used for backwards compatability with OTP16 - and
        % will manually call sync() after each write (rather than use the
        % O_SYNC option on startup)
        | {head_only, false | with_lookup | no_lookup}
        % When set to true, there are three fundamental changes as to how
        % leveled will work:
        % - Compaction of the journalwill be managed by simply removing any
        % journal file thathas a highest sequence number persisted to the
        % ledger;
        % - GETs are not supported, only head requests;
        % - PUTs should arrive batched object specs using the book_mput/2
        % function.
        % head_only mode is disabled with false (default).  There are two
        % different modes in which head_only can run with_lookup or
        % no_lookup and heaD_only mode is enabled by passing one of these
        % atoms:
        % - with_lookup assumes that individual objects may need to be
        % fetched;
        % - no_lookup prevents individual objects from being fetched, so
        % that the store can only be used for folds (without segment list
        % acceleration)
        | {waste_retention_period, undefined | pos_integer()}
        % If a value is not required in the journal (i.e. it has been
        % replaced and is now to be removed for compaction) for how long
        % should it be retained.  For example should it be kept for a
        % period until the operator cna be sure a backup has been
        % completed?
        % If undefined, will not retian waste, otherwise the period is the
        % number of seconds to wait
        | {max_run_length, undefined | pos_integer()}
        % The maximum number of consecutive files that can be compacted in
        % one compaction operation.
        % Defaults to leveled_iclerk:?MAX_COMPACTION_RUN (if undefined)
        | {singlefile_compactionpercentage, float()}
        % What is the percentage of space to be recovered from compacting
        % a single file, before that file can be a compaction candidate in
        % a compaction run of length 1
        | {maxrunlength_compactionpercentage, float()}
        % What is the percentage of space to be recovered from compacting
        % a run of max_run_length, before that run can be a compaction
        % candidate.  For runs between 1 and max_run_length, a
        % proportionate score is calculated
        | {journalcompaction_scoreonein, pos_integer()}
        % When scoring for compaction run a probability (1 in x) of whether
        % any file will be scored this run.  If not scored a cached score
        % will be used, and the cached score is the average of the latest
        % score and the rolling average of previous scores
        | {reload_strategy, list()}
        % The reload_strategy is exposed as an option as currently no firm
        % decision has been made about how recovery from failure should
        % work.  For instance if we were to trust everything as permanent
        % in the Ledger once it is persisted, then there would be no need
        % to retain a skinny history of key changes in the Journal after
        % compaction.  If, as an alternative we assume the Ledger is never
        % permanent, and retain the skinny hisory - then backups need only
        % be made against the Journal.  The skinny history of key changes
        % is primarily related to the issue of supporting secondary indexes
        % in Riak.
        %
        % These two strategies are referred to as recovr (assume we can
        % recover any deltas from a lost ledger and a lost history through
        % resilience outside of the store), or retain (retain a history of
        % key changes, even when the object value has been compacted).
        %
        % There is a third strategy, which is recalc, where on reloading
        % the Ledger from the Journal, the key changes are recalculated by
        % comparing the extracted metadata from the Journal object, with the
        % extracted metadata from the current Ledger object it is set to
        % replace (should one be present).  Implementing the recalc
        % strategy requires a override function for
        % `leveled_head:diff_indexspecs/3`.
        % A function for the ?RIAK_TAG is provided and tested.
        %
        % reload_strategy options are a list - to map from a tag to the
        % strategy (recovr|retain|recalc).  Defualt strategies are:
        % [{?RIAK_TAG, retain}, {?STD_TAG, retain}]
        | {max_pencillercachesize, pos_integer() | undefined}
        % How many ledger keys should the penciller retain in memory
        % between flushing new level zero files.
        % Defaults to ?MAX_PCL_CACHE_SIZE when undefined
        % The minimum size 400 - attempt to set this vlaue lower will be
        % ignored.  As a rule the value should be at least 4 x the Bookie's
        % cache size
        | {ledger_preloadpagecache_level, pos_integer()}
        % To which level of the ledger should the ledger contents be
        % pre-loaded into the pagecache (using fadvise on creation and
        % startup)
        | {compression_method, native | lz4 | zstd | none}
        % Compression method and point allow Leveled to be switched from
        % using bif based compression (zlib) to using nif based compression
        % (lz4 or zstd).
        % Defaults to ?COMPRESSION_METHOD
        | {ledger_compression, as_store | native | lz4 | zstd | none}
        % Define an alternative to the compression method to be used by the
        % ledger only.  Default is as_store - use the method defined as
        % compression_method for the whole store
        | {block_version, 0 | 1}
        % Version of the leveled_sst blocks.  Block version 0 does not use
        % sub-blocks, whereas block version 1 has multiple types of blocks
        % which can be split into sub-blocks
        | {compression_point, on_compact | on_receipt}
        % The =compression point can be changed between on_receipt (all
        % values are compressed as they are received), to on_compact where
        % values are originally stored uncompressed (speeding PUT times),
        % and are only compressed when they are first subject to compaction
        % Defaults to ?COMPRESSION_POINT
        | {compression_level, 0..7}
        % At what level of the LSM tree in the ledger should compression be
        % enabled.
        % Defaults to ?COMPRESSION_LEVEL
        | {log_level, debug | info | warn | error | critical}
        % Set the log level.  The default log_level of info is noisy - the
        % current implementation was targetted at environments that have
        % facilities to index large proportions of logs and allow for
        % dynamic querying of those indexes to output relevant stats.
        %
        % As an alternative a higher log_level can be used to reduce this
        % 'noise', however, there is currently no separate stats facility
        % to gather relevant information outside of info level logs.  So
        % moving to higher log levels will at present make the operator
        % blind to sample performance statistics of leveled sub-components
        % etc
        | {forced_logs, list(atom())}
        % Forced logs allow for specific info level logs, such as those
        % logging stats to be logged even when the default log level has
        % been set to a higher log level.  Using:
        % {forced_logs,
        %   [b0015, b0016, b0017, b0018, p0032, sst12]}
        % Will log all timing points even when log_level is not set to
        % support info
        | {database_id, non_neg_integer()}
        % Integer database ID to be used in logs
        | {override_functions, list(leveled_head:appdefinable_function_tuple())}
        % Provide a list of override functions that will be used for
        % user-defined tags
        | {snapshot_timeout_short, pos_integer()}
        % Time in seconds before a snapshot that has not been shutdown is
        % assumed to have failed, and so requires to be torndown.  The
        % short timeout is applied to queries where long_running is set to
        % false
        | {snapshot_timeout_long, pos_integer()}
        % Time in seconds before a snapshot that has not been shutdown is
        % assumed to have failed, and so requires to be torndown.  The
        % short timeout is applied to queries where long_running is set to
        % true
        | {stats_percentage, 0..100}
        % Probability that stats will be collected for an individual
        % request.
        | {stats_logfrequency, pos_integer()}
        % Time in seconds before logging the next timing log. This covers
        % the logs associated with the timing of GET/PUTs in various parts
        % of the system.  There are 7 such logs - so setting to 30s will
        % mean that each inidividual log will occur every 210s
        | {monitor_loglist, list(leveled_monitor:log_type())}
    ].

-type load_item() ::
    {
        leveled_codec:journal_key_tag() | null,
        leveled_codec:primary_key() | ?DUMMY,
        leveled_codec:sqn(),
        dynamic(),
        leveled_codec:journal_keychanges(),
        integer()
    }.

-type initial_loadfun() ::
    fun(
        (
            leveled_codec:journal_key(),
            dynamic(),
            non_neg_integer(),
            {non_neg_integer(), non_neg_integer(), list(load_item())},
            fun((any()) -> {binary(), non_neg_integer()})
        ) ->
            {loop | stop, {
                non_neg_integer(),
                non_neg_integer(),
                list(load_item())
            }}
    ).

-export_type([initial_loadfun/0, ledger_cache/0]).

%%%============================================================================
%%% API
%%%============================================================================

-spec book_start(string(), integer(), integer(), sync_mode()) -> {ok, pid()}.

%% @doc Start a Leveled Key/Value store - limited options support.
%%
%% The most common startup parameters are extracted out from the options to
%% provide this startup method.  This will start a KV store from the previous
%% store at root path - or an empty one if there is no store at the path.
%%
%% Fiddling with the LedgerCacheSize and JournalSize may improve performance,
%% but these are primarily exposed to support special situations (e.g. very
%% low memory installations), there should not be huge variance in outcomes
%% from modifying these numbers.
%%
%% The sync_strategy determines if the store is going to flush writes to disk
%% before returning an ack.  There are three settings currrently supported:
%% - sync - sync to disk by passing the sync flag to the file writer (only
%% works in OTP 18)
%% - riak_sync - sync to disk by explicitly calling data_sync after the write
%% - none - leave it to the operating system to control flushing
%%
%% On startup the Bookie must restart both the Inker to load the Journal, and
%% the Penciller to load the Ledger.  Once the Penciller has started, the
%% Bookie should request the highest sequence number in the Ledger, and then
%% and try and rebuild any missing information from the Journal.
%%
%% To rebuild the Ledger it requests the Inker to scan over the files from
%% the sequence number and re-generate the Ledger changes - pushing the changes
%% directly back into the Ledger.

book_start(RootPath, LedgerCacheSize, JournalSize, SyncStrategy) ->
    book_start(
        set_defaults([
            {root_path, RootPath},
            {cache_size, LedgerCacheSize},
            {max_journalsize, JournalSize},
            {sync_strategy, SyncStrategy}
        ])
    ).

-spec book_start(list(tuple())) -> {ok, pid()} | {error, term()}.

%% @doc Start a Leveled Key/Value store - full options support.
%%
%% For full description of options see ../docs/STARTUP_OPTIONS.md and also
%% comments on the open_options() type

book_start(Opts) ->
    gen_server:start_link(?MODULE, [set_defaults(Opts)], []).

-spec book_plainstart(list(tuple())) -> {ok, pid()}.

%% @doc
%% Start used in tests to start without linking
book_plainstart(Opts) ->
    {ok, Bookie} =
        gen_server:start(?MODULE, [set_defaults(Opts)], []),
    {ok, Bookie}.

-spec book_tempput(
    pid(),
    leveled_codec:key(),
    leveled_codec:key(),
    any(),
    leveled_codec:index_specs(),
    leveled_codec:tag(),
    integer()
) -> ok | pause | {error, term()}.

%% @doc Put an object with an expiry time
%%
%% Put an item in the store but with a Time To Live - the time when the object
%% should expire, in gregorian_seconds (add the required number of seconds to
%% leveled_util:integer_time/1).
%%
%% There exists the possibility of per object expiry times, not just whole
%% store expiry times as has traditionally been the feature in Riak.  Care
%% will need to be taken if implementing per-object times about the choice of
%% reload_strategy.  If expired objects are to be compacted entirely, then the
%% history of KeyChanges will be lost on reload.

book_tempput(
    Pid, Bucket, Key, Object, IndexSpecs, Tag, TTL
) when is_integer(TTL) ->
    book_put(Pid, Bucket, Key, Object, IndexSpecs, Tag, TTL).

%% @doc - Standard PUT
%%
%% A PUT request consists of
%% - A Primary Key and a Value
%% - IndexSpecs - a set of secondary key changes associated with the
%% transaction
%% - A tag indicating the type of object.  Behaviour for metadata extraction,
%% and ledger compaction will vary by type.  There are three currently
%% implemented types i (Index), o (Standard), o_rkv (Riak).  Keys added with
%% Index tags are not fetchable (as they will not be hashed), but are
%% extractable via range query.
%%
%% The extended-arity book_put functions support the addition of an object
%% TTL and a `sync` boolean to flush this PUT (and any other buffered PUTs to
%% disk when the sync_stategy is `none`.
%%
%% The Bookie takes the request and passes it first to the Inker to add the
%% request to the journal.
%%
%% The inker will pass the PK/Value/IndexSpecs to the current (append only)
%% CDB journal file to persist the change.  The call should return either 'ok'
%% or 'roll'. 'roll' indicates that the CDB file has insufficient capacity for
%% this write, and a new journal file should be created (with appropriate
%% manifest changes to be made).
%%
%% The inker will return the SQN which the change has been made at, as well as
%% the object size on disk within the Journal.
%%
%% Once the object has been persisted to the Journal, the Ledger can be updated.
%% The Ledger is updated by the Bookie applying a function (extract_metadata/4)
%% to the Value to return the Object Metadata, a function to generate a hash
%% of the Value and also taking the Primary Key, the IndexSpecs, the Sequence
%% Number in the Journal and the Object Size (returned from the Inker).
%%
%% A set of Ledger Key changes are then generated and placed in the Bookie's
%% Ledger Key cache.
%%
%% The PUT can now be acknowledged.  In the background the Bookie may then
%% choose to push the cache to the Penciller for eventual persistence within
%% the ledger.  This push will either be acccepted or returned (if the
%% Penciller has a backlog of key changes).  The back-pressure should lead to
%% the Bookie entering into a slow-offer status whereby the next PUT will be
%% acknowledged by a PAUSE signal - with the expectation that the this will
%% lead to a back-off behaviour.

book_put(Pid, Bucket, Key, Object, IndexSpecs) ->
    book_put(Pid, Bucket, Key, Object, IndexSpecs, ?STD_TAG).

book_put(Pid, Bucket, Key, Object, IndexSpecs, Tag) ->
    book_put(Pid, Bucket, Key, Object, IndexSpecs, Tag, infinity).

-spec book_put(
    pid(),
    leveled_codec:key(),
    leveled_codec:key(),
    any(),
    leveled_codec:index_specs(),
    leveled_codec:tag(),
    infinity | integer()
) -> ok | pause | {error, term()}.

book_put(Pid, Bucket, Key, Object, IndexSpecs, Tag, TTL) when is_atom(Tag) ->
    book_put(Pid, Bucket, Key, Object, IndexSpecs, Tag, TTL, false).

-spec book_put(
    pid(),
    leveled_codec:key(),
    leveled_codec:key(),
    any(),
    leveled_codec:index_specs(),
    leveled_codec:tag(),
    infinity | integer(),
    boolean()
) -> ok | pause.
book_put(Pid, Bucket, Key, Object, IndexSpecs, Tag, TTL, DataSync) ->
    %% Caller-side write path (TARGET_API §3.2): the journal write (the
    %% disk IO) executes in THIS process via ink_put; the Bookie's
    %% mailbox is touched only by the in-memory {publish, ...} absorb -
    %% concurrent writers no longer serialize behind each other's disk
    %% time inside the Bookie. Writes into FTS-indexed buckets keep the
    %% direct path: their posting augmentation needs Bookie-held shard
    %% state (caller-side FTS is a later stage of the migration).
    %%
    %% write_refs (inker pid + static FTS schema set) is cached in the
    %% caller's process dictionary per Bookie: both are fixed for the
    %% Bookie's lifetime. A dead cached inker falls back to a refresh
    %% and then to the direct path - degradation is a retried write.
    case write_refs(Pid) of
        {ok, Inker, FtsIndexes} ->
            case leveled_fts:bucket_has_schema(Bucket, FtsIndexes) of
                true ->
                    put_caller_side_fts(
                        Pid, Bucket, Key, Object, IndexSpecs, Tag, TTL, DataSync
                    );
                false ->
                    put_caller_side(
                        Pid, Inker, Bucket, Key, Object, IndexSpecs, Tag, TTL,
                        DataSync
                    )
            end;
        unsupported ->
            book_put_direct(Pid, Bucket, Key, Object, IndexSpecs, Tag, TTL, DataSync)
    end.

put_caller_side(Pid, Inker, Bucket, Key, Object, IndexSpecs, Tag, TTL, DataSync) ->
    %% the same public-spec validation the direct path enforces in its
    %% handle_call: forged internal payload specs (fts_term carriers etc.)
    %% must be rejected regardless of which path a caller takes
    case valid_public_index_specs(IndexSpecs) of
        false ->
            {error, invalid_index_specs};
        true ->
            put_caller_side_validated(
                Pid, Inker, Bucket, Key, Object, IndexSpecs, Tag, TTL, DataSync
            )
    end.

put_caller_side_validated(Pid, Inker, Bucket, Key, Object, IndexSpecs, Tag, TTL, DataSync) ->
    LedgerKey = leveled_codec:to_objectkey(Bucket, Key, Tag),
    try
        {ok, SQN, ObjSize} =
            leveled_inker:ink_put(
                Inker, LedgerKey, Object, {IndexSpecs, TTL}, DataSync
            ),
        %% ledger-change preparation is a pure function: it runs here,
        %% in the caller, so PUBLISH is a plain cache absorb
        Changes =
            preparefor_ledgercache(
                null, LedgerKey, SQN, Object, ObjSize, {IndexSpecs, TTL}
            ),
        gen_server:call(Pid, {publish, SQN, Changes}, infinity)
    catch
        _:_ ->
            %% inker restart / journal roll race: refresh refs and take
            %% the direct path, which re-plans under the Bookie
            erlang:erase({leveled_bookie_write_refs, Pid}),
            book_put_direct(Pid, Bucket, Key, Object, IndexSpecs, Tag, TTL, DataSync)
    end.

%% Caller-side FTS write: the same three phases as put_caller_side with
%% the pure augmentation inserted into the IO phase. RESOLVE allocates
%% the fts batch seq; augmentation (tokenisation + page encoding - the
%% dominant CPU of an FTS write) runs here against the static schema
%% set; single-change results take ink_put, multi-change results (shard
%% delta carriers) take ink_batchput - both journal writes in THIS
%% process; PUBLISH absorbs ledger changes + the shard-cache advance
%% behind the frontier. Any race falls back to the direct path.
put_caller_side_fts(Pid, Bucket, Key, Object, IndexSpecs, Tag, TTL, DataSync) ->
    case valid_public_index_specs(IndexSpecs) of
        false ->
            {error, invalid_index_specs};
        true ->
            put_caller_side_fts_validated(
                Pid, Bucket, Key, Object, IndexSpecs, Tag, TTL, DataSync
            )
    end.

put_caller_side_fts_validated(Pid, Bucket, Key, Object, IndexSpecs, Tag, TTL, DataSync) ->
    LedgerKey = leveled_codec:to_objectkey(Bucket, Key, Tag),
    try
        {ok, Seq, FtsIndexes, Inker} =
            gen_server:call(Pid, {fts_put_intent}, infinity),
        case
            leveled_fts:augment_object_changes(
                [{LedgerKey, Object, {IndexSpecs, TTL}}], FtsIndexes, Seq
            )
        of
            {ok, AugChanges, Touched} ->
                Markers =
                    leveled_fts:marker_cache_updates(AugChanges, FtsIndexes),
                FtsAdvance = {Touched, Seq, Seq - 1, Markers},
                publish_fts_changes(
                    Pid, Inker, AugChanges, FtsAdvance, DataSync
                );
            {error, _Reason} ->
                book_put_direct(
                    Pid, Bucket, Key, Object, IndexSpecs, Tag, TTL, DataSync
                )
        end
    catch
        _:_ ->
            erlang:erase({leveled_bookie_write_refs, Pid}),
            book_put_direct(Pid, Bucket, Key, Object, IndexSpecs, Tag, TTL, DataSync)
    end.

publish_fts_changes(Pid, Inker, [{LK, Obj, {Specs, TTL}}], FtsAdvance, DataSync) ->
    {ok, SQN, ObjSize} =
        leveled_inker:ink_put(Inker, LK, Obj, {Specs, TTL}, DataSync),
    Changes = preparefor_ledgercache(null, LK, SQN, Obj, ObjSize, {Specs, TTL}),
    gen_server:call(Pid, {publish_fts, SQN, [Changes], FtsAdvance}, infinity);
publish_fts_changes(Pid, Inker, MultiChanges, FtsAdvance, DataSync) ->
    {ok, SQN, ObjectWriteInfos} =
        leveled_inker:ink_batchput(Inker, MultiChanges, DataSync),
    ChangesList =
        lists:map(
            fun({LK, Obj, KeyChanges, ObjSize}) ->
                preparefor_ledgercache(null, LK, SQN, Obj, ObjSize, KeyChanges)
            end,
            ObjectWriteInfos
        ),
    gen_server:call(Pid, {publish_fts, SQN, ChangesList, FtsAdvance}, infinity).

write_refs(Pid) ->
    CacheKey = {leveled_bookie_write_refs, Pid},
    case erlang:get(CacheKey) of
        {ok, Inker, _FtsIndexes} = Cached ->
            case is_process_alive(Inker) of
                true -> Cached;
                false -> refresh_write_refs(Pid, CacheKey)
            end;
        undefined ->
            refresh_write_refs(Pid, CacheKey)
    end.

refresh_write_refs(Pid, CacheKey) ->
    try gen_server:call(Pid, {write_refs}, infinity) of
        {ok, _Inker, _FtsIndexes} = Refs ->
            erlang:put(CacheKey, Refs),
            Refs;
        _Other ->
            unsupported
    catch
        _:_ -> unsupported
    end.

-spec book_put_direct(
    pid(),
    leveled_codec:key(),
    leveled_codec:key(),
    any(),
    list(),
    leveled_codec:tag(),
    infinity | integer(),
    boolean()
) -> ok | pause | {error, term()}.
%% @doc The strict in-Bookie put: journal write and ledger absorb both
%% execute inside the Bookie's handle_call. Semantically identical to
%% book_put/8 (pinned by put_threephase_differential_test_); the race
%% fallback target, the FTS-bucket path, and the escape hatch for
%% callers that require the write fully serialized through the Bookie.
book_put_direct(Pid, Bucket, Key, Object, IndexSpecs, Tag, TTL, DataSync) ->
    gen_server:call(
        Pid,
        {put, Bucket, Key, Object, IndexSpecs, Tag, TTL, DataSync},
        infinity
    ).

-spec book_batchput(pid(), list(leveled_codec:batch_object_spec())) ->
    ok | pause | {error, term()}.
%% @doc
%%
%% Batch standard-mode object puts/deletes under one Bookie call. The batch is
%% invalid in head_only mode. All objects are written to the Journal under one
%% SQN, and their object/index ledger changes are inserted into the ledger
%% cache before the caller is acknowledged. A `pause' return has the same
%% meaning as book_put/8: the batch has been accepted, and the caller should
%% back off before sending more writes.
-spec book_mput_std(pid(), list(tuple()), boolean()) ->
    ok | pause | {error, term()}.
%% @doc Standard KV plural put (TARGET_API.md §3.2). For standard (not
%% head_only) stores: Entries are the batch write specs; per-entry
%% semantics are identical to N book_put calls, PLUS atomic durability -
%% the batch commits as one journal group, so after a crash either every
%% entry is recoverable or none is. No isolation is claimed; the
%% transaction layer owns isolation. For head_only stores the historical
%% ObjectSpecs/TTL semantics are preserved (dispatched by store mode).
%%
%% This is the target-state name for book_batchput, which is retained as
%% a deprecated alias until consumers migrate.
book_mput_std(Pid, Entries, DataSync) ->
    gen_server:call(Pid, {batchput, Entries, DataSync}, infinity).

book_mput_std(Pid, Entries) ->
    book_mput_std(Pid, Entries, false).

-spec book_casmput(pid(), list(tuple()), list(tuple()), boolean()) ->
    ok | pause | {error, term()}.
%% @doc Plural conditional put: book_mput_std semantics with CAS
%% conditions checked before commit (target state per TARGET_API.md
%% §3.3: per-entry publish-time evaluation; current stage preserves the
%% existing whole-batch precondition contract of casbatchput, which the
%% ash_leveled atomic layer relies on). Target-state name for
%% book_casbatchput (retained as deprecated alias).
book_casmput(Pid, Entries, Conditions, DataSync) ->
    gen_server:call(Pid, {casbatchput, Entries, Conditions, DataSync}, infinity).

book_casmput(Pid, Entries, Conditions) ->
    book_casmput(Pid, Entries, Conditions, false).

%% @deprecated Use book_mput_std/3 (standard stores). Alias retained for
%% consumer migration; removed once ash_leveled is off it.
book_batchput(Pid, BatchSpecs) ->
    book_batchput(Pid, BatchSpecs, false).

-spec book_batchput(
    pid(), list(leveled_codec:batch_object_spec()), boolean()
) ->
    ok | pause | {error, term()}.
%% @doc
%% See book_batchput/2.  DataSync applies to the whole batch.
book_batchput(Pid, BatchSpecs, DataSync) when is_boolean(DataSync) ->
    gen_server:call(Pid, {batchput, BatchSpecs, DataSync}, infinity).

-spec book_ftssearch(
    pid(), leveled_codec:key(), term(), iodata() | all_docs, map() | list()
) ->
    {async, fun(() -> {ok, list(map())} | {error, term()})}.
book_ftssearch(Pid, Bucket, Index, Query, Opts) ->
    leveled_fts:book_ftssearch(Pid, Bucket, Index, Query, Opts).

-spec book_ftsconsolidate(pid(), leveled_codec:key(), binary() | atom(), map()) ->
    {async, fun(() -> ok | noop | {error, term()})} | {error, term()}.
%% @doc
%% Fold every shard's pending posting deltas into its base
%% (docs/FTS.md "Consolidation"). Returns a runner: shards derive in
%% parallel against snapshots in the calling process, then apply in
%% chunks through the bookie. Shard applies are independent — a failure
%% mid-way leaves a partially consolidated, fully consistent store.
%% Concurrent reads and writes are safe. Administrative — callers
%% serialise their own maintenance schedule (one at a time).
book_ftsconsolidate(Pid, Bucket, Index0, Opts) when is_map(Opts) ->
    Index = leveled_fts:normalise_index(Index0),
    gen_server:call(Pid, {ftsconsolidate, Bucket, Index, Opts}, infinity).

-spec book_casput(
    pid(),
    leveled_codec:key(),
    leveled_codec:key(),
    any(),
    leveled_codec:index_specs(),
    leveled_codec:tag(),
    infinity | integer(),
    boolean(),
    absent | present | {sqn, non_neg_integer()}
) ->
    ok | pause | {error, term()}.
%% @doc
%%
%% Compare-and-set standard-mode put. The condition is evaluated against the
%% object identified by Bucket/Key/Tag inside the Bookie gen_server before the
%% object is written. A failed condition returns without writing the object or
%% any index changes.
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
) when is_boolean(DataSync), is_atom(Tag) ->
    book_casbatchput(
        Pid,
        [{put, Bucket, Key, Object, IndexSpecs, Tag, TTL}],
        [{Bucket, Key, Tag, Condition}],
        DataSync
    ).

-spec book_casbatchput(
    pid(),
    list(leveled_codec:batch_object_spec()),
    list({leveled_codec:key(), leveled_codec:key(), leveled_codec:tag(), term()})
) ->
    ok | pause | {error, term()}.
%% @doc
%%
%% Compare-and-set batch publication. All conditions are evaluated before any
%% batch object is accepted. Conditions may refer to keys outside the write set
%% so callers can implement reservation records or multi-key invariants.
book_casbatchput(Pid, BatchSpecs, Conditions) ->
    book_casbatchput(Pid, BatchSpecs, Conditions, false).

-spec book_casbatchput(
    pid(),
    list(leveled_codec:batch_object_spec()),
    list({leveled_codec:key(), leveled_codec:key(), leveled_codec:tag(), term()}),
    boolean()
) ->
    ok | pause | {error, term()}.
%% @doc
%% See book_casbatchput/3. DataSync applies to the whole accepted batch.
book_casbatchput(Pid, BatchSpecs, Conditions, DataSync) when is_boolean(DataSync) ->
    gen_server:call(
        Pid, {casbatchput, BatchSpecs, Conditions, DataSync}, infinity
    ).

-spec book_mput(pid(), list(leveled_codec:object_spec())) -> ok | pause.
%% @doc
%%
%% When the store is being run in head_only mode, batches of object specs may
%% be inserted in to the store using book_mput/2.  ObjectSpecs should be
%% of the form {ObjectOp, Bucket, Key, SubKey, Value}.  The Value will be
%% stored within the HEAD of the object (in the Ledger), so the full object
%% is retrievable using a HEAD request.  The ObjectOp is either add or remove.
%%
%% The list should be de-duplicated before it is passed to the bookie.
book_mput(Pid, ObjectSpecs) ->
    book_mput(Pid, ObjectSpecs, infinity).

-spec book_mput(pid(), list(leveled_codec:object_spec()), infinity | integer()) ->
    ok | pause.
%% @doc
%%
%% When the store is being run in head_only mode, batches of object specs may
%% be inserted in to the store using book_mput/2.  ObjectSpecs should be
%% of the form {action, Bucket, Key, SubKey, Value}.  The Value will be
%% stored within the HEAD of the object (in the Ledger), so the full object
%% is retrievable using a HEAD request.
%%
%% The list should be de-duplicated before it is passed to the bookie.
book_mput(Pid, ObjectSpecs, TTL) ->
    gen_server:call(Pid, {mput, ObjectSpecs, TTL}, infinity).

-spec book_delete(
    pid(),
    leveled_codec:key(),
    leveled_codec:key(),
    leveled_codec:index_specs()
) -> ok | pause.

%% @doc
%%
%% A thin wrap around the put of a special tombstone object.  There is no
%% immediate reclaim of space, simply the addition of a more recent tombstone.

book_delete(Pid, Bucket, Key, IndexSpecs) ->
    book_put(Pid, Bucket, Key, delete, IndexSpecs, ?STD_TAG).

-spec book_get(
    pid(),
    leveled_codec:key(),
    leveled_codec:key(),
    leveled_codec:tag()
) ->
    {ok, any()} | not_found.
-spec book_get_sqn(
    pid(),
    leveled_codec:key(),
    leveled_codec:key(),
    leveled_codec:tag()
) ->
    {ok, any(), non_neg_integer()} | not_found.
-spec book_head(
    pid(),
    leveled_codec:key(),
    leveled_codec:key(),
    leveled_codec:tag()
) ->
    {ok, any()} | not_found.
-spec book_head_sqn(
    pid(),
    leveled_codec:key(),
    leveled_codec:key(),
    leveled_codec:tag()
) ->
    {ok, any(), non_neg_integer()} | not_found.

-spec book_sqn(
    pid(),
    leveled_codec:key(),
    leveled_codec:key(),
    leveled_codec:tag()
) ->
    {ok, non_neg_integer()} | not_found.

-spec book_headonly(
    pid(),
    leveled_codec:key(),
    leveled_codec:key(),
    leveled_codec:key()
) ->
    {ok, any()} | not_found.

%% @doc - GET and HEAD requests
%%
%% The Bookie supports both GET and HEAD requests, with the HEAD request
%% returning only the metadata and not the actual object value.  The HEAD
%% requets cna be serviced by reference to the Ledger Cache and the Penciller.
%%
%% GET requests first follow the path of a HEAD request, and if an object is
%% found, then fetch the value from the Journal via the Inker.
%%
%% to perform a head request in head_only mode with_lookup, book_headonly/4
%% should be used.  Not if head_only mode is false or no_lookup, then this
%% request would not be supported

book_get(Pid, Bucket, Key, Tag) ->
    %% Caller-side execution: the bookie call only creates a ledger
    %% snapshot and returns the inker reference; the head lookup and the
    %% journal body read run in the calling process (the same machinery
    %% as book_mget, without its chunk workers). This keeps large/cold
    %% value reads out of the bookie and inker singletons, so concurrent
    %% readers no longer serialize behind each other's disk IO.
    %%
    %% Any crash in the caller-side read (a journal re-organisation race:
    %% file close/truncation between the plan and the read) falls back to
    %% the direct in-bookie path, which re-plans under the inker's own
    %% serialization and is always correct - degradation is a retried
    %% read, never a wrong answer. The {get, ...} handle_call clause is
    %% retained unchanged as that fallback and as the differential-test
    %% oracle (see get_runner_differential_test_).
    case gen_server:call(Pid, {get_fetchspec, Bucket, Key, Tag}, infinity) of
        not_found ->
            not_found;
        {fetch, LK, SQN, Inker} ->
            try leveled_inker:ink_mget(Inker, [{LK, SQN}]) of
                [{ok, Object}] ->
                    {ok, Object};
                [not_present] ->
                    %% the head said fetch: not_present here is a journal
                    %% re-organisation race, never a real absence - the
                    %% direct path re-plans under the inker's serialization
                    book_get_direct(Pid, Bucket, Key, Tag)
            catch
                _:_ ->
                    book_get_direct(Pid, Bucket, Key, Tag)
            end
    end.

-spec book_get_direct(pid(), leveled_codec:key(), leveled_codec:key(), leveled_codec:tag()) ->
    {ok, any()} | not_found.
%% @doc The strict single-round-trip GET: head lookup and journal fetch
%% both execute inside the bookie's handle_call. Semantically identical
%% to book_get/4 (pinned by differential test); used as the race
%% fallback and available to callers that need the read fully
%% serialized through the bookie.
book_get_direct(Pid, Bucket, Key, Tag) ->
    gen_server:call(Pid, {get, Bucket, Key, Tag}, infinity).

-spec book_mget(
    pid(),
    leveled_codec:key(),
    list(leveled_codec:key()),
    leveled_codec:tag()
) ->
    list({leveled_codec:key(), {ok, any()} | not_found}).
%% @doc - Batched GET
%%
%% Fetch a list of objects from a single bucket, returning results in the
%% same order as the requested Keys.  Semantics per key match book_get/4
%% (tombstone and TTL handling included).
%%
%% The bookie only takes a ledger snapshot; head lookups and the journal
%% reads run in the calling process, with all journal fetches batched into
%% a single inker request (grouped by journal file, files read in
%% parallel).  Many callers can therefore fetch concurrently without
%% serialising one journal round-trip per object through the inker.
book_mget(Pid, Bucket, Keys, Tag) ->
    %% Plural book_get: one bookie call resolves every head inline (no
    %% snapshot - see {mget_fetchspecs, ...}); the caller reads all
    %% journal values in one batched ink_mget. A journal
    %% re-organisation race (crash, or not_present after a positive
    %% head) falls back to the direct per-key path for the affected
    %% keys - a retried read, never a wrong answer.
    {Specs, Inker} = gen_server:call(Pid, {mget_fetchspecs, Bucket, Keys, Tag}, infinity),
    Pairs = [{LK, SQN} || {_Key, {fetch, LK, SQN}} <- Specs],
    Values =
        try
            leveled_inker:ink_mget(Inker, Pairs)
        catch
            _:_ -> race
        end,
    case Values of
        race ->
            [{Key, book_get_direct(Pid, Bucket, Key, Tag)} || {Key, _} <- Specs];
        _ ->
            zip_mget_fetchspecs(Pid, Bucket, Tag, Specs, Values)
    end.

zip_mget_fetchspecs(_Pid, _Bucket, _Tag, [], []) ->
    [];
zip_mget_fetchspecs(Pid, Bucket, Tag, [{Key, not_found} | RestS], Values) ->
    [{Key, not_found} | zip_mget_fetchspecs(Pid, Bucket, Tag, RestS, Values)];
zip_mget_fetchspecs(Pid, Bucket, Tag, [{Key, {fetch, _LK, _SQN}} | RestS], [Value | RestV]) ->
    Result =
        case Value of
            {ok, Object} ->
                {ok, Object};
            not_present ->
                %% positive head + missing journal value = reorganisation
                %% race, never real absence - confirm via the direct path
                book_get_direct(Pid, Bucket, Key, Tag)
        end,
    [{Key, Result} | zip_mget_fetchspecs(Pid, Bucket, Tag, RestS, RestV)].

book_mget(Pid, Bucket, Keys) ->
    book_mget(Pid, Bucket, Keys, ?STD_TAG).

book_get_sqn(Pid, Bucket, Key, Tag) ->
    gen_server:call(Pid, {get_sqn, Bucket, Key, Tag}, infinity).

book_head(Pid, Bucket, Key, Tag) ->
    gen_server:call(Pid, {head, Bucket, Key, Tag, false}, infinity).

-spec book_mhead(pid(), leveled_codec:key(), [leveled_codec:key()], leveled_codec:tag()) ->
    [{leveled_codec:key(), {ok, any()} | not_found}].
%% @doc Plural book_head: one call resolves every head inline (pure
%% in-memory work, no snapshot). Input order preserved, duplicates
%% allowed; per-key semantics identical to book_head/4.
book_mhead(Pid, Bucket, Keys, Tag) ->
    gen_server:call(Pid, {mhead, Bucket, Keys, Tag}, infinity).

book_mhead(Pid, Bucket, Keys) ->
    book_mhead(Pid, Bucket, Keys, ?STD_TAG).

book_head_sqn(Pid, Bucket, Key, Tag) ->
    gen_server:call(Pid, {head_sqn, Bucket, Key, Tag}, infinity).

book_get(Pid, Bucket, Key) ->
    book_get(Pid, Bucket, Key, ?STD_TAG).

book_get_sqn(Pid, Bucket, Key) ->
    book_get_sqn(Pid, Bucket, Key, ?STD_TAG).

book_head(Pid, Bucket, Key) ->
    book_head(Pid, Bucket, Key, ?STD_TAG).

book_head_sqn(Pid, Bucket, Key) ->
    book_head_sqn(Pid, Bucket, Key, ?STD_TAG).

book_headonly(Pid, Bucket, Key, SubKey) ->
    gen_server:call(
        Pid,
        {head, Bucket, {Key, SubKey}, ?HEAD_TAG, false},
        infinity
    ).

book_sqn(Pid, Bucket, Key) ->
    book_sqn(Pid, Bucket, Key, ?STD_TAG).

book_sqn(Pid, Bucket, Key, Tag) ->
    gen_server:call(Pid, {head, Bucket, Key, Tag, true}, infinity).

-spec book_returnfolder(pid(), tuple()) -> {async, fun(() -> dynamic())}.

%% @doc Folds over store - deprecated
%% The tuple() is a query, and book_returnfolder will return an {async, Folder}
%% whereby calling Folder() will run a particular fold over a snapshot of the
%% store, and close the snapshot when complete
%%
%% For any new application requiring a fold - use the API below instead, and
%% one of:
%% - book_indexfold
%% - book_bucketlist
%% - book_keylist
%% - book_headfold
%% - book_objectfold

book_returnfolder(Pid, RunnerType) ->
    gen_server:call(Pid, {return_runner, RunnerType}, infinity).

-spec book_journalfold(
    pid(),
    leveled_codec:tag(),
    non_neg_integer(),
    {fun((term(), term(), non_neg_integer(), {put, term()} | delete, term()) -> term()), term()}
) ->
    {async, fun(() -> dynamic())}.
%% @doc
%% Changefeed fold over the journal from FromSQN (inclusive), in order of
%% receipt. FoldFun(Bucket, Key, SQN, {put, Object} | delete, Acc) is called
%% for every standard put and tombstone of the Tag, including superseded
%% versions. Bounded by the journal SQN at snapshot time; resume from the
%% highest seen SQN + 1. Journal compaction can remove superseded entries,
%% so consumers should not lag indefinitely behind the compaction horizon.
book_journalfold(Pid, Tag, FromSQN, FoldAccT) ->
    book_returnfolder(Pid, {foldobjects_journal, Tag, FromSQN, FoldAccT}).

-spec book_journalsqn(pid()) -> {ok, non_neg_integer()}.
%% @doc
%% The current journal sequence number: the high-water mark for
%% book_journalfold cursors ("start from now" is JournalSQN + 1).
book_journalsqn(Pid) ->
    gen_server:call(Pid, journal_sqn, infinity).

%% Different runner types for async queries:
%% - book_indexfold
%% - book_bucketlist
%% - book_keylist
%% - book_headfold
%% - book_objectfold
%%
%% See individual instructions for each one.  All folds can be completed early
%% by using a fold_function that throws an exception when some threshold is
%% reached - and a worker that catches that exception.
%%
%% See test/end_to_end/iterator_SUITE:breaking_folds/1

%% @doc Builds and returns an `{async, Runner}' pair for secondary
%% index queries. Calling `Runner' will fold over keys (ledger) tagged
%% with the index `?IDX_TAG' and Constrain the fold to a specific
%% `Bucket''s index fields, as specified by the `Constraint'
%% argument. If `Constraint' is a tuple of `{Bucket, Key}' the fold
%% starts at `Key', meaning any keys lower than `Key' and which match
%% the start of the range query, will not be folded over (this is
%% useful for implementing pagination, for example.)
%%
%% Provide a `FoldAccT' tuple of fold fun ( which is 3 arity fun that
%% will be called once per-matching index entry, with the Bucket,
%% Primary Key (or {IndexVal and Primary key} if `ReturnTerms' is
%% true)) and an initial Accumulator, which will be passed as the 3rd
%% argument in the initial call to FoldFun. Subsequent calls to
%% FoldFun will use the previous return of FoldFun as the 3rd
%% argument, and the final return of `Runner' is the final return of
%% `FoldFun', the final Accumulator value. The query can filter inputs
%% based on `Range' and `TermHandling'.  `Range' specifies the name of
%% `IndexField' to query, and `Start' and `End' optionally provide the
%% range to query over.  `TermHandling' is a 2-tuple, the first
%% element is a `boolean()', `true' meaning return terms, (see fold
%% fun above), `false' meaning just return primary keys. `TermRegex'
%% is either a regular expression of type `re:mp()' (that will be run
%% against each index term value, and only those that match will be
%% accumulated) or `undefined', which means no regular expression
%% filtering of index values. NOTE: Regular Expressions can ONLY be
%% run on indexes that have binary or string values, NOT integer
%% values. In the Riak sense of secondary indexes, there are two types
%% of indexes `_bin' indexes and `_int' indexes. Term regex may only
%% be run against the `_bin' type.
%%
%% Any book_indexfold query will fold over the snapshot under the control
%% of the worker process controlling the function - and that process can
%% be interrupted by a throw, which will be forwarded to the worker (whilst
%% still closing down the snapshot).  This may be used, for example, to
%% curtail a fold in the application at max_results
-spec book_indexfold(
    pid(),
    Constraint :: {Bucket, StartKey},
    FoldAccT :: {FoldFun, Acc},
    Range :: {IndexField, Start, End},
    TermHandling :: {ReturnTerms, TermExpression}
) ->
    {async, Runner :: fun(() -> dynamic())}
when
    Bucket :: term(),
    Key :: term(),
    StartKey :: term(),
    FoldFun :: fun((Bucket, Key | {IndexVal, Key}, Acc) -> Acc),
    Acc :: dynamic(),
    IndexField :: term(),
    IndexVal :: term(),
    Start :: IndexVal,
    End :: IndexVal,
    ReturnTerms :: boolean() | binary() | payload,
    TermExpression :: leveled_codec:term_expression().

book_indexfold(Pid, Constraint, FoldAccT, Range, TermHandling) when
    is_tuple(Constraint)
->
    RunnerType =
        {index_query, Constraint, FoldAccT, Range, TermHandling},
    book_returnfolder(Pid, RunnerType);
book_indexfold(Pid, Bucket, FoldAccT, Range, TermHandling) ->
    % StartKey must be specified to avoid confusion when bucket is a tuple.
    % Use an empty StartKey if no StartKey is required (e.g. <<>>).  In a
    % future release this code branch may be removed, and such queries may
    % instead return `error`.  For now null is assumed to be lower than any
    % key
    ?STD_LOG(b0019, [Bucket]),
    book_indexfold(Pid, {Bucket, null}, FoldAccT, Range, TermHandling).

-type query() ::
    {binary(), binary(), binary(), leveled_codec:term_expression()}.
-type combo_fun() ::
    fun((list(sets:set(leveled_codec:key()))) -> sets:set(leveled_codec:key())).

-spec book_multiindexfold(
    pid(),
    leveled_codec:key(),
    {
        fun((leveled_codec:key(), leveled_codec:key(), term()) -> term()),
        term()
    },
    list({non_neg_integer(), query()}),
    combo_fun()
) ->
    {async, fun(() -> term())}.
book_multiindexfold(Pid, Bucket, FoldAccT, Queries, ComboFun) ->
    RunnerType =
        {multi_index_query, Bucket, FoldAccT, Queries, ComboFun},
    book_returnfolder(Pid, RunnerType).

%% @doc list buckets. Folds over the ledger only. Given a `Tag' folds
%% over the keyspace calling `FoldFun' from `FoldAccT' for each
%% `Bucket'. `FoldFun' is a 2-arity function that is passed `Bucket'
%% and `Acc'. On first call `Acc' is the initial `Acc' from
%% `FoldAccT', thereafter the result of the previous call to
%% `FoldFun'. `Constraint' can be either atom `all' or `first' meaning
%% return all buckets, or just the first one found. Returns `{async,
%% Runner}' where `Runner' is a fun that returns the final value of
%% `FoldFun', the final `Acc' accumulator.
-spec book_bucketlist(pid(), Tag, FoldAccT, Constraint) ->
    {async, Runner}
when
    Tag :: leveled_codec:tag(),
    FoldAccT :: {FoldFun, Acc},
    FoldFun :: fun((Bucket, Acc) -> Acc),
    Acc :: dynamic(),
    Constraint :: first | all,
    Bucket :: term(),
    Runner :: fun(() -> Acc).
book_bucketlist(Pid, Tag, FoldAccT, Constraint) ->
    RunnerType =
        case Constraint of
            first -> {first_bucket, Tag, FoldAccT};
            all -> {bucket_list, Tag, FoldAccT}
        end,
    book_returnfolder(Pid, RunnerType).

%% @doc fold over the keys (ledger only) for a given `Tag'. Each key
%% will result in a call to `FoldFun' from `FoldAccT'. `FoldFun' is a
%% 3-arity function, called with `Bucket', `Key' and `Acc'. The
%% initial value of `Acc' is the second element of `FoldAccT'. Returns
%% `{async, Runner}' where `Runner' is a function that will run the
%% fold and return the final value of `Acc'
%%
%% Any book_keylist query will fold over the snapshot under the control
%% of the worker process controlling the function - and that process can
%% be interrupted by a throw, which will be forwarded to the worker (whilst
%% still closing down the snapshot).  This may be used, for example, to
%% curtail a fold in the application at max_results
-spec book_keylist(pid(), Tag, FoldAccT) -> {async, Runner} when
    Tag :: leveled_codec:tag(),
    FoldAccT :: {FoldFun, Acc},
    FoldFun :: fun((Bucket, Key, Acc) -> Acc),
    Acc :: dynamic(),
    Bucket :: term(),
    Key :: term(),
    Runner :: fun(() -> Acc).
book_keylist(Pid, Tag, FoldAccT) ->
    RunnerType = {keylist, Tag, FoldAccT},
    book_returnfolder(Pid, RunnerType).

%% @doc as for book_keylist/3 but constrained to only those keys in
%% `Bucket'
-spec book_keylist(pid(), Tag, Bucket, FoldAccT) -> {async, Runner} when
    Tag :: leveled_codec:tag(),
    FoldAccT :: {FoldFun, Acc},
    FoldFun :: fun((Bucket, Key, Acc) -> Acc),
    Acc :: dynamic(),
    Bucket :: term(),
    Key :: term(),
    Runner :: fun(() -> Acc).
book_keylist(Pid, Tag, Bucket, FoldAccT) ->
    RunnerType = {keylist, Tag, Bucket, FoldAccT},
    book_returnfolder(Pid, RunnerType).

%% @doc as for book_keylist/4 with additional constraint that only
%% keys in the `KeyRange' tuple will be folder over, where `KeyRange'
%% is `StartKey', the first key in the range and `EndKey' the last,
%% (inclusive.) Or the atom `all', which will return all keys in the
%% `Bucket'.
-spec book_keylist(pid(), Tag, Bucket, KeyRange, FoldAccT) ->
    {async, Runner}
when
    Tag :: leveled_codec:tag(),
    FoldAccT :: {FoldFun, Acc},
    FoldFun :: fun((Bucket, Key, Acc) -> Acc),
    Acc :: dynamic(),
    Bucket :: term(),
    KeyRange :: {StartKey, EndKey} | all,
    StartKey :: Key,
    EndKey :: Key,
    Key :: term(),
    Runner :: fun(() -> Acc).
book_keylist(Pid, Tag, Bucket, KeyRange, FoldAccT) ->
    RunnerType = {keylist, Tag, Bucket, KeyRange, FoldAccT, undefined},
    book_returnfolder(Pid, RunnerType).

%% @doc as for book_keylist/5 with additional constraint that a compile regular
%% expression is passed to be applied against any key that is in the range.
%% This is always applied to the Key and only the Key, not to any SubKey.
-spec book_keylist(pid(), Tag, Bucket, KeyRange, FoldAccT, TermRegex) ->
    {async, Runner}
when
    Tag :: leveled_codec:tag(),
    FoldAccT :: {FoldFun, Acc},
    FoldFun :: fun((Bucket, Key, Acc) -> Acc),
    Acc :: dynamic(),
    Bucket :: term(),
    KeyRange :: {StartKey, EndKey} | all,
    StartKey :: Key,
    EndKey :: Key,
    Key :: term(),
    TermRegex :: leveled_codec:term_expression(),
    Runner :: fun(() -> Acc).
book_keylist(Pid, Tag, Bucket, KeyRange, FoldAccT, TermRegex) ->
    RunnerType = {keylist, Tag, Bucket, KeyRange, FoldAccT, TermRegex},
    book_returnfolder(Pid, RunnerType).

%% @doc fold over all the objects/values in the store in key
%% order. `Tag' is the tagged type of object. `FoldAccT' is a 2-tuple,
%% the first element being a 4-arity fun, that is called once for each
%% key with the arguments `Bucket', `Key', `Value', `Acc'. The 2nd
%% element is the initial accumulator `Acc' which is passed to
%% `FoldFun' on it's first call. Thereafter the return value from
%% `FoldFun' is the 4th argument to the next call of
%% `FoldFun'. `SnapPreFold' is a boolean where `true' means take the
%% snapshot at once, and `false' means take the snapshot when the
%% returned `Runner' is executed. Return `{async, Runner}' where
%% `Runner' is a 0-arity function that returns the final accumulator
%% from `FoldFun'
-spec book_objectfold(pid(), Tag, FoldAccT, SnapPreFold) -> {async, Runner} when
    Tag :: leveled_codec:tag(),
    FoldAccT :: {FoldFun, Acc},
    FoldFun :: fun((Bucket, Key, Value, Acc) -> Acc),
    Acc :: dynamic(),
    Bucket :: term(),
    Key :: term(),
    Value :: term(),
    SnapPreFold :: boolean(),
    Runner :: fun(() -> Acc).
book_objectfold(Pid, Tag, FoldAccT, SnapPreFold) ->
    RunnerType = {foldobjects_allkeys, Tag, FoldAccT, SnapPreFold},
    book_returnfolder(Pid, RunnerType).

%% @doc exactly as book_objectfold/4 with the additional parameter
%% `Order'. `Order' can be `sqn_order' or `key_order'. In
%% book_objectfold/4 and book_objectfold/6 `key_order' is
%% implied. This function called with `Option == key_order' is
%% identical to book_objectfold/4. NOTE: if you most fold over ALL
%% objects, this is quicker than `key_order' due to accessing the
%% journal objects in thei ron disk order, not via a fold over the
%% ledger.
-spec book_objectfold(pid(), Tag, FoldAccT, SnapPreFold, Order) ->
    {async, Runner}
when
    Tag :: leveled_codec:tag(),
    FoldAccT :: {FoldFun, Acc},
    FoldFun :: fun((Bucket, Key, Value, Acc) -> Acc),
    Acc :: dynamic(),
    Bucket :: term(),
    Key :: term(),
    Value :: term(),
    SnapPreFold :: boolean(),
    Runner :: fun(() -> Acc),
    Order :: key_order | sqn_order.
book_objectfold(Pid, Tag, FoldAccT, SnapPreFold, Order) ->
    RunnerType = {foldobjects_allkeys, Tag, FoldAccT, SnapPreFold, Order},
    book_returnfolder(Pid, RunnerType).

%% @doc as book_objectfold/4, with the addition of some constraints on
%% the range of objects folded over. The 3rd argument `Bucket' limits
%% ths fold to that specific bucket only. The 4th argument `Limiter'
%% further constrains the fold. `Limiter' can be either a `Range' or
%% `Index' query. `Range' is either that atom `all', meaning {min,
%% max}, or, a two tuple of start key and end key, inclusive. Index
%% Query is a 3-tuple of `{IndexField, StartTerm, EndTerm}`, just as
%% in book_indexfold/5
-spec book_objectfold(pid(), Tag, Bucket, Limiter, FoldAccT, SnapPreFold) ->
    {async, Runner}
when
    Tag :: leveled_codec:tag(),
    FoldAccT :: {FoldFun, Acc},
    FoldFun :: fun((Bucket, Key, Value, Acc) -> Acc),
    Acc :: dynamic(),
    Bucket :: term(),
    Key :: term(),
    Value :: term(),
    Limiter :: Range | Index,
    Range :: {StartKey, EndKey} | all,
    Index :: {IndexField, Start, End},
    IndexField :: term(),
    IndexVal :: term(),
    Start :: IndexVal,
    End :: IndexVal,
    StartKey :: Key,
    EndKey :: Key,
    SnapPreFold :: boolean(),
    Runner :: fun(() -> Acc).
book_objectfold(Pid, Tag, Bucket, Limiter, FoldAccT, SnapPreFold) ->
    RunnerType =
        case Limiter of
            all ->
                {foldobjects_bybucket, Tag, Bucket, all, FoldAccT, SnapPreFold};
            Range when is_tuple(Range) andalso size(Range) == 2 ->
                {foldobjects_bybucket, Tag, Bucket, Range, FoldAccT,
                    SnapPreFold};
            IndexQuery when
                is_tuple(IndexQuery) andalso size(IndexQuery) == 3
            ->
                IndexQuery = Limiter,
                {foldobjects_byindex, Tag, Bucket, IndexQuery, FoldAccT,
                    SnapPreFold}
        end,
    book_returnfolder(Pid, RunnerType).

%% @doc LevelEd stores not just Keys in the ledger, but also may store
%% object metadata, referred to as heads (after Riak head request for
%% object metadata) Often when folding over objects all that is really
%% required is the object metadata. These "headfolds" are an efficient
%% way to fold over the ledger (possibly wholly in memory) and get
%% object metadata.
%%
%% Fold over the object's head. `Tag' is the tagged type of the
%% objects to fold over. `FoldAccT' is a 2-tuple. The 1st element is a
%% 4-arity fold fun, that takes a Bucket, Key, ProxyObject, and the
%% `Acc'. The ProxyObject is an object that only contains the
%% head/metadata, and no object data from the journal. The `Acc' in
%% the first call is that provided as the second element of `FoldAccT'
%% and thereafter the return of the previous all to the fold fun.
%%
%% If `JournalCheck' is `true' then the journal is checked to see if the
%% object in the ledger is present, which means a snapshot of the whole store
%% is required, if `false', then no such check is performed, and only ledger
%% need be snapshotted. However, if the intention is to defer fetching the
%% value but don't wish to cost of chekcing the Journal to be made during the
%% fold (e.g. as any exception will be handled later), then the `defer`
%% option can be used.  This will snapshot the Journal, but not check for
%% presence.  Note that the fetch must still be made within the timefroma of
%% the fold (as the snapshot will expire with the fold).
%%
%% `SnapPreFold' is a boolean that determines if the snapshot is taken when
%% the folder is requested `true', or when when run `false'. `SegmentList' can
%% be `false' meaning, all heads, or a list of integers that designate segments
%% in a TicTac Tree.
-spec book_headfold(
    pid(), Tag, FoldAccT, JournalCheck, SnapPreFold, SegmentList
) ->
    {async, Runner}
when
    Tag :: leveled_codec:tag(),
    FoldAccT :: {FoldFun, Acc},
    FoldFun :: fun((Bucket, Key, Value, Acc) -> Acc),
    Acc :: dynamic(),
    Bucket :: term(),
    Key :: term(),
    Value :: term(),
    JournalCheck :: boolean() | defer,
    SnapPreFold :: boolean(),
    SegmentList :: false | list(integer()),
    Runner :: fun(() -> Acc).
book_headfold(Pid, Tag, FoldAccT, JournalCheck, SnapPreFold, SegmentList) ->
    book_headfold(
        Pid,
        Tag,
        all,
        FoldAccT,
        JournalCheck,
        SnapPreFold,
        SegmentList,
        false,
        false
    ).

%% @doc as book_headfold/6, but with the addition of a `Limiter' that
%% restricts the set of objects folded over. `Limiter' can either be a
%% bucket list, or a key range of a single bucket. For bucket list,
%% the `Limiter' should be a 2-tuple, the first element the tag
%% `bucket_list' and the second a `list()' of `Bucket'. Only heads
%% from the listed buckets will be folded over. A single bucket key
%% range may also be used as a `Limiter', in which case the argument
%% is a 3-tuple of `{range ,Bucket, Range}' where `Bucket' is a
%% bucket, and `Range' is a 2-tuple of start key and end key,
%% inclusive, or the atom `all'. The rest of the arguments are as
%% `book_headfold/6'
-spec book_headfold(
    pid(), Tag, Limiter, FoldAccT, JournalCheck, SnapPreFold, SegmentList
) ->
    {async, Runner}
when
    Tag :: leveled_codec:tag(),
    Limiter :: BucketList | BucketKeyRange,
    BucketList :: {bucket_list, list(Bucket)},
    BucketKeyRange :: {range, Bucket, KeyRange},
    KeyRange :: {StartKey, EndKey} | all,
    StartKey :: Key,
    EndKey :: Key,
    FoldAccT :: {FoldFun, Acc},
    FoldFun :: fun((Bucket, Key, Value, Acc) -> Acc),
    Acc :: dynamic(),
    Bucket :: term(),
    Key :: term(),
    Value :: term(),
    JournalCheck :: boolean() | defer,
    SnapPreFold :: boolean(),
    SegmentList :: false | list(integer()),
    Runner :: fun(() -> Acc).
book_headfold(
    Pid, Tag, Limiter, FoldAccT, JournalCheck, SnapPreFold, SegmentList
) ->
    book_headfold(
        Pid,
        Tag,
        Limiter,
        FoldAccT,
        JournalCheck,
        SnapPreFold,
        SegmentList,
        false,
        false
    ).

%% @doc as book_headfold/7, but with the addition of a Last Modified Date
%% Range and Max Object Count.  For version 2 objects this will filter out
%% all objects with a highest Last Modified Date that is outside of the range.
%% All version 1 objects will be included in the result set regardless of Last
%% Modified Date.
%% The Max Object Count will stop the fold once the count has been reached on
%% this store only.  The Max Object Count if provided will mean that the runner
%% will return {RemainingCount, Acc} not just Acc
-spec book_headfold(
    pid(),
    Tag,
    Limiter,
    FoldAccT,
    JournalCheck,
    SnapPreFold,
    SegmentList,
    LastModRange,
    MaxObjectCount
) ->
    {async, Runner}
when
    Tag :: leveled_codec:tag(),
    Limiter :: BucketList | BucketKeyRange | all,
    BucketList :: {bucket_list, list(Bucket)},
    BucketKeyRange :: {range, Bucket, KeyRange},
    KeyRange :: {StartKey, EndKey} | all,
    StartKey :: Key,
    EndKey :: Key,
    FoldAccT :: {FoldFun, Acc},
    FoldFun :: fun((Bucket, Key, Value, Acc) -> Acc),
    Acc :: dynamic(),
    Bucket :: term(),
    Key :: term(),
    Value :: term(),
    JournalCheck :: boolean() | defer,
    SnapPreFold :: boolean(),
    SegmentList :: false | list(integer()),
    LastModRange :: false | leveled_codec:lastmod_range(),
    MaxObjectCount :: false | pos_integer(),
    Runner :: fun(() -> ResultingAcc),
    ResultingAcc :: Acc | {non_neg_integer(), Acc}.
book_headfold(
    Pid,
    Tag,
    {bucket_list, BucketList},
    FoldAccT,
    JournalCheck,
    SnapPreFold,
    SegmentList,
    LastModRange,
    MaxObjectCount
) ->
    RunnerType =
        {foldheads_bybucket, Tag, BucketList, bucket_list, FoldAccT,
            JournalCheck, SnapPreFold, SegmentList, LastModRange,
            MaxObjectCount},
    book_returnfolder(Pid, RunnerType);
book_headfold(
    Pid,
    Tag,
    {range, Bucket, KeyRange},
    FoldAccT,
    JournalCheck,
    SnapPreFold,
    SegmentList,
    LastModRange,
    MaxObjectCount
) ->
    RunnerType =
        {foldheads_bybucket, Tag, Bucket, KeyRange, FoldAccT, JournalCheck,
            SnapPreFold, SegmentList, LastModRange, MaxObjectCount},
    book_returnfolder(Pid, RunnerType);
book_headfold(
    Pid,
    Tag,
    all,
    FoldAccT,
    JournalCheck,
    SnapPreFold,
    SegmentList,
    LastModRange,
    MaxObjectCount
) ->
    RunnerType =
        {foldheads_allkeys, Tag, FoldAccT, JournalCheck, SnapPreFold,
            SegmentList, LastModRange, MaxObjectCount},
    book_returnfolder(Pid, RunnerType).

-spec book_snapshot(
    pid(), store | ledger, tuple() | no_lookup | undefined, boolean()
) ->
    {ok, pid(), pid() | null}.

%% @doc create a snapshot of the store
%%
%% Snapshot can be based on a pre-defined query (which will be used to filter
%% caches prior to copying for the snapshot), and can be defined as long
%% running to avoid timeouts (snapshots are generally expected to be required
%% for < 60s)

book_snapshot(Pid, SnapType, Query, LongRunning) ->
    gen_server:call(Pid, {snapshot, SnapType, Query, LongRunning}, infinity).

-spec book_compactjournal(pid(), integer()) -> ok | busy.
-spec book_islastcompactionpending(pid()) -> boolean().
-spec book_trimjournal(pid()) -> ok.

%% @doc Call for compaction of the Journal
%%
%% the scheduling of Journla compaction is called externally, so it is assumed
%% in Riak it will be triggered by a vnode callback.

book_compactjournal(Pid, Timeout) ->
    {R, _P} = gen_server:call(Pid, {compact_journal, Timeout}, infinity),
    R.

%% @doc Check on progress of the last compaction

book_islastcompactionpending(Pid) ->
    gen_server:call(Pid, confirm_compact, infinity).

%% @doc Outcome of the most recent journal compaction cycle
%%
%% pending while a cycle is in flight; {done, RunLength} once complete,
%% where RunLength is the number of journal files the cycle compacted
%% (0 = the scorer found no run worth compacting, so an external
%% compact-until-quiescent loop can stop; undefined = no compaction cycle
%% has completed since startup). book_islastcompactionpending/1 cannot
%% answer this - it only reports whether a cycle is running, which makes
%% timing-based caller loops nondeterministic.

-spec book_lastcompactionresult(pid()) ->
    pending | {done, non_neg_integer() | undefined}.
book_lastcompactionresult(Pid) ->
    gen_server:call(Pid, last_compaction_result, infinity).

%% @doc Trim the journal when in head_only mode
%%
%% In head_only mode the journlacna be trimmed of entries which are before the
%% persisted SQN.  This is much quicker than compacting the journal

book_trimjournal(Pid) ->
    gen_server:call(Pid, trim, infinity).

-spec book_close(pid()) -> ok.
-spec book_destroy(pid()) -> ok.

%% @doc Clean shutdown
%%
%% A clean shutdown will persist all the information in the Penciller memory
%% before closing, so shutdown is not instantaneous.
book_close(Pid) ->
    gen_server:call(Pid, close, infinity).

%% @doc Close and clean-out files
book_destroy(Pid) ->
    gen_server:call(Pid, destroy, infinity).

-spec book_hotbackup(pid()) -> {async, fun((string()) -> ok)}.
%% @doc Backup the Bookie
%% Return a function that will take a backup of a snapshot of the Journal.
%% The function will be 1-arity, and can be passed the absolute folder name
%% to store the backup.
%%
%% Backup files are hard-linked.  Does not work in head_only mode, or if
%% index changes are used with a `recovr` compaction/reload strategy
book_hotbackup(Pid) ->
    gen_server:call(Pid, hot_backup, infinity).

-spec book_isempty(pid(), leveled_codec:tag()) -> boolean().
%% @doc
%% Confirm if the store is empty, or if it contains a Key and Value for a
%% given tag
book_isempty(Pid, Tag) ->
    FoldAccT = {fun(_B, _Acc) -> false end, true},
    {async, Runner} = book_bucketlist(Pid, Tag, FoldAccT, first),
    Runner().

-spec book_logsettings(pid()) -> {leveled_log:log_level(), list(string())}.
%% @doc
%% Retrieve the current log settings
book_logsettings(Pid) ->
    gen_server:call(Pid, log_settings, infinity).

-spec book_loglevel(pid(), leveled_log:log_level()) -> ok.
%% @doc
%% Change the log level of the store
book_loglevel(Pid, LogLevel) ->
    gen_server:cast(Pid, {log_level, LogLevel}).

-spec book_addlogs(pid(), list(string())) -> ok.
%% @doc
%% Add to the list of forced logs, a list of more forced logs
book_addlogs(Pid, ForcedLogs) ->
    gen_server:cast(Pid, {add_logs, ForcedLogs}).

-spec book_removelogs(pid(), list(string())) -> ok.
%% @doc
%% Remove from the list of forced logs, a list of forced logs
book_removelogs(Pid, ForcedLogs) ->
    gen_server:cast(Pid, {remove_logs, ForcedLogs}).

-spec book_headstatus(pid()) -> {boolean(), boolean()}.
%% @doc
%% Return booleans to state the bookie is in head_only mode, and supporting
%% lookups
book_headstatus(Pid) ->
    gen_server:call(Pid, head_status, infinity).

-spec book_status(pid()) -> map().
%% @doc
%% Return a proplist containing the following items:
%% * current size of the ledger cache;
%% * number of active journal files;
%% * average compaction score for the journal;
%% * current distribution of files across the ledger (e.g. count of files by level);
%% * current size of the penciller in-memory cache;
%% * penciller work backlog status;
%% * last merge time (penciller);
%% * last compaction time (journal);
%% * last compaction result (journal) e.g. files compacted and compaction score;
%% * ratio of metadata to object size (recent PUTs);
%% * PUT/GET/HEAD recent time/count metrics;
%% * mean level for recent fetches.
book_status(Pid) ->
    gen_server:call(Pid, status, infinity).

%%%============================================================================
%%% gen_server callbacks
%%%============================================================================

-spec init([open_options()]) -> {ok, book_state()} | {stop, atom()}.
init([Opts]) ->
    case
        {
            proplists:get_value(snapshot_bookie, Opts),
            proplists:get_value(root_path, Opts)
        }
    of
        {undefined, undefined} ->
            {stop, no_root_path};
        {undefined, _RP} ->
            % Start from file not snapshot

            % Must set log level first - as log level will be fetched within
            % set_options/1.  Also logs can now be added to set_options/1
            LogLevel = proplists:get_value(log_level, Opts),
            leveled_log:set_loglevel(LogLevel),
            ForcedLogs = proplists:get_value(forced_logs, Opts),
            leveled_log:add_forcedlogs(ForcedLogs),
            DatabaseID = proplists:get_value(database_id, Opts),
            leveled_log:set_databaseid(DatabaseID),
            case
                leveled_fts:normalise_indexes(
                    proplists:get_value(fts_indexes, Opts)
                )
            of
                {ok, FtsIndexes} ->

            {ok, Monitor} =
                leveled_monitor:monitor_start(
                    proplists:get_value(stats_logfrequency, Opts),
                    proplists:get_value(monitor_loglist, Opts)
                ),
            StatLogFrequency = proplists:get_value(stats_percentage, Opts),

            {InkerOpts, PencillerOpts} =
                set_options(Opts, {Monitor, StatLogFrequency}),

            OverrideFunctions = proplists:get_value(override_functions, Opts),
            SetFun =
                fun({FuncName, Func}) ->
                    application:set_env(leveled, FuncName, Func)
                end,
            lists:foreach(SetFun, OverrideFunctions),

            ConfiguredCacheSize =
                max(proplists:get_value(cache_size, Opts), ?MIN_CACHE_SIZE),
            CacheJitter =
                max(1, ConfiguredCacheSize div (100 div ?CACHE_SIZE_JITTER)),
            CacheSize =
                ConfiguredCacheSize + erlang:phash2(self()) rem CacheJitter,
            MaxCacheMultiple =
                proplists:get_value(cache_multiple, Opts),
            PCLMaxSize =
                PencillerOpts#penciller_options.max_inmemory_tablesize,
            CacheRatio = PCLMaxSize div ConfiguredCacheSize,
            % It is expected that the maximum size of the penciller
            % in-memory store should not be more than about 10 x the size
            % of the ledger cache.  In this case there will be a larger
            % than tested list of ledger_caches in the penciller memory,
            % and performance may be unpredictable
            case CacheRatio > 32 of
                true ->
                    ?STD_LOG(b0020, [PCLMaxSize, ConfiguredCacheSize]);
                false ->
                    ok
            end,

            PageCacheLevel = proplists:get_value(
                ledger_preloadpagecache_level, Opts
            ),

            {HeadOnly, HeadLookup, SSTPageCacheLevel} =
                case proplists:get_value(head_only, Opts) of
                    false ->
                        {false, true, PageCacheLevel};
                    with_lookup ->
                        {true, true, PageCacheLevel};
                    no_lookup ->
                        {true, false, ?SST_PAGECACHELEVEL_NOLOOKUP}
                end,
            % Override the default page cache level - we want to load into the
            % page cache many levels if we intend to support lookups, and only
            % levels 0 and 1 otherwise
            SSTOpts = PencillerOpts#penciller_options.sst_options,
            SSTOpts0 = SSTOpts#sst_options{pagecache_level = SSTPageCacheLevel},
            PencillerOpts0 =
                PencillerOpts#penciller_options{sst_options = SSTOpts0},

            {Inker, Penciller} = startup(InkerOpts, PencillerOpts0),

            %% Load the modules exercised by the put/fold hot paths now, so
            %% the first operations do not pay code-loading latency.
            _ = code:ensure_modules_loaded([
                leveled_cdb, leveled_sst, leveled_penciller, leveled_pmem,
                leveled_ebloom, leveled_iclerk, leveled_inker, leveled_codec,
                leveled_head, leveled_tictac, leveled_util, leveled_runner,
                leveled_fts, zlib
            ]),
            NewETS = ets:new(mem, [ordered_set]),
            ?STD_LOG(b0001, [Inker, Penciller]),
            FtsSeq =
                case FtsIndexes of
                    [] ->
                        0;
                    _ ->
                        {ok, JournalSQN} = leveled_inker:ink_getjournalsqn(Inker),
                        JournalSQN
                end,
            FtsDirCache = maybe_new_fts_dir_cache(FtsIndexes),
            {ok, PublishFrontier} = leveled_inker:ink_getjournalsqn(Inker),
            {ok, #state{
                cache_size = CacheSize,
                cache_multiple = MaxCacheMultiple,
                is_snapshot = false,
                publish_frontier = PublishFrontier,
                head_only = HeadOnly,
                head_lookup = HeadLookup,
                inker = Inker,
                penciller = Penciller,
                fts_indexes = FtsIndexes,
                fts_seq = FtsSeq,
                fts_dir_cache = FtsDirCache,
                ledger_cache = #ledger_cache{mem = NewETS},
                monitor = {Monitor, StatLogFrequency}
            }};
                {error, Reason} ->
                    {stop, Reason}
            end;
        {Bookie, undefined} ->
            {ok, Penciller, Inker} =
                book_snapshot(Bookie, store, undefined, true),
            FtsIndexes = gen_server:call(Bookie, fts_indexes, infinity),
            FtsDirCache = maybe_new_fts_dir_cache(FtsIndexes),
            BookieMonitor = erlang:monitor(process, Bookie),
            NewETS = ets:new(mem, [ordered_set]),
            {HeadOnly, Lookup} = leveled_bookie:book_headstatus(Bookie),
            ?STD_LOG(b0002, [Inker, Penciller]),
            {ok, #state{
                penciller = Penciller,
                inker = Inker,
                ledger_cache = #ledger_cache{mem = NewETS},
                head_only = HeadOnly,
                head_lookup = Lookup,
                fts_indexes = FtsIndexes,
                fts_dir_cache = FtsDirCache,
                bookie_monref = BookieMonitor,
                is_snapshot = true
            }}
    end.

handle_call(
    {put, Bucket, Key, Object, IndexSpecs, Tag, TTL, DataSync},
    From,
    State
) when
    State#state.head_only == false, Tag =/= ?HEAD_TAG
->
    LedgerKey = leveled_codec:to_objectkey(Bucket, Key, Tag),
    case valid_public_index_specs(IndexSpecs) of
        false ->
            gen_server:reply(From, {error, invalid_index_specs}),
            {noreply, State};
        true ->
            case
                augment_fts_object_changes(
                    [{LedgerKey, Object, {IndexSpecs, TTL}}], State
                )
            of
                {error, FtsReason} ->
                    gen_server:reply(From, {error, FtsReason}),
                    {noreply, State};
                {[{LedgerKey, Object, {AugIndexSpecs, TTL}}], State0, FtsAdvance} ->
                    do_augmented_put(
                        LedgerKey, Object, AugIndexSpecs, TTL, DataSync, From, State0,
                        FtsAdvance
                    );
                {MultiChanges, State0, FtsAdvance} ->
                    %% the write derived per-shard delta carriers: commit
                    %% object plus carriers as one atomic batch.
                    do_batchput(MultiChanges, DataSync, From, State0, FtsAdvance)
            end
    end;
handle_call({ftsconsolidate, Bucket, Index, _Opts}, _From, State) when
    State#state.head_only == false
->
    case leveled_fts:find_schema(Bucket, Index, State#state.fts_indexes) of
        {ok, Schema} ->
            Ref = leveled_fts:index_ref(Schema),
            SnapFun = return_snapfun(State, ledger, no_lookup, false, true),
            Inker = State#state.inker,
            Self = self(),
            Runner =
                fun() ->
                    {ok, LS0, _J0, After0} = SnapFun(),
                    Plan =
                        try
                            leveled_fts:consolidate_plan(
                                fts_fold_source(LS0, Inker), Bucket, Ref
                            )
                        catch
                            throw:{fts_error, Reason0} -> {error, Reason0}
                        after
                            After0()
                        end,
                    case Plan of
                        {error, _R} = Error -> Error;
                        [] -> noop;
                        Shards ->
                            run_fts_consolidation(
                                Self, Bucket, Ref, Schema, Shards, Inker
                            )
                    end
                end,
            {reply, {async, Runner}, State};
        _NotFound ->
            {reply, {error, missing_fts_schema}, State}
    end;
handle_call({ftsconsolidate_apply, Bucket, Ref, Results}, From, State) when
    State#state.head_only == false
->
    {Index, Tag} = Ref,
    Field = {fts_term, Index, Tag},
    %% reserved objects (bases, carriers) live under the STANDARD tag.
    Changes =
        lists:append([
            [
                {leveled_codec:to_objectkey(
                     Bucket, leveled_fts:base_object_key(Index, Shard), ?STD_TAG
                 ),
                    Base,
                    {[
                         {add_payload, Field, leveled_fts:summary_term(Shard),
                             Summary}
                     ],
                        infinity}},
                {leveled_codec:to_objectkey(
                     Bucket, leveled_fts:delta_carrier_key(Index, Shard), ?STD_TAG
                 ),
                    <<0>>,
                    {[
                         {remove, Field, leveled_fts:delta_term(Shard, Seq)}
                      || Seq <- Consumed
                     ],
                        infinity}}
            ]
         || #{shard := Shard, base := Base, summary := Summary,
                consumed := Consumed} <- Results
        ]),
    Updates =
        [
            {Shard, ConsSeq, Consumed}
         || #{shard := Shard, cons_seq := ConsSeq, consumed := Consumed} <- Results
        ],
    do_batchput(
        Changes, false, From, State,
        {consolidate, Bucket, Ref, Updates}
    );
handle_call({batchput, BatchSpecs, DataSync}, From, State) when
    State#state.head_only == false
->
    case normalise_batch_specs(BatchSpecs) of
        {ok, ObjectChanges} ->
            case augment_fts_object_changes(ObjectChanges, State) of
                {error, FtsReason} ->
                    gen_server:reply(From, {error, FtsReason}),
                    {noreply, State};
                {AugObjectChanges, State0, FtsAdvance} ->
                    do_batchput(AugObjectChanges, DataSync, From, State0, FtsAdvance)
            end;
        {error, Reason} ->
            gen_server:reply(From, {error, Reason}),
            {noreply, State}
    end;
handle_call({casbatchput, BatchSpecs, Conditions, DataSync}, From, State) when
    State#state.head_only == false
->
    case normalise_batch_specs(BatchSpecs) of
        {ok, ObjectChanges} ->
            case normalise_cas_conditions(Conditions) of
                {ok, CasConditions} ->
                    case check_cas_conditions(CasConditions, State) of
                        ok ->
                            case augment_fts_object_changes(ObjectChanges, State) of
                                {error, FtsReason} ->
                                    gen_server:reply(From, {error, FtsReason}),
                                    {noreply, State};
                                {AugObjectChanges, State0, FtsAdvance} ->
                                    do_batchput(
                                        AugObjectChanges, DataSync, From, State0,
                                        FtsAdvance
                                    )
                            end;
                        {error, Failures} ->
                            gen_server:reply(
                                From, {error, {precondition_failed, Failures}}
                            ),
                            {noreply, State}
                    end;
                {error, Reason} ->
                    gen_server:reply(From, {error, Reason}),
                    {noreply, State}
            end;
        {error, Reason} ->
            gen_server:reply(From, {error, Reason}),
            {noreply, State}
    end;
handle_call({mput, ObjectSpecs, TTL}, From, State) when
    State#state.head_only == true
->
    {ok, SQN} =
        leveled_inker:ink_mput(State#state.inker, dummy, {ObjectSpecs, TTL}),
    Changes =
        preparefor_ledgercache(
            ?INKT_MPUT,
            ?DUMMY,
            SQN,
            null,
            length(ObjectSpecs),
            {ObjectSpecs, TTL}
        ),
    Cache0 = addto_ledgercache(Changes, State#state.ledger_cache),
    case State#state.slow_offer of
        true ->
            gen_server:reply(From, pause);
        false ->
            gen_server:reply(From, ok)
    end,
    StateA = absorb_sqns([SQN], State),
    case maybe_gated_push(Cache0, StateA) of
        {{ok, Cache}, StateB} ->
            {noreply, StateB#state{ledger_cache = Cache, slow_offer = false}};
        {{returned, Cache}, StateB} ->
            {noreply, StateB#state{ledger_cache = Cache, slow_offer = true}}
    end;
handle_call({publish, SQN, Changes}, _From, State) when
    State#state.head_only == false
->
    %% PUBLISH phase of the caller-side write path (TARGET_API §3.2):
    %% the journal write already happened in the caller; this is a pure
    %% in-memory ledger-cache absorb. Reply carries the same ok|pause
    %% backpressure contract as the direct put path.
    Cache0 = addto_ledgercache(Changes, State#state.ledger_cache),
    Reply =
        case State#state.slow_offer of
            true -> pause;
            false -> ok
        end,
    StateA = absorb_sqns([SQN], State),
    case maybe_gated_push(Cache0, StateA) of
        {{ok, Cache}, StateB} ->
            {reply, Reply, StateB#state{ledger_cache = Cache, slow_offer = false}};
        {{returned, Cache}, StateB} ->
            {reply, Reply, StateB#state{ledger_cache = Cache, slow_offer = true}}
    end;
handle_call({write_refs}, _From, State) when
    State#state.head_only == false
->
    %% RESOLVE-adjacent: static references for the caller-side write
    %% path. FtsIndexes is start-time configuration, so callers may
    %% cache this reply for the bookie's lifetime.
    {reply, {ok, State#state.inker, State#state.fts_indexes}, State};
handle_call({fts_put_intent}, _From, State) when
    State#state.head_only == false
->
    %% RESOLVE for a caller-side FTS write (TARGET_API §3.2, FTS stage):
    %% allocates the batch sequence the caller's augmentation embeds in
    %% its posting deltas. Pure in-memory counter bump; the heavy
    %% derivation (tokenisation, page encoding) runs caller-side against
    %% the static index set, and the resulting cache advance rides the
    %% caller's {publish_fts, ...} through the absorption frontier.
    Seq = State#state.fts_seq + 1,
    {reply, {ok, Seq, State#state.fts_indexes, State#state.inker},
        State#state{fts_seq = Seq}};
handle_call({publish_fts, SQN, ChangesList, FtsAdvance}, _From, State) when
    State#state.head_only == false
->
    %% PUBLISH for a caller-side FTS write: identical to {publish, ...}
    %% plus the FTS cache advance, which applies only when this SQN
    %% joins the contiguous frontier (completeness-honest stamping).
    Cache0 =
        lists:foldl(
            fun addto_ledgercache/2, State#state.ledger_cache, ChangesList
        ),
    Reply =
        case State#state.slow_offer of
            true -> pause;
            false -> ok
        end,
    StateA = absorb_sqn(SQN, fts_advance_or_none(FtsAdvance), State),
    case maybe_gated_push(Cache0, StateA) of
        {{ok, Cache}, StateB} ->
            {reply, Reply, StateB#state{ledger_cache = Cache, slow_offer = false}};
        {{returned, Cache}, StateB} ->
            {reply, Reply, StateB#state{ledger_cache = Cache, slow_offer = true}}
    end;
handle_call({get, Bucket, Key, Tag}, _From, State) when
    State#state.head_only == false
->
    LedgerKey = leveled_codec:to_objectkey(Bucket, Key, Tag),
    SW0 = leveled_monitor:maybe_time(State#state.monitor),
    {H0, _CacheHit} =
        fetch_head(
            LedgerKey,
            State#state.penciller,
            State#state.ledger_cache
        ),
    HeadResult =
        case H0 of
            not_present ->
                not_found;
            Head ->
                {Seqn, Status, _MH, _MD} =
                    leveled_codec:striphead_to_v1details(Head),
                case Status of
                    tomb ->
                        not_found;
                    {active, TS} ->
                        case TS >= leveled_util:integer_now() of
                            false ->
                                not_found;
                            true ->
                                {LedgerKey, Seqn}
                        end
                end
        end,
    {TS0, SW1} = leveled_monitor:step_time(SW0),
    GetResult =
        case HeadResult of
            not_found ->
                not_found;
            {LK, SQN} ->
                Object = fetch_value(State#state.inker, {LK, SQN}),
                case Object of
                    not_present ->
                        not_found;
                    _ ->
                        {ok, Object}
                end
        end,
    {TS1, _SW2} = leveled_monitor:step_time(SW1),
    maybelog_get_timing(
        State#state.monitor, TS0, TS1, GetResult == not_found
    ),
    {reply, GetResult, State};
handle_call({get_fetchspec, Bucket, Key, Tag}, _From, State) when
    State#state.head_only == false
->
    %% Caller-side GET (see book_get/4): the bookie resolves the head
    %% INLINE - the same cheap in-memory work the direct {get, ...} path
    %% does (ledger cache + penciller, no snapshot, no ledger-cache
    %% clone) - and returns the journal fetch spec; the caller performs
    %% the disk read via a single-pair ink_mget. A value at a given SQN
    %% is immutable, so reading it after the reply is identical to
    %% reading it inside the call; the only divergence is journal
    %% compaction reaping the entry in between, which surfaces as
    %% not_present/crash and falls back to the direct path.
    LedgerKey = leveled_codec:to_objectkey(Bucket, Key, Tag),
    Reply =
        case current_head_state(LedgerKey, State) of
            {active, Seqn, _MD} ->
                {fetch, LedgerKey, Seqn, State#state.inker};
            _Other ->
                not_found
        end,
    {reply, Reply, State};
handle_call({mhead, Bucket, Keys, Tag}, _From, State) when
    State#state.head_only == false
->
    %% Plural head: N inline resolutions, no snapshot (TARGET_API §3.1).
    Results =
        lists:map(
            fun(Key) ->
                LedgerKey = leveled_codec:to_objectkey(Bucket, Key, Tag),
                case current_head_state(LedgerKey, State) of
                    {active, _Seqn, MD} ->
                        {Key, {ok, leveled_head:build_head(Tag, MD)}};
                    _Other ->
                        {Key, not_found}
                end
            end,
            Keys
        ),
    {reply, Results, State};
handle_call({mget_fetchspecs, Bucket, Keys, Tag}, _From, State) when
    State#state.head_only == false
->
    %% Plural form of {get_fetchspec, ...}: N inline head resolutions in
    %% one call - the same cheap in-memory work per key as the singular,
    %% with NO ledger snapshot (a snapshot clones the write-heavy ledger
    %% cache, measured as 3-37x regressions when paid per small batch).
    %% The caller performs the journal reads for all fetch specs via one
    %% batched ink_mget. Per-key semantics are pinned to book_get by
    %% mget_fetchspec_differential_test_.
    Inker = State#state.inker,
    Specs =
        lists:map(
            fun(Key) ->
                LedgerKey = leveled_codec:to_objectkey(Bucket, Key, Tag),
                case current_head_state(LedgerKey, State) of
                    {active, Seqn, _MD} -> {Key, {fetch, LedgerKey, Seqn}};
                    _Other -> {Key, not_found}
                end
            end,
            Keys
        ),
    {reply, {Specs, Inker}, State};
handle_call({mget, Bucket, Keys, Tag}, _From, State) when
    State#state.head_only == false
->
    SnapFun = return_snapfun(State, ledger, no_lookup, false, true),
    Inker = State#state.inker,
    Runner =
        fun() ->
            {ok, LS, _JS, AfterFun} = SnapFun(),
            try
                mget_objects(LS, Inker, Bucket, Keys, Tag)
            after
                AfterFun()
            end
        end,
    {reply, {async, Runner}, State};
handle_call({get_sqn, Bucket, Key, Tag}, _From, State) when
    State#state.head_only == false
->
    LedgerKey = leveled_codec:to_objectkey(Bucket, Key, Tag),
    SW0 = leveled_monitor:maybe_time(State#state.monitor),
    HeadResult =
        case current_head_state(LedgerKey, State) of
            {active, Seqn, _MD} ->
                {LedgerKey, Seqn};
            _Other ->
                not_found
        end,
    {TS0, SW1} = leveled_monitor:step_time(SW0),
    GetResult =
        case HeadResult of
            not_found ->
                not_found;
            {LK, SQN} ->
                Object = fetch_value(State#state.inker, {LK, SQN}),
                case Object of
                    not_present ->
                        not_found;
                    _ ->
                        {ok, Object, SQN}
                end
        end,
    {TS1, _SW2} = leveled_monitor:step_time(SW1),
    maybelog_get_timing(
        State#state.monitor, TS0, TS1, GetResult == not_found
    ),
    {reply, GetResult, State};
handle_call({head, Bucket, Key, Tag, SQNOnly}, _From, State) when
    State#state.head_lookup == true
->
    SW0 = leveled_monitor:maybe_time(State#state.monitor),
    LK = leveled_codec:to_objectkey(Bucket, Key, Tag),
    {Head, CacheHit} =
        fetch_head(
            LK,
            State#state.penciller,
            State#state.ledger_cache,
            State#state.head_only
        ),
    {TS0, SW1} = leveled_monitor:step_time(SW0),
    JrnalCheckFreq =
        case State#state.head_only of
            true ->
                0;
            false ->
                State#state.ink_checking
        end,
    {LedgerMD, SQN, UpdJrnalCheckFreq} =
        case Head of
            not_present ->
                {not_found, null, JrnalCheckFreq};
            Head ->
                case leveled_codec:striphead_to_v1details(Head) of
                    {_SeqN, tomb, _MH, _MD} ->
                        {not_found, null, JrnalCheckFreq};
                    {SeqN, {active, TS}, _MH, MD} ->
                        case TS >= leveled_util:integer_now() of
                            true ->
                                I = State#state.inker,
                                case
                                    journal_notfound(
                                        JrnalCheckFreq, I, LK, SeqN
                                    )
                                of
                                    {true, UppedFrequency} ->
                                        {not_found, null, UppedFrequency};
                                    {false, ReducedFrequency} ->
                                        {MD, SeqN, ReducedFrequency}
                                end;
                            false ->
                                {not_found, null, JrnalCheckFreq}
                        end
                end
        end,
    Reply =
        case {LedgerMD, SQNOnly} of
            {not_found, _} ->
                not_found;
            {LedgerMD, false} when LedgerMD =/= null ->
                {ok, leveled_head:build_head(Tag, LedgerMD)};
            {_, true} ->
                {ok, SQN}
        end,
    {TS1, _SW2} = leveled_monitor:step_time(SW1),
    maybelog_head_timing(
        State#state.monitor, TS0, TS1, LedgerMD == not_found, CacheHit
    ),
    case UpdJrnalCheckFreq of
        JrnalCheckFreq ->
            {reply, Reply, State};
        UpdJrnalCheckFreq ->
            {reply, Reply, State#state{ink_checking = UpdJrnalCheckFreq}}
    end;
handle_call({head_sqn, Bucket, Key, Tag}, _From, State) when
    State#state.head_lookup == true
->
    SW0 = leveled_monitor:maybe_time(State#state.monitor),
    LK = leveled_codec:to_objectkey(Bucket, Key, Tag),
    {Head, CacheHit} =
        fetch_head(
            LK,
            State#state.penciller,
            State#state.ledger_cache,
            State#state.head_only
        ),
    {TS0, SW1} = leveled_monitor:step_time(SW0),
    JrnalCheckFreq =
        case State#state.head_only of
            true ->
                0;
            false ->
                State#state.ink_checking
        end,
    {LedgerMD, SQN, UpdJrnalCheckFreq} =
        case Head of
            not_present ->
                {not_found, null, JrnalCheckFreq};
            Head ->
                case leveled_codec:striphead_to_v1details(Head) of
                    {_SeqN, tomb, _MH, _MD} ->
                        {not_found, null, JrnalCheckFreq};
                    {SeqN, {active, TS}, _MH, MD} ->
                        case TS >= leveled_util:integer_now() of
                            true ->
                                I = State#state.inker,
                                case
                                    journal_notfound(
                                        JrnalCheckFreq, I, LK, SeqN
                                    )
                                of
                                    {true, UppedFrequency} ->
                                        {not_found, null, UppedFrequency};
                                    {false, ReducedFrequency} ->
                                        {MD, SeqN, ReducedFrequency}
                                end;
                            false ->
                                {not_found, null, JrnalCheckFreq}
                        end
                end
        end,
    Reply =
        case LedgerMD of
            not_found ->
                not_found;
            LedgerMD when LedgerMD =/= null ->
                {ok, leveled_head:build_head(Tag, LedgerMD), SQN}
        end,
    {TS1, _SW2} = leveled_monitor:step_time(SW1),
    maybelog_head_timing(
        State#state.monitor, TS0, TS1, LedgerMD == not_found, CacheHit
    ),
    case UpdJrnalCheckFreq of
        JrnalCheckFreq ->
            {reply, Reply, State};
        UpdJrnalCheckFreq ->
            {reply, Reply, State#state{ink_checking = UpdJrnalCheckFreq}}
    end;
handle_call(
    {snapshot, SnapType, Query, LongRunning},
    _From,
    State = #state{penciller = Pcl}
) when
    is_pid(Pcl)
->
    % Snapshot the store, specifying if the snapshot should be long running
    % (i.e. will the snapshot be queued or be required for an extended period
    % e.g. many minutes)
    {ok, PclSnap, InkSnap} =
        snapshot_store(
            State#state.ledger_cache,
            Pcl,
            State#state.inker,
            State#state.monitor,
            SnapType,
            Query,
            LongRunning
        ),
    {reply, {ok, PclSnap, InkSnap}, State};
handle_call(log_settings, _From, State) ->
    {reply, leveled_log:return_settings(), State};
handle_call({return_runner, QueryType}, _From, State) ->
    Runner = get_runner(State, QueryType),
    {reply, Runner, State};
handle_call(fts_indexes, _From, State) ->
    {reply, State#state.fts_indexes, State};
handle_call({compact_journal, Timeout}, From, State) when
    State#state.head_only == false
->
    case leveled_inker:ink_compactionpending(State#state.inker) of
        true ->
            {reply, {busy, undefined}, State};
        false ->
            {ok, PclSnap, null} =
                snapshot_store(
                    State#state.ledger_cache,
                    State#state.penciller,
                    State#state.inker,
                    State#state.monitor,
                    ledger,
                    undefined,
                    true
                ),
            R = leveled_inker:ink_compactjournal(
                State#state.inker,
                PclSnap,
                Timeout
            ),
            gen_server:reply(From, R),
            case
                maybepush_ledgercache(
                    State#state.cache_size,
                    State#state.cache_multiple,
                    State#state.ledger_cache,
                    State#state.penciller,
                    State#state.monitor
                )
            of
                {_, NewCache} ->
                    {noreply, State#state{ledger_cache = NewCache}}
            end
    end;
handle_call(confirm_compact, _From, State) when
    State#state.head_only == false
->
    {reply, leveled_inker:ink_compactionpending(State#state.inker), State};
handle_call(last_compaction_result, _From, State) when
    State#state.head_only == false
->
    {reply, leveled_inker:ink_lastcompactionresult(State#state.inker), State};
handle_call(trim, _From, State) when State#state.head_only == true ->
    PSQN = leveled_penciller:pcl_persistedsqn(State#state.penciller),
    {reply, leveled_inker:ink_trim(State#state.inker, PSQN), State};
handle_call(hot_backup, _From, State) when State#state.head_only == false ->
    ok = leveled_inker:ink_roll(State#state.inker),
    BackupFun =
        fun(InkerSnapshot) ->
            fun(BackupPath) ->
                ok = leveled_inker:ink_backup(InkerSnapshot, BackupPath),
                ok = leveled_inker:ink_close(InkerSnapshot)
            end
        end,
    InkerOpts =
        #inker_options{
            start_snapshot = true,
            source_inker = State#state.inker,
            bookies_pid = self()
        },
    {ok, Snapshot} = leveled_inker:ink_snapstart(InkerOpts),
    {reply, {async, BackupFun(Snapshot)}, State};
handle_call(
    close, _From, State = #state{inker = Inker, penciller = Pcl}
) when
    is_pid(Inker), is_pid(Pcl)
->
    leveled_inker:ink_close(Inker),
    leveled_penciller:pcl_close(Pcl),
    leveled_monitor:monitor_close(element(1, State#state.monitor)),
    {stop, normal, ok, State};
handle_call(destroy, _From, State = #state{is_snapshot = Snp}) when
    Snp == false
->
    ?STD_LOG(b0011, []),
    {ok, InkPathList} = leveled_inker:ink_doom(State#state.inker),
    {ok, PCLPathList} = leveled_penciller:pcl_doom(State#state.penciller),
    leveled_monitor:monitor_close(element(1, State#state.monitor)),
    lists:foreach(fun(DirPath) -> delete_path(DirPath) end, InkPathList),
    lists:foreach(fun(DirPath) -> delete_path(DirPath) end, PCLPathList),
    {stop, normal, ok, State};
handle_call(return_actors, _From, State) ->
    {reply, {ok, State#state.inker, State#state.penciller}, State};
handle_call(journal_sqn, _From, State) ->
    {reply, leveled_inker:ink_getjournalsqn(State#state.inker), State};
handle_call(head_status, _From, State) ->
    {reply, {State#state.head_only, State#state.head_lookup}, State};
handle_call(status, _From, State) ->
    {reply, status(State), State};
handle_call(Msg, _From, State) ->
    {reply, {unsupported_message, element(1, Msg)}, State}.

handle_cast(
    {log_level, LogLevel}, State = #state{inker = Inker, penciller = Pcl}
) when
    is_pid(Inker), is_pid(Pcl)
->
    ok = leveled_penciller:pcl_loglevel(Pcl, LogLevel),
    ok = leveled_inker:ink_loglevel(Inker, LogLevel),
    case element(1, State#state.monitor) of
        no_monitor ->
            ok;
        Monitor ->
            leveled_monitor:log_level(Monitor, LogLevel)
    end,
    ok = leveled_log:set_loglevel(LogLevel),
    {noreply, State};
handle_cast(
    {add_logs, ForcedLogs}, State = #state{inker = Inker, penciller = Pcl}
) when
    is_pid(Inker), is_pid(Pcl)
->
    ok = leveled_penciller:pcl_addlogs(Pcl, ForcedLogs),
    ok = leveled_inker:ink_addlogs(Inker, ForcedLogs),
    case element(1, State#state.monitor) of
        no_monitor ->
            ok;
        Monitor ->
            leveled_monitor:log_add(Monitor, ForcedLogs)
    end,
    ok = leveled_log:add_forcedlogs(ForcedLogs),
    {noreply, State};
handle_cast(
    {remove_logs, ForcedLogs}, State = #state{inker = Inker, penciller = Pcl}
) when
    is_pid(Inker), is_pid(Pcl)
->
    ok = leveled_penciller:pcl_removelogs(Pcl, ForcedLogs),
    ok = leveled_inker:ink_removelogs(Inker, ForcedLogs),
    case element(1, State#state.monitor) of
        no_monitor ->
            ok;
        Monitor ->
            leveled_monitor:log_remove(Monitor, ForcedLogs)
    end,
    ok = leveled_log:remove_forcedlogs(ForcedLogs),
    {noreply, State}.

%% handle the bookie stopping and stop this snapshot
handle_info(
    {'DOWN', BookieMonRef, process, BookiePid, Info},
    State = #state{bookie_monref = BookieMonRef, is_snapshot = true}
) ->
    ?STD_LOG(b0004, [BookiePid, Info]),
    {stop, normal, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, _State) ->
    ?STD_LOG(b0003, [Reason]).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%============================================================================
%%% External functions
%%%============================================================================

-spec empty_ledgercache() -> ledger_cache().
%% @doc
%% Empty the ledger cache table following a push
empty_ledgercache() ->
    #ledger_cache{mem = ets:new(empty, [ordered_set])}.

-spec push_to_penciller(
    pid(),
    list(load_item()),
    ledger_cache(),
    leveled_codec:compaction_strategy()
) ->
    ledger_cache().
%% @doc
%% The push to penciller must start as a tree to correctly de-duplicate
%% the list by order before becoming a de-duplicated list for loading
push_to_penciller(Penciller, LoadItemList, LedgerCache, ReloadStrategy) ->
    CompleteLoadItemList = drop_incomplete_batch_loaditems(LoadItemList),
    UpdLedgerCache =
        lists:foldl(
            fun({InkTag, PK, SQN, Obj, IndexSpecs0, ValSize}, AccLC) ->
                IndexSpecs = leveled_codec:unwrap_batch_keychanges(IndexSpecs0),
                Chngs =
                    case leveled_codec:get_tagstrategy(PK, ReloadStrategy) of
                        recalc ->
                            recalcfor_ledgercache(
                                InkTag,
                                PK,
                                SQN,
                                Obj,
                                ValSize,
                                IndexSpecs,
                                AccLC,
                                Penciller
                            );
                        _ ->
                            preparefor_ledgercache(
                                InkTag, PK, SQN, Obj, ValSize, IndexSpecs
                            )
                    end,
                addto_ledgercache(Chngs, AccLC, loader)
            end,
            LedgerCache,
            lists:reverse(CompleteLoadItemList)
        ),
    case length(UpdLedgerCache#ledger_cache.load_queue) of
        N when N > ?LOADING_BATCH ->
            ?STD_LOG(b0006, [UpdLedgerCache#ledger_cache.max_sqn]),
            ok =
                push_to_penciller_loop(
                    Penciller, loadqueue_ledgercache(UpdLedgerCache)
                ),
            empty_ledgercache();
        _ ->
            UpdLedgerCache
    end.

-spec push_to_penciller_loop(pid(), ledger_cache()) -> ok.
push_to_penciller_loop(Penciller, LedgerCache) ->
    case push_ledgercache(Penciller, LedgerCache) of
        returned ->
            timer:sleep(?LOADING_PAUSE),
            push_to_penciller_loop(Penciller, LedgerCache);
        ok ->
            ok
    end.

-spec push_ledgercache(pid(), ledger_cache()) -> ok | returned.
%% @doc
%% Push the ledgercache to the Penciller - which should respond ok or
%% returned.  If the response is ok the cache can be flushed, but if the
%% response is returned the cache should continue to build and it should try
%% to flush at a later date
push_ledgercache(Penciller, Cache) ->
    CacheToLoad = {
        Cache#ledger_cache.loader,
        Cache#ledger_cache.index,
        Cache#ledger_cache.min_sqn,
        Cache#ledger_cache.max_sqn
    },
    leveled_penciller:pcl_pushmem(Penciller, CacheToLoad).

-spec loadqueue_ledgercache(ledger_cache()) -> ledger_cache().
%% @doc
%% The ledger cache can be built from a queue, for example when loading the
%% ledger from the head of the journal on startup
%%
%% The queue should be build using [NewKey|Acc] so that the most recent
%% key is kept in the sort
loadqueue_ledgercache(Cache) ->
    SL = lists:ukeysort(1, Cache#ledger_cache.load_queue),
    T = leveled_tree:from_orderedlist(SL, ?CACHE_TYPE),
    Cache#ledger_cache{load_queue = [], loader = T}.

-spec snapshot_store(
    ledger_cache(),
    pid(),
    null | pid(),
    leveled_monitor:monitor(),
    store | ledger,
    undefined | no_lookup | tuple(),
    boolean()
) ->
    {ok, pid(), pid() | null}.
%% @doc
%% Allow all a snapshot to be created from part of the store, preferably
%% passing in a query filter so that all of the LoopState does not need to
%% be copied from the real actor to the clone
%%
%% SnapType can be store (requires journal and ledger) or ledger (requires
%% ledger only)
%%
%% Query can be no_lookup, indicating the snapshot will be used for non-specific
%% range queries and not direct fetch requests.  {StartKey, EndKey} if the the
%% snapshot is to be used for one specific query only (this is much quicker to
%% setup, assuming the range is a small subset of the overall key space).  If
%% lookup is required but the range isn't defined then 'undefined' should be
%% passed as the query
snapshot_store(
    LedgerCache, Penciller, Ink, Monitor, SnapType, Query, LongRunning
) ->
    SW0 = leveled_monitor:maybe_time(Monitor),
    LedgerCacheReady = readycache_forsnapshot(LedgerCache, Query),
    BookiesMem = {
        LedgerCacheReady#ledger_cache.loader,
        LedgerCacheReady#ledger_cache.index,
        LedgerCacheReady#ledger_cache.min_sqn,
        LedgerCacheReady#ledger_cache.max_sqn
    },
    PCLopts =
        #penciller_options{
            start_snapshot = true,
            source_penciller = Penciller,
            snapshot_query = Query,
            snapshot_longrunning = LongRunning,
            bookies_pid = self(),
            bookies_mem = BookiesMem
        },
    {TS0, SW1} = leveled_monitor:step_time(SW0),
    {ok, LedgerSnapshot} = leveled_penciller:pcl_snapstart(PCLopts),
    {TS1, _SW2} = leveled_monitor:step_time(SW1),
    ok = maybelog_snap_timing(Monitor, TS0, TS1),
    case SnapType of
        store when is_pid(Ink) ->
            InkerOpts =
                #inker_options{
                    start_snapshot = true,
                    bookies_pid = self(),
                    source_inker = Ink
                },
            {ok, JournalSnapshot} = leveled_inker:ink_snapstart(InkerOpts),
            {ok, LedgerSnapshot, JournalSnapshot};
        ledger ->
            {ok, LedgerSnapshot, null}
    end.

-spec fetch_value(pid(), leveled_codec:journal_ref()) -> not_present | any().
%% @doc
%% Fetch a value from the Journal
fetch_value(Inker, {Key, SQN}) ->
    SW = os:timestamp(),
    case leveled_inker:ink_fetch(Inker, Key, SQN) of
        {ok, Value} ->
            maybe_longrunning(SW, inker_fetch),
            Value;
        not_present ->
            not_present
    end.

%%%============================================================================
%%% Internal functions
%%%============================================================================

-spec startup(#inker_options{}, #penciller_options{}) -> {pid(), pid()}.
%% @doc
%% Startup the Inker and the Penciller, and prompt the loading of the Penciller
%% from the Inker.  The Penciller may be shutdown without the latest data
%% having been persisted: and so the Iker must be able to update the Penciller
%% on startup with anything that happened but wasn't flushed to disk.
startup(InkerOpts, PencillerOpts) ->
    {ok, Inker} = leveled_inker:ink_start(InkerOpts),
    {ok, Penciller} = leveled_penciller:pcl_start(PencillerOpts),
    LedgerSQN = leveled_penciller:pcl_getstartupsequencenumber(Penciller),
    ?STD_LOG(b0005, [LedgerSQN]),
    ReloadStrategy = InkerOpts#inker_options.reload_strategy,
    LoadFun = get_loadfun(),
    BatchFun =
        fun(BatchAcc, Acc) ->
            push_to_penciller(
                Penciller, BatchAcc, Acc, ReloadStrategy
            )
        end,
    InitAccFun =
        fun(FN, CurrentMinSQN) ->
            ?STD_LOG(i0014, [FN, CurrentMinSQN]),
            []
        end,
    FinalAcc =
        leveled_inker:ink_loadpcl(
            Inker, LedgerSQN + 1, LoadFun, InitAccFun, BatchFun
        ),
    ok = push_to_penciller_loop(Penciller, loadqueue_ledgercache(FinalAcc)),
    ok = leveled_inker:ink_checksqn(Inker, LedgerSQN),
    {Inker, Penciller}.

-spec set_defaults(list()) -> open_options().
%% @doc
%% Set any pre-defined defaults for options if the option is not present in
%% the passed in options
set_defaults(Opts) ->
    lists:ukeymerge(
        1,
        lists:ukeysort(1, Opts),
        lists:ukeysort(1, ?OPTION_DEFAULTS)
    ).

-spec set_options(
    open_options(), leveled_monitor:monitor()
) ->
    {#inker_options{}, #penciller_options{}}.
%% @doc
%% Take the passed in property list of operations and extract out any relevant
%% options to the Inker or the Penciller
set_options(Opts, Monitor) ->
    MaxJournalSize0 =
        min(
            ?ABSOLUTEMAX_JOURNALSIZE,
            proplists:get_value(max_journalsize, Opts)
        ),
    JournalSizeJitter = MaxJournalSize0 div (100 div ?JOURNAL_SIZE_JITTER),
    MaxJournalSize =
        min(
            ?ABSOLUTEMAX_JOURNALSIZE,
            MaxJournalSize0 - erlang:phash2(self()) rem JournalSizeJitter
        ),
    MaxJournalCount0 =
        proplists:get_value(max_journalobjectcount, Opts),
    JournalCountJitter = MaxJournalCount0 div (100 div ?JOURNAL_SIZE_JITTER),
    MaxJournalCount =
        MaxJournalCount0 - erlang:phash2(self()) rem JournalCountJitter,

    SyncStrat = proplists:get_value(sync_strategy, Opts),
    WRP = proplists:get_value(waste_retention_period, Opts),

    SnapTimeoutShort = proplists:get_value(snapshot_timeout_short, Opts),
    SnapTimeoutLong = proplists:get_value(snapshot_timeout_long, Opts),

    AltStrategy = proplists:get_value(reload_strategy, Opts),
    ReloadStrategy = leveled_codec:inker_reload_strategy(AltStrategy),

    PCLL0CacheSize =
        case proplists:get_value(max_pencillercachesize, Opts) of
            undefined ->
                ?MAX_PCL_CACHE_SIZE;
            P0CS when is_integer(P0CS), P0CS > ?MIN_PCL_CACHE_SIZE ->
                P0CS;
            _ ->
                ?MIN_PCL_CACHE_SIZE
        end,

    RootPath = proplists:get_value(root_path, Opts),

    JournalFP = filename:join(RootPath, ?JOURNAL_FP),
    LedgerFP = filename:join(RootPath, ?LEDGER_FP),
    ok = filelib:ensure_dir(JournalFP),
    ok = filelib:ensure_dir(LedgerFP),

    SFL_CompPerc =
        proplists:get_value(singlefile_compactionpercentage, Opts),
    MRL_CompPerc =
        proplists:get_value(maxrunlength_compactionpercentage, Opts),
    true = MRL_CompPerc >= SFL_CompPerc,
    true = 100.0 >= MRL_CompPerc,
    true = SFL_CompPerc >= 0.0,

    CompressionMethod = proplists:get_value(compression_method, Opts),
    JournalCompression = CompressionMethod,
    LedgerCompression =
        case proplists:get_value(ledger_compression, Opts) of
            as_store ->
                CompressionMethod;
            AltMethod ->
                AltMethod
        end,
    CompressOnReceipt =
        case proplists:get_value(compression_point, Opts) of
            on_receipt ->
                % Note this will add measurable delay to PUT time
                % https://github.com/martinsumner/leveled/issues/95
                true;
            on_compact ->
                % If using lz4 this is not recommended
                false
        end,
    CompressionLevel = proplists:get_value(compression_level, Opts),

    BlockVersion = proplists:get_value(block_version, Opts),
    MaxSSTSlots = proplists:get_value(max_sstslots, Opts),
    MaxMergeBelow = proplists:get_value(max_mergebelow, Opts),

    ScoreOneIn = proplists:get_value(journalcompaction_scoreonein, Opts),

    {
        #inker_options{
            root_path = JournalFP,
            reload_strategy = ReloadStrategy,
            max_run_length = proplists:get_value(max_run_length, Opts),
            singlefile_compactionperc = SFL_CompPerc,
            maxrunlength_compactionperc = MRL_CompPerc,
            waste_retention_period = WRP,
            snaptimeout_long = SnapTimeoutLong,
            compression_method = JournalCompression,
            compress_on_receipt = CompressOnReceipt,
            score_onein = ScoreOneIn,
            cdb_options =
                #cdb_options{
                    max_size = MaxJournalSize,
                    max_count = MaxJournalCount,
                    binary_mode = true,
                    sync_strategy = SyncStrat,
                    log_options = leveled_log:get_opts(),
                    monitor = Monitor
                },
            monitor = Monitor
        },
        #penciller_options{
            root_path = LedgerFP,
            max_inmemory_tablesize = PCLL0CacheSize,
            levelzero_cointoss = true,
            snaptimeout_short = SnapTimeoutShort,
            snaptimeout_long = SnapTimeoutLong,
            sst_options =
                #sst_options{
                    press_method = LedgerCompression,
                    press_level = CompressionLevel,
                    block_version = BlockVersion,
                    log_options = leveled_log:get_opts(),
                    max_sstslots = MaxSSTSlots,
                    max_mergebelow = MaxMergeBelow,
                    monitor = Monitor
                },
            monitor = Monitor
        }
    }.

-spec return_snapfun(
    book_state(),
    store | ledger,
    tuple() | no_lookup | undefined,
    boolean(),
    boolean()
) ->
    fun(() -> {ok, pid(), pid() | null, fun(() -> ok)}).
%% @doc
%% Generates a function from which a snapshot can be created.  The primary
%% factor here is the SnapPreFold boolean.  If this is true then the snapshot
%% will be taken before the Fold function is returned.  If SnapPreFold is
%% false then the snapshot will be taken when the Fold function is called.
%%
%% SnapPrefold is to be used when the intention is to queue the fold, and so
%% calling of the fold may be delayed, but it is still desired that the fold
%% represent the point in time that the query was requested.
%%
%% Also returns a function which will close any snapshots to be used in the
%% runners post-query cleanup action
%%
%% When the bookie is a snapshot, a fresh snapshot should not be taken, the
%% previous snapshot should be used instead.  Also the snapshot should not be
%% closed as part of the post-query activity as the snapshot may be reused, and
%% should be manually closed.
return_snapfun(
    State = #state{penciller = Pcl, inker = Ink},
    SnapType,
    Query,
    LongRunning,
    SnapPreFold
) when
    is_pid(Pcl), is_pid(Ink)
->
    CloseFun =
        fun(LS0, JS0) ->
            fun() ->
                ok = leveled_penciller:pcl_close(LS0),
                case JS0 of
                    JS0 when is_pid(JS0) ->
                        leveled_inker:ink_close(JS0);
                    _ ->
                        ok
                end
            end
        end,
    case {SnapPreFold, State#state.is_snapshot} of
        {true, false} ->
            {ok, LS, JS} =
                snapshot_store(
                    State#state.ledger_cache,
                    State#state.penciller,
                    State#state.inker,
                    State#state.monitor,
                    SnapType,
                    Query,
                    LongRunning
                ),
            fun() -> {ok, LS, JS, CloseFun(LS, JS)} end;
        {false, false} ->
            Self = self(),
            % Timeout will be ignored, as will Requestor
            %
            % This uses the external snapshot - as the snapshot will need
            % to have consistent state between Bookie and Penciller when
            % it is made.
            fun() ->
                {ok, LS, JS} =
                    book_snapshot(Self, SnapType, Query, LongRunning),
                {ok, LS, JS, CloseFun(LS, JS)}
            end;
        {_, true} ->
            LS = State#state.penciller,
            JS = State#state.inker,
            fun() -> {ok, LS, JS, fun() -> ok end} end
    end.

-spec snaptype_by_presence(boolean() | defer) -> store | ledger.
%% @doc
%% Folds that traverse over object heads, may also either require to return
%% the object, or at least confirm the object is present in the Ledger.  This
%% is achieved by enabling presence - and this will change the type of
%% snapshot to one that covers the whole store (i.e. both ledger and journal),
%% rather than just the ledger.
snaptype_by_presence(true) ->
    store;
snaptype_by_presence(defer) ->
    store;
snaptype_by_presence(false) ->
    ledger.

-spec get_runner(book_state(), tuple()) -> {async, fun(() -> term())}.
%% @doc
%% Get an {async, Runner} for a given fold type.  Fold types have different
%% tuple inputs
get_runner(State, {index_query, Constraint, FoldAccT, Range, TermHandling}) ->
    {StartKey, EndKey} = index_range(Constraint, Range),
    SnapFun = return_snapfun(State, ledger, {StartKey, EndKey}, false, false),
    leveled_runner:index_query(
        SnapFun, {StartKey, EndKey, TermHandling}, FoldAccT
    );
get_runner(State, {fts_query, Bucket, Index, Query, Opts0}) ->
    SnapFun = return_snapfun(State, ledger, no_lookup, false, false),
    Inker = State#state.inker,
    FtsCache =
        case State#state.fts_dir_cache of
            undefined -> undefined;
            Cache -> {Cache, State#state.fts_seq}
        end,
    {IncludeDocs, Opts} = leveled_fts:split_include_docs(Opts0),
    {async, fun() ->
        case leveled_fts:cached_search(FtsCache, Bucket, Index, Query, Opts) of
            {ok, CachedResult} when IncludeDocs ->
                attach_fts_documents(CachedResult, SnapFun, Inker, Bucket, Index, State);
            {ok, CachedResult} ->
                CachedResult;
            miss ->
                run_fts_query(
                    SnapFun, Bucket, Index, Query, Opts, State, FtsCache, IncludeDocs,
                    Inker
                )
        end
    end};
get_runner(
    State,
    {multi_index_query, Bucket, FoldAccT, Queries, ComboFun}
) ->
    {FoldFun, InitAcc} = FoldAccT,
    KeyFolder = fun(_B, K, Acc) -> [K | Acc] end,
    QueryRunners =
        lists:map(
            fun({SetId, {IdxFld, StartTerm, EndTerm, Expr}}) ->
                {SK, EK} =
                    index_range(
                        {Bucket, null}, {IdxFld, StartTerm, EndTerm}
                    ),
                SnapFun =
                    return_snapfun(State, ledger, {SK, EK}, false, true),
                {async, Runner} =
                    leveled_runner:index_query(
                        SnapFun, {SK, EK, {false, Expr}}, {KeyFolder, []}
                    ),
                {SetId, Runner}
            end,
            Queries
        ),
    OverallRunner =
        fun() ->
            FinalSet =
                ComboFun(
                    maps:from_list(
                        lists:map(
                            fun({SetId, R}) ->
                                case R() of
                                    KLR when is_list(KLR) ->
                                        {SetId, sets:from_list(KLR)}
                                end
                            end,
                            QueryRunners
                        )
                    )
                ),
            lists:foldl(
                fun(K, Acc) -> FoldFun(Bucket, K, Acc) end,
                InitAcc,
                sets:to_list(FinalSet)
            )
        end,
    {async, OverallRunner};
get_runner(State, {keylist, Tag, FoldAccT}) ->
    SnapFun = return_snapfun(State, ledger, no_lookup, true, true),
    leveled_runner:bucketkey_query(SnapFun, Tag, null, FoldAccT);
get_runner(State, {keylist, Tag, Bucket, FoldAccT}) ->
    SnapFun = return_snapfun(State, ledger, no_lookup, true, true),
    leveled_runner:bucketkey_query(SnapFun, Tag, Bucket, FoldAccT);
get_runner(State, {keylist, Tag, Bucket, KeyRange, FoldAccT, TermRegex}) ->
    SnapFun = return_snapfun(State, ledger, no_lookup, true, true),
    leveled_runner:bucketkey_query(
        SnapFun, Tag, Bucket, KeyRange, FoldAccT, TermRegex
    );
%% Set of runners for object or metadata folds
get_runner(
    State,
    {foldheads_allkeys, Tag, FoldFun, JournalCheck, SnapPreFold, SegmentList,
        LastModRange, MaxObjectCount}
) ->
    SnapType = snaptype_by_presence(JournalCheck),
    SnapFun = return_snapfun(State, SnapType, no_lookup, true, SnapPreFold),
    leveled_runner:foldheads_allkeys(
        SnapFun,
        Tag,
        FoldFun,
        JournalCheck,
        SegmentList,
        LastModRange,
        MaxObjectCount
    );
get_runner(State, {foldobjects_allkeys, Tag, FoldFun, SnapPreFold}) ->
    get_runner(
        State, {foldobjects_allkeys, Tag, FoldFun, SnapPreFold, key_order}
    );
get_runner(State, {foldobjects_allkeys, Tag, FoldFun, SnapPreFold, Order}) ->
    case Order of
        key_order ->
            SnapFun =
                return_snapfun(State, store, no_lookup, true, SnapPreFold),
            leveled_runner:foldobjects_allkeys(
                SnapFun, Tag, FoldFun, key_order
            );
        sqn_order ->
            SnapFun =
                return_snapfun(State, store, undefined, true, SnapPreFold),
            leveled_runner:foldobjects_allkeys(
                SnapFun, Tag, FoldFun, sqn_order
            )
    end;
get_runner(State, {foldobjects_journal, Tag, FromSQN, FoldAccT}) ->
    SnapFun = return_snapfun(State, store, undefined, true, false),
    leveled_runner:foldobjects_journal(SnapFun, Tag, FromSQN, FoldAccT);
get_runner(
    State,
    {foldheads_bybucket, Tag, BucketList, bucket_list, FoldFun, JournalCheck,
        SnapPreFold, SegmentList, LastModRange, MaxObjectCount}
) ->
    KeyRangeFun =
        fun(Bucket) ->
            {StartKey, EndKey, _} = return_ledger_keyrange(Tag, Bucket, all),
            {StartKey, EndKey}
        end,
    SnapType = snaptype_by_presence(JournalCheck),
    SnapFun = return_snapfun(State, SnapType, no_lookup, true, SnapPreFold),
    leveled_runner:foldheads_bybucket(
        SnapFun,
        Tag,
        lists:map(KeyRangeFun, BucketList),
        FoldFun,
        JournalCheck,
        SegmentList,
        LastModRange,
        MaxObjectCount
    );
get_runner(
    State,
    {foldheads_bybucket, Tag, Bucket, KeyRange, FoldFun, JournalCheck,
        SnapPreFold, SegmentList, LastModRange, MaxObjectCount}
) ->
    {StartKey, EndKey, SnapQ} = return_ledger_keyrange(Tag, Bucket, KeyRange),
    SnapType = snaptype_by_presence(JournalCheck),
    SnapFun = return_snapfun(State, SnapType, SnapQ, true, SnapPreFold),
    leveled_runner:foldheads_bybucket(
        SnapFun,
        Tag,
        [{StartKey, EndKey}],
        FoldFun,
        JournalCheck,
        SegmentList,
        LastModRange,
        MaxObjectCount
    );
get_runner(
    State,
    {foldobjects_bybucket, Tag, Bucket, KeyRange, FoldFun, SnapPreFold}
) ->
    {StartKey, EndKey, SnapQ} = return_ledger_keyrange(Tag, Bucket, KeyRange),
    SnapFun = return_snapfun(State, store, SnapQ, true, SnapPreFold),
    leveled_runner:foldobjects_bybucket(
        SnapFun, Tag, [{StartKey, EndKey}], FoldFun
    );
get_runner(
    State,
    {foldobjects_byindex, Tag, Bucket, {Field, FromTerm, ToTerm},
        FoldObjectsFun, SnapPreFold}
) ->
    SnapFun = return_snapfun(State, store, no_lookup, true, SnapPreFold),
    leveled_runner:foldobjects_byindex(
        SnapFun, {Tag, Bucket, Field, FromTerm, ToTerm}, FoldObjectsFun
    );
get_runner(State, {bucket_list, Tag, FoldAccT}) ->
    {FoldBucketsFun, Acc} = FoldAccT,
    SnapFun = return_snapfun(State, ledger, no_lookup, false, false),
    leveled_runner:bucket_list(SnapFun, Tag, FoldBucketsFun, Acc);
get_runner(State, {first_bucket, Tag, FoldAccT}) ->
    {FoldBucketsFun, Acc} = FoldAccT,
    SnapFun = return_snapfun(State, ledger, no_lookup, false, false),
    leveled_runner:bucket_list(SnapFun, Tag, FoldBucketsFun, Acc, 1);
%% Set of specific runners, primarily used as exmaples for tests
get_runner(State, DeprecatedQuery) ->
    get_deprecatedrunner(State, DeprecatedQuery).

index_range(Constraint, Range) ->
    {IdxFld, StartT, EndT} = Range,
    {Bucket, ObjKey0} =
        case Constraint of
            {B, SK} ->
                {B, SK};
            B ->
                {B, null}
        end,
    StartKey =
        leveled_codec:to_querykey(Bucket, ObjKey0, ?IDX_TAG, IdxFld, StartT),
    EndKey =
        leveled_codec:to_querykey(Bucket, null, ?IDX_TAG, IdxFld, EndT),
    {StartKey, EndKey}.

-spec get_deprecatedrunner(book_state(), tuple()) ->
    {async, fun(() -> term())}.
%% @doc
%% Get an {async, Runner} for a given fold type.  Fold types have different
%% tuple inputs.  These folds are currently used in tests, but are deprecated.
%% Most of these folds should be achievable through other available folds.
get_deprecatedrunner(State, {bucket_stats, Bucket}) ->
    SnapFun = return_snapfun(State, ledger, no_lookup, true, true),
    leveled_runner:bucket_sizestats(SnapFun, Bucket, ?STD_TAG);
get_deprecatedrunner(State, {riakbucket_stats, Bucket}) ->
    SnapFun = return_snapfun(State, ledger, no_lookup, true, true),
    leveled_runner:bucket_sizestats(SnapFun, Bucket, ?RIAK_TAG);
get_deprecatedrunner(State, {hashlist_query, Tag, JournalCheck}) ->
    SnapType = snaptype_by_presence(JournalCheck),
    SnapFun = return_snapfun(State, SnapType, no_lookup, true, true),
    leveled_runner:hashlist_query(SnapFun, Tag, JournalCheck);
get_deprecatedrunner(
    State,
    {tictactree_obj, {Tag, Bucket, StartK, EndK, JournalCheck}, TreeSize,
        PartitionFilter}
) ->
    SnapType = snaptype_by_presence(JournalCheck),
    SnapFun = return_snapfun(State, SnapType, no_lookup, true, true),
    leveled_runner:tictactree(
        SnapFun,
        {Tag, Bucket, {StartK, EndK}},
        JournalCheck,
        TreeSize,
        PartitionFilter
    );
get_deprecatedrunner(
    State,
    {tictactree_idx, {Bucket, IdxField, StartK, EndK}, TreeSize,
        PartitionFilter}
) ->
    SnapFun = return_snapfun(State, ledger, no_lookup, true, true),
    leveled_runner:tictactree(
        SnapFun,
        {?IDX_TAG, Bucket, {IdxField, StartK, EndK}},
        false,
        TreeSize,
        PartitionFilter
    ).

-spec return_ledger_keyrange(
    atom(), leveled_codec:key(), tuple() | all
) ->
    {
        leveled_codec:query_key(),
        leveled_codec:query_key(),
        {leveled_codec:query_key(), leveled_codec:query_key()}
        | no_lookup
    }.
%% @doc
%% Convert a range of binary keys into a ledger key range, returning
%% {StartLK, EndLK, Query} where Query is to indicate whether the query
%% range is worth using to minimise the cost of the snapshot
return_ledger_keyrange(Tag, Bucket, KeyRange) ->
    {StartKey, EndKey, Snap} =
        case KeyRange of
            all ->
                {
                    leveled_codec:to_querykey(Bucket, null, Tag),
                    leveled_codec:to_querykey(Bucket, null, Tag),
                    false
                };
            {StartTerm, <<"$all">>} ->
                {
                    leveled_codec:to_querykey(Bucket, StartTerm, Tag),
                    leveled_codec:to_querykey(Bucket, null, Tag),
                    false
                };
            {StartTerm, EndTerm} ->
                {
                    leveled_codec:to_querykey(Bucket, StartTerm, Tag),
                    leveled_codec:to_querykey(Bucket, EndTerm, Tag),
                    true
                }
        end,
    SnapQuery =
        case Snap of
            true ->
                {StartKey, EndKey};
            false ->
                no_lookup
        end,
    {StartKey, EndKey, SnapQuery}.

-spec maybe_longrunning(erlang:timestamp(), atom()) -> ok.
%% @doc
%% Check the length of time an operation (named by Aspect) has taken, and
%% see if it has crossed the long running threshold.  If so log to indicate
%% a long running event has occurred.
maybe_longrunning(SW, Aspect) ->
    case timer:now_diff(os:timestamp(), SW) of
        N when N > ?LONG_RUNNING ->
            ?STD_LOG(b0013, [N, Aspect]);
        _ ->
            ok
    end.

-spec readycache_forsnapshot(
    ledger_cache(), tuple() | no_lookup | undefined
) -> ledger_cache().
%% @doc
%% Strip the ledger cach back to only the relevant information needed in
%% the query, and to make the cache a snapshot (and so not subject to changes
%% such as additions to the ets table)
readycache_forsnapshot(LedgerCache, {StartKey, EndKey}) ->
    {KL, MinSQN, MaxSQN} = scan_table(
        LedgerCache#ledger_cache.mem,
        StartKey,
        EndKey
    ),
    case KL of
        [] ->
            #ledger_cache{
                loader = empty_cache,
                index = empty_index,
                min_sqn = MinSQN,
                max_sqn = MaxSQN
            };
        _ ->
            #ledger_cache{
                loader = leveled_tree:from_orderedlist(
                    KL,
                    ?CACHE_TYPE
                ),
                index = empty_index,
                min_sqn = MinSQN,
                max_sqn = MaxSQN
            }
    end;
readycache_forsnapshot(LedgerCache, Query) ->
    % Need to convert the Ledger Cache away from using the ETS table
    Tree = leveled_tree:from_orderedset(
        LedgerCache#ledger_cache.mem,
        ?CACHE_TYPE
    ),
    case leveled_tree:tsize(Tree) of
        0 ->
            #ledger_cache{
                loader = empty_cache,
                index = empty_index,
                min_sqn = LedgerCache#ledger_cache.min_sqn,
                max_sqn = LedgerCache#ledger_cache.max_sqn
            };
        _ ->
            Idx =
                case Query of
                    no_lookup ->
                        empty_index;
                    _ ->
                        LedgerCache#ledger_cache.index
                end,
            #ledger_cache{
                loader = Tree,
                index = Idx,
                min_sqn = LedgerCache#ledger_cache.min_sqn,
                max_sqn = LedgerCache#ledger_cache.max_sqn
            }
    end.

-spec scan_table(
    ets:tab(),
    leveled_codec:ledger_key(),
    leveled_codec:ledger_key()
) ->
    {
        list(leveled_codec:ledger_kv()),
        non_neg_integer() | infinity,
        non_neg_integer()
    }.
%% @doc
%% Query the ETS table to find a range of keys (start inclusive).  Should also
%% return the miniumum and maximum sequence number found in the query.  This
%% is just then used as a safety check when loading these results into the
%% penciller snapshot
scan_table(Table, StartKey, EndKey) ->
    case ets:lookup(Table, StartKey) of
        [] ->
            scan_table(Table, StartKey, EndKey, [], infinity, 0);
        [{StartKey, StartVal}] ->
            SQN = leveled_codec:strip_to_seqonly({StartKey, StartVal}),
            scan_table(
                Table,
                StartKey,
                EndKey,
                [{StartKey, StartVal}],
                SQN,
                SQN
            )
    end.

scan_table(Table, StartKey, EndKey, Acc, MinSQN, MaxSQN) ->
    case ets:next(Table, StartKey) of
        '$end_of_table' ->
            {lists:reverse(Acc), MinSQN, MaxSQN};
        NextKey ->
            case leveled_codec:endkey_passed(EndKey, NextKey) of
                true ->
                    {lists:reverse(Acc), MinSQN, MaxSQN};
                false ->
                    [{NextKey, NextVal}] = ets:lookup(Table, NextKey),
                    SQN = leveled_codec:strip_to_seqonly({NextKey, NextVal}),
                    scan_table(
                        Table,
                        NextKey,
                        EndKey,
                        [{NextKey, NextVal} | Acc],
                        min(MinSQN, SQN),
                        max(MaxSQN, SQN)
                    )
            end
    end.

%% Ledger preparation is pure (key hashing and metadata extraction), so large
%% batches are prepared in parallel worker processes; the resulting key
%% changes for the whole batch are then added to the ledger cache with a
%% single ETS insert.
-define(PREPARE_PARALLEL_MIN, 64).
-define(PREPARE_CHUNKS, 8).

prepare_batch_changes(ObjectWriteInfos, SQN) ->
    Prepare =
        fun({LedgerKey, Object, KeyChanges, ObjSize}) ->
            preparefor_ledgercache(
                null, LedgerKey, SQN, Object, ObjSize, KeyChanges
            )
        end,
    case length(ObjectWriteInfos) >= ?PREPARE_PARALLEL_MIN of
        false ->
            lists:map(Prepare, ObjectWriteInfos);
        true ->
            Chunks = chunk_list(ObjectWriteInfos, ?PREPARE_CHUNKS),
            Parent = self(),
            Ref = make_ref(),
            Pids =
                [
                    spawn_opt(
                        fun() ->
                            Parent ! {Ref, self(), lists:map(Prepare, Chunk)}
                        end,
                        [link, {min_heap_size, 8192}]
                    )
                 || Chunk <- Chunks
                ],
            lists:append([
                receive
                    {Ref, Pid, Result} -> Result
                end
             || Pid <- Pids
            ])
    end.

chunk_list(List, N) ->
    Size = max(1, (length(List) + N - 1) div N),
    chunk_list_split(List, Size).

chunk_list_split([], _Size) ->
    [];
chunk_list_split(List, Size) when length(List) =< Size ->
    [List];
chunk_list_split(List, Size) ->
    {Chunk, Rest} = lists:split(Size, List),
    [Chunk | chunk_list_split(Rest, Size)].

addto_ledgercache_batch(PreparedChanges, Cache) ->
    AllKeyChanges =
        lists:append([KeyChanges || {_H, _SQN, KeyChanges} <- PreparedChanges]),
    ets:insert(Cache#ledger_cache.mem, AllKeyChanges),
    {UpdIndex, MinSQN, MaxSQN} =
        lists:foldl(
            fun({H, SQN, _KeyChanges}, {IndexAcc, MinAcc, MaxAcc}) ->
                {
                    leveled_pmem:prepare_for_index(IndexAcc, H),
                    min(SQN, MinAcc),
                    max(SQN, MaxAcc)
                }
            end,
            {Cache#ledger_cache.index, Cache#ledger_cache.min_sqn,
                Cache#ledger_cache.max_sqn},
            PreparedChanges
        ),
    Cache#ledger_cache{
        index = UpdIndex,
        min_sqn = MinSQN,
        max_sqn = MaxSQN
    }.

%% TARGET_API §3.2 absorption tracking: every newly allocated journal
%% SQN must pass through absorb_sqns before the ledger cache may be
%% pushed past it. In-order SQNs advance the frontier; out-of-order ones
%% buffer until the gap fills. A gap older than ?PUBLISH_GAP_TIMEOUT_MS
%% is an abandoned intent (caller died between journal write and
%% publish): the write was never acked, so the frontier skips it -
%% unacked writes are indeterminate by contract.
%% Each absorbed SQN may carry an FTS cache advance. Advances apply ONLY
%% when their SQN joins the contiguous frontier: the FTS query cache
%% treats its stamp as a completeness claim (Stamp =< QuerySeq serves the
%% cache), so an advance stamping ahead of an unabsorbed hole would let a
%% query trust a cache that is missing a concurrent writer's delta.
%% Frontier-ordered application makes the stamp honest by construction -
%% for the direct path exactly as for caller-side publishes.
absorb_sqns([], State) ->
    State;
absorb_sqns([SQN | Rest], State) ->
    absorb_sqns(Rest, absorb_sqn(SQN, none, State)).

absorb_sqn(SQN, FtsAdvance, State) when SQN == State#state.publish_frontier + 1 ->
    apply_fts_advance(SQN, FtsAdvance, State),
    drain_pending(State#state{
        publish_frontier = SQN,
        publish_gap_since = undefined
    });
absorb_sqn(SQN, _FtsAdvance, State) when SQN =< State#state.publish_frontier ->
    %% replay/duplicate absorption (restart paths) - already covered
    State;
absorb_sqn(SQN, FtsAdvance, State) ->
    GapSince =
        case State#state.publish_gap_since of
            undefined -> os:timestamp();
            TS -> TS
        end,
    State#state{
        publish_pending =
            gb_trees:enter(SQN, FtsAdvance, State#state.publish_pending),
        publish_gap_since = GapSince
    }.

drain_pending(State) ->
    Pending = State#state.publish_pending,
    Next = State#state.publish_frontier + 1,
    case gb_trees:is_empty(Pending) of
        true ->
            State;
        false ->
            case gb_trees:smallest(Pending) of
                {Next, FtsAdvance} ->
                    apply_fts_advance(Next, FtsAdvance, State),
                    drain_pending(State#state{
                        publish_frontier = Next,
                        publish_pending = gb_trees:delete(Next, Pending)
                    });
                {_Larger, _} ->
                    State
            end
    end.

fts_advance_or_none({[], 0, 0, []}) -> none;
fts_advance_or_none(FtsAdvance) -> FtsAdvance.

apply_fts_advance(_SQN, none, _State) ->
    ok;
apply_fts_advance(SQN, FtsAdvance, State) ->
    ok = advance_fts_seqs_cache(State#state.fts_dir_cache, SQN, FtsAdvance).

publish_gap_expired(State) ->
    case {gb_trees:is_empty(State#state.publish_pending), State#state.publish_gap_since} of
        {true, _} ->
            false;
        {false, undefined} ->
            false;
        {false, TS} ->
            timer:now_diff(os:timestamp(), TS) div 1000 > ?PUBLISH_GAP_TIMEOUT_MS
    end.

%% Push gate: the opportunistic cache->penciller push is skipped while an
%% absorption gap is open (so the persisted watermark cannot pass an
%% unabsorbed acked write); an expired gap force-advances the frontier
%% (the missing SQN was never acked).
maybe_gated_push(Cache0, State) ->
    State1 =
        case publish_gap_expired(State) of
            true ->
                {SkippedTo, SkipAdv} = gb_trees:smallest(State#state.publish_pending),
                apply_fts_advance(SkippedTo, SkipAdv, State),
                drain_pending(State#state{
                    publish_frontier = SkippedTo,
                    publish_pending =
                        gb_trees:delete(SkippedTo, State#state.publish_pending),
                    publish_gap_since = undefined
                });
            false ->
                State
        end,
    case gb_trees:is_empty(State1#state.publish_pending) of
        false ->
            {{ok, Cache0}, State1};
        true ->
            Result =
                maybepush_ledgercache(
                    State1#state.cache_size,
                    State1#state.cache_multiple,
                    Cache0,
                    State1#state.penciller,
                    State1#state.monitor
                ),
            {Result, State1}
    end.

do_augmented_put(LedgerKey, Object, AugIndexSpecs, TTL, DataSync, From, State0, FtsAdvance) ->
    SWLR = os:timestamp(),
    SW0 = leveled_monitor:maybe_time(State0#state.monitor),
    {ok, SQN, ObjSize} =
        leveled_inker:ink_put(
            State0#state.inker,
            LedgerKey,
            Object,
            {AugIndexSpecs, TTL},
            DataSync
        ),
    {T0, SW1} = leveled_monitor:step_time(SW0),
    Changes =
        preparefor_ledgercache(
            null, LedgerKey, SQN, Object, ObjSize, {AugIndexSpecs, TTL}
        ),
    {T1, SW2} = leveled_monitor:step_time(SW1),
    Cache0 = addto_ledgercache(Changes, State0#state.ledger_cache),
    {T2, _SW3} = leveled_monitor:step_time(SW2),
    case State0#state.slow_offer of
        true ->
            gen_server:reply(From, pause);
        false ->
            gen_server:reply(From, ok)
    end,
    maybe_longrunning(SWLR, overall_put),
    maybelog_put_timing(State0#state.monitor, T0, T1, T2, ObjSize),
    StateA = absorb_sqn(SQN, fts_advance_or_none(FtsAdvance), State0),
    case maybe_gated_push(Cache0, StateA) of
        {{ok, Cache}, StateB} ->
            {noreply, StateB#state{
                slow_offer = false,
                ledger_cache = Cache,
                fts_seq = max(SQN, StateB#state.fts_seq)
            }};
        {{returned, Cache}, StateB} ->
            {noreply, StateB#state{
                slow_offer = true,
                ledger_cache = Cache,
                fts_seq = max(SQN, StateB#state.fts_seq)
            }}
    end.

-spec do_batchput(
    list({leveled_codec:ledger_key(), any(), leveled_codec:journal_keychanges()}),
    boolean(),
    gen_server:from(),
    #state{},
    {list(), non_neg_integer(), non_neg_integer()}
) ->
    {noreply, #state{}}.
do_batchput(ObjectChanges, DataSync, From, State, FtsAdvance) ->
    SWLR = os:timestamp(),
    SW0 = leveled_monitor:maybe_time(State#state.monitor),
    case
        leveled_inker:ink_batchput(
            State#state.inker,
            ObjectChanges,
            DataSync
        )
    of
        {ok, SQN, ObjectWriteInfos} ->
            {T0, SW1} = leveled_monitor:step_time(SW0),
            %% The batch is durable in the journal, and no read can be served
            %% before this callback completes (the bookie is process-serial),
            %% so the caller is released before the ledger bookkeeping: the
            %% remaining work overlaps the caller's preparation of its next
            %% batch. Crash recovery rebuilds the ledger from the journal in
            %% either ordering.
            case State#state.slow_offer of
                true ->
                    gen_server:reply(From, pause);
                false ->
                    gen_server:reply(From, ok)
            end,
            PreparedChanges = prepare_batch_changes(ObjectWriteInfos, SQN),
            {T1, SW2} = leveled_monitor:step_time(SW1),
            Cache0 =
                addto_ledgercache_batch(
                    PreparedChanges, State#state.ledger_cache
                ),
            ObjSizeTotal =
                lists:sum([
                    ObjSize
                 || {_LK, _Obj, _KeyChanges, ObjSize} <- ObjectWriteInfos
                ]),
            {T2, _SW3} = leveled_monitor:step_time(SW2),
            maybe_longrunning(SWLR, overall_put),
            maybelog_put_timing(
                State#state.monitor, T0, T1, T2, ObjSizeTotal
            ),
            StateA = absorb_sqn(SQN, fts_advance_or_none(FtsAdvance), State),
            case maybe_gated_push(Cache0, StateA) of
                {{ok, Cache}, StateB} ->
                    {noreply, StateB#state{
                        slow_offer = false,
                        ledger_cache = Cache,
                        fts_seq = max(SQN, StateB#state.fts_seq)
                    }};
                {{returned, Cache}, StateB} ->
                    {noreply, StateB#state{
                        slow_offer = true,
                        ledger_cache = Cache,
                        fts_seq = max(SQN, StateB#state.fts_seq)
                    }}
            end;
        {error, Reason} ->
            gen_server:reply(From, {error, Reason}),
            %% The FTS sequence was advanced at augmentation but the journal
            %% did not move; resync so the next batch cannot stamp a sequence
            %% that an earlier write already persisted (which would alias
            %% marker and page-directory terms after a later reseed). Cached
            %% batch lists are stamped with pre-resync sequences: drop them
            %% and let the next query rediscover.
            ok = leveled_fts:reset_fts_caches(State#state.fts_dir_cache),
            {ok, JournalSQN} =
                leveled_inker:ink_getjournalsqn(State#state.inker),
            {noreply, State#state{fts_seq = JournalSQN}}
    end.

-spec current_head_state(leveled_codec:ledger_key(), #state{}) ->
    absent
    | tombstone
    | expired
    | {active, non_neg_integer(), term()}.
%% FTS derivation never reads existing objects: documents in the write are
%% tokenised and written as packed page/marker index specs by leveled_fts, and
%% superseded postings are filtered against the doc marker at query time. The
%% batch sequence is seeded from the journal SQN at startup so it stays
%% monotonic across restarts.
run_fts_query(SnapFun, Bucket, Index, Query, Opts, State, FtsCache, IncludeDocs, Inker) ->
    {ok, LedgerSnapshot, _JournalSnapshot, AfterFun} = SnapFun(),
    FoldSource = fts_fold_source(LedgerSnapshot, Inker),
    try
        Result =
            leveled_fts:search(
                FoldSource,
                Bucket,
                Index,
                Query,
                Opts,
                State#state.fts_indexes,
                FtsCache
            ),
        case {IncludeDocs, Result} of
            {true, {ok, Hits}} when is_list(Hits) ->
                {ok,
                    attach_documents(
                        Hits, LedgerSnapshot, Inker, Bucket, Index, State
                    )};
            _NoDocs ->
                Result
        end
    after
        AfterFun()
    end.

%% The fold source handed to leveled_fts: index folds against one ledger
%% snapshot, plus an object fetch (heads through the snapshot, values
%% through the LIVE inker — book_get's split, no per-query inker clone)
%% for shard bases and include_docs hydration.
fts_fold_source(LedgerSnapshot, Inker) ->
    #{
        fold => fts_index_fold_fun(LedgerSnapshot),
        fetch =>
            fun(Bucket, Key, Tag) ->
                fetch_object_snapshot(LedgerSnapshot, Inker, Bucket, Key, Tag)
            end
    }.

%% Enrich a cached hit list with documents through a fresh snapshot
%% (cache hits are keyed by the write sequence, so the current view is
%% the view the hits were computed against).
attach_fts_documents({ok, Hits}, SnapFun, Inker, Bucket, Index, State) when is_list(Hits) ->
    {ok, LedgerSnapshot, _JS, AfterFun} = SnapFun(),
    try
        {ok, attach_documents(Hits, LedgerSnapshot, Inker, Bucket, Index, State)}
    after
        AfterFun()
    end;
attach_fts_documents(Result, _SnapFun, _Inker, _Bucket, _Index, _State) ->
    Result.

attach_documents(Hits, LedgerSnapshot, Inker, Bucket, Index, State) ->
    Tag =
        case leveled_fts:find_schema(Bucket, Index, State#state.fts_indexes) of
            {ok, #{tag := T}} -> T;
            _ -> ?STD_TAG
        end,
    Heads =
        [
            {Hit,
                snapshot_head_fetchspec(
                    LedgerSnapshot, Bucket, maps:get(key, Hit), Tag
                )}
         || Hit <- Hits
        ],
    case [{LK, SQN} || {_Hit, {fetch, LK, SQN}} <- Heads] of
        [] ->
            [];
        Pairs ->
            zip_documents(Heads, leveled_inker:ink_mget(Inker, Pairs))
    end.

%% Hits whose object is no longer live (deleted or expired since the
%% search snapshot) are dropped, as attach_documents always has.
zip_documents([], []) ->
    [];
zip_documents([{_Hit, not_found} | RestH], Values) ->
    zip_documents(RestH, Values);
zip_documents([{Hit, {fetch, _LK, _SQN}} | RestH], [Value | RestV]) ->
    case Value of
        {ok, Object} ->
            [Hit#{document => Object} | zip_documents(RestH, RestV)];
        not_present ->
            zip_documents(RestH, RestV)
    end.

%% book_get's liveness semantics (tombstone and TTL handling, journal
%% fetch by SQN): heads from the query's ledger snapshot (index-less L0
%% fetch - fold-shaped snapshots carry no L0 index), values through the
%% live inker exactly as book_get reads them.
fetch_object_snapshot(LedgerSnapshot, Inker, Bucket, Key, Tag) ->
    case snapshot_head_fetchspec(LedgerSnapshot, Bucket, Key, Tag) of
        not_found ->
            not_found;
        {fetch, LedgerKey, SQN} ->
            case fetch_value(Inker, {LedgerKey, SQN}) of
                not_present -> not_found;
                Object -> {ok, Object}
            end
    end.

-spec snapshot_head_fetchspec(
    pid(),
    leveled_codec:key(),
    leveled_codec:key(),
    leveled_codec:tag()
) ->
    not_found | {fetch, leveled_codec:ledger_key(), non_neg_integer()}.
%% Resolve a key against a ledger snapshot to the journal reference of
%% its live value - or not_found for absent, tombstoned or expired keys
%% (book_get's liveness rules).
snapshot_head_fetchspec(LedgerSnapshot, Bucket, Key, Tag) ->
    LedgerKey = leveled_codec:to_objectkey(Bucket, Key, Tag),
    Hash = leveled_codec:segment_hash(LedgerKey),
    case leveled_penciller:pcl_fetch(LedgerSnapshot, LedgerKey, Hash, false) of
        not_present ->
            not_found;
        {_LedgerKey, Head} ->
            {SQN, Status, _MH, _MD} = leveled_codec:striphead_to_v1details(
                Head
            ),
            case Status of
                tomb ->
                    not_found;
                {active, TS} ->
                    case TS >= leveled_util:integer_now() of
                        false -> not_found;
                        true -> {fetch, LedgerKey, SQN}
                    end
            end
    end.

% mget batches are processed in chunks of this size; 13 divides a
% typical limit-50 hydration batch into four balanced chunks
-define(MGET_CHUNKSIZE, 13).

%% book_mget's runner: resolve heads against the ledger snapshot (same
%% liveness semantics as book_get), then batched inker requests for the
%% live values.  The batch is split into chunks that are processed
%% concurrently - each chunk worker walks its heads and then blocks on
%% one batched journal read - so a batch's ledger and journal reads
%% overlap and the batch completes in the time of its slowest chunk,
%% not the sum of its parts.
mget_objects(LedgerSnapshot, Inker, Bucket, Keys, Tag) ->
    Parent = self(),
    Workers =
        [
            begin
                ChunkRef = make_ref(),
                {_Pid, Mon} =
                    spawn_monitor(
                        fun() ->
                            Parent ! {ChunkRef, mget_chunk(
                                LedgerSnapshot, Inker, Bucket, Chunk, Tag
                            )}
                        end
                    ),
                {ChunkRef, Mon, Chunk}
            end
         || Chunk <- mget_chunks(Keys)
        ],
    lists:flatmap(
        fun({ChunkRef, Mon, Chunk}) ->
            receive
                {ChunkRef, Results} ->
                    erlang:demonitor(Mon, [flush]),
                    Results;
                {'DOWN', Mon, process, _Pid, _Reason} ->
                    % the same degradation ink_mget applies on a
                    % journal file-close race
                    [{Key, not_found} || Key <- Chunk]
            end
        end,
        Workers
    ).

mget_chunk(LedgerSnapshot, Inker, Bucket, Chunk, Tag) ->
    Heads =
        [
            {Key, snapshot_head_fetchspec(LedgerSnapshot, Bucket, Key, Tag)}
         || Key <- Chunk
        ],
    case [{LK, SQN} || {_Key, {fetch, LK, SQN}} <- Heads] of
        [] ->
            Heads;
        Pairs ->
            zip_mget(Heads, leveled_inker:ink_mget(Inker, Pairs))
    end.

mget_chunks(Keys) when length(Keys) =< ?MGET_CHUNKSIZE ->
    [Keys];
mget_chunks(Keys) ->
    {Chunk, Rest} = lists:split(?MGET_CHUNKSIZE, Keys),
    [Chunk | mget_chunks(Rest)].

zip_mget([], []) ->
    [];
zip_mget([{Key, not_found} | RestH], Values) ->
    [{Key, not_found} | zip_mget(RestH, Values)];
zip_mget([{Key, {fetch, _LK, _SQN}} | RestH], [Value | RestV]) ->
    Result =
        case Value of
            {ok, Object} -> {ok, Object};
            not_present -> not_found
        end,
    [{Key, Result} | zip_mget(RestH, RestV)].

%% Consolidation orchestration, run in the caller's process: shards
%% derive in parallel (one snapshot each), then apply in chunks through
%% the bookie. Shard applies are independent by design.
run_fts_consolidation(Bookie, Bucket, Ref, Schema, Shards, Inker) ->
    DeriveOne =
        fun(Shard) ->
            {ok, LS, _JS} = book_snapshot(Bookie, ledger, no_lookup, false),
            try
                %% one-shot reads: never pollute the query cache (a full
                %% consolidation would blow its budget and leave every
                %% later query folding cold).
                leveled_fts:consolidate_shard(
                    fts_fold_source(LS, Inker), Bucket, Schema, undefined, Shard
                )
            catch
                throw:{fts_error, Reason} -> {error, Reason}
            after
                ok = leveled_penciller:pcl_close(LS)
            end
        end,
    Parent = self(),
    Refs =
        [
            begin
                MRef = make_ref(),
                spawn_link(fun() -> Parent ! {MRef, DeriveOne(Shard)} end),
                MRef
            end
         || Shard <- Shards
        ],
    Results =
        [
            receive
                {MRef, R} -> R
            end
         || MRef <- Refs
        ],
    case [E || {error, _} = E <- Results] of
        [FirstError | _] ->
            FirstError;
        [] ->
            Derived = [D || {ok, D} <- Results],
            apply_fts_consolidation(Bookie, Bucket, Ref, Derived)
    end.

apply_fts_consolidation(_Bookie, _Bucket, _Ref, []) ->
    ok;
apply_fts_consolidation(Bookie, Bucket, Ref, Derived) ->
    %% a chunk must fit one journal file: bound by bytes, not count.
    {Chunk, Rest} = take_consolidation_chunk(Derived, 64 * 1024 * 1024, []),
    case
        gen_server:call(
            Bookie, {ftsconsolidate_apply, Bucket, Ref, Chunk}, infinity
        )
    of
        ok -> apply_fts_consolidation(Bookie, Bucket, Ref, Rest);
        pause -> apply_fts_consolidation(Bookie, Bucket, Ref, Rest);
        {error, _Reason} = Error -> Error
    end.

take_consolidation_chunk([], _Budget, Acc) ->
    {lists:reverse(Acc), []};
take_consolidation_chunk([#{base := Base} = D | Rest], Budget, Acc) ->
    Size = byte_size(Base),
    case Size > Budget andalso Acc =/= [] of
        true -> {lists:reverse(Acc), [D | Rest]};
        false -> take_consolidation_chunk(Rest, Budget - Size, [D | Acc])
    end.

%% The fold source handed to leveled_fts: index folds against one ledger
%% snapshot, executed in the calling process.
fts_index_fold_fun(LedgerSnapshot) ->
    fun(FoldBucketKey, FoldAccT, Range, TermHandling) ->
        {StartKey, EndKey} =
            index_range(FoldBucketKey, Range),
        {FoldKeysFun, InitAcc} = FoldAccT,
        Folder =
            leveled_penciller:pcl_fetchkeys(
                LedgerSnapshot,
                StartKey,
                EndKey,
                leveled_codec:accumulate_index(TermHandling, FoldKeysFun),
                InitAcc,
                by_runner
            ),
        Folder()
    end.

%% Decoded page directories are immutable per batch sequence within a store
%% instance, so the cache is fill-only and owned by (and dies with) the bookie.
maybe_new_fts_dir_cache([]) ->
    undefined;
maybe_new_fts_dir_cache(_FtsIndexes) ->
    ets:new(fts_dir_cache, [set, public, {read_concurrency, true}]).

augment_fts_object_changes(ObjectChanges, #state{fts_indexes = []} = State) ->
    {ObjectChanges, State, {[], 0, 0, []}};
augment_fts_object_changes(ObjectChanges, State) ->
    PrevSeq = State#state.fts_seq,
    Seq = PrevSeq + 1,
    case
        leveled_fts:augment_object_changes(
            ObjectChanges, State#state.fts_indexes, Seq
        )
    of
        {ok, AugObjectChanges, Touched} ->
            Markers =
                leveled_fts:marker_cache_updates(
                    AugObjectChanges, State#state.fts_indexes
                ),
            {AugObjectChanges, State#state{fts_seq = Seq},
                {Touched, Seq, PrevSeq, Markers}};
        {error, Reason} ->
            {error, Reason}
    end.

%% Both augment paths return an fts_advance() alongside the changes:
%% the touched shard deltas, the batch sequence, the pre-write sequence,
%% and the per-doc marker updates. The success path applies them to the
%% write-through shard-state and marker caches; consolidation applies
%% resets its shards instead.
advance_fts_seqs_cache(_DirCache, _NewSeq, {[], 0, 0, []}) ->
    ok;
advance_fts_seqs_cache(DirCache, NewFtsSeq, {consolidate, Bucket, Ref, Updates}) ->
    leveled_fts:consolidate_shard_cache(DirCache, Bucket, Ref, Updates, NewFtsSeq);
advance_fts_seqs_cache(DirCache, NewSeq, {Touched, _BatchSeq, _PrevSeq, Markers}) ->
    ok = leveled_fts:advance_shard_cache(DirCache, Touched, NewSeq),
    leveled_fts:advance_marker_cache(DirCache, Markers, NewSeq).

current_head_state(LedgerKey, State) ->
    {Head, _CacheHit} =
        fetch_head(
            LedgerKey,
            State#state.penciller,
            State#state.ledger_cache,
            State#state.head_only
        ),
    case Head of
        not_present ->
            absent;
        Head ->
            case leveled_codec:striphead_to_v1details(Head) of
                {_SeqN, tomb, _MH, _MD} ->
                    tombstone;
                {SeqN, {active, TS}, _MH, MD} ->
                    case TS >= leveled_util:integer_now() of
                        true ->
                            {active, SeqN, MD};
                        false ->
                            expired
                    end
            end
    end.

-spec fetch_head(leveled_codec:ledger_key(), pid(), ledger_cache()) ->
    {not_present | leveled_codec:ledger_value(), boolean()}.
%% @doc
%% Fetch only the head of the object from the Ledger (or the bookie's recent
%% ledger cache if it has just been updated).  not_present is returned if the
%% Key is not found
fetch_head(Key, Penciller, LedgerCache) ->
    fetch_head(Key, Penciller, LedgerCache, false).

-spec fetch_head(leveled_codec:ledger_key(), pid(), ledger_cache(), boolean()) ->
    {not_present | leveled_codec:ledger_value(), boolean()}.
%% doc
%% The L0Index needs to be bypassed when running head_only
fetch_head(Key, Penciller, LedgerCache, HeadOnly) ->
    SW = os:timestamp(),
    case ets:lookup(LedgerCache#ledger_cache.mem, Key) of
        [{Key, Head}] ->
            {Head, true};
        [] ->
            Hash = leveled_codec:segment_hash(Key),
            UseL0Idx = not HeadOnly,
            % don't use the L0Index in head only mode. Object specs don't
            % get an addition on the L0 index
            case leveled_penciller:pcl_fetch(Penciller, Key, Hash, UseL0Idx) of
                {Key, Head} ->
                    maybe_longrunning(SW, pcl_head),
                    {Head, false};
                not_present ->
                    maybe_longrunning(SW, pcl_head),
                    {not_present, false}
            end
    end.

-spec journal_notfound(integer(), pid(), leveled_codec:ledger_key(), integer()) ->
    {boolean(), integer()}.
%% @doc Check to see if the item is not_found in the journal.  If it is found
%% return false, and drop the counter that represents the frequency this check
%% should be made.  If it is not_found, this is not expected so up the check
%% frequency to the maximum value
journal_notfound(CheckFrequency, Inker, LK, SQN) ->
    check_notfound(
        CheckFrequency,
        fun() ->
            leveled_inker:ink_keycheck(Inker, LK, SQN)
        end
    ).

-spec check_notfound(integer(), fun(() -> probably | missing)) ->
    {boolean(), integer()}.
%% @doc Use a function to check if an item is found
check_notfound(CheckFrequency, CheckFun) ->
    case rand:uniform(?MAX_KEYCHECK_FREQUENCY) of
        X when X =< CheckFrequency ->
            case CheckFun() of
                probably ->
                    {false, max(?MIN_KEYCHECK_FREQUENCY, CheckFrequency - 1)};
                missing ->
                    {true, ?MAX_KEYCHECK_FREQUENCY}
            end;
        _X ->
            {false, CheckFrequency}
    end.

-spec normalise_batch_specs(list()) ->
    {ok, list({
        leveled_codec:ledger_key(),
        any(),
        leveled_codec:journal_keychanges()
    })}
    | {error, term()}.
normalise_batch_specs([]) ->
    {error, empty_batch};
normalise_batch_specs(BatchSpecs) when is_list(BatchSpecs) ->
    normalise_batch_specs(BatchSpecs, #{}, []);
normalise_batch_specs(_BatchSpecs) ->
    {error, invalid_batch}.

normalise_batch_specs([], _Seen, Acc) ->
    {ok, lists:reverse(Acc)};
normalise_batch_specs([Spec | Rest], Seen, Acc) ->
    case normalise_batch_spec(Spec) of
        {ok, LedgerKey, Object, KeyChanges} ->
            case maps:is_key(LedgerKey, Seen) of
                true ->
                    {error, {duplicate_key, LedgerKey}};
                false ->
                    normalise_batch_specs(
                        Rest,
                        maps:put(LedgerKey, true, Seen),
                        [{LedgerKey, Object, KeyChanges} | Acc]
                    )
            end;
        {error, Reason} ->
            {error, Reason}
    end.

normalise_batch_spec(
    {put, Bucket, Key, Object, IndexSpecs, Tag, TTL}
) when is_atom(Tag) ->
    normalise_batch_spec(Bucket, Key, Object, IndexSpecs, Tag, TTL);
normalise_batch_spec(
    {delete, Bucket, Key, IndexSpecs, Tag, TTL}
) when is_atom(Tag) ->
    normalise_batch_spec(Bucket, Key, delete, IndexSpecs, Tag, TTL);
normalise_batch_spec(Spec) ->
    {error, {invalid_batch_object_spec, Spec}}.

normalise_batch_spec(_Bucket, _Key, _Object, _IndexSpecs, ?HEAD_TAG, _TTL) ->
    {error, head_tag_not_supported};
normalise_batch_spec(Bucket, Key, Object, IndexSpecs, Tag, TTL) ->
    case {valid_public_index_specs(IndexSpecs), valid_ttl(TTL)} of
        {true, true} ->
            LedgerKey = leveled_codec:to_objectkey(Bucket, Key, Tag),
            {ok, LedgerKey, Object, {IndexSpecs, TTL}};
        {false, _} ->
            {error, invalid_index_specs};
        {_, false} ->
            {error, invalid_ttl}
    end.

-spec normalise_cas_conditions(list()) ->
    {ok, list({leveled_codec:ledger_key(), absent | present | {sqn, non_neg_integer()}})}
    | {error, term()}.
normalise_cas_conditions([]) ->
    {error, empty_cas_conditions};
normalise_cas_conditions(Conditions) when is_list(Conditions) ->
    normalise_cas_conditions(Conditions, #{}, []);
normalise_cas_conditions(_Conditions) ->
    {error, invalid_cas_condition}.

normalise_cas_conditions([], _Seen, Acc) ->
    {ok, lists:reverse(Acc)};
normalise_cas_conditions([{Bucket, Key, Tag, Condition} | Rest], Seen, Acc) when
    is_atom(Tag)
->
    case normalise_cas_condition(Condition) of
        {ok, NormalCondition} ->
            case Tag of
                ?HEAD_TAG ->
                    LedgerKey = leveled_codec:to_objectkey(Bucket, Key, Tag),
                    {error, {precondition_failed, [
                        {precondition_failed, LedgerKey,
                            {expected, NormalCondition},
                            {actual, {invalid_tag, Tag}}}
                    ]}};
                _ ->
                    LedgerKey = leveled_codec:to_objectkey(Bucket, Key, Tag),
                    case maps:is_key(LedgerKey, Seen) of
                        true ->
                            {error, {precondition_failed, [
                                {precondition_failed, LedgerKey,
                                    {expected, NormalCondition},
                                    {actual, duplicate_precondition}}
                            ]}};
                        false ->
                            normalise_cas_conditions(
                                Rest,
                                maps:put(LedgerKey, true, Seen),
                                [{LedgerKey, NormalCondition} | Acc]
                            )
                    end
            end;
        {error, Reason} ->
            {error, Reason}
    end;
normalise_cas_conditions([_Condition | _Rest], _Seen, _Acc) ->
    {error, invalid_cas_condition}.

normalise_cas_condition(absent) ->
    {ok, absent};
normalise_cas_condition(present) ->
    {ok, present};
normalise_cas_condition({sqn, SQN}) when is_integer(SQN), SQN >= 0 ->
    {ok, {sqn, SQN}};
normalise_cas_condition(_Condition) ->
    {error, invalid_cas_condition}.

-spec check_cas_conditions(
    list({leveled_codec:ledger_key(), absent | present | {sqn, non_neg_integer()}}),
    #state{}
) ->
    ok | {error, list(term())}.
check_cas_conditions(Conditions, State) ->
    Failures =
        lists:foldl(
            fun({LedgerKey, Condition}, Acc) ->
                Current = current_head_state(LedgerKey, State),
                case cas_condition_passes(Condition, Current) of
                    true ->
                        Acc;
                    false ->
                        [
                            {precondition_failed, LedgerKey,
                                {expected, Condition},
                                {actual, cas_actual_state(Current)}}
                            | Acc
                        ]
                end
            end,
            [],
            Conditions
        ),
    case Failures of
        [] ->
            ok;
        _ ->
            {error, lists:reverse(Failures)}
    end.

cas_condition_passes(absent, absent) ->
    true;
cas_condition_passes(absent, tombstone) ->
    true;
cas_condition_passes(absent, expired) ->
    true;
cas_condition_passes(present, {active, _SQN, _MD}) ->
    true;
cas_condition_passes({sqn, ExpectedSQN}, {active, ExpectedSQN, _MD}) ->
    true;
cas_condition_passes(_Condition, _Current) ->
    false.

cas_actual_state({active, SQN, _MD}) ->
    {active, SQN};
cas_actual_state(Current) ->
    Current.

valid_public_index_specs(IndexSpecs) when is_list(IndexSpecs) ->
    lists:all(fun valid_public_index_spec/1, IndexSpecs);
valid_public_index_specs(_IndexSpecs) ->
    false.

valid_public_index_spec({add, IdxField, _IdxTerm}) ->
    not reserved_fts_index_field(IdxField);
valid_public_index_spec({add_payload, IdxField, _IdxTerm, Payload}) when is_binary(Payload) ->
    not reserved_fts_index_field(IdxField);
valid_public_index_spec({remove, IdxField, _IdxTerm}) ->
    not reserved_fts_index_field(IdxField);
valid_public_index_spec(_Other) ->
    false.

reserved_fts_index_field(Field) when is_tuple(Field), tuple_size(Field) > 0 ->
    reserved_fts_index_atom(element(1, Field));
reserved_fts_index_field(Field) when is_atom(Field) ->
    reserved_fts_index_atom(Field);
reserved_fts_index_field(_Field) ->
    false.

reserved_fts_index_atom(Atom) when is_atom(Atom) ->
    lists:prefix("fts_", atom_to_list(Atom));
reserved_fts_index_atom(_Other) ->
    false.

valid_ttl(infinity) ->
    true;
valid_ttl(TTL) when is_integer(TTL) ->
    true;
valid_ttl(_TTL) ->
    false.

-spec preparefor_ledgercache(
    leveled_codec:journal_key_tag() | null,
    leveled_codec:primary_key() | ?DUMMY,
    non_neg_integer(),
    any(),
    integer(),
    leveled_codec:journal_keychanges()
) ->
    {
        leveled_codec:segment_hash(),
        non_neg_integer(),
        list(leveled_codec:ledger_kv())
    }.
%% @doc
%% Prepare an object and its related key changes for addition to the Ledger
%% via the Ledger Cache.
preparefor_ledgercache(?INKT_MPUT, ?DUMMY, SQN, _O, _S, {ObjSpecs, TTL}) ->
    ObjChanges = leveled_codec:obj_objectspecs(ObjSpecs, SQN, TTL),
    {no_lookup, SQN, ObjChanges};
preparefor_ledgercache(
    ?INKT_KEYD, LedgerKey, SQN, _Obj, _Size, KeyChanges0
) when
    LedgerKey =/= ?DUMMY
->
    {IdxSpecs, TTL} = leveled_codec:unwrap_batch_keychanges(KeyChanges0),
    {Bucket, Key} = leveled_codec:from_ledgerkey(LedgerKey),
    KeyChanges =
        leveled_codec:idx_indexspecs(IdxSpecs, Bucket, Key, SQN, TTL),
    {no_lookup, SQN, KeyChanges};
preparefor_ledgercache(
    _InkTag, LedgerKey, SQN, Obj, Size, KeyChanges0
) when
    LedgerKey =/= ?DUMMY
->
    {IdxSpecs, TTL} = leveled_codec:unwrap_batch_keychanges(KeyChanges0),
    {Bucket, Key, MetaValue, {KeyH, _ObjH}, _LastMods} =
        leveled_codec:generate_ledgerkv(LedgerKey, SQN, Obj, Size, TTL),
    KeyChanges =
        [{LedgerKey, MetaValue}] ++
            leveled_codec:idx_indexspecs(IdxSpecs, Bucket, Key, SQN, TTL),
    {KeyH, SQN, KeyChanges}.

-spec recalcfor_ledgercache(
    leveled_codec:journal_key_tag() | null,
    leveled_codec:primary_key() | ?DUMMY,
    non_neg_integer(),
    binary() | term(),
    integer(),
    leveled_codec:journal_keychanges(),
    ledger_cache(),
    pid()
) ->
    {
        leveled_codec:segment_hash(),
        non_neg_integer(),
        list(leveled_codec:ledger_kv())
    }.
%% @doc
%% When loading from the journal to the ledger, may hit a key which has the
%% `recalc` strategy.  Such a key needs to recalculate the key changes by
%% comparison with the current state of the ledger, assuming it is a full
%% journal entry (i.e. KeyDeltas which may be a result of previously running
%% with a retain strategy should be ignored).
recalcfor_ledgercache(
    InkTag, _LedgerKey, SQN, _Obj, _Size, {_IdxSpecs, _TTL}, _LC, _Pcl
) when
    InkTag == ?INKT_MPUT; InkTag == ?INKT_KEYD
->
    {no_lookup, SQN, []};
recalcfor_ledgercache(
    _InkTag, LK, SQN, Obj, Size, {JournalIdxSpecs, TTL}, LedgerCache, Penciller
) when
    LK =/= ?DUMMY
->
    {Bucket, Key, MetaValue, {KeyH, _ObjH}, _LastMods} =
        leveled_codec:generate_ledgerkv(LK, SQN, Obj, Size, TTL),
    OldObject =
        case check_in_ledgercache(LK, KeyH, LedgerCache, loader) of
            false ->
                leveled_penciller:pcl_fetch(Penciller, LK, KeyH, true);
            KV ->
                KV
        end,
    OldMetadata =
        case OldObject of
            not_present ->
                not_present;
            {LK, LV} ->
                case leveled_codec:get_metadata(LV) of
                    MDO when is_tuple(MDO) ->
                        MDO
                end
        end,
    UpdMetadata =
        case leveled_codec:get_metadata(MetaValue) of
            MDU when is_tuple(MDU) ->
                MDU
        end,
    IdxSpecs =
        leveled_head:diff_indexspecs(element(1, LK), UpdMetadata, OldMetadata) ++
            reserved_fts_index_specs(JournalIdxSpecs),
    {KeyH, SQN,
        [{LK, MetaValue}] ++
            leveled_codec:idx_indexspecs(IdxSpecs, Bucket, Key, SQN, TTL)}.

reserved_fts_index_specs(IndexSpecs) ->
    [IndexSpec || IndexSpec <- IndexSpecs, reserved_fts_index_spec(IndexSpec)].

reserved_fts_index_spec({add, Field, _Term}) ->
    reserved_fts_index_field(Field);
reserved_fts_index_spec({add_payload, Field, _Term, Payload}) when is_binary(Payload) ->
    reserved_fts_index_field(Field);
reserved_fts_index_spec({remove, Field, _Term}) ->
    reserved_fts_index_field(Field);
reserved_fts_index_spec(_Other) ->
    false.

drop_incomplete_batch_loaditems(LoadItemList) ->
    BatchCounts =
        lists:foldl(
            fun({_InkTag, _PK, SQN, _Obj, KeyChanges, _ValSize}, Acc) ->
                case leveled_codec:batch_keychange_count(KeyChanges) of
                    undefined ->
                        Acc;
                    BatchSize ->
                        maps:update_with(
                            SQN,
                            fun({Count, _Expected}) ->
                                {Count + 1, BatchSize}
                            end,
                            {1, BatchSize},
                            Acc
                        )
                end
            end,
            #{},
            LoadItemList
        ),
    lists:filter(
        fun({_InkTag, _PK, SQN, _Obj, KeyChanges, _ValSize}) ->
            case leveled_codec:batch_keychange_count(KeyChanges) of
                undefined ->
                    true;
                BatchSize ->
                    case maps:get(SQN, BatchCounts) of
                        {BatchSize, BatchSize} ->
                            true;
                        _ ->
                            false
                    end
            end
        end,
        LoadItemList
    ).

-spec addto_ledgercache(
    {
        leveled_codec:segment_hash(),
        non_neg_integer(),
        list(leveled_codec:ledger_kv())
    },
    ledger_cache()
) ->
    ledger_cache().
%% @doc
%% Add a set of changes associated with a single sequence number (journal
%% update) and key to the ledger cache.  If the changes are not to be looked
%% up directly, then they will not be indexed to accelerate lookup
addto_ledgercache({H, SQN, KeyChanges}, Cache) ->
    ets:insert(Cache#ledger_cache.mem, KeyChanges),
    UpdIndex = leveled_pmem:prepare_for_index(Cache#ledger_cache.index, H),
    Cache#ledger_cache{
        index = UpdIndex,
        min_sqn = min(SQN, Cache#ledger_cache.min_sqn),
        max_sqn = max(SQN, Cache#ledger_cache.max_sqn)
    }.

-spec addto_ledgercache(
    {
        leveled_codec:segment_hash() | no_lookup,
        integer(),
        list(leveled_codec:ledger_kv())
    },
    ledger_cache(),
    loader
) ->
    ledger_cache().
%% @doc
%% Add a set of changes associated with a single sequence number (journal
%% update) to the ledger cache.  This is used explicitly when loading the
%% ledger from the Journal (i.e. at startup) - and in this case the ETS insert
%% can be bypassed, as all changes will be flushed to the Penciller before the
%% load is complete.
addto_ledgercache({H, SQN, KeyChanges}, Cache, loader) ->
    UpdQ = KeyChanges ++ Cache#ledger_cache.load_queue,
    UpdIndex = leveled_pmem:prepare_for_index(Cache#ledger_cache.index, H),
    Cache#ledger_cache{
        index = UpdIndex,
        load_queue = UpdQ,
        min_sqn = min(SQN, Cache#ledger_cache.min_sqn),
        max_sqn = max(SQN, Cache#ledger_cache.max_sqn)
    }.

-spec check_in_ledgercache(
    leveled_codec:ledger_key(),
    leveled_codec:segment_hash(),
    ledger_cache(),
    loader
) ->
    false | leveled_codec:ledger_kv().
%% @doc
%% Check the ledger cache for a Key, when the ledger cache is in loader mode
%% and so is populating a queue not an ETS table
check_in_ledgercache(PK, Hash, Cache, loader) ->
    case leveled_pmem:check_index(Hash, [Cache#ledger_cache.index]) of
        [] ->
            false;
        _ ->
            lists:keyfind(PK, 1, Cache#ledger_cache.load_queue)
    end.

-spec maybepush_ledgercache(
    pos_integer(),
    pos_integer(),
    ledger_cache(),
    pid(),
    leveled_monitor:monitor()
) ->
    {ok | returned, ledger_cache()}.
%% @doc
%% Following an update to the ledger cache, check if this now big enough to be
%% pushed down to the Penciller.  There is some random jittering here, to
%% prevent coordination across leveled instances (e.g. when running in Riak).
%%
%% The penciller may be too busy, as the LSM tree is backed up with merge
%% activity.  In this case the update is not made and 'returned' not ok is set
%% in the reply.  Try again later when it isn't busy (and also potentially
%% implement a slow_offer state to slow down the pace at which PUTs are being
%% received)
maybepush_ledgercache(
    MaxCacheSize, MaxCacheMult, Cache, Penciller, {Monitor, _}
) ->
    Tab = Cache#ledger_cache.mem,
    CacheSize = ets:info(Tab, size),
    leveled_monitor:add_stat(Monitor, {ledger_cache_size_update, CacheSize}),
    TimeToPush = maybe_withjitter(CacheSize, MaxCacheSize, MaxCacheMult),
    if
        TimeToPush ->
            CacheToLoad =
                {
                    Tab,
                    Cache#ledger_cache.index,
                    Cache#ledger_cache.min_sqn,
                    Cache#ledger_cache.max_sqn
                },
            case leveled_penciller:pcl_pushmem(Penciller, CacheToLoad) of
                ok ->
                    Cache0 = #ledger_cache{},
                    true = ets:delete(Tab),
                    NewTab = ets:new(mem, [ordered_set]),
                    {ok, Cache0#ledger_cache{mem = NewTab}};
                returned ->
                    {returned, Cache}
            end;
        true ->
            {ok, Cache}
    end.

-spec maybe_withjitter(
    non_neg_integer(), pos_integer(), pos_integer()
) -> boolean().
%% @doc
%% Push down randomly, but the closer to 4 * the maximum size, the more likely
%% a push should be
maybe_withjitter(
    CacheSize, MaxCacheSize, MaxCacheMult
) when CacheSize > MaxCacheSize ->
    R = rand:uniform(MaxCacheMult * MaxCacheSize),
    (CacheSize - MaxCacheSize) > R;
maybe_withjitter(_CacheSize, _MaxCacheSize, _MaxCacheMult) ->
    false.

-spec get_loadfun() -> initial_loadfun().
%% @doc
%% The LoadFun will be used by the Inker when walking across the Journal to
%% load the Penciller at startup.
get_loadfun() ->
    fun(KeyInJournal, ValueInJournal, _Pos, Acc0, ExtractFun) ->
        {MinSQN, MaxSQN, LoadItems} = Acc0,
        {SQN, InkTag, PK} = KeyInJournal,
        case SQN of
            SQN when SQN < MinSQN ->
                {loop, Acc0};
            SQN when SQN > MaxSQN ->
                {stop, Acc0};
            _ ->
                {VBin, ValSize} = ExtractFun(ValueInJournal),
                % VBin may already be a term
                {Obj, IdxSpecs} =
                    leveled_codec:revert_value_from_journal(VBin),
                StopAtSQN =
                    case leveled_codec:batch_keychange_count(IdxSpecs) of
                        undefined ->
                            MaxSQN;
                        _BatchSize ->
                            MaxSQN + 1
                    end,
                case SQN of
                    StopAtSQN ->
                        {stop,
                            {MinSQN, MaxSQN, [
                                {InkTag, PK, SQN, Obj, IdxSpecs, ValSize}
                                | LoadItems
                            ]}};
                    _ ->
                        {loop,
                            {MinSQN, MaxSQN, [
                                {InkTag, PK, SQN, Obj, IdxSpecs, ValSize}
                                | LoadItems
                            ]}}
                end
        end
    end.

delete_path(DirPath) ->
    ok = filelib:ensure_dir(DirPath),
    {ok, Files} = file:list_dir(DirPath),
    [file:delete(filename:join([DirPath, File])) || File <- Files],
    file:del_dir(DirPath).

-spec maybelog_put_timing(
    leveled_monitor:monitor(),
    leveled_monitor:timing(),
    leveled_monitor:timing(),
    leveled_monitor:timing(),
    pos_integer()
) -> ok.
maybelog_put_timing(
    {Pid, _StatsFreq}, InkTime, PrepTime, MemTime, Size
) when
    is_pid(Pid),
    is_integer(InkTime),
    is_integer(PrepTime),
    is_integer(MemTime)
->
    leveled_monitor:add_stat(
        Pid, {bookie_put_update, InkTime, PrepTime, MemTime, Size}
    );
maybelog_put_timing(_Monitor, _, _, _, _Size) ->
    ok.

-spec maybelog_head_timing(
    leveled_monitor:monitor(),
    leveled_monitor:timing(),
    leveled_monitor:timing(),
    boolean(),
    boolean()
) -> ok.
maybelog_head_timing({Pid, _StatsFreq}, FetchTime, RspTime, false, CH) when
    is_pid(Pid), is_integer(FetchTime), is_integer(RspTime)
->
    CH0 =
        case CH of
            true -> 1;
            false -> 0
        end,
    leveled_monitor:add_stat(
        Pid, {bookie_head_update, FetchTime, RspTime, CH0}
    );
maybelog_head_timing({Pid, _StatsFreq}, FetchTime, _, true, _CH) when
    is_pid(Pid), is_integer(FetchTime)
->
    leveled_monitor:add_stat(
        Pid, {bookie_head_update, FetchTime, not_found, 0}
    );
maybelog_head_timing(_Monitor, _, _, _NF, _CH) ->
    ok.

-spec maybelog_get_timing(
    leveled_monitor:monitor(),
    leveled_monitor:timing(),
    leveled_monitor:timing(),
    boolean()
) -> ok.
maybelog_get_timing({Pid, _StatsFreq}, HeadTime, BodyTime, false) when
    is_pid(Pid), is_integer(HeadTime), is_integer(BodyTime)
->
    leveled_monitor:add_stat(Pid, {bookie_get_update, HeadTime, BodyTime});
maybelog_get_timing({Pid, _StatsFreq}, HeadTime, _BodyTime, true) when
    is_pid(Pid), is_integer(HeadTime)
->
    leveled_monitor:add_stat(Pid, {bookie_get_update, HeadTime, not_found});
maybelog_get_timing(_Monitor, _, _, _NF) ->
    ok.

-spec maybelog_snap_timing(
    leveled_monitor:monitor(),
    leveled_monitor:timing(),
    leveled_monitor:timing()
) -> ok.
maybelog_snap_timing({Pid, _StatsFreq}, BookieTime, PCLTime) when
    is_pid(Pid), is_integer(BookieTime), is_integer(PCLTime)
->
    leveled_monitor:add_stat(Pid, {bookie_snap_update, BookieTime, PCLTime});
maybelog_snap_timing(_Monitor, _, _) ->
    ok.

status(#state{monitor = {no_monitor, 0}}) ->
    #{};
status(#state{monitor = {Monitor, _}}) ->
    leveled_monitor:get_bookie_status(Monitor).

%%%============================================================================
%%% Test
%%%============================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

%% @doc
%% Return the Inker and Penciller - {ok, Inker, Penciller}.  Used only in tests
book_returnactors(Pid) ->
    gen_server:call(Pid, return_actors).

reset_filestructure() ->
    reset_filestructure("test/test_area").

reset_filestructure(RootPath) ->
    leveled_inker:clean_testdir(RootPath ++ "/" ++ ?JOURNAL_FP),
    leveled_penciller:clean_testdir(RootPath ++ "/" ++ ?LEDGER_FP),
    RootPath.

generate_multiple_objects(Count, KeyNumber) ->
    generate_multiple_objects(Count, KeyNumber, []).

generate_multiple_objects(0, _KeyNumber, ObjL) ->
    ObjL;
generate_multiple_objects(Count, KeyNumber, ObjL) ->
    Key = list_to_binary("Key" ++ integer_to_list(KeyNumber)),
    Value = crypto:strong_rand_bytes(256),
    IndexSpec =
        [
            {
                add,
                <<"idx1_bin">>,
                list_to_binary("f" ++ integer_to_list(KeyNumber rem 10))
            }
        ],
    generate_multiple_objects(
        Count - 1,
        KeyNumber + 1,
        ObjL ++ [{Key, Value, IndexSpec}]
    ).

shutdown_test_() ->
    {timeout, 10, fun shutdown_tester/0}.

shutdown_tester() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} = book_start([{root_path, RootPath}]),
    lists:foreach(
        fun({K, V, S}) ->
            ok = book_put(Bookie1, <<"Bucket">>, K, V, S, ?STD_TAG)
        end,
        generate_multiple_objects(5000, 1)
    ),
    {ok, SnpPCL1, SnpJrnl1} =
        leveled_bookie:book_snapshot(Bookie1, store, undefined, true),

    TestPid = self(),
    spawn(
        fun() ->
            ok = leveled_bookie:book_close(Bookie1),
            TestPid ! ok
        end
    ),

    timer:sleep(2000),
    ok = leveled_penciller:pcl_close(SnpPCL1),
    case SnpJrnl1 of
        P when is_pid(P) -> ok = leveled_inker:ink_close(SnpJrnl1)
    end,
    SW = os:timestamp(),
    receive
        ok -> ok
    end,
    WaitForShutDown = timer:now_diff(SW, os:timestamp()) div 1000,
    ?assert(WaitForShutDown =< (1000 + 1)),
    _ = reset_filestructure().

ttl_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} = book_start([{root_path, RootPath}]),
    ObjL1 = generate_multiple_objects(100, 1),
    % Put in all the objects with a TTL in the future
    Future = leveled_util:integer_now() + 300,
    lists:foreach(
        fun({K, V, S}) ->
            ok =
                book_tempput(Bookie1, <<"Bucket">>, K, V, S, ?STD_TAG, Future)
        end,
        ObjL1
    ),
    lists:foreach(
        fun({K, V, _S}) ->
            {ok, V} = book_get(Bookie1, <<"Bucket">>, K, ?STD_TAG)
        end,
        ObjL1
    ),
    lists:foreach(
        fun({K, _V, _S}) ->
            {ok, _} = book_head(Bookie1, <<"Bucket">>, K, ?STD_TAG)
        end,
        ObjL1
    ),

    ObjL2 = generate_multiple_objects(100, 101),
    Past = leveled_util:integer_now() - 300,
    lists:foreach(
        fun({K, V, S}) ->
            ok = book_tempput(Bookie1, <<"Bucket">>, K, V, S, ?STD_TAG, Past)
        end,
        ObjL2
    ),
    lists:foreach(
        fun({K, _V, _S}) ->
            not_found = book_get(Bookie1, <<"Bucket">>, K, ?STD_TAG)
        end,
        ObjL2
    ),
    lists:foreach(
        fun({K, _V, _S}) ->
            not_found = book_head(Bookie1, <<"Bucket">>, K, ?STD_TAG)
        end,
        ObjL2
    ),

    {async, BucketFolder} =
        book_returnfolder(Bookie1, {bucket_stats, <<"Bucket">>}),
    {_Size, Count} = BucketFolder(),
    ?assertMatch(100, Count),
    FoldKeysFun = fun(_B, Item, FKFAcc) -> FKFAcc ++ [Item] end,
    {async, IndexFolder} =
        book_returnfolder(
            Bookie1,
            {
                index_query,
                <<"Bucket">>,
                {FoldKeysFun, []},
                {<<"idx1_bin">>, <<"f8">>, <<"f9">>},
                {false, undefined}
            }
        ),
    KeyList = IndexFolder(),
    ?assertMatch(20, length(KeyList)),

    {ok, Regex} = leveled_util:regex_compile("f8"),
    {async, IndexFolderTR} =
        book_returnfolder(
            Bookie1,
            {index_query, <<"Bucket">>, {FoldKeysFun, []},
                {<<"idx1_bin">>, <<"f8">>, <<"f9">>}, {true, Regex}}
        ),
    TermKeyList = IndexFolderTR(),
    ?assertMatch(10, length(TermKeyList)),

    ok = book_close(Bookie1),
    {ok, Bookie2} = book_start([{root_path, RootPath}]),

    {async, IndexFolderTR2} =
        book_returnfolder(
            Bookie2,
            {
                index_query,
                <<"Bucket">>,
                {FoldKeysFun, []},
                {<<"idx1_bin">>, <<"f7">>, <<"f9">>},
                {false, Regex}
            }
        ),
    KeyList2 = IndexFolderTR2(),
    ?assertMatch(10, length(KeyList2)),

    lists:foreach(
        fun({K, _V, _S}) ->
            not_found = book_get(Bookie2, <<"Bucket">>, K, ?STD_TAG)
        end,
        ObjL2
    ),
    lists:foreach(
        fun({K, _V, _S}) ->
            not_found = book_head(Bookie2, <<"Bucket">>, K, ?STD_TAG)
        end,
        ObjL2
    ),

    ok = book_close(Bookie2),
    reset_filestructure().

mget_test_() ->
    {timeout, 60, fun mget_testto/0}.

mget_testto() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} = book_start([{root_path, RootPath}]),
    ObjL1 = generate_multiple_objects(200, 1),
    lists:foreach(
        fun({K, V, S}) ->
            ok = book_put(Bookie1, <<"Bucket">>, K, V, S, ?STD_TAG)
        end,
        ObjL1
    ),
    % a tombstone and an expired object should both be not_found
    {DelK, _DelV, _DelS} = lists:nth(7, ObjL1),
    ok = book_delete(Bookie1, <<"Bucket">>, DelK, []),
    {ExpK, ExpV, ExpS} = lists:nth(11, ObjL1),
    Past = leveled_util:integer_now() - 300,
    ok = book_tempput(Bookie1, <<"Bucket">>, ExpK, ExpV, ExpS, ?STD_TAG, Past),
    Keys =
        [K || {K, _V, _S} <- ObjL1] ++
            [<<"no_such_key">>, hd([K || {K, _V, _S} <- ObjL1])],
    Results = book_mget(Bookie1, <<"Bucket">>, Keys, ?STD_TAG),
    % results are in input order (duplicates included) and every result
    % matches book_get
    ?assertEqual(Keys, [K || {K, _R} <- Results]),
    lists:foreach(
        fun({K, R}) ->
            % book_get_direct is the in-bookie oracle: book_get itself now
            % executes caller-side via the same machinery as mget
            ?assertEqual(book_get_direct(Bookie1, <<"Bucket">>, K, ?STD_TAG), R)
        end,
        Results
    ),
    ?assertEqual(not_found, proplists:get_value(DelK, Results)),
    ?assertEqual(not_found, proplists:get_value(ExpK, Results)),
    ?assertEqual([], book_mget(Bookie1, <<"Bucket">>, [], ?STD_TAG)),
    ?assertEqual(
        [{<<"no_such_key">>, not_found}],
        book_mget(Bookie1, <<"Bucket">>, [<<"no_such_key">>])
    ),
    % values are served from the journal after a restart
    ok = book_close(Bookie1),
    {ok, Bookie2} = book_start([{root_path, RootPath}]),
    ?assertEqual(Results, book_mget(Bookie2, <<"Bucket">>, Keys, ?STD_TAG)),
    ok = book_close(Bookie2),
    reset_filestructure().

fts_seq_no_regress_test_() ->
    {timeout, 60, fun fts_seq_no_regress_testto/0}.

fts_seq_no_regress_testto() ->
    % Regression for lost postings under interleaved write paths: a
    % direct put's fts_seq maintenance must never move the allocator
    % BACKWARD while caller-side intents hold higher provisional seqs -
    % a regressed counter re-allocates an in-flight seq and the two
    % batches' posting delta carriers collide (last write wins, postings
    % silently lost; observed live as a search-hit count drop).
    % Interleave held intents with direct puts, complete the held
    % caller-side writes LAST, and assert every document is searchable.
    RootPath = reset_filestructure(),
    Indexes = [#{bucket => <<"docs">>, index => <<"main">>, columns => [body]}],
    StartOpts = [{root_path, RootPath}, {fts_indexes, Indexes}],
    {ok, Bookie} = book_start(StartOpts),
    % take N intents up front (simulating concurrent workers mid-flight)
    Intents =
        [gen_server:call(Bookie, {fts_put_intent}, infinity) || _ <- lists:seq(1, 5)],
    % direct puts interleave - their fts_seq maintenance runs with
    % higher provisional seqs outstanding
    lists:foreach(
        fun(I) ->
            K = list_to_binary("direct" ++ integer_to_list(I)),
            ok = book_put_direct(
                Bookie, <<"docs">>, K, #{body => <<"direct common">>}, [],
                ?STD_TAG, infinity, false
            )
        end,
        lists:seq(1, 5)
    ),
    % a fresh intent AFTER the direct puts must not collide with the
    % held ones
    Fresh = gen_server:call(Bookie, {fts_put_intent}, infinity),
    HeldSeqs = [Seq || {ok, Seq, _Ix, _Ink} <- Intents],
    {ok, FreshSeq, _FIx, _FInk} = Fresh,
    ?assertNot(lists:member(FreshSeq, HeldSeqs)),
    ?assert(FreshSeq > lists:max(HeldSeqs)),
    % complete the held caller-side writes and the fresh one
    All = Intents ++ [Fresh],
    lists:foreach(
        fun({Idx, {ok, Seq, Ix, _Ink}}) ->
            K = list_to_binary("held" ++ integer_to_list(Idx)),
            LK = leveled_codec:to_objectkey(<<"docs">>, K, ?STD_TAG),
            Obj = #{body => <<"held common">>},
            {ok, Aug, Touched} =
                leveled_fts:augment_object_changes(
                    [{LK, Obj, {[], infinity}}], Ix, Seq
                ),
            Markers = leveled_fts:marker_cache_updates(Aug, Ix),
            ok = publish_fts_changes(
                Bookie,
                element(4, lists:nth(Idx, All)),
                Aug,
                {Touched, Seq, Seq - 1, Markers},
                false
            )
        end,
        lists:zip(lists:seq(1, length(All)), All)
    ),
    {async, Runner} =
        book_ftssearch(Bookie, <<"docs">>, <<"main">>, <<"common">>, #{}),
    {ok, Hits} = Runner(),
    ?assertEqual(11, length(Hits)),
    ok = book_close(Bookie),
    % restart: postings must all be journal-recoverable
    {ok, Bookie2} = book_start(StartOpts),
    {async, Runner2} =
        book_ftssearch(Bookie2, <<"docs">>, <<"main">>, <<"common">>, #{}),
    {ok, Hits2} = Runner2(),
    ?assertEqual(11, length(Hits2)),
    ok = book_close(Bookie2),
    reset_filestructure().

fts_put_callerside_differential_test_() ->
    {timeout, 120, fun fts_put_callerside_differential_testto/0}.

fts_put_callerside_differential_testto() ->
    % Caller-side FTS writes (put_caller_side_fts: caller augmentation,
    % caller journal write, frontier-published cache advance) must be
    % search-identical to direct-path writes: interleave both paths into
    % one FTS-indexed bucket, then every term must return exactly the
    % same keys via book_ftssearch, before AND after restart (postings
    % served from journal-recovered state).
    RootPath = reset_filestructure(),
    Indexes = [#{bucket => <<"docs">>, index => <<"main">>, columns => [body]}],
    StartOpts = [{root_path, RootPath}, {fts_indexes, Indexes}],
    {ok, Bookie1} = book_start(StartOpts),
    Put =
        fun(Bookie, K, Body, Direct) ->
            Obj = #{body => Body},
            case Direct of
                true ->
                    ok = book_put_direct(
                        Bookie, <<"docs">>, K, Obj, [], ?STD_TAG, infinity, false
                    );
                false ->
                    ok = book_put(Bookie, <<"docs">>, K, Obj, [], ?STD_TAG)
            end
        end,
    Words = [<<"alpha">>, <<"beta">>, <<"gamma">>, <<"delta">>],
    Expected =
        lists:foldl(
            fun(I, Acc) ->
                K = list_to_binary("doc" ++ integer_to_list(I)),
                Word = lists:nth(1 + (I rem length(Words)), Words),
                Body = <<Word/binary, " filler text number ",
                    (integer_to_binary(I))/binary>>,
                Put(Bookie1, K, Body, I rem 2 == 0),
                maps:update_with(Word, fun(Ks) -> [K | Ks] end, [K], Acc)
            end,
            #{},
            lists:seq(1, 60)
        ),
    CheckAll =
        fun(Bookie) ->
            maps:foreach(
                fun(Word, Ks) ->
                    {async, Runner} =
                        book_ftssearch(Bookie, <<"docs">>, <<"main">>, Word, #{}),
                    {ok, Hits} = Runner(),
                    Got = lists:sort([maps:get(key, H) || H <- Hits]),
                    ?assertEqual(lists:sort(Ks), Got)
                end,
                Expected
            )
        end,
    CheckAll(Bookie1),
    ok = book_close(Bookie1),
    {ok, Bookie2} = book_start(StartOpts),
    CheckAll(Bookie2),
    ok = book_close(Bookie2),
    reset_filestructure().

put_threephase_differential_test_() ->
    {timeout, 60, fun put_threephase_differential_testto/0}.

put_threephase_differential_testto() ->
    % book_put (caller-side journal write + in-memory publish) must equal
    % book_put_direct for every observable shape: stored value, overwrite
    % generations, delete, TTL expiry, and journal serving after restart.
    RootPath = reset_filestructure(),
    {ok, Bookie1} = book_start([{root_path, RootPath}, {max_journalsize, 100000}]),
    ObjL = generate_multiple_objects(200, 1),
    {DirectHalf, CallerHalf} = lists:split(100, ObjL),
    lists:foreach(
        fun({K, V, S}) ->
            ok = book_put_direct(
                Bookie1, <<"Bucket">>, K, V, S, ?STD_TAG, infinity, false
            )
        end,
        DirectHalf
    ),
    lists:foreach(
        fun({K, V, S}) ->
            ok = book_put(Bookie1, <<"Bucket">>, K, V, S, ?STD_TAG)
        end,
        CallerHalf
    ),
    % overwrite a range through the caller-side path
    lists:foreach(
        fun({K, V, S}) ->
            ok = book_put(Bookie1, <<"Bucket">>, K, {updated, V}, S, ?STD_TAG)
        end,
        lists:sublist(ObjL, 50, 100)
    ),
    {DelK, _DV, _DS} = lists:nth(7, ObjL),
    ok = book_delete(Bookie1, <<"Bucket">>, DelK, []),
    CheckAll =
        fun(Bookie) ->
            lists:foreach(
                fun({K, _V, _S}) ->
                    ?assertEqual(
                        book_get_direct(Bookie, <<"Bucket">>, K, ?STD_TAG),
                        book_get(Bookie, <<"Bucket">>, K, ?STD_TAG)
                    )
                end,
                ObjL
            )
        end,
    CheckAll(Bookie1),
    ok = book_close(Bookie1),
    {ok, Bookie2} = book_start([{root_path, RootPath}]),
    CheckAll(Bookie2),
    ok = book_close(Bookie2),
    reset_filestructure().

put_publish_reorder_test_() ->
    {timeout, 60, fun put_publish_reorder_testto/0}.

put_publish_reorder_testto() ->
    % Out-of-order publishes (concurrent caller-side writers) must not
    % let the ledger push watermark pass an unabsorbed SQN: interleave
    % journal writes and publishes in reversed order, force cache
    % pressure, restart, and assert every acked write survives.
    RootPath = reset_filestructure(),
    {ok, Bookie1} =
        book_start([{root_path, RootPath}, {cache_size, 10}]),
    {ok, Inker, _Fts} = gen_server:call(Bookie1, {write_refs}, infinity),
    Pairs =
        lists:map(
            fun(I) ->
                K = list_to_binary("reorder" ++ integer_to_list(I)),
                LK = leveled_codec:to_objectkey(<<"Bucket">>, K, ?STD_TAG),
                {ok, SQN, ObjSize} =
                    leveled_inker:ink_put(Inker, LK, {v, I}, {[], infinity}, false),
                Changes =
                    preparefor_ledgercache(
                        null, LK, SQN, {v, I}, ObjSize, {[], infinity}
                    ),
                {K, SQN, Changes}
            end,
            lists:seq(1, 40)
        ),
    % publish in reverse SQN order - every absorb but the last buffers
    lists:foreach(
        fun({_K, SQN, Changes}) ->
            Reply = gen_server:call(Bookie1, {publish, SQN, Changes}, infinity),
            ?assert(Reply == ok orelse Reply == pause)
        end,
        lists:reverse(Pairs)
    ),
    % interleave direct puts to drive cache pressure through the gate
    lists:foreach(
        fun(I) ->
            K = list_to_binary("direct" ++ integer_to_list(I)),
            ok = book_put(Bookie1, <<"Bucket">>, K, {d, I}, [], ?STD_TAG)
        end,
        lists:seq(1, 200)
    ),
    CheckAll =
        fun(Bookie) ->
            lists:foreach(
                fun({K, _SQN, _Changes}) ->
                    ?assertMatch(
                        {ok, {v, _}}, book_get(Bookie, <<"Bucket">>, K, ?STD_TAG)
                    )
                end,
                Pairs
            )
        end,
    CheckAll(Bookie1),
    ok = book_close(Bookie1),
    {ok, Bookie2} = book_start([{root_path, RootPath}]),
    CheckAll(Bookie2),
    ok = book_close(Bookie2),
    reset_filestructure().

mput_std_differential_test_() ->
    {timeout, 60, fun mput_std_differential_testto/0}.

mput_std_differential_testto() ->
    % book_mput_std per-entry semantics must equal N book_puts: same
    % stored values (via book_get_direct), same index behaviour, values
    % served from the journal after restart. Also pins the deprecated
    % book_batchput alias to book_mput_std equality, and book_mhead to
    % per-key book_head.
    RootPath = reset_filestructure(),
    {ok, Bookie1} = book_start([{root_path, RootPath}]),
    ObjL = generate_multiple_objects(60, 1),
    {PutHalf, MputHalf} = lists:split(30, ObjL),
    lists:foreach(
        fun({K, V, S}) ->
            ok = book_put(Bookie1, <<"Bucket">>, K, V, S, ?STD_TAG)
        end,
        PutHalf
    ),
    Specs =
        [{put, <<"Bucket">>, K, V, S, ?STD_TAG, infinity} || {K, V, S} <- MputHalf],
    ok = book_mput_std(Bookie1, Specs, false),
    CheckAll =
        fun(Bookie) ->
            lists:foreach(
                fun({K, V, _S}) ->
                    ?assertEqual(
                        {ok, V}, book_get_direct(Bookie, <<"Bucket">>, K, ?STD_TAG)
                    )
                end,
                ObjL
            ),
            Keys = [K || {K, _V, _S} <- ObjL],
            MHeads = book_mhead(Bookie, <<"Bucket">>, Keys, ?STD_TAG),
            ?assertEqual(Keys, [K || {K, _R} <- MHeads]),
            lists:foreach(
                fun({K, R}) ->
                    ?assertEqual(book_head(Bookie, <<"Bucket">>, K, ?STD_TAG), R)
                end,
                MHeads
            )
        end,
    CheckAll(Bookie1),
    ok = book_close(Bookie1),
    {ok, Bookie2} = book_start([{root_path, RootPath}]),
    CheckAll(Bookie2),
    ok = book_close(Bookie2),
    reset_filestructure().

mget_fetchspec_differential_test_() ->
    {timeout, 60, fun mget_fetchspec_differential_testto/0}.

mget_fetchspec_differential_testto() ->
    % book_mget (fetchspec path) must equal per-key book_get_direct for
    % every shape: live values across journal generations, tombstones,
    % expired TTLs, absent keys, duplicates, and post-restart serving.
    RootPath = reset_filestructure(),
    {ok, Bookie1} =
        book_start([{root_path, RootPath}, {max_journalsize, 100000}]),
    ObjL1 = generate_multiple_objects(300, 1),
    lists:foreach(
        fun({K, V, S}) ->
            ok = book_put(Bookie1, <<"Bucket">>, K, V, S, ?STD_TAG)
        end,
        ObjL1
    ),
    {DelK, _DelV, _DelS} = lists:nth(3, ObjL1),
    ok = book_delete(Bookie1, <<"Bucket">>, DelK, []),
    {ExpK, ExpV, ExpS} = lists:nth(5, ObjL1),
    Past = leveled_util:integer_now() - 300,
    ok = book_tempput(Bookie1, <<"Bucket">>, ExpK, ExpV, ExpS, ?STD_TAG, Past),
    lists:foreach(
        fun({K, V, S}) ->
            ok = book_put(Bookie1, <<"Bucket">>, K, {updated, V}, S, ?STD_TAG)
        end,
        lists:sublist(ObjL1, 20, 40)
    ),
    Keys =
        [K || {K, _V, _S} <- ObjL1] ++
            [<<"absent_key">>, hd([K || {K, _V, _S} <- ObjL1])],
    CheckAll =
        fun(Bookie) ->
            Results = book_mget(Bookie, <<"Bucket">>, Keys, ?STD_TAG),
            ?assertEqual(Keys, [K || {K, _R} <- Results]),
            lists:foreach(
                fun({K, R}) ->
                    ?assertEqual(book_get_direct(Bookie, <<"Bucket">>, K, ?STD_TAG), R)
                end,
                Results
            )
        end,
    CheckAll(Bookie1),
    ok = book_close(Bookie1),
    {ok, Bookie2} = book_start([{root_path, RootPath}]),
    CheckAll(Bookie2),
    ok = book_close(Bookie2),
    reset_filestructure().

get_runner_differential_test_() ->
    {timeout, 60, fun get_runner_differential_testto/0}.

get_runner_differential_testto() ->
    % book_get/4 (caller-side runner) must equal book_get_direct/4 (the
    % in-bookie path) for every observable shape: live values across
    % journal generations, tombstones, expired TTLs, missing keys, and
    % values served from the journal after a restart.
    RootPath = reset_filestructure(),
    {ok, Bookie1} =
        book_start([{root_path, RootPath}, {max_journalsize, 100000}]),
    ObjL1 = generate_multiple_objects(300, 1),
    lists:foreach(
        fun({K, V, S}) ->
            ok = book_put(Bookie1, <<"Bucket">>, K, V, S, ?STD_TAG)
        end,
        ObjL1
    ),
    {DelK, _DelV, _DelS} = lists:nth(3, ObjL1),
    ok = book_delete(Bookie1, <<"Bucket">>, DelK, []),
    {ExpK, ExpV, ExpS} = lists:nth(5, ObjL1),
    Past = leveled_util:integer_now() - 300,
    ok = book_tempput(Bookie1, <<"Bucket">>, ExpK, ExpV, ExpS, ?STD_TAG, Past),
    lists:foreach(
        fun({K, V, S}) ->
            ok = book_put(Bookie1, <<"Bucket">>, K, {updated, V}, S, ?STD_TAG)
        end,
        lists:sublist(ObjL1, 20, 40)
    ),
    CheckAll =
        fun(Bookie) ->
            lists:foreach(
                fun(K) ->
                    ?assertEqual(
                        book_get_direct(Bookie, <<"Bucket">>, K, ?STD_TAG),
                        book_get(Bookie, <<"Bucket">>, K, ?STD_TAG)
                    )
                end,
                [K || {K, _V, _S} <- ObjL1] ++ [<<"absent_key">>]
            )
        end,
    CheckAll(Bookie1),
    ok = book_close(Bookie1),
    {ok, Bookie2} = book_start([{root_path, RootPath}]),
    CheckAll(Bookie2),
    ok = book_close(Bookie2),
    reset_filestructure().

get_concurrent_scaling_test_() ->
    {timeout, 120, fun get_concurrent_scaling_testto/0}.

get_concurrent_scaling_testto() ->
    % The property caller-side execution exists for: N concurrent readers
    % of journal-resident values must not serialize through the
    % bookie/inker singletons. Loose bound (guards the architecture, not
    % a machine-specific ratio).
    RootPath = reset_filestructure(),
    {ok, Bookie1} =
        book_start([{root_path, RootPath}, {max_journalsize, 10000000}]),
    Value = crypto:strong_rand_bytes(65536),
    Keys =
        [
            begin
                K = list_to_binary("conc_key" ++ integer_to_list(I)),
                ok = book_put(Bookie1, <<"Bucket">>, K, Value, [], ?STD_TAG),
                K
            end
         || I <- lists:seq(1, 64)
        ],
    ReadAll =
        fun() ->
            lists:foreach(
                fun(K) ->
                    {ok, _} = book_get(Bookie1, <<"Bucket">>, K, ?STD_TAG)
                end,
                Keys
            )
        end,
    ReadAll(),
    {SerialUS, ok} = timer:tc(fun() -> ReadAll(), ok end),
    Readers = 8,
    Parent = self(),
    {ConcurrentUS, ok} =
        timer:tc(
            fun() ->
                Refs =
                    [
                        begin
                            Ref = make_ref(),
                            spawn_link(fun() ->
                                ReadAll(),
                                Parent ! {done, Ref}
                            end),
                            Ref
                        end
                     || _ <- lists:seq(1, Readers)
                    ],
                lists:foreach(
                    fun(Ref) ->
                        receive
                            {done, Ref} -> ok
                        end
                    end,
                    Refs
                ),
                ok
            end
        ),
    ok = book_close(Bookie1),
    reset_filestructure(),
    ?assert(ConcurrentUS < SerialUS * 4 + 500000).

hashlist_query_test_() ->
    {timeout, 60, fun hashlist_query_testto/0}.

hashlist_query_testto() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} =
        book_start(
            [
                {root_path, RootPath},
                {max_journalsize, 1000000},
                {cache_size, 500}
            ]
        ),
    ObjL1 = generate_multiple_objects(1200, 1),
    % Put in all the objects with a TTL in the future
    Future = leveled_util:integer_now() + 300,
    lists:foreach(
        fun({K, V, S}) ->
            ok = book_tempput(Bookie1, <<"Bucket">>, K, V, S, ?STD_TAG, Future)
        end,
        ObjL1
    ),
    ObjL2 = generate_multiple_objects(20, 1201),
    % Put in a few objects with a TTL in the past
    Past = leveled_util:integer_now() - 300,
    lists:foreach(
        fun({K, V, S}) ->
            ok = book_tempput(Bookie1, <<"Bucket">>, K, V, S, ?STD_TAG, Past)
        end,
        ObjL2
    ),
    % Scan the store for the Bucket, Keys and Hashes
    {async, HTFolder} =
        book_returnfolder(Bookie1, {hashlist_query, ?STD_TAG, false}),
    KeyHashList = HTFolder(),
    lists:foreach(
        fun({B, _K, H}) ->
            ?assertMatch(<<"Bucket">>, B),
            ?assertMatch(true, is_integer(H))
        end,
        KeyHashList
    ),

    ?assertMatch(1200, length(KeyHashList)),
    ok = book_close(Bookie1),
    {ok, Bookie2} =
        book_start(
            [
                {root_path, RootPath},
                {max_journalsize, 200000},
                {cache_size, 500}
            ]
        ),
    {async, HTFolder2} =
        book_returnfolder(Bookie2, {hashlist_query, ?STD_TAG, false}),
    L0 = length(KeyHashList),
    HTR2 = HTFolder2(),
    ?assertMatch(L0, length(HTR2)),
    ?assertMatch(KeyHashList, HTR2),
    ok = book_close(Bookie2),
    reset_filestructure().

hashlist_query_withjournalcheck_test_() ->
    {timeout, 60, fun hashlist_query_withjournalcheck_testto/0}.

hashlist_query_withjournalcheck_testto() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} =
        book_start(
            [
                {root_path, RootPath},
                {max_journalsize, 1000000},
                {cache_size, 500}
            ]
        ),
    ObjL1 = generate_multiple_objects(800, 1),
    % Put in all the objects with a TTL in the future
    Future = leveled_util:integer_now() + 300,
    lists:foreach(
        fun({K, V, S}) ->
            ok = book_tempput(Bookie1, <<"Bucket">>, K, V, S, ?STD_TAG, Future)
        end,
        ObjL1
    ),
    {async, HTFolder1} =
        book_returnfolder(Bookie1, {hashlist_query, ?STD_TAG, false}),
    KeyHashList = HTFolder1(),
    {async, HTFolder2} =
        book_returnfolder(Bookie1, {hashlist_query, ?STD_TAG, true}),
    ?assertMatch(KeyHashList, HTFolder2()),
    ok = book_close(Bookie1),
    reset_filestructure().

foldobjects_vs_hashtree_test_() ->
    {timeout, 60, fun foldobjects_vs_hashtree_testto/0}.

foldobjects_vs_hashtree_testto() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} =
        book_start(
            [
                {
                    root_path, RootPath
                },
                {max_journalsize, 1000000},
                {cache_size, 500}
            ]
        ),
    ObjL1 = generate_multiple_objects(800, 1),
    % Put in all the objects with a TTL in the future
    Future = leveled_util:integer_now() + 300,
    lists:foreach(
        fun({K, V, S}) ->
            ok =
                book_tempput(Bookie1, <<"Bucket">>, K, V, S, ?STD_TAG, Future)
        end,
        ObjL1
    ),
    {async, HTFolder1} =
        book_returnfolder(Bookie1, {hashlist_query, ?STD_TAG, false}),
    KeyHashList1 = lists:usort(HTFolder1()),

    FoldObjectsFun =
        fun(B, K, V, Acc) ->
            [{B, K, erlang:phash2(term_to_binary(V))} | Acc]
        end,
    {async, HTFolder2} =
        book_returnfolder(
            Bookie1,
            {
                foldobjects_allkeys,
                ?STD_TAG,
                FoldObjectsFun,
                true
            }
        ),
    KeyHashList2 = HTFolder2(),
    ?assertMatch(KeyHashList1, lists:usort(KeyHashList2)),

    FoldHeadsFun =
        fun(B, K, ProxyV, Acc) ->
            {proxy_object, _MDBin, _Size, {FetchFun, Clone, JK}} =
                binary_to_term(ProxyV),
            V = FetchFun(Clone, JK),
            [{B, K, erlang:phash2(term_to_binary(V))} | Acc]
        end,

    {async, HTFolder3} =
        book_returnfolder(
            Bookie1,
            {
                foldheads_allkeys,
                ?STD_TAG,
                FoldHeadsFun,
                true,
                true,
                false,
                false,
                false
            }
        ),
    KeyHashList3 = HTFolder3(),
    ?assertMatch(KeyHashList1, lists:usort(KeyHashList3)),

    FoldHeadsFun2 =
        fun(B, K, ProxyV, Acc) ->
            {proxy_object, MD, _Size1, _Fetcher} = binary_to_term(ProxyV),
            {Hash, _Size0, _UserDefinedMD} = MD,
            [{B, K, Hash} | Acc]
        end,

    {async, HTFolder4} =
        book_returnfolder(
            Bookie1,
            {
                foldheads_allkeys,
                ?STD_TAG,
                FoldHeadsFun2,
                false,
                false,
                false,
                false,
                false
            }
        ),
    KeyHashList4 = HTFolder4(),
    ?assertMatch(KeyHashList1, lists:usort(KeyHashList4)),

    ok = book_close(Bookie1),
    reset_filestructure().

foldobjects_vs_foldheads_bybucket_test_() ->
    {timeout, 60, fun foldobjects_vs_foldheads_bybucket_testto/0}.

foldobjects_vs_foldheads_bybucket_testto() ->
    folder_cache_test(10),
    folder_cache_test(100),
    folder_cache_test(300),
    folder_cache_test(1000).

folder_cache_test(CacheSize) ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} =
        book_start(
            [
                {
                    root_path, RootPath
                },
                {max_journalsize, 1000000},
                {cache_size, CacheSize}
            ]
        ),
    _ = book_returnactors(Bookie1),
    ObjL1 = generate_multiple_objects(400, 1),
    ObjL2 = generate_multiple_objects(400, 1),
    % Put in all the objects with a TTL in the future
    Future = leveled_util:integer_now() + 300,
    lists:foreach(
        fun({K, V, S}) ->
            ok =
                book_tempput(Bookie1, <<"BucketA">>, K, V, S, ?STD_TAG, Future)
        end,
        ObjL1
    ),
    lists:foreach(
        fun({K, V, S}) ->
            ok =
                book_tempput(Bookie1, <<"BucketB">>, K, V, S, ?STD_TAG, Future)
        end,
        ObjL2
    ),

    FoldObjectsFun =
        fun(B, K, V, Acc) ->
            [{B, K, erlang:phash2(term_to_binary(V))} | Acc]
        end,
    {async, HTFolder1A} =
        book_returnfolder(
            Bookie1,
            {
                foldobjects_bybucket,
                ?STD_TAG,
                <<"BucketA">>,
                all,
                FoldObjectsFun,
                false
            }
        ),
    KeyHashList1A = HTFolder1A(),
    {async, HTFolder1B} =
        book_returnfolder(
            Bookie1,
            {
                foldobjects_bybucket,
                ?STD_TAG,
                <<"BucketB">>,
                all,
                FoldObjectsFun,
                true
            }
        ),
    KeyHashList1B = HTFolder1B(),
    ?assertMatch(
        false,
        lists:usort(KeyHashList1A) == lists:usort(KeyHashList1B)
    ),

    FoldHeadsFun =
        fun(B, K, ProxyV, Acc) ->
            {proxy_object, _MDBin, _Size, {FetchFun, Clone, JK}} =
                binary_to_term(ProxyV),
            V = FetchFun(Clone, JK),
            [{B, K, erlang:phash2(term_to_binary(V))} | Acc]
        end,

    {async, HTFolder2A} =
        book_returnfolder(
            Bookie1,
            {
                foldheads_bybucket,
                ?STD_TAG,
                <<"BucketA">>,
                all,
                FoldHeadsFun,
                true,
                true,
                false,
                false,
                false
            }
        ),
    KeyHashList2A = return_list_result(HTFolder2A),
    {async, HTFolder2B} =
        book_returnfolder(
            Bookie1,
            {
                foldheads_bybucket,
                ?STD_TAG,
                <<"BucketB">>,
                all,
                FoldHeadsFun,
                true,
                false,
                false,
                false,
                false
            }
        ),
    KeyHashList2B = return_list_result(HTFolder2B),

    ?assertMatch(
        true,
        lists:usort(KeyHashList1A) == lists:usort(KeyHashList2A)
    ),
    ?assertMatch(
        true,
        lists:usort(KeyHashList1B) == lists:usort(KeyHashList2B)
    ),

    {async, HTFolder2C} =
        book_returnfolder(
            Bookie1,
            {
                foldheads_bybucket,
                ?STD_TAG,
                <<"BucketB">>,
                {<<"Key">>, <<"$all">>},
                FoldHeadsFun,
                true,
                false,
                false,
                false,
                false
            }
        ),
    KeyHashList2C = return_list_result(HTFolder2C),
    {async, HTFolder2D} =
        book_returnfolder(
            Bookie1,
            {
                foldheads_bybucket,
                ?STD_TAG,
                <<"BucketB">>,
                {<<"Key">>, <<"Keyzzzzz">>},
                FoldHeadsFun,
                true,
                true,
                false,
                false,
                false
            }
        ),
    KeyHashList2D = return_list_result(HTFolder2D),
    ?assertMatch(
        true,
        lists:usort(KeyHashList2B) == lists:usort(KeyHashList2C)
    ),
    ?assertMatch(
        true,
        lists:usort(KeyHashList2B) == lists:usort(KeyHashList2D)
    ),

    CheckSplitQueryFun =
        fun(SplitInt) ->
            io:format("Testing SplitInt ~w~n", [SplitInt]),
            SplitIntEnd =
                list_to_binary("Key" ++ integer_to_list(SplitInt) ++ "|"),
            SplitIntStart =
                list_to_binary("Key" ++ integer_to_list(SplitInt + 1)),
            {async, HTFolder2E} =
                book_returnfolder(
                    Bookie1,
                    {
                        foldheads_bybucket,
                        ?STD_TAG,
                        <<"BucketB">>,
                        {<<"Key">>, SplitIntEnd},
                        FoldHeadsFun,
                        true,
                        false,
                        false,
                        false,
                        false
                    }
                ),
            KeyHashList2E = return_list_result(HTFolder2E),
            {async, HTFolder2F} =
                book_returnfolder(
                    Bookie1,
                    {
                        foldheads_bybucket,
                        ?STD_TAG,
                        <<"BucketB">>,
                        {SplitIntStart, <<"Key|">>},
                        FoldHeadsFun,
                        true,
                        false,
                        false,
                        false,
                        false
                    }
                ),
            KeyHashList2F = return_list_result(HTFolder2F),

            ?assertMatch(true, length(KeyHashList2E) > 0),
            ?assertMatch(true, length(KeyHashList2F) > 0),

            io:format(
                "Length of 2B ~w 2E ~w 2F ~w~n",
                [
                    length(KeyHashList2B),
                    length(KeyHashList2E),
                    length(KeyHashList2F)
                ]
            ),
            CompareL = lists:usort(KeyHashList2E ++ KeyHashList2F),
            ?assertMatch(true, lists:usort(KeyHashList2B) == CompareL)
        end,

    lists:foreach(CheckSplitQueryFun, [1, 4, 8, 300, 100, 400, 200, 600]),

    ok = book_close(Bookie1),
    reset_filestructure().

-spec return_list_result(fun(() -> list(dynamic()))) -> list().
return_list_result(FoldFun) ->
    case FoldFun() of
        HL when is_list(HL) ->
            HL
    end.

small_cachesize_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} =
        book_start(
            [
                {
                    root_path, RootPath
                },
                {max_journalsize, 1000000},
                {cache_size, 1}
            ]
        ),
    ok = leveled_bookie:book_close(Bookie1).

is_empty_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} =
        book_start(
            [
                {
                    root_path, RootPath
                },
                {max_journalsize, 1000000},
                {cache_size, 500}
            ]
        ),
    % Put in an object with a TTL in the future
    Future = leveled_util:integer_now() + 300,
    ?assertMatch(true, leveled_bookie:book_isempty(Bookie1, ?STD_TAG)),
    ok =
        book_tempput(
            Bookie1, <<"B">>, <<"K">>, {value, <<"V">>}, [], ?STD_TAG, Future
        ),
    ?assertMatch(false, leveled_bookie:book_isempty(Bookie1, ?STD_TAG)),
    ?assertMatch(true, leveled_bookie:book_isempty(Bookie1, ?RIAK_TAG)),

    ok = leveled_bookie:book_close(Bookie1).

is_empty_headonly_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} =
        book_start(
            [
                {root_path, RootPath},
                {max_journalsize, 1000000},
                {cache_size, 500},
                {head_only, no_lookup}
            ]
        ),
    ?assertMatch(true, book_isempty(Bookie1, ?HEAD_TAG)),
    ObjSpecs =
        [
            {add, <<"B1">>, <<"K1">>, <<1:8/integer>>, {size, 100}},
            {remove, <<"B1">>, <<"K1">>, <<0:8/integer>>, null}
        ],
    ok = book_mput(Bookie1, ObjSpecs),
    ?assertMatch(false, book_isempty(Bookie1, ?HEAD_TAG)),
    ok = book_close(Bookie1).

undefined_rootpath_test() ->
    Opts = [{max_journalsize, 1000000}, {cache_size, 500}],
    error_logger:tty(false),
    R = gen_server:start(?MODULE, [set_defaults(Opts)], []),
    ?assertMatch({error, no_root_path}, R),
    error_logger:tty(true).

foldkeys_headonly_test() ->
    foldkeys_headonly_tester(5000, 25, <<"BucketStr">>),
    foldkeys_headonly_tester(2000, 25, <<"B0">>).

foldkeys_headonly_tester(ObjectCount, BlockSize, BStr) ->
    RootPath = reset_filestructure(),

    {ok, Bookie1} =
        book_start(
            [
                {root_path, RootPath},
                {max_journalsize, 1000000},
                {cache_size, 500},
                {head_only, no_lookup}
            ]
        ),
    GenObjSpecFun =
        fun(I) ->
            Key = I rem 6,
            {add, BStr, <<Key:8/integer>>, <<I:32/integer>>, null}
        end,
    ObjSpecs = lists:map(GenObjSpecFun, lists:seq(1, ObjectCount)),
    ObjSpecBlocks =
        lists:map(
            fun(I) ->
                lists:sublist(ObjSpecs, I * BlockSize + 1, BlockSize)
            end,
            lists:seq(0, ObjectCount div BlockSize - 1)
        ),
    lists:map(fun(Block) -> book_mput(Bookie1, Block) end, ObjSpecBlocks),
    ?assertMatch(false, book_isempty(Bookie1, ?HEAD_TAG)),

    FolderT =
        {keylist, ?HEAD_TAG, BStr,
            {fun(_B, {K, SK}, Acc) -> [{K, SK} | Acc] end, []}},

    Key_SKL_Compare =
        lists:usort(
            lists:map(
                fun({add, _B, K, SK, _V}) -> {K, SK} end, ObjSpecs
            )
        ),
    {async, Folder1} = book_returnfolder(Bookie1, FolderT),
    case Folder1() of
        Key_SKL1 when is_list(Key_SKL1) ->
            ?assertMatch(Key_SKL_Compare, lists:reverse(Key_SKL1))
    end,

    ok = book_close(Bookie1),

    {ok, Bookie2} =
        book_start(
            [
                {root_path, RootPath},
                {max_journalsize, 1000000},
                {cache_size, 500},
                {head_only, no_lookup}
            ]
        ),

    {async, Folder2} = book_returnfolder(Bookie2, FolderT),
    case Folder2() of
        Key_SKL2 when is_list(Key_SKL2) ->
            ?assertMatch(Key_SKL_Compare, lists:reverse(Key_SKL2))
    end,

    ok = book_close(Bookie2).

is_empty_stringkey_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} =
        book_start(
            [
                {root_path, RootPath},
                {max_journalsize, 1000000},
                {cache_size, 500}
            ]
        ),
    ?assertMatch(true, book_isempty(Bookie1, ?STD_TAG)),
    Past = leveled_util:integer_now() - 300,
    ?assertMatch(true, leveled_bookie:book_isempty(Bookie1, ?STD_TAG)),
    ok =
        book_tempput(
            Bookie1, <<"B">>, <<"K">>, {value, <<"V">>}, [], ?STD_TAG, Past
        ),
    ok = book_put(Bookie1, <<"B">>, <<"K0">>, {value, <<"V">>}, [], ?STD_TAG),
    ?assertMatch(false, book_isempty(Bookie1, ?STD_TAG)),
    ok = book_close(Bookie1).

scan_table_test() ->
    K1 =
        leveled_codec:to_objectkey(
            <<"B1">>, <<"K1">>, ?IDX_TAG, <<"F1-bin">>, <<"AA1">>
        ),
    K2 =
        leveled_codec:to_objectkey(
            <<"B1">>, <<"K2">>, ?IDX_TAG, <<"F1-bin">>, <<"AA1">>
        ),
    K3 =
        leveled_codec:to_objectkey(
            <<"B1">>, <<"K3">>, ?IDX_TAG, <<"F1-bin">>, <<"AB1">>
        ),
    K4 =
        leveled_codec:to_objectkey(
            <<"B1">>, <<"K4">>, ?IDX_TAG, <<"F1-bin">>, <<"AA2">>
        ),
    K5 =
        leveled_codec:to_objectkey(
            <<"B2">>, <<"K5">>, ?IDX_TAG, <<"F1-bin">>, <<"AA2">>
        ),
    Tab0 = ets:new(mem, [ordered_set]),

    SK_A0 =
        leveled_codec:to_querykey(
            <<"B1">>, null, ?IDX_TAG, <<"F1-bin">>, <<"AA0">>
        ),
    EK_A9 =
        leveled_codec:to_querykey(
            <<"B1">>, null, ?IDX_TAG, <<"F1-bin">>, <<"AA9">>
        ),
    Empty = {[], infinity, 0},
    ?assertMatch(Empty, scan_table(Tab0, SK_A0, EK_A9)),
    ets:insert(Tab0, [{K1, {1, active, no_lookup, null}}]),
    ?assertMatch({[{K1, _}], 1, 1}, scan_table(Tab0, SK_A0, EK_A9)),
    ets:insert(Tab0, [{K2, {2, active, no_lookup, null}}]),
    ?assertMatch({[{K1, _}, {K2, _}], 1, 2}, scan_table(Tab0, SK_A0, EK_A9)),
    ets:insert(Tab0, [{K3, {3, active, no_lookup, null}}]),
    ?assertMatch({[{K1, _}, {K2, _}], 1, 2}, scan_table(Tab0, SK_A0, EK_A9)),
    ets:insert(Tab0, [{K4, {4, active, no_lookup, null}}]),
    ?assertMatch(
        {[{K1, _}, {K2, _}, {K4, _}], 1, 4},
        scan_table(Tab0, SK_A0, EK_A9)
    ),
    ets:insert(Tab0, [{K5, {5, active, no_lookup, null}}]),
    ?assertMatch(
        {[{K1, _}, {K2, _}, {K4, _}], 1, 4},
        scan_table(Tab0, SK_A0, EK_A9)
    ).

longrunning_test() ->
    SW = os:timestamp(),
    timer:sleep(?LONG_RUNNING div 1000 + 100),
    ok = maybe_longrunning(SW, put).

coverage_cheat_test() ->
    DummyState = #state{inker = list_to_pid("<0.101.0>")},
    {noreply, _State0} = handle_info(timeout, DummyState),
    {ok, _State1} = code_change(null, DummyState, null).

erase_journal_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} =
        book_start(
            [
                {root_path, RootPath},
                {max_journalsize, 50000},
                {cache_size, 100}
            ]
        ),
    ObjL1 = generate_multiple_objects(500, 1),
    % Put in all the objects with a TTL in the future
    lists:foreach(
        fun({K, V, S}) ->
            ok = book_put(Bookie1, <<"Bucket">>, K, V, S, ?STD_TAG)
        end,
        ObjL1
    ),
    lists:foreach(
        fun({K, V, _S}) ->
            {ok, V} = book_get(Bookie1, <<"Bucket">>, K, ?STD_TAG)
        end,
        ObjL1
    ),

    CheckHeadFun =
        fun(Book) ->
            fun({K, _V, _S}, Acc) ->
                case book_head(Book, <<"Bucket">>, K, ?STD_TAG) of
                    {ok, _Head} -> Acc;
                    not_found -> Acc + 1
                end
            end
        end,
    HeadsNotFound1 = lists:foldl(CheckHeadFun(Bookie1), 0, ObjL1),
    ?assertMatch(0, HeadsNotFound1),

    ok = book_close(Bookie1),
    io:format("Bookie closed - clearing Journal~n"),
    leveled_inker:clean_testdir(RootPath ++ "/" ++ ?JOURNAL_FP),
    {ok, Bookie2} =
        book_start([
            {root_path, RootPath},
            {max_journalsize, 5000},
            {cache_size, 100}
        ]),
    HeadsNotFound2 = lists:foldl(CheckHeadFun(Bookie2), 0, ObjL1),
    ?assertMatch(500, HeadsNotFound2),
    ok = book_destroy(Bookie2).

journalfold_changefeed_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} =
        book_start([
            {root_path, RootPath},
            {max_journalsize, 1000000},
            {cache_size, 500},
            {compression_method, none}
        ]),
    ok = book_put(Bookie1, <<"B">>, <<"K1">>, {value, <<"V1">>}, [], ?STD_TAG),
    {ok, MidSQN} = book_journalsqn(Bookie1),
    ok =
        book_batchput(Bookie1, [
            {put, <<"B">>, <<"K2">>, {value, <<"V2">>}, [], ?STD_TAG, infinity},
            {put, <<"B">>, <<"K1">>, {value, <<"V1b">>}, [], ?STD_TAG, infinity}
        ]),
    ok = book_delete(Bookie1, <<"B">>, <<"K2">>, []),
    FoldAccT =
        {fun(B, K, SQN, Change, Acc) -> [{B, K, SQN, Change} | Acc] end, []},
    {async, FullFolder} = book_journalfold(Bookie1, ?STD_TAG, 0, FoldAccT),
    Full = lists:reverse(FullFolder()),
    ?assertMatch(
        [
            {<<"B">>, <<"K1">>, _, {put, {value, <<"V1">>}}},
            {<<"B">>, <<"K2">>, _, {put, {value, <<"V2">>}}},
            {<<"B">>, <<"K1">>, _, {put, {value, <<"V1b">>}}},
            {<<"B">>, <<"K2">>, _, delete}
        ],
        Full
    ),
    %% SQNs are non-decreasing in fold order; both batch entries share one.
    SQNs = [SQN || {_B, _K, SQN, _C} <- Full],
    ?assertMatch(true, SQNs =:= lists:sort(SQNs)),
    [_S1, S2, S3, _S4] = SQNs,
    ?assertMatch(S2, S3),
    %% Resume from a cursor: only entries after the first put.
    {async, TailFolder} =
        book_journalfold(Bookie1, ?STD_TAG, MidSQN + 1, FoldAccT),
    Tail = lists:reverse(TailFolder()),
    ?assertMatch(3, length(Tail)),
    ?assertMatch(
        [{<<"B">>, <<"K2">>, _, {put, _}} | _Rest],
        Tail
    ),
    ok = book_destroy(Bookie1).

batchput_standard_objects_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} =
        book_start([
            {root_path, RootPath},
            {max_journalsize, 1000000},
            {cache_size, 500},
            {compression_method, none}
        ]),
    BatchSpecs = [
        {put, <<"B">>, <<"K1">>, {value, <<"V1">>}, [
            {add, <<"city_bin">>, <<"NYC">>}
        ], ?STD_TAG, infinity},
        {put, <<"B">>, <<"K2">>, {value, <<"V2">>}, [
            {add, <<"city_bin">>, <<"SYD">>}
        ], ?STD_TAG, infinity}
    ],
    ok = book_batchput(Bookie1, BatchSpecs),
    {ok, {value, <<"V1">>}} = book_get(Bookie1, <<"B">>, <<"K1">>),
    {ok, {value, <<"V2">>}} = book_get(Bookie1, <<"B">>, <<"K2">>),
    {ok, _Head1} = book_head(Bookie1, <<"B">>, <<"K1">>),
    {ok, SQN1} = book_sqn(Bookie1, <<"B">>, <<"K1">>),
    {ok, SQN1} = book_sqn(Bookie1, <<"B">>, <<"K2">>),
    {async, IdxFolder1} =
        book_indexfold(
            Bookie1,
            <<"B">>,
            {fun(_B, {IdxV, K}, Acc) -> [{IdxV, K} | Acc] end, []},
            {<<"city_bin">>, <<"NYC">>, <<"NYC">>},
            {true, undefined}
        ),
    ?assertMatch([{<<"NYC">>, <<"K1">>}], IdxFolder1()),
    ok = book_close(Bookie1),
    {ok, Bookie2} =
        book_start([
            {root_path, RootPath},
            {max_journalsize, 1000000},
            {cache_size, 500},
            {compression_method, none}
        ]),
    {ok, {value, <<"V1">>}} = book_get(Bookie2, <<"B">>, <<"K1">>),
    {ok, {value, <<"V2">>}} = book_get(Bookie2, <<"B">>, <<"K2">>),
    {async, IdxFolder2} =
        book_indexfold(
            Bookie2,
            <<"B">>,
            {fun(_B, {IdxV, K}, Acc) -> [{IdxV, K} | Acc] end, []},
            {<<"city_bin">>, <<"NYC">>, <<"NYC">>},
            {true, undefined}
        ),
    ?assertMatch([{<<"NYC">>, <<"K1">>}], IdxFolder2()),
    ok =
        book_batchput(Bookie2, [
            {delete, <<"B">>, <<"K1">>, [
                {remove, <<"city_bin">>, <<"NYC">>}
            ], ?STD_TAG, infinity}
        ]),
    not_found = book_get(Bookie2, <<"B">>, <<"K1">>),
    not_found = book_head(Bookie2, <<"B">>, <<"K1">>),
    {async, IdxFolder3} =
        book_indexfold(
            Bookie2,
            <<"B">>,
            {fun(_B, {IdxV, K}, Acc) -> [{IdxV, K} | Acc] end, []},
            {<<"city_bin">>, <<"NYC">>, <<"NYC">>},
            {true, undefined}
        ),
    ?assertMatch([], IdxFolder3()),
    ok = book_destroy(Bookie2).

casput_conditions_and_sqn_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} =
        book_start([
            {root_path, RootPath},
            {max_journalsize, 1000000},
            {cache_size, 500},
            {compression_method, none}
        ]),
    not_found = book_get_sqn(Bookie1, <<"B">>, <<"K1">>),
    ok =
        book_casput(
            Bookie1,
            <<"B">>,
            <<"K1">>,
            {value, <<"V1">>},
            [{add, <<"city_bin">>, <<"NYC">>}],
            ?STD_TAG,
            infinity,
            false,
            absent
        ),
    {ok, {value, <<"V1">>}, SQN1} =
        book_get_sqn(Bookie1, <<"B">>, <<"K1">>),
    {ok, _Head1, SQN1} = book_head_sqn(Bookie1, <<"B">>, <<"K1">>),
    {async, NYCFolder1} =
        book_indexfold(
            Bookie1,
            <<"B">>,
            {fun(_B, {IdxV, K}, Acc) -> [{IdxV, K} | Acc] end, []},
            {<<"city_bin">>, <<"NYC">>, <<"NYC">>},
            {true, undefined}
        ),
    ?assertMatch([{<<"NYC">>, <<"K1">>}], NYCFolder1()),
    ?assertMatch(
        {error, {precondition_failed, [
            {precondition_failed, _, {expected, absent}, {actual, {active, SQN1}}}
        ]}},
        book_casput(
            Bookie1,
            <<"B">>,
            <<"K1">>,
            {value, <<"LEAK">>},
            [{add, <<"city_bin">>, <<"LEAK">>}],
            ?STD_TAG,
            infinity,
            false,
            absent
        )
    ),
    {ok, {value, <<"V1">>}, SQN1} =
        book_get_sqn(Bookie1, <<"B">>, <<"K1">>),
    {async, LeakFolder1} =
        book_indexfold(
            Bookie1,
            <<"B">>,
            {fun(_B, {IdxV, K}, Acc) -> [{IdxV, K} | Acc] end, []},
            {<<"city_bin">>, <<"LEAK">>, <<"LEAK">>},
            {true, undefined}
        ),
    ?assertMatch([], LeakFolder1()),
    ?assertMatch(
        {error, {precondition_failed, [_]}},
        book_casput(
            Bookie1,
            <<"B">>,
            <<"K1">>,
            {value, <<"STALE">>},
            [{add, <<"city_bin">>, <<"STALE">>}],
            ?STD_TAG,
            infinity,
            false,
            {sqn, SQN1 + 1000}
        )
    ),
    {ok, {value, <<"V1">>}, SQN1} =
        book_get_sqn(Bookie1, <<"B">>, <<"K1">>),
    {async, StaleFolder1} =
        book_indexfold(
            Bookie1,
            <<"B">>,
            {fun(_B, {IdxV, K}, Acc) -> [{IdxV, K} | Acc] end, []},
            {<<"city_bin">>, <<"STALE">>, <<"STALE">>},
            {true, undefined}
        ),
    ?assertMatch([], StaleFolder1()),
    ok =
        book_casput(
            Bookie1,
            <<"B">>,
            <<"K1">>,
            {value, <<"V2">>},
            [
                {remove, <<"city_bin">>, <<"NYC">>},
                {add, <<"city_bin">>, <<"SYD">>}
            ],
            ?STD_TAG,
            infinity,
            false,
            {sqn, SQN1}
        ),
    {ok, {value, <<"V2">>}, SQN2} =
        book_get_sqn(Bookie1, <<"B">>, <<"K1">>),
    true = SQN2 > SQN1,
    {async, NYCFolder2} =
        book_indexfold(
            Bookie1,
            <<"B">>,
            {fun(_B, {IdxV, K}, Acc) -> [{IdxV, K} | Acc] end, []},
            {<<"city_bin">>, <<"NYC">>, <<"NYC">>},
            {true, undefined}
        ),
    ?assertMatch([], NYCFolder2()),
    {async, SYDFolder1} =
        book_indexfold(
            Bookie1,
            <<"B">>,
            {fun(_B, {IdxV, K}, Acc) -> [{IdxV, K} | Acc] end, []},
            {<<"city_bin">>, <<"SYD">>, <<"SYD">>},
            {true, undefined}
        ),
    ?assertMatch([{<<"SYD">>, <<"K1">>}], SYDFolder1()),
    ok =
        book_casbatchput(
            Bookie1,
            [
                {delete, <<"B">>, <<"K1">>, [
                    {remove, <<"city_bin">>, <<"SYD">>}
                ], ?STD_TAG, infinity}
            ],
            [{<<"B">>, <<"K1">>, ?STD_TAG, present}]
        ),
    not_found = book_get_sqn(Bookie1, <<"B">>, <<"K1">>),
    ok =
        book_casput(
            Bookie1,
            <<"B">>,
            <<"K1">>,
            {value, <<"V3">>},
            [],
            ?STD_TAG,
            infinity,
            false,
            absent
        ),
    {ok, {value, <<"V3">>}, _SQN3} =
        book_get_sqn(Bookie1, <<"B">>, <<"K1">>),
    ok =
        book_casput(
            Bookie1,
            <<"B">>,
            <<"K1">>,
            {value, <<"V4">>},
            [],
            ?STD_TAG,
            infinity,
            false,
            present
        ),
    {ok, {value, <<"V4">>}, _SQN4} =
        book_get_sqn(Bookie1, <<"B">>, <<"K1">>),
    Past = leveled_util:integer_now() - 300,
    ok =
        book_put(
            Bookie1,
            <<"B">>,
            <<"KTTL">>,
            {value, expired},
            [{add, <<"ttl_bin">>, <<"OLD">>}],
            ?STD_TAG,
            Past,
            false
        ),
    not_found = book_get_sqn(Bookie1, <<"B">>, <<"KTTL">>),
    ok =
        book_casput(
            Bookie1,
            <<"B">>,
            <<"KTTL">>,
            {value, <<"LIVE">>},
            [{add, <<"ttl_bin">>, <<"NEW">>}],
            ?STD_TAG,
            infinity,
            false,
            absent
        ),
    {ok, {value, <<"LIVE">>}, _TTLRefreshSQN} =
        book_get_sqn(Bookie1, <<"B">>, <<"KTTL">>),
    {async, OldTTLFolder} =
        book_indexfold(
            Bookie1,
            <<"B">>,
            {fun(_B, {IdxV, K}, Acc) -> [{IdxV, K} | Acc] end, []},
            {<<"ttl_bin">>, <<"OLD">>, <<"OLD">>},
            {true, undefined}
        ),
    ?assertMatch([], OldTTLFolder()),
    {async, NewTTLFolder} =
        book_indexfold(
            Bookie1,
            <<"B">>,
            {fun(_B, {IdxV, K}, Acc) -> [{IdxV, K} | Acc] end, []},
            {<<"ttl_bin">>, <<"NEW">>, <<"NEW">>},
            {true, undefined}
        ),
    ?assertMatch([{<<"NEW">>, <<"KTTL">>}], NewTTLFolder()),
    _ =
        sys:replace_state(
            Bookie1,
            fun(State) -> State#state{slow_offer = true} end
        ),
    pause =
        book_casput(
            Bookie1,
            <<"B">>,
            <<"KPAUSE">>,
            {value, <<"ACCEPTED">>},
            [],
            ?STD_TAG,
            infinity,
            false,
            absent
        ),
    {ok, {value, <<"ACCEPTED">>}, _PauseSQN} =
        book_get_sqn(Bookie1, <<"B">>, <<"KPAUSE">>),
    ok = book_destroy(Bookie1).

casbatchput_atomic_preconditions_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} =
        book_start([
            {root_path, RootPath},
            {max_journalsize, 1000000},
            {cache_size, 500},
            {compression_method, none}
        ]),
    ok =
        book_batchput(Bookie1, [
            {put, <<"B">>, <<"K1">>, {value, <<"V1">>}, [
                {add, <<"idx_bin">>, <<"OLD1">>}
            ], ?STD_TAG, infinity},
            {put, <<"B">>, <<"K2">>, {value, <<"V2">>}, [
                {add, <<"idx_bin">>, <<"OLD2">>}
            ], ?STD_TAG, infinity}
        ]),
    {ok, {value, <<"V1">>}, SQN1} =
        book_get_sqn(Bookie1, <<"B">>, <<"K1">>),
    {ok, {value, <<"V2">>}, SQN2} =
        book_get_sqn(Bookie1, <<"B">>, <<"K2">>),
    ?assertMatch(
        {error, {precondition_failed, [_]}},
        book_casbatchput(
            Bookie1,
            [
                {put, <<"B">>, <<"K1">>, {value, <<"LEAK1">>}, [
                    {remove, <<"idx_bin">>, <<"OLD1">>},
                    {add, <<"idx_bin">>, <<"LEAK">>}
                ], ?STD_TAG, infinity},
                {put, <<"B">>, <<"K2">>, {value, <<"LEAK2">>}, [
                    {remove, <<"idx_bin">>, <<"OLD2">>},
                    {add, <<"idx_bin">>, <<"LEAK">>}
                ], ?STD_TAG, infinity}
            ],
            [
                {<<"B">>, <<"K1">>, ?STD_TAG, {sqn, SQN1 + 1}},
                {<<"B">>, <<"K2">>, ?STD_TAG, {sqn, SQN2}}
            ]
        )
    ),
    {ok, {value, <<"V1">>}, SQN1} =
        book_get_sqn(Bookie1, <<"B">>, <<"K1">>),
    {ok, {value, <<"V2">>}, SQN2} =
        book_get_sqn(Bookie1, <<"B">>, <<"K2">>),
    {async, LeakFolder} =
        book_indexfold(
            Bookie1,
            <<"B">>,
            {fun(_B, {IdxV, K}, Acc) -> [{IdxV, K} | Acc] end, []},
            {<<"idx_bin">>, <<"LEAK">>, <<"LEAK">>},
            {true, undefined}
        ),
    ?assertMatch([], LeakFolder()),
    ok =
        book_casbatchput(
            Bookie1,
            [
                {put, <<"B">>, <<"K1">>, {value, <<"V1B">>}, [
                    {remove, <<"idx_bin">>, <<"OLD1">>},
                    {add, <<"idx_bin">>, <<"NEW">>}
                ], ?STD_TAG, infinity},
                {delete, <<"B">>, <<"K2">>, [
                    {remove, <<"idx_bin">>, <<"OLD2">>}
                ], ?STD_TAG, infinity}
            ],
            [
                {<<"B">>, <<"K1">>, ?STD_TAG, {sqn, SQN1}},
                {<<"B">>, <<"K2">>, ?STD_TAG, {sqn, SQN2}}
            ],
            false
        ),
    {ok, {value, <<"V1B">>}, BatchSQN} =
        book_get_sqn(Bookie1, <<"B">>, <<"K1">>),
    {ok, _Head, BatchSQN} =
        book_head_sqn(Bookie1, <<"B">>, <<"K1">>),
    not_found = book_get_sqn(Bookie1, <<"B">>, <<"K2">>),
    not_found = book_head_sqn(Bookie1, <<"B">>, <<"K2">>),
    {ok, BatchSQN} = book_sqn(Bookie1, <<"B">>, <<"K1">>),
    {async, NewFolder} =
        book_indexfold(
            Bookie1,
            <<"B">>,
            {fun(_B, {IdxV, K}, Acc) -> [{IdxV, K} | Acc] end, []},
            {<<"idx_bin">>, <<"NEW">>, <<"NEW">>},
            {true, undefined}
        ),
    ?assertMatch([{<<"NEW">>, <<"K1">>}], NewFolder()),
    {async, Old2Folder} =
        book_indexfold(
            Bookie1,
            <<"B">>,
            {fun(_B, {IdxV, K}, Acc) -> [{IdxV, K} | Acc] end, []},
            {<<"idx_bin">>, <<"OLD2">>, <<"OLD2">>},
            {true, undefined}
        ),
    ?assertMatch([], Old2Folder()),
    ok = book_destroy(Bookie1).

casbatchput_validation_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} =
        book_start([
            {root_path, RootPath},
            {max_journalsize, 1000000},
            {cache_size, 500},
            {compression_method, none}
        ]),
    PutSpec = {put, <<"B">>, <<"K1">>, {value, <<"V1">>}, [], ?STD_TAG, infinity},
    DuplicatePutSpec = {put, <<"B">>, <<"K1">>, {value, <<"V2">>}, [], ?STD_TAG, infinity},
    ?assertMatch(
        {error, empty_cas_conditions},
        book_casbatchput(Bookie1, [PutSpec], [])
    ),
    ?assertMatch(
        {error, {duplicate_key, _}},
        book_casbatchput(Bookie1, [PutSpec, DuplicatePutSpec], [
            {<<"B">>, <<"K1">>, ?STD_TAG, absent}
        ])
    ),
    ?assertMatch(
        {error, {precondition_failed, [
            {precondition_failed, _, {expected, present}, {actual, duplicate_precondition}}
        ]}},
        book_casbatchput(Bookie1, [PutSpec], [
            {<<"B">>, <<"K1">>, ?STD_TAG, absent},
            {<<"B">>, <<"K1">>, ?STD_TAG, present}
        ])
    ),
    ?assertMatch(
        {error, invalid_cas_condition},
        book_casbatchput(Bookie1, [PutSpec], [
            {<<"B">>, <<"K1">>, ?STD_TAG, invalid}
        ])
    ),
    ?assertMatch(
        {error, invalid_cas_condition},
        book_casbatchput(Bookie1, [PutSpec], invalid)
    ),
    ?assertMatch(
        {error, invalid_cas_condition},
        book_casbatchput(Bookie1, [PutSpec], [invalid])
    ),
    ?assertMatch(
        {error, {precondition_failed, [
            {precondition_failed, _, {expected, absent}, {actual, {invalid_tag, ?HEAD_TAG}}}
        ]}},
        book_casbatchput(Bookie1, [PutSpec], [
            {<<"B">>, <<"K1">>, ?HEAD_TAG, absent}
        ])
    ),
    ?assertMatch(
        {error, {precondition_failed, [
            {precondition_failed, _, {expected, present}, {actual, absent}}
        ]}},
        book_casbatchput(Bookie1, [PutSpec], [
            {<<"B">>, <<"KMISSING">>, ?STD_TAG, present}
        ])
    ),
    ok = book_delete(Bookie1, <<"B">>, <<"KTOMB">>, []),
    ?assertMatch(
        {error, {precondition_failed, [
            {precondition_failed, _, {expected, present}, {actual, tombstone}}
        ]}},
        book_casbatchput(Bookie1, [PutSpec], [
            {<<"B">>, <<"KTOMB">>, ?STD_TAG, present}
        ])
    ),
    Past = leveled_util:integer_now() - 300,
    ok =
        book_put(
            Bookie1,
            <<"B">>,
            <<"KEXPIRED">>,
            {value, expired},
            [],
            ?STD_TAG,
            Past,
            false
        ),
    ?assertMatch(
        {error, {precondition_failed, [
            {precondition_failed, _, {expected, present}, {actual, expired}}
        ]}},
        book_casbatchput(Bookie1, [PutSpec], [
            {<<"B">>, <<"KEXPIRED">>, ?STD_TAG, present}
        ])
    ),
    not_found = book_get(Bookie1, <<"B">>, <<"K1">>),
    ok = book_destroy(Bookie1).

casput_bookput_interleaving_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} =
        book_start([
            {root_path, RootPath},
            {max_journalsize, 1000000},
            {cache_size, 500},
            {compression_method, none}
        ]),
    ok =
        book_put(
            Bookie1,
            <<"B">>,
            <<"K1">>,
            {counter, 0},
            [{add, <<"race_bin">>, <<"OLD">>}],
            ?STD_TAG,
            infinity,
            false
        ),
    {ok, {counter, 0}, SQN1} = book_get_sqn(Bookie1, <<"B">>, <<"K1">>),
    ok =
        book_put(
            Bookie1,
            <<"B">>,
            <<"K1">>,
            {counter, 1},
            [
                {remove, <<"race_bin">>, <<"OLD">>},
                {add, <<"race_bin">>, <<"PUT_WON">>}
            ],
            ?STD_TAG,
            infinity,
            false
        ),
    {ok, {counter, 1}, SQN2} = book_get_sqn(Bookie1, <<"B">>, <<"K1">>),
    true = SQN2 > SQN1,
    ?assertMatch(
        {error, {precondition_failed, [
            {precondition_failed, _, {expected, {sqn, SQN1}}, {actual, {active, SQN2}}}
        ]}},
        book_casput(
            Bookie1,
            <<"B">>,
            <<"K1">>,
            {counter, 2},
            [
                {remove, <<"race_bin">>, <<"PUT_WON">>},
                {add, <<"race_bin">>, <<"CAS_LEAK">>}
            ],
            ?STD_TAG,
            infinity,
            false,
            {sqn, SQN1}
        )
    ),
    {ok, {counter, 1}, SQN2} = book_get_sqn(Bookie1, <<"B">>, <<"K1">>),
    {async, LeakFolder} =
        book_indexfold(
            Bookie1,
            <<"B">>,
            {fun(_B, {IdxV, K}, Acc) -> [{IdxV, K} | Acc] end, []},
            {<<"race_bin">>, <<"CAS_LEAK">>, <<"CAS_LEAK">>},
            {true, undefined}
        ),
    ?assertMatch([], LeakFolder()),
    ok = book_destroy(Bookie1).

casput_concurrent_single_winner_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} =
        book_start([
            {root_path, RootPath},
            {max_journalsize, 1000000},
            {cache_size, 500},
            {compression_method, none}
        ]),
    ok =
        book_put(
            Bookie1,
            <<"B">>,
            <<"K1">>,
            {counter, 0},
            [],
            ?STD_TAG,
            infinity,
            false
        ),
    {ok, {counter, 0}, SQN1} = book_get_sqn(Bookie1, <<"B">>, <<"K1">>),
    Parent = self(),
    Workers =
        [
            spawn(fun() ->
                Result =
                    book_casput(
                        Bookie1,
                        <<"B">>,
                        <<"K1">>,
                        {counter, N},
                        [],
                        ?STD_TAG,
                        infinity,
                        false,
                        {sqn, SQN1}
                    ),
                Parent ! {self(), Result}
            end)
         || N <- lists:seq(1, 20)
        ],
    Results =
        [
            receive
                {Pid, Result} when Pid == Worker ->
                    Result
            after 5000 ->
                timeout
            end
         || Worker <- Workers
        ],
    1 = length([Result || Result <- Results, Result == ok]),
    19 =
        length([
            Result
         || Result <- Results,
            case Result of
                {error, {precondition_failed, [_ | _]}} -> true;
                _ -> false
            end
        ]),
    {ok, {counter, Winner}, SQN2} = book_get_sqn(Bookie1, <<"B">>, <<"K1">>),
    true = Winner >= 1,
    true = Winner =< 20,
    true = SQN2 > SQN1,
    ok = book_destroy(Bookie1).

casbatchput_concurrent_overlapping_keys_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} =
        book_start([
            {root_path, RootPath},
            {max_journalsize, 1000000},
            {cache_size, 500},
            {compression_method, none}
        ]),
    ok =
        book_batchput(Bookie1, [
            {put, <<"B">>, <<"K1">>, {counter, 0}, [], ?STD_TAG, infinity},
            {put, <<"B">>, <<"K2">>, {counter, 0}, [], ?STD_TAG, infinity}
        ]),
    {ok, {counter, 0}, SQN1} = book_get_sqn(Bookie1, <<"B">>, <<"K1">>),
    {ok, {counter, 0}, SQN2} = book_get_sqn(Bookie1, <<"B">>, <<"K2">>),
    Parent = self(),
    Workers =
        [
            spawn(fun() ->
                Result =
                    book_casbatchput(
                        Bookie1,
                        [
                            {put, <<"B">>, <<"K1">>, {counter, N}, [], ?STD_TAG,
                                infinity},
                            {put, <<"B">>, <<"K2">>, {counter, N}, [], ?STD_TAG,
                                infinity}
                        ],
                        [
                            {<<"B">>, <<"K1">>, ?STD_TAG, {sqn, SQN1}},
                            {<<"B">>, <<"K2">>, ?STD_TAG, {sqn, SQN2}}
                        ]
                    ),
                Parent ! {self(), Result}
            end)
         || N <- lists:seq(1, 20)
        ],
    Results =
        [
            receive
                {Pid, Result} when Pid == Worker ->
                    Result
            after 5000 ->
                timeout
            end
         || Worker <- Workers
        ],
    1 = length([Result || Result <- Results, Result == ok]),
    19 =
        length([
            Result
         || Result <- Results,
            case Result of
                {error, {precondition_failed, [_ | _]}} -> true;
                _ -> false
            end
        ]),
    {ok, {counter, Winner}, BatchSQN} =
        book_get_sqn(Bookie1, <<"B">>, <<"K1">>),
    {ok, {counter, Winner}, BatchSQN} =
        book_get_sqn(Bookie1, <<"B">>, <<"K2">>),
    ok = book_destroy(Bookie1).

casbatchput_crash_recovery_test() ->
    RootPath = reset_filestructure(),
    Opts = [
        {root_path, RootPath},
        {max_journalsize, 1000000},
        {cache_size, 500},
        {sync_strategy, riak_sync},
        {compression_method, none}
    ],
    {ok, Bookie1} = book_plainstart(Opts),
    ok =
        book_casbatchput(
            Bookie1,
            [
                {put, <<"B">>, <<"K1">>, {value, <<"V1">>}, [
                    {add, <<"idx_bin">>, <<"CRASH">>}
                ], ?STD_TAG, infinity},
                {put, <<"B">>, <<"K2">>, {value, <<"V2">>}, [
                    {add, <<"idx_bin">>, <<"CRASH">>}
                ], ?STD_TAG, infinity}
            ],
            [
                {<<"B">>, <<"K1">>, ?STD_TAG, absent},
                {<<"B">>, <<"K2">>, ?STD_TAG, absent}
            ],
            true
        ),
    {ok, {value, <<"V1">>}, BatchSQN} =
        book_get_sqn(Bookie1, <<"B">>, <<"K1">>),
    {ok, {value, <<"V2">>}, BatchSQN} =
        book_get_sqn(Bookie1, <<"B">>, <<"K2">>),
    ok =
        book_casput(
            Bookie1,
            <<"B">>,
            <<"KDEL">>,
            {value, <<"DELETE_ME">>},
            [{add, <<"idx_bin">>, <<"DELETE_ME">>}],
            ?STD_TAG,
            infinity,
            true,
            absent
        ),
    {ok, {value, <<"DELETE_ME">>}, DeleteSQN} =
        book_get_sqn(Bookie1, <<"B">>, <<"KDEL">>),
    ok =
        book_casbatchput(
            Bookie1,
            [
                {delete, <<"B">>, <<"KDEL">>, [
                    {remove, <<"idx_bin">>, <<"DELETE_ME">>}
                ], ?STD_TAG, infinity}
            ],
            [{<<"B">>, <<"KDEL">>, ?STD_TAG, {sqn, DeleteSQN}}],
            true
        ),
    Future = leveled_util:integer_now() + 300,
    Past = leveled_util:integer_now() - 300,
    ok =
        book_casput(
            Bookie1,
            <<"B">>,
            <<"KTTL_LIVE">>,
            {value, live},
            [{add, <<"ttl_bin">>, <<"LIVE">>}],
            ?STD_TAG,
            Future,
            true,
            absent
        ),
    ok =
        book_casput(
            Bookie1,
            <<"B">>,
            <<"KTTL_EXPIRED">>,
            {value, expired},
            [{add, <<"ttl_bin">>, <<"EXPIRED">>}],
            ?STD_TAG,
            Past,
            true,
            absent
        ),
    {ok, Inker, Penciller} = book_returnactors(Bookie1),
    BookieRef = erlang:monitor(process, Bookie1),
    InkerRef = erlang:monitor(process, Inker),
    PencillerRef = erlang:monitor(process, Penciller),
    exit(Bookie1, kill),
    wait_down(BookieRef, Bookie1, bookie),
    wait_down(InkerRef, Inker, inker),
    wait_down(PencillerRef, Penciller, penciller),

    {ok, Bookie2} = book_start(Opts),
    {ok, {value, <<"V1">>}, BatchSQN} =
        book_get_sqn(Bookie2, <<"B">>, <<"K1">>),
    {ok, _Head1, BatchSQN} =
        book_head_sqn(Bookie2, <<"B">>, <<"K1">>),
    {ok, {value, <<"V2">>}, BatchSQN} =
        book_get_sqn(Bookie2, <<"B">>, <<"K2">>),
    {ok, _Head2, BatchSQN} =
        book_head_sqn(Bookie2, <<"B">>, <<"K2">>),
    not_found = book_get_sqn(Bookie2, <<"B">>, <<"KDEL">>),
    not_found = book_head_sqn(Bookie2, <<"B">>, <<"KDEL">>),
    {ok, {value, live}, LiveSQN} =
        book_get_sqn(Bookie2, <<"B">>, <<"KTTL_LIVE">>),
    {ok, _LiveHead, LiveSQN} =
        book_head_sqn(Bookie2, <<"B">>, <<"KTTL_LIVE">>),
    not_found = book_get_sqn(Bookie2, <<"B">>, <<"KTTL_EXPIRED">>),
    ?assertEqual(
        [{<<"CRASH">>, <<"K1">>}, {<<"CRASH">>, <<"K2">>}],
        indexfold_matches(Bookie2, <<"idx_bin">>, <<"CRASH">>)
    ),
    ?assertEqual([], indexfold_matches(Bookie2, <<"idx_bin">>, <<"DELETE_ME">>)),
    ?assertEqual(
        [{<<"LIVE">>, <<"KTTL_LIVE">>}],
        indexfold_matches(Bookie2, <<"ttl_bin">>, <<"LIVE">>)
    ),
    ?assertEqual([], indexfold_matches(Bookie2, <<"ttl_bin">>, <<"EXPIRED">>)),
    ok =
        book_casput(
            Bookie2,
            <<"B">>,
            <<"KNEXT">>,
            {value, next},
            [],
            ?STD_TAG,
            infinity,
            true,
            absent
        ),
    {ok, {value, next}, NextSQN} =
        book_get_sqn(Bookie2, <<"B">>, <<"KNEXT">>),
    true = NextSQN > LiveSQN,
    true = LiveSQN > BatchSQN,
    ok = book_destroy(Bookie2).

casbatchput_partial_tail_test() ->
    RootPath = reset_filestructure(),
    Opts = [
        {root_path, RootPath},
        {max_journalsize, 1000000},
        {cache_size, 500},
        {sync_strategy, riak_sync},
        {compression_method, none}
    ],
    {ok, Bookie1} = book_plainstart(Opts),
    ok =
        book_casbatchput(
            Bookie1,
            [
                {put, <<"B">>, <<"K1">>, {value, <<"V1">>}, [
                    {add, <<"idx_bin">>, <<"CASTAIL">>}
                ], ?STD_TAG, infinity},
                {put, <<"B">>, <<"K2">>, {value, <<"V2">>}, [
                    {add, <<"idx_bin">>, <<"CASTAIL">>}
                ], ?STD_TAG, infinity}
            ],
            [
                {<<"B">>, <<"K1">>, ?STD_TAG, absent},
                {<<"B">>, <<"K2">>, ?STD_TAG, absent}
            ],
            true
        ),
    {ok, Inker, Penciller} = book_returnactors(Bookie1),
    [ActiveJournal | _] = leveled_inker:ink_getcdbpids(Inker),
    JournalFile = leveled_cdb:cdb_filename(ActiveJournal),
    BookieRef = erlang:monitor(process, Bookie1),
    InkerRef = erlang:monitor(process, Inker),
    PencillerRef = erlang:monitor(process, Penciller),
    exit(Bookie1, kill),
    wait_down(BookieRef, Bookie1, bookie),
    wait_down(InkerRef, Inker, inker),
    wait_down(PencillerRef, Penciller, penciller),
    truncate_after_first_cdb_record(JournalFile),
    {ok, Bookie2} = book_start(Opts),
    not_found = book_get(Bookie2, <<"B">>, <<"K1">>),
    not_found = book_get(Bookie2, <<"B">>, <<"K2">>),
    ?assertEqual([], indexfold_matches(Bookie2, <<"idx_bin">>, <<"CASTAIL">>)),
    ok = book_destroy(Bookie2).

lastcompactionresult_test() ->
    %% book_lastcompactionresult reports what the last cycle DID:
    %% {done, N>0} when a run was compacted, {done, 0} when scoring found
    %% nothing worth compacting - the signal an external
    %% compact-until-quiescent loop needs (islastcompactionpending only
    %% says whether a cycle is in flight, which is timing-dependent).
    RootPath = reset_filestructure(),
    Opts = cas_compaction_opts(RootPath, [{?STD_TAG, retain}]),
    {ok, Bookie1} = book_start(Opts),
    ?assertEqual({done, undefined}, book_lastcompactionresult(Bookie1)),
    seed_cas_compaction_state(Bookie1),
    {ok, Inker, _Penciller} = book_returnactors(Bookie1),
    ok = leveled_inker:ink_roll(Inker),
    %% compact-until-quiescent: trigger cycles until the last completed
    %% cycle reports it compacted nothing. This loop is exactly what the
    %% signal exists for and MUST terminate deterministically.
    RunLengths = compact_until_quiescent(Bookie1, 10, []),
    %% the loop observed a definitive terminal {done, 0}
    ?assertEqual(0, hd(RunLengths)),
    %% every completed cycle reported an integer outcome
    ?assert(lists:all(fun is_integer/1, RunLengths)),
    %% quiescence is stable: another cycle still reports {done, 0}
    ok = book_compactjournal(Bookie1, 30000),
    wait_for_batch_compaction(Bookie1),
    ?assertEqual({done, 0}, book_lastcompactionresult(Bookie1)),
    ok = book_destroy(Bookie1).

compact_until_quiescent(_Bookie, 0, _Acc) ->
    error(compaction_never_quiescent);
compact_until_quiescent(Bookie, Remaining, Acc) ->
    ok = book_compactjournal(Bookie, 30000),
    wait_for_batch_compaction(Bookie),
    case book_lastcompactionresult(Bookie) of
        {done, 0} ->
            [0 | Acc];
        {done, N} when is_integer(N), N > 0 ->
            compact_until_quiescent(Bookie, Remaining - 1, [N | Acc])
    end.

casbatchput_compaction_retain_test() ->
    RootPath = reset_filestructure(),
    Opts = cas_compaction_opts(RootPath, [{?STD_TAG, retain}]),
    {ok, Bookie1} = book_start(Opts),
    seed_cas_compaction_state(Bookie1),
    {ok, Inker, _Penciller} = book_returnactors(Bookie1),
    ok = leveled_inker:ink_roll(Inker),
    ok = book_compactjournal(Bookie1, 30000),
    wait_for_batch_compaction(Bookie1),
    assert_cas_compacted_state(Bookie1),
    ok = book_close(Bookie1),
    leveled_penciller:clean_testdir(RootPath ++ "/" ++ ?LEDGER_FP),
    {ok, Bookie2} = book_start(Opts),
    assert_cas_compacted_state(Bookie2),
    ok = book_destroy(Bookie2).

casbatchput_hot_backup_after_compaction_test() ->
    RootPath = reset_filestructure(),
    BackupPath = reset_filestructure("test/test_cas_backup"),
    Opts = cas_compaction_opts(RootPath, [{?STD_TAG, retain}]),
    {ok, Bookie1} = book_start(Opts),
    seed_cas_compaction_state(Bookie1),
    {ok, Inker, _Penciller} = book_returnactors(Bookie1),
    ok = leveled_inker:ink_roll(Inker),
    ok = book_compactjournal(Bookie1, 30000),
    wait_for_batch_compaction(Bookie1),
    assert_cas_compacted_state(Bookie1),

    {async, BackupFun} = book_hotbackup(Bookie1),
    ok = BackupFun(BackupPath),
    ok = book_destroy(Bookie1),

    {ok, Bookie2} =
        book_start([{root_path, BackupPath} | proplists:delete(root_path, Opts)]),
    assert_cas_compacted_state(Bookie2),
    ok = book_destroy(Bookie2).

casbatchput_recalc_compaction_test() ->
    RootPath = reset_filestructure(),
    Tag = cas_recalc_tag,
    ExtractMDFun =
        fun(cas_recalc_tag, Size, delete) ->
            {{erlang:phash2(delete), Size, {index, []}}, []};
        (cas_recalc_tag, Size, Obj) ->
            [{index, Indexes}, {value, _Value}] = Obj,
            {{erlang:phash2(term_to_binary(Obj)), Size, {index, Indexes}}, []}
        end,
    CalcIndexFun =
        fun(cas_recalc_tag, UpdMeta, PrvMeta) ->
            {index, UpdIndexes} = element(3, UpdMeta),
            PrvIndexes =
                case PrvMeta of
                    not_present ->
                        [];
                    PrvMeta when is_tuple(PrvMeta) ->
                        {index, Indexes} = element(3, PrvMeta),
                        Indexes
                end,
            AddSpecs =
                lists:map(
                    fun(I) -> {add, <<"temp_int">>, I} end,
                    lists:subtract(UpdIndexes, PrvIndexes)
                ),
            RemoveSpecs =
                lists:map(
                    fun(I) -> {remove, <<"temp_int">>, I} end,
                    lists:subtract(PrvIndexes, UpdIndexes)
                ),
            AddSpecs ++ RemoveSpecs
        end,
    Opts =
        cas_compaction_opts(RootPath, [{Tag, recalc}]) ++
            [{override_functions, [
                {extract_metadata, ExtractMDFun},
                {diff_indexspecs, CalcIndexFun}
            ]}],
    {ok, Bookie1} = book_start(Opts),
    ok =
        book_casbatchput(
            Bookie1,
            [
                {put, <<"B">>, <<"K1">>, [{index, [1]}, {value, <<"V1">>}], [
                    {add, <<"temp_int">>, 1}
                ], Tag, infinity},
                {put, <<"B">>, <<"K2">>, [{index, [2]}, {value, <<"V2">>}], [
                    {add, <<"temp_int">>, 2}
                ], Tag, infinity},
                {put, <<"B">>, <<"K3">>, [{index, [3]}, {value, <<"V3">>}], [
                    {add, <<"temp_int">>, 3}
                ], Tag, infinity}
            ],
            [
                {<<"B">>, <<"K1">>, Tag, absent},
                {<<"B">>, <<"K2">>, Tag, absent},
                {<<"B">>, <<"K3">>, Tag, absent}
            ],
            true
        ),
    {ok, _V1, SQN1} = book_get_sqn(Bookie1, <<"B">>, <<"K1">>, Tag),
    {ok, _V2, SQN2} = book_get_sqn(Bookie1, <<"B">>, <<"K2">>, Tag),
    {ok, _V3, SQN3} = book_get_sqn(Bookie1, <<"B">>, <<"K3">>, Tag),
    ?assertMatch(
        {error, {precondition_failed, [_]}},
        book_casput(
            Bookie1,
            <<"B">>,
            <<"K3">>,
            [{index, [99]}, {value, <<"STALE">>}],
            [
                {remove, <<"temp_int">>, 3},
                {add, <<"temp_int">>, 99}
            ],
            Tag,
            infinity,
            true,
            {sqn, SQN3 + 1000}
        )
    ),
    ok =
        book_casbatchput(
            Bookie1,
            [
                {put, <<"B">>, <<"K1">>, [{index, [4]}, {value, <<"V1B">>}], [
                    {remove, <<"temp_int">>, 1},
                    {add, <<"temp_int">>, 4}
                ], Tag, infinity},
                {delete, <<"B">>, <<"K2">>, [
                    {remove, <<"temp_int">>, 2}
                ], Tag, infinity}
            ],
            [
                {<<"B">>, <<"K1">>, Tag, {sqn, SQN1}},
                {<<"B">>, <<"K2">>, Tag, {sqn, SQN2}}
            ],
            true
        ),
    {ok, Inker, _Penciller} = book_returnactors(Bookie1),
    ok = leveled_inker:ink_roll(Inker),
    ok = book_compactjournal(Bookie1, 30000),
    wait_for_batch_compaction(Bookie1),
    assert_cas_recalc_state(Bookie1, Tag),
    ok = book_close(Bookie1),
    leveled_penciller:clean_testdir(RootPath ++ "/" ++ ?LEDGER_FP),
    {ok, Bookie2} = book_start(Opts),
    assert_cas_recalc_state(Bookie2, Tag),
    ok = book_destroy(Bookie2),
    application:unset_env(leveled, extract_metadata),
    application:unset_env(leveled, diff_indexspecs).

batchput_indexfold_atomic_visibility_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} =
        book_start([
            {root_path, RootPath},
            {max_journalsize, 1000000},
            {cache_size, 500},
            {compression_method, none}
        ]),
    ok =
        book_put(
            Bookie1,
            <<"B">>,
            <<"K0">>,
            {value, <<"V0">>},
            [{add, <<"city_bin">>, <<"OLD">>}],
            ?STD_TAG
        ),
    ?assertEqual([], indexfold_matches(Bookie1, <<"city_bin">>, <<"BATCH">>)),
    ?assertEqual(
        [{<<"OLD">>, <<"K0">>}],
        indexfold_matches(Bookie1, <<"city_bin">>, <<"OLD">>)
    ),
    ok =
        book_batchput(Bookie1, [
            {put, <<"B">>, <<"K1">>, {value, <<"V1">>}, [
                {add, <<"city_bin">>, <<"BATCH">>}
            ], ?STD_TAG, infinity},
            {put, <<"B">>, <<"K2">>, {value, <<"V2">>}, [
                {add, <<"city_bin">>, <<"BATCH">>}
            ], ?STD_TAG, infinity},
            {put, <<"B">>, <<"K0">>, {value, <<"V0B">>}, [
                {remove, <<"city_bin">>, <<"OLD">>},
                {add, <<"city_bin">>, <<"NEW">>}
            ], ?STD_TAG, infinity}
        ]),
    {ok, BatchSQN} = book_sqn(Bookie1, <<"B">>, <<"K1">>),
    {ok, BatchSQN} = book_sqn(Bookie1, <<"B">>, <<"K2">>),
    {ok, BatchSQN} = book_sqn(Bookie1, <<"B">>, <<"K0">>),
    ?assertEqual(
        [{<<"BATCH">>, <<"K1">>}, {<<"BATCH">>, <<"K2">>}],
        indexfold_matches(Bookie1, <<"city_bin">>, <<"BATCH">>)
    ),
    ?assertEqual([], indexfold_matches(Bookie1, <<"city_bin">>, <<"OLD">>)),
    ?assertEqual(
        [{<<"NEW">>, <<"K0">>}],
        indexfold_matches(Bookie1, <<"city_bin">>, <<"NEW">>)
    ),
    ok = book_destroy(Bookie1).

batchput_roll_retry_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} =
        book_start([
            {root_path, RootPath},
            {max_journalsize, 1000000},
            {max_journalobjectcount, 5},
            {cache_size, 500},
            {compression_method, none}
        ]),
    lists:foreach(
        fun(N) ->
            Key = integer_to_binary(N),
            ok = book_put(Bookie1, <<"B">>, Key, {seed, N}, [], ?STD_TAG)
        end,
        lists:seq(1, 4)
    ),
    {ok, Inker, _Penciller} = book_returnactors(Bookie1),
    BeforeManifestCount = length(gen_server:call(Inker, get_manifest)),
    ok =
        book_batchput(Bookie1, [
            {put, <<"B">>, <<"K5">>, {value, <<"V5">>}, [], ?STD_TAG, infinity}
        ]),
    AfterManifestCount = length(gen_server:call(Inker, get_manifest)),
    ?assert(AfterManifestCount > BeforeManifestCount),
    {ok, {value, <<"V5">>}} = book_get(Bookie1, <<"B">>, <<"K5">>),
    ok = book_destroy(Bookie1).

batchput_too_large_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} =
        book_start([
            {root_path, RootPath},
            {max_journalsize, 5000},
            {cache_size, 500},
            {compression_method, none}
        ]),
    BigObject = binary:copy(<<"x">>, 20000),
    ?assertMatch(
        {error, batch_too_large},
        book_batchput(Bookie1, [
            {put, <<"B">>, <<"K1">>, BigObject, [], ?STD_TAG, infinity}
        ])
    ),
    not_found = book_get(Bookie1, <<"B">>, <<"K1">>),
    not_found = book_head(Bookie1, <<"B">>, <<"K1">>),
    ?assertMatch(true, book_isempty(Bookie1, ?STD_TAG)),
    ok = book_destroy(Bookie1).

batchput_crash_recovery_test() ->
    RootPath = reset_filestructure(),
    Opts = [
        {root_path, RootPath},
        {max_journalsize, 1000000},
        {cache_size, 500},
        {sync_strategy, riak_sync},
        {compression_method, none}
    ],
    {ok, Bookie1} = book_plainstart(Opts),
    ok =
        book_batchput(Bookie1, [
            {put, <<"B">>, <<"K1">>, {value, <<"V1">>}, [
                {add, <<"city_bin">>, <<"CRASH">>}
            ], ?STD_TAG, infinity},
            {put, <<"B">>, <<"K2">>, {value, <<"V2">>}, [
                {add, <<"city_bin">>, <<"CRASH">>}
            ], ?STD_TAG, infinity}
        ]),
    {ok, SQN1} = book_sqn(Bookie1, <<"B">>, <<"K1">>),
    {ok, SQN1} = book_sqn(Bookie1, <<"B">>, <<"K2">>),
    {ok, Inker, Penciller} = book_returnactors(Bookie1),
    BookieRef = erlang:monitor(process, Bookie1),
    InkerRef = erlang:monitor(process, Inker),
    PencillerRef = erlang:monitor(process, Penciller),
    exit(Bookie1, kill),
    wait_down(BookieRef, Bookie1, bookie),
    wait_down(InkerRef, Inker, inker),
    wait_down(PencillerRef, Penciller, penciller),
    {ok, Bookie2} = book_start(Opts),
    {ok, {value, <<"V1">>}} = book_get(Bookie2, <<"B">>, <<"K1">>),
    {ok, {value, <<"V2">>}} = book_get(Bookie2, <<"B">>, <<"K2">>),
    ?assertEqual(
        [{<<"CRASH">>, <<"K1">>}, {<<"CRASH">>, <<"K2">>}],
        indexfold_matches(Bookie2, <<"city_bin">>, <<"CRASH">>)
    ),
    ok = book_destroy(Bookie2).

batchput_hot_backup_test() ->
    RootPath = reset_filestructure(),
    BackupPath = reset_filestructure("test/test_batchput_backup"),
    Opts = [
        {root_path, RootPath},
        {max_journalsize, 1000000},
        {cache_size, 500},
        {sync_strategy, riak_sync},
        {compression_method, none},
        {reload_strategy, [{?STD_TAG, retain}]}
    ],
    {ok, Bookie1} = book_start(Opts),
    ok =
        book_batchput(Bookie1, [
            {put, <<"B">>, <<"K1">>, {value, <<"V1">>}, [
                {add, <<"idx_bin">>, <<"BACKUP">>}
            ], ?STD_TAG, infinity},
            {put, <<"B">>, <<"K2">>, {value, <<"V2">>}, [
                {add, <<"idx_bin">>, <<"BACKUP">>}
            ], ?STD_TAG, infinity}
        ]),
    {async, BackupFun} = book_hotbackup(Bookie1),
    ok = BackupFun(BackupPath),
    ok = book_destroy(Bookie1),

    {ok, Bookie2} =
        book_start([{root_path, BackupPath} | proplists:delete(root_path, Opts)]),
    {ok, {value, <<"V1">>}} = book_get(Bookie2, <<"B">>, <<"K1">>),
    {ok, {value, <<"V2">>}} = book_get(Bookie2, <<"B">>, <<"K2">>),
    ?assertEqual(
        [{<<"BACKUP">>, <<"K1">>}, {<<"BACKUP">>, <<"K2">>}],
        indexfold_matches(Bookie2, <<"idx_bin">>, <<"BACKUP">>)
    ),
    ok = book_destroy(Bookie2).

batchput_loading_boundary_test() ->
    batchput_loading_boundary_case(199, ?LOADING_BATCH),
    batchput_loading_boundary_case(200, ?LOADING_BATCH + 1).

batchput_loading_boundary_case(SeedCount, ExpectedSQN) ->
    RootPath = reset_filestructure(),
    Opts = [
        {root_path, RootPath},
        {max_journalsize, 1000000},
        {cache_size, 500},
        {sync_strategy, riak_sync},
        {compression_method, none}
    ],
    {ok, Bookie1} = book_plainstart(Opts),
    lists:foreach(
        fun(N) ->
            Key = integer_to_binary(N),
            ok = book_put(Bookie1, <<"B">>, Key, {seed, N}, [], ?STD_TAG)
        end,
        lists:seq(1, SeedCount)
    ),
    ok =
        book_batchput(Bookie1, [
            {put, <<"B">>, <<"KA">>, {value, <<"VA">>}, [
                {add, <<"idx_bin">>, <<"BOUNDARY">>}
            ], ?STD_TAG, infinity},
            {put, <<"B">>, <<"KB">>, {value, <<"VB">>}, [
                {add, <<"idx_bin">>, <<"BOUNDARY">>}
            ], ?STD_TAG, infinity}
        ]),
    {ok, BoundarySQN} = book_sqn(Bookie1, <<"B">>, <<"KA">>),
    ?assertEqual(ExpectedSQN, BoundarySQN),
    {ok, BoundarySQN} = book_sqn(Bookie1, <<"B">>, <<"KB">>),
    {ok, Inker, Penciller} = book_returnactors(Bookie1),
    BookieRef = erlang:monitor(process, Bookie1),
    InkerRef = erlang:monitor(process, Inker),
    PencillerRef = erlang:monitor(process, Penciller),
    exit(Bookie1, kill),
    wait_down(BookieRef, Bookie1, bookie),
    wait_down(InkerRef, Inker, inker),
    wait_down(PencillerRef, Penciller, penciller),
    {ok, Bookie2} = book_start(Opts),
    {ok, {value, <<"VA">>}} = book_get(Bookie2, <<"B">>, <<"KA">>),
    {ok, {value, <<"VB">>}} = book_get(Bookie2, <<"B">>, <<"KB">>),
    ?assertEqual(
        [{<<"BOUNDARY">>, <<"KA">>}, {<<"BOUNDARY">>, <<"KB">>}],
        indexfold_matches(Bookie2, <<"idx_bin">>, <<"BOUNDARY">>)
    ),
    ok = book_destroy(Bookie2).

batchput_partial_tail_test() ->
    RootPath = reset_filestructure(),
    Opts = [
        {root_path, RootPath},
        {max_journalsize, 1000000},
        {cache_size, 500},
        {sync_strategy, riak_sync},
        {compression_method, none}
    ],
    {ok, Bookie1} = book_plainstart(Opts),
    ok =
        book_batchput(Bookie1, [
            {put, <<"B">>, <<"K1">>, {value, <<"V1">>}, [
                {add, <<"idx_bin">>, <<"TAIL">>}
            ], ?STD_TAG, infinity},
            {put, <<"B">>, <<"K2">>, {value, <<"V2">>}, [
                {add, <<"idx_bin">>, <<"TAIL">>}
            ], ?STD_TAG, infinity}
        ]),
    {ok, Inker, Penciller} = book_returnactors(Bookie1),
    [ActiveJournal | _] = leveled_inker:ink_getcdbpids(Inker),
    JournalFile = leveled_cdb:cdb_filename(ActiveJournal),
    BookieRef = erlang:monitor(process, Bookie1),
    InkerRef = erlang:monitor(process, Inker),
    PencillerRef = erlang:monitor(process, Penciller),
    exit(Bookie1, kill),
    wait_down(BookieRef, Bookie1, bookie),
    wait_down(InkerRef, Inker, inker),
    wait_down(PencillerRef, Penciller, penciller),
    truncate_after_first_cdb_record(JournalFile),
    {ok, Bookie2} = book_start(Opts),
    not_found = book_get(Bookie2, <<"B">>, <<"K1">>),
    not_found = book_get(Bookie2, <<"B">>, <<"K2">>),
    ?assertEqual([], indexfold_matches(Bookie2, <<"idx_bin">>, <<"TAIL">>)),
    ok = book_destroy(Bookie2).

batchput_compaction_retain_test() ->
    RootPath = reset_filestructure(),
    Opts = [
        {root_path, RootPath},
        {max_journalsize, 1000000},
        {max_run_length, 1},
        {cache_size, 500},
        {sync_strategy, riak_sync},
        {compression_method, none},
        {reload_strategy, [{?STD_TAG, retain}]},
        {journalcompaction_scoreonein, 1},
        {singlefile_compactionpercentage, 0.0},
        {maxrunlength_compactionpercentage, 0.0}
    ],
    {ok, Bookie1} = book_start(Opts),
    ok =
        book_batchput(Bookie1, [
            {put, <<"B">>, <<"K1">>, {value, <<"V1">>}, [
                {add, <<"idx_bin">>, <<"OLD">>}
            ], ?STD_TAG, infinity},
            {put, <<"B">>, <<"K2">>, {value, <<"V2">>}, [
                {add, <<"idx_bin">>, <<"DEAD">>}
            ], ?STD_TAG, infinity}
        ]),
    ok =
        book_batchput(Bookie1, [
            {put, <<"B">>, <<"K1">>, {value, <<"V1B">>}, [
                {remove, <<"idx_bin">>, <<"OLD">>},
                {add, <<"idx_bin">>, <<"NEW">>}
            ], ?STD_TAG, infinity},
            {delete, <<"B">>, <<"K2">>, [
                {remove, <<"idx_bin">>, <<"DEAD">>}
            ], ?STD_TAG, infinity}
        ]),
    {ok, Inker, _Penciller} = book_returnactors(Bookie1),
    ok = leveled_inker:ink_roll(Inker),
    ok = book_compactjournal(Bookie1, 30000),
    wait_for_batch_compaction(Bookie1),
    assert_compacted_batch_state(Bookie1),
    ok = book_close(Bookie1),
    leveled_penciller:clean_testdir(RootPath ++ "/" ++ ?LEDGER_FP),
    {ok, Bookie2} = book_start(Opts),
    assert_compacted_batch_state(Bookie2),
    ok = book_destroy(Bookie2).

batchput_hot_backup_after_compaction_test() ->
    RootPath = reset_filestructure(),
    BackupPath = reset_filestructure("test/test_batchput_backup"),
    Opts = [
        {root_path, RootPath},
        {max_journalsize, 1000000},
        {max_run_length, 1},
        {cache_size, 500},
        {sync_strategy, riak_sync},
        {compression_method, none},
        {reload_strategy, [{?STD_TAG, retain}]},
        {journalcompaction_scoreonein, 1},
        {singlefile_compactionpercentage, 0.0},
        {maxrunlength_compactionpercentage, 0.0}
    ],
    {ok, Bookie1} = book_start(Opts),
    ok =
        book_batchput(Bookie1, [
            {put, <<"B">>, <<"K1">>, {value, <<"V1">>}, [
                {add, <<"idx_bin">>, <<"OLD">>}
            ], ?STD_TAG, infinity},
            {put, <<"B">>, <<"K2">>, {value, <<"V2">>}, [
                {add, <<"idx_bin">>, <<"DEAD">>}
            ], ?STD_TAG, infinity}
        ]),
    ok =
        book_batchput(Bookie1, [
            {put, <<"B">>, <<"K1">>, {value, <<"V1B">>}, [
                {remove, <<"idx_bin">>, <<"OLD">>},
                {add, <<"idx_bin">>, <<"NEW">>}
            ], ?STD_TAG, infinity},
            {delete, <<"B">>, <<"K2">>, [
                {remove, <<"idx_bin">>, <<"DEAD">>}
            ], ?STD_TAG, infinity}
        ]),
    {ok, Inker, _Penciller} = book_returnactors(Bookie1),
    ok = leveled_inker:ink_roll(Inker),
    ok = book_compactjournal(Bookie1, 30000),
    wait_for_batch_compaction(Bookie1),
    assert_compacted_batch_state(Bookie1),

    {async, BackupFun} = book_hotbackup(Bookie1),
    ok = BackupFun(BackupPath),
    ok = book_destroy(Bookie1),

    {ok, Bookie2} =
        book_start([{root_path, BackupPath} | proplists:delete(root_path, Opts)]),
    assert_compacted_batch_state(Bookie2),
    ok = book_destroy(Bookie2).

batchput_recovery_strategy_test() ->
    batchput_recovr_reload_case(),
    batchput_appdefined_recalc_case().

batchput_recovr_reload_case() ->
    RootPath = reset_filestructure(),
    Opts = [
        {root_path, RootPath},
        {max_journalsize, 1000000},
        {cache_size, 500},
        {sync_strategy, riak_sync},
        {compression_method, none},
        {reload_strategy, [{?STD_TAG, recovr}]}
    ],
    {ok, Bookie1} = book_start(Opts),
    ok =
        book_batchput(Bookie1, [
            {put, <<"B">>, <<"K1">>, {value, <<"V1">>}, [
                {add, <<"idx_bin">>, <<"RECOVR">>}
            ], ?STD_TAG, infinity},
            {put, <<"B">>, <<"K2">>, {value, <<"V2">>}, [
                {add, <<"idx_bin">>, <<"RECOVR">>}
            ], ?STD_TAG, infinity}
        ]),
    ok = book_close(Bookie1),
    leveled_penciller:clean_testdir(RootPath ++ "/" ++ ?LEDGER_FP),
    {ok, Bookie2} = book_start(Opts),
    {ok, {value, <<"V1">>}} = book_get(Bookie2, <<"B">>, <<"K1">>),
    {ok, {value, <<"V2">>}} = book_get(Bookie2, <<"B">>, <<"K2">>),
    ?assertEqual(
        [{<<"RECOVR">>, <<"K1">>}, {<<"RECOVR">>, <<"K2">>}],
        indexfold_matches(Bookie2, <<"idx_bin">>, <<"RECOVR">>)
    ),
    ok = book_destroy(Bookie2).

batchput_appdefined_recalc_case() ->
    RootPath = reset_filestructure(),
    Tag = batch_recalc_tag,
    ExtractMDFun =
        fun(batch_recalc_tag, Size, Obj) ->
            [{index, Indexes}, {value, _Value}] = Obj,
            {{erlang:phash2(term_to_binary(Obj)), Size, {index, Indexes}}, []}
        end,
    CalcIndexFun =
        fun(batch_recalc_tag, UpdMeta, PrvMeta) ->
            {index, UpdIndexes} = element(3, UpdMeta),
            PrvIndexes =
                case PrvMeta of
                    not_present ->
                        [];
                    PrvMeta when is_tuple(PrvMeta) ->
                        {index, Indexes} = element(3, PrvMeta),
                        Indexes
                end,
            AddSpecs =
                lists:map(
                    fun(I) -> {add, <<"temp_int">>, I} end,
                    lists:subtract(UpdIndexes, PrvIndexes)
                ),
            RemoveSpecs =
                lists:map(
                    fun(I) -> {remove, <<"temp_int">>, I} end,
                    lists:subtract(PrvIndexes, UpdIndexes)
                ),
            AddSpecs ++ RemoveSpecs
        end,
    Opts = [
        {root_path, RootPath},
        {max_journalsize, 1000000},
        {max_run_length, 1},
        {cache_size, 500},
        {sync_strategy, riak_sync},
        {compression_method, none},
        {reload_strategy, [{Tag, recalc}]},
        {override_functions, [
            {extract_metadata, ExtractMDFun},
            {diff_indexspecs, CalcIndexFun}
        ]},
        {journalcompaction_scoreonein, 1},
        {singlefile_compactionpercentage, 0.0},
        {maxrunlength_compactionpercentage, 0.0}
    ],
    {ok, Bookie1} = book_start(Opts),
    ok =
        book_batchput(Bookie1, [
            {put, <<"B">>, <<"K1">>, [{index, [1]}, {value, <<"V1">>}], [
                {add, <<"temp_int">>, 1}
            ], Tag, infinity},
            {put, <<"B">>, <<"K2">>, [{index, [2]}, {value, <<"V2">>}], [
                {add, <<"temp_int">>, 2}
            ], Tag, infinity}
        ]),
    ok =
        book_batchput(Bookie1, [
            {put, <<"B">>, <<"K1">>, [{index, [3]}, {value, <<"V1B">>}], [
                {remove, <<"temp_int">>, 1},
                {add, <<"temp_int">>, 3}
            ], Tag, infinity}
        ]),
    {ok, Inker, _Penciller} = book_returnactors(Bookie1),
    ok = leveled_inker:ink_roll(Inker),
    ok = book_compactjournal(Bookie1, 30000),
    wait_for_batch_compaction(Bookie1),
    assert_recalc_batch_state(Bookie1, Tag),
    ok = book_close(Bookie1),
    leveled_penciller:clean_testdir(RootPath ++ "/" ++ ?LEDGER_FP),
    {ok, Bookie2} = book_start(Opts),
    assert_recalc_batch_state(Bookie2, Tag),
    ok = book_destroy(Bookie2),
    application:unset_env(leveled, extract_metadata),
    application:unset_env(leveled, diff_indexspecs).

batchput_manifest_sqn_test() ->
    RootPath = reset_filestructure(),
    Opts = [
        {root_path, RootPath},
        {max_journalsize, 1000000},
        {cache_size, 500},
        {compression_method, none}
    ],
    {ok, Bookie1} = book_start(Opts),
    ok =
        book_batchput(Bookie1, [
            {put, <<"B">>, <<"K1">>, {value, <<"V1">>}, [], ?STD_TAG, infinity},
            {put, <<"B">>, <<"K2">>, {value, <<"V2">>}, [], ?STD_TAG, infinity}
        ]),
    {ok, BatchSQN} = book_sqn(Bookie1, <<"B">>, <<"K1">>),
    {ok, BatchSQN} = book_sqn(Bookie1, <<"B">>, <<"K2">>),
    ok = book_close(Bookie1),
    {ok, Bookie2} = book_start(Opts),
    {ok, Inker, _Penciller} = book_returnactors(Bookie2),
    ?assertMatch({ok, BatchSQN}, leveled_inker:ink_getjournalsqn(Inker)),
    ?assertMatch(
        {{BatchSQN, {o, <<"B">>, <<"K1">>, null}}, {{value, <<"V1">>}, _}},
        leveled_inker:ink_get(Inker, {o, <<"B">>, <<"K1">>, null}, BatchSQN)
    ),
    ?assertMatch(
        {{BatchSQN, {o, <<"B">>, <<"K2">>, null}}, {{value, <<"V2">>}, _}},
        leveled_inker:ink_get(Inker, {o, <<"B">>, <<"K2">>, null}, BatchSQN)
    ),
    [ActiveJournal | _] = leveled_inker:ink_getcdbpids(Inker),
    ?assertMatch({BatchSQN, ?INKT_STND, _}, leveled_cdb:cdb_lastkey(ActiveJournal)),
    ok = book_put(Bookie2, <<"B">>, <<"K3">>, {value, <<"V3">>}, [], ?STD_TAG),
    {ok, NextSQN} = book_sqn(Bookie2, <<"B">>, <<"K3">>),
    ?assertEqual(BatchSQN + 1, NextSQN),
    ok = book_destroy(Bookie2).

batchput_large_batch_reload_test() ->
    RootPath = reset_filestructure(),
    Opts = [
        {root_path, RootPath},
        {max_journalsize, 5000000},
        {cache_size, 500},
        {sync_strategy, riak_sync},
        {compression_method, none}
    ],
    BatchSize = ?LOADING_BATCH + 300,
    ?assert(BatchSize >= 500),
    BatchSpecs =
        lists:map(
            fun(N) ->
                Key = integer_to_binary(N),
                {put, <<"B">>, Key, {value, N}, [
                    {add, <<"patch_int">>, N}
                ], ?STD_TAG, infinity}
            end,
            lists:seq(1, BatchSize)
        ),
    {ok, Bookie1} = book_start(Opts),
    ok = book_batchput(Bookie1, BatchSpecs, true),
    ok = book_close(Bookie1),
    leveled_penciller:clean_testdir(RootPath ++ "/" ++ ?LEDGER_FP),

    {ok, Bookie2} = book_start(Opts),
    ExpectedObjects =
        lists:sort([
            {<<"B">>, integer_to_binary(N), {value, N}}
            || N <- lists:seq(1, BatchSize)
        ]),
    ExpectedIndexes =
        lists:sort([
            {N, integer_to_binary(N)}
            || N <- lists:seq(1, BatchSize)
        ]),
    ?assertEqual(ExpectedObjects, batch_objectfold(Bookie2)),
    ?assertEqual(
        ExpectedIndexes,
        indexfold_matches_range(Bookie2, <<"patch_int">>, 1, BatchSize)
    ),
    ok = book_destroy(Bookie2).

batchput_mixed_large_reload_test() ->
    RootPath = reset_filestructure(),
    Opts = [
        {root_path, RootPath},
        {max_journalsize, 5000000},
        {cache_size, 500},
        {sync_strategy, riak_sync},
        {compression_method, none}
    ],
    ExistingCount = ?LOADING_BATCH + 20,
    UpdateCount = 80,
    DeleteCount = 80,
    AddCount = 80,
    ?assert(UpdateCount + DeleteCount + AddCount > ?LOADING_BATCH),
    UpdateRange = lists:seq(1, UpdateCount),
    DeleteRange = lists:seq(UpdateCount + 1, UpdateCount + DeleteCount),
    RetainRange = lists:seq(UpdateCount + DeleteCount + 1, ExistingCount),
    AddRange = lists:seq(ExistingCount + 1, ExistingCount + AddCount),
    SeedSpecs = [
        {put, <<"B">>, integer_to_binary(N), {seed, N}, [
            {add, <<"mix_int">>, N}
        ], ?STD_TAG, infinity}
     || N <- lists:seq(1, ExistingCount)
    ],
    UpdateSpecs = [
        {put, <<"B">>, integer_to_binary(N), {updated, N}, [
            {remove, <<"mix_int">>, N},
            {add, <<"mix_int">>, N + 1000}
        ], ?STD_TAG, infinity}
     || N <- UpdateRange
    ],
    DeleteSpecs = [
        {delete, <<"B">>, integer_to_binary(N), [
            {remove, <<"mix_int">>, N}
        ], ?STD_TAG, infinity}
     || N <- DeleteRange
    ],
    AddSpecs = [
        {put, <<"B">>, integer_to_binary(N), {added, N}, [
            {add, <<"mix_int">>, N + 1000}
        ], ?STD_TAG, infinity}
     || N <- AddRange
    ],

    {ok, Bookie1} = book_start(Opts),
    ok = book_batchput(Bookie1, SeedSpecs, true),
    ok = book_batchput(Bookie1, UpdateSpecs ++ DeleteSpecs ++ AddSpecs, true),
    ok = book_close(Bookie1),
    leveled_penciller:clean_testdir(RootPath ++ "/" ++ ?LEDGER_FP),

    {ok, Bookie2} = book_start(Opts),
    ExpectedObjects =
        lists:sort(
            [
                {<<"B">>, integer_to_binary(N), {updated, N}}
             || N <- UpdateRange
            ] ++
                [
                    {<<"B">>, integer_to_binary(N), {seed, N}}
                 || N <- RetainRange
                ] ++
                [
                    {<<"B">>, integer_to_binary(N), {added, N}}
                 || N <- AddRange
                ]
        ),
    ExpectedIndexes =
        lists:sort(
            [
                {N + 1000, integer_to_binary(N)}
             || N <- UpdateRange
            ] ++
                [
                    {N, integer_to_binary(N)}
                 || N <- RetainRange
                ] ++
                [
                    {N + 1000, integer_to_binary(N)}
                 || N <- AddRange
                ]
        ),
    ?assertEqual(ExpectedObjects, batch_objectfold(Bookie2)),
    ?assertEqual(
        ExpectedIndexes,
        indexfold_matches_range(
            Bookie2, <<"mix_int">>, 1, ExistingCount + AddCount + 1000
        )
    ),
    ok = book_destroy(Bookie2).

batchput_snapshot_and_fold_test() ->
    RootPath = reset_filestructure(),
    Opts = [
        {root_path, RootPath},
        {max_journalsize, 1000000},
        {cache_size, 500},
        {compression_method, none}
    ],
    {ok, Bookie1} = book_start(Opts),
    ok =
        book_put(
            Bookie1,
            <<"B">>,
            <<"K0">>,
            {value, <<"V0">>},
            [{add, <<"idx_bin">>, <<"OLD">>}],
            ?STD_TAG
        ),
    {ok, SnapshotBefore} = book_start([{snapshot_bookie, Bookie1}]),
    ok =
        book_batchput(Bookie1, [
            {put, <<"B">>, <<"K1">>, {value, <<"V1">>}, [
                {add, <<"idx_bin">>, <<"BATCH">>}
            ], ?STD_TAG, infinity},
            {put, <<"B">>, <<"K2">>, {value, <<"V2">>}, [
                {add, <<"idx_bin">>, <<"BATCH">>}
            ], ?STD_TAG, infinity}
        ]),
    not_found = book_get(SnapshotBefore, <<"B">>, <<"K1">>),
    ?assertEqual([], indexfold_matches(SnapshotBefore, <<"idx_bin">>, <<"BATCH">>)),
    {ok, SnapshotAfter} = book_start([{snapshot_bookie, Bookie1}]),
    {ok, {value, <<"V1">>}} = book_get(SnapshotAfter, <<"B">>, <<"K1">>),
    {ok, {value, <<"V2">>}} = book_get(SnapshotAfter, <<"B">>, <<"K2">>),
    ?assertEqual(
        [{<<"BATCH">>, <<"K1">>}, {<<"BATCH">>, <<"K2">>}],
        indexfold_matches(SnapshotAfter, <<"idx_bin">>, <<"BATCH">>)
    ),
    ?assertEqual(
        [{<<"B">>, <<"K0">>}, {<<"B">>, <<"K1">>}, {<<"B">>, <<"K2">>}],
        batch_keylist(Bookie1)
    ),
    ?assertEqual(
        [
            {<<"B">>, <<"K0">>, {value, <<"V0">>}},
            {<<"B">>, <<"K1">>, {value, <<"V1">>}},
            {<<"B">>, <<"K2">>, {value, <<"V2">>}}
        ],
        batch_objectfold(Bookie1)
    ),
    ?assertEqual(
        [{<<"B">>, <<"K0">>}, {<<"B">>, <<"K1">>}, {<<"B">>, <<"K2">>}],
        batch_headfold(Bookie1)
    ),
    {async, ThrowingIndexFold} =
        book_indexfold(
            Bookie1,
            <<"B">>,
            {fun(_B, _K, _Acc) -> throw(stop_fold) end, []},
            {<<"idx_bin">>, <<"BATCH">>, <<"BATCH">>},
            {true, undefined}
        ),
    ?assertThrow(stop_fold, ThrowingIndexFold()),
    ?assertEqual(
        [{<<"BATCH">>, <<"K1">>}, {<<"BATCH">>, <<"K2">>}],
        indexfold_matches(Bookie1, <<"idx_bin">>, <<"BATCH">>)
    ),
    ok = book_close(SnapshotBefore),
    ok = book_close(SnapshotAfter),
    ok = book_destroy(Bookie1).

batchput_sqnorder_fold_test() ->
    RootPath = reset_filestructure(),
    Opts = [
        {root_path, RootPath},
        {max_journalsize, 1000000},
        {cache_size, 500},
        {sync_strategy, riak_sync},
        {compression_method, none}
    ],
    {ok, Bookie1} = book_start(Opts),
    ok = book_put(Bookie1, <<"B">>, <<"K0">>, {value, <<"V0">>}, [], ?STD_TAG),
    ok =
        book_batchput(Bookie1, [
            {put, <<"B">>, <<"K1">>, {value, <<"V1">>}, [], ?STD_TAG, infinity},
            {put, <<"B">>, <<"K2">>, {value, <<"V2">>}, [], ?STD_TAG, infinity},
            {put, <<"B">>, <<"K3">>, {value, <<"V3">>}, [], ?STD_TAG, infinity}
        ]),
    {ok, BatchSQN} = book_sqn(Bookie1, <<"B">>, <<"K1">>),
    {ok, BatchSQN} = book_sqn(Bookie1, <<"B">>, <<"K2">>),
    {ok, BatchSQN} = book_sqn(Bookie1, <<"B">>, <<"K3">>),
    ok = book_put(Bookie1, <<"B">>, <<"K4">>, {value, <<"V4">>}, [], ?STD_TAG),
    {ok, NextSQN} = book_sqn(Bookie1, <<"B">>, <<"K4">>),
    ?assertEqual(BatchSQN + 1, NextSQN),

    FoldObjectsFun = fun(B, K, V, Acc) -> Acc ++ [{B, K, V}] end,
    {async, ObjFPre} =
        book_objectfold(
            Bookie1, ?STD_TAG, {FoldObjectsFun, []}, true, sqn_order
        ),
    ObjLPre = ObjFPre(),
    ?assertEqual(
        [
            {<<"B">>, <<"K0">>, {value, <<"V0">>}},
            {<<"B">>, <<"K1">>, {value, <<"V1">>}},
            {<<"B">>, <<"K2">>, {value, <<"V2">>}},
            {<<"B">>, <<"K3">>, {value, <<"V3">>}},
            {<<"B">>, <<"K4">>, {value, <<"V4">>}}
        ],
        ObjLPre
    ),
    ok = book_close(Bookie1),
    leveled_penciller:clean_testdir(RootPath ++ "/" ++ ?LEDGER_FP),
    {ok, Bookie2} = book_start(Opts),
    {async, ObjFReload} =
        book_objectfold(
            Bookie2, ?STD_TAG, {FoldObjectsFun, []}, true, sqn_order
        ),
    ?assertEqual(ObjLPre, ObjFReload()),
    ok = book_destroy(Bookie2).

batchput_pause_semantics_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} =
        book_start([
            {root_path, RootPath},
            {max_journalsize, 1000000},
            {cache_size, 500},
            {compression_method, none}
        ]),
    _ =
        sys:replace_state(
            Bookie1,
            fun(State) -> State#state{slow_offer = true} end
        ),
    pause =
        book_batchput(Bookie1, [
            {put, <<"B">>, <<"K1">>, {value, <<"V1">>}, [], ?STD_TAG, infinity}
        ]),
    {ok, {value, <<"V1">>}} = book_get(Bookie1, <<"B">>, <<"K1">>),
    ok = book_destroy(Bookie1).

batchput_validation_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} =
        book_start([
            {root_path, RootPath},
            {max_journalsize, 1000000},
            {cache_size, 500},
            {compression_method, none}
        ]),
    PutSpec = {put, <<"B">>, <<"K1">>, {value, <<"V1">>}, [], ?STD_TAG, infinity},
    ?assertMatch({error, empty_batch}, book_batchput(Bookie1, [])),
    ?assertMatch(
        {error, {duplicate_key, _}},
        book_batchput(Bookie1, [PutSpec, PutSpec])
    ),
    ?assertMatch(
        {error, head_tag_not_supported},
        book_batchput(Bookie1, [
            {put, <<"B">>, <<"K1">>, {value, <<"V1">>}, [], ?HEAD_TAG, infinity}
        ])
    ),
    ?assertMatch(
        {error, invalid_index_specs},
        book_batchput(Bookie1, [
            {put, <<"B">>, <<"K1">>, {value, <<"V1">>}, [bad], ?STD_TAG, infinity}
        ])
    ),
    ok = book_destroy(Bookie1).

batchput_validation_atomic_rejection_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} =
        book_start([
            {root_path, RootPath},
            {max_journalsize, 1000000},
            {cache_size, 500},
            {compression_method, none}
        ]),
    ValidSpec = {put, <<"B">>, <<"K1">>, {value, <<"V1">>}, [
        {add, <<"idx_bin">>, <<"LEAK">>}
    ], ?STD_TAG, infinity},
    ?assertMatch(
        {error, invalid_index_specs},
        book_batchput(Bookie1, [
            ValidSpec,
            {put, <<"B">>, <<"K2">>, {value, <<"V2">>}, [bad], ?STD_TAG,
                infinity}
        ])
    ),
    not_found = book_get(Bookie1, <<"B">>, <<"K1">>),
    not_found = book_head(Bookie1, <<"B">>, <<"K1">>),
    ?assertEqual([], indexfold_matches(Bookie1, <<"idx_bin">>, <<"LEAK">>)),

    ok =
        book_put(
            Bookie1,
            <<"B">>,
            <<"K0">>,
            {value, <<"OLD">>},
            [{add, <<"idx_bin">>, <<"OLD">>}],
            ?STD_TAG
        ),
    UpdateSpec = {put, <<"B">>, <<"K0">>, {value, <<"NEW">>}, [
        {remove, <<"idx_bin">>, <<"OLD">>},
        {add, <<"idx_bin">>, <<"NEW">>}
    ], ?STD_TAG, infinity},
    ?assertMatch(
        {error, {duplicate_key, _}},
        book_batchput(Bookie1, [UpdateSpec, UpdateSpec])
    ),
    {ok, {value, <<"OLD">>}} = book_get(Bookie1, <<"B">>, <<"K0">>),
    ?assertEqual(
        [{<<"OLD">>, <<"K0">>}],
        indexfold_matches(Bookie1, <<"idx_bin">>, <<"OLD">>)
    ),
    ?assertEqual([], indexfold_matches(Bookie1, <<"idx_bin">>, <<"NEW">>)),
    ok = book_destroy(Bookie1).

batchput_bucket_isolation_test() ->
    RootPath = reset_filestructure(),
    Opts = [
        {root_path, RootPath},
        {max_journalsize, 1000000},
        {cache_size, 500},
        {compression_method, none}
    ],
    {ok, Bookie1} = book_start(Opts),
    ok =
        book_batchput(Bookie1, [
            {put, <<"B1">>, <<"K1">>, {value, b1_k1}, [
                {add, <<"shared_bin">>, <<"SAME">>}
            ], ?STD_TAG, infinity},
            {put, <<"B2">>, <<"K1">>, {value, b2_k1}, [
                {add, <<"shared_bin">>, <<"SAME">>}
            ], ?STD_TAG, infinity},
            {put, <<"B2">>, <<"K2">>, {value, b2_k2}, [
                {add, <<"shared_bin">>, <<"SAME">>}
            ], ?STD_TAG, infinity}
        ]),
    ?assertEqual(
        [{<<"SAME">>, <<"K1">>}],
        indexfold_matches(Bookie1, <<"B1">>, <<"shared_bin">>, <<"SAME">>)
    ),
    ?assertEqual(
        [{<<"SAME">>, <<"K1">>}, {<<"SAME">>, <<"K2">>}],
        indexfold_matches(Bookie1, <<"B2">>, <<"shared_bin">>, <<"SAME">>)
    ),
    ok =
        book_batchput(Bookie1, [
            {delete, <<"B2">>, <<"K1">>, [
                {remove, <<"shared_bin">>, <<"SAME">>}
            ], ?STD_TAG, infinity}
        ]),
    ok = book_close(Bookie1),
    {ok, Bookie2} = book_start(Opts),
    {ok, {value, b1_k1}} = book_get(Bookie2, <<"B1">>, <<"K1">>),
    not_found = book_get(Bookie2, <<"B2">>, <<"K1">>),
    {ok, {value, b2_k2}} = book_get(Bookie2, <<"B2">>, <<"K2">>),
    ?assertEqual(
        [{<<"SAME">>, <<"K1">>}],
        indexfold_matches(Bookie2, <<"B1">>, <<"shared_bin">>, <<"SAME">>)
    ),
    ?assertEqual(
        [{<<"SAME">>, <<"K2">>}],
        indexfold_matches(Bookie2, <<"B2">>, <<"shared_bin">>, <<"SAME">>)
    ),
    ok = book_destroy(Bookie2).

batchput_ttl_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} =
        book_start([
            {root_path, RootPath},
            {max_journalsize, 1000000},
            {cache_size, 500},
            {compression_method, none}
        ]),
    Future = leveled_util:integer_now() + 300,
    Past = leveled_util:integer_now() - 300,
    ok =
        book_batchput(Bookie1, [
            {put, <<"B">>, <<"KF">>, {value, future}, [
                {add, <<"ttl_bin">>, <<"LIVE">>}
            ], ?STD_TAG, Future},
            {put, <<"B">>, <<"KP">>, {value, past}, [
                {add, <<"ttl_bin">>, <<"LIVE">>}
            ], ?STD_TAG, Past}
        ]),
    {ok, {value, future}} = book_get(Bookie1, <<"B">>, <<"KF">>),
    not_found = book_get(Bookie1, <<"B">>, <<"KP">>),
    not_found = book_head(Bookie1, <<"B">>, <<"KP">>),
    ?assertEqual(
        [{<<"LIVE">>, <<"KF">>}],
        indexfold_matches(Bookie1, <<"ttl_bin">>, <<"LIVE">>)
    ),
    ok = book_destroy(Bookie1).

cas_compaction_opts(RootPath, ReloadStrategy) ->
    [
        {root_path, RootPath},
        {max_journalsize, 1000000},
        {max_run_length, 1},
        {cache_size, 500},
        {sync_strategy, riak_sync},
        {compression_method, none},
        {reload_strategy, ReloadStrategy},
        {journalcompaction_scoreonein, 1},
        {singlefile_compactionpercentage, 0.0},
        {maxrunlength_compactionpercentage, 0.0}
    ].

seed_cas_compaction_state(Bookie) ->
    ok =
        book_casbatchput(
            Bookie,
            [
                {put, <<"B">>, <<"K1">>, {value, <<"V1">>}, [
                    {add, <<"idx_bin">>, <<"OLD">>}
                ], ?STD_TAG, infinity},
                {put, <<"B">>, <<"K2">>, {value, <<"V2">>}, [
                    {add, <<"idx_bin">>, <<"DEAD">>}
                ], ?STD_TAG, infinity},
                {put, <<"B">>, <<"K3">>, {value, <<"V3">>}, [
                    {add, <<"idx_bin">>, <<"KEEP">>}
                ], ?STD_TAG, infinity}
            ],
            [
                {<<"B">>, <<"K1">>, ?STD_TAG, absent},
                {<<"B">>, <<"K2">>, ?STD_TAG, absent},
                {<<"B">>, <<"K3">>, ?STD_TAG, absent}
            ],
            true
        ),
    {ok, _V1, SQN1} = book_get_sqn(Bookie, <<"B">>, <<"K1">>),
    {ok, _V2, SQN2} = book_get_sqn(Bookie, <<"B">>, <<"K2">>),
    {ok, _V3, SQN3} = book_get_sqn(Bookie, <<"B">>, <<"K3">>),
    ?assertMatch(
        {error, {precondition_failed, [_]}},
        book_casput(
            Bookie,
            <<"B">>,
            <<"K3">>,
            {value, <<"STALE">>},
            [
                {remove, <<"idx_bin">>, <<"KEEP">>},
                {add, <<"idx_bin">>, <<"STALE">>}
            ],
            ?STD_TAG,
            infinity,
            true,
            {sqn, SQN3 + 1000}
        )
    ),
    ok =
        book_casbatchput(
            Bookie,
            [
                {put, <<"B">>, <<"K1">>, {value, <<"V1B">>}, [
                    {remove, <<"idx_bin">>, <<"OLD">>},
                    {add, <<"idx_bin">>, <<"NEW">>}
                ], ?STD_TAG, infinity},
                {delete, <<"B">>, <<"K2">>, [
                    {remove, <<"idx_bin">>, <<"DEAD">>}
                ], ?STD_TAG, infinity}
            ],
            [
                {<<"B">>, <<"K1">>, ?STD_TAG, {sqn, SQN1}},
                {<<"B">>, <<"K2">>, ?STD_TAG, {sqn, SQN2}}
            ],
            true
        ).

assert_cas_compacted_state(Bookie) ->
    {ok, {value, <<"V1B">>}} = book_get(Bookie, <<"B">>, <<"K1">>),
    {ok, _Head1, _SQN1} = book_head_sqn(Bookie, <<"B">>, <<"K1">>),
    not_found = book_get(Bookie, <<"B">>, <<"K2">>),
    not_found = book_head(Bookie, <<"B">>, <<"K2">>),
    {ok, {value, <<"V3">>}} = book_get(Bookie, <<"B">>, <<"K3">>),
    {ok, _Head3, _SQN3} = book_head_sqn(Bookie, <<"B">>, <<"K3">>),
    ?assertEqual([], indexfold_matches(Bookie, <<"idx_bin">>, <<"OLD">>)),
    ?assertEqual([], indexfold_matches(Bookie, <<"idx_bin">>, <<"DEAD">>)),
    ?assertEqual([], indexfold_matches(Bookie, <<"idx_bin">>, <<"STALE">>)),
    ?assertEqual(
        [{<<"NEW">>, <<"K1">>}],
        indexfold_matches(Bookie, <<"idx_bin">>, <<"NEW">>)
    ),
    ?assertEqual(
        [{<<"KEEP">>, <<"K3">>}],
        indexfold_matches(Bookie, <<"idx_bin">>, <<"KEEP">>)
    ).

assert_cas_recalc_state(Bookie, Tag) ->
    {ok, [{index, [4]}, {value, <<"V1B">>}]} =
        book_get(Bookie, <<"B">>, <<"K1">>, Tag),
    {ok, _Head1, _SQN1} = book_head_sqn(Bookie, <<"B">>, <<"K1">>, Tag),
    not_found = book_get(Bookie, <<"B">>, <<"K2">>, Tag),
    not_found = book_head(Bookie, <<"B">>, <<"K2">>, Tag),
    {ok, [{index, [3]}, {value, <<"V3">>}]} =
        book_get(Bookie, <<"B">>, <<"K3">>, Tag),
    {ok, _Head3, _SQN3} = book_head_sqn(Bookie, <<"B">>, <<"K3">>, Tag),
    ?assertEqual([], indexfold_matches(Bookie, <<"temp_int">>, 1)),
    ?assertEqual([], indexfold_matches(Bookie, <<"temp_int">>, 2)),
    ?assertEqual([], indexfold_matches(Bookie, <<"temp_int">>, 99)),
    ?assertEqual(
        [{3, <<"K3">>}],
        indexfold_matches(Bookie, <<"temp_int">>, 3)
    ),
    ?assertEqual(
        [{4, <<"K1">>}],
        indexfold_matches(Bookie, <<"temp_int">>, 4)
    ).

indexfold_matches(Bookie, Bucket, IndexName, IndexValue) ->
    {async, Folder} =
        book_indexfold(
            Bookie,
            Bucket,
            {fun(_B, {IdxV, K}, Acc) -> [{IdxV, K} | Acc] end, []},
            {IndexName, IndexValue, IndexValue},
            {true, undefined}
        ),
    lists:sort(Folder()).

indexfold_matches(Bookie, IndexName, IndexValue) ->
    indexfold_matches(Bookie, <<"B">>, IndexName, IndexValue).

indexfold_matches_range(Bookie, IndexName, LowIndexValue, HighIndexValue) ->
    indexfold_matches_range(
        Bookie, <<"B">>, IndexName, LowIndexValue, HighIndexValue
    ).

indexfold_matches_range(
    Bookie, Bucket, IndexName, LowIndexValue, HighIndexValue
) ->
    {async, Folder} =
        book_indexfold(
            Bookie,
            Bucket,
            {fun(_B, {IdxV, K}, Acc) -> [{IdxV, K} | Acc] end, []},
            {IndexName, LowIndexValue, HighIndexValue},
            {true, undefined}
        ),
    lists:sort(Folder()).

wait_down(Ref, Pid, Label) ->
    receive
        {'DOWN', Ref, process, Pid, _Reason} ->
            ok
    after 5000 ->
        error({process_still_alive, Label})
    end.

wait_for_batch_compaction(Bookie) ->
    wait_for_batch_compaction(Bookie, 50).

wait_for_batch_compaction(Bookie, Remaining) when Remaining > 0 ->
    case book_islastcompactionpending(Bookie) of
        false ->
            ok;
        true ->
            timer:sleep(100),
            wait_for_batch_compaction(Bookie, Remaining - 1)
    end;
wait_for_batch_compaction(_Bookie, 0) ->
    error(compaction_still_pending).

assert_compacted_batch_state(Bookie) ->
    {ok, {value, <<"V1B">>}} = book_get(Bookie, <<"B">>, <<"K1">>),
    not_found = book_get(Bookie, <<"B">>, <<"K2">>),
    not_found = book_head(Bookie, <<"B">>, <<"K2">>),
    ?assertEqual([], indexfold_matches(Bookie, <<"idx_bin">>, <<"OLD">>)),
    ?assertEqual([], indexfold_matches(Bookie, <<"idx_bin">>, <<"DEAD">>)),
    ?assertEqual(
        [{<<"NEW">>, <<"K1">>}],
        indexfold_matches(Bookie, <<"idx_bin">>, <<"NEW">>)
    ).

assert_recalc_batch_state(Bookie, Tag) ->
    {ok, [{index, [3]}, {value, <<"V1B">>}]} =
        book_get(Bookie, <<"B">>, <<"K1">>, Tag),
    {ok, [{index, [2]}, {value, <<"V2">>}]} =
        book_get(Bookie, <<"B">>, <<"K2">>, Tag),
    ?assertEqual([], indexfold_matches(Bookie, <<"temp_int">>, 1)),
    ?assertEqual(
        [{2, <<"K2">>}],
        indexfold_matches(Bookie, <<"temp_int">>, 2)
    ),
    ?assertEqual(
        [{3, <<"K1">>}],
        indexfold_matches(Bookie, <<"temp_int">>, 3)
    ).

truncate_after_first_cdb_record(JournalFile) ->
    {ok, Handle} = file:open(JournalFile, [read, write, binary]),
    {ok, 2048} = file:position(Handle, {bof, 2048}),
    {ok, <<KeyLength:32/little-integer, ValueLength:32/little-integer>>} =
        file:read(Handle, 8),
    FirstRecordEnd = 2048 + 8 + KeyLength + ValueLength,
    {ok, _} = file:position(Handle, {bof, FirstRecordEnd + 4}),
    ok = file:truncate(Handle),
    ok = file:close(Handle).

batch_keylist(Bookie) ->
    {async, Folder} =
        book_keylist(
            Bookie,
            ?STD_TAG,
            <<"B">>,
            {fun(B, K, Acc) -> [{B, K} | Acc] end, []}
        ),
    lists:sort(Folder()).

batch_objectfold(Bookie) ->
    {async, Folder} =
        book_objectfold(
            Bookie,
            ?STD_TAG,
            <<"B">>,
            all,
            {fun(B, K, V, Acc) -> [{B, K, V} | Acc] end, []},
            true
        ),
    lists:sort(Folder()).

batch_headfold(Bookie) ->
    {async, Folder} =
        book_headfold(
            Bookie,
            ?STD_TAG,
            {range, <<"B">>, all},
            {fun(B, K, _V, Acc) -> [{B, K} | Acc] end, []},
            false,
            true,
            false
        ),
    lists:sort(Folder()).

batchput_headonly_rejection_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} =
        book_start([
            {root_path, RootPath},
            {head_only, no_lookup},
            {compression_method, none}
        ]),
    ?assertMatch(
        {unsupported_message, batchput},
        book_batchput(Bookie1, [
            {put, <<"B">>, <<"K1">>, {value, <<"V1">>}, [], ?STD_TAG, infinity}
        ])
    ),
    ok = book_destroy(Bookie1).

sqnorder_fold_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} =
        book_start([
            {root_path, RootPath},
            {max_journalsize, 1000000},
            {cache_size, 500}
        ]),
    ok = book_put(Bookie1, <<"B">>, <<"K1">>, {value, <<"V1">>}, [], ?STD_TAG),
    ok = book_put(Bookie1, <<"B">>, <<"K2">>, {value, <<"V2">>}, [], ?STD_TAG),

    FoldObjectsFun = fun(B, K, V, Acc) -> Acc ++ [{B, K, V}] end,
    {async, ObjFPre} =
        book_objectfold(
            Bookie1, ?STD_TAG, {FoldObjectsFun, []}, true, sqn_order
        ),
    {async, ObjFPost} =
        book_objectfold(
            Bookie1, ?STD_TAG, {FoldObjectsFun, []}, false, sqn_order
        ),

    ok = book_put(Bookie1, <<"B">>, <<"K3">>, {value, <<"V3">>}, [], ?STD_TAG),

    ObjLPre = ObjFPre(),
    ?assertMatch(
        [
            {<<"B">>, <<"K1">>, {value, <<"V1">>}},
            {<<"B">>, <<"K2">>, {value, <<"V2">>}}
        ],
        ObjLPre
    ),
    ObjLPost = ObjFPost(),
    ?assertMatch(
        [
            {<<"B">>, <<"K1">>, {value, <<"V1">>}},
            {<<"B">>, <<"K2">>, {value, <<"V2">>}},
            {<<"B">>, <<"K3">>, {value, <<"V3">>}}
        ],
        ObjLPost
    ),

    ok = book_destroy(Bookie1).

sqnorder_mutatefold_test() ->
    RootPath = reset_filestructure(),
    {ok, Bookie1} =
        book_start([
            {root_path, RootPath},
            {max_journalsize, 1000000},
            {cache_size, 500}
        ]),
    ok = book_put(Bookie1, <<"B">>, <<"K1">>, {value, <<"V1">>}, [], ?STD_TAG),
    ok = book_put(Bookie1, <<"B">>, <<"K1">>, {value, <<"V2">>}, [], ?STD_TAG),

    FoldObjectsFun = fun(B, K, V, Acc) -> Acc ++ [{B, K, V}] end,
    {async, ObjFPre} =
        book_objectfold(
            Bookie1, ?STD_TAG, {FoldObjectsFun, []}, true, sqn_order
        ),
    {async, ObjFPost} =
        book_objectfold(
            Bookie1, ?STD_TAG, {FoldObjectsFun, []}, false, sqn_order
        ),

    ok = book_put(Bookie1, <<"B">>, <<"K1">>, {value, <<"V3">>}, [], ?STD_TAG),

    ObjLPre = ObjFPre(),
    ?assertMatch([{<<"B">>, <<"K1">>, {value, <<"V2">>}}], ObjLPre),
    ObjLPost = ObjFPost(),
    ?assertMatch([{<<"B">>, <<"K1">>, {value, <<"V3">>}}], ObjLPost),

    ok = book_destroy(Bookie1).

check_notfound_test() ->
    ProbablyFun = fun() -> probably end,
    MissingFun = fun() -> missing end,
    MinFreq =
        lists:foldl(
            fun(_I, Freq) ->
                {false, Freq0} = check_notfound(Freq, ProbablyFun),
                Freq0
            end,
            100,
            lists:seq(1, 5000)
        ),
    % 5000 as needs to be a lot as doesn't decrement
    % when random interval is not hit
    ?assertMatch(?MIN_KEYCHECK_FREQUENCY, MinFreq),

    ?assertMatch(
        {true, ?MAX_KEYCHECK_FREQUENCY},
        check_notfound(?MAX_KEYCHECK_FREQUENCY, MissingFun)
    ),

    ?assertMatch({false, 0}, check_notfound(0, MissingFun)).

-endif.

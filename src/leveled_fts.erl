%% -------- Full-text search: a pure client library ---------
%%
%% leveled_fts consumes ONLY the public store surface (docs/FTS.md):
%% book_mput/book_casmput/book_sqn/book_headonly/folds/snapshots. The
%% store carries no FTS hooks; all index state is ordinary HEAD_TAG
%% object-spec rows (postings, doc manifests, per-shard epoch rows,
%% consolidated bases, stats), committed in the caller's own batches.

-module(leveled_fts).

-include("leveled.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
    schema/1,
    capacities/0,
    derive/3,
    remove/3,
    update/4,
    search/4,
    consolidate/3,
    cache_table/2
]).

-define(DEFAULT_LIMIT, 10000).
-define(MAX_LIMIT, 20000).
-define(MAX_WINDOW, 20000).
-define(MAX_QUERY_BYTES, 4096).
-define(MAX_QUERY_TOKENS, 128).
-define(MAX_AST_DEPTH, 32).
-define(MAX_NEAR_DISTANCE, 64).
-define(MAX_PREFIX_BYTES, 64).
-define(MAX_RETURN_POSITIONS, 4096).
-define(DEFAULT_NEAR, 10).

%% Client-library wire format capacities.  These are deliberately exported by
%% capacities/0 and consumed by schema/1.  Every fixed-width writer below also
%% checks the same bound immediately before constructing a bit syntax.
-define(POSTING_VERSION, 2).
-define(BASE_VERSION, 1).
-define(MANIFEST_VERSION, 2).
-define(STATS_VERSION, 1).
-define(MAX_COLUMNS, 255).
-define(MAX_COLUMN_ID, 254).
-define(MAX_TOKEN_BYTES, 65535).
-define(MAX_POSITION_BYTES, 65535).
-define(MAX_POSITION_PREFIX_BYTES, 65525).
-define(MAX_DOC_KEY_BYTES, 65533).
-define(MAX_SHARDS, 65536).
-define(DEFAULT_SHARDS, 256).
-define(MAX_U16, 16#FFFF).
-define(MAX_U32, 16#FFFFFFFF).
-define(MAX_U64, 16#FFFFFFFFFFFFFFFF).
-define(CLIENT_CACHE_REGISTRY, leveled_fts_cache_registry).

%% ---------------------------------------------------------------------------
%% Pure client API (docs/FTS.md).
%%
%% All persisted state is made from ordinary HEAD_TAG object specs.  The store
%% has no FTS callback, configuration, or privileged payload.  The logical
%% stats row is a row family keyed by document: {Index, <<"stats">>, DocKey}.
%% LWW replacement of that contribution makes blind derive/update/remove exact;
%% ranked reads fold the family to {document_count,total_length}.
%% ---------------------------------------------------------------------------

-spec capacities() -> map().
capacities() ->
    #{
        columns => ?MAX_COLUMNS,
        column_id => ?MAX_COLUMN_ID,
        token_bytes => ?MAX_TOKEN_BYTES,
        position_bytes => ?MAX_POSITION_BYTES,
        doc_key_bytes => ?MAX_DOC_KEY_BYTES,
        shards => ?MAX_SHARDS,
        true_occurrences => ?MAX_U64
    }.

-spec schema(map()) -> {ok, map()} | {error, term()}.
schema(Definition) when is_map(Definition) ->
    try
        Index = normalise_index(maps:get(index, Definition)),
        true = is_binary(Index) andalso Index =/= <<>>,
        {ok, ColumnSpecs} = normalise_column_specs(maps:get(columns, Definition)),
        ColumnCount = length(ColumnSpecs),
        client_guard(columns, ColumnCount, maps:get(columns, capacities())),
        Shards = maps:get(shards, Definition, ?DEFAULT_SHARDS),
        true = is_integer(Shards) andalso Shards > 0 andalso
            Shards =< maps:get(shards, capacities()) andalso
            (Shards band (Shards - 1)) =:= 0,
        Opts0 = maps:with(
            [tokenizer, remove_diacritics, tokenchars, separators, stopwords,
                decode, prefixes],
            Definition
        ),
        true = valid_tokenizer(maps:get(tokenizer, Opts0, unicode61)),
        true = valid_remove_diacritics(
            maps:get(remove_diacritics, Opts0, 1)
        ),
        true = valid_char_option(maps:get(tokenchars, Opts0, [])),
        true = valid_char_option(maps:get(separators, Opts0, [])),
        Opts1 = normalise_options(
            Opts0#{remove_diacritics => client_rd_mode(
                maps:get(remove_diacritics, Opts0, 1)
            )}
        ),
        Columns = [Column || {Column, _Path, _Mode} <- ColumnSpecs],
        Canonical = #{
            index => Index,
            columns => Columns,
            column_specs => ColumnSpecs,
            column_modes => maps:from_list([
                {Column, Mode} || {Column, _Path, Mode} <- ColumnSpecs
            ]),
            options => Opts1,
            tokenizer => tokenizer_description(Opts1),
            prefixes => maps:get(prefixes, Opts1, []),
            shards => Shards
        },
        Fingerprint = crypto:hash(sha256, term_to_binary(Canonical, [deterministic])),
        {ok, Canonical#{fingerprint => Fingerprint}}
    catch
        error:{fts_capacity_exceeded, _, _, _} = Reason -> {error, Reason};
        _:_ -> {error, invalid_fts_schema}
    end;
schema(_Definition) ->
    {error, invalid_fts_schema}.

client_rd_mode(false) -> 0;
client_rd_mode(true) -> 1;
client_rd_mode(Mode) -> Mode.

-spec derive(map(), binary(), term()) -> {ok, [leveled_codec:object_spec()]}.
derive(#{fingerprint := Fingerprint} = Schema, DocKey, Object)
when is_binary(DocKey), is_binary(Fingerprint) ->
    client_guard(doc_key_bytes, byte_size(DocKey), maps:get(doc_key_bytes, capacities())),
    Fields = extract_fields(maybe_decode_object(Object, Schema), maps:get(column_specs, Schema)),
    ColTerms = build_column_terms(Fields, maps:get(options, Schema)),
    {ByShard, DocLength} = client_group_postings(ColTerms, Schema),
    Touched = lists:sort(maps:keys(ByShard)),
    Bucket = maps:get(index, Schema),
    %% content-derived version stamp: pure, idempotent (an identical
    %% reindex is version-stable), shared by every row of this batch
    DocVersion =
        binary:part(
            erlang:md5(term_to_binary({ByShard, DocLength})), 0, 8
        ),
    PostingSpecs = [
        {add, Bucket, client_shard_key(Shard), client_doc_subkey(DocKey),
            client_encode_posting({DocVersion, maps:get(Shard, ByShard)})}
     || Shard <- Touched
    ],
    Manifest =
        client_encode_manifest(DocVersion, Touched, DocLength, Fingerprint),
    EpochSpecs = [client_epoch_spec(Bucket, Shard) || Shard <- Touched],
    {ok,
        PostingSpecs ++
            [
                {add, Bucket, <<"doc">>, DocKey, Manifest},
                {add, Bucket, <<"stats">>, DocKey,
                    client_encode_stats(DocLength)},
                client_docs_epoch_spec(Bucket)
            ] ++ EpochSpecs};
derive(_Schema, DocKey, _Object) ->
    erlang:error({invalid_fts_derive, DocKey}).

-spec remove(map(), binary(), binary() | map()) -> [leveled_codec:object_spec()].
remove(#{index := Bucket, fingerprint := Fingerprint}, DocKey, Manifest0)
when is_binary(DocKey) ->
    #{shards := Shards, fingerprint := Fingerprint} =
        client_decode_manifest_value(Manifest0),
    [
        {remove, Bucket, client_shard_key(Shard), client_doc_subkey(DocKey), <<>>}
     || Shard <- Shards
    ] ++
        [
            {remove, Bucket, <<"doc">>, DocKey, <<>>},
            {remove, Bucket, <<"stats">>, DocKey, <<>>},
            client_docs_epoch_spec(Bucket)
        ] ++
        [client_epoch_spec(Bucket, Shard) || Shard <- Shards].

-spec update(map(), binary(), term(), binary() | map()) ->
    {ok, [leveled_codec:object_spec()]}.
update(Schema, DocKey, Object, OldManifest) ->
    {ok, NewSpecs} = derive(Schema, DocKey, Object),
    {ok, client_dedupe_specs(remove(Schema, DocKey, OldManifest) ++ NewSpecs)}.

client_dedupe_specs(Specs) ->
    {_Seen, Kept} = lists:foldl(
        fun({_, B, K, SK, _} = Spec, {Seen, Acc}) ->
            Id = {B, K, SK},
            case sets:is_element(Id, Seen) of
                true -> {Seen, Acc};
                false -> {sets:add_element(Id, Seen), [Spec | Acc]}
            end
        end,
        {sets:new(), []},
        lists:reverse(Specs)
    ),
    Kept.

client_group_postings(ColTerms, Schema) ->
    lists:foldl(
        fun({ColId, {_Column, TokenPositions}}, {ShardAcc, LengthAcc}) ->
            client_guard(column_id, ColId, maps:get(column_id, capacities())),
            lists:foldl(
                fun({Token, Positions}, {SA, LA}) ->
                    client_guard(token_bytes, byte_size(Token), maps:get(token_bytes, capacities())),
                    Count = length(Positions),
                    client_guard(true_occurrences, Count, maps:get(true_occurrences, capacities())),
                    Shard = client_shard_id(Token, maps:get(shards, Schema)),
                    ByCol = maps:get(Shard, SA, #{}),
                    ByToken = maps:get(ColId, ByCol, #{}),
                    Entry = #{count => Count, positions => Positions},
                    {SA#{Shard => ByCol#{ColId => ByToken#{Token => Entry}}}, LA + Count}
                end,
                {ShardAcc, LengthAcc},
                TokenPositions
            )
        end,
        {#{}, 0},
        lists:zip(lists:seq(0, length(ColTerms) - 1), ColTerms)
    ).

client_doc_subkey(DocKey) ->
    <<"d:", DocKey/binary>>.

client_shard_key(Shard) ->
    client_guard(shard_id, Shard, ?MAX_U16),
    <<Shard:16/unsigned-big>>.

client_shard_id(Token, Shards) ->
    Raw = case Token of
        <<>> -> 0;
        <<B1:8>> -> B1 bsl 8;
        <<B1:8, B2:8, _/binary>> -> (B1 bsl 8) bor B2
    end,
    (Raw * Shards) bsr 16.

client_epoch_spec(Bucket, Shard) ->
    {add, Bucket, client_shard_key(Shard), <<"epoch">>, <<1>>}.

%% Index-level epoch row rewritten by EVERY doc write/remove: its SQN is
%% the validity token for the cached manifest map (docs/FTS.md §4 - the
%% same admission law as shard states, one plane up). Eliminates the
%% per-matched-doc manifest point-read at query time.
client_docs_epoch_spec(Bucket) ->
    {add, Bucket, <<"docs_epoch">>, <<"epoch">>, <<1>>}.

client_docs_epoch_sqn(Bookie, Schema) ->
    leveled_bookie:book_sqn(
        Bookie, maps:get(index, Schema), {<<"docs_epoch">>, <<"epoch">>},
        ?HEAD_TAG
    ).

%% Cached manifest map: DocKey => compact {Version, DocLength} for docs
%% matching the schema fingerprint. Serve/install only with a token
%% equal to the CURRENT docs-epoch SQN; a raced fill is discarded.
client_cached_manifests(Bookie, Schema, Table) ->
    Epoch0 = client_docs_epoch_sqn(Bookie, Schema),
    case ets:lookup(Table, manifests) of
        [{manifests, {Epoch0, Map}}] ->
            Map;
        _ ->
            client_fill_manifests(Bookie, Schema, Table, Epoch0)
    end.

client_fill_manifests(Bookie, Schema, Table, Epoch0) ->
    Map = client_fold_manifests(Bookie, Schema),
    Epoch1 = client_docs_epoch_sqn(Bookie, Schema),
    case Epoch1 =:= Epoch0 of
        true ->
            true = ets:insert(Table, {manifests, {Epoch1, Map}}),
            Map;
        false ->
            client_fill_manifests(Bookie, Schema, Table, Epoch1)
    end.

client_guard(_What, Value, Max) when is_integer(Value), Value >= 0, Value =< Max ->
    ok;
client_guard(What, Value, Max) ->
    erlang:error({fts_capacity_exceeded, What, Value, Max}).

%% Posting rows carry the DOC VERSION STAMP (docs/FTS.md §2): an 8-byte
%% content hash shared by every row of one derive batch. The search
%% merge admits a shard's contribution for a doc ONLY when its stamp
%% equals the current manifest's stamp, so a multi-shard query can
%% never assemble postings from two different document versions
%% (cross-shard read skew) into a false match.
client_encode_posting({DocVersion, ByColumn}) when byte_size(DocVersion) == 8 ->
    Columns = lists:sort(maps:to_list(ByColumn)),
    client_guard(columns, length(Columns), ?MAX_COLUMNS),
    Body = iolist_to_binary([client_encode_column(C, T) || {C, T} <- Columns]),
    <<?POSTING_VERSION:8, DocVersion:8/binary, (length(Columns)):8, Body/binary>>.

client_encode_column(ColumnId, ByToken) ->
    client_guard(column_id, ColumnId, ?MAX_COLUMN_ID),
    Tokens = lists:sort(maps:to_list(ByToken)),
    client_guard(tokens_per_column, length(Tokens), ?MAX_U32),
    Body = iolist_to_binary([
        client_encode_token(Token, Entry) || {Token, Entry} <- Tokens
    ]),
    <<ColumnId:8, (length(Tokens)):32/unsigned-big, Body/binary>>.

client_encode_token(Token, #{count := Count, positions := Positions}) ->
    TokenBytes = byte_size(Token),
    client_guard(token_bytes, TokenBytes, ?MAX_TOKEN_BYTES),
    client_guard(true_occurrences, Count, ?MAX_U64),
    PosBin = client_encode_positions(Positions),
    PosBytes = byte_size(PosBin),
    client_guard(position_bytes, PosBytes, ?MAX_POSITION_BYTES),
    <<TokenBytes:16/unsigned-big, Token/binary, Count:64/unsigned-big,
        PosBytes:16/unsigned-big, PosBin/binary>>.

client_encode_positions(Positions) ->
    client_encode_positions(Positions, 0, <<>>).

client_encode_positions([], _Last, Acc) ->
    Acc;
client_encode_positions(_Positions, _Last, Acc)
when byte_size(Acc) >= ?MAX_POSITION_PREFIX_BYTES ->
    Acc;
client_encode_positions([Position | Rest], Last, Acc)
when is_integer(Position), Position >= Last ->
    client_encode_positions(Rest, Position, varint_append(Position - Last, Acc));
client_encode_positions(Bad, _Last, _Acc) ->
    erlang:error({invalid_fts_positions, Bad}).

client_decode_posting(
    <<?POSTING_VERSION:8, DocVersion:8/binary, NCols:8, Rest/binary>>
) ->
    {DocVersion, client_decode_columns(NCols, Rest, #{})};
client_decode_posting(Bad) ->
    erlang:error({invalid_fts_posting, Bad}).

client_decode_columns(0, <<>>, Acc) -> Acc;
client_decode_columns(N, <<ColId:8, NTokens:32/unsigned-big, Rest/binary>>, Acc)
when N > 0 ->
    {ByToken, Tail} = client_decode_tokens(NTokens, Rest, #{}),
    client_decode_columns(N - 1, Tail, Acc#{ColId => ByToken});
client_decode_columns(_N, Bad, _Acc) ->
    erlang:error({invalid_fts_posting_columns, Bad}).

client_decode_tokens(0, Rest, Acc) -> {Acc, Rest};
client_decode_tokens(N,
    <<TokenBytes:16/unsigned-big, Token:TokenBytes/binary,
        Count:64/unsigned-big, PosBytes:16/unsigned-big,
        PosBin:PosBytes/binary, Rest/binary>>, Acc) when N > 0 ->
    Positions = case decode_positions(PosBin, 0, []) of
        {ok, Ps} -> Ps;
        error -> erlang:error({invalid_fts_positions, PosBin})
    end,
    client_decode_tokens(N - 1, Rest,
        Acc#{Token => #{count => Count, positions => Positions}});
client_decode_tokens(_N, Bad, _Acc) ->
    erlang:error({invalid_fts_posting_tokens, Bad}).

client_encode_manifest(DocVersion, Shards, DocLength, Fingerprint) when
    byte_size(DocVersion) == 8
->
    client_guard(manifest_shards, length(Shards), ?MAX_U16),
    client_guard(doc_length, DocLength, ?MAX_U64),
    32 = byte_size(Fingerprint),
    ShardBin = iolist_to_binary([client_encode_shard_id(S) || S <- Shards]),
    <<?MANIFEST_VERSION:8, DocVersion:8/binary,
        (length(Shards)):16/unsigned-big, ShardBin/binary,
        DocLength:64/unsigned-big, Fingerprint/binary>>.

client_decode_manifest_value(#{shards := _, doc_length := _, fingerprint := _} = M) -> M;
client_decode_manifest_value(
    <<?MANIFEST_VERSION:8, DocVersion:8/binary, N:16/unsigned-big,
        Rest/binary>>
) ->
    ShardBytes = N * 2,
    case Rest of
        <<ShardBin:ShardBytes/binary, DocLength:64/unsigned-big,
            Fingerprint:32/binary>> ->
            #{version => DocVersion,
                shards => [S || <<S:16/unsigned-big>> <= ShardBin],
                doc_length => DocLength, fingerprint => Fingerprint};
        _ -> erlang:error({invalid_fts_manifest, Rest})
    end;
client_decode_manifest_value(Bad) ->
    erlang:error({invalid_fts_manifest, Bad}).

client_encode_shard_id(Shard) ->
    client_guard(shard_id, Shard, ?MAX_U16),
    <<Shard:16/unsigned-big>>.

client_encode_stats(DocLength) ->
    client_guard(doc_length, DocLength, ?MAX_U64),
    <<?STATS_VERSION:8, DocLength:64/unsigned-big>>.

client_decode_stats(<<?STATS_VERSION:8, DocLength:64/unsigned-big>>) -> DocLength;
client_decode_stats(Bad) -> erlang:error({invalid_fts_stats, Bad}).

%% cache_table/2 returns a public ETS table owned by the process that won its
%% lazy creation.  The table (and the small registry) therefore lives exactly
%% as long as that owner.  Callers wanting cache lifetime independent of a
%% request should first call this function from their own supervisor process.
-spec cache_table(pid(), map()) -> ets:tid().
cache_table(Bookie, #{index := Index}) when is_pid(Bookie) ->
    Registry = client_cache_registry(),
    CacheKey = {Bookie, Index},
    case ets:lookup(Registry, CacheKey) of
        [{CacheKey, Table}] ->
            case ets:info(Table) of
                undefined -> client_new_cache(Registry, CacheKey);
                _ -> Table
            end;
        [] -> client_new_cache(Registry, CacheKey)
    end.

client_cache_registry() ->
    case ets:whereis(?CLIENT_CACHE_REGISTRY) of
        undefined ->
            try ets:new(?CLIENT_CACHE_REGISTRY,
                [named_table, public, set, {read_concurrency, true},
                    {write_concurrency, true}])
            catch error:badarg -> ets:whereis(?CLIENT_CACHE_REGISTRY)
            end;
        Table -> Table
    end.

client_new_cache(Registry, CacheKey) ->
    Table = ets:new(leveled_fts_cache,
        [public, set, {read_concurrency, true}, {write_concurrency, true}]),
    case ets:insert_new(Registry, {CacheKey, Table}) of
        true -> Table;
        false ->
            ets:delete(Table),
            [{CacheKey, Existing}] = ets:lookup(Registry, CacheKey),
            Existing
    end.

-spec search(pid(), map(), binary() | list() | all_docs, map() | list()) ->
    {ok, [map()]} | {error, term()}.
search(Bookie, #{fingerprint := _} = Schema, Query, Opts0) when is_pid(Bookie) ->
    try
        {Hook, Opts1} = client_take_option(cache_fill_hook, Opts0),
        case normalise_search_options(Opts1, Schema) of
            {ok, Opts} ->
                case parse(Query, Opts) of
                    {ok, AST0} ->
                        Columns = option_columns(Opts, Schema),
                        case validate_ast_columns(AST0, Columns) of
                            ok ->
                                AST = canonicalise_ast_columns(
                                    restrict_ast_columns(AST0, Columns)
                                ),
                                client_search(Bookie, Schema, AST, Opts, Hook);
                            Error -> Error
                        end;
                    Error -> Error
                end;
            Error -> Error
        end
    catch
        error:Reason -> {error, Reason};
        throw:{fts_error, Reason} -> {error, Reason}
    end.

client_take_option(Key, Opts) when is_map(Opts) ->
    {maps:get(Key, Opts, undefined), maps:remove(Key, Opts)};
client_take_option(Key, Opts) when is_list(Opts) ->
    {proplists:get_value(Key, Opts, undefined), proplists:delete(Key, Opts)}.

client_search(Bookie, Schema, {all_docs} = AST, Opts, _Hook) ->
    Table = cache_table(Bookie, Schema),
    Manifests = client_cached_manifests(Bookie, Schema, Table),
    Metas =
        maps:map(
            fun(Key, {_V, DocLength}) -> client_empty_meta(Key, DocLength) end,
            Manifests
        ),
    client_evaluate(AST, Metas, Bookie, Schema, Opts);
client_search(Bookie, Schema, AST, Opts, Hook) ->
    Shards = client_ast_shards(AST, Schema),
    Table = cache_table(Bookie, Schema),
    States = [client_cached_shard(Bookie, Schema, Table, Shard, Hook)
        || Shard <- Shards],
    Raw = lists:foldl(fun client_merge_shard_docs/2, #{}, States),
    %% Manifests come from the docs-epoch-validated cache (one book_sqn
    %% per query, never a per-doc point read). Version-stamp admission:
    %% only the contribution matching the CURRENT manifest merges, so
    %% shard states read at different instants can never assemble two
    %% document versions into one match.
    Manifests = client_cached_manifests(Bookie, Schema, Table),
    Metas = maps:fold(
        fun(DocKey, ByVersion, Acc) ->
            case maps:get(DocKey, Manifests, none) of
                {Version, DocLength} ->
                    case maps:get(Version, ByVersion, none) of
                        none ->
                            Acc;
                        Posting ->
                            Acc#{
                                DocKey =>
                                    client_meta(
                                        DocKey, DocLength, Posting, Schema
                                    )
                            }
                    end;
                none ->
                    Acc
            end
        end,
        #{}, Raw
    ),
    client_evaluate(AST, Metas, Bookie, Schema, Opts).

client_cached_shard(Bookie, Schema, Table, Shard, Hook) ->
    Epoch0 = client_epoch_sqn(Bookie, Schema, Shard),
    case ets:lookup(Table, {shard, Shard}) of
        [{{shard, Shard}, {Epoch0, State}}] -> State;
        _ -> client_fill_shard(Bookie, Schema, Table, Shard, Epoch0, Hook)
    end.

client_fill_shard(Bookie, Schema, Table, Shard, Epoch0, Hook) ->
    State = client_fold_shard(Bookie, Schema, Shard),
    client_call_hook(Hook, {Shard, Epoch0, State}),
    Epoch1 = client_epoch_sqn(Bookie, Schema, Shard),
    case Epoch1 =:= Epoch0 of
        true ->
            true = ets:insert(Table, {{shard, Shard}, {Epoch1, State}}),
            State;
        false ->
            %% The snapshot is stale.  It is neither installed nor served;
            %% refill once without the test/coordination hook.
            client_fill_shard(Bookie, Schema, Table, Shard, Epoch1, undefined)
    end.

client_epoch_sqn(Bookie, Schema, Shard) ->
    leveled_bookie:book_sqn(Bookie, maps:get(index, Schema),
        {client_shard_key(Shard), <<"epoch">>}, ?HEAD_TAG).

client_fold_shard(Bookie, Schema, Shard) ->
    Bucket = maps:get(index, Schema),
    ShardKey = client_shard_key(Shard),
    Fold = fun
        (B, {K, <<"base">>}, Value, {_Base, Docs, Keys})
        when B =:= Bucket, K =:= ShardKey ->
            {client_decode_base(Value), Docs, Keys};
        (B, {K, <<"d:", DocKey/binary>> = SubKey}, Value,
            {Base, Docs, Keys}) when B =:= Bucket, K =:= ShardKey ->
            {Base, Docs#{DocKey => client_decode_posting(Value)}, [SubKey | Keys]};
        (_B, _K, _V, Acc) -> Acc
    end,
    {async, Runner} = leveled_bookie:book_headfold(Bookie, ?HEAD_TAG,
        {range, Bucket, {{ShardKey, <<>>}, {ShardKey, <<255>>}}},
        {Fold, {#{}, #{}, []}}, false, true, false),
    {Base, Docs, _Keys} = Runner(),
    maps:merge(Base, Docs).

client_fold_shard_rows(Bookie, Schema, Shard) ->
    Bucket = maps:get(index, Schema),
    ShardKey = client_shard_key(Shard),
    Fold = fun
        (B, {K, <<"base">>}, Value, {_Base, Docs, Keys})
        when B =:= Bucket, K =:= ShardKey ->
            {client_decode_base(Value), Docs, Keys};
        (B, {K, <<"d:", DocKey/binary>> = SubKey}, Value,
            {Base, Docs, Keys}) when B =:= Bucket, K =:= ShardKey ->
            {Base, Docs#{DocKey => client_decode_posting(Value)}, [SubKey | Keys]};
        (_B, _K, _V, Acc) -> Acc
    end,
    {async, Runner} = leveled_bookie:book_headfold(Bookie, ?HEAD_TAG,
        {range, Bucket, {{ShardKey, <<>>}, {ShardKey, <<255>>}}},
        {Fold, {#{}, #{}, []}}, false, true, false),
    {Base, Docs, Keys} = Runner(),
    {maps:merge(Base, Docs), Keys}.

client_merge_shard_docs(State, Acc) ->
    maps:fold(fun(Key, {DocVersion, Posting}, A) ->
        ByVersion = maps:get(Key, A, #{}),
        Merged =
            client_merge_posting(
                maps:get(DocVersion, ByVersion, #{}), Posting
            ),
        A#{Key => ByVersion#{DocVersion => Merged}}
    end, Acc, State).

client_merge_posting(A, B) ->
    maps:fold(fun(Col, Tokens, Acc) ->
        Acc#{Col => maps:merge(maps:get(Col, Acc, #{}), Tokens)}
    end, A, B).

client_meta(Key, DocLength, Posting, Schema) ->
    Columns = maps:get(columns, Schema),
    Positions = maps:from_list([{lists:nth(ColId + 1, Columns),
        maps:map(fun(_Token, Entry) -> maps:get(positions, Entry) end, Tokens)}
        || {ColId, Tokens} <- maps:to_list(Posting)]),
    Counts = maps:from_list([{lists:nth(ColId + 1, Columns),
        maps:map(fun(_Token, Entry) -> maps:get(count, Entry) end, Tokens)}
        || {ColId, Tokens} <- maps:to_list(Posting)]),
    #{key => Key, doc_length => DocLength,
        positions => Positions, counts => Counts}.

client_empty_meta(Key, DocLength) ->
    #{key => Key, doc_length => DocLength,
        positions => #{}, counts => #{}}.

client_ast_shards(AST, Schema) ->
    lists:usort(lists:append([
        client_token_shards(Token, Prefix, maps:get(shards, Schema))
     || {Token, Prefix} <- client_ast_tokens(AST)
    ])).

client_ast_tokens({term, Token, Prefix, _Cols}) -> [{Token, Prefix}];
client_ast_tokens({phrase, Specs, _Cols}) -> [{T, P} || {T, P, _} <- Specs];
client_ast_tokens({near, Items, _D, _Cols}) ->
    lists:append([client_ast_tokens(I) || I <- Items]);
client_ast_tokens({anchor, A}) -> client_ast_tokens(A);
client_ast_tokens({'and', A, B}) -> client_ast_tokens(A) ++ client_ast_tokens(B);
client_ast_tokens({'or', A, B}) -> client_ast_tokens(A) ++ client_ast_tokens(B);
client_ast_tokens({'not', A, B}) -> client_ast_tokens(A) ++ client_ast_tokens(B);
client_ast_tokens(_) -> [].

client_token_shards(Token, false, Shards) -> [client_shard_id(Token, Shards)];
client_token_shards(<<>>, true, Shards) -> lists:seq(0, Shards - 1);
client_token_shards(Token, true, Shards) ->
    Lo = client_shard_id(Token, Shards),
    HiToken = case Token of
        <<B:8>> -> <<B, 255>>;
        <<B1:8, B2:8, _/binary>> -> <<B1, B2>>
    end,
    lists:seq(Lo, client_shard_id(HiToken, Shards)).

client_evaluate(AST, Metas, Bookie, Schema, Opts) ->
    Matches = [Meta || {_K, Meta} <- maps:to_list(Metas), eval(AST, Meta) =/= false],
    Ranked = maps:get(rank, Opts, none) =:= bm25,
    Hits0 = case Ranked of
        false -> [client_hit(AST, Meta, Opts, 0.0) || Meta <- Matches];
        true ->
            Stats = client_corpus_stats(Bookie, Schema),
            Leaves = scoring_phrases(AST),
            Np = np_map(Leaves, maps:values(Metas)),
            {DocCount, TotalLength} = Stats,
            Avg = case DocCount of 0 -> 0.0; _ -> TotalLength / DocCount end,
            [client_hit(AST, Meta, Opts,
                bm25_score(Meta, Leaves, Np, DocCount, Avg)) || Meta <- Matches]
    end,
    Hits1 = case Ranked of
        true -> lists:sort(fun(A, B) ->
            {-maps:get(score, A), maps:get(key, A)} =<
                {-maps:get(score, B), maps:get(key, B)}
        end, Hits0);
        false -> lists:sort(fun(A, B) -> maps:get(key, A) =< maps:get(key, B) end, Hits0)
    end,
    {ok, page_hits(Hits1, Opts)}.

client_hit(AST, Meta, Opts, Score) ->
    {true, MatchPositions} = eval(AST, Meta),
    Base = #{key => maps:get(key, Meta), score => Score,
        doc_length => maps:get(doc_length, Meta)},
    case maps:get(return_positions, Opts, false) of
        true ->
            case position_count(MatchPositions) =< ?MAX_RETURN_POSITIONS of
                true -> Base#{positions => MatchPositions};
                false -> throw({fts_error, fts_query_positions_limit_exceeded})
            end;
        false -> Base
    end.

client_fold_manifests(Bookie, Schema) ->
    Bucket = maps:get(index, Schema),
    Fingerprint = maps:get(fingerprint, Schema),
    Fold = fun
        (B, {<<"doc">>, DocKey}, Value, Acc) when B =:= Bucket ->
            M = client_decode_manifest_value(Value),
            case maps:get(fingerprint, M) =:= Fingerprint of
                true ->
                    Acc#{
                        DocKey =>
                            {maps:get(version, M), maps:get(doc_length, M)}
                    };
                false ->
                    Acc
            end;
        (_B, _K, _V, Acc) -> Acc
    end,
    {async, Runner} = leveled_bookie:book_headfold(Bookie, ?HEAD_TAG,
        {range, Bucket, all},
        {Fold, #{}}, false, true, false),
    Runner().

client_corpus_stats(Bookie, Schema) ->
    Bucket = maps:get(index, Schema),
    Fold = fun
        (B, {<<"stats">>, _DocKey}, Value, {N, L}) when B =:= Bucket ->
            {N + 1, L + client_decode_stats(Value)};
        (_B, _K, _V, Acc) -> Acc
    end,
    {async, Runner} = leveled_bookie:book_headfold(Bookie, ?HEAD_TAG,
        {range, Bucket, all},
        {Fold, {0, 0}}, false, true, false),
    Runner().

client_encode_base(Docs) ->
    Rows = lists:sort(maps:to_list(Docs)),
    client_guard(base_documents, length(Rows), ?MAX_U32),
    Body = iolist_to_binary([client_encode_base_doc(K, P) || {K, P} <- Rows]),
    <<?BASE_VERSION:8, (length(Rows)):32/unsigned-big, Body/binary>>.

client_encode_base_doc(DocKey, Posting) ->
    KeyBytes = byte_size(DocKey),
    client_guard(base_doc_key_bytes, KeyBytes, ?MAX_U16),
    PostingBin = client_encode_posting(Posting),
    PostingBytes = byte_size(PostingBin),
    client_guard(base_posting_bytes, PostingBytes, ?MAX_U32),
    <<KeyBytes:16/unsigned-big, DocKey/binary,
        PostingBytes:32/unsigned-big, PostingBin/binary>>.

client_decode_base(<<?BASE_VERSION:8, N:32/unsigned-big, Rest/binary>>) ->
    client_decode_base_docs(N, Rest, #{});
client_decode_base(Bad) -> erlang:error({invalid_fts_base, Bad}).

client_decode_base_docs(0, <<>>, Acc) -> Acc;
client_decode_base_docs(N,
    <<KeyBytes:16/unsigned-big, DocKey:KeyBytes/binary,
        PostingBytes:32/unsigned-big, Posting:PostingBytes/binary, Rest/binary>>, Acc)
when N > 0 ->
    client_decode_base_docs(N - 1, Rest,
        Acc#{DocKey => client_decode_posting(Posting)});
client_decode_base_docs(_N, Bad, _Acc) ->
    erlang:error({invalid_fts_base_rows, Bad}).

-spec consolidate(pid(), map(), map() | list()) -> {ok, map()} | {error, term()}.
consolidate(Bookie, #{fingerprint := _} = Schema, Opts0) when is_pid(Bookie) ->
    try
        {Hook, Opts} = client_take_option(before_consolidate_commit, Opts0),
        Shards = case client_option(shards, Opts, all) of
            all -> lists:seq(0, maps:get(shards, Schema) - 1);
            L when is_list(L) -> L
        end,
        Result = lists:foldl(fun(Shard, Acc) ->
            client_consolidate_shard(Bookie, Schema, Shard, Hook, Acc)
        end, #{consolidated => [], skipped => []}, Shards),
        {ok, maps:map(fun(_K, V) -> lists:reverse(V) end, Result)}
    catch error:Reason -> {error, Reason} end.

client_option(Key, Opts, Default) when is_map(Opts) -> maps:get(Key, Opts, Default);
client_option(Key, Opts, Default) when is_list(Opts) -> proplists:get_value(Key, Opts, Default).

client_consolidate_shard(Bookie, Schema, Shard, Hook, Acc) ->
    case client_epoch_sqn(Bookie, Schema, Shard) of
        not_found -> Acc;
        {ok, ObservedSQN} ->
            {Docs, DocRows} = client_fold_shard_rows(Bookie, Schema, Shard),
            case DocRows of
                [] -> Acc;
                _ ->
                    client_call_hook(Hook, {Shard, ObservedSQN}),
                    Bucket = maps:get(index, Schema),
                    ShardKey = client_shard_key(Shard),
                    Specs = [{add, Bucket, ShardKey, <<"base">>, client_encode_base(Docs)}] ++
                        [{remove, Bucket, ShardKey, SubKey, <<>>} || SubKey <- DocRows] ++
                        [client_epoch_spec(Bucket, Shard)],
                    Condition = [{Bucket, ShardKey, <<"epoch">>, {sqn, ObservedSQN}}],
                    case leveled_bookie:book_casmput(Bookie, Specs, Condition) of
                        ok -> Acc#{consolidated := [Shard | maps:get(consolidated, Acc)]};
                        pause -> Acc#{consolidated := [Shard | maps:get(consolidated, Acc)]};
                        {error, {precondition_failed, _}} ->
                            Acc#{skipped := [Shard | maps:get(skipped, Acc)]};
                        {error, Reason} -> erlang:error({fts_consolidation_failed, Reason})
                    end
            end
    end.

client_call_hook(undefined, _Arg) -> ok;
client_call_hook(Fun, Arg) when is_function(Fun, 1) -> Fun(Arg);
client_call_hook(Fun, _Arg) when is_function(Fun, 0) -> Fun().

%% Persisted page entries carry a 16-bit doc count.

normalise_column_specs(Columns) when is_list(Columns), Columns =/= [] ->
    try
        Specs = [normalise_column_spec(Column) || Column <- Columns],
        case duplicate_columns(Specs) of
            false -> {ok, Specs};
            true -> {error, invalid_fts_columns}
        end
    catch
        _:_ -> {error, invalid_fts_columns}
    end;
normalise_column_specs(_Columns) ->
    {error, invalid_fts_columns}.

%% A column spec may carry a mode: text (default -- tokenised through the
%% schema tokenizer) or verbatim (the field value posts as one exact token
%% at position 0; no tokenisation, case folding, or diacritic removal).
%% Verbatim columns exist for filter facts such as tenant ids, where
%% partial or case-folded matches would be wrong. Changing a column's mode
%% changes what is posted at write time, so it requires a reindex, exactly
%% like any other schema change.
normalise_column_spec(#{name := Name, path := Path} = Spec) when is_list(Path) ->
    {normalise_column(Name), Path, normalise_column_mode(maps:get(mode, Spec, text))};
normalise_column_spec({Name, Path, Mode}) when is_list(Path) ->
    {normalise_column(Name), Path, normalise_column_mode(Mode)};
normalise_column_spec({Name, Path}) when is_list(Path) ->
    {normalise_column(Name), Path, text};
normalise_column_spec(Name) ->
    Column = normalise_column(Name),
    {Column, [Name], text}.

normalise_column_mode(text) -> text;
normalise_column_mode(verbatim) -> verbatim;
normalise_column_mode(<<"text">>) -> text;
normalise_column_mode(<<"verbatim">>) -> verbatim.

duplicate_columns(Specs) ->
    Columns = [Column || {Column, _Path, _Mode} <- Specs],
    length(Columns) =/= length(lists:usort(Columns)).

extract_fields(Object, ColumnSpecs) ->
    [
        {Column, Mode, normalise_text(extract_path(Object, Path))}
     || {Column, Path, Mode} <- ColumnSpecs
    ].

%% Stores that journal externally-encoded terms (term_to_binary bodies)
%% declare decode => external_term so column paths address the decoded
%% term. Decoding happens only at write-time derivation; replay rebuilds
%% from stored key changes and never re-derives.
maybe_decode_object(Object, #{options := #{decode := external_term}}) when
    is_binary(Object)
->
    try
        binary_to_term(Object)
    catch
        _:_ -> Object
    end;
maybe_decode_object(Object, _Schema) ->
    Object.

extract_path(Value, []) ->
    Value;
extract_path(Map, [Key | Rest]) when is_map(Map) ->
    case maps:find(Key, Map) of
        {ok, Value} ->
            extract_path(Value, Rest);
        error ->
            AltKey = alternate_map_key(Key),
            case maps:find(AltKey, Map) of
                {ok, Value} -> extract_path(Value, Rest);
                error -> <<>>
            end
    end;
extract_path(Tuple, [N | Rest]) when is_tuple(Tuple), is_integer(N), N > 0, N =< tuple_size(Tuple) ->
    extract_path(element(N, Tuple), Rest);
extract_path(List, [N | Rest]) when is_list(List), is_integer(N), N > 0, N =< length(List) ->
    extract_path(lists:nth(N, List), Rest);
extract_path(_Value, _Path) ->
    <<>>.

alternate_map_key(Key) when is_atom(Key) ->
    atom_to_binary(Key, utf8);
alternate_map_key(Key) when is_binary(Key) ->
    try binary_to_existing_atom(Key, utf8) catch _:_ -> Key end;
alternate_map_key(Key) ->
    Key.


validate_schema_contract(Schema, Opts, Mode) ->
    case validate_schema_columns(Schema, Opts, Mode) of
        ok ->
            case validate_schema_prefixes(Schema, Opts, Mode) of
                ok -> validate_schema_tokenizer(Schema, Opts);
                Error -> Error
            end;
        Error ->
            Error
    end.

validate_schema_columns(#{columns := Existing}, #{columns := Columns0}, write) ->
    Columns = schema_columns(Columns0),
    case Columns =:= Existing of
        true -> ok;
        false -> {error, {invalid_fts_contract_change, columns, Existing, Columns}}
    end;
validate_schema_columns(#{columns := _Existing}, _Opts, write) ->
    ok;
validate_schema_columns(#{columns := Existing}, #{columns := Columns0}, search) ->
    Columns = schema_columns(Columns0),
    case first_unknown(Columns, Existing) of
        none -> ok;
        {unknown, Column} -> {error, {fts_parse, unknown_column, Column}}
    end;
validate_schema_columns(_Schema, _Opts, search) ->
    ok.

validate_schema_prefixes(#{prefixes := Existing}, #{prefixes := Prefixes0}, write) ->
    Prefixes = normalise_prefixes(Prefixes0),
    case Prefixes =:= [] orelse Prefixes =:= Existing of
        true -> ok;
        false -> {error, {invalid_fts_contract_change, prefixes, Existing, Prefixes}}
    end;
validate_schema_prefixes(_Schema, _Opts, write) ->
    ok;
validate_schema_prefixes(#{prefixes := Existing}, #{prefixes := Prefixes0}, search) ->
    Prefixes = normalise_prefixes(Prefixes0),
    case Prefixes =:= [] orelse lists:all(fun(P) -> lists:member(P, Existing) end, Prefixes) of
        true -> ok;
        false -> {error, {invalid_fts_contract_change, prefixes, Existing, Prefixes}}
    end;
validate_schema_prefixes(_Schema, _Opts, search) ->
    ok.

validate_schema_tokenizer(#{tokenizer := Existing}, Opts) ->
    Tokenizer = tokenizer_description(Opts),
    case Tokenizer =:= Existing of
        true -> ok;
        false -> {error, {invalid_fts_contract_change, tokenizer, Existing, Tokenizer}}
    end.

build_column_terms(Fields, Opts) ->
    [
        {Column, column_token_positions(Mode, Text, Opts)}
     || {Column, Mode, Text} <- Fields
    ].

%% Verbatim columns post the exact field value as one token at position 0;
%% an empty or over-frame value posts nothing. Text columns tokenise
%% through the schema tokenizer; tokens beyond the page format's 16-bit
%% length frame are dropped after position assignment: queries are capped
%% at ?MAX_QUERY_BYTES, so no exact query can ever name such a token, and
%% surviving tokens keep their positions so phrase and NEAR distances are
%% unaffected.
column_token_positions(verbatim, <<>>, _Opts) ->
    [];
column_token_positions(verbatim, Token, _Opts) when byte_size(Token) =< 65535 ->
    [{Token, [0]}];
column_token_positions(verbatim, _Oversized, _Opts) ->
    [];
column_token_positions(text, Text, Opts) ->
    group_positions([
        TP
     || {Token, _Pos} = TP <- tokenize(Text, Opts),
        byte_size(Token) =< 65535
    ]).

group_positions(Tokens) ->
    group_positions(Tokens, #{}).

group_positions([], Acc) ->
    %% Positions are appended in increasing order during tokenisation and
    %% prepended here, so each list is descending; a single reverse yields the
    %% ascending order encode_positions/1 expects (no per-token sort needed).
    [{Token, lists:reverse(Positions)} || {Token, Positions} <- maps:to_list(Acc)];
group_positions([{Token, Pos} | Rest], Acc) ->
    group_positions(Rest, maps:update_with(Token, fun(Ps) -> [Pos | Ps] end, [Pos], Acc)).

near_positions([LegA, LegB], Distance) ->
    [Start || {Start, _End} <- near_sweep(LegA, LegB, Distance)];
near_positions([First | Rest], Distance) ->
    [
        Start
     || {Start, _End} = Span <- First,
        near_position_matches([Span], Rest, Distance)
    ].

%% Emit every span of the FIRST list within Distance of some span of the
%% second. Both lists ascend by start (and by end — spans within a leg
%% share their phrase length), so each list is walked at most once.
near_sweep([], _B, _Distance) ->
    [];
near_sweep(_A, [], _Distance) ->
    [];
near_sweep([{SA, EA} = Span | RestA] = A, [{SB, EB} | RestB] = B, Distance) ->
    if
        %% b entirely too far left for this a — and every later a starts
        %% at or after SA, so b can never match again.
        EB < SA - Distance - 1 ->
            near_sweep(A, RestB, Distance);
        %% nearest surviving b is already too far right: no match for a.
        SB > EA + Distance + 1 ->
            near_sweep(RestA, B, Distance);
        true ->
            [Span | near_sweep(RestA, B, Distance)]
    end.

near_position_matches(_Chosen, [], _Distance) ->
    true;
near_position_matches(Chosen, [Spans | Rest], Distance) ->
    lists:any(
        fun(Span) ->
            span_matches_all(Span, Chosen, Distance) andalso
                near_position_matches([Span | Chosen], Rest, Distance)
        end,
        Spans
    ).

span_matches_all(Span, Chosen, Distance) ->
    lists:all(fun(ChosenSpan) -> span_distance(Span, ChosenSpan) =< Distance end, Chosen).

span_distance({_StartA, EndA}, {StartB, _EndB}) when EndA < StartB ->
    StartB - EndA - 1;
span_distance({StartA, _EndA}, {_StartB, EndB}) when EndB < StartA ->
    StartA - EndB - 1;
span_distance(_A, _B) ->
    0.

anchored_positions(Positions) ->
    lists:any(fun(P) -> P =:= 0 end, flatten_position_map(Positions)).

flatten_position_map(Map) when is_map(Map) ->
    lists:append([flatten_position_value(V) || {_K, V} <- maps:to_list(Map)]);
flatten_position_map(_Other) ->
    [].

flatten_position_value(V) when is_list(V) ->
    V;
flatten_position_value(V) when is_map(V) ->
    flatten_position_map(V);
flatten_position_value(_V) ->
    [].

column_positions(Meta, Column) ->
    maps:get(Column, maps:get(positions, Meta, #{}), #{}).

page_hits(Hits, Opts) ->
    Offset = maps:get(offset, Opts, 0),
    Limit = maps:get(limit, Opts, ?DEFAULT_LIMIT),
    lists:sublist(drop(Offset, Hits), Limit).

drop(0, List) ->
    List;
drop(_N, []) ->
    [];
drop(N, [_ | Rest]) when N > 0 ->
    drop(N - 1, Rest).

normalise_search_options(Opts0, Schema) ->
    case options_map(Opts0) of
        {ok, Opts0Map} ->
            SearchOpts0 = maps:merge(schema_tokenizer_options(Schema), Opts0Map),
            case validate_search_options(SearchOpts0) of
                {ok, Opts1} ->
                    case validate_schema_contract(Schema, Opts1, search) of
                        ok ->
                            Prefixes =
                                case maps:is_key(prefixes, Opts0Map) of
                                    true -> maps:get(prefixes, Opts1);
                                    false -> maps:get(prefixes, Schema)
                                end,
                            {ok, Opts1#{
                                columns => maps:get(columns, Opts1, maps:get(columns, Schema)),
                                prefixes => Prefixes
                            }};
                        {error, Reason} ->
                            {error, Reason}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        error ->
            {error, invalid_fts_options}
    end.

schema_tokenizer_options(Schema) ->
    maps:with(
        [tokenizer, remove_diacritics, tokenchars, separators, stopwords],
        maps:get(options, Schema, #{})
    ).

options_map(Opts) when is_map(Opts) ->
    {ok, Opts};
options_map(Opts) when is_list(Opts) ->
    try {ok, maps:from_list(Opts)} catch
        _:_ -> error
    end;
options_map(_Opts) ->
    error.

validate_search_options(Opts) ->
    case validate_search_option_list(maps:to_list(Opts)) of
        ok -> validate_search_window(normalise_options(Opts));
        {error, Reason} -> {error, Reason};
        error -> {error, invalid_fts_options}
    end.

validate_search_option_list([]) ->
    ok;
validate_search_option_list([{columns, Columns} | Rest]) ->
    case valid_columns(Columns) of true -> validate_search_option_list(Rest); false -> error end;
validate_search_option_list([{prefixes, Prefixes} | Rest]) ->
    case valid_prefixes(Prefixes) of true -> validate_search_option_list(Rest); false -> error end;
validate_search_option_list([{tokenizer, Tokenizer} | Rest]) ->
    case valid_tokenizer(Tokenizer) of true -> validate_search_option_list(Rest); false -> error end;
validate_search_option_list([{remove_diacritics, Value} | Rest]) ->
    case valid_remove_diacritics(Value) of true -> validate_search_option_list(Rest); false -> error end;
validate_search_option_list([{tokenchars, Value} | Rest]) ->
    case valid_char_option(Value) of true -> validate_search_option_list(Rest); false -> error end;
validate_search_option_list([{separators, Value} | Rest]) ->
    case valid_char_option(Value) of true -> validate_search_option_list(Rest); false -> error end;
validate_search_option_list([{stopwords, Words} | Rest]) when is_list(Words) ->
    validate_search_option_list(Rest);
validate_search_option_list([{rank, none} | Rest]) ->
    validate_search_option_list(Rest);
validate_search_option_list([{rank, bm25} | Rest]) ->
    validate_search_option_list(Rest);
validate_search_option_list([{rank, _Other} | _Rest]) ->
    {error, invalid_rank_option};
validate_search_option_list([{limit, Limit} | Rest]) when is_integer(Limit), Limit >= 0 ->
    validate_search_option_list(Rest);
validate_search_option_list([{offset, Offset} | Rest]) when is_integer(Offset), Offset >= 0 ->
    validate_search_option_list(Rest);
validate_search_option_list([{return_positions, Bool} | Rest]) when is_boolean(Bool) ->
    validate_search_option_list(Rest);
validate_search_option_list([{_Other, _Value} | _Rest]) ->
    error.

validate_search_window(Opts) ->
    Limit = maps:get(limit, Opts, ?DEFAULT_LIMIT),
    Offset = maps:get(offset, Opts, 0),
    case Limit =< ?MAX_LIMIT andalso Offset + Limit =< ?MAX_WINDOW of
        true -> {ok, Opts};
        false -> {error, fts_query_limit_exceeded}
    end.

normalise_options(Opts) ->
    Opts0 = Opts#{
        prefixes => normalise_prefixes(maps:get(prefixes, Opts, [])),
        tokenchars => normalise_char_list(maps:get(tokenchars, Opts, [])),
        separators => normalise_char_list(maps:get(separators, Opts, [])),
        remove_diacritics => maps:get(remove_diacritics, Opts, false),
        tokenizer => normalise_tokenizer(maps:get(tokenizer, Opts, unicode61))
    },
    Opts1 = Opts0#{stopwords => normalise_stopwords(maps:get(stopwords, Opts, []), Opts0)},
    case maps:find(columns, Opts1) of
        {ok, Columns} -> Opts1#{columns => schema_columns(Columns)};
        error -> Opts1
    end.

normalise_text(T) when is_binary(T) ->
    T;
normalise_text(T) when is_list(T) ->
    unicode:characters_to_binary(T, utf8);
normalise_text(T) ->
    client_term_binary(T).

valid_columns(Columns) when is_list(Columns), Columns =/= [] ->
    true;
valid_columns(_Columns) ->
    false.

valid_prefixes(Prefixes) when is_list(Prefixes) ->
    lists:all(fun(P) -> is_integer(P) andalso P > 0 end, Prefixes);
valid_prefixes(_Prefixes) ->
    false.

valid_tokenizer(unicode61) -> true;
valid_tokenizer(<<"unicode61">>) -> true;
valid_tokenizer("unicode61") -> true;
valid_tokenizer(_Tokenizer) -> false.

valid_remove_diacritics(false) -> true;
valid_remove_diacritics(true) -> true;
valid_remove_diacritics(0) -> true;
valid_remove_diacritics(1) -> true;
valid_remove_diacritics(2) -> true;
valid_remove_diacritics(_Other) -> false.

valid_char_option(Bin) when is_binary(Bin) -> true;
valid_char_option(C) when is_integer(C) -> true;
valid_char_option(List) when is_list(List) ->
    lists:all(fun valid_char_option/1, List);
valid_char_option(_Other) -> false.

normalise_prefixes(Prefixes) ->
    lists:usort([P || P <- Prefixes, is_integer(P), P > 0]).

normalise_char_list(Bin) when is_binary(Bin) ->
    unicode_chars(Bin);
normalise_char_list(C) when is_integer(C) ->
    [C];
normalise_char_list(List) when is_list(List) ->
    lists:append([normalise_char_list(Item) || Item <- List]);
normalise_char_list(_Other) ->
    [].

normalise_stopwords(Words, Opts) ->
    [Token || Word <- Words, {Token, _Pos} <- tokenize(Word, Opts#{stopwords => []})].

normalise_tokenizer(unicode61) -> unicode61;
normalise_tokenizer(<<"unicode61">>) -> unicode61;
normalise_tokenizer("unicode61") -> unicode61.

tokenizer_description(Opts) ->
    #{
        tokenizer => normalise_tokenizer(maps:get(tokenizer, Opts, unicode61)),
        remove_diacritics => maps:get(remove_diacritics, Opts, false),
        tokenchars => normalise_char_list(maps:get(tokenchars, Opts, [])),
        separators => normalise_char_list(maps:get(separators, Opts, [])),
        stopwords => maps:get(stopwords, Opts, [])
    }.

schema_columns(Columns) ->
    lists:usort([normalise_column(C) || C <- Columns]).

normalise_column(C) when is_binary(C) ->
    normalise_column_binary(C);
normalise_column(C) when is_atom(C) ->
    normalise_column_binary(atom_to_binary(C, utf8));
normalise_column(C) when is_list(C) ->
    normalise_column_binary(unicode:characters_to_binary(C, utf8));
normalise_column(C) ->
    normalise_column_binary(client_term_binary(C)).

normalise_column_binary(Bin) ->
    unicode:characters_to_binary(lower_chars(unicode_chars(Bin)), utf8).

normalise_index(I) when is_binary(I) -> I;
normalise_index(I) when is_atom(I) -> atom_to_binary(I, utf8);
normalise_index(I) when is_list(I) -> unicode:characters_to_binary(I, utf8);
normalise_index(I) -> client_term_binary(I).

client_term_binary(Term) ->
    iolist_to_binary(io_lib:format("~0p", [Term])).

first_unknown([], _Known) ->
    none;
first_unknown([Item | Rest], Known) ->
    case lists:member(Item, Known) of
        true -> first_unknown(Rest, Known);
        false -> {unknown, Item}
    end.

tokenize(Text0, Opts) ->
    Text = sqlite_utf8_compat(normalise_text(Text0)),
    case maps:get(tokenchars, Opts, []) =:= [] andalso maps:get(separators, Opts, []) =:= [] of
        true ->
            Stopwords = maps:get(stopwords, Opts, []),
            fast_tokens(Text, Opts, Stopwords, <<>>, false, 0, []);
        false ->
            tokenize_unicode(Text, Opts)
    end.

%% SQLite's UTF-8 reader has one long-standing compatibility quirk included in
%% the differential corpus: a truncated F0 9F 92 sequence is decoded as U+07D2
%% (DF 92) when the following byte is not a continuation.  Preserve that exact
%% behaviour; all other malformed bytes remain hard token boundaries.
sqlite_utf8_compat(Bin) when is_binary(Bin) ->
    sqlite_utf8_compat(Bin, <<>>).

sqlite_utf8_compat(<<16#F0, 16#9F, 16#92, Next, Rest/binary>>, Acc)
when Next band 16#C0 =/= 16#80 ->
    sqlite_utf8_compat(<<Next, Rest/binary>>, <<Acc/binary, 16#DF, 16#92>>);
sqlite_utf8_compat(<<Byte, Rest/binary>>, Acc) ->
    sqlite_utf8_compat(Rest, <<Acc/binary, Byte>>);
sqlite_utf8_compat(<<>>, Acc) ->
    Acc.

tokenize_unicode(Text0, Opts) ->
    %% Keep malformed input explicit.  Dropping a bad byte before this fold
    %% concatenates the valid runs on either side and changes token identity.
    Text = unicode_chars_with_boundaries(normalise_text(Text0)),
    Stopwords = maps:get(stopwords, Opts, []),
    {Tokens, Current, Pos} =
        lists:foldl(
            fun(invalid_utf8, {Acc, Current, Pos}) ->
                    finish_token(Acc, Current, Pos, Stopwords, Opts);
               (Char, {Acc, Current, Pos}) ->
                case token_char(Char, Opts) of
                    true -> {Acc, lists:reverse(normalise_char(Char, Opts)) ++ Current, Pos};
                    false -> finish_token(Acc, Current, Pos, Stopwords, Opts)
                end
            end,
            {[], [], 0},
            Text
        ),
    {Final, _Current2, _Pos2} = finish_token(Tokens, Current, Pos, Stopwords, Opts),
    lists:reverse(Final).

%% Fast tokenizer for the common case where no custom tokenchars/separators are
%% configured. Runs of ASCII alphanumerics are classified and lowercased with
%% byte comparisons only (no per-character Unicode table lookups). Non-ASCII
%% characters fall back to the Unicode classifier, and any token that contained
%% a non-ASCII byte is normalised through normalise_token/2, so output is
%% byte-identical to tokenize_unicode/2 (unicode61 + remove_diacritics parity
%% with SQLite FTS5).
fast_tokens(<<C, Rest/binary>>, Opts, SW, Tok, NA, Pos, Acc) when C >= $a, C =< $z ->
    fast_tokens(Rest, Opts, SW, <<Tok/binary, C>>, NA, Pos, Acc);
fast_tokens(<<C, Rest/binary>>, Opts, SW, Tok, NA, Pos, Acc) when C >= $0, C =< $9 ->
    fast_tokens(Rest, Opts, SW, <<Tok/binary, C>>, NA, Pos, Acc);
fast_tokens(<<C, Rest/binary>>, Opts, SW, Tok, NA, Pos, Acc) when C >= $A, C =< $Z ->
    fast_tokens(Rest, Opts, SW, <<Tok/binary, (C bor 16#20)>>, NA, Pos, Acc);
fast_tokens(<<C, Rest/binary>>, Opts, SW, Tok, NA, Pos, Acc) when C < 128 ->
    {Pos1, Acc1} = fast_flush(Tok, NA, Opts, SW, Pos, Acc),
    fast_tokens(Rest, Opts, SW, <<>>, false, Pos1, Acc1);
fast_tokens(<<CP/utf8, Rest/binary>> = Bin, Opts, SW, Tok, NA, Pos, Acc) ->
    case unicode_token_char(CP, Opts) of
        true ->
            CharLen = byte_size(Bin) - byte_size(Rest),
            <<Char:CharLen/binary, _/binary>> = Bin,
            fast_tokens(Rest, Opts, SW, <<Tok/binary, Char/binary>>, true, Pos, Acc);
        false ->
            {Pos1, Acc1} = fast_flush(Tok, NA, Opts, SW, Pos, Acc),
            fast_tokens(Rest, Opts, SW, <<>>, false, Pos1, Acc1)
    end;
fast_tokens(<<_Bad, Rest/binary>>, Opts, SW, Tok, NA, Pos, Acc) ->
    {Pos1, Acc1} = fast_flush(Tok, NA, Opts, SW, Pos, Acc),
    fast_tokens(Rest, Opts, SW, <<>>, false, Pos1, Acc1);
fast_tokens(<<>>, Opts, SW, Tok, NA, Pos, Acc) ->
    {_Pos1, Acc1} = fast_flush(Tok, NA, Opts, SW, Pos, Acc),
    lists:reverse(Acc1).

fast_flush(<<>>, _NA, _Opts, _SW, Pos, Acc) ->
    {Pos, Acc};
fast_flush(Tok, false, _Opts, SW, Pos, Acc) ->
    case lists:member(Tok, SW) of
        true -> {Pos + 1, Acc};
        false -> {Pos + 1, [{Tok, Pos} | Acc]}
    end;
fast_flush(Tok, true, Opts, SW, Pos, Acc) ->
    Norm = normalise_token(Tok, Opts),
    case {Norm =:= <<>>, lists:member(Norm, SW)} of
        {true, _} -> {Pos, Acc};
        {false, true} -> {Pos + 1, Acc};
        {false, false} -> {Pos + 1, [{Norm, Pos} | Acc]}
    end.

finish_token(Acc, [], Pos, _Stopwords, _Opts) ->
    {Acc, [], Pos};
finish_token(Acc, Current, Pos, Stopwords, Opts) ->
    Token = normalise_token(unicode:characters_to_binary(lists:reverse(Current), utf8), Opts),
    case {Token =:= <<>>, lists:member(Token, Stopwords)} of
        {true, _} -> {Acc, [], Pos};
        {false, true} -> {Acc, [], Pos + 1};
        {false, false} -> {[{Token, Pos} | Acc], [], Pos + 1}
    end.

token_char(Char, Opts) ->
    TokenChars = maps:get(tokenchars, Opts, []),
    Separators = maps:get(separators, Opts, []),
    (unicode_token_char(Char, Opts) orelse lists:member(Char, TokenChars)) andalso
        not lists:member(Char, Separators).

unicode_token_char(Char, Opts) ->
    case fts5_token_char(Char) of
        true -> true;
        false -> remove_diacritics_enabled(Opts) andalso sqlite_diacritic_mark(Char)
    end.

%% Generated from SQLite ext/fts5/fts5_unicode2.c by replaying
%% sqlite3Fts5UnicodeCategory over every codepoint with the unicode61
%% default categories "L* N* Co" PLUS category 0: fts5's CatParse sets
%% aArray[0]=1 unconditionally, so codepoints the data table does not
%% cover — including the deliberately under-encoded CJK Unified/Ext and
%% Hangul Syllable mega-blocks, unassigned gaps, and everything at or
%% above 2^20 — are token characters in FTS5. Classification therefore
%% matches SQLite FTS5 unicode61 exactly and does not depend on the OTP
%% unicode_util data version.
fts5_token_ranges() ->
    {
        48, 57, 65, 90, 97, 122, 170, 170, 178, 179, 181, 181,
        185, 186, 188, 190, 192, 214, 216, 246, 248, 705, 710, 721,
        736, 740, 748, 748, 750, 750, 880, 884, 886, 893, 895, 899,
        902, 902, 904, 1013, 1015, 1153, 1162, 1369, 1376, 1416, 1419, 1422,
        1424, 1424, 1480, 1522, 1525, 1535, 1541, 1541, 1564, 1565, 1568, 1610,
        1632, 1641, 1646, 1647, 1649, 1747, 1749, 1749, 1765, 1766, 1774, 1788,
        1791, 1791, 1806, 1806, 1808, 1808, 1810, 1839, 1867, 1957, 1969, 2026,
        2036, 2037, 2042, 2069, 2074, 2074, 2084, 2084, 2088, 2088, 2094, 2095,
        2111, 2136, 2140, 2141, 2143, 2275, 2303, 2303, 2308, 2361, 2365, 2365,
        2384, 2384, 2392, 2401, 2406, 2415, 2417, 2432, 2436, 2491, 2493, 2493,
        2501, 2502, 2505, 2506, 2510, 2518, 2520, 2529, 2532, 2545, 2548, 2553,
        2556, 2560, 2564, 2619, 2621, 2621, 2627, 2630, 2633, 2634, 2638, 2640,
        2642, 2671, 2674, 2676, 2678, 2688, 2692, 2747, 2749, 2749, 2758, 2758,
        2762, 2762, 2766, 2785, 2788, 2799, 2802, 2816, 2820, 2875, 2877, 2877,
        2885, 2886, 2889, 2890, 2894, 2901, 2904, 2913, 2916, 2927, 2929, 2945,
        2947, 3005, 3011, 3013, 3017, 3017, 3022, 3030, 3032, 3058, 3067, 3072,
        3076, 3133, 3141, 3141, 3145, 3145, 3150, 3156, 3159, 3169, 3172, 3198,
        3200, 3201, 3204, 3259, 3261, 3261, 3269, 3269, 3273, 3273, 3278, 3284,
        3287, 3297, 3300, 3329, 3332, 3389, 3397, 3397, 3401, 3401, 3406, 3414,
        3416, 3425, 3428, 3448, 3450, 3457, 3460, 3529, 3531, 3534, 3541, 3541,
        3543, 3543, 3552, 3569, 3573, 3632, 3634, 3635, 3643, 3646, 3648, 3654,
        3664, 3673, 3676, 3760, 3762, 3763, 3770, 3770, 3773, 3783, 3790, 3840,
        3872, 3891, 3904, 3952, 3976, 3980, 3992, 3992, 4029, 4029, 4045, 4045,
        4059, 4138, 4159, 4169, 4176, 4181, 4186, 4189, 4193, 4193, 4197, 4198,
        4206, 4208, 4213, 4225, 4238, 4238, 4240, 4249, 4256, 4346, 4348, 4956,
        4969, 5007, 5018, 5119, 5121, 5740, 5743, 5759, 5761, 5786, 5789, 5866,
        5870, 5905, 5909, 5937, 5943, 5969, 5972, 6001, 6004, 6067, 6103, 6103,
        6108, 6108, 6110, 6143, 6159, 6312, 6314, 6431, 6444, 6447, 6460, 6463,
        6465, 6467, 6470, 6575, 6593, 6599, 6602, 6621, 6656, 6678, 6684, 6685,
        6688, 6740, 6751, 6751, 6781, 6782, 6784, 6815, 6823, 6823, 6830, 6911,
        6917, 6963, 6981, 7001, 7037, 7039, 7043, 7072, 7086, 7141, 7156, 7163,
        7168, 7203, 7224, 7226, 7232, 7293, 7296, 7359, 7368, 7375, 7401, 7404,
        7406, 7409, 7413, 7615, 7655, 7675, 7680, 8124, 8126, 8126, 8130, 8140,
        8144, 8156, 8160, 8172, 8176, 8188, 8191, 8191, 8293, 8297, 8304, 8313,
        8319, 8329, 8335, 8351, 8378, 8399, 8433, 8447, 8450, 8450, 8455, 8455,
        8458, 8467, 8469, 8469, 8473, 8477, 8484, 8484, 8486, 8486, 8488, 8488,
        8490, 8493, 8495, 8505, 8508, 8511, 8517, 8521, 8526, 8526, 8528, 8591,
        9204, 9215, 9255, 9279, 9291, 9371, 9450, 9471, 9984, 9984, 10102, 10131,
        11085, 11087, 11098, 11492, 11499, 11502, 11506, 11512, 11517, 11517, 11520, 11631,
        11633, 11646, 11648, 11743, 11823, 11823, 11836, 11903, 11930, 11930, 12020, 12031,
        12246, 12271, 12284, 12287, 12293, 12295, 12321, 12329, 12337, 12341, 12344, 12348,
        12352, 12440, 12445, 12447, 12449, 12538, 12540, 12687, 12690, 12693, 12704, 12735,
        12772, 12799, 12831, 12841, 12872, 12879, 12881, 12895, 12928, 12937, 12977, 12991,
        13055, 13055, 13312, 19903, 19968, 42127, 42183, 42237, 42240, 42508, 42512, 42606,
        42623, 42654, 42656, 42735, 42744, 42751, 42775, 42783, 42786, 42888, 42891, 43009,
        43011, 43013, 43015, 43018, 43020, 43042, 43052, 43061, 43066, 43123, 43128, 43135,
        43138, 43187, 43205, 43213, 43216, 43231, 43250, 43255, 43259, 43301, 43312, 43334,
        43348, 43358, 43360, 43391, 43396, 43442, 43470, 43485, 43488, 43560, 43575, 43586,
        43588, 43595, 43598, 43611, 43616, 43638, 43642, 43642, 43644, 43695, 43697, 43697,
        43701, 43702, 43705, 43709, 43712, 43712, 43714, 43741, 43744, 43754, 43762, 43764,
        43767, 44002, 44014, 55295, 55297, 56190, 56193, 56318, 56321, 57342, 57344, 64285,
        64287, 64296, 64298, 64433, 64450, 64829, 64832, 65019, 65022, 65023, 65050, 65055,
        65063, 65071, 65107, 65107, 65127, 65127, 65132, 65278, 65280, 65280, 65296, 65305,
        65313, 65338, 65345, 65370, 65382, 65503, 65511, 65511, 65519, 65528, 65534, 65791,
        65795, 65846, 65856, 65912, 65930, 65935, 65948, 65999, 66046, 66462, 66464, 66511,
        66513, 67670, 67672, 67870, 67872, 67902, 67904, 68096, 68100, 68100, 68103, 68107,
        68112, 68151, 68155, 68158, 68160, 68175, 68185, 68222, 68224, 68408, 68416, 69631,
        69635, 69687, 69710, 69759, 69763, 69807, 69826, 69887, 69891, 69926, 69941, 69951,
        69956, 70015, 70019, 70066, 70081, 70084, 70089, 71338, 71352, 74863, 74868, 94032,
        94079, 94094, 94099, 118783, 119030, 119039, 119079, 119080, 119262, 119295, 119366, 119551,
        119639, 120512, 120514, 120538, 120540, 120570, 120572, 120596, 120598, 120628, 120630, 120654,
        120656, 120686, 120688, 120712, 120714, 120744, 120746, 120770, 120772, 126703, 126706, 126975,
        127020, 127023, 127124, 127135, 127151, 127152, 127167, 127168, 127184, 127184, 127200, 127247,
        127279, 127279, 127340, 127343, 127387, 127461, 127491, 127503, 127547, 127551, 127561, 127567,
        127570, 127743, 127777, 127791, 127798, 127798, 127869, 127871, 127892, 127903, 127941, 127941,
        127947, 127967, 127985, 127999, 128063, 128063, 128065, 128065, 128248, 128248, 128253, 128255,
        128318, 128319, 128324, 128335, 128360, 128506, 128577, 128580, 128592, 128639, 128710, 128767,
        128884, 917504, 917506, 917535, 917632, 917759, 918000, 1114111
    }.

fts5_token_char(Char) when is_integer(Char), Char >= 0 ->
    Ranges = fts5_token_ranges(),
    fts5_token_char(Char, Ranges, 1, tuple_size(Ranges) div 2).

fts5_token_char(_Char, _Ranges, Lo, Hi) when Lo > Hi ->
    false;
fts5_token_char(Char, Ranges, Lo, Hi) ->
    Mid = (Lo + Hi) div 2,
    Start = element(2 * Mid - 1, Ranges),
    End = element(2 * Mid, Ranges),
    if
        Char < Start -> fts5_token_char(Char, Ranges, Lo, Mid - 1);
        Char > End -> fts5_token_char(Char, Ranges, Mid + 1, Hi);
        true -> true
    end.


normalise_char(Char, _Opts) ->
    lower_chars([Char]).

normalise_token(Token0, Opts) ->
    Lower = unicode:characters_to_binary(lower_chars(unicode_chars(Token0)), utf8),
    case maps:get(remove_diacritics, Opts, false) of
        false -> Lower;
        0 -> Lower;
        true -> strip_diacritics(Lower, 1);
        1 -> strip_diacritics(Lower, 1);
        2 -> strip_diacritics(Lower, 2)
    end.

lower_chars(Chars) ->
    [client_simple_fold(Char) || Char <- lists:flatten(
        [unicode_util:lowercase([C]) || C <- lists:flatten(Chars)]
    )].

%% SQLite's Unicode-6.1 simple fold places both Greek sigma forms in the same
%% equivalence class.  OTP's context-sensitive lowercase retains final sigma.
client_simple_fold(16#03C2) -> 16#03C3;
client_simple_fold(Char) -> Char.

%% NFD exposes each character's combining marks so the SQLite diacritic
%% mask can drop them; recomposing with NFC afterwards restores every
%% decomposition the mask did not consume (Hangul syllables decompose to
%% Jamo under NFD, and FTS5 — which never decomposes — keeps them
%% precomposed; marks outside the mask likewise recompose back).
strip_diacritics(Token, Mode) ->
    Kept = lists:append([
        sqlite_fold_diacritic(Char, Mode)
     || Char <- lists:flatten(unicode_chars(Token))
    ]),
    unicode:characters_to_nfc_binary(Kept).

%% unicode61 drops combining marks from decomposed input, but its precomposed
%% fold table is Latin-only.  Mode 1 deliberately leaves a Latin codepoint
%% carrying multiple combining marks unchanged; mode 2 folds it to its ASCII
%% base.  Testing the NFD base reproduces the generated SQLite table without
%% applying the incorrect Greek/Cyrillic-wide NFD transform.
sqlite_fold_diacritic(Char, _Mode) when Char >= 16#0300, Char =< 16#036F ->
    case sqlite_diacritic_mark(Char) of true -> []; false -> [Char] end;
sqlite_fold_diacritic(Char, Mode) ->
    Decomposed = lists:flatten(unicode_util:nfd([Char])),
    case Decomposed of
        [Base | Marks] when
            (Base >= $a andalso Base =< $z) orelse
                (Base >= $A andalso Base =< $Z)
        ->
            Removable = [M || M <- Marks, sqlite_diacritic_mark(M)],
            case {Mode, length(Removable), length(Removable) =:= length(Marks)} of
                {_Any, 0, _} -> [Char];
                {1, N, true} when N > 1 -> [Char];
                {_Any, _N, true} -> [Base];
                {_Any, _N, false} ->
                    [Base | [M || M <- Marks, not sqlite_diacritic_mark(M)]]
            end;
        _ -> [Char]
    end.

remove_diacritics_enabled(Opts) ->
    case maps:get(remove_diacritics, Opts, false) of
        false -> false;
        0 -> false;
        _ -> true
    end.

sqlite_diacritic_mark(Char) when Char >= 16#0300, Char =< 16#0331 ->
    Offset = Char - 16#0300,
    {Mask, Bit} =
        case Offset < 32 of
            true -> {16#08029FDF, Offset};
            false -> {16#000361F8, Offset - 32}
        end,
    (Mask band (1 bsl Bit)) =/= 0;
sqlite_diacritic_mark(_Char) ->
    false.

parse(all_docs, _Opts) ->
    {ok, {all_docs}};
parse(Query0, Opts) ->
    case bounded_query(Query0) of
        {ok, Query} ->
            case blank(Query) of
                true -> {error, {fts_parse, empty_query}};
                false ->
                    case lex(Query, Opts) of
                        {ok, Tokens0} ->
                            case length(Tokens0) =< ?MAX_QUERY_TOKENS of
                                true ->
                                    Tokens = resolve_near(Tokens0),
                                    parse_tokens(Tokens, Opts);
                                false ->
                                    {error, fts_query_too_many_tokens}
                            end;
                        Error ->
                            Error
                    end
            end;
        Error ->
            Error
    end.

parse_tokens([], _Opts) ->
    {ok, {empty}};
parse_tokens(Tokens, Opts) ->
    case parse_or(Tokens, Opts) of
        {ok, AST, []} -> validate_ast_caps(ungroup(AST));
        {ok, _AST, Rest} -> {error, {fts_parse, trailing_tokens, Rest}};
        Error -> Error
    end.

bounded_query(Query) when is_binary(Query) ->
    case byte_size(Query) =< ?MAX_QUERY_BYTES of
        true -> {ok, Query};
        false -> {error, fts_query_too_large}
    end;
bounded_query(Query) when is_list(Query) ->
    try unicode:characters_to_binary(Query, utf8) of
        Bin when byte_size(Bin) =< ?MAX_QUERY_BYTES -> {ok, Bin};
        _ -> {error, fts_query_too_large}
    catch
        _:_ -> {error, invalid_fts_query}
    end;
bounded_query(Query) ->
    bounded_query(client_term_binary(Query)).

blank(Query) ->
    lists:all(fun(C) -> lists:member(C, " \t\r\n") end, unicode_chars(Query)).

lex(Query, Opts) ->
    lex_chars(unicode_chars(Query), Opts, [], true).

lex_chars([], _Opts, Acc, _AfterSpace) -> {ok, lists:reverse(Acc)};
lex_chars([C | Rest], Opts, Acc, _AfterSpace) when C == 32; C == 9; C == 10; C == 13 ->
    lex_chars(Rest, Opts, Acc, true);
lex_chars([$( | Rest], Opts, Acc, _AfterSpace) -> lex_chars(Rest, Opts, [lparen | Acc], false);
lex_chars([$) | Rest], Opts, Acc, _AfterSpace) -> lex_chars(Rest, Opts, [rparen | Acc], false);
lex_chars([$: | Rest], Opts, Acc, _AfterSpace) -> lex_chars(Rest, Opts, [colon | Acc], false);
lex_chars([$* | Rest], Opts, Acc, _AfterSpace) -> lex_chars(Rest, Opts, [star | Acc], false);
lex_chars([$+ | Rest], Opts, Acc, _AfterSpace) -> lex_chars(Rest, Opts, [plus | Acc], false);
lex_chars([$, | Rest], Opts, Acc, _AfterSpace) -> lex_chars(Rest, Opts, [comma | Acc], false);
lex_chars([${ | Rest], Opts, Acc, _AfterSpace) -> lex_chars(Rest, Opts, [lbrace | Acc], false);
lex_chars([$} | Rest], Opts, Acc, _AfterSpace) -> lex_chars(Rest, Opts, [rbrace | Acc], false);
lex_chars([$- | Rest], Opts, Acc, _AfterSpace) -> lex_chars(Rest, Opts, [minus | Acc], false);
lex_chars([$^ | Rest], Opts, Acc, _AfterSpace) -> lex_chars(Rest, Opts, [caret | Acc], false);
lex_chars([$" | Rest], Opts, Acc, _AfterSpace) ->
    case collect_quote(Rest, []) of
        {ok, Phrase, Rest2} ->
            lex_chars(Rest2, Opts, [
                {phrase, unicode:characters_to_binary(Phrase, utf8)} | Acc
            ], false);
        error -> {error, {fts_parse, unterminated_quote}}
    end;
lex_chars([C | Rest], Opts, Acc, AfterSpace) ->
    case token_char(C, Opts) of
        true ->
            {Chars, Rest2} = collect_word(Rest, Opts, [C]),
            Word = unicode:characters_to_binary(lists:reverse(Chars), utf8),
            case classify_word(Word, Opts) of
                skip -> lex_chars(Rest2, Opts, Acc, false);
                Token -> lex_chars(Rest2, Opts, [Token | Acc], false)
            end;
        false ->
            case separator_concat(Rest, Opts, Acc, AfterSpace) of
                true -> lex_chars(Rest, Opts, [plus | Acc], false);
                false -> lex_chars(Rest, Opts, Acc, false)
            end
    end.

separator_concat([Next | _Rest], Opts, [Token | _Acc], false) ->
    lexical_token(Token) andalso token_char(Next, Opts);
separator_concat(_Rest, _Opts, _Acc, _AfterSpace) ->
    false.

lexical_token({word, _Token}) -> true;
lexical_token({phrase, _Text}) -> true;
lexical_token({phrase_tokens, _Tokens}) -> true;
lexical_token(_Other) -> false.

collect_quote([], _Acc) -> error;
collect_quote([$" | Rest], Acc) -> {ok, lists:reverse(Acc), Rest};
collect_quote([C | Rest], Acc) -> collect_quote(Rest, [C | Acc]).

collect_word([], _Opts, Acc) -> {Acc, []};
collect_word([C | Rest], Opts, Acc) ->
    case token_char(C, Opts) of
        true -> collect_word(Rest, Opts, [C | Acc]);
        false -> {Acc, [C | Rest]}
    end.

classify_word(<<"AND">>, _Opts) -> 'and';
classify_word(<<"OR">>, _Opts) -> 'or';
classify_word(<<"NOT">>, _Opts) -> 'not';
classify_word(<<"NEAR">>, _Opts) -> near_candidate;
classify_word(Word, Opts) ->
    case tokenize(Word, Opts) of
        [] -> skip;
        [{Token, _}] -> {word, Token};
        Tokens -> {phrase_tokens, [Token || {Token, _} <- Tokens]}
    end.

resolve_near([near_candidate, lparen | Rest]) -> [near, lparen | resolve_near(Rest)];
resolve_near([near_candidate | Rest]) -> [{word, <<"near">>} | resolve_near(Rest)];
resolve_near([T | Rest]) -> [T | resolve_near(Rest)];
resolve_near([]) -> [].

parse_or(Tokens, Opts) ->
    case parse_and(Tokens, Opts) of
        {ok, Left, Rest} -> parse_or_tail(Left, Rest, Opts);
        Error -> Error
    end.

parse_or_tail(Left, ['or' | Rest], Opts) ->
    case parse_and(Rest, Opts) of
        {ok, Right, Rest2} -> parse_or_tail({'or', ungroup(Left), ungroup(Right)}, Rest2, Opts);
        Error -> Error
    end;
parse_or_tail(Left, Rest, _Opts) -> {ok, Left, Rest}.

parse_and(Tokens, Opts) ->
    case parse_not(Tokens, Opts) of
        {ok, Left, Rest} -> parse_and_tail(Left, Rest, Opts);
        Error -> Error
    end.

parse_and_tail(Left, ['and' | Rest], Opts) ->
    case parse_not(Rest, Opts) of
        {ok, Right, Rest2} -> parse_and_tail({'and', ungroup(Left), ungroup(Right)}, Rest2, Opts);
        Error -> Error
    end;
parse_and_tail(Left, Rest, _Opts) -> {ok, Left, Rest}.

parse_not(Tokens, Opts) ->
    case parse_implicit(Tokens, Opts) of
        {ok, Left, Rest} -> parse_not_tail(Left, Rest, Opts);
        Error -> Error
    end.

parse_not_tail(Left, ['not' | Rest], Opts) ->
    case parse_implicit(Rest, Opts) of
        {ok, Right, Rest2} -> parse_not_tail({'not', ungroup(Left), ungroup(Right)}, Rest2, Opts);
        Error -> Error
    end;
parse_not_tail(Left, Rest, _Opts) -> {ok, Left, Rest}.

parse_implicit(Tokens, Opts) ->
    case parse_primary(Tokens, Opts) of
        {ok, Left, Rest} -> parse_implicit_tail(Left, Rest, Opts);
        Error -> Error
    end.

parse_implicit_tail(Left, Rest, Opts) ->
    case starts_primary(Rest) of
        true ->
            case is_group(Left) orelse starts_group(Rest) of
                true ->
                    {error, {fts_parse, invalid_group_adjacency}};
                false ->
                    case parse_primary(Rest, Opts) of
                        {ok, Right, Rest2} ->
                            parse_implicit_tail({'and', ungroup(Left), ungroup(Right)}, Rest2,
                                Opts);
                        Error ->
                            Error
                    end
            end;
        false ->
            {ok, Left, Rest}
    end.

parse_primary([], _Opts) -> {error, {fts_parse, unexpected_end}};
parse_primary(Tokens, Opts) ->
    case parse_primary_base(Tokens, Opts) of
        {ok, AST, Rest} -> parse_concat_tail(AST, Rest, Opts);
        Error -> Error
    end.

parse_primary_base([], _Opts) -> {error, {fts_parse, unexpected_end}};
parse_primary_base([near, lparen | Rest], Opts) -> parse_near(Rest, Opts);
parse_primary_base([caret, {word, _Column}, colon | _Rest], _Opts) ->
    {error, {fts_parse, invalid_anchor}};
parse_primary_base([caret, near, lparen | _Rest], _Opts) ->
    {error, {fts_parse, invalid_anchor}};
parse_primary_base([caret | Rest], Opts) ->
    case parse_primary_base(Rest, Opts) of
        {ok, AST, Rest2} -> {ok, {anchor, ungroup(AST)}, Rest2};
        Error -> Error
    end;
parse_primary_base([{word, Column}, colon | Rest], Opts) ->
    parse_column_primary([normalise_column(Column)], Rest, Opts);
parse_primary_base([minus, {word, Column}, colon | Rest], Opts) ->
    parse_column_primary({not_columns, [normalise_column(Column)]}, Rest, Opts);
parse_primary_base([lbrace | Rest], Opts) ->
    case collect_columns(Rest, []) of
        {ok, Columns, [colon | Rest2]} -> parse_column_primary(Columns, Rest2, Opts);
        Error -> Error
    end;
parse_primary_base([minus, lbrace | Rest], Opts) ->
    case collect_columns(Rest, []) of
        {ok, Columns, [colon | Rest2]} -> parse_column_primary({not_columns, Columns}, Rest2,
            Opts);
        Error -> Error
    end;
parse_primary_base([lparen | Rest], Opts) ->
    case parse_or(Rest, Opts) of
        {ok, AST, [rparen | Rest2]} -> {ok, {group, AST}, Rest2};
        {ok, _AST, Other} -> {error, {fts_parse, expected_rparen, Other}};
        Error -> Error
    end;
parse_primary_base([{phrase, Text}, star | Rest], Opts) ->
    {ok, phrase_ast(Text, Opts, true), Rest};
parse_primary_base([{phrase, Text} | Rest], Opts) ->
    {ok, phrase_ast(Text, Opts, false), Rest};
parse_primary_base([{phrase_tokens, Tokens}, star | Rest], _Opts) ->
    {ok, phrase_tokens_ast(Tokens, true), Rest};
parse_primary_base([{phrase_tokens, Tokens} | Rest], _Opts) ->
    {ok, phrase_tokens_ast(Tokens, false), Rest};
parse_primary_base([{word, Token}, star | Rest], _Opts) ->
    {ok, {term, Token, true, all}, Rest};
parse_primary_base([{word, Token} | Rest], _Opts) ->
    {ok, {term, Token, false, all}, Rest};
parse_primary_base([Other | _Rest], _Opts) ->
    {error, {fts_parse, unexpected_token, Other}}.

parse_column_primary(Columns, Rest, Opts) ->
    case parse_primary(Rest, Opts) of
        {ok, AST, Rest2} -> {ok, restrict_ast_columns(ungroup(AST), Columns), Rest2};
        Error -> Error
    end.

collect_columns([rbrace | _Rest], []) -> {error, {fts_parse, empty_column_list}};
collect_columns([rbrace | Rest], Acc) -> {ok, lists:reverse(Acc), Rest};
collect_columns([{word, Column} | Rest], Acc) ->
    collect_columns(Rest, [normalise_column(Column) | Acc]);
collect_columns([{phrase, Column} | Rest], Acc) ->
    collect_columns(Rest, [normalise_column(Column) | Acc]);
collect_columns(Other, _Acc) -> {error, {fts_parse, invalid_column_list, Other}}.

parse_concat_tail(Left, [plus | Rest], Opts) ->
    case parse_primary_base(Rest, Opts) of
        {ok, Right, Rest2} ->
            case concat_phrase(ungroup(Left), ungroup(Right)) of
                {ok, AST} -> parse_concat_tail(AST, Rest2, Opts);
                Error -> Error
            end;
        Error ->
            Error
    end;
parse_concat_tail(Left, Rest, _Opts) ->
    {ok, Left, Rest}.

parse_near(Tokens, Opts) ->
    case parse_near_items(Tokens, Opts, []) of
        {ok, Items, Distance, Rest} -> {ok, {near, Items, Distance, all}, Rest};
        Error -> Error
    end.

parse_near_items([rparen | Rest], _Opts, []) ->
    {error, {fts_parse, empty_near, Rest}};
parse_near_items([rparen | Rest], _Opts, Acc) ->
    {ok, lists:reverse(Acc), ?DEFAULT_NEAR, Rest};
parse_near_items([comma, {word, NBin}, rparen | Rest], _Opts, Acc) ->
    try binary_to_integer(NBin) of
        N when N >= 0 -> {ok, lists:reverse(Acc), N, Rest};
        _ -> {error, {fts_parse, invalid_near_distance, NBin}}
    catch
        _:_ -> {error, {fts_parse, invalid_near_distance, NBin}}
    end;
parse_near_items(Tokens, Opts, Acc) ->
    case parse_primary_base(Tokens, Opts) of
        {ok, Item, Rest} ->
            %% SQLite FTS5 rejects NEAR nested inside NEAR; match that.
            case ungroup(Item) of
                {near, _Items, _Distance, _Columns} ->
                    {error, {fts_parse, invalid_near_nesting}};
                Item1 ->
                    parse_near_items(Rest, Opts, [Item1 | Acc])
            end;
        Error ->
            Error
    end.

phrase_ast(Text, Opts, PrefixLast) ->
    phrase_position_ast(tokenize(Text, Opts), PrefixLast).

phrase_position_ast(Tokens, PrefixLast) ->
    LastPos =
        case Tokens of
            [] -> -1;
            _ -> lists:max([Pos || {_Token, Pos} <- Tokens])
        end,
    Specs =
        [
            {Token, PrefixLast andalso Pos =:= LastPos, Pos}
         || {Token, Pos} <- Tokens
        ],
    {phrase, Specs, all}.

phrase_tokens_ast(Tokens, PrefixLast) ->
    Specs =
        [
            {Token, PrefixLast andalso N =:= length(Tokens) - 1, N}
         || {Token, N} <- lists:zip(Tokens, lists:seq(0, max(0, length(Tokens) - 1)))
        ],
    {phrase, Specs, all}.

concat_phrase({term, Token, Prefix, Columns}, Right) ->
    concat_phrase({phrase, [{Token, Prefix, 0}], Columns}, Right);
concat_phrase({phrase, LeftSpecs, LeftColumns}, {term, Token, Prefix, RightColumns}) ->
    Offset = length(LeftSpecs),
    {ok, {phrase, LeftSpecs ++ [{Token, Prefix, Offset}],
        concat_columns(LeftColumns, RightColumns)}};
concat_phrase({phrase, LeftSpecs, LeftColumns}, {phrase, RightSpecs, RightColumns}) ->
    Offset = length(LeftSpecs),
    {ok, {phrase, LeftSpecs ++ [{Token, Prefix, Offset + Pos}
        || {Token, Prefix, Pos} <- RightSpecs],
        concat_columns(LeftColumns, RightColumns)}};
concat_phrase(_Left, _Right) ->
    {error, {fts_parse, invalid_phrase_concat}}.

concat_columns(all, Columns) ->
    Columns;
concat_columns(Columns, all) ->
    Columns;
concat_columns({not_columns, A}, {not_columns, B}) ->
    {not_columns, lists:usort(schema_columns(A) ++ schema_columns(B))};
concat_columns({not_columns, Excluded}, Columns) ->
    lists:subtract(schema_columns(Columns), schema_columns(Excluded));
concat_columns(Columns, {not_columns, Excluded}) ->
    lists:subtract(schema_columns(Columns), schema_columns(Excluded));
concat_columns(A, B) ->
    [Column || Column <- schema_columns(A), lists:member(Column, schema_columns(B))].

starts_primary([{word, _} | _]) -> true;
starts_primary([{phrase, _} | _]) -> true;
starts_primary([{phrase_tokens, _} | _]) -> true;
starts_primary([near, lparen | _]) -> true;
starts_primary([caret | _]) -> true;
starts_primary([lparen | _]) -> true;
starts_primary([lbrace | _]) -> true;
starts_primary([minus, {word, _}, colon | _]) -> true;
starts_primary([minus, lbrace | _]) -> true;
starts_primary(_Other) -> false.

ungroup({group, AST}) -> ungroup(AST);
ungroup(AST) -> AST.

is_group({group, _AST}) -> true;
is_group(_AST) -> false.

starts_group([lparen | _Rest]) -> true;
starts_group(_Rest) -> false.

validate_ast_caps(AST) ->
    case ast_depth(AST) =< ?MAX_AST_DEPTH of
        true ->
            case validate_near_distance(AST) of
                ok ->
                    case validate_prefix_bytes(AST) of
                        ok -> {ok, AST};
                        Error -> Error
                    end;
                Error ->
                    Error
            end;
        false ->
            {error, fts_query_ast_too_deep}
    end.

ast_depth({'and', A, B}) -> 1 + max(ast_depth(A), ast_depth(B));
ast_depth({'or', A, B}) -> 1 + max(ast_depth(A), ast_depth(B));
ast_depth({'not', A, B}) -> 1 + max(ast_depth(A), ast_depth(B));
ast_depth({anchor, A}) -> 1 + ast_depth(A);
ast_depth({near, Items, _Distance, _Columns}) ->
    1 + lists:max([0 | [ast_depth(I) || I <- Items]]);
ast_depth(_Other) -> 1.

validate_near_distance({near, Items, Distance, _Columns}) when
    is_integer(Distance), Distance >= 0, Distance =< ?MAX_NEAR_DISTANCE
->
    validate_ast_list(Items, fun validate_near_distance/1);
validate_near_distance({near, _Items, _Distance, _Columns}) ->
    {error, fts_query_near_distance_exceeded};
validate_near_distance(AST) ->
    validate_children(AST, fun validate_near_distance/1).

validate_prefix_bytes({term, Token, true, _Columns}) when byte_size(Token) > ?MAX_PREFIX_BYTES ->
    {error, fts_query_prefix_too_large};
validate_prefix_bytes({phrase, Specs, _Columns}) ->
    case lists:any(fun({Token, true, _Pos}) -> byte_size(Token) > ?MAX_PREFIX_BYTES; (_) -> false
    end, Specs) of
        true -> {error, fts_query_prefix_too_large};
        false -> ok
    end;
validate_prefix_bytes(AST) ->
    validate_children(AST, fun validate_prefix_bytes/1).

validate_children({'and', A, B}, Fun) -> validate_pair(A, B, Fun);
validate_children({'or', A, B}, Fun) -> validate_pair(A, B, Fun);
validate_children({'not', A, B}, Fun) -> validate_pair(A, B, Fun);
validate_children({anchor, A}, Fun) -> Fun(A);
validate_children({near, Items, _Distance, _Columns}, Fun) -> validate_ast_list(Items, Fun);
validate_children(_Other, _Fun) -> ok.

validate_pair(A, B, Fun) ->
    case Fun(A) of
        ok -> Fun(B);
        Error -> Error
    end.

validate_ast_list([], _Fun) -> ok;
validate_ast_list([Item | Rest], Fun) ->
    case Fun(Item) of
        ok -> validate_ast_list(Rest, Fun);
        Error -> Error
    end.

option_columns(Opts, Schema) ->
    schema_columns(maps:get(columns, Opts, maps:get(columns, Schema))).

concrete_columns(all) ->
    [];
concrete_columns({not_columns, _Columns}) ->
    [];
concrete_columns(Columns) ->
    Columns.

validate_ast_columns(AST, Columns) ->
    case [Column || Column <- ast_columns(AST), not lists:member(Column, Columns)] of
        [] -> ok;
        [Unknown | _Rest] -> {error, {fts_parse, unknown_column, Unknown}}
    end.

ast_columns({term, _T, _P, Columns}) -> selector_columns(Columns);
ast_columns({phrase, _Specs, Columns}) -> selector_columns(Columns);
ast_columns({near, Items, _Distance, Columns}) ->
    lists:usort(selector_columns(Columns) ++ lists:append([ast_columns(Item) || Item <- Items]));
ast_columns({anchor, AST}) -> ast_columns(AST);
ast_columns({'and', A, B}) -> lists:usort(ast_columns(A) ++ ast_columns(B));
ast_columns({'or', A, B}) -> lists:usort(ast_columns(A) ++ ast_columns(B));
ast_columns({'not', A, B}) -> lists:usort(ast_columns(A) ++ ast_columns(B));
ast_columns(_Other) -> [].

selector_columns(all) -> [];
selector_columns({not_columns, Columns}) -> schema_columns(Columns);
selector_columns(Columns) -> schema_columns(Columns).

restrict_ast_columns(AST, all) ->
    AST;
restrict_ast_columns(_AST, []) ->
    {empty};
restrict_ast_columns({term, T, P, Cols0}, Cols) ->
    case combine_columns(Cols0, Cols) of [] -> {empty}; Cols1 -> {term, T, P, Cols1} end;
restrict_ast_columns({phrase, Specs, Cols0}, Cols) ->
    case combine_columns(Cols0, Cols) of [] -> {empty}; Cols1 -> {phrase, Specs, Cols1} end;
restrict_ast_columns({near, Items, Distance, Cols0}, Cols) ->
    case combine_columns(Cols0, Cols) of
        [] -> {empty};
        Cols1 -> {near, [restrict_ast_columns(I, Cols1) || I <- Items], Distance, Cols1}
    end;
restrict_ast_columns({anchor, AST}, Cols) ->
    case restrict_ast_columns(AST, Cols) of {empty} -> {empty}; AST1 -> {anchor, AST1} end;
restrict_ast_columns({'and', A, B}, Cols) ->
    case {restrict_ast_columns(A, Cols), restrict_ast_columns(B, Cols)} of
        {{empty}, _} -> {empty};
        {_, {empty}} -> {empty};
        {A1, B1} -> {'and', A1, B1}
    end;
restrict_ast_columns({'or', A, B}, Cols) ->
    {'or', restrict_ast_columns(A, Cols), restrict_ast_columns(B, Cols)};
restrict_ast_columns({'not', A, B}, Cols) ->
    {'not', restrict_ast_columns(A, Cols), restrict_ast_columns(B, Cols)};
restrict_ast_columns(Other, _Cols) ->
    Other.

combine_columns(all, Cols) -> normalise_column_selector(Cols);
combine_columns(Cols, all) -> normalise_column_selector(Cols);
combine_columns({not_columns, ExcludedA}, {not_columns, ExcludedB}) ->
    %% Nested negative selectors compose by union: the term must avoid
    %% every excluded column from both levels.
    {not_columns,
        lists:usort(
            schema_columns(ExcludedA) ++ schema_columns(ExcludedB)
        )};
combine_columns({not_columns, Excluded}, Cols) ->
    lists:subtract(normalise_column_selector(Cols), normalise_column_selector(Excluded));
combine_columns(Cols, {not_columns, Excluded}) ->
    lists:subtract(normalise_column_selector(Cols), normalise_column_selector(Excluded));
combine_columns(A, B) ->
    [C || C <- normalise_column_selector(A), lists:member(C, normalise_column_selector(B))].

normalise_column_selector({not_columns, Columns}) ->
    {not_columns, schema_columns(Columns)};
normalise_column_selector(Columns) ->
    schema_columns(Columns).

binary_prefix(Bin, Prefix) when is_binary(Bin), is_binary(Prefix), byte_size(Bin) >= byte_size(Prefix) ->
    binary:part(Bin, 0, byte_size(Prefix)) =:= Prefix;
binary_prefix(_Bin, _Prefix) ->
    false.

unicode_chars(Bin) when is_binary(Bin) ->
    case unicode:characters_to_list(Bin, utf8) of
        {error, Good, <<_Bad, Rest/binary>>} ->
            Good ++ unicode_chars(Rest);
        {error, Good, _Rest} ->
            Good;
        {incomplete, Good, _Rest} ->
            Good;
        Chars ->
            Chars
    end;
unicode_chars(List) when is_list(List) ->
    case unicode:characters_to_list(List, utf8) of
        {error, Good, [_Bad | Rest]} ->
            Good ++ unicode_chars(Rest);
        {error, Good, _Rest} ->
            Good;
        {incomplete, Good, _Rest} ->
            Good;
        Chars ->
            Chars
    end.

unicode_chars_with_boundaries(Bin) when is_binary(Bin) ->
    case unicode:characters_to_list(Bin, utf8) of
        {error, Good, <<_Bad, Rest/binary>>} ->
            Good ++ [invalid_utf8 | unicode_chars_with_boundaries(Rest)];
        {error, Good, _Rest} ->
            Good ++ [invalid_utf8];
        {incomplete, Good, <<>>} ->
            Good;
        {incomplete, Good, _Rest} ->
            Good ++ [invalid_utf8];
        Chars ->
            Chars
    end.


scoring_phrases({term, _Token, _Prefix, _Cols} = Leaf) -> [Leaf];
scoring_phrases({phrase, _Specs, _Cols} = Leaf) -> [Leaf];
scoring_phrases({near, Items, Distance, Cols}) ->
    [{near_member, Index, Items, Distance, Cols} || Index <- lists:seq(1, length(Items))];
scoring_phrases({anchor, AST}) -> scoring_phrases(AST);
scoring_phrases({'and', A, B}) -> scoring_phrases(A) ++ scoring_phrases(B);
scoring_phrases({'or', A, B}) -> scoring_phrases(A) ++ scoring_phrases(B);
scoring_phrases({'not', A, _B}) -> scoring_phrases(A);
scoring_phrases(_Other) -> [].

leaf_tf(Meta, {term, Token, Prefix, Cols}) ->
    client_term_frequency(Meta, Token, Prefix, Cols);
leaf_tf(Meta, {phrase, Specs, Cols}) ->
    length(phrase_match_positions(Meta, Specs, Cols));
leaf_tf(Meta, {near_member, Index, Items, Distance, Cols}) ->
    near_member_tf(Meta, Items, Index, Distance, Cols, filtered).

%% New client postings persist uncapped occurrence counts separately from the
%% capped positional payload.  Old in-memory metas (used only by legacy private
%% helpers retained below) have no counts and retain their former behaviour.
client_term_frequency(#{counts := Counts}, Token, Prefix, Cols) ->
    lists:sum([
        lists:sum([
            Count
         || {StoredToken, Count} <- maps:to_list(maps:get(Column, Counts, #{})),
            (Prefix andalso binary_prefix(StoredToken, Token)) orelse
                (not Prefix andalso StoredToken =:= Token)
        ])
     || Column <- concrete_columns(Cols)
    ]);
client_term_frequency(Meta, Token, Prefix, Cols) ->
    length(term_positions(Meta, Token, Prefix, Cols)).

%% Document frequency counts for a leaf use the member's STANDALONE
%% matches (FTS5's xQueryPhrase runs each phrase alone for nHit), while
%% per-document tf for NEAR members is NEAR-filtered above.
leaf_df_tf(Meta, {near_member, Index, Items, Distance, Cols}) ->
    near_member_tf(Meta, Items, Index, Distance, Cols, standalone);
leaf_df_tf(Meta, Leaf) ->
    leaf_tf(Meta, Leaf).

near_member_tf(Meta, Items, Index, Distance, Cols, Mode) ->
    Member = lists:nth(Index, Items),
    lists:sum([
        length(near_member_column_spans(Meta, Items, Index, Member, Distance, Column, Mode))
     || Column <- concrete_columns(Cols)
    ]).

near_member_column_spans(Meta, _Items, _Index, Member, _Distance, Column, standalone) ->
    item_spans_in_column(Meta, Member, Column);
near_member_column_spans(Meta, Items, Index, _Member, Distance, Column, filtered) ->
    SpanLists = [item_spans_in_column(Meta, Item, Column) || Item <- Items],
    case lists:any(fun(Spans) -> Spans =:= [] end, SpanLists) of
        true ->
            [];
        false ->
            {Before, [MemberSpans | After]} = lists:split(Index - 1, SpanLists),
            case Before ++ After of
                [OtherSpans] ->
                    near_sweep(MemberSpans, OtherSpans, Distance);
                Others ->
                    [
                        Span
                     || Span <- MemberSpans,
                        near_position_matches([Span], Others, Distance)
                    ]
            end
    end.

np_map(Leaves, MetaList) ->
    lists:foldl(
        fun(Leaf, Acc) ->
            case maps:is_key(Leaf, Acc) of
                true ->
                    Acc;
                false ->
                    N = length([ok || Meta <- MetaList, leaf_df_tf(Meta, Leaf) > 0]),
                    Acc#{Leaf => N}
            end
        end,
        #{},
        Leaves
    ).

bm25_score(Meta, Leaves, Np, DocCount, AvgDl) ->
    K1 = 1.2,
    B = 0.75,
    Dl = maps:get(doc_length, Meta, 0),
    lists:foldl(
        fun(Leaf, Acc) ->
            Tf = leaf_tf(Meta, Leaf),
            case Tf > 0 of
                false ->
                    Acc;
                true ->
                    NHit = maps:get(Leaf, Np),
                    Idf0 = math:log((DocCount - NHit + 0.5) / (NHit + 0.5)),
                    Idf =
                        case Idf0 > 0.0 of
                            true -> Idf0;
                            false -> 1.0e-6
                        end,
                    LenRatio =
                        case AvgDl > 0.0 of
                            true -> Dl / AvgDl;
                            false -> 1.0
                        end,
                    Acc +
                        Idf * (Tf * (K1 + 1)) /
                            (Tf + K1 * (1 - B + B * LenRatio))
            end
        end,
        0.0,
        Leaves
    ).


decode_positions(<<>>, _Last, Acc) ->
    {ok, lists:reverse(Acc)};
decode_positions(Bin, Last, Acc) ->
    case decode_varint(Bin) of
        {ok, Delta, Rest} ->
            Pos = Last + Delta,
            decode_positions(Rest, Pos, [Pos | Acc]);
        error ->
            error
    end.

varint_append(N, Acc) when N < 128 ->
    <<Acc/binary, N:8>>;
varint_append(N, Acc) ->
    varint_append(N bsr 7, <<Acc/binary, (16#80 bor (N band 16#7F)):8>>).

decode_varint(Bin) ->
    decode_varint(Bin, 0, 0).

decode_varint(<<Byte:8, Rest/binary>>, Shift, Acc) when Shift =< 63 ->
    Value = Acc bor ((Byte band 16#7F) bsl Shift),
    case Byte band 16#80 of
        0 -> {ok, Value, Rest};
        _ -> decode_varint(Rest, Shift + 7, Value)
    end;
decode_varint(_Bin, _Shift, _Acc) ->
    error.

position_count(Value) when is_map(Value) ->
    lists:sum([position_count(V) || {_K, V} <- maps:to_list(Value)]);
position_count(Value) when is_list(Value) ->
    length(Value);
position_count(_Value) ->
    0.

eval({empty}, _Meta) ->
    false;
eval({all_docs}, Meta) ->
    {true, maps:get(positions, Meta, #{})};
eval({term, Token, Prefix, Columns}, Meta) ->
    case term_positions(Meta, Token, Prefix, Columns) of
        [] -> false;
        Positions -> {true, #{Token => Positions}}
    end;
eval({phrase, Specs, Columns}, Meta) ->
    case phrase_match_positions(Meta, Specs, Columns) of
        [] -> false;
        Positions -> {true, #{phrase => Positions}}
    end;
eval({near, Items, Distance, Columns}, Meta) ->
    case near_match_positions(Meta, Items, Distance, Columns) of
        [] -> false;
        Positions -> {true, #{near => Positions}}
    end;
eval({anchor, AST}, Meta) ->
    case eval(AST, Meta) of
        {true, Positions} ->
            case anchored_positions(Positions) of
                true -> {true, Positions};
                false -> false
            end;
        false ->
            false
    end;
eval({'and', A, B}, Meta) ->
    case {eval(A, Meta), eval(B, Meta)} of
        {{true, PosA}, {true, PosB}} -> {true, maps:merge(PosA, PosB)};
        _ -> false
    end;
eval({'or', A, B}, Meta) ->
    case {eval(A, Meta), eval(B, Meta)} of
        {{true, PosA}, {true, PosB}} -> {true, maps:merge(PosA, PosB)};
        {{true, PosA}, false} -> {true, PosA};
        {false, {true, PosB}} -> {true, PosB};
        _ -> false
    end;
eval({'not', A, B}, Meta) ->
    case eval(A, Meta) of
        {true, PosA} ->
            case eval(B, Meta) of
                false -> {true, PosA};
                {true, _PosB} -> false
            end;
        false ->
            false
    end.

term_positions(Meta, Token, Prefix, Columns) ->
    lists:append([
        column_term_positions(Meta, Column, Token, Prefix)
     || Column <- concrete_columns(Columns)
    ]).

column_term_positions(Meta, Column, Token, false) ->
    maps:get(Token, column_positions(Meta, Column), []);
column_term_positions(Meta, Column, Prefix, true) ->
    lists:append([
        Positions
     || {Token, Positions} <- maps:to_list(column_positions(Meta, Column)),
        binary_prefix(Token, Prefix)
    ]).

phrase_match_positions(Meta, Specs, Columns) ->
    lists:append([
        phrase_column_match_positions(Meta, Specs, Column)
     || Column <- concrete_columns(Columns)
    ]).

phrase_column_match_positions(_Meta, [], _Column) ->
    [];
phrase_column_match_positions(Meta, [{FirstToken, FirstPrefix, FirstOffset} | Rest], Column) ->
    FirstPositions = column_term_positions(Meta, Column, FirstToken, FirstPrefix),
    [
        Pos - FirstOffset
     || Pos <- FirstPositions,
        phrase_rest_matches(Meta, Rest, Column, Pos - FirstOffset)
    ].

phrase_rest_matches(_Meta, [], _Column, _Start) ->
    true;
phrase_rest_matches(Meta, [{Token, Prefix, Offset} | Rest], Column, Start) ->
    Positions = column_term_positions(Meta, Column, Token, Prefix),
    lists:member(Start + Offset, Positions) andalso
        phrase_rest_matches(Meta, Rest, Column, Start).

near_match_positions(Meta, Items, Distance, Columns) ->
    lists:append([
        near_column_match_positions(Meta, Items, Distance, Column)
     || Column <- concrete_columns(Columns)
    ]).

near_column_match_positions(Meta, Items, Distance, Column) ->
    SpanLists = [item_spans_in_column(Meta, Item, Column) || Item <- Items],
    case lists:any(fun(Spans) -> Spans =:= [] end, SpanLists) of
        true -> [];
        false -> near_positions(SpanLists, Distance)
    end.

item_positions_in_column(Meta, {term, Token, Prefix, Columns}, Column) ->
    case item_allows_column(Columns, Column) of
        true -> column_term_positions(Meta, Column, Token, Prefix);
        false -> []
    end;
item_positions_in_column(Meta, {phrase, Specs, Columns}, Column) ->
    case item_allows_column(Columns, Column) of
        true -> phrase_column_match_positions(Meta, Specs, Column);
        false -> []
    end;
item_positions_in_column(Meta, {anchor, AST}, Column) ->
    [P || P <- item_positions_in_column(Meta, AST, Column), P =:= 0];
item_positions_in_column(Meta, Other, Column) ->
    case eval(restrict_ast_columns(Other, [Column]), Meta) of
        {true, PosMap} -> flatten_position_map(PosMap);
        false -> []
    end.

item_spans_in_column(Meta, {term, Token, Prefix, Columns}, Column) ->
    case item_allows_column(Columns, Column) of
        true -> [{P, P} || P <- column_term_positions(Meta, Column, Token, Prefix)];
        false -> []
    end;
item_spans_in_column(Meta, {phrase, Specs, Columns}, Column) ->
    case item_allows_column(Columns, Column) of
        true -> phrase_column_match_spans(Meta, Specs, Column);
        false -> []
    end;
item_spans_in_column(Meta, {anchor, AST}, Column) ->
    [{P, P} || P <- item_positions_in_column(Meta, AST, Column), P =:= 0];
item_spans_in_column(Meta, Other, Column) ->
    [{P, P} || P <- item_positions_in_column(Meta, Other, Column)].

%% Column selectors reaching per-document checks are canonical (see
%% canonicalise_ast_columns/1) — plain membership, no re-normalising.
item_allows_column(all, _Column) ->
    true;
item_allows_column({not_columns, Columns}, Column) ->
    not lists:member(Column, Columns);
item_allows_column(Columns, Column) ->
    lists:member(Column, Columns).

canonicalise_ast_columns({term, T, P, Cols}) ->
    {term, T, P, canonical_selector(Cols)};
canonicalise_ast_columns({phrase, Specs, Cols}) ->
    {phrase, Specs, canonical_selector(Cols)};
canonicalise_ast_columns({near, Items, D, Cols}) ->
    {near, [canonicalise_ast_columns(I) || I <- Items], D, canonical_selector(Cols)};
canonicalise_ast_columns({anchor, A}) ->
    {anchor, canonicalise_ast_columns(A)};
canonicalise_ast_columns({'and', A, B}) ->
    {'and', canonicalise_ast_columns(A), canonicalise_ast_columns(B)};
canonicalise_ast_columns({'or', A, B}) ->
    {'or', canonicalise_ast_columns(A), canonicalise_ast_columns(B)};
canonicalise_ast_columns({'not', A, B}) ->
    {'not', canonicalise_ast_columns(A), canonicalise_ast_columns(B)};
canonicalise_ast_columns(Other) ->
    Other.

canonical_selector(all) ->
    all;
canonical_selector({not_columns, Cols}) ->
    {not_columns, schema_columns(Cols)};
canonical_selector(Cols) ->
    schema_columns(Cols).

phrase_column_match_spans(_Meta, [], _Column) ->
    [];
phrase_column_match_spans(Meta, [{FirstToken, FirstPrefix, FirstOffset} | Rest] = Specs, Column) ->
    FirstPositions = column_term_positions(Meta, Column, FirstToken, FirstPrefix),
    LastOffset = phrase_last_offset(Specs),
    [
        {Pos - FirstOffset, Pos - FirstOffset + LastOffset}
     || Pos <- FirstPositions,
        phrase_rest_matches(Meta, Rest, Column, Pos - FirstOffset)
    ].

phrase_last_offset(Specs) ->
    lists:max([Offset || {_Token, _Prefix, Offset} <- Specs]).

-ifdef(TEST).

client_codec_and_capacity_test() ->
    Posting = #{0 => #{<<"alpha">> => #{count => 70000,
        positions => lists:seq(0, 69999)}}},
    V = <<1, 2, 3, 4, 5, 6, 7, 8>>,
    Encoded = client_encode_posting({V, Posting}),
    {V, Decoded} = client_decode_posting(Encoded),
    #{0 := #{<<"alpha">> := #{count := 70000, positions := Capped}}} = Decoded,
    ?assert(length(Capped) < 70000),
    ?assertEqual(70000, maps:get(count, maps:get(<<"alpha">>, maps:get(0, Decoded)))),
    Columns255 = [integer_to_binary(I) || I <- lists:seq(1, 255)],
    ?assertMatch({ok, _}, schema(#{index => <<"cap255">>, columns => Columns255})),
    Columns256 = [integer_to_binary(I) || I <- lists:seq(1, 256)],
    ?assertMatch({error, {fts_capacity_exceeded, columns, 256, 255}},
        schema(#{index => <<"cap256">>, columns => Columns256})),
    ?assertError({fts_capacity_exceeded, column_id, 255, 254},
        client_encode_posting(
            {<<0:64>>, #{255 => #{<<"x">> => #{count => 1, positions => [0]}}}}
        )).

derive_remove_update_shape_test() ->
    {ok, Schema} = schema(#{index => <<"shape-unit">>, columns => [body]}),
    {ok, Specs} = derive(Schema, <<"doc">>, #{body => <<"alpha beta">>}),
    ?assert(lists:any(fun
        ({add, <<"shape-unit">>, <<_Shard:16>>, <<"d:doc">>, _}) -> true;
        (_) -> false
    end, Specs)),
    {add, <<"shape-unit">>, <<"doc">>, <<"doc">>, Manifest} =
        lists:keyfind(<<"doc">>, 3, Specs),
    Removes = remove(Schema, <<"doc">>, Manifest),
    ?assert(lists:member({remove, <<"shape-unit">>, <<"doc">>, <<"doc">>, <<>>},
        Removes)),
    {ok, Updated} = update(Schema, <<"doc">>, #{body => <<"gamma">>}, Manifest),
    Ids = [{B, K, SK} || {_, B, K, SK, _} <- Updated],
    ?assertEqual(length(Ids), length(lists:usort(Ids))).

cache_admission_test_() ->
    {timeout, 60, fun cache_admission_tester/0}.

cache_admission_tester() ->
    client_with_test_bookie(fun(Bookie) ->
        {ok, Schema} = schema(#{index => <<"cache-unit">>, columns => [body]}),
        ok = client_test_put(Bookie, Schema, <<"seed">>, <<"common">>),
        Gate = atomics:new(1, []),
        Hook = fun(_) ->
            case atomics:exchange(Gate, 1, 1) of
                0 -> client_test_put(Bookie, Schema, <<"racer">>, <<"common">>);
                1 -> ok
            end
        end,
        {ok, Hits} = search(Bookie, Schema, <<"common">>, #{cache_fill_hook => Hook}),
        ?assertEqual([<<"racer">>, <<"seed">>], [maps:get(key, H) || H <- Hits]),
        Table = cache_table(Bookie, Schema),
        %% exactly one shard-state entry (the raced fill was discarded
        %% and refilled once) plus the docs-epoch-validated manifest map
        ?assertEqual(1, length(ets:select(Table, [{{{shard, '_'}, '_'}, [], [true]}]))),
        ?assertMatch([{manifests, {_Epoch, _Map}}], ets:lookup(Table, manifests))
    end).

bm25_true_count_at_cap_test_() ->
    {timeout, 60, fun bm25_true_count_at_cap_tester/0}.

bm25_true_count_at_cap_tester() ->
    client_with_test_bookie(fun(Bookie) ->
        {ok, Schema} = schema(#{index => <<"bm25-cap">>, columns => [body]}),
        ok = client_test_put(Bookie, Schema, <<"65000">>,
            binary:copy(<<"hot ">>, 65000)),
        ok = client_test_put(Bookie, Schema, <<"70000">>,
            binary:copy(<<"hot ">>, 70000)),
        {ok, [High, Low]} = search(Bookie, Schema, <<"hot">>, #{rank => bm25}),
        ?assertEqual(<<"70000">>, maps:get(key, High)),
        ?assertEqual(<<"65000">>, maps:get(key, Low)),
        ?assert(maps:get(score, High) > maps:get(score, Low))
    end).

client_test_put(Bookie, Schema, Key, Text) ->
    {ok, Specs} = derive(Schema, Key, #{body => Text}),
    leveled_bookie:book_mput(Bookie, Specs).

client_with_test_bookie(Fun) ->
    Root = filename:join("/tmp", "leveled_fts_" ++
        integer_to_list(erlang:unique_integer([positive]))),
    _ = os:cmd("rm -rf " ++ Root),
    {ok, Bookie} = leveled_bookie:book_start([{root_path, Root},
        {compression_method, none}, {ledger_compression, none}]),
    try Fun(Bookie)
    after
        try leveled_bookie:book_destroy(Bookie)
        catch _:_ -> ok
        end
    end.

-endif.

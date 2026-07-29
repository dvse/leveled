%% -------- Full-text search: a pure client library ---------
%%
%% leveled_fts consumes ONLY the public store surface (docs/FTS.md):
%% book_mput/book_casmput/book_sqn/book_headonly/folds/snapshots. The
%% store carries no FTS hooks; all index state is ordinary HEAD_TAG
%% object-spec rows (postings, doc manifests, per-shard epoch rows,
%% tail summaries, consolidated token pages, stats), committed in the
%% caller's own batches.

-module(leveled_fts).

-include("leveled.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([test_v7_entry_size_sample/0]).
-endif.

-export([
    schema/1,
    capacities/0,
    tokenize_with_offsets/2,
    derive/3,
    remove/3,
    update/4,
    prepare/3,
    search/4,
    posting_read/5,
    consolidate/3
]).

-define(DEFAULT_LIMIT, 10000).
-define(MAX_LIMIT, 20000).
-define(MAX_WINDOW, 20000).
-define(MAX_QUERY_BYTES, 4096).
-define(MAX_QUERY_TOKENS, 128).
-define(MAX_AST_DEPTH, 32).
-define(MAX_NEAR_DISTANCE, 64).
-define(MAX_PREFIX_BYTES, 64).
%% With return_positions=true, match_count is always computed from the full
%% ordered match list.  The positions payload is then windowed to this many
%% ordinals; every returned list is a deterministic prefix, so its zero-based
%% list index is the occurrence number a later source resolver must select.
%% Phrase and NEAR lists contain their match-start ordinals.
-define(MAX_RETURN_POSITIONS, 4096).
-define(DEFAULT_NEAR, 10).

%% Client-library wire format capacities.  These are deliberately exported by
%% capacities/0 and consumed by schema/1.  Every fixed-width writer below also
%% checks the same bound immediately before constructing a bit syntax.
-define(POSTING_VERSION, 2).
-define(TAIL_VERSION, 3).
% "LFT"
-define(PAGE_MAGIC, 16#4C4654).
%% v7 replaces repeated document keys/version fields with global integer ids.
%% Boolean entries retain doc length for self-contained BM25 scoring. Older
%% pages are deliberately rejected and require reindexing; position lists
%% remain varint-delta encoded unchanged.
-define(PREVIOUS_PAGE_VERSION, 7).
-define(PAGE_VERSION, 8).
-define(BOOLEAN_PLANE, 0).
-define(POSITION_PLANE, 1).
-define(BOUNDARY_PLANE, 2).
-define(BOUNDARY_MARKER, 16#FFFF).
-define(BOUNDARY_OVERFLOW_HEADER_BYTES, 15).
-define(PAGE_DIR_STRIDE, 8).
-define(CHUNK_DIR_STRIDE, 25).
-define(CHUNK_TARGET_BYTES, 2048).
-define(SCORE_BOUND_SCALE, 64).
-define(TAILSUM_VERSION, 1).
-define(MANIFEST_VERSION, 4).
-define(DOCID_ROW_VERSION, 1).
-define(TRANSIENT_DOC_ID_BIT, 16#8000000000000000).
-define(STATS_VERSION, 2).
-define(PAGE_MAX_BYTES, 32768).
%% Page 0 carries the all-pages boundary index on top of a full chunk payload.
%% The usual compact index fits in the ordinary page headroom. Pathological
%% hot terms spill that index into bounded rows before page 0 exceeds the
%% ordinary page limit; readers reassemble it only when the marker is present.
-define(PAGE_TARGET_BYTES, 24000).
%% Leave room inside PAGE_TARGET_BYTES for the position entry, payload marker,
%% and directory row.  Larger per-document lists are split into independently
%% decodable chunks and may span multiple pages.
-define(PAGE_POSITION_BYTES, 23968).
-define(TAIL_BLOOM_BYTES, 32).
-define(MAX_COLUMNS, 255).
-define(MAX_COLUMN_ID, 254).
-define(MAX_TOKEN_BYTES, 65535).
-define(MAX_POSITION_BYTES, 65535).
-define(MAX_DOC_KEY_BYTES, 65533).
-define(MAX_SHARDS, 65536).
-define(DEFAULT_SHARDS, 256).
-define(MAX_U16, 16#FFFF).
-define(MAX_U32, 16#FFFFFFFF).
-define(MAX_U64, 16#FFFFFFFFFFFFFFFF).
-define(MAX_DOC_ID, ?MAX_U64).

%% ---------------------------------------------------------------------------
%% Pure client API (docs/FTS.md).
%%
%% All persisted state is made from ordinary HEAD_TAG object specs.  The store
%% has no FTS callback, configuration, or privileged payload.  The logical
%% Consolidated reads are token-page point reads (or a bounded token-key range
%% fold for prefixes), plus a shard tail only when its summary admits it.  The
%% library owns no ETS or other cache; leveled's ledger/page caches are the
%% sole source of warmth.
%% ---------------------------------------------------------------------------

-spec capacities() -> map().
capacities() ->
    #{
        columns => ?MAX_COLUMNS,
        column_id => ?MAX_COLUMN_ID,
        token_bytes => ?MAX_TOKEN_BYTES,
        position_bytes => ?MAX_POSITION_BYTES,
        doc_key_bytes => ?MAX_DOC_KEY_BYTES,
        doc_id => ?MAX_DOC_ID,
        shards => ?MAX_SHARDS,
        true_occurrences => ?MAX_U64
    }.

-spec schema(map()) -> {ok, map()} | {error, term()}.
schema(Definition) when is_map(Definition) ->
    try
        Index = normalise_index(maps:get(index, Definition)),
        true = is_binary(Index) andalso Index =/= <<>>,
        {ok, ColumnSpecs} = normalise_column_specs(
            maps:get(columns, Definition)
        ),
        ColumnCount = length(ColumnSpecs),
        client_guard(columns, ColumnCount, maps:get(columns, capacities())),
        Shards = maps:get(shards, Definition, ?DEFAULT_SHARDS),
        true =
            is_integer(Shards) andalso Shards > 0 andalso
                Shards =< maps:get(shards, capacities()) andalso
                (Shards band (Shards - 1)) =:= 0,
        Opts0 = maps:with(
            [
                tokenizer,
                remove_diacritics,
                tokenchars,
                separators,
                stopwords,
                decode,
                prefixes
            ],
            Definition
        ),
        true = valid_tokenizer(maps:get(tokenizer, Opts0, unicode61)),
        true = valid_remove_diacritics(
            maps:get(remove_diacritics, Opts0, 1)
        ),
        true = valid_char_option(maps:get(tokenchars, Opts0, [])),
        true = valid_char_option(maps:get(separators, Opts0, [])),
        Opts1 = normalise_options(
            Opts0#{
                remove_diacritics => client_rd_mode(
                    maps:get(remove_diacritics, Opts0, 1)
                )
            }
        ),
        Columns = [Column || {Column, _Path, _Mode} <- ColumnSpecs],
        Canonical = #{
            index => Index,
            columns => Columns,
            column_specs => ColumnSpecs,
            column_modes => maps:from_list([
                {Column, Mode}
             || {Column, _Path, Mode} <- ColumnSpecs
            ]),
            options => Opts1,
            tokenizer => tokenizer_description(Opts1),
            prefixes => maps:get(prefixes, Opts1, []),
            shards => Shards
        },
        Fingerprint = crypto:hash(
            sha256, term_to_binary(Canonical, [deterministic])
        ),
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
derive(Schema, DocKey, Object) ->
    client_derive(Schema, DocKey, Object, none, none).

client_derive(
    #{fingerprint := Fingerprint} = Schema,
    DocKey,
    Object,
    BaseLength,
    OldManifest
) when
    is_binary(DocKey), is_binary(Fingerprint)
->
    client_guard(
        doc_key_bytes, byte_size(DocKey), maps:get(doc_key_bytes, capacities())
    ),
    Fields = extract_fields(
        maybe_decode_object(Object, Schema), maps:get(column_specs, Schema)
    ),
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
    DocId = client_doc_id(DocKey, DocVersion),
    RetiredIds =
        case OldManifest of
            #{doc_id := DocId} -> [];
            #{doc_id := OldDocId} -> [OldDocId];
            _ -> []
        end,
    PostingSpecs = [
        {add, Bucket, client_shard_key(Shard), client_doc_subkey(DocKey),
            client_encode_tail(
                DocVersion,
                DocId,
                RetiredIds,
                DocLength,
                BaseLength,
                live,
                maps:get(Shard, ByShard)
            )}
     || Shard <- Touched
    ],
    Manifest =
        client_encode_manifest(
            DocVersion, DocId, Touched, DocLength, BaseLength, Fingerprint
        ),
    WriteShards = client_write_shards(Touched),
    {ok,
        PostingSpecs ++
            [
                {add, Bucket, <<"doc">>, DocKey, Manifest},
                client_doc_id_spec(Bucket, DocId, DocKey, DocVersion),
                client_stats_dirty_spec(Bucket),
                client_stats_tail_spec(
                    Bucket, DocKey, DocVersion, DocLength, BaseLength, live
                )
            ] ++
            [client_tailsum_dirty_spec(Bucket, Shard) || Shard <- Touched] ++
            [client_epoch_spec(Bucket, Shard) || Shard <- WriteShards]};
client_derive(_Schema, DocKey, _Object, _BaseLength, _OldManifest) ->
    erlang:error({invalid_fts_derive, DocKey}).

-spec remove(map(), binary(), binary() | map()) ->
    [leveled_codec:object_spec()].
remove(#{index := Bucket, fingerprint := Fingerprint}, DocKey, Manifest0) when
    is_binary(DocKey)
->
    #{
        version := DocVersion,
        doc_id := DocId,
        shards := Shards,
        doc_length := DocLength,
        fingerprint := Fingerprint
    } =
        Manifest = client_decode_manifest_value(Manifest0),
    BaseLength = maps:get(base_length, Manifest, none),
    [
        {add, Bucket, client_shard_key(Shard), client_doc_subkey(DocKey),
            client_encode_tail(
                DocVersion,
                DocId,
                [],
                DocLength,
                BaseLength,
                remove,
                #{}
            )}
     || Shard <- Shards
    ] ++
        [
            {remove, Bucket, <<"doc">>, DocKey, <<>>},
            client_remove_doc_id_spec(Bucket, DocId),
            client_stats_dirty_spec(Bucket),
            client_stats_tail_spec(
                Bucket, DocKey, DocVersion, DocLength, BaseLength, remove
            )
        ] ++
        [client_tailsum_dirty_spec(Bucket, Shard) || Shard <- Shards] ++
        [
            client_epoch_spec(Bucket, Shard)
         || Shard <- client_write_shards(Shards)
        ].

-spec update(map(), binary(), term(), binary() | map()) ->
    {ok, [leveled_codec:object_spec()]}.
update(Schema, DocKey, Object, OldManifest) ->
    Old = client_decode_manifest_value(OldManifest),
    BaseLength = maps:get(base_length, Old, none),
    {ok, NewSpecs} = client_derive(
        Schema, DocKey, Object, BaseLength, Old
    ),
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
                    client_guard(
                        token_bytes,
                        byte_size(Token),
                        maps:get(token_bytes, capacities())
                    ),
                    Count = length(Positions),
                    client_guard(
                        true_occurrences,
                        Count,
                        maps:get(true_occurrences, capacities())
                    ),
                    Shard = client_shard_id(Token, maps:get(shards, Schema)),
                    ByCol = maps:get(Shard, SA, #{}),
                    ByToken = maps:get(ColId, ByCol, #{}),
                    Entry = #{count => Count, positions => Positions},
                    {
                        SA#{Shard => ByCol#{ColId => ByToken#{Token => Entry}}},
                        LA + Count
                    }
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

client_doc_id_subkey(DocId) ->
    client_guard(doc_id, DocId, ?MAX_DOC_ID),
    <<DocId:64/unsigned-big>>.

client_doc_id(DocKey, DocVersion) ->
    <<RawId:64/unsigned-big, _/binary>> = crypto:hash(
        sha256,
        <<
            (byte_size(DocKey)):16/unsigned-big,
            DocKey/binary,
            DocVersion/binary
        >>
    ),
    RawId bor ?TRANSIENT_DOC_ID_BIT.

client_transient_doc_id(DocId) ->
    (DocId band ?TRANSIENT_DOC_ID_BIT) =/= 0.

client_doc_id_spec(Bucket, DocId, DocKey, DocVersion) ->
    {add, Bucket, <<"id">>, client_doc_id_subkey(DocId),
        client_encode_doc_id_row(DocKey, DocVersion)}.

client_remove_doc_id_spec(Bucket, DocId) ->
    {remove, Bucket, <<"id">>, client_doc_id_subkey(DocId), <<>>}.

client_token_key(Token) ->
    <<"t:", Token/binary>>.

client_page_subkey(Plane, Column, PageNo) ->
    client_guard(page_plane, Plane, 1),
    client_guard(page_column, Column, ?MAX_COLUMN_ID),
    client_guard(page_number, PageNo, ?MAX_U16),
    <<Plane:8, Column:8, PageNo:16/unsigned-big>>.

client_v8_page_subkey(Plane, Column, PageNo, FirstDocId, LastDocId) ->
    client_guard(page_plane, Plane, 1),
    client_guard(page_column, Column, ?MAX_COLUMN_ID),
    client_guard(page_number, PageNo, ?MAX_U16),
    client_guard(doc_id, FirstDocId, ?MAX_DOC_ID),
    client_guard(doc_id, LastDocId, ?MAX_DOC_ID),
    true = FirstDocId =< LastDocId,
    %% LastDocId leads the ordered suffix: a range starting at a target docid
    %% lands on the first page which can contain it. FirstDocId and PageNo
    %% complete the durable identity and make the full range self-describing.
    <<Plane:8, Column:8, LastDocId:64/unsigned-big, FirstDocId:64/unsigned-big,
        PageNo:16/unsigned-big>>.

client_v8_plane_range(Plane, Column) ->
    {
        <<Plane:8, Column:8, 0:144>>,
        <<Plane:8, Column:8, ?MAX_U64:64/unsigned-big, ?MAX_U64:64/unsigned-big,
            ?MAX_U16:16/unsigned-big>>
    }.

client_boundary_overflow_subkey(Plane, Column, PartNo) ->
    client_guard(page_plane, Plane, ?POSITION_PLANE),
    client_guard(page_column, Column, ?MAX_COLUMN_ID),
    client_guard(boundary_part, PartNo, ?MAX_U16),
    <<?BOUNDARY_PLANE:8, Plane:8, Column:8, PartNo:16/unsigned-big>>.

%% Every page row, including page zero's cross-page directory, carries an
%% unambiguous magic + format stamp in its value.  Page formats are
%% intentionally not migrated in place; an older index must be reindexed.

client_read_page(Bookie, Bucket, Key, Plane, Column, PageNo) ->
    case
        leveled_bookie:book_headonly(
            Bookie, Bucket, Key, client_page_subkey(Plane, Column, PageNo)
        )
    of
        {ok, Value} = Found ->
            client_require_page_value(Value),
            Found;
        not_found ->
            case
                client_read_v8_plane_pages(
                    Bookie, Bucket, Key, Plane, Column, [PageNo]
                )
            of
                #{PageNo := {_SubKey, Value}} -> {ok, Value};
                #{} -> not_found
            end
    end.

client_read_v8_plane_pages(
    Bookie, Bucket, Key, Plane, Column, WantedPages
) ->
    {Start, Finish} = client_v8_plane_range(Plane, Column),
    Wanted =
        case WantedPages of
            all -> all;
            _ -> maps:from_list([{PageNo, true} || PageNo <- WantedPages])
        end,
    Fold = fun
        (B, {K, SubKey}, Value, Acc) when
            B =:= Bucket, K =:= Key, byte_size(SubKey) =:= 20
        ->
            case client_decode_page_row(SubKey, Value) of
                {page, Plane, Column, PageNo} ->
                    case Wanted =:= all orelse maps:is_key(PageNo, Wanted) of
                        true -> Acc#{PageNo => {SubKey, Value}};
                        false -> Acc
                    end;
                _ ->
                    Acc
            end;
        (_B, _K, _Value, Acc) ->
            Acc
    end,
    {async, Runner} = leveled_bookie:book_headfold(
        Bookie,
        ?HEAD_TAG,
        {range, Bucket, {{Key, Start}, {Key, Finish}}},
        {Fold, #{}},
        false,
        true,
        false
    ),
    Pages = Runner(),
    case maps:find(0, Pages) of
        {ok, {SubKey0, Value0}} ->
            Pages#{
                0 => {
                    SubKey0,
                    client_expand_boundary_overflow(
                        Bookie, Bucket, Key, Plane, Column, Value0
                    )
                }
            };
        error ->
            Pages
    end.

client_expand_boundary_overflow(
    Bookie,
    Bucket,
    Key,
    Plane,
    Column,
    <<?PAGE_MAGIC:24/unsigned-big, ?PAGE_VERSION:8, Plane:8,
        PageCount:16/unsigned-big, TotalDocs:32/unsigned-big,
        CollectionFrequency:64/unsigned-big, TermMaxBound:8,
        EntryCount:32/unsigned-big, ChunkDirBytes:32/unsigned-big,
        PageDirBytes:32/unsigned-big, ChunkDirectory:ChunkDirBytes/binary,
        PageDirectory:PageDirBytes/binary, Payload/binary>> = Value
) ->
    case PageDirectory of
        <<GlobalDocs:32/unsigned-big, ?BOUNDARY_MARKER:16/unsigned-big,
            PartCount:16/unsigned-big,
            TotalBoundaryBytes:32/unsigned-big>> when
            PartCount > 0
        ->
            Boundaries = client_read_boundary_overflow(
                Bookie,
                Bucket,
                Key,
                Plane,
                Column,
                PartCount,
                TotalBoundaryBytes
            ),
            FullDirectory = <<GlobalDocs:32/unsigned-big, Boundaries/binary>>,
            <<?PAGE_MAGIC:24/unsigned-big, ?PAGE_VERSION:8, Plane:8,
                PageCount:16/unsigned-big, TotalDocs:32/unsigned-big,
                CollectionFrequency:64/unsigned-big, TermMaxBound:8,
                EntryCount:32/unsigned-big, ChunkDirBytes:32/unsigned-big,
                (byte_size(FullDirectory)):32/unsigned-big,
                ChunkDirectory/binary, FullDirectory/binary, Payload/binary>>;
        _Inline ->
            Value
    end;
client_expand_boundary_overflow(
    _Bookie, _Bucket, _Key, _Plane, _Column, Value
) ->
    Value.

client_read_boundary_overflow(
    Bookie, Bucket, Key, Plane, Column, PartCount, TotalBoundaryBytes
) ->
    Parts = [
        begin
            SubKey = client_boundary_overflow_subkey(
                Plane, Column, PartNo
            ),
            {ok, Value} = leveled_bookie:book_headonly(
                Bookie, Bucket, Key, SubKey
            ),
            client_decode_boundary_overflow(
                Value,
                Plane,
                Column,
                PartNo,
                PartCount,
                TotalBoundaryBytes
            )
        end
     || PartNo <- lists:seq(0, PartCount - 1)
    ],
    Boundaries = iolist_to_binary(Parts),
    true = byte_size(Boundaries) =:= TotalBoundaryBytes,
    Boundaries.

client_decode_boundary_overflow(
    <<?PAGE_MAGIC:24/unsigned-big, ?PAGE_VERSION:8, ?BOUNDARY_PLANE:8, Plane:8,
        Column:8, PartNo:16/unsigned-big, PartCount:16/unsigned-big,
        TotalBoundaryBytes:32/unsigned-big, Part/binary>>,
    Plane,
    Column,
    PartNo,
    PartCount,
    TotalBoundaryBytes
) ->
    Part;
client_decode_boundary_overflow(
    Bad, _Plane, _Column, _PartNo, _PartCount, _TotalBoundaryBytes
) ->
    erlang:error({invalid_fts_boundary_overflow, Bad}).

client_decode_boundary_overflow_row(
    <<?BOUNDARY_PLANE:8, Plane:8, Column:8, PartNo:16/unsigned-big>>,
    <<?PAGE_MAGIC:24/unsigned-big, ?PAGE_VERSION:8, ?BOUNDARY_PLANE:8, Plane:8,
        Column:8, PartNo:16/unsigned-big, _PartCount:16/unsigned-big,
        _TotalBoundaryBytes:32/unsigned-big, _Part/binary>>
) ->
    boundary_overflow;
client_decode_boundary_overflow_row(_SubKey, _Value) ->
    not_boundary_overflow.

client_read_v8_selected_pages_from_head(
    _Bookie,
    _Bucket,
    _Key,
    _Plane,
    _Column,
    <<?PAGE_MAGIC:24/unsigned-big, ?PREVIOUS_PAGE_VERSION:8, _/binary>>,
    _PageNumbers
) ->
    previous_format;
client_read_v8_selected_pages_from_head(
    _Bookie,
    _Bucket,
    _Key,
    _Plane,
    _Column,
    <<?PAGE_MAGIC:24/unsigned-big, ?PAGE_VERSION:8, _/binary>>,
    []
) ->
    {ok, #{}};
client_read_v8_selected_pages_from_head(
    Bookie,
    Bucket,
    Key,
    Plane,
    Column,
    <<?PAGE_MAGIC:24/unsigned-big, ?PAGE_VERSION:8, _/binary>> = Head,
    PageNumbers
) ->
    Ordered = lists:sort(PageNumbers),
    Boundaries = client_page_boundaries(Head),
    FirstPage = hd(Ordered),
    LastPage = lists:last(Ordered),
    {FirstPage, FirstDocId, FirstLastDocId} =
        element(FirstPage + 1, Boundaries),
    {LastPage, LastFirstDocId, LastDocId} =
        element(LastPage + 1, Boundaries),
    Start = client_v8_page_subkey(
        Plane, Column, FirstPage, FirstDocId, FirstLastDocId
    ),
    Finish = client_v8_page_subkey(
        Plane, Column, LastPage, LastFirstDocId, LastDocId
    ),
    Wanted = maps:from_list([{PageNo, true} || PageNo <- Ordered]),
    Fold = fun
        (B, {K, SubKey}, Value, Acc) when
            B =:= Bucket, K =:= Key, byte_size(SubKey) =:= 20
        ->
            case client_decode_page_row(SubKey, Value) of
                {page, Plane, Column, PageNo} ->
                    case maps:is_key(PageNo, Wanted) of
                        true -> Acc#{PageNo => Value};
                        false -> Acc
                    end;
                _ ->
                    Acc
            end;
        (_B, _K, _Value, Acc) ->
            Acc
    end,
    {async, Runner} = leveled_bookie:book_headfold(
        Bookie,
        ?HEAD_TAG,
        {range, Bucket, {{Key, Start}, {Key, Finish}}},
        {Fold, #{}},
        false,
        true,
        false
    ),
    Values = Runner(),
    case map_size(Values) =:= length(Ordered) of
        true -> {ok, Values};
        false -> erlang:error({missing_fts_page_range, Plane, Ordered})
    end.

client_read_cached_direct_term_pages(
    Bookie, #{index := Bucket}, Token, Column
) ->
    CacheKey = {fts_term_docs, Bucket, Token, Column},
    case client_clean_stats_epoch(Bookie, Bucket) of
        {ok, Epoch} ->
            case
                leveled_bookie:book_valuecache_get(
                    Bookie, CacheKey, Epoch
                )
            of
                {ok, {Docs, Validators}} ->
                    case client_term_cache_current(
                        Bookie, Bucket, Validators
                    ) of
                        true ->
                            Docs;
                        false ->
                            client_refresh_direct_term_cache(
                                Bookie,
                                Bucket,
                                Token,
                                Column,
                                CacheKey,
                                Epoch
                            )
                    end;
                miss ->
                    client_refresh_direct_term_cache(
                        Bookie,
                        Bucket,
                        Token,
                        Column,
                        CacheKey,
                        Epoch
                    )
            end;
        dirty ->
            {Docs, _Validators} = client_read_direct_term_pages(
                Bookie, Bucket, Token, Column
            ),
            Docs
    end.

client_refresh_direct_term_cache(
    Bookie, Bucket, Token, Column, CacheKey, Epoch
) ->
    {Docs, Validators} = client_read_direct_term_pages(
        Bookie, Bucket, Token, Column
    ),
    case Validators of
        no_cache ->
            ok;
        _ ->
            ok = leveled_bookie:book_valuecache_put(
                Bookie, CacheKey, Epoch, {Docs, Validators}
            )
    end,
    Docs.

client_term_cache_current(_Bookie, _Bucket, no_cache) ->
    false;
client_term_cache_current(Bookie, Bucket, Validators) ->
    Rows = [Row || {Row, _SQN} <- Validators],
    leveled_bookie:book_mhead_sqn(Bookie, Bucket, Rows) =:=
        Validators.

client_clean_stats_epoch(Bookie, Bucket) ->
    case
        leveled_bookie:book_mhead_sqn(
            Bookie,
            Bucket,
            [
                {<<"stats">>, <<"summary">>},
                {<<"stats">>, <<"dirty">>}
            ]
        )
    of
        [
            {{<<"stats">>, <<"summary">>}, {ok, Epoch}},
            {{<<"stats">>, <<"dirty">>}, not_found}
        ] ->
            {ok, Epoch};
        _ ->
            dirty
    end.

client_read_direct_term_pages(
    Bookie, Bucket, Token, Column
) ->
    Key = client_token_key(Token),
    case
        client_read_v8_plane_pages(
            Bookie, Bucket, Key, ?BOOLEAN_PLANE, Column, all
        )
    of
        V8Pages when map_size(V8Pages) > 0 ->
            Rows = [
                {Key, SubKey}
             || {_PageNo, {SubKey, _Value}} <- maps:to_list(V8Pages)
            ],
            SQNs = maps:from_list(
                leveled_bookie:book_mhead_sqn(Bookie, Bucket, Rows)
            ),
            Docs =
                maps:fold(
                    fun(_PageNo, {SubKey, Value}, Acc) ->
                        Row = {Key, SubKey},
                        {ok, SQN} = maps:get(Row, SQNs),
                        {ok, Page} = client_read_cached_v8_direct_term_page(
                            Bookie, Bucket, Key, SubKey, SQN, Value
                        ),
                        client_merge_direct_term_docs(
                            maps:get(docs, Page), Acc
                        )
                    end,
                    #{},
                    V8Pages
                ),
            {Docs, maps:to_list(SQNs)};
        #{} ->
            {client_read_cached_direct_term_v7_pages(
                Bookie, Bucket, Key, Column
            ), no_cache}
    end.

client_read_cached_direct_term_v7_pages(Bookie, Bucket, Key, Column) ->
    case
        client_read_cached_direct_term_page(
            Bookie, Bucket, Key, Column, 0
        )
    of
        not_found ->
            #{};
        {ok, HeadPage} ->
            PageCount = maps:get(page_count, HeadPage),
            Pages =
                case PageCount of
                    1 ->
                        [HeadPage];
                    _ ->
                        [
                            HeadPage
                            | client_read_cached_direct_term_overflow(
                                Bookie,
                                Bucket,
                                Key,
                                Column,
                                lists:seq(1, PageCount - 1)
                            )
                        ]
                end,
            lists:foldl(
                fun(Page, Acc) ->
                    client_merge_direct_term_docs(
                        maps:get(docs, Page), Acc
                    )
                end,
                #{},
                Pages
            )
    end.

client_read_cached_direct_term_overflow(
    Bookie, Bucket, Key, Column, PageNumbers
) ->
    Rows = [
        {Key, client_page_subkey(?BOOLEAN_PLANE, Column, PageNo)}
     || PageNo <- PageNumbers
    ],
    SQNs = leveled_bookie:book_mhead_sqn(Bookie, Bucket, Rows),
    [
        begin
            {ok, Page} = client_read_cached_direct_term_page(
                Bookie, Bucket, Key, Column, PageNo, SQN
            ),
            Page
        end
     || {PageNo, {_Row, {ok, SQN}}} <-
            lists:zip(PageNumbers, SQNs)
    ].

client_read_cached_direct_term_page(
    Bookie, Bucket, Key, Column, PageNo
) ->
    Row = {Key, client_page_subkey(?BOOLEAN_PLANE, Column, PageNo)},
    case leveled_bookie:book_mhead_sqn(Bookie, Bucket, [Row]) of
        [{Row, {ok, SQN}}] ->
            client_read_cached_direct_term_page(
                Bookie, Bucket, Key, Column, PageNo, SQN
            );
        [{Row, not_found}] ->
            case
                client_read_v8_plane_pages(
                    Bookie,
                    Bucket,
                    Key,
                    ?BOOLEAN_PLANE,
                    Column,
                    [PageNo]
                )
            of
                #{PageNo := {SubKey, Value}} ->
                    [{_Row, {ok, SQN}}] = leveled_bookie:book_mhead_sqn(
                        Bookie,
                        Bucket,
                        [{Key, SubKey}]
                    ),
                    client_read_cached_v8_direct_term_page(
                        Bookie, Bucket, Key, SubKey, SQN, Value
                    );
                #{} ->
                    not_found
            end
    end.

client_read_cached_v8_direct_term_page(
    Bookie, Bucket, Key, SubKey, SQN, Value
) ->
    CacheKey = {fts_decoded_page, direct_boolean_v2, Bucket, Key, SubKey},
    case leveled_bookie:book_valuecache_get(Bookie, CacheKey, SQN) of
        {ok, Page} ->
            {ok, Page};
        miss ->
            PageNo = client_v8_subkey_page_number(SubKey),
            Page = #{
                page_count => client_page_count(Value),
                total_docs =>
                    case PageNo of
                        0 -> client_page_total_docs(Value);
                        _ -> 0
                    end,
                docs => client_decode_direct_term_page(Value, #{})
            },
            ok = leveled_bookie:book_valuecache_put(
                Bookie, CacheKey, SQN, Page
            ),
            {ok, Page}
    end.

client_v8_subkey_page_number(
    <<_Plane:8, _Column:8, _Last:64/unsigned-big, _First:64/unsigned-big,
        PageNo:16/unsigned-big>>
) ->
    PageNo.

client_read_cached_direct_term_page(
    Bookie, Bucket, Key, Column, PageNo, SQN
) ->
    client_read_cached_direct_term_page(
        Bookie, Bucket, Key, Column, PageNo, SQN, undefined
    ).

client_read_cached_direct_term_page(
    Bookie, Bucket, Key, Column, PageNo, SQN, RawValue
) ->
    SubKey = client_page_subkey(?BOOLEAN_PLANE, Column, PageNo),
    CacheKey = {fts_decoded_page, direct_boolean_v1, Bucket, Key, SubKey},
    case leveled_bookie:book_valuecache_get(Bookie, CacheKey, SQN) of
        {ok, Page} ->
            {ok, Page};
        miss ->
            ReadResult =
                case RawValue of
                    undefined ->
                        leveled_bookie:book_headonly(
                            Bookie, Bucket, Key, SubKey
                        );
                    ProvidedValue when is_binary(ProvidedValue) ->
                        {ok, ProvidedValue}
                end,
            case ReadResult of
                {ok, PageValue} ->
                    client_require_page_value(PageValue),
                    Page = #{
                        page_count => client_page_count(PageValue),
                        total_docs =>
                            case PageNo of
                                0 -> client_page_total_docs(PageValue);
                                _ -> 0
                            end,
                        docs =>
                            client_decode_direct_term_page(
                                PageValue, #{}
                            )
                    },
                    ok = leveled_bookie:book_valuecache_put(
                        Bookie, CacheKey, SQN, Page
                    ),
                    {ok, Page};
                not_found ->
                    not_found
            end
    end.

client_merge_direct_term_docs(Docs, Acc) ->
    maps:fold(
        fun(DocId, {DocId, Length, Tf}, DocAcc) ->
            case maps:find(DocId, DocAcc) of
                {ok, {DocId, ExistingLength, ExistingTf}} ->
                    DocAcc#{
                        DocId =>
                            {DocId, ExistingLength, ExistingTf + Tf}
                    };
                error ->
                    DocAcc#{DocId => {DocId, Length, Tf}}
            end
        end,
        Acc,
        Docs
    ).

client_require_page_value(
    <<?PAGE_MAGIC:24/unsigned-big, ?PAGE_VERSION:8, _/binary>>
) ->
    ok;
client_require_page_value(
    <<?PAGE_MAGIC:24/unsigned-big, ?PREVIOUS_PAGE_VERSION:8, _/binary>>
) ->
    ok;
client_require_page_value(
    <<?PAGE_MAGIC:24/unsigned-big, Found:8, _/binary>>
) ->
    erlang:error({fts_page_format, Found, ?PAGE_VERSION});
client_require_page_value(
    <<Version:8, _/binary>>
) when
    Version =:= ?PAGE_VERSION orelse Version =:= ?PREVIOUS_PAGE_VERSION
->
    erlang:error({fts_page_format, unstamped, ?PAGE_VERSION});
client_require_page_value(<<Found:8, _/binary>>) ->
    erlang:error({fts_page_format, Found, ?PAGE_VERSION});
client_require_page_value(_Bad) ->
    erlang:error({fts_page_format, unstamped, ?PAGE_VERSION}).

client_page_version(
    <<?PAGE_MAGIC:24/unsigned-big, Version:8, _/binary>>
) when
    Version =:= ?PAGE_VERSION orelse Version =:= ?PREVIOUS_PAGE_VERSION
->
    Version.

client_decode_page_row(
    <<Plane:8, Column:8, PageNo:16/unsigned-big>>, Value
) when
    Plane =< ?POSITION_PLANE, Column =< ?MAX_COLUMN_ID
->
    client_require_page_value(Value),
    {page, Plane, Column, PageNo};
client_decode_page_row(
    <<Plane:8, Column:8, LastDocId:64/unsigned-big, FirstDocId:64/unsigned-big,
        PageNo:16/unsigned-big>>,
    Value
) when
    Plane =< ?POSITION_PLANE,
    Column =< ?MAX_COLUMN_ID,
    FirstDocId =< LastDocId
->
    client_require_page_value(Value),
    case Value of
        <<?PAGE_MAGIC:24/unsigned-big, _Version:8, Plane:8, _/binary>> ->
            {page, Plane, Column, PageNo};
        _ ->
            erlang:error({invalid_fts_page_subkey, Value})
    end;
client_decode_page_row(_SubKey, _Value) ->
    not_page.

client_shard_key(Shard) ->
    client_guard(shard_id, Shard, ?MAX_U16),
    <<Shard:16/unsigned-big>>.

client_shard_id(Token, Shards) ->
    Raw =
        case Token of
            <<>> -> 0;
            <<B1:8>> -> B1 bsl 8;
            <<B1:8, B2:8, _/binary>> -> (B1 bsl 8) bor B2
        end,
    (Raw * Shards) bsr 16.

client_epoch_spec(Bucket, Shard) ->
    {add, Bucket, client_shard_key(Shard), <<"epoch">>, <<1>>}.

client_write_shards([]) -> [0];
client_write_shards(Shards) -> Shards.

client_tailsum_dirty_spec(Bucket, Shard) ->
    {add, Bucket, client_shard_key(Shard), <<"tailsum">>,
        client_encode_tailsum(dirty)}.

client_stats_dirty_spec(Bucket) ->
    {add, Bucket, <<"stats">>, <<"dirty">>, <<1>>}.

client_stats_tail_spec(Bucket, DocKey, Version, DocLength, BaseLength, Kind) ->
    {add, Bucket, <<"stats">>, client_doc_subkey(DocKey),
        client_encode_stats_tail(
            Version, DocLength, BaseLength, Kind
        )}.

client_guard(_What, Value, Max) when
    is_integer(Value), Value >= 0, Value =< Max
->
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
    <<?POSTING_VERSION:8, DocVersion:8/binary, (length(Columns)):8,
        Body/binary>>.

client_encode_column(ColumnId, ByToken) ->
    client_guard(column_id, ColumnId, ?MAX_COLUMN_ID),
    Tokens = lists:sort(maps:to_list(ByToken)),
    EncodedTokens = lists:append([
        client_encode_token(Token, Entry)
     || {Token, Entry} <- Tokens
    ]),
    client_guard(tokens_per_column, length(EncodedTokens), ?MAX_U32),
    Body = iolist_to_binary(EncodedTokens),
    <<ColumnId:8, (length(EncodedTokens)):32/unsigned-big, Body/binary>>.

client_encode_token(Token, #{count := Count, positions := Positions}) ->
    TokenBytes = byte_size(Token),
    client_guard(token_bytes, TokenBytes, ?MAX_TOKEN_BYTES),
    client_guard(true_occurrences, Count, ?MAX_U64),
    case length(Positions) =:= Count of
        true ->
            ok;
        false ->
            erlang:error(
                {invalid_fts_position_count, Count, length(Positions)}
            )
    end,
    [
        begin
            PosBytes = byte_size(PosBin),
            client_guard(position_bytes, PosBytes, ?MAX_POSITION_BYTES),
            <<TokenBytes:16/unsigned-big, Token/binary,
                ChunkCount:64/unsigned-big, PosBytes:16/unsigned-big,
                PosBin/binary>>
        end
     || {ChunkCount, PosBin} <-
            client_encode_position_chunks(Positions, ?MAX_POSITION_BYTES)
    ].

client_encode_positions(Positions) ->
    client_encode_positions(Positions, 0, <<>>).

client_encode_positions([], _Last, Acc) ->
    Acc;
client_encode_positions([Position | Rest], Last, Acc) when
    is_integer(Position), Position >= Last
->
    client_encode_positions(
        Rest, Position, varint_append(Position - Last, Acc)
    );
client_encode_positions(Bad, _Last, _Acc) ->
    erlang:error({invalid_fts_positions, Bad}).

client_encode_position_chunks([], _Limit) ->
    [{0, <<>>}];
client_encode_position_chunks(Positions, Limit) ->
    client_encode_position_chunks(Positions, Limit, 0, <<>>, 0, []).

client_encode_position_chunks([], _Limit, _Last, Acc, Count, Chunks) ->
    lists:reverse([{Count, Acc} | Chunks]);
client_encode_position_chunks(
    [Position | Rest],
    Limit,
    Last,
    Acc,
    Count,
    Chunks
) when is_integer(Position), Position >= Last ->
    Encoded = varint_append(Position - Last, <<>>),
    case Count > 0 andalso byte_size(Acc) + byte_size(Encoded) > Limit of
        true ->
            First = varint_append(Position, <<>>),
            client_guard(position_bytes, byte_size(First), Limit),
            client_encode_position_chunks(
                Rest, Limit, Position, First, 1, [{Count, Acc} | Chunks]
            );
        false ->
            client_guard(
                position_bytes, byte_size(Acc) + byte_size(Encoded), Limit
            ),
            client_encode_position_chunks(
                Rest,
                Limit,
                Position,
                <<Acc/binary, Encoded/binary>>,
                Count + 1,
                Chunks
            )
    end;
client_encode_position_chunks(Bad, _Limit, _Last, _Acc, _Count, _Chunks) ->
    erlang:error({invalid_fts_positions, Bad}).

client_decode_posting(
    <<?POSTING_VERSION:8, DocVersion:8/binary, NCols:8, Rest/binary>>
) ->
    {DocVersion, client_decode_columns(NCols, Rest, #{})};
client_decode_posting(Bad) ->
    erlang:error({invalid_fts_posting, Bad}).

client_encode_tail(
    DocVersion,
    DocId,
    RetiredIds,
    DocLength,
    BaseLength,
    Kind,
    ByColumn
) ->
    client_guard(doc_id, DocId, ?MAX_DOC_ID),
    client_guard(retired_doc_ids, length(RetiredIds), ?MAX_U16),
    client_guard(doc_length, DocLength, ?MAX_U64),
    KindByte =
        case Kind of
            live -> 1;
            remove -> 0
        end,
    {BaseFlag, BaseValue} = client_encode_base_length(BaseLength),
    Posting = client_encode_posting({DocVersion, ByColumn}),
    RetiredBin = iolist_to_binary([
        begin
            client_guard(doc_id, RetiredId, ?MAX_DOC_ID),
            <<RetiredId:64/unsigned-big>>
        end
     || RetiredId <- RetiredIds
    ]),
    <<?TAIL_VERSION:8, KindByte:8, BaseFlag:8, DocId:64/unsigned-big,
        (length(RetiredIds)):16/unsigned-big, RetiredBin/binary,
        DocLength:64/unsigned-big, BaseValue:64/unsigned-big, Posting/binary>>.

client_decode_tail(
    <<?TAIL_VERSION:8, KindByte:8, BaseFlag:8, DocId:64/unsigned-big,
        RetiredCount:16/unsigned-big, Rest0/binary>>
) when
    (KindByte =:= 0 orelse KindByte =:= 1) andalso
        (BaseFlag =:= 0 orelse BaseFlag =:= 1)
->
    RetiredBytes = RetiredCount * 8,
    <<RetiredBin:RetiredBytes/binary, DocLength:64/unsigned-big,
        BaseValue:64/unsigned-big, Posting/binary>> = Rest0,
    {DocVersion, ByColumn} = client_decode_posting(Posting),
    Kind =
        case KindByte of
            0 -> remove;
            1 -> live
        end,
    BaseLength = client_decode_base_length(BaseFlag, BaseValue),
    RetiredIds = [Id || <<Id:64/unsigned-big>> <= RetiredBin],
    {DocVersion, DocId, RetiredIds, DocLength, BaseLength, Kind, ByColumn};
client_decode_tail(Bad) ->
    erlang:error({invalid_fts_tail, Bad}).

client_encode_base_length(none) ->
    {0, 0};
client_encode_base_length(Length) when is_integer(Length), Length >= 0 ->
    client_guard(base_doc_length, Length, ?MAX_U64),
    {1, Length}.

client_decode_base_length(0, _Value) -> none;
client_decode_base_length(1, Value) -> Value.

client_decode_columns(0, <<>>, Acc) ->
    Acc;
client_decode_columns(
    N, <<ColId:8, NTokens:32/unsigned-big, Rest/binary>>, Acc
) when
    N > 0
->
    {ByToken, Tail} = client_decode_tokens(NTokens, Rest, #{}),
    client_decode_columns(N - 1, Tail, Acc#{ColId => ByToken});
client_decode_columns(_N, Bad, _Acc) ->
    erlang:error({invalid_fts_posting_columns, Bad}).

%% Current writers may emit repeated token frames whose chunks are merged into
%% one logical entry.  Do not assert count/position equality here: a legacy
%% capped frame can legitimately decode with count > length(positions).
client_decode_tokens(0, Rest, Acc) ->
    {Acc, Rest};
client_decode_tokens(
    N,
    <<TokenBytes:16/unsigned-big, Token:TokenBytes/binary,
        Count:64/unsigned-big, PosBytes:16/unsigned-big, PosBin:PosBytes/binary,
        Rest/binary>>,
    Acc
) when N > 0 ->
    Positions =
        case decode_positions(PosBin, 0, []) of
            {ok, Ps} -> Ps;
            error -> erlang:error({invalid_fts_positions, PosBin})
        end,
    Entry =
        case maps:find(Token, Acc) of
            error ->
                #{count => Count, positions => Positions};
            {ok, #{count := ExistingCount, positions := ExistingPositions}} ->
                #{
                    count => ExistingCount + Count,
                    positions => ExistingPositions ++ Positions
                }
        end,
    client_decode_tokens(N - 1, Rest, Acc#{Token => Entry});
client_decode_tokens(_N, Bad, _Acc) ->
    erlang:error({invalid_fts_posting_tokens, Bad}).

client_encode_manifest(
    DocVersion,
    DocId,
    Shards,
    DocLength,
    BaseLength,
    Fingerprint
) when
    byte_size(DocVersion) == 8
->
    client_guard(doc_id, DocId, ?MAX_DOC_ID),
    client_guard(manifest_shards, length(Shards), ?MAX_U16),
    client_guard(doc_length, DocLength, ?MAX_U64),
    32 = byte_size(Fingerprint),
    {BaseFlag, BaseValue} = client_encode_base_length(BaseLength),
    ShardBin = iolist_to_binary([
        client_encode_shard_id(Shard)
     || Shard <- Shards
    ]),
    <<?MANIFEST_VERSION:8, DocVersion:8/binary, DocId:64/unsigned-big,
        (length(Shards)):16/unsigned-big, ShardBin/binary,
        DocLength:64/unsigned-big, BaseFlag:8, BaseValue:64/unsigned-big,
        Fingerprint/binary>>.

client_decode_manifest_value(
    #{shards := _, doc_length := _, fingerprint := _} = M
) ->
    M;
client_decode_manifest_value(
    <<?MANIFEST_VERSION:8, DocVersion:8/binary, DocId:64/unsigned-big,
        N:16/unsigned-big, Rest/binary>>
) ->
    ShardBytes = N * 2,
    case Rest of
        <<ShardBin:ShardBytes/binary, DocLength:64/unsigned-big, BaseFlag:8,
            BaseValue:64/unsigned-big, Fingerprint:32/binary>> ->
            #{
                version => DocVersion,
                doc_id => DocId,
                shards => [S || <<S:16/unsigned-big>> <= ShardBin],
                doc_length => DocLength,
                base_length => client_decode_base_length(BaseFlag, BaseValue),
                fingerprint => Fingerprint
            };
        _ ->
            erlang:error({invalid_fts_manifest, Rest})
    end;
client_decode_manifest_value(Bad) ->
    erlang:error({invalid_fts_manifest, Bad}).

client_encode_doc_id_row(DocKey, DocVersion) ->
    client_guard(doc_key_bytes, byte_size(DocKey), ?MAX_DOC_KEY_BYTES),
    <<?DOCID_ROW_VERSION:8, DocVersion:8/binary,
        (byte_size(DocKey)):16/unsigned-big, DocKey/binary>>.

client_decode_doc_id_row(
    <<?DOCID_ROW_VERSION:8, DocVersion:8/binary, DocKeyBytes:16/unsigned-big,
        DocKey:DocKeyBytes/binary>>
) ->
    {DocKey, DocVersion};
client_decode_doc_id_row(Bad) ->
    erlang:error({invalid_fts_doc_id_row, Bad}).

client_encode_shard_id(Shard) ->
    client_guard(shard_id, Shard, ?MAX_U16),
    <<Shard:16/unsigned-big>>.

client_encode_tailsum(empty) ->
    <<?TAILSUM_VERSION:8, 0:32/unsigned-big, 0:(?TAIL_BLOOM_BYTES * 8)>>;
client_encode_tailsum(dirty) ->
    %% derive/3 is deliberately store-independent, so a rewrite cannot safely
    %% union with the previous shard bloom.  An all-one bloom is the compact,
    %% conservative representation: never a false negative, and consolidation
    %% replaces it with the exact empty summary in the page/tail CAS.
    <<?TAILSUM_VERSION:8, ?MAX_U32:32/unsigned-big, ?MAX_U64:64/unsigned-big,
        ?MAX_U64:64/unsigned-big, ?MAX_U64:64/unsigned-big,
        ?MAX_U64:64/unsigned-big>>.

client_tailsum_nonempty(Summary) ->
    {Count, _Bloom} = client_decode_tailsum(Summary),
    Count =/= 0.

client_tailsum_might_contain(Summary, Token, Prefix) when
    is_binary(Token), is_boolean(Prefix)
->
    {Count, Bloom} = client_decode_tailsum(Summary),
    case {Count =/= 0, Prefix} of
        {false, _} ->
            false;
        {true, true} ->
            %% An exact-token bloom cannot disprove a prefix match.
            true;
        {true, false} ->
            lists:all(
                fun(Position) -> client_bloom_bit_set(Bloom, Position) end,
                client_tail_bloom_positions(Token)
            )
    end.

client_decode_tailsum(
    <<?TAILSUM_VERSION:8, Count:32/unsigned-big,
        Bloom:?TAIL_BLOOM_BYTES/binary>>
) ->
    {Count, Bloom};
client_decode_tailsum(Bad) ->
    erlang:error({invalid_fts_tailsum, Bad}).

client_tail_bloom_positions(Token) ->
    <<A:8, B:8, C:8, D:8, _/binary>> =
        crypto:hash(sha256, <<"leveled-fts-tail-v1", Token/binary>>),
    lists:usort([A, B, C, D]).

client_bloom_bit_set(Bloom, Position) ->
    Byte = binary:at(Bloom, Position bsr 3),
    Byte band (1 bsl (Position band 7)) =/= 0.

-spec prepare(map(), binary() | list() | all_docs, map() | list()) ->
    {ok, tuple()} | {error, term()}.
prepare(#{fingerprint := Fingerprint} = Schema, Query, Opts0) ->
    try
        case normalise_search_options(Opts0, Schema) of
            {ok, Opts} ->
                case client_prepare_query_ast(Schema, Query, Opts) of
                    {ok, AST} ->
                        {ok,
                            {leveled_fts_prepared_v1, Fingerprint,
                                option_columns(Opts, Schema), AST}};
                    Error ->
                        Error
                end;
            Error ->
                Error
        end
    catch
        error:Reason -> {error, Reason};
        throw:{fts_error, Reason} -> {error, Reason}
    end.

-spec search(
    pid(), map(), binary() | list() | all_docs | tuple(), map() | list()
) ->
    {ok, [map()] | #{hits := [map()], count := non_neg_integer()}}
    | {error, term()}.
search(Bookie, #{fingerprint := _} = Schema, Query, Opts0) when
    is_pid(Bookie)
->
    try
        {Hook, Opts1} = client_take_option(tail_fold_hook, Opts0),
        case normalise_search_options(Opts1, Schema) of
            {ok, Opts} ->
                case client_query_ast(Schema, Query, Opts) of
                    {ok, AST} ->
                        client_search(Bookie, Schema, AST, Opts, Hook);
                    Error ->
                        Error
                end;
            Error ->
                Error
        end
    catch
        error:Reason -> {error, Reason};
        throw:{fts_error, Reason} -> {error, Reason}
    end.

-spec posting_read(
    pid(), map(), binary() | list() | tuple(), [binary()], map() | list()
) -> {ok, [map()]} | {error, term()}.
posting_read(Bookie, #{fingerprint := _} = Schema, Query, DocKeys, Opts0) when
    is_pid(Bookie), is_list(DocKeys)
->
    try
        case normalise_search_options(Opts0, Schema) of
            {ok, Opts0N} ->
                case client_query_ast(Schema, Query, Opts0N) of
                    {ok, AST} ->
                        Opts = Opts0N#{
                            limit => length(DocKeys),
                            offset => 0,
                            rank => none,
                            return_positions => true
                        },
                        client_posting_read(
                            Bookie,
                            Schema,
                            AST,
                            lists:usort(DocKeys),
                            Opts
                        );
                    Error ->
                        Error
                end;
            Error ->
                Error
        end
    catch
        error:Reason -> {error, Reason};
        throw:{fts_error, Reason} -> {error, Reason}
    end.

client_query_ast(
    #{fingerprint := Fingerprint} = Schema,
    {leveled_fts_prepared_v1, Fingerprint, Columns, AST},
    Opts
) ->
    case Columns =:= option_columns(Opts, Schema) of
        true -> {ok, AST};
        false -> {error, invalid_fts_prepared_query}
    end;
client_query_ast(
    _Schema,
    {leveled_fts_prepared_v1, _Fingerprint, _Columns, _AST},
    _Opts
) ->
    {error, invalid_fts_prepared_query};
client_query_ast(Schema, Query, Opts) ->
    client_prepare_query_ast(Schema, Query, Opts).

client_prepare_query_ast(Schema, Query, Opts) ->
    case parse(Query, Opts) of
        {ok, AST0} ->
            Columns = option_columns(Opts, Schema),
            case validate_ast_columns(AST0, Columns) of
                ok ->
                    {ok,
                        canonicalise_ast_columns(
                            restrict_ast_columns(AST0, Columns)
                        )};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

client_posting_read(_Bookie, _Schema, _AST, [], _Opts) ->
    {ok, []};
client_posting_read(Bookie, Schema, AST, DocKeys, Opts) ->
    Candidates = client_posting_candidates(Bookie, Schema, DocKeys),
    TokenSpecs = lists:usort(client_ast_token_specs(AST)),
    Shards = client_ast_shards(AST, Schema),
    TailSummaries = client_read_tail_summaries(Bookie, Schema, Shards),
    BooleanStates = client_read_query_pages(
        Bookie, Schema, TokenSpecs, Candidates, #{}
    ),
    PositionSpecs = lists:usort(client_position_specs(AST, Opts)),
    PageStates = client_read_query_positions(
        Bookie, Schema, PositionSpecs, BooleanStates
    ),
    Raw = lists:foldl(
        fun(Shard, Acc) ->
            PageState = maps:get(Shard, PageStates, #{}),
            Specs = client_specs_for_shard(TokenSpecs, Shard, Schema),
            State0 = client_apply_tail(
                Bookie,
                Schema,
                Shard,
                Specs,
                PageState,
                undefined,
                maps:get(Shard, TailSummaries)
            ),
            State = maps:with(maps:keys(Candidates), State0),
            client_merge_shard_docs(State, Acc)
        end,
        #{},
        Shards
    ),
    Metas = client_raw_metas(Raw, Schema, AST, #{}),
    client_evaluate(AST, Metas, Bookie, Schema, Opts).

client_posting_candidates(Bookie, Schema, DocKeys) ->
    Bucket = maps:get(index, Schema),
    Fingerprint = maps:get(fingerprint, Schema),
    lists:foldl(
        fun
            (DocKey, Acc) when is_binary(DocKey) ->
                case
                    leveled_bookie:book_headonly(
                        Bookie, Bucket, <<"doc">>, DocKey
                    )
                of
                    {ok, ManifestValue} ->
                        Manifest = client_decode_manifest_value(ManifestValue),
                        case maps:get(fingerprint, Manifest) of
                            Fingerprint ->
                                Acc#{maps:get(doc_id, Manifest) => true};
                            _OtherFingerprint ->
                                Acc
                        end;
                    not_found ->
                        Acc
                end;
            (_BadDocKey, Acc) ->
                Acc
        end,
        #{},
        DocKeys
    ).

client_take_option(Key, Opts) when is_map(Opts) ->
    {maps:get(Key, Opts, undefined), maps:remove(Key, Opts)};
client_take_option(Key, Opts) when is_list(Opts) ->
    {proplists:get_value(Key, Opts, undefined), proplists:delete(Key, Opts)}.

client_search(Bookie, Schema, {all_docs} = AST, Opts, _Hook) ->
    Manifests = client_fold_manifests(Bookie, Schema),
    Metas =
        maps:map(
            fun(Key, {_V, DocLength}) -> client_empty_meta(Key, DocLength) end,
            Manifests
        ),
    client_evaluate(AST, Metas, Bookie, Schema, Opts);
client_search(Bookie, Schema, AST, Opts, Hook) ->
    TokenSpecs = lists:usort(client_ast_token_specs(AST)),
    Shards = client_ast_shards(AST, Schema),
    TailSummaries = client_read_tail_summaries(Bookie, Schema, Shards),
    HasDirtyTail =
        client_has_dirty_tail(TailSummaries, TokenSpecs, Schema),
    case client_can_direct_term(AST, Opts, HasDirtyTail) of
        true ->
            client_search_direct_term_store(
                Bookie, Schema, AST, Opts, Hook, Shards, TailSummaries
            );
        false ->
            case client_can_direct_near(AST, Opts, HasDirtyTail) of
                true ->
                    client_search_direct_near_store(
                        Bookie, Schema, AST, Opts
                    );
                false ->
                    case client_can_direct_boolean(AST, Opts, HasDirtyTail) of
                        true ->
                            client_search_direct_boolean_store(
                                Bookie, Schema, AST, Opts
                            );
                        false ->
                            case
                                client_can_direct_not(AST, Opts, HasDirtyTail)
                            of
                                true ->
                                    client_search_direct_not_store(
                                        Bookie, Schema, AST, Opts
                                    );
                                false ->
                                    case
                                        client_can_direct_phrase(
                                            AST, Opts, HasDirtyTail
                                        )
                                    of
                                        true ->
                                            client_search_direct_phrase_store(
                                                Bookie, Schema, AST, Opts
                                            );
                                        false ->
                                            BooleanStates =
                                                case HasDirtyTail of
                                                    true ->
                                                        client_read_query_pages(
                                                            Bookie,
                                                            Schema,
                                                            TokenSpecs,
                                                            all,
                                                            #{}
                                                        );
                                                    false ->
                                                        client_read_planned_pages(
                                                            Bookie,
                                                            Schema,
                                                            AST,
                                                            TokenSpecs
                                                        )
                                                end,
                                            PositionSpecs = lists:usort(
                                                client_position_specs(AST, Opts)
                                            ),
                                            client_search_with_positions(
                                                Bookie,
                                                Schema,
                                                AST,
                                                Opts,
                                                Hook,
                                                TokenSpecs,
                                                Shards,
                                                TailSummaries,
                                                BooleanStates,
                                                PositionSpecs,
                                                HasDirtyTail
                                            )
                                    end
                            end
                    end
            end
    end.

client_search_with_positions(
    Bookie,
    Schema,
    AST,
    Opts,
    Hook,
    TokenSpecs,
    Shards,
    TailSummaries,
    BooleanStates,
    PositionSpecs,
    HasDirtyTail
) ->
    case not HasDirtyTail andalso client_use_raw_near(AST, BooleanStates) of
        true ->
            {ok, NearRaw, NearMatches} = client_read_near_raw(
                Bookie, Schema, AST, BooleanStates
            ),
            Metas = client_raw_metas(
                NearRaw, Schema, AST, NearMatches
            ),
            client_evaluate(AST, Metas, Bookie, Schema, Opts);
        false ->
            PageStates =
                case PositionSpecs of
                    [] ->
                        BooleanStates;
                    _ ->
                        client_read_query_positions(
                            Bookie, Schema, PositionSpecs, BooleanStates
                        )
                end,
            client_finish_search(
                Bookie,
                Schema,
                AST,
                Opts,
                Hook,
                TokenSpecs,
                Shards,
                TailSummaries,
                PageStates
            )
    end.

client_can_direct_term({term, _Token, false, _Columns}, Opts, _HasDirtyTail) ->
    (maps:get(rank, Opts, none) =:= none orelse
        maps:get(rank, Opts, none) =:= bm25) andalso
        not maps:get(return_positions, Opts, false);
client_can_direct_term(_AST, _Opts, _HasDirtyTail) ->
    false.

client_can_direct_near({near, Items, _Distance, _Columns}, Opts, false) ->
    (maps:get(rank, Opts, none) =:= none orelse
        maps:get(rank, Opts, none) =:= bm25) andalso
        lists:all(
            fun
                ({term, _Token, false, _ItemColumns}) -> true;
                (_Item) -> false
            end,
            Items
        );
client_can_direct_near(_AST, _Opts, _HasDirtyTail) ->
    false.

client_can_direct_boolean(AST, Opts, false) ->
    Plan = client_boolean_plan(AST),
    Rank = maps:get(rank, Opts, none),
    RankSupported =
        case {Rank, Plan} of
            {bm25, {'tree', _}} -> true;
            {bm25, {'and', _}} -> true;
            {bm25, {'or', _}} -> true;
            {bm25, {'not', _, _}} -> true;
            {none, {'and', _}} -> true;
            {none, {'not', _, _}} -> true;
            _ -> false
        end,
    RankSupported andalso
        not maps:get(return_positions, Opts, false);
client_can_direct_boolean(_AST, _Opts, _HasDirtyTail) ->
    false.

client_boolean_plan({'and', A, B} = AST) ->
    case {client_flatten_and_terms(A), client_flatten_and_terms(B)} of
        {{ok, TermsA}, {ok, TermsB}} -> {'and', TermsA ++ TermsB};
        _ -> client_boolean_tree_plan(AST)
    end;
client_boolean_plan(
    {'not', {term, _Positive, false, _PositiveColumns} = Positive,
        {term, _Negative, false, _NegativeColumns} = Negative}
) ->
    {'not', Positive, Negative};
client_boolean_plan({'or', _A, _B} = AST) ->
    case client_flatten_or_terms(AST) of
        {ok, Terms} -> {'or', Terms};
        error -> client_boolean_tree_plan(AST)
    end;
client_boolean_plan({'not', _A, _B} = AST) ->
    client_boolean_tree_plan(AST);
client_boolean_plan(_AST) ->
    none.

client_boolean_tree_plan(AST) ->
    case client_exact_boolean_tree(AST) of
        true -> {'tree', AST};
        false -> none
    end.

client_exact_boolean_tree({term, _Token, false, _Columns}) ->
    true;
client_exact_boolean_tree({'and', A, B}) ->
    client_exact_boolean_tree(A) andalso client_exact_boolean_tree(B);
client_exact_boolean_tree({'or', A, B}) ->
    client_exact_boolean_tree(A) andalso client_exact_boolean_tree(B);
client_exact_boolean_tree({'not', A, B}) ->
    client_exact_boolean_tree(A) andalso client_exact_boolean_tree(B);
client_exact_boolean_tree(_AST) ->
    false.

client_flatten_and_terms(
    {term, _Token, false, _Columns} = Term
) ->
    {ok, [Term]};
client_flatten_and_terms({'and', A, B}) ->
    case {client_flatten_and_terms(A), client_flatten_and_terms(B)} of
        {{ok, TermsA}, {ok, TermsB}} -> {ok, TermsA ++ TermsB};
        _ -> error
    end;
client_flatten_and_terms(_AST) ->
    error.

client_flatten_or_terms(
    {term, _Token, false, _Columns} = Term
) ->
    {ok, [Term]};
client_flatten_or_terms({'or', A, B}) ->
    case {client_flatten_or_terms(A), client_flatten_or_terms(B)} of
        {{ok, TermsA}, {ok, TermsB}} -> {ok, TermsA ++ TermsB};
        _ -> error
    end;
client_flatten_or_terms(_AST) ->
    error.

client_can_direct_not(
    {'not', {term, _Positive, false, _PositiveColumns},
        {term, _Negative, false, _NegativeColumns}},
    Opts,
    false
) ->
    maps:get(rank, Opts, none) =:= none andalso
        not maps:get(return_positions, Opts, false);
client_can_direct_not(_AST, _Opts, _HasDirtyTail) ->
    false.

client_can_direct_phrase({phrase, Specs, _Columns}, Opts, false) ->
    (maps:get(rank, Opts, none) =:= none orelse
        maps:get(rank, Opts, none) =:= bm25) andalso
        lists:all(
            fun({_Token, Prefix, _Offset}) -> Prefix =:= false end,
            Specs
        );
client_can_direct_phrase(_AST, _Opts, _HasDirtyTail) ->
    false.

client_search_direct_term_store(
    Bookie,
    Schema,
    AST,
    Opts,
    Hook,
    Shards,
    TailSummaries
) ->
    case
        client_try_ranked_v8_direct_term(
            Bookie, Schema, AST, Opts, Shards, TailSummaries
        )
    of
        {ok, _Result} = Complete ->
            Complete;
        fallback ->
            client_search_direct_term_store_fallback(
                Bookie,
                Schema,
                AST,
                Opts,
                Hook,
                Shards,
                TailSummaries
            )
    end.

client_try_ranked_v8_direct_term(
    Bookie,
    Schema,
    {term, Token, false, Columns},
    #{rank := bm25} = Opts,
    _Shards,
    TailSummaries
) ->
    ColumnIds = client_selector_column_ids(Columns, Schema),
    case
        {
            ColumnIds,
            client_direct_term_tails_clean(TailSummaries, Token),
            client_ranked_window_size(Opts) > 0
        }
    of
        {[Column], true, true} ->
            Bucket = maps:get(index, Schema),
            Key = client_token_key(Token),
            case
                client_read_v8_plane_pages(
                    Bookie, Bucket, Key, ?BOOLEAN_PLANE, Column, all
                )
            of
                Pages when map_size(Pages) > 0 ->
                    client_ranked_v8_direct_term(
                        Bookie, Schema, Pages, Opts
                    );
                #{} ->
                    client_resolved_search_result([], 0, Opts)
            end;
        _ ->
            fallback
    end;
client_try_ranked_v8_direct_term(
    _Bookie, _Schema, _AST, _Opts, _Shards, _TailSummaries
) ->
    fallback.

client_direct_term_tails_clean(TailSummaries, Token) ->
    maps:fold(
        fun
            (_Shard, _Summary, false) ->
                false;
            (_Shard, not_found, true) ->
                true;
            (_Shard, {ok, Summary}, true) ->
                not client_tailsum_might_contain(Summary, Token, false)
        end,
        true,
        TailSummaries
    ).

client_ranked_v8_direct_term(Bookie, Schema, Pages, Opts) ->
    {_SubKey0, Head} = maps:get(0, Pages),
    PageCount = client_page_count(Head),
    case map_size(Pages) =:= PageCount of
        false ->
            fallback;
        true ->
            NHit = client_page_total_docs(Head),
            {DocCount, TotalLength} = client_corpus_stats(Bookie, Schema),
            AvgLength =
                case DocCount of
                    0 -> 0.0;
                    _ -> TotalLength / DocCount
                end,
            [Idf] =
                Idfs = client_direct_bm25_idfs(
                    [NHit], DocCount
                ),
            Window = client_ranked_window_size(Opts),
            {_Tree, SkippedBound, SeenHits} = lists:foldl(
                fun(PageNo, State) ->
                    {_SubKey, Value} = maps:get(PageNo, Pages),
                    client_scan_ranked_v8_page(
                        Value, Window, Idfs, Idf, AvgLength, State
                    )
                end,
                {gb_trees:empty(), none, []},
                lists:seq(0, PageCount - 1)
            ),
            Live = client_bounded_ranked_hits(
                Bookie, Schema, SeenHits, Window
            ),
            case
                client_ranked_v8_skip_safe(
                    SkippedBound, Live, erlang:min(Window, NHit)
                )
            of
                true ->
                    client_resolved_search_result(Live, NHit, Opts);
                false ->
                    fallback
            end
    end.

client_scan_ranked_v8_page(
    <<?PAGE_MAGIC:24/unsigned-big, ?PAGE_VERSION:8, ?BOOLEAN_PLANE:8,
        _PageCount:16/unsigned-big, _TotalDocs:32/unsigned-big,
        _CollectionFrequency:64/unsigned-big, _TermMaxBound:8,
        _N:32/unsigned-big, ChunkDirBytes:32/unsigned-big,
        PageDirBytes:32/unsigned-big, ChunkDirectory:ChunkDirBytes/binary,
        _PageDirectory:PageDirBytes/binary, Payload/binary>>,
    Window,
    Idfs,
    Idf,
    AvgLength,
    State
) ->
    client_scan_ranked_v8_chunks(
        ChunkDirectory, Payload, Window, Idfs, Idf, AvgLength, State
    ).

client_scan_ranked_v8_chunks(
    <<>>, _Payload, _Window, _Idfs, _Idf, _AvgLength, State
) ->
    State;
client_scan_ranked_v8_chunks(
    <<First:64/unsigned-big, _Last:64/unsigned-big, Offset:32/unsigned-big,
        Bytes:16/unsigned-big, Count:16/unsigned-big, MaxBound:8,
        RestDirectory/binary>>,
    Payload,
    Window,
    Idfs,
    Idf,
    AvgLength,
    {Tree, SkippedBound, SeenHits}
) ->
    UpperBound = Idf * MaxBound / ?SCORE_BOUND_SCALE,
    Skip =
        case gb_trees:size(Tree) >= Window andalso Window > 0 of
            true ->
                {_WorstKey, WorstHit} = gb_trees:largest(Tree),
                UpperBound < client_ranked_candidate_score(WorstHit);
            false ->
                false
        end,
    {Tree1, SkippedBound1, SeenHits1} =
        case Skip of
            true ->
                {
                    Tree,
                    case SkippedBound of
                        none -> UpperBound;
                        _ -> erlang:max(SkippedBound, UpperBound)
                    end,
                    SeenHits
                };
            false ->
                Chunk = binary:part(Payload, Offset, Bytes),
                Docs = client_decode_v8_direct_chunk(
                    Count, First, First, Chunk, true, all, #{}
                ),
                {NextTree, ChunkHits} = maps:fold(
                    fun(DocId, {DocId, Length, Tf}, {TreeAcc, HitAcc}) ->
                        Hit = client_ranked_candidate(
                            client_direct_bm25_score_precomputed(
                                [Tf], Idfs, AvgLength, Length
                            ),
                            DocId,
                            Length,
                            Tf
                        ),
                        {
                            client_ranked_heap_add(
                                Hit, Window, TreeAcc
                            ),
                            [Hit | HitAcc]
                        }
                    end,
                    {Tree, []},
                    Docs
                ),
                {NextTree, SkippedBound, ChunkHits ++ SeenHits}
        end,
    client_scan_ranked_v8_chunks(
        RestDirectory,
        Payload,
        Window,
        Idfs,
        Idf,
        AvgLength,
        {Tree1, SkippedBound1, SeenHits1}
    ).

client_ranked_v8_skip_safe(none, _Live, _Needed) ->
    true;
client_ranked_v8_skip_safe(_SkippedBound, Live, Needed) when
    length(Live) < Needed
->
    false;
client_ranked_v8_skip_safe(SkippedBound, Live, _Needed) ->
    WorstLive = lists:last(Live),
    SkippedBound < maps:get(score, WorstLive).

client_resolved_search_result(Hits, Count, Opts) ->
    Offset = maps:get(offset, Opts, 0),
    Limit = maps:get(limit, Opts, ?DEFAULT_LIMIT),
    Window = lists:sublist(
        client_drop(Offset, Hits), Limit
    ),
    case maps:get(return_count, Opts, false) of
        true -> {ok, #{hits => Window, count => Count}};
        false -> {ok, Window}
    end.

client_drop(0, Values) ->
    Values;
client_drop(_Count, []) ->
    [];
client_drop(Count, [_ | Rest]) when Count > 0 ->
    client_drop(Count - 1, Rest).

client_search_direct_term_store_fallback(
    Bookie,
    Schema,
    {term, Token, false, Columns},
    Opts,
    Hook,
    Shards,
    TailSummaries
) ->
    ColumnIds = client_selector_column_ids(Columns, Schema),
    PageDocs = lists:foldl(
        fun(Column, Acc) ->
            client_merge_direct_term_docs(
                client_read_cached_direct_term_pages(
                    Bookie, Schema, Token, Column
                ),
                Acc
            )
        end,
        #{},
        ColumnIds
    ),
    Docs = lists:foldl(
        fun(Shard, Acc) ->
            client_apply_direct_term_tail(
                Bookie,
                Schema,
                Shard,
                Token,
                ColumnIds,
                Acc,
                Hook,
                maps:get(Shard, TailSummaries)
            )
        end,
        PageDocs,
        Shards
    ),
    Ranked = maps:get(rank, Opts, none) =:= bm25,
    {DocCount, TotalLength} =
        case Ranked of
            true -> client_corpus_stats(Bookie, Schema);
            false -> {0, 0}
        end,
    AvgLength =
        case DocCount of
            0 -> 0.0;
            _ -> TotalLength / DocCount
        end,
    NHit = map_size(Docs),
    Idfs =
        case Ranked of
            true -> client_direct_bm25_idfs([NHit], DocCount);
            false -> []
        end,
    Hits =
        case Ranked of
            true ->
                [
                    client_ranked_candidate(
                        client_direct_bm25_score_precomputed(
                            [Tf], Idfs, AvgLength, Length
                        ),
                        DocId,
                        Length,
                        Tf
                    )
                 || {DocId, {_Version, Length, Tf}} <-
                        maps:to_list(Docs)
                ];
            false ->
                [
                    #{
                        key => DocId,
                        score => 0.0,
                        doc_length => Length,
                        match_count => Tf
                    }
                 || {DocId, {_Version, Length, Tf}} <-
                        maps:to_list(Docs)
                ]
        end,
    Sorted =
        case Ranked of
            true ->
                client_bounded_ranked_hits(
                    Bookie, Schema, Hits, client_ranked_window_size(Opts)
                );
            false ->
                lists:sort(
                    fun(A, B) -> maps:get(key, A) =< maps:get(key, B) end,
                    Hits
                )
        end,
    client_search_result(Bookie, Schema, Sorted, NHit, Opts).

client_apply_direct_term_tail(
    _Bookie,
    _Schema,
    _Shard,
    _Token,
    _ColumnIds,
    PageDocs,
    _Hook,
    not_found
) ->
    PageDocs;
client_apply_direct_term_tail(
    Bookie,
    Schema,
    Shard,
    Token,
    ColumnIds,
    PageDocs,
    Hook,
    {ok, Summary}
) ->
    case client_tailsum_might_contain(Summary, Token, false) of
        false ->
            PageDocs;
        true ->
            {Tail0, _Rows} = client_fold_shard_tail(
                Bookie, Schema, Shard
            ),
            Tail = client_canonical_tail(Bookie, Schema, Tail0),
            client_call_hook(Hook, {Shard, Tail}),
            maps:fold(
                fun
                    (
                        _DocKey,
                        {_Version, _DocId, _RetiredIds, _Length, _Base, remove,
                            _Posting},
                        Acc
                    ) ->
                        Acc;
                    (
                        _DocKey,
                        {_Version, DocId, _RetiredIds, Length, _Base, live,
                            Posting},
                        Acc
                    ) ->
                        Tf = client_direct_term_tail_tf(
                            Posting, Token, ColumnIds
                        ),
                        case Tf > 0 of
                            true -> Acc#{DocId => {DocId, Length, Tf}};
                            false -> Acc
                        end
                end,
                maps:without(client_tail_doc_ids(Tail), PageDocs),
                Tail
            )
    end.

client_direct_term_tail_tf(Posting, Token, ColumnIds) ->
    lists:sum([
        case maps:find(Token, maps:get(Column, Posting, #{})) of
            {ok, Entry} -> maps:get(count, Entry);
            error -> 0
        end
     || Column <- ColumnIds
    ]).

client_decode_direct_term_page(
    <<?PAGE_MAGIC:24/unsigned-big, ?PAGE_VERSION:8, ?BOOLEAN_PLANE:8,
        _PageCount:16/unsigned-big, _TotalDocs:32/unsigned-big,
        _CollectionFrequency:64/unsigned-big, _TermMaxBound:8,
        _N:32/unsigned-big, ChunkDirBytes:32/unsigned-big,
        PageDirBytes:32/unsigned-big, ChunkDirectory:ChunkDirBytes/binary,
        _PageDirectory:PageDirBytes/binary, Payload/binary>>,
    Acc
) when ChunkDirBytes rem ?CHUNK_DIR_STRIDE =:= 0 ->
    client_note_direct_page_decode(),
    client_decode_v8_direct_chunks(
        ChunkDirectory, Payload, all, Acc
    );
client_decode_direct_term_page(
    <<?PAGE_MAGIC:24/unsigned-big, ?PREVIOUS_PAGE_VERSION:8, ?BOOLEAN_PLANE:8,
        _PageCount:16/unsigned-big, _TotalDocs:32/unsigned-big,
        N:32/unsigned-big, DirBytes:32/unsigned-big,
        PageDirBytes:32/unsigned-big, Directory:DirBytes/binary,
        _PageDirectory:PageDirBytes/binary, Payload/binary>>,
    Acc
) ->
    client_note_direct_page_decode(),
    client_decode_direct_term_rows(N, Directory, Payload, Acc).

client_decode_v8_direct_term_candidates(
    <<?PAGE_MAGIC:24/unsigned-big, ?PAGE_VERSION:8, ?BOOLEAN_PLANE:8,
        _PageCount:16/unsigned-big, _TotalDocs:32/unsigned-big,
        _CollectionFrequency:64/unsigned-big, _TermMaxBound:8,
        _N:32/unsigned-big, ChunkDirBytes:32/unsigned-big,
        PageDirBytes:32/unsigned-big, ChunkDirectory:ChunkDirBytes/binary,
        _PageDirectory:PageDirBytes/binary, Payload/binary>>,
    Candidates
) ->
    Wanted = maps:from_list([{DocId, true} || {DocId, _} <- Candidates]),
    client_decode_v8_direct_chunks(
        ChunkDirectory, Payload, Wanted, #{}
    ).

client_decode_v8_direct_chunks(<<>>, _Payload, _Wanted, Acc) ->
    Acc;
client_decode_v8_direct_chunks(
    <<First:64/unsigned-big, Last:64/unsigned-big, Offset:32/unsigned-big,
        Bytes:16/unsigned-big, Count:16/unsigned-big, _MaxBound:8,
        RestDirectory/binary>>,
    Payload,
    Wanted,
    Acc
) ->
    ChunkWanted =
        case Wanted of
            all ->
                all;
            _ ->
                maps:filter(
                    fun(DocId, _Value) ->
                        DocId >= First andalso DocId =< Last
                    end,
                    Wanted
                )
        end,
    Acc1 =
        case ChunkWanted =:= all orelse map_size(ChunkWanted) > 0 of
            true ->
                Chunk = binary:part(Payload, Offset, Bytes),
                client_decode_v8_direct_chunk(
                    Count, First, First, Chunk, true, ChunkWanted, Acc
                );
            false ->
                Acc
        end,
    client_decode_v8_direct_chunks(
        RestDirectory, Payload, Wanted, Acc1
    ).

client_decode_v8_direct_chunk(
    0, _First, _Previous, _Payload, _Initial, _Wanted, Acc
) ->
    Acc;
client_decode_v8_direct_chunk(
    Count, First, Previous, Payload, Initial, Wanted, Acc
) ->
    {DocId, ValueBin} =
        case Initial of
            true ->
                {First, Payload};
            false ->
                {Delta, Rest0} = client_decode_page_varint(Payload),
                {Previous + Delta, Rest0}
        end,
    {Length, CountBin} = client_decode_page_varint(ValueBin),
    {Tf, Rest} = client_decode_page_varint(CountBin),
    Acc1 =
        case Wanted =:= all orelse maps:is_key(DocId, Wanted) of
            false ->
                Acc;
            true ->
                case maps:get(DocId, Acc, none) of
                    {DocId, ExistingLength, ExistingTf} ->
                        Acc#{
                            DocId =>
                                {DocId, ExistingLength, ExistingTf + Tf}
                        };
                    _ ->
                        Acc#{DocId => {DocId, Length, Tf}}
                end
        end,
    client_decode_v8_direct_chunk(
        Count - 1, First, DocId, Rest, false, Wanted, Acc1
    ).

-ifdef(TEST).
client_note_direct_page_decode() ->
    case erlang:get({?MODULE, direct_page_decodes}) of
        Count when is_integer(Count) ->
            erlang:put({?MODULE, direct_page_decodes}, Count + 1),
            ok;
        _ ->
            ok
    end.
-else.
client_note_direct_page_decode() ->
    ok.
-endif.

client_decode_direct_term_rows(0, _Directory, _Payload, Acc) ->
    Acc;
client_decode_direct_term_rows(N, Directory, Payload, Acc) ->
    Index = N - 1,
    <<_Hash:32/unsigned-big, Offset:32/unsigned-big>> =
        binary:part(
            Directory, Index * ?PAGE_DIR_STRIDE, ?PAGE_DIR_STRIDE
        ),
    {DocId, Length, Tf} = client_decode_direct_term_entry(Payload, Offset),
    Acc1 =
        case maps:get(DocId, Acc, none) of
            {DocId, ExistingLength, ExistingTf} ->
                Acc#{
                    DocId =>
                        {DocId, ExistingLength, ExistingTf + Tf}
                };
            _ ->
                Acc#{DocId => {DocId, Length, Tf}}
        end,
    client_decode_direct_term_rows(
        Index, Directory, Payload, Acc1
    ).

client_decode_direct_term_entry(Payload, Offset) ->
    Body = binary:part(Payload, Offset, byte_size(Payload) - Offset),
    {DocId, LengthBin} = client_decode_page_varint(Body),
    {Length, CountBin} = client_decode_page_varint(LengthBin),
    {Tf, _Rest} = client_decode_page_varint(CountBin),
    {DocId, Length, Tf}.

client_search_direct_boolean_store(Bookie, Schema, AST, Opts) ->
    case maps:get(rank, Opts, none) of
        bm25 ->
            client_search_ranked_boolean_store(
                Bookie, Schema, client_boolean_plan(AST), Opts
            );
        none ->
            client_search_unranked_boolean_store(Bookie, Schema, AST, Opts)
    end.

client_search_unranked_boolean_store(Bookie, Schema, AST, Opts) ->
    Plan = client_boolean_plan(AST),
    Terms =
        case Plan of
            {'and', AndTerms} -> AndTerms;
            {'not', Positive, Negative} -> [Positive, Negative]
        end,
    Sources = [
        client_direct_boolean_source(Bookie, Schema, Term)
     || Term <- Terms
    ],
    NHits = [length(Source) || Source <- Sources],
    ScoreNHits =
        case Plan of
            {'not', _, _} -> [hd(NHits)];
            {'and', _} -> NHits
        end,
    Ranked = maps:get(rank, Opts, none) =:= bm25,
    Cap =
        case Ranked of
            true ->
                infinity;
            false ->
                maps:get(offset, Opts, 0) +
                    maps:get(limit, Opts, ?DEFAULT_LIMIT) + 32
        end,
    Matches =
        case {Plan, Sources} of
            {{'and', _AndTerms}, AndSources} ->
                client_kway_and(AndSources, Cap, []);
            {{'not', _Positive, _Negative}, [PositiveSource, NegativeSource]} ->
                client_kway_not(
                    PositiveSource, NegativeSource, Cap, []
                )
        end,
    {DocCount, TotalLength} =
        case Ranked of
            true -> client_corpus_stats(Bookie, Schema);
            false -> {0, 0}
        end,
    AvgLength =
        case DocCount of
            0 -> 0.0;
            _ -> TotalLength / DocCount
        end,
    Idfs =
        case Ranked of
            true -> client_direct_bm25_idfs(ScoreNHits, DocCount);
            false -> []
        end,
    Hits0 = [
        #{
            key => DocId,
            score =>
                case Ranked of
                    true ->
                        client_direct_bm25_score_precomputed(
                            Tfs, Idfs, AvgLength, Length
                        );
                    false ->
                        0.0
                end,
            doc_length => Length,
            match_count => lists:sum(Tfs)
        }
     || {DocId, Length, Tfs} <- Matches
    ],
    Hits =
        case Ranked of
            true ->
                client_bounded_ranked_hits(
                    Bookie, Schema, Hits0, client_ranked_window_size(Opts)
                );
            false ->
                Hits0
        end,
    client_search_result(Bookie, Schema, Hits, length(Matches), Opts).

client_search_ranked_boolean_store(
    Bookie, Schema, {'tree', AST}, Opts
) ->
    ScoringTerms = scoring_phrases(AST),
    Terms = lists:usort(client_exact_boolean_terms(AST)),
    NHits = maps:from_list([
        {Term, client_ranked_boolean_term_nhit(Bookie, Schema, Term)}
     || Term <- Terms
    ]),
    {Candidates, ScoreCandidates} =
        case AST of
            {'not', Positive, Negative} ->
                Included = client_ranked_boolean_tree(
                    Bookie, Schema, Positive, NHits, all
                ),
                Excluded = client_ranked_boolean_exclusions(
                    Bookie, Schema, Negative, NHits, Included
                ),
                {maps:without(maps:keys(Excluded), Included), Included};
            _ ->
                Matches = client_ranked_boolean_tree(
                    Bookie, Schema, AST, NHits, all
                ),
                {Matches, Matches}
        end,
    client_finish_ranked_boolean_tree(
        Bookie,
        Schema,
        Candidates,
        ScoreCandidates,
        ScoringTerms,
        NHits,
        Opts
    );
client_search_ranked_boolean_store(
    Bookie, Schema, {'or', Terms}, Opts
) ->
    IndexedDocs = [
        {Term, Index,
            client_ranked_boolean_anchor(Bookie, Schema, Term)}
     || {Term, Index} <- lists:zip(Terms, lists:seq(1, length(Terms)))
    ],
    Candidates = lists:foldl(
        fun({Term, Index, Docs}, Acc) ->
            client_merge_ranked_boolean_or_term(
                Term, Index, Docs, Acc
            )
        end,
        #{},
        IndexedDocs
    ),
    NHits = [map_size(Docs) || {_Term, _Index, Docs} <- IndexedDocs],
    client_finish_ranked_boolean_or(
        Bookie, Schema, Candidates, NHits, Opts
    );
client_search_ranked_boolean_store(
    Bookie, Schema, {'and', Terms}, Opts
) ->
    IndexedTerms = lists:zip3(
        Terms,
        lists:seq(1, length(Terms)),
        [
            client_ranked_boolean_term_nhit(Bookie, Schema, Term)
         || Term <- Terms
        ]
    ),
    [{Anchor, AnchorIndex, _AnchorNHit} | Rest] =
        client_order_ranked_boolean_terms(IndexedTerms),
    AnchorDocs = client_ranked_boolean_anchor(
        Bookie, Schema, Anchor
    ),
    Candidates0 = maps:map(
        fun(_DocId, {Length, Tf}) ->
            {Length, #{AnchorIndex => Tf}}
        end,
        AnchorDocs
    ),
    Candidates = lists:foldl(
        fun({Term, Index, _NHit}, Acc) ->
            client_ranked_boolean_and_probe(
                Bookie, Schema, Term, Index, Acc
            )
        end,
        Candidates0,
        Rest
    ),
    NHits = [
        NHit
     || {_Term, _Index, NHit} <-
            lists:sort(
                fun({_TA, IA, _NA}, {_TB, IB, _NB}) -> IA =< IB end,
                IndexedTerms
            )
    ],
    client_finish_ranked_boolean(
        Bookie, Schema, Candidates, NHits, Opts
    );
client_search_ranked_boolean_store(
    Bookie, Schema, {'not', Positive, Negative}, Opts
) ->
    PositiveNHit = client_ranked_boolean_term_nhit(
        Bookie, Schema, Positive
    ),
    PositiveDocs = client_ranked_boolean_anchor(
        Bookie, Schema, Positive
    ),
    Candidates0 = maps:map(
        fun(_DocId, {Length, Tf}) -> {Length, #{1 => Tf}} end,
        PositiveDocs
    ),
    Excluded = client_ranked_boolean_probe(
        Bookie, Schema, Negative, Candidates0
    ),
    Candidates = maps:without(maps:keys(Excluded), Candidates0),
    client_finish_ranked_boolean(
        Bookie, Schema, Candidates, [PositiveNHit], Opts
    ).

client_exact_boolean_terms({term, _Token, false, _Columns} = Term) ->
    [Term];
client_exact_boolean_terms({'and', A, B}) ->
    client_exact_boolean_terms(A) ++ client_exact_boolean_terms(B);
client_exact_boolean_terms({'or', A, B}) ->
    client_exact_boolean_terms(A) ++ client_exact_boolean_terms(B);
client_exact_boolean_terms({'not', A, B}) ->
    client_exact_boolean_terms(A) ++ client_exact_boolean_terms(B).

client_ranked_boolean_tree(
    Bookie,
    Schema,
    {term, Token, false, _Columns} = Term,
    _NHits,
    Candidates
) ->
    Docs =
        case Candidates of
            all ->
                client_ranked_boolean_anchor(Bookie, Schema, Term);
            _ ->
                client_ranked_boolean_probe(
                    Bookie, Schema, Term, Candidates
                )
        end,
    maps:map(
        fun(_DocId, {Length, Tf}) ->
            {Length, #{Term => Tf}, [Token]}
        end,
        Docs
    );
client_ranked_boolean_tree(
    Bookie, Schema, {'and', A, B}, NHits, Candidates
) ->
    {First, Second} =
        client_order_boolean_tree_branches(A, B, NHits),
    FirstMatches = client_ranked_boolean_tree(
        Bookie, Schema, First, NHits, Candidates
    ),
    SecondMatches = client_ranked_boolean_tree(
        Bookie, Schema, Second, NHits, FirstMatches
    ),
    client_intersect_ranked_boolean_trees(
        FirstMatches, SecondMatches
    );
client_ranked_boolean_tree(
    Bookie, Schema, {'or', A, B}, NHits, Candidates
) ->
    Left = client_ranked_boolean_tree(
        Bookie, Schema, A, NHits, Candidates
    ),
    Right = client_ranked_boolean_tree(
        Bookie, Schema, B, NHits, Candidates
    ),
    client_union_ranked_boolean_trees(Left, Right);
client_ranked_boolean_tree(
    Bookie, Schema, {'not', A, B}, NHits, Candidates
) ->
    Included = client_ranked_boolean_tree(
        Bookie, Schema, A, NHits, Candidates
    ),
    Excluded = client_ranked_boolean_tree(
        Bookie, Schema, B, NHits, Included
    ),
    maps:without(maps:keys(Excluded), Included).

client_ranked_boolean_exclusions(
    Bookie,
    Schema,
    {term, _Token, false, _Columns} = Term,
    _NHits,
    Candidates
) ->
    Matches = client_ranked_boolean_probe(
        Bookie, Schema, Term, Candidates
    ),
    maps:with(maps:keys(Matches), Candidates);
client_ranked_boolean_exclusions(
    Bookie, Schema, {'and', A, B}, NHits, Candidates
) ->
    First = client_ranked_boolean_exclusions(
        Bookie, Schema, A, NHits, Candidates
    ),
    client_ranked_boolean_exclusions(
        Bookie, Schema, B, NHits, First
    );
client_ranked_boolean_exclusions(
    Bookie, Schema, {'or', A, B}, NHits, Candidates
) ->
    {FirstBranch, SecondBranch} =
        client_order_boolean_exclusion_branches(A, B, NHits),
    First = client_ranked_boolean_exclusions(
        Bookie, Schema, FirstBranch, NHits, Candidates
    ),
    Remaining = maps:without(maps:keys(First), Candidates),
    Second = client_ranked_boolean_exclusions(
        Bookie, Schema, SecondBranch, NHits, Remaining
    ),
    maps:merge(First, Second);
client_ranked_boolean_exclusions(
    Bookie, Schema, {'not', A, B}, NHits, Candidates
) ->
    Included = client_ranked_boolean_exclusions(
        Bookie, Schema, A, NHits, Candidates
    ),
    Excluded = client_ranked_boolean_exclusions(
        Bookie, Schema, B, NHits, Included
    ),
    maps:without(maps:keys(Excluded), Included).

client_order_boolean_exclusion_branches(A, B, NHits) ->
    case
        client_boolean_tree_estimate(A, NHits) >=
            client_boolean_tree_estimate(B, NHits)
    of
        true -> {A, B};
        false -> {B, A}
    end.

client_order_boolean_tree_branches(A, B, NHits) ->
    case
        client_boolean_tree_estimate(A, NHits) =<
            client_boolean_tree_estimate(B, NHits)
    of
        true -> {A, B};
        false -> {B, A}
    end.

client_boolean_tree_estimate({term, _Token, false, _Columns} = Term, NHits) ->
    maps:get(Term, NHits);
client_boolean_tree_estimate({'and', A, B}, NHits) ->
    erlang:min(
        client_boolean_tree_estimate(A, NHits),
        client_boolean_tree_estimate(B, NHits)
    );
client_boolean_tree_estimate({'or', A, B}, NHits) ->
    client_boolean_tree_estimate(A, NHits) +
        client_boolean_tree_estimate(B, NHits);
client_boolean_tree_estimate({'not', A, _B}, NHits) ->
    client_boolean_tree_estimate(A, NHits).

client_intersect_ranked_boolean_trees(Left, Right) ->
    maps:fold(
        fun(DocId, RightValue, Acc) ->
            case maps:find(DocId, Left) of
                {ok, LeftValue} ->
                    Acc#{
                        DocId =>
                            client_merge_ranked_boolean_tree_value(
                                LeftValue, RightValue
                            )
                    };
                error ->
                    Acc
            end
        end,
        #{},
        Right
    ).

client_union_ranked_boolean_trees(Left, Right) ->
    maps:fold(
        fun(DocId, RightValue, Acc) ->
            case maps:find(DocId, Acc) of
                {ok, LeftValue} ->
                    Acc#{
                        DocId =>
                            client_merge_ranked_boolean_tree_value(
                                LeftValue, RightValue
                            )
                    };
                error ->
                    Acc#{DocId => RightValue}
            end
        end,
        Left,
        Right
    ).

client_merge_ranked_boolean_tree_value(
    {Length, LeftTfs, LeftTerms},
    {Length, RightTfs, RightTerms}
) ->
    {
        Length,
        maps:merge(LeftTfs, RightTfs),
        lists:usort(LeftTerms ++ RightTerms)
    }.

client_merge_ranked_boolean_or_term(
    {term, Token, false, _Columns}, Index, Docs, Acc
) ->
    maps:fold(
        fun(DocId, {Length, Tf}, CandidateAcc) ->
            case maps:find(DocId, CandidateAcc) of
                error ->
                    CandidateAcc#{
                        DocId => {Length, #{Index => Tf}, [Token]}
                    };
                {ok, {Length, Tfs, MatchedTerms}} ->
                    CandidateAcc#{
                        DocId =>
                            {
                                Length,
                                Tfs#{Index => Tf},
                                lists:usort([Token | MatchedTerms])
                            }
                    };
                {ok, {_OtherLength, _Tfs, _MatchedTerms}} ->
                    CandidateAcc
            end
        end,
        Acc,
        Docs
    ).

client_finish_ranked_boolean_or(
    Bookie, Schema, Candidates, NHits, Opts
) ->
    {DocCount, TotalLength} = client_corpus_stats(Bookie, Schema),
    AvgLength =
        case DocCount of
            0 -> 0.0;
            _ -> TotalLength / DocCount
        end,
    Idfs = client_direct_bm25_idfs(NHits, DocCount),
    ReturnTerms = maps:get(return_terms, Opts, false),
    CandidateFun =
        fun(DocId, Candidate) ->
            client_ranked_boolean_or_candidate(
                DocId,
                Candidate,
                length(NHits),
                Idfs,
                AvgLength,
                ReturnTerms
            )
        end,
    Window = client_ranked_window_size(Opts),
    Tree =
        case Window > 0 of
            true ->
                maps:fold(
                    fun(DocId, Candidate, Acc) ->
                        client_ranked_heap_add(
                            CandidateFun(DocId, Candidate), Window, Acc
                        )
                    end,
                    gb_trees:empty(),
                    Candidates
                );
            false ->
                gb_trees:empty()
        end,
    TopCandidates = [
        client_materialize_ranked_candidate(Hit)
     || {_RankKey, Hit} <- gb_trees:to_list(Tree)
    ],
    Resolved = client_resolve_hit_batch(
        Bookie, Schema, TopCandidates
    ),
    RankedHits =
        case length(Resolved) =:= erlang:min(Window, map_size(Candidates)) of
            true ->
                Resolved;
            false ->
                Hits = maps:fold(
                    fun(DocId, Candidate, Acc) ->
                        [CandidateFun(DocId, Candidate) | Acc]
                    end,
                    [],
                    Candidates
                ),
                client_bounded_ranked_hits(
                    Bookie, Schema, Hits, Window
                )
        end,
    client_search_result(
        Bookie, Schema, RankedHits, map_size(Candidates), Opts
    ).

client_ranked_boolean_or_candidate(
    DocId,
    {Length, TfsByIndex, MatchedTerms},
    TermCount,
    Idfs,
    AvgLength,
    ReturnTerms
) ->
    Tfs = [
        maps:get(Index, TfsByIndex, 0)
     || Index <- lists:seq(1, TermCount)
    ],
    Extra =
        case ReturnTerms of
            true -> {matched_terms, MatchedTerms};
            false -> none
        end,
    client_ranked_candidate(
        client_direct_bm25_score_precomputed(
            Tfs, Idfs, AvgLength, Length
        ),
        DocId,
        Length,
        lists:sum(Tfs),
        Extra
    ).

client_finish_ranked_boolean_tree(
    Bookie,
    Schema,
    Candidates,
    ScoreCandidates,
    ScoringTerms,
    _NHits,
    Opts
) ->
    {DocCount, TotalLength} = client_corpus_stats(Bookie, Schema),
    AvgLength =
        case DocCount of
            0 -> 0.0;
            _ -> TotalLength / DocCount
        end,
    ScoringNHits = [
        maps:fold(
            fun
                (_DocId, {_Length, TfsByTerm, _MatchedTerms}, Count) ->
                    case maps:get(Term, TfsByTerm, 0) > 0 of
                        true -> Count + 1;
                        false -> Count
                    end
            end,
            0,
            ScoreCandidates
        )
     || Term <- ScoringTerms
    ],
    Idfs = client_direct_bm25_idfs(
        ScoringNHits, DocCount
    ),
    ReturnTerms = maps:get(return_terms, Opts, false),
    Hits = maps:fold(
        fun(DocId, {Length, TfsByTerm, MatchedTerms}, Acc) ->
            Tfs = [
                maps:get(Term, TfsByTerm, 0)
             || Term <- ScoringTerms
            ],
            Extra =
                case ReturnTerms of
                    true -> {matched_terms, MatchedTerms};
                    false -> none
                end,
            [
                client_ranked_candidate(
                    client_direct_bm25_score_precomputed(
                        Tfs, Idfs, AvgLength, Length
                    ),
                    DocId,
                    Length,
                    lists:sum(Tfs),
                    Extra
                )
                | Acc
            ]
        end,
        [],
        Candidates
    ),
    RankedHits = client_bounded_ranked_hits(
        Bookie, Schema, Hits, client_ranked_window_size(Opts)
    ),
    client_search_result(
        Bookie, Schema, RankedHits, map_size(Candidates), Opts
    ).

client_order_ranked_boolean_terms(Terms) ->
    lists:sort(
        fun({_TermA, _IndexA, NHitA}, {_TermB, _IndexB, NHitB}) ->
            NHitA =< NHitB
        end,
        Terms
    ).

client_ranked_boolean_term_nhit(
    Bookie, Schema, {term, Token, false, Columns}
) ->
    client_ranked_boolean_term_nhit(
        Bookie,
        Schema,
        Token,
        client_selector_column_ids(Columns, Schema)
    ).

client_ranked_boolean_term_nhit(_Bookie, _Schema, _Token, []) ->
    0;
client_ranked_boolean_term_nhit(
    Bookie, Schema, Token, [Column | Rest]
) ->
    Bucket = maps:get(index, Schema),
    case
        client_read_cached_direct_term_page(
            Bookie, Bucket, client_token_key(Token), Column, 0
        )
    of
        not_found ->
            client_ranked_boolean_term_nhit(
                Bookie, Schema, Token, Rest
            );
        {ok, HeadPage} ->
            maps:get(total_docs, HeadPage)
    end.

client_ranked_boolean_anchor(
    Bookie, Schema, {term, Token, false, Columns}
) ->
    lists:foldl(
        fun(Column, Acc) ->
            client_merge_ranked_boolean_term(
                client_read_cached_direct_term_pages(
                    Bookie, Schema, Token, Column
                ),
                Acc
            )
        end,
        #{},
        client_selector_column_ids(Columns, Schema)
    ).

client_ranked_boolean_and_probe(
    _Bookie, _Schema, _Term, _Index, Candidates
) when map_size(Candidates) =:= 0 ->
    Candidates;
client_ranked_boolean_and_probe(
    Bookie, Schema, Term, Index, Candidates
) ->
    Matches = client_ranked_boolean_probe(
        Bookie, Schema, Term, Candidates
    ),
    maps:fold(
        fun(DocId, {Length, Tfs}, Acc) ->
            case maps:find(DocId, Matches) of
                {ok, {Length, Tf}} ->
                    Acc#{DocId => {Length, Tfs#{Index => Tf}}};
                _ ->
                    Acc
            end
        end,
        #{},
        Candidates
    ).

client_ranked_boolean_probe(
    Bookie,
    #{index := Bucket} = Schema,
    {term, Token, false, Columns} = Term,
    Candidates
) ->
    case client_clean_stats_epoch(Bookie, Bucket) of
        {ok, _Epoch} ->
            lists:foldl(
                fun(Column, Acc) ->
                    Docs = client_read_cached_direct_term_pages(
                        Bookie, Schema, Token, Column
                    ),
                    Matches = maps:with(
                        maps:keys(Candidates), Docs
                    ),
                    client_merge_ranked_boolean_term(
                        Matches, Acc
                    )
                end,
                #{},
                client_selector_column_ids(Columns, Schema)
            );
        dirty ->
            client_ranked_boolean_probe_pages(
                Bookie, Schema, Term, Candidates
            )
    end.

client_ranked_boolean_probe_pages(
    Bookie, Schema, {term, Token, false, Columns}, Candidates
) ->
    lists:foldl(
        fun(Column, Acc) ->
            Head = client_read_boolean_head(
                Bookie, Schema, Token, Column
            ),
            Source = client_read_boolean_probe_source(
                Bookie, Schema, Token, Column, Head, Candidates
            ),
            client_merge_ranked_boolean_term(
                client_ranked_boolean_probe_source(Source), Acc
            )
        end,
        #{},
        client_selector_column_ids(Columns, Schema)
    ).

client_ranked_boolean_probe_source(missing) ->
    #{};
client_ranked_boolean_probe_source(
    {boolean_pages, Bookie, Bucket, Key, Column, CandidateGroups, Pages}
) ->
    PageNumbers = maps:keys(CandidateGroups),
    Rows = [
        {Key, client_page_subkey(?BOOLEAN_PLANE, Column, PageNo)}
     || PageNo <- PageNumbers
    ],
    SQNs = maps:from_list(
        leveled_bookie:book_mhead_sqn(Bookie, Bucket, Rows)
    ),
    maps:fold(
        fun(PageNo, Candidates, Acc) ->
            PageValue = maps:get(PageNo, Pages),
            PageDocs =
                case client_page_version(PageValue) of
                    ?PAGE_VERSION ->
                        client_decode_v8_direct_term_candidates(
                            PageValue, Candidates
                        );
                    ?PREVIOUS_PAGE_VERSION ->
                        Row =
                            {Key,
                                client_page_subkey(
                                    ?BOOLEAN_PLANE, Column, PageNo
                                )},
                        {ok, SQN} = maps:get(Row, SQNs),
                        {ok, Page} = client_read_cached_direct_term_page(
                            Bookie,
                            Bucket,
                            Key,
                            Column,
                            PageNo,
                            SQN,
                            PageValue
                        ),
                        maps:get(docs, Page)
                end,
            lists:foldl(
                fun({DocId, Candidate}, PageAcc) ->
                    CandidateLength = element(1, Candidate),
                    case maps:find(DocId, PageDocs) of
                        {ok, {DocId, CandidateLength, Tf}} ->
                            client_merge_ranked_boolean_doc(
                                DocId, CandidateLength, Tf, PageAcc
                            );
                        _ ->
                            PageAcc
                    end
                end,
                Acc,
                Candidates
            )
        end,
        #{},
        CandidateGroups
    ).

client_merge_ranked_boolean_term(Docs, Acc) ->
    maps:fold(
        fun(DocId, Value, TermAcc) ->
            {Length, Tf} =
                case Value of
                    {DocId, DocLength, Count} -> {DocLength, Count};
                    {DocLength, Count} -> {DocLength, Count}
                end,
            client_merge_ranked_boolean_doc(
                DocId, Length, Tf, TermAcc
            )
        end,
        Acc,
        Docs
    ).

client_merge_ranked_boolean_doc(DocId, Length, Tf, Acc) ->
    case maps:find(DocId, Acc) of
        {ok, {Length, ExistingTf}} ->
            Acc#{DocId => {Length, ExistingTf + Tf}};
        {ok, {_OtherLength, _ExistingTf}} ->
            Acc;
        error ->
            Acc#{DocId => {Length, Tf}}
    end.

client_finish_ranked_boolean(
    Bookie, Schema, Candidates, NHits, Opts
) ->
    {DocCount, TotalLength} = client_corpus_stats(Bookie, Schema),
    AvgLength =
        case DocCount of
            0 -> 0.0;
            _ -> TotalLength / DocCount
        end,
    Idfs = client_direct_bm25_idfs(NHits, DocCount),
    Hits = maps:fold(
        fun(DocId, {Length, TfsByIndex}, Acc) ->
            Tfs = [
                maps:get(Index, TfsByIndex)
             || Index <- lists:seq(1, length(NHits))
            ],
            [
                client_ranked_candidate(
                    client_direct_bm25_score_precomputed(
                        Tfs, Idfs, AvgLength, Length
                    ),
                    DocId,
                    Length,
                    lists:sum(Tfs)
                )
                | Acc
            ]
        end,
        [],
        Candidates
    ),
    RankedHits = client_bounded_ranked_hits(
        Bookie, Schema, Hits, client_ranked_window_size(Opts)
    ),
    client_search_result(
        Bookie, Schema, RankedHits, map_size(Candidates), Opts
    ).

client_direct_boolean_source(
    Bookie,
    Schema,
    {term, Token, false, Columns}
) ->
    Docs = lists:foldl(
        fun(Column, Acc) ->
            client_merge_direct_term_docs(
                client_read_cached_direct_term_pages(
                    Bookie, Schema, Token, Column
                ),
                Acc
            )
        end,
        #{},
        client_selector_column_ids(Columns, Schema)
    ),
    lists:sort([
        {DocId, Length, Tf}
     || {DocId, {_Version, Length, Tf}} <- maps:to_list(Docs)
    ]).

client_kway_and(Sources, Cap, Acc) ->
    client_kway_and(Sources, Cap, 0, Acc).

client_kway_and(Sources, _Cap, _Count, Acc) when length(Sources) =:= 0 ->
    lists:reverse(Acc);
client_kway_and(Sources, Cap, Count, Acc) ->
    case lists:any(fun(Source) -> Source =:= [] end, Sources) of
        true ->
            lists:reverse(Acc);
        false ->
            MaxDocId = lists:max([
                DocId
             || [{DocId, _Length, _Tf} | _] <- Sources
            ]),
            Advanced = [
                lists:dropwhile(
                    fun({DocId, _Length, _Tf}) -> DocId < MaxDocId end,
                    Source
                )
             || Source <- Sources
            ],
            case lists:any(fun(Source) -> Source =:= [] end, Advanced) of
                true ->
                    lists:reverse(Acc);
                false ->
                    Heads = [Head || [Head | _] <- Advanced],
                    case
                        lists:all(
                            fun({DocId, _Length, _Tf}) ->
                                DocId =:= MaxDocId
                            end,
                            Heads
                        )
                    of
                        true ->
                            [{MaxDocId, Length, _FirstTf} | _] = Heads,
                            Tfs = [Tf || {_DocId, _Length, Tf} <- Heads],
                            Acc1 = [{MaxDocId, Length, Tfs} | Acc],
                            Count1 = Count + 1,
                            case client_merge_cap_reached(Cap, Count1) of
                                true ->
                                    lists:reverse(Acc1);
                                false ->
                                    client_kway_and(
                                        [tl(Source) || Source <- Advanced],
                                        Cap,
                                        Count1,
                                        Acc1
                                    )
                            end;
                        false ->
                            client_kway_and(Advanced, Cap, Count, Acc)
                    end
            end
    end.

client_kway_not(Positive, Negative, Cap, Acc) ->
    client_kway_not(Positive, Negative, Cap, 0, Acc).

client_kway_not([], _Negative, _Cap, _Count, Acc) ->
    lists:reverse(Acc);
client_kway_not(Positive, [], Cap, Count, Acc) ->
    client_take_positive(Positive, Cap, Count, Acc);
client_kway_not(
    [{PositiveId, Length, Tf} | PositiveRest] = Positive,
    [{NegativeId, _NegativeLength, _NegativeTf} | NegativeRest] = Negative,
    Cap,
    Count,
    Acc
) ->
    if
        PositiveId < NegativeId ->
            Acc1 = [{PositiveId, Length, [Tf]} | Acc],
            Count1 = Count + 1,
            case client_merge_cap_reached(Cap, Count1) of
                true ->
                    lists:reverse(Acc1);
                false ->
                    client_kway_not(
                        PositiveRest, Negative, Cap, Count1, Acc1
                    )
            end;
        PositiveId =:= NegativeId ->
            client_kway_not(
                PositiveRest, NegativeRest, Cap, Count, Acc
            );
        true ->
            client_kway_not(
                Positive, NegativeRest, Cap, Count, Acc
            )
    end.

client_take_positive([], _Cap, _Count, Acc) ->
    lists:reverse(Acc);
client_take_positive(
    [{DocId, Length, Tf} | Rest], Cap, Count, Acc
) ->
    Acc1 = [{DocId, Length, [Tf]} | Acc],
    Count1 = Count + 1,
    case client_merge_cap_reached(Cap, Count1) of
        true -> lists:reverse(Acc1);
        false -> client_take_positive(Rest, Cap, Count1, Acc1)
    end.

client_merge_cap_reached(infinity, _Count) ->
    false;
client_merge_cap_reached(Cap, Count) ->
    Count >= Cap.

client_search_direct_not_store(
    Bookie,
    Schema,
    {'not', {term, Positive, false, PositiveColumns},
        {term, Negative, false, NegativeColumns}},
    Opts
) ->
    PositiveDocs = lists:foldl(
        fun(Column, Acc) ->
            Head = client_read_boolean_head(
                Bookie, Schema, Positive, Column
            ),
            Pages = client_read_boolean_values_from_head(
                Bookie, Schema, Positive, Column, all, Head
            ),
            maps:merge(Acc, client_decode_boolean_compact_filtered(Pages))
        end,
        #{},
        client_selector_column_ids(PositiveColumns, Schema)
    ),
    Survivors = lists:foldl(
        fun(Column, Candidates) ->
            Head = client_read_boolean_head(
                Bookie, Schema, Negative, Column
            ),
            Source = client_read_boolean_probe_source(
                Bookie, Schema, Negative, Column, Head, Candidates
            ),
            Excluded = client_intersect_boolean_docids(
                Source, member
            ),
            maps:without(
                maps:keys(Excluded), Candidates
            )
        end,
        PositiveDocs,
        client_selector_column_ids(NegativeColumns, Schema)
    ),
    Hits = lists:sort(
        fun(A, B) -> maps:get(key, A) =< maps:get(key, B) end,
        [
            #{key => DocId, score => 0.0, doc_length => Length}
         || {DocId, {_Version, Length}} <- maps:to_list(Survivors)
        ]
    ),
    client_search_result(Bookie, Schema, Hits, map_size(Survivors), Opts).

client_search_direct_phrase_store(
    Bookie,
    Schema,
    {phrase, Specs, Columns},
    Opts
) ->
    Items = [
        {term, Token, false, Columns}
     || {Token, false, _Offset} <- Specs
    ],
    Offsets = [Offset || {_Token, false, Offset} <- Specs],
    client_search_direct_position_store(
        Bookie, Schema, Items, Columns, Opts, {phrase, Offsets}
    ).

client_search_direct_near_store(
    Bookie,
    Schema,
    {near,
        [
            {term, _TokenA, false, _ColumnsA},
            {term, _TokenB, false, _ColumnsB}
        ] = Items,
        Distance, Columns},
    Opts
) ->
    client_search_direct_binary_near_store(
        Bookie, Schema, Items, Columns, Distance, Opts
    );
client_search_direct_near_store(
    Bookie,
    Schema,
    {near, Items, Distance, Columns},
    Opts
) ->
    client_search_direct_position_store(
        Bookie, Schema, Items, Columns, Opts, {near, Distance}
    ).

client_search_direct_binary_near_store(
    Bookie,
    Schema,
    [
        {term, TokenA, false, _ColumnsA},
        {term, TokenB, false, _ColumnsB}
    ] = Items,
    Columns,
    Distance,
    Opts
) ->
    case client_defer_position_verification(Opts) of
        true ->
            client_search_direct_position_deferred(
                Bookie,
                Schema,
                Items,
                Columns,
                Opts,
                {near, Distance}
            );
        false ->
            client_search_direct_binary_near_store_eager(
                Bookie,
                Schema,
                Items,
                Columns,
                Distance,
                Opts,
                TokenA,
                TokenB
            )
    end.

client_search_direct_binary_near_store_eager(
    Bookie,
    Schema,
    Items,
    Columns,
    Distance,
    Opts,
    TokenA,
    TokenB
) ->
    ColumnIds = client_selector_column_ids(Columns, Schema),
    Ranked = maps:get(rank, Opts, none) =:= bm25,
    ReturnPositions = maps:get(return_positions, Opts, false),
    {Matches, SourcesByColumn, GlobalNHits} = lists:foldl(
        fun(Column, {MatchAcc, SourceAcc, NHitAcc}) ->
            {Candidates, ColumnNHits} = client_direct_binary_near_candidates(
                Bookie, Schema, Items, Column
            ),
            NHits = lists:zipwith(
                fun erlang:max/2, NHitAcc, ColumnNHits
            ),
            case map_size(Candidates) of
                0 ->
                    {MatchAcc, SourceAcc, NHits};
                _ ->
                    {SourceA, SourceB} =
                        client_read_two_position_binary_sources(
                            Bookie,
                            Schema,
                            TokenA,
                            TokenB,
                            Column,
                            Candidates
                        ),
                    ColumnMatches = client_fold_binary_near_candidates(
                        maps:to_list(Candidates),
                        SourceA,
                        SourceB,
                        Ranked,
                        Distance,
                        Column,
                        MatchAcc
                    ),
                    {ColumnMatches, SourceAcc#{Column => {SourceA, SourceB}},
                        NHits}
            end
        end,
        {#{}, #{}, [0, 0]},
        ColumnIds
    ),
    {DocCount, TotalLength} =
        case Ranked of
            true -> client_corpus_stats(Bookie, Schema);
            false -> {0, 0}
        end,
    AvgLength =
        case DocCount of
            0 -> 0.0;
            _ -> TotalLength / DocCount
        end,
    NearIdfs =
        case Ranked of
            true -> client_direct_near_idfs(GlobalNHits, DocCount);
            false -> []
        end,
    HitRows =
        case Ranked of
            true ->
                [
                    client_ranked_candidate(
                        client_direct_near_bm25_score(
                            Tfs, NearIdfs, AvgLength, Length
                        ),
                        DocId,
                        Length,
                        0,
                        {internal_near, Version, MatchColumns}
                    )
                 || {DocId, {Version, Length, Tfs, MatchColumns}} <-
                        maps:to_list(Matches)
                ];
            false ->
                [
                    #{
                        key => DocId,
                        score => 0.0,
                        doc_length => Length,
                        internal_version => Version,
                        internal_columns => MatchColumns
                    }
                 || {DocId, {Version, Length, _Tfs, MatchColumns}} <-
                        maps:to_list(Matches)
                ]
        end,
    SortedRows =
        case Ranked of
            true ->
                client_bounded_ranked_hits(
                    Bookie, Schema, HitRows, client_ranked_window_size(Opts)
                );
            false ->
                lists:sort(
                    fun(A, B) ->
                        maps:get(key, A) =< maps:get(key, B)
                    end,
                    HitRows
                )
        end,
    ResolvedRows = client_resolve_hits_with_ids(
        Bookie, Schema, SortedRows, Opts
    ),
    Hits =
        case ReturnPositions of
            false ->
                [
                    maps:without(
                        [doc_id, internal_version, internal_columns], Hit
                    )
                 || Hit <- ResolvedRows
                ];
            true ->
                [
                    begin
                        DocId = maps:get(doc_id, Hit),
                        Version = maps:get(internal_version, Hit),
                        MatchColumns = maps:get(internal_columns, Hit),
                        NearPositions = client_materialize_binary_near(
                            DocId,
                            Version,
                            MatchColumns,
                            SourcesByColumn,
                            Distance
                        ),
                        maps:without(
                            [doc_id, internal_version, internal_columns],
                            Hit#{
                                match_count => length(NearPositions),
                                positions => client_window_positions(
                                    #{near => NearPositions}
                                )
                            }
                        )
                    end
                 || Hit <- ResolvedRows
                ]
        end,
    client_search_result(Bookie, Schema, Hits, map_size(Matches), Opts).

client_direct_near_idfs(NHits, DocCount) ->
    client_direct_bm25_idfs(NHits, DocCount).

client_direct_bm25_idfs(NHits, DocCount) ->
    [
        client_bm25_idf(NHit, DocCount)
     || NHit <- NHits
    ].

client_bm25_idf(NHit, DocCount) ->
    Idf0 = math:log(
        (DocCount - NHit + 0.5) / (NHit + 0.5)
    ),
    case Idf0 > 0.0 of
        true -> Idf0;
        false -> 1.0e-6
    end.

client_direct_near_bm25_score(Tfs, Idfs, AvgLength, DocLength) ->
    client_direct_bm25_score_precomputed(
        Tfs, Idfs, AvgLength, DocLength
    ).

client_direct_bm25_score_precomputed(
    Tfs, Idfs, AvgLength, DocLength
) ->
    LenRatio =
        case AvgLength > 0.0 of
            true -> DocLength / AvgLength;
            false -> 1.0
        end,
    lists:sum([
        Idf * (Tf * 2.2) /
            (Tf + 1.2 * (0.25 + 0.75 * LenRatio))
     || {Tf, Idf} <- lists:zip(Tfs, Idfs), Tf > 0
    ]).

client_fold_binary_near_candidates(
    [],
    _SourceA,
    _SourceB,
    _Ranked,
    _Distance,
    _Column,
    Acc
) ->
    Acc;
client_fold_binary_near_candidates(
    [{DocKey, {Version, Length}} | Rest],
    SourceA,
    SourceB,
    Ranked,
    Distance,
    Column,
    Acc
) ->
    Hash = client_docid_hash(DocKey),
    Acc1 =
        case
            {
                client_direct_position_binary_lookup(
                    SourceA, DocKey, Hash, Version
                ),
                client_direct_position_binary_lookup(
                    SourceB, DocKey, Hash, Version
                )
            }
        of
            {{ok, PositionsA}, {ok, PositionsB}} ->
                case Ranked of
                    false ->
                        case
                            client_raw_near_any(
                                PositionsA, PositionsB, Distance
                            )
                        of
                            true ->
                                client_merge_binary_near_match(
                                    DocKey, Version, Length, [], Column, Acc
                                );
                            false ->
                                Acc
                        end;
                    true ->
                        {Matched, TfA, TfB} = client_raw_near_tfs(
                            PositionsA, PositionsB, Distance
                        ),
                        case Matched of
                            true ->
                                client_merge_binary_near_match(
                                    DocKey,
                                    Version,
                                    Length,
                                    [TfA, TfB],
                                    Column,
                                    Acc
                                );
                            false ->
                                Acc
                        end
                end;
            _ ->
                Acc
        end,
    client_fold_binary_near_candidates(
        Rest, SourceA, SourceB, Ranked, Distance, Column, Acc1
    ).

client_read_two_position_binary_sources(
    Bookie, Schema, TokenA, TokenB, Column, Candidates
) ->
    Parent = self(),
    RefA = make_ref(),
    RefB = make_ref(),
    spawn(fun() ->
        Parent !
            {RefA,
                try
                    {ok,
                        client_read_direct_position_binary_source(
                            Bookie, Schema, TokenA, Column, Candidates
                        )}
                catch
                    ClassA:ReasonA:StackA ->
                        {error, ClassA, ReasonA, StackA}
                end}
    end),
    spawn(fun() ->
        Parent !
            {RefB,
                try
                    {ok,
                        client_read_direct_position_binary_source(
                            Bookie, Schema, TokenB, Column, Candidates
                        )}
                catch
                    ClassB:ReasonB:StackB ->
                        {error, ClassB, ReasonB, StackB}
                end}
    end),
    SourceA = client_receive_position_binary_source(RefA),
    SourceB = client_receive_position_binary_source(RefB),
    {SourceA, SourceB}.

client_receive_position_binary_source(Ref) ->
    receive
        {Ref, {ok, Source}} ->
            Source;
        {Ref, {error, Class, Reason, Stack}} ->
            erlang:raise(Class, Reason, Stack)
    end.

client_direct_binary_near_candidates(Bookie, Schema, Items, Column) ->
    TokenHeads = [
        {Token, client_read_boolean_head(Bookie, Schema, Token, Column)}
     || {term, Token, false, _ItemColumns} <- Items
    ],
    ColumnNHits = [
        case Head of
            not_found -> 0;
            {ok, Value} -> client_page_global_docs(Value)
        end
     || {_Token, Head} <- TokenHeads
    ],
    Ordered = lists:sort(
        fun({_TokenA, HeadA}, {_TokenB, HeadB}) ->
            client_boolean_head_total(HeadA) =<
                client_boolean_head_total(HeadB)
        end,
        TokenHeads
    ),
    Candidates =
        case Ordered of
            [{_Token, not_found} | _] ->
                #{};
            [{AnchorToken, AnchorHead} | _] ->
                %% The rare boolean stream drives NEAR.  Common-token membership
                %% is checked by the versioned position lookups below, avoiding a
                %% duplicate pass over the common boolean plane.
                AnchorPages = client_read_boolean_values_from_head(
                    Bookie, Schema, AnchorToken, Column, all, AnchorHead
                ),
                client_decode_boolean_compact_filtered(AnchorPages)
        end,
    {Candidates, ColumnNHits}.

client_direct_boolean_candidates(_Bookie, _Schema, _Column, []) ->
    #{};
client_direct_boolean_candidates(
    _Bookie,
    _Schema,
    _Column,
    [{_Token, not_found} | _]
) ->
    #{};
client_direct_boolean_candidates(
    Bookie,
    Schema,
    Column,
    [{AnchorToken, AnchorHead} | Rest]
) ->
    AnchorPages = client_read_boolean_values_from_head(
        Bookie, Schema, AnchorToken, Column, all, AnchorHead
    ),
    Candidates = client_decode_boolean_compact_filtered(AnchorPages),
    lists:foldl(
        fun
            ({_Token, not_found}, _Acc) ->
                #{};
            ({Token, Head}, Acc) when map_size(Acc) > 0 ->
                Source = client_read_boolean_probe_source(
                    Bookie, Schema, Token, Column, Head, Acc
                ),
                client_intersect_boolean_docids(Source, exact);
            ({_Token, _Head}, Acc) ->
                Acc
        end,
        Candidates,
        Rest
    ).

client_receive_parallel_value(Ref) ->
    receive
        {Ref, {ok, Value}} ->
            Value;
        {Ref, {error, Class, Reason, Stack}} ->
            erlang:raise(Class, Reason, Stack)
    end.

client_search_direct_position_store(
    Bookie,
    Schema,
    Items,
    Columns,
    Opts,
    MatchSpec
) ->
    case client_defer_position_verification(Opts) of
        true ->
            client_search_direct_position_deferred(
                Bookie, Schema, Items, Columns, Opts, MatchSpec
            );
        false ->
            client_search_direct_position_store_eager(
                Bookie, Schema, Items, Columns, Opts, MatchSpec
            )
    end.

client_defer_position_verification(Opts) ->
    maps:get(rank, Opts, none) =:= bm25 andalso
        not maps:get(return_count, Opts, false) andalso
        client_ranked_window_size(Opts) > 0.

client_search_direct_position_deferred(
    Bookie,
    Schema,
    Items,
    Columns,
    Opts,
    MatchSpec
) ->
    ColumnIds = client_selector_column_ids(Columns, Schema),
    Candidates = client_deferred_position_candidates(
        Bookie, Schema, Items, ColumnIds
    ),
    {DocCount, TotalLength} = client_corpus_stats(Bookie, Schema),
    AvgLength =
        case DocCount of
            0 -> 0.0;
            _ -> TotalLength / DocCount
        end,
    NHits = client_deferred_position_nhits(
        Bookie, Schema, Items, ColumnIds, MatchSpec
    ),
    Idfs = client_direct_bm25_idfs(NHits, DocCount),
    Ordered = lists:sort(
        fun(
            {ScoreA, DocIdA, _LengthA, _ColumnsA},
            {ScoreB, DocIdB, _LengthB, _ColumnsB}
        ) ->
            {-ScoreA, DocIdA} =< {-ScoreB, DocIdB}
        end,
        [
            begin
                UpperTfs = client_deferred_position_upper_tfs(
                    CandidateColumns, MatchSpec
                ),
                UpperScore = client_direct_bm25_score_precomputed(
                    UpperTfs, Idfs, AvgLength, Length
                ),
                {UpperScore, DocId, Length, CandidateColumns}
            end
         || {DocId, {Length, CandidateColumns}} <-
                maps:to_list(Candidates)
        ]
    ),
    Window = client_ranked_window_size(Opts),
    Hits = client_deferred_position_verify(
        Bookie,
        Schema,
        Items,
        MatchSpec,
        Opts,
        Idfs,
        AvgLength,
        Window,
        Ordered,
        []
    ),
    client_search_result(Bookie, Schema, Hits, 0, Opts).

client_deferred_position_nhits(
    Bookie,
    #{index := Bucket} = Schema,
    Items,
    [Column],
    MatchSpec
) ->
    case client_clean_stats_epoch(Bookie, Bucket) of
        {ok, _Epoch} ->
            Counts = [
                map_size(
                    client_read_cached_direct_term_pages(
                        Bookie, Schema, Token, Column
                    )
                )
             || {term, Token, false, _ItemColumns} <- Items
            ],
            case MatchSpec of
                {near, _Distance} -> Counts;
                {phrase, _Offsets} -> [lists:min(Counts)]
            end;
        dirty ->
            client_deferred_position_nhits_from_heads(
                Bookie, Schema, Items, [Column], MatchSpec
            )
    end;
client_deferred_position_nhits(
    Bookie, Schema, Items, ColumnIds, MatchSpec
) ->
    client_deferred_position_nhits_from_heads(
        Bookie, Schema, Items, ColumnIds, MatchSpec
    ).

client_deferred_position_nhits_from_heads(
    Bookie, Schema, Items, ColumnIds, {near, _Distance}
) ->
    [
        client_token_global_docs(Bookie, Schema, Token, ColumnIds)
     || {term, Token, false, _ItemColumns} <- Items
    ];
client_deferred_position_nhits_from_heads(
    Bookie, Schema, Items, ColumnIds, {phrase, _Offsets}
) ->
    [
        lists:min([
            client_token_global_docs(Bookie, Schema, Token, ColumnIds)
         || {term, Token, false, _ItemColumns} <- Items
        ])
    ].

client_deferred_position_candidates(
    Bookie, Schema, Items, ColumnIds
) ->
    client_deferred_position_candidates_full(
        Bookie, Schema, Items, ColumnIds
    ).

client_deferred_position_candidates_full(
    Bookie, Schema, Items, [Column]
) ->
    TermDocs = [
        client_read_cached_direct_term_pages(
            Bookie, Schema, Token, Column
        )
     || {term, Token, false, _ItemColumns} <- Items
    ],
    maps:map(
        fun(_DocId, {Length, Tfs}) ->
            {Length, #{Column => Tfs}}
        end,
        client_intersect_ranked_position_terms(TermDocs)
    );
client_deferred_position_candidates_full(
    Bookie, Schema, Items, ColumnIds
) ->
    lists:foldl(
        fun(Column, Acc) ->
            TermDocs = [
                client_read_cached_direct_term_pages(
                    Bookie, Schema, Token, Column
                )
             || {term, Token, false, _ItemColumns} <- Items
            ],
            ColumnCandidates =
                client_intersect_ranked_position_terms(TermDocs),
            maps:fold(
                fun(DocId, {Length, Tfs}, CandidateAcc) ->
                    case maps:find(DocId, CandidateAcc) of
                        error ->
                            CandidateAcc#{
                                DocId => {Length, #{Column => Tfs}}
                            };
                        {ok, {Length, ExistingColumns}} ->
                            CandidateAcc#{
                                DocId =>
                                    {Length, ExistingColumns#{Column => Tfs}}
                            };
                        {ok, {_OtherLength, _ExistingColumns}} ->
                            CandidateAcc
                    end
                end,
                Acc,
                ColumnCandidates
            )
        end,
        #{},
        ColumnIds
    ).

client_intersect_ranked_position_terms([]) ->
    #{};
client_intersect_ranked_position_terms([Only]) ->
    maps:map(
        fun(_DocId, {_Version, Length, Tf}) -> {Length, [Tf]} end,
        Only
    );
client_intersect_ranked_position_terms([First, Second | Rest]) ->
    Candidates = maps:intersect_with(
        fun(
            _DocId,
            {_FirstVersion, Length, FirstTf},
            {_SecondVersion, Length, SecondTf}
        ) ->
            {Length, [FirstTf, SecondTf]}
        end,
        First,
        Second
    ),
    lists:foldl(
        fun(TermDocs, Acc) ->
            maps:intersect_with(
                fun(
                    _DocId,
                    {Length, Tfs},
                    {_Version, Length, Tf}
                ) ->
                    {Length, Tfs ++ [Tf]}
                end,
                Acc,
                TermDocs
            )
        end,
        Candidates,
        Rest
    ).

client_deferred_position_upper_tfs(CandidateColumns, {near, _Distance}) ->
    maps:fold(
        fun
            (_Column, Tfs, none) ->
                Tfs;
            (_Column, Tfs, Acc) ->
                lists:zipwith(fun(A, B) -> A + B end, Tfs, Acc)
        end,
        none,
        CandidateColumns
    );
client_deferred_position_upper_tfs(CandidateColumns, {phrase, _Offsets}) ->
    [
        maps:fold(
            fun(_Column, Tfs, Acc) -> lists:min(Tfs) + Acc end,
            0,
            CandidateColumns
        )
    ].

client_deferred_position_verify(
    _Bookie,
    _Schema,
    _Items,
    _MatchSpec,
    _Opts,
    _Idfs,
    _AvgLength,
    _Window,
    [],
    Hits
) ->
    client_bounded_ranked_hits(Hits, length(Hits));
client_deferred_position_verify(
    Bookie,
    Schema,
    Items,
    MatchSpec,
    Opts,
    Idfs,
    AvgLength,
    Window,
    Ordered,
    Hits
) ->
    %% Position pages are shared by many adjacent ranked candidates.  Vet two
    %% result windows at a time so a query that needs a second round does not
    %% fetch and decode the same page range twice.  The upper-bound stopping
    %% test below still runs after every bounded batch.
    BatchSize = client_position_batch_size(Window),
    {Batch, Rest} = client_take_position_batch(BatchSize, Ordered, []),
    Matches = client_verify_position_batch(
        Bookie, Schema, Items, MatchSpec, Batch
    ),
    ReturnPositions = maps:get(return_positions, Opts, false),
    BatchHits = [
        client_ranked_candidate(
            client_direct_bm25_score_precomputed(
                Tfs, Idfs, AvgLength, Length
            ),
            DocId,
            Length,
            position_count(MatchPositions),
            case ReturnPositions of
                true ->
                    {positions,
                        client_window_positions(#{
                            client_direct_match_key(MatchSpec) =>
                                MatchPositions
                        })};
                false ->
                    none
            end
        )
     || {DocId, {Length, MatchPositions, Tfs}} <- maps:to_list(Matches)
    ],
    Hits1 = BatchHits ++ Hits,
    Live = client_bounded_ranked_hits(Bookie, Schema, Hits1, Window),
    case client_deferred_position_floor_met(Rest, Live, Window) of
        true ->
            Live;
        false ->
            client_deferred_position_verify(
                Bookie,
                Schema,
                Items,
                MatchSpec,
                Opts,
                Idfs,
                AvgLength,
                Window,
                Rest,
                Hits1
            )
    end.

client_position_batch_size(Window) ->
    Window * 2.

client_take_position_batch(0, Rest, Acc) ->
    {lists:reverse(Acc), Rest};
client_take_position_batch(_Count, [], Acc) ->
    {lists:reverse(Acc), []};
client_take_position_batch(Count, [Candidate | Rest], Acc) ->
    client_take_position_batch(Count - 1, Rest, [Candidate | Acc]).

client_deferred_position_floor_met([], _Live, _Window) ->
    true;
client_deferred_position_floor_met(_Rest, Live, Window) when
    length(Live) < Window
->
    false;
client_deferred_position_floor_met(
    [{UpperScore, DocId, _Length, _Columns} | _],
    Live,
    _Window
) ->
    Worst = lists:last(Live),
    {-UpperScore, DocId} > client_ranked_candidate_key(Worst).

client_verify_position_batch(Bookie, Schema, Items, MatchSpec, Batch) ->
    Columns = lists:usort(
        lists:append([
            maps:keys(CandidateColumns)
         || {_Score, _DocId, _Length, CandidateColumns} <- Batch
        ])
    ),
    lists:foldl(
        fun(Column, MatchAcc) ->
            Candidates = maps:from_list([
                {DocId, {DocId, Length}}
             || {_Score, DocId, Length, CandidateColumns} <- Batch,
                maps:is_key(Column, CandidateColumns)
            ]),
            PositionRows = [
                client_read_direct_position_source(
                    Bookie, Schema, Token, Column, Candidates
                )
             || {term, Token, false, _ItemColumns} <- Items
            ],
            maps:fold(
                fun(DocKey, {Version, Length}, Acc) ->
                    PositionLists = lists:filtermap(
                        fun(Source) ->
                            case
                                client_direct_position_lookup(
                                    Source, DocKey, Version
                                )
                            of
                                none -> false;
                                Positions -> {true, Positions}
                            end
                        end,
                        PositionRows
                    ),
                    case length(PositionLists) =:= length(Items) of
                        false ->
                            Acc;
                        true ->
                            case
                                client_direct_position_match(
                                    PositionLists, MatchSpec
                                )
                            of
                                [] ->
                                    Acc;
                                MatchPositions ->
                                    Tfs =
                                        case MatchSpec of
                                            {near, Distance} ->
                                                client_direct_near_tfs(
                                                    PositionLists, Distance
                                                );
                                            {phrase, _} ->
                                                [length(MatchPositions)]
                                        end,
                                    client_merge_direct_near_match(
                                        DocKey,
                                        Length,
                                        MatchPositions,
                                        Tfs,
                                        Acc
                                    )
                            end
                    end
                end,
                MatchAcc,
                Candidates
            )
        end,
        #{},
        Columns
    ).

client_search_direct_position_store_eager(
    Bookie,
    Schema,
    Items,
    Columns,
    Opts,
    MatchSpec
) ->
    ColumnIds = client_selector_column_ids(Columns, Schema),
    GlobalNHits =
        case MatchSpec of
            {near, _} ->
                [
                    client_token_global_docs(
                        Bookie, Schema, Token, ColumnIds
                    )
                 || {term, Token, false, _ItemColumns} <- Items
                ];
            {phrase, _} ->
                []
        end,
    Matches = lists:foldl(
        fun(Column, MatchAcc) ->
            TokenHeads = [
                {Token,
                    client_read_boolean_head(
                        Bookie, Schema, Token, Column
                    )}
             || {term, Token, false, _ItemColumns} <- Items
            ],
            Ordered = lists:sort(
                fun({_TokenA, HeadA}, {_TokenB, HeadB}) ->
                    client_boolean_head_total(HeadA) =<
                        client_boolean_head_total(HeadB)
                end,
                TokenHeads
            ),
            Candidates =
                case Ordered of
                    [] ->
                        #{};
                    [{_AnchorToken, not_found} | _] ->
                        #{};
                    _ ->
                        client_direct_boolean_candidates(
                            Bookie, Schema, Column, Ordered
                        )
                end,
            PositionRows =
                case map_size(Candidates) of
                    0 ->
                        [];
                    _ ->
                        [
                            client_read_direct_position_source(
                                Bookie, Schema, Token, Column, Candidates
                            )
                         || {term, Token, false, _ItemColumns} <- Items
                        ]
                end,
            ColumnMatches = maps:fold(
                fun(DocKey, {Version, Length}, Acc) ->
                    PositionLists = lists:filtermap(
                        fun(Source) ->
                            case
                                client_direct_position_lookup(
                                    Source, DocKey, Version
                                )
                            of
                                none -> false;
                                Positions -> {true, Positions}
                            end
                        end,
                        PositionRows
                    ),
                    case length(PositionLists) =:= length(Items) of
                        false ->
                            Acc;
                        true ->
                            case
                                client_direct_position_match(
                                    PositionLists, MatchSpec
                                )
                            of
                                [] ->
                                    Acc;
                                MatchPositions ->
                                    Tfs =
                                        case MatchSpec of
                                            {near, Distance} ->
                                                client_direct_near_tfs(
                                                    PositionLists, Distance
                                                );
                                            {phrase, _} ->
                                                [
                                                    length(MatchPositions)
                                                ]
                                        end,
                                    client_merge_direct_near_match(
                                        DocKey, Length, MatchPositions, Tfs, Acc
                                    )
                            end
                    end
                end,
                MatchAcc,
                Candidates
            ),
            ColumnMatches
        end,
        #{},
        ColumnIds
    ),
    Ranked = maps:get(rank, Opts, none) =:= bm25,
    {DocCount, TotalLength} =
        case Ranked of
            true -> client_corpus_stats(Bookie, Schema);
            false -> {0, 0}
        end,
    AvgLength =
        case DocCount of
            0 -> 0.0;
            _ -> TotalLength / DocCount
        end,
    ScoringNHits =
        case MatchSpec of
            {near, _} -> GlobalNHits;
            {phrase, _} -> [map_size(Matches)]
        end,
    Idfs =
        case Ranked of
            true -> client_direct_bm25_idfs(ScoringNHits, DocCount);
            false -> []
        end,
    ReturnPositions = maps:get(return_positions, Opts, false),
    Hits =
        case Ranked of
            true ->
                [
                    client_ranked_candidate(
                        client_direct_bm25_score_precomputed(
                            Tfs, Idfs, AvgLength, Length
                        ),
                        DocKey,
                        Length,
                        position_count(NearPositions),
                        case ReturnPositions of
                            true ->
                                {positions,
                                    client_window_positions(#{
                                        client_direct_match_key(MatchSpec) =>
                                            NearPositions
                                    })};
                            false ->
                                none
                        end
                    )
                 || {DocKey, {Length, NearPositions, Tfs}} <-
                        maps:to_list(Matches)
                ];
            false ->
                [
                    begin
                        Base = #{
                            key => DocKey,
                            score => 0.0,
                            doc_length => Length,
                            match_count => position_count(NearPositions)
                        },
                        case ReturnPositions of
                            true ->
                                Base#{
                                    positions => client_window_positions(#{
                                        client_direct_match_key(MatchSpec) =>
                                            NearPositions
                                    })
                                };
                            false ->
                                Base
                        end
                    end
                 || {DocKey, {Length, NearPositions, _Tfs}} <-
                        maps:to_list(Matches)
                ]
        end,
    Sorted =
        case Ranked of
            true ->
                client_bounded_ranked_hits(
                    Bookie, Schema, Hits, client_ranked_window_size(Opts)
                );
            false ->
                lists:sort(
                    fun(A, B) -> maps:get(key, A) =< maps:get(key, B) end, Hits
                )
        end,
    client_search_result(Bookie, Schema, Sorted, map_size(Matches), Opts).

client_token_global_docs(_Bookie, _Schema, _Token, []) ->
    0;
client_token_global_docs(Bookie, Schema, Token, [Column | Rest]) ->
    case client_read_boolean_head(Bookie, Schema, Token, Column) of
        not_found ->
            client_token_global_docs(
                Bookie, Schema, Token, Rest
            );
        {ok, Value} ->
            client_page_global_docs(Value)
    end.

client_direct_position_match(SpanLists, {near, Distance}) ->
    client_direct_near_positions(SpanLists, Distance);
client_direct_position_match([First | Rest], {phrase, [FirstOffset | Offsets]}) ->
    [
        Position - FirstOffset
     || Position <- First,
        client_direct_phrase_rest(
            Position - FirstOffset, Rest, Offsets
        )
    ].

client_direct_phrase_rest(_Start, [], []) ->
    true;
client_direct_phrase_rest(Start, [Positions | Rest], [Offset | Offsets]) ->
    lists:member(Start + Offset, Positions) andalso
        client_direct_phrase_rest(Start, Rest, Offsets).

client_direct_match_key({near, _Distance}) -> near;
client_direct_match_key({phrase, _Offsets}) -> phrase.

client_direct_near_positions([A, B], Distance) ->
    client_direct_near_sweep(A, B, Distance, []);
client_direct_near_positions(PositionLists, Distance) ->
    SpanLists = [
        [{Position, Position} || Position <- Positions]
     || Positions <- PositionLists
    ],
    near_positions(SpanLists, Distance).

client_direct_near_sweep([], _B, _Distance, Acc) ->
    lists:reverse(Acc);
client_direct_near_sweep(_A, [], _Distance, Acc) ->
    lists:reverse(Acc);
client_direct_near_sweep(
    [A | RestA] = As,
    [B | RestB] = Bs,
    Distance,
    Acc
) ->
    if
        B < A - Distance - 1 ->
            client_direct_near_sweep(As, RestB, Distance, Acc);
        B > A + Distance + 1 ->
            client_direct_near_sweep(RestA, Bs, Distance, Acc);
        true ->
            client_direct_near_sweep(RestA, Bs, Distance, [A | Acc])
    end.

client_direct_near_tfs([A, B], Distance) ->
    [
        client_direct_near_count(A, B, Distance, 0),
        client_direct_near_count(B, A, Distance, 0)
    ];
client_direct_near_tfs(PositionLists, Distance) ->
    SpanLists = [
        [{Position, Position} || Position <- Positions]
     || Positions <- PositionLists
    ],
    [
        length(
            client_direct_near_member_spans(
                Index, SpanLists, Distance
            )
        )
     || Index <- lists:seq(1, length(SpanLists))
    ].

client_direct_near_count([], _B, _Distance, Acc) ->
    Acc;
client_direct_near_count(_A, [], _Distance, Acc) ->
    Acc;
client_direct_near_count(
    [A | RestA] = As,
    [B | RestB] = Bs,
    Distance,
    Acc
) ->
    if
        B < A - Distance - 1 ->
            client_direct_near_count(As, RestB, Distance, Acc);
        B > A + Distance + 1 ->
            client_direct_near_count(RestA, Bs, Distance, Acc);
        true ->
            client_direct_near_count(RestA, Bs, Distance, Acc + 1)
    end.

client_direct_near_member_spans(Index, SpanLists, Distance) ->
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
    end.

client_merge_direct_near_match(DocKey, Length, NearPositions, Tfs, Acc) ->
    case maps:find(DocKey, Acc) of
        error ->
            Acc#{DocKey => {Length, NearPositions, Tfs}};
        {ok, {ExistingLength, ExistingPositions, ExistingTfs}} ->
            Acc#{
                DocKey =>
                    {ExistingLength, ExistingPositions ++ NearPositions,
                        lists:zipwith(fun(A, B) -> A + B end, ExistingTfs, Tfs)}
            }
    end.

client_direct_bm25_score(Tfs, NHits, DocCount, AvgLength, DocLength) ->
    client_direct_bm25_score_precomputed(
        Tfs,
        client_direct_bm25_idfs(NHits, DocCount),
        AvgLength,
        DocLength
    ).

client_read_boolean_head(Bookie, Schema, Token, Column) ->
    Bucket = maps:get(index, Schema),
    client_read_page(
        Bookie,
        Bucket,
        client_token_key(Token),
        ?BOOLEAN_PLANE,
        Column,
        0
    ).

client_boolean_head_total(not_found) -> 0;
client_boolean_head_total({ok, Value}) -> client_page_total_docs(Value).

client_read_boolean_values_from_head(
    _Bookie,
    _Schema,
    _Token,
    _Column,
    _Candidates,
    not_found
) ->
    [];
client_read_boolean_values_from_head(
    Bookie,
    Schema,
    Token,
    Column,
    Candidates,
    {ok, Value0}
) ->
    Bucket = maps:get(index, Schema),
    Key = client_token_key(Token),
    PageNumbers = client_candidate_page_numbers(Value0, Candidates),
    OverflowNumbers = [PageNo || PageNo <- PageNumbers, PageNo > 0],
    V8Selected = client_read_v8_selected_pages_from_head(
        Bookie,
        Bucket,
        Key,
        ?BOOLEAN_PLANE,
        Column,
        Value0,
        OverflowNumbers
    ),
    Folded =
        case V8Selected of
            {ok, Values} ->
                Values;
            previous_format ->
                case
                    Candidates =:= all andalso
                        client_page_count(Value0) >= 4
                of
                    true ->
                        client_fold_exact_plane_pages(
                            Bookie,
                            Bucket,
                            Key,
                            ?BOOLEAN_PLANE,
                            Column,
                            OverflowNumbers
                        );
                    false ->
                        none
                end
        end,
    Parallel =
        case Folded =:= none of
            true ->
                client_read_selected_plane_pages(
                    Bookie,
                    Bucket,
                    Key,
                    ?BOOLEAN_PLANE,
                    Column,
                    OverflowNumbers
                );
            false ->
                #{}
        end,
    [
        case PageNo of
            0 ->
                Value0;
            _ ->
                case Folded of
                    none ->
                        maps:get(PageNo, Parallel);
                    PageValues ->
                        maps:get(PageNo, PageValues)
                end
        end
     || PageNo <- PageNumbers
    ].

client_fold_exact_plane_pages(_Bookie, _Bucket, _Key, _Plane, _Column, []) ->
    #{};
client_fold_exact_plane_pages(
    Bookie,
    Bucket,
    Key,
    Plane,
    Column,
    PageNumbers
) ->
    case
        client_read_v8_plane_pages(
            Bookie, Bucket, Key, Plane, Column, PageNumbers
        )
    of
        V8Pages when map_size(V8Pages) =:= length(PageNumbers) ->
            maps:map(fun(_PageNo, {_SubKey, Value}) -> Value end, V8Pages);
        _NoCompleteV8Set ->
            client_fold_exact_v7_plane_pages(
                Bookie, Bucket, Key, Plane, Column, PageNumbers
            )
    end.

client_fold_exact_v7_plane_pages(
    Bookie, Bucket, Key, Plane, Column, PageNumbers
) ->
    FirstPage = hd(PageNumbers),
    LastPage = lists:last(PageNumbers),
    Wanted = maps:from_list([{PageNo, true} || PageNo <- PageNumbers]),
    Start = client_page_subkey(Plane, Column, FirstPage),
    Finish = client_page_subkey(Plane, Column, LastPage),
    Fold = fun
        (B, {K, SubKey}, Value, Acc) when
            B =:= Bucket, K =:= Key
        ->
            case client_decode_page_row(SubKey, Value) of
                {page, Plane, Column, PageNo} ->
                    case maps:is_key(PageNo, Wanted) of
                        true -> Acc#{PageNo => Value};
                        false -> Acc
                    end;
                {page, _OtherPlane, _OtherColumn, _PageNo} ->
                    Acc;
                not_page ->
                    Acc
            end;
        (_B, _K, _Value, Acc) ->
            Acc
    end,
    {async, Runner} = leveled_bookie:book_headfold(
        Bookie,
        ?HEAD_TAG,
        {range, Bucket, {{Key, Start}, {Key, Finish}}},
        {Fold, #{}},
        false,
        true,
        false
    ),
    Folded = Runner(),
    lists:foldl(
        fun(PageNo, Acc) ->
            case maps:is_key(PageNo, Acc) of
                true ->
                    Acc;
                false ->
                    case
                        client_read_page(
                            Bookie, Bucket, Key, Plane, Column, PageNo
                        )
                    of
                        {ok, Value} ->
                            Acc#{PageNo => Value};
                        not_found ->
                            erlang:error(
                                {missing_fts_page, Plane, PageNo}
                            )
                    end
            end
        end,
        Folded,
        PageNumbers
    ).

client_read_boolean_probe_source(
    _Bookie,
    _Schema,
    _Token,
    _Column,
    not_found,
    _Candidates
) ->
    missing;
client_read_boolean_probe_source(
    Bookie,
    Schema,
    Token,
    Column,
    {ok, Value0},
    Candidates
) ->
    Bucket = maps:get(index, Schema),
    Key = client_token_key(Token),
    {PageNumbers, CandidateGroups} = client_candidate_page_groups(
        Value0, Candidates
    ),
    OverflowNumbers = [PageNo || PageNo <- PageNumbers, PageNo > 0],
    OverflowValues =
        case
            client_read_v8_selected_pages_from_head(
                Bookie,
                Bucket,
                Key,
                ?BOOLEAN_PLANE,
                Column,
                Value0,
                OverflowNumbers
            )
        of
            {ok, Values} ->
                Values;
            previous_format ->
                client_read_selected_plane_pages(
                    Bookie,
                    Bucket,
                    Key,
                    ?BOOLEAN_PLANE,
                    Column,
                    OverflowNumbers
                )
        end,
    Pages =
        case lists:member(0, PageNumbers) of
            true -> OverflowValues#{0 => Value0};
            false -> OverflowValues
        end,
    {boolean_pages, Bookie, Bucket, Key, Column, CandidateGroups, Pages}.

client_decode_boolean_compact_filtered(Values) ->
    lists:foldl(
        fun(Value, Acc) ->
            client_decode_boolean_filtered_page(
                Value, Acc
            )
        end,
        #{},
        Values
    ).

client_decode_boolean_filtered_page(
    <<?PAGE_MAGIC:24/unsigned-big, ?PAGE_VERSION:8, ?BOOLEAN_PLANE:8,
        _PageCount:16/unsigned-big, _TotalDocs:32/unsigned-big,
        _CollectionFrequency:64/unsigned-big, _TermMaxBound:8,
        _N:32/unsigned-big, ChunkDirBytes:32/unsigned-big,
        PageDirBytes:32/unsigned-big, ChunkDirectory:ChunkDirBytes/binary,
        _PageDirectory:PageDirBytes/binary, Payload/binary>>,
    Acc
) when ChunkDirBytes rem ?CHUNK_DIR_STRIDE =:= 0 ->
    client_decode_v8_boolean_compact_chunks(
        ChunkDirectory, Payload, Acc
    );
client_decode_boolean_filtered_page(
    <<?PAGE_MAGIC:24/unsigned-big, ?PREVIOUS_PAGE_VERSION:8, ?BOOLEAN_PLANE:8,
        _PageCount:16/unsigned-big, _TotalDocs:32/unsigned-big,
        N:32/unsigned-big, DirBytes:32/unsigned-big,
        PageDirBytes:32/unsigned-big, Directory:DirBytes/binary,
        _PageDirectory:PageDirBytes/binary, Payload/binary>>,
    Acc
) ->
    client_decode_boolean_filtered_rows(
        N, Directory, Payload, Acc
    ).

client_decode_v8_boolean_compact_chunks(<<>>, _Payload, Acc) ->
    Acc;
client_decode_v8_boolean_compact_chunks(
    <<First:64/unsigned-big, _Last:64/unsigned-big, Offset:32/unsigned-big,
        Bytes:16/unsigned-big, Count:16/unsigned-big, _MaxBound:8,
        RestDirectory/binary>>,
    Payload,
    Acc
) ->
    Chunk = binary:part(Payload, Offset, Bytes),
    Acc1 = client_decode_v8_boolean_compact_chunk(
        Count, First, First, Chunk, true, Acc
    ),
    client_decode_v8_boolean_compact_chunks(
        RestDirectory, Payload, Acc1
    ).

client_decode_v8_boolean_compact_chunk(
    0, _First, _Previous, _Payload, _Initial, Acc
) ->
    Acc;
client_decode_v8_boolean_compact_chunk(
    Count, First, Previous, Payload, Initial, Acc
) ->
    {DocId, ValueBin} =
        case Initial of
            true ->
                {First, Payload};
            false ->
                {Delta, Rest0} = client_decode_page_varint(Payload),
                {Previous + Delta, Rest0}
        end,
    {DocLength, CountBin} = client_decode_page_varint(ValueBin),
    {_Tf, Rest} = client_decode_page_varint(CountBin),
    client_decode_v8_boolean_compact_chunk(
        Count - 1,
        First,
        DocId,
        Rest,
        false,
        Acc#{DocId => {DocId, DocLength}}
    ).

client_decode_boolean_filtered_rows(0, _Directory, _Payload, Acc) ->
    Acc;
client_decode_boolean_filtered_rows(N, Directory, Payload, Acc) ->
    Index = N - 1,
    <<_Hash:32/unsigned-big, Offset:32/unsigned-big>> =
        binary:part(Directory, Index * ?PAGE_DIR_STRIDE, ?PAGE_DIR_STRIDE),
    {DocId, VersionLength} = client_decode_boolean_compact_entry(
        Payload, Offset
    ),
    Acc1 = Acc#{DocId => VersionLength},
    client_decode_boolean_filtered_rows(
        Index, Directory, Payload, Acc1
    ).

client_intersect_boolean_docids(missing, _Mode) ->
    #{};
client_intersect_boolean_docids(
    {boolean_pages, Bookie, Bucket, Key, Column, CandidateGroups, Pages}, Mode
) ->
    PageNumbers = maps:keys(CandidateGroups),
    Rows = [
        {Key, client_page_subkey(?BOOLEAN_PLANE, Column, PageNo)}
     || PageNo <- PageNumbers
    ],
    SQNs = maps:from_list(
        leveled_bookie:book_mhead_sqn(Bookie, Bucket, Rows)
    ),
    maps:fold(
        fun(PageNo, Candidates, Acc) ->
            PageValue = maps:get(PageNo, Pages),
            PageDocs =
                case client_page_version(PageValue) of
                    ?PAGE_VERSION ->
                        client_decode_v8_direct_term_candidates(
                            PageValue, Candidates
                        );
                    ?PREVIOUS_PAGE_VERSION ->
                        Row =
                            {Key,
                                client_page_subkey(
                                    ?BOOLEAN_PLANE, Column, PageNo
                                )},
                        {ok, SQN} = maps:get(Row, SQNs),
                        {ok, Page} = client_read_cached_direct_term_page(
                            Bookie,
                            Bucket,
                            Key,
                            Column,
                            PageNo,
                            SQN,
                            PageValue
                        ),
                        maps:get(docs, Page)
                end,
            lists:foldl(
                fun({DocKey, CandidateVersionLength}, PageAcc) ->
                    case maps:find(DocKey, PageDocs) of
                        {ok, {DocKey, DocLength, _Tf}} when
                            Mode =:= member orelse
                                {DocKey, DocLength} =:=
                                    CandidateVersionLength
                        ->
                            PageAcc#{DocKey => CandidateVersionLength};
                        _ ->
                            PageAcc
                    end
                end,
                Acc,
                Candidates
            )
        end,
        #{},
        CandidateGroups
    ).

client_decode_boolean_compact_entry(Payload, Offset) ->
    Body = binary:part(Payload, Offset, byte_size(Payload) - Offset),
    {DocId, LengthBin} = client_decode_page_varint(Body),
    {DocLength, CountBin} = client_decode_page_varint(LengthBin),
    {_Count, _Rest} = client_decode_page_varint(CountBin),
    {DocId, {DocId, DocLength}}.

client_read_direct_position_source(Bookie, Schema, Token, Column, Candidates) ->
    Bucket = maps:get(index, Schema),
    case
        client_read_cached_position_pages(
            Bookie, Bucket, Token, Column
        )
    of
        {ok, Pages} ->
            {decoded,
                client_decode_cached_position_pages(
                    Schema, Token, Column, Candidates, Pages
                )};
        fallback ->
            Head = client_read_page(
                Bookie,
                Bucket,
                client_token_key(Token),
                ?POSITION_PLANE,
                Column,
                0
            ),
            case Head of
                not_found ->
                    missing;
                {ok, _Value} ->
                    {decoded,
                        client_read_position_column_pages(
                            Bookie,
                            Schema,
                            Token,
                            Column,
                            Candidates,
                            Head
                        )}
            end
    end.

client_read_cached_position_pages(
    Bookie, Bucket, Token, Column
) ->
    CacheKey = {fts_position_pages, Bucket, Token, Column},
    case client_clean_stats_epoch(Bookie, Bucket) of
        {ok, Epoch} ->
            case
                leveled_bookie:book_valuecache_get(
                    Bookie, CacheKey, Epoch
                )
            of
                {ok, Pages} ->
                    {ok, Pages};
                miss ->
                    Key = client_token_key(Token),
                    Pages0 = client_read_v8_plane_pages(
                        Bookie,
                        Bucket,
                        Key,
                        ?POSITION_PLANE,
                        Column,
                        all
                    ),
                    case maps:find(0, Pages0) of
                        {ok, {_SubKey, Head}} ->
                            case
                                client_page_version(Head) =:=
                                    ?PAGE_VERSION andalso
                                    map_size(Pages0) =:=
                                        client_page_count(Head)
                            of
                                true ->
                                    Pages = maps:map(
                                        fun(
                                            _PageNo,
                                            {_PageSubKey, Value}
                                        ) ->
                                            Value
                                        end,
                                        Pages0
                                    ),
                                    ok =
                                        leveled_bookie:book_valuecache_put(
                                            Bookie,
                                            CacheKey,
                                            Epoch,
                                            Pages
                                        ),
                                    {ok, Pages};
                                false ->
                                    fallback
                            end;
                        _ ->
                            fallback
                    end
            end;
        dirty ->
            fallback
    end.

client_decode_cached_position_pages(
    Schema, Token, Column, Candidates, Pages
) ->
    Head = maps:get(0, Pages),
    lists:foldl(
        fun({PageNo, PageCandidates}, Acc) ->
            {_IgnoredCount, Positions} = client_decode_plane_page(
                Token,
                Column,
                ?POSITION_PLANE,
                maps:get(PageNo, Pages),
                PageCandidates,
                Schema
            ),
            client_merge_position_rows(Acc, Positions)
        end,
        #{},
        client_page_candidate_groups(Head, Candidates)
    ).

client_direct_position_lookup(missing, _DocKey, _Version) ->
    none;
client_direct_position_lookup({decoded, PositionsByDoc}, DocKey, Version) ->
    case maps:get(DocKey, PositionsByDoc, none) of
        {Version, Positions} -> Positions;
        _ -> none
    end.

client_read_direct_position_binary_source(
    Bookie, Schema, Token, Column, Candidates
) ->
    Bucket = maps:get(index, Schema),
    Key = client_token_key(Token),
    case
        client_read_page(
            Bookie, Bucket, Key, ?POSITION_PLANE, Column, 0
        )
    of
        not_found ->
            missing;
        {ok, Value0} ->
            {PageNumbers, CandidatePages} = client_candidate_page_map(
                Value0, Candidates
            ),
            OverflowPages = [PageNo || PageNo <- PageNumbers, PageNo > 0],
            OverflowValues =
                case
                    client_read_v8_selected_pages_from_head(
                        Bookie,
                        Bucket,
                        Key,
                        ?POSITION_PLANE,
                        Column,
                        Value0,
                        OverflowPages
                    )
                of
                    {ok, Values} ->
                        Values;
                    previous_format ->
                        client_read_parallel_plane_pages(
                            Bookie,
                            Bucket,
                            Key,
                            ?POSITION_PLANE,
                            Column,
                            OverflowPages
                        )
                end,
            Pages = maps:from_list([
                {PageNo,
                    client_prepare_position_binary_page(
                        case PageNo of
                            0 -> Value0;
                            _ -> maps:get(PageNo, OverflowValues)
                        end
                    )}
             || PageNo <- PageNumbers
            ]),
            {binary_positions, CandidatePages, Pages}
    end.

client_read_selected_plane_pages(
    _Bookie, _Bucket, _Key, _Plane, _Column, []
) ->
    #{};
client_read_selected_plane_pages(
    Bookie, Bucket, Key, Plane, Column, PageNumbers
) ->
    Ordered = lists:sort(PageNumbers),
    case
        client_read_v8_plane_pages(
            Bookie, Bucket, Key, Plane, Column, Ordered
        )
    of
        V8Pages when map_size(V8Pages) =:= length(Ordered) ->
            maps:map(fun(_PageNo, {_SubKey, Value}) -> Value end, V8Pages);
        _NoCompleteV8Set ->
            First = hd(Ordered),
            Last = lists:last(Ordered),
            case
                length(Ordered) > 1 andalso
                    Ordered =:= lists:seq(First, Last)
            of
                true ->
                    client_fold_exact_plane_pages(
                        Bookie, Bucket, Key, Plane, Column, Ordered
                    );
                false ->
                    client_read_parallel_plane_pages(
                        Bookie, Bucket, Key, Plane, Column, Ordered
                    )
            end
    end.

client_read_parallel_plane_pages(
    _Bookie, _Bucket, _Key, _Plane, _Column, []
) ->
    #{};
client_read_parallel_plane_pages(
    Bookie, Bucket, Key, Plane, Column, PageNumbers
) ->
    case
        client_read_v8_plane_pages(
            Bookie, Bucket, Key, Plane, Column, PageNumbers
        )
    of
        V8Pages when map_size(V8Pages) =:= length(PageNumbers) ->
            maps:map(fun(_PageNo, {_SubKey, Value}) -> Value end, V8Pages);
        _NoCompleteV8Set ->
            Parent = self(),
            PageRefs = [
                begin
                    Ref = make_ref(),
                    spawn(fun() ->
                        Parent !
                            {Ref,
                                try
                                    client_read_page(
                                        Bookie,
                                        Bucket,
                                        Key,
                                        Plane,
                                        Column,
                                        PageNo
                                    )
                                of
                                    {ok, Value} ->
                                        {ok, Value};
                                    not_found ->
                                        {error, error,
                                            {missing_fts_page, Plane, PageNo},
                                            []}
                                catch
                                    Class:Reason:Stack ->
                                        {error, Class, Reason, Stack}
                                end}
                    end),
                    {PageNo, Ref}
                end
             || PageNo <- PageNumbers
            ],
            maps:from_list([
                {PageNo, client_receive_parallel_value(Ref)}
             || {PageNo, Ref} <- PageRefs
            ])
    end.

client_prepare_position_binary_page(
    <<?PAGE_MAGIC:24/unsigned-big, ?PAGE_VERSION:8, ?POSITION_PLANE:8,
        _PageCount:16/unsigned-big, _TotalDocs:32/unsigned-big,
        _CollectionFrequency:64/unsigned-big, _TermMaxBound:8,
        _N:32/unsigned-big, ChunkDirBytes:32/unsigned-big,
        PageDirBytes:32/unsigned-big, ChunkDirectory:ChunkDirBytes/binary,
        _PageDirectory:PageDirBytes/binary, Payload/binary>>
) when ChunkDirBytes rem ?CHUNK_DIR_STRIDE =:= 0 ->
    {v8_position_chunks, ChunkDirectory, Payload};
client_prepare_position_binary_page(
    <<?PAGE_MAGIC:24/unsigned-big, ?PREVIOUS_PAGE_VERSION:8, ?POSITION_PLANE:8,
        _PageCount:16/unsigned-big, _TotalDocs:32/unsigned-big,
        N:32/unsigned-big, DirBytes:32/unsigned-big,
        PageDirBytes:32/unsigned-big, Directory:DirBytes/binary,
        _PageDirectory:PageDirBytes/binary, Payload/binary>>
) ->
    {N, Directory, Payload}.

client_direct_position_binary_lookup(
    missing, _DocKey, _Hash, _Version
) ->
    none;
client_direct_position_binary_lookup(
    {binary_positions, CandidatePages, Pages},
    DocId,
    Hash,
    _Version
) ->
    case maps:find(DocId, CandidatePages) of
        {ok, PageNos} ->
            Chunks = lists:append([
                case maps:find(PageNo, Pages) of
                    error ->
                        [];
                    {ok, Page} ->
                        case
                            client_lookup_position_binary_page(
                                Page, DocId, Hash
                            )
                        of
                            none -> [];
                            {ok, PageChunks} -> PageChunks
                        end
                end
             || PageNo <- lists:sort(PageNos)
            ]),
            case Chunks of
                [] ->
                    none;
                _ ->
                    Positions = lists:sort(
                        lists:append([
                            case decode_positions(Chunk, 0, []) of
                                {ok, Ps} ->
                                    Ps;
                                error ->
                                    erlang:error(
                                        {invalid_fts_positions, Chunk}
                                    )
                            end
                             || Chunk <- Chunks
                        ])
                    ),
                    {ok, client_encode_positions(Positions)}
            end;
        error ->
            none
    end.

client_lookup_position_binary_page(
    {v8_position_chunks, ChunkDirectory, Payload}, DocId, _Hash
) ->
    Chunks = client_lookup_v8_position_chunks(
        ChunkDirectory, Payload, DocId, []
    ),
    case Chunks of
        [] -> none;
        _ -> {ok, lists:reverse(Chunks)}
    end;
client_lookup_position_binary_page(
    {N, Directory, Payload}, DocId, Hash
) ->
    Index = client_position_directory_lower_bound(
        Directory, Hash, 0, N
    ),
    case
        client_lookup_position_binary_hash(
            DocId, Hash, Index, N, Directory, Payload, []
        )
    of
        [] -> none;
        Chunks -> {ok, Chunks}
    end.

client_lookup_v8_position_chunks(<<>>, _Payload, _DocId, Acc) ->
    Acc;
client_lookup_v8_position_chunks(
    <<First:64/unsigned-big, Last:64/unsigned-big, Offset:32/unsigned-big,
        Bytes:16/unsigned-big, Count:16/unsigned-big, _MaxBound:8,
        RestDirectory/binary>>,
    Payload,
    DocId,
    Acc
) ->
    Acc1 =
        case DocId >= First andalso DocId =< Last of
            true ->
                Chunk = binary:part(Payload, Offset, Bytes),
                client_lookup_v8_position_chunk(
                    Count, First, First, Chunk, DocId, true, Acc
                );
            false ->
                Acc
        end,
    client_lookup_v8_position_chunks(
        RestDirectory, Payload, DocId, Acc1
    ).

client_lookup_v8_position_chunk(
    0, _First, _Previous, _Payload, _Wanted, _Initial, Acc
) ->
    Acc;
client_lookup_v8_position_chunk(
    Count, First, Previous, Payload, Wanted, Initial, Acc
) ->
    {DocId, ValueBin} =
        case Initial of
            true ->
                {First, Payload};
            false ->
                {Delta, Rest0} = client_decode_page_varint(Payload),
                {Previous + Delta, Rest0}
        end,
    {PositionBytes, PositionsBin} = client_decode_page_varint(ValueBin),
    <<Positions:PositionBytes/binary, Rest/binary>> = PositionsBin,
    Acc1 =
        case DocId of
            Wanted -> [Positions | Acc];
            _ -> Acc
        end,
    case DocId > Wanted of
        true ->
            Acc1;
        false ->
            client_lookup_v8_position_chunk(
                Count - 1,
                First,
                DocId,
                Rest,
                Wanted,
                false,
                Acc1
            )
    end.

client_lookup_position_binary_hash(
    _DocId, _Hash, Index, N, _Directory, _Payload, Acc
) when
    Index >= N
->
    lists:reverse(Acc);
client_lookup_position_binary_hash(
    DocId, Hash, Index, N, Directory, Payload, Acc
) ->
    DirectoryOffset = Index * ?PAGE_DIR_STRIDE,
    <<_:DirectoryOffset/binary, RowHash:32/unsigned-big, Offset:32/unsigned-big,
        _/binary>> = Directory,
    case RowHash =:= Hash of
        false ->
            lists:reverse(Acc);
        true ->
            {StoredDocId, Positions} =
                client_decode_position_binary_entry(Payload, Offset),
            Acc1 =
                case StoredDocId =:= DocId of
                    true -> [Positions | Acc];
                    false -> Acc
                end,
            client_lookup_position_binary_hash(
                DocId,
                Hash,
                Index + 1,
                N,
                Directory,
                Payload,
                Acc1
            )
    end.

client_position_directory_lower_bound(_Directory, _Hash, Lo, Lo) ->
    Lo;
client_position_directory_lower_bound(Directory, Hash, Lo, Hi) ->
    Mid = (Lo + Hi) div 2,
    DirectoryOffset = Mid * ?PAGE_DIR_STRIDE,
    <<_:DirectoryOffset/binary, MidHash:32/unsigned-big,
        _RowOffset:32/unsigned-big, _/binary>> = Directory,
    case MidHash < Hash of
        true ->
            client_position_directory_lower_bound(
                Directory, Hash, Mid + 1, Hi
            );
        false ->
            client_position_directory_lower_bound(
                Directory, Hash, Lo, Mid
            )
    end.

client_decode_position_binary_entry(Payload, Offset) ->
    Body = binary:part(Payload, Offset, byte_size(Payload) - Offset),
    {DocId, Rest} = client_decode_page_varint(Body),
    {PosBytes, PositionsBin} = client_decode_page_varint(Rest),
    <<Positions:PosBytes/binary, _/binary>> = PositionsBin,
    {DocId, Positions}.

client_raw_near_any(PositionsA, PositionsB, Distance) ->
    case
        {
            client_raw_position_start(PositionsA),
            client_raw_position_start(PositionsB)
        }
    of
        {done, _} ->
            false;
        {_, done} ->
            false;
        {{PositionA, <<>>}, {PositionB, <<>>}} ->
            abs(PositionA - PositionB) =< Distance + 1;
        {CursorA, CursorB} ->
            client_raw_near_any_loop(
                CursorA, CursorB, Distance + 1
            )
    end.

client_raw_near_any_loop(
    {PositionA, _RestA},
    {PositionB, _RestB},
    Window
) when
    PositionA >= PositionB - Window,
    PositionA =< PositionB + Window
->
    true;
client_raw_near_any_loop(
    {PositionA, RestA},
    CursorB = {PositionB, _},
    Window
) when PositionA < PositionB ->
    case client_raw_position_next(RestA, PositionA) of
        done ->
            false;
        NextA ->
            client_raw_near_any_loop(
                NextA, CursorB, Window
            )
    end;
client_raw_near_any_loop(CursorA, {PositionB, RestB}, Window) ->
    case client_raw_position_next(RestB, PositionB) of
        done ->
            false;
        NextB ->
            client_raw_near_any_loop(
                CursorA, NextB, Window
            )
    end.

client_raw_near_tfs(PositionsA, PositionsB, Distance) ->
    case
        {
            client_raw_position_start(PositionsA),
            client_raw_position_start(PositionsB)
        }
    of
        {done, _} ->
            {false, 0, 0};
        {_, done} ->
            {false, 0, 0};
        {{PositionA, <<>>}, {PositionB, <<>>}} ->
            case abs(PositionA - PositionB) =< Distance + 1 of
                true -> {true, 1, 1};
                false -> {false, 0, 0}
            end;
        {CursorA, CursorB} ->
            client_raw_near_tfs_loop(
                CursorA,
                CursorB,
                none,
                none,
                Distance + 1,
                0,
                0
            )
    end.

client_raw_near_tfs_loop(
    done,
    done,
    _PreviousA,
    _PreviousB,
    _Window,
    CountA,
    CountB
) ->
    {CountA > 0, CountA, CountB};
client_raw_near_tfs_loop(
    done,
    CursorB,
    PreviousA,
    _PreviousB,
    Window,
    CountA,
    CountB
) ->
    FinalCountB = client_raw_near_remaining(
        CursorB, PreviousA, Window, CountB
    ),
    {CountA > 0, CountA, FinalCountB};
client_raw_near_tfs_loop(
    CursorA,
    done,
    _PreviousA,
    PreviousB,
    Window,
    CountA,
    CountB
) ->
    FinalCountA = client_raw_near_remaining(
        CursorA, PreviousB, Window, CountA
    ),
    {FinalCountA > 0, FinalCountA, CountB};
client_raw_near_tfs_loop(
    {PositionA, RestA},
    CursorB = {PositionB, _RestB},
    _PreviousA,
    PreviousB,
    Window,
    CountA,
    CountB
) when
    PositionA =< PositionB
->
    CountA1 =
        case PositionB - PositionA =< Window of
            true ->
                CountA + 1;
            false ->
                case PreviousB of
                    none -> CountA;
                    _ when PositionA - PreviousB =< Window -> CountA + 1;
                    _ -> CountA
                end
        end,
    NextA = client_raw_position_next(RestA, PositionA),
    client_raw_near_tfs_loop(
        NextA,
        CursorB,
        PositionA,
        PreviousB,
        Window,
        CountA1,
        CountB
    );
client_raw_near_tfs_loop(
    CursorA = {_PositionA, _RestA},
    {PositionB, RestB},
    PreviousA,
    _PreviousB,
    Window,
    CountA,
    CountB
) ->
    {PositionA, _} = CursorA,
    CountB1 =
        case PositionA - PositionB =< Window of
            true ->
                CountB + 1;
            false ->
                case PreviousA of
                    none -> CountB;
                    _ when PositionB - PreviousA =< Window -> CountB + 1;
                    _ -> CountB
                end
        end,
    NextB = client_raw_position_next(RestB, PositionB),
    client_raw_near_tfs_loop(
        CursorA,
        NextB,
        PreviousA,
        PositionB,
        Window,
        CountA,
        CountB1
    ).

client_raw_near_remaining(done, _PreviousOther, _Window, Count) ->
    Count;
client_raw_near_remaining(
    {Position, Rest},
    PreviousOther,
    Window,
    Count
) ->
    Count1 =
        case PreviousOther of
            none -> Count;
            _ when Position - PreviousOther =< Window -> Count + 1;
            _ -> Count
        end,
    Next = client_raw_position_next(Rest, Position),
    client_raw_near_remaining(
        Next, PreviousOther, Window, Count1
    ).

client_raw_position_start(Bin) ->
    client_raw_position_next(Bin, 0).

client_raw_position_next(<<>>, _Last) ->
    done;
client_raw_position_next(<<Byte:8, Rest/binary>>, Last) when Byte < 128 ->
    {Last + Byte, Rest};
client_raw_position_next(<<Byte:8, Rest/binary>> = Bin, Last) ->
    client_raw_position_varint(
        Rest, Last, 7, Byte band 16#7F, Bin
    ).

client_raw_position_varint(
    <<Byte:8, Rest/binary>>, Last, Shift, Acc, Original
) when
    Shift =< 63
->
    Value = Acc bor ((Byte band 16#7F) bsl Shift),
    case Byte band 16#80 of
        0 ->
            {Last + Value, Rest};
        _ ->
            client_raw_position_varint(
                Rest, Last, Shift + 7, Value, Original
            )
    end;
client_raw_position_varint(_Bin, _Last, _Shift, _Acc, Original) ->
    erlang:error({invalid_fts_positions, Original}).

client_merge_binary_near_match(
    DocKey, Version, Length, Tfs, Column, Acc
) ->
    case maps:find(DocKey, Acc) of
        error ->
            Acc#{DocKey => {Version, Length, Tfs, [Column]}};
        {ok, {Version, ExistingLength, ExistingTfs, Columns}} ->
            MergedTfs =
                case {ExistingTfs, Tfs} of
                    {[], []} ->
                        [];
                    _ ->
                        lists:zipwith(
                            fun(A, B) -> A + B end, ExistingTfs, Tfs
                        )
                end,
            Acc#{
                DocKey =>
                    {Version, ExistingLength, MergedTfs, [Column | Columns]}
            };
        {ok, {_OtherVersion, _Length, _Tfs, _Columns}} ->
            Acc
    end.

client_materialize_binary_near(
    DocKey, Version, Columns, SourcesByColumn, Distance
) ->
    Hash = client_docid_hash(DocKey),
    Positions = lists:append([
        begin
            {SourceA, SourceB} = maps:get(Column, SourcesByColumn),
            {ok, BinaryA} = client_direct_position_binary_lookup(
                SourceA, DocKey, Hash, Version
            ),
            {ok, BinaryB} = client_direct_position_binary_lookup(
                SourceB, DocKey, Hash, Version
            ),
            {ok, ListA} = decode_positions(BinaryA, 0, []),
            {ok, ListB} = decode_positions(BinaryB, 0, []),
            client_direct_near_positions([ListA, ListB], Distance)
        end
     || Column <- Columns
    ]),
    lists:sort(Positions).

client_page_count(
    <<?PAGE_MAGIC:24/unsigned-big, ?PAGE_VERSION:8, _Plane:8,
        PageCount:16/unsigned-big, _/binary>>
) ->
    PageCount;
client_page_count(
    <<?PAGE_MAGIC:24/unsigned-big, ?PREVIOUS_PAGE_VERSION:8, _Plane:8,
        PageCount:16/unsigned-big, _/binary>>
) ->
    PageCount.

client_page_chunk_count(
    <<?PAGE_MAGIC:24/unsigned-big, ?PAGE_VERSION:8, _Plane:8,
        _PageCount:16/unsigned-big, _TotalDocs:32/unsigned-big,
        _CollectionFrequency:64/unsigned-big, _TermMaxBound:8,
        _N:32/unsigned-big, ChunkDirBytes:32/unsigned-big, _/binary>>
) ->
    ChunkDirBytes div ?CHUNK_DIR_STRIDE.

client_candidate_page_numbers(Value0, all) ->
    lists:seq(0, client_page_count(Value0) - 1);
client_candidate_page_numbers(_Value0, Candidates) when
    map_size(Candidates) =:= 0
->
    [];
client_candidate_page_numbers(Value0, Candidates) ->
    Boundaries = client_page_boundaries(Value0),
    lists:usort(
        lists:append([
            client_pages_for_doc(DocKey, Boundaries)
         || DocKey <- maps:keys(Candidates)
        ])
    ).

client_candidate_page_map(Value0, Candidates) ->
    Boundaries = client_page_boundaries(Value0),
    {Pages, CandidatePages} = maps:fold(
        fun(DocKey, _CandidateValue, {PageAcc, CandidateAcc}) ->
            case client_pages_for_doc(DocKey, Boundaries) of
                [] ->
                    {PageAcc, CandidateAcc};
                PageNos ->
                    {
                        lists:foldl(
                            fun(PageNo, Acc) -> Acc#{PageNo => true} end,
                            PageAcc,
                            PageNos
                        ),
                        CandidateAcc#{DocKey => PageNos}
                    }
            end
        end,
        {#{}, #{}},
        Candidates
    ),
    {maps:keys(Pages), CandidatePages}.

client_candidate_page_groups(Value0, Candidates) ->
    Boundaries = client_page_boundaries(Value0),
    {Pages, CandidateGroups} = maps:fold(
        fun(DocKey, CandidateValue, {PageAcc, GroupAcc}) ->
            case client_pages_for_doc(DocKey, Boundaries) of
                [] ->
                    {PageAcc, GroupAcc};
                PageNos ->
                    lists:foldl(
                        fun(PageNo, {PagesAcc, GroupsAcc}) ->
                            {
                                PagesAcc#{PageNo => true},
                                GroupsAcc#{
                                    PageNo => [
                                        {DocKey, CandidateValue}
                                        | maps:get(PageNo, GroupsAcc, [])
                                    ]
                                }
                            }
                        end,
                        {PageAcc, GroupAcc},
                        PageNos
                    )
            end
        end,
        {#{}, #{}},
        Candidates
    ),
    {maps:keys(Pages), CandidateGroups}.

client_page_candidate_groups(Value0, all) ->
    [
        {PageNo, all}
     || PageNo <- lists:seq(0, client_page_count(Value0) - 1)
    ];
client_page_candidate_groups(_Value0, Candidates) when
    map_size(Candidates) =:= 0
->
    [];
client_page_candidate_groups(Value0, Candidates) when
    is_map(Candidates)
->
    case client_page_count(Value0) of
        1 -> [{0, Candidates}];
        _ -> client_partition_page_candidates(Value0, Candidates)
    end.

client_partition_page_candidates(Value0, Candidates) ->
    Boundaries = client_page_boundaries(Value0),
    maps:to_list(
        maps:fold(
            fun(DocKey, CandidateValue, Acc) ->
                lists:foldl(
                    fun(PageNo, PageAcc) ->
                        PageCandidates = maps:get(PageNo, PageAcc, #{}),
                        PageAcc#{
                            PageNo =>
                                PageCandidates#{DocKey => CandidateValue}
                        }
                    end,
                    Acc,
                    client_pages_for_doc(DocKey, Boundaries)
                )
            end,
            #{},
            Candidates
        )
    ).

client_page_boundaries(
    <<?PAGE_MAGIC:24/unsigned-big, ?PAGE_VERSION:8, _Plane:8,
        _PageCount:16/unsigned-big, _TotalDocs:32/unsigned-big,
        _CollectionFrequency:64/unsigned-big, _TermMaxBound:8,
        _N:32/unsigned-big, ChunkDirBytes:32/unsigned-big,
        PageDirBytes:32/unsigned-big, _Directory:ChunkDirBytes/binary,
        PageDirectory:PageDirBytes/binary, _Payload/binary>>
) ->
    <<_GlobalDocs:32/unsigned-big, Boundaries/binary>> = PageDirectory,
    client_decode_page_boundaries(Boundaries, <<>>, 0, []);
client_page_boundaries(
    <<?PAGE_MAGIC:24/unsigned-big, ?PREVIOUS_PAGE_VERSION:8, _Plane:8,
        _PageCount:16/unsigned-big, _TotalDocs:32/unsigned-big,
        _N:32/unsigned-big, DirBytes:32/unsigned-big,
        PageDirBytes:32/unsigned-big, _Directory:DirBytes/binary,
        PageDirectory:PageDirBytes/binary, _Payload/binary>>
) ->
    <<_GlobalDocs:32/unsigned-big, Boundaries/binary>> = PageDirectory,
    client_decode_page_boundaries(Boundaries, <<>>, 0, []).

client_page_global_docs(
    <<?PAGE_MAGIC:24/unsigned-big, ?PAGE_VERSION:8, _Plane:8,
        PageCount:16/unsigned-big, _TotalDocs:32/unsigned-big,
        _CollectionFrequency:64/unsigned-big, _TermMaxBound:8,
        _N:32/unsigned-big, ChunkDirBytes:32/unsigned-big,
        PageDirBytes:32/unsigned-big, _Directory:ChunkDirBytes/binary,
        PageDirectory:PageDirBytes/binary, _Payload/binary>>
) when PageCount > 0, PageDirBytes >= 4 ->
    <<GlobalDocs:32/unsigned-big, _/binary>> = PageDirectory,
    GlobalDocs;
client_page_global_docs(
    <<?PAGE_MAGIC:24/unsigned-big, ?PREVIOUS_PAGE_VERSION:8, _Plane:8,
        PageCount:16/unsigned-big, _TotalDocs:32/unsigned-big,
        _N:32/unsigned-big, DirBytes:32/unsigned-big,
        PageDirBytes:32/unsigned-big, _Directory:DirBytes/binary,
        PageDirectory:PageDirBytes/binary, _Payload/binary>>
) when PageCount > 0, PageDirBytes >= 4 ->
    <<GlobalDocs:32/unsigned-big, _/binary>> = PageDirectory,
    GlobalDocs.

client_decode_page_boundaries(<<>>, _Prev, _PageNo, Acc) ->
    list_to_tuple(lists:reverse(Acc));
client_decode_page_boundaries(Bin, Prev, PageNo, Acc) ->
    {MinDocKey, Rest0} = client_decode_boundary_key(Prev, Bin),
    {MaxDocKey, Rest} = client_decode_boundary_key(MinDocKey, Rest0),
    client_decode_page_boundaries(
        Rest,
        MaxDocKey,
        PageNo + 1,
        [
            {PageNo, binary:decode_unsigned(MinDocKey),
                binary:decode_unsigned(MaxDocKey)}
            | Acc
        ]
    ).

client_decode_boundary_key(
    Base,
    <<Shared:16/unsigned-big, SuffixBytes:16/unsigned-big,
        Suffix:SuffixBytes/binary, Rest/binary>>
) ->
    <<Prefix:Shared/binary, _/binary>> = Base,
    {<<Prefix/binary, Suffix/binary>>, Rest}.

client_pages_for_doc(DocKey, Boundaries) ->
    First = client_first_page_for_doc(
        DocKey, Boundaries, 1, tuple_size(Boundaries) + 1
    ),
    client_collect_pages_for_doc(DocKey, Boundaries, First, []).

client_first_page_for_doc(_DocKey, _Boundaries, Lo, Lo) ->
    Lo;
client_first_page_for_doc(DocKey, Boundaries, Lo, Hi) ->
    Mid = (Lo + Hi) div 2,
    {_PageNo, _MinDocKey, MaxDocKey} = element(Mid, Boundaries),
    case MaxDocKey < DocKey of
        true ->
            client_first_page_for_doc(
                DocKey, Boundaries, Mid + 1, Hi
            );
        false ->
            client_first_page_for_doc(
                DocKey, Boundaries, Lo, Mid
            )
    end.

client_collect_pages_for_doc(_DocKey, Boundaries, Index, Acc) when
    Index > tuple_size(Boundaries)
->
    lists:reverse(Acc);
client_collect_pages_for_doc(DocKey, Boundaries, Index, Acc) ->
    {PageNo, MinDocKey, MaxDocKey} = element(Index, Boundaries),
    case MinDocKey > DocKey of
        true ->
            lists:reverse(Acc);
        false ->
            Acc1 =
                case MaxDocKey >= DocKey of
                    true -> [PageNo | Acc];
                    false -> Acc
                end,
            client_collect_pages_for_doc(
                DocKey, Boundaries, Index + 1, Acc1
            )
    end.

client_finish_search(
    Bookie,
    Schema,
    AST,
    Opts,
    Hook,
    TokenSpecs,
    Shards,
    TailSummaries,
    PageStates
) ->
    Raw = lists:foldl(
        fun(Shard, Acc) ->
            PageState = maps:get(Shard, PageStates, #{}),
            Specs = client_specs_for_shard(TokenSpecs, Shard, Schema),
            State = client_apply_tail(
                Bookie,
                Schema,
                Shard,
                Specs,
                PageState,
                Hook,
                maps:get(Shard, TailSummaries)
            ),
            client_merge_shard_docs(State, Acc)
        end,
        #{},
        Shards
    ),
    Metas = client_raw_metas(Raw, Schema, AST, #{}),
    client_evaluate(AST, Metas, Bookie, Schema, Opts).

client_use_raw_near(_AST, _BooleanStates) ->
    false.

client_read_tail_summaries(Bookie, Schema, Shards) ->
    Bucket = maps:get(index, Schema),
    maps:from_list([
        {Shard,
            leveled_bookie:book_headonly(
                Bookie, Bucket, client_shard_key(Shard), <<"tailsum">>
            )}
     || Shard <- Shards
    ]).

client_has_dirty_tail(Summaries, TokenSpecs, Schema) ->
    maps:fold(
        fun
            (_Shard, _SummaryRow, true) ->
                true;
            (Shard, {ok, Summary}, false) ->
                Specs = client_specs_for_shard(TokenSpecs, Shard, Schema),
                client_tailsum_admits_specs(Summary, Specs);
            (_Shard, not_found, false) ->
                false
        end,
        false,
        Summaries
    ).

client_ast_token_specs({term, Token, Prefix, Columns}) ->
    [{Token, Prefix, Columns}];
client_ast_token_specs({phrase, Specs, Columns}) ->
    [{Token, Prefix, Columns} || {Token, Prefix, _Offset} <- Specs];
client_ast_token_specs({near, Items, _Distance, _Columns}) ->
    lists:append([client_ast_token_specs(Item) || Item <- Items]);
client_ast_token_specs({anchor, AST}) ->
    client_ast_token_specs(AST);
client_ast_token_specs({'and', A, B}) ->
    client_ast_token_specs(A) ++ client_ast_token_specs(B);
client_ast_token_specs({'or', A, B}) ->
    client_ast_token_specs(A) ++ client_ast_token_specs(B);
client_ast_token_specs({'not', A, B}) ->
    client_ast_token_specs(A) ++ client_ast_token_specs(B);
client_ast_token_specs(_AST) ->
    [].

client_position_specs(AST, Opts) ->
    case maps:get(return_positions, Opts, false) of
        true -> client_ast_token_specs(AST);
        false -> client_required_position_specs(AST)
    end.

client_required_position_specs({phrase, Specs, Columns}) ->
    [{Token, Prefix, Columns} || {Token, Prefix, _Offset} <- Specs];
client_required_position_specs({near, Items, _Distance, _Columns}) ->
    lists:append([client_ast_token_specs(Item) || Item <- Items]);
client_required_position_specs({anchor, AST}) ->
    client_ast_token_specs(AST);
client_required_position_specs({'and', A, B}) ->
    client_required_position_specs(A) ++ client_required_position_specs(B);
client_required_position_specs({'or', A, B}) ->
    client_required_position_specs(A) ++ client_required_position_specs(B);
client_required_position_specs({'not', A, B}) ->
    client_required_position_specs(A) ++ client_required_position_specs(B);
client_required_position_specs(_AST) ->
    [].

client_read_planned_pages(Bookie, Schema, AST, TokenSpecs) ->
    Required = lists:usort(client_required_specs(AST)),
    case client_choose_anchor(Bookie, Schema, Required) of
        none ->
            client_read_query_pages(Bookie, Schema, TokenSpecs, all, #{});
        {Anchor, RequiredRest, Heads} ->
            AnchorStates = client_read_query_pages(
                Bookie, Schema, [Anchor], all, Heads
            ),
            Candidates0 = client_state_doc_keys(AnchorStates),
            {RequiredStates, Candidates} = client_read_required_pages(
                Bookie,
                Schema,
                RequiredRest,
                Candidates0,
                Heads,
                AnchorStates
            ),
            Optional = lists:subtract(TokenSpecs, Required),
            OptionalStates = client_read_query_pages(
                Bookie, Schema, Optional, Candidates, Heads
            ),
            Combined = maps:fold(
                fun(Shard, State, Acc) ->
                    Acc#{
                        Shard => client_merge_state(
                            maps:get(Shard, Acc, #{}), State
                        )
                    }
                end,
                RequiredStates,
                OptionalStates
            ),
            client_filter_states(Combined, Candidates)
    end.

client_read_required_pages(
    _Bookie,
    _Schema,
    [],
    Candidates,
    _Heads,
    States
) ->
    {States, Candidates};
client_read_required_pages(
    Bookie,
    Schema,
    [Spec | Rest],
    Candidates0,
    Heads,
    States0
) ->
    SpecStates = client_read_query_pages(
        Bookie, Schema, [Spec], Candidates0, Heads
    ),
    SpecKeys = client_state_doc_keys(SpecStates),
    Candidates = maps:filter(
        fun(DocKey, _True) -> maps:is_key(DocKey, SpecKeys) end,
        Candidates0
    ),
    States = maps:fold(
        fun(Shard, State, Acc) ->
            Acc#{
                Shard => client_merge_state(
                    maps:get(Shard, Acc, #{}), State
                )
            }
        end,
        States0,
        SpecStates
    ),
    client_read_required_pages(
        Bookie, Schema, Rest, Candidates, Heads, States
    ).

client_filter_states(States, Candidates) ->
    maps:map(
        fun(_Shard, State) ->
            maps:filter(
                fun(DocKey, _Entry) -> maps:is_key(DocKey, Candidates) end,
                State
            )
        end,
        States
    ).

client_required_specs({term, Token, Prefix, Columns}) ->
    [{Token, Prefix, Columns}];
client_required_specs({phrase, Specs, Columns}) ->
    [{Token, Prefix, Columns} || {Token, Prefix, _Offset} <- Specs];
client_required_specs({near, Items, _Distance, _Columns}) ->
    lists:append([client_required_specs(Item) || Item <- Items]);
client_required_specs({anchor, AST}) ->
    client_required_specs(AST);
client_required_specs({'and', A, B}) ->
    client_required_specs(A) ++ client_required_specs(B);
client_required_specs({'not', A, _B}) ->
    client_required_specs(A);
client_required_specs({'or', _A, _B}) ->
    [];
client_required_specs(_AST) ->
    [].

client_choose_anchor(_Bookie, _Schema, []) ->
    none;
client_choose_anchor(Bookie, Schema, Specs) ->
    Estimated = [
        begin
            HeadRows =
                case Prefix of
                    true ->
                        prefix;
                    false ->
                        client_read_plane_heads(
                            Bookie,
                            Schema,
                            Token,
                            ?BOOLEAN_PLANE,
                            client_selector_column_ids(Columns, Schema)
                        )
                end,
            Estimate =
                case HeadRows of
                    prefix ->
                        ?MAX_U32;
                    _ ->
                        lists:sum([
                            case Head of
                                not_found -> 0;
                                {ok, Value} -> client_page_total_docs(Value)
                            end
                         || {_Column, Head} <- HeadRows
                        ])
                end,
            {Estimate, Spec, HeadRows}
        end
     || {Token, Prefix, Columns} = Spec <- Specs
    ],
    [{_Estimate, Anchor, _} | OrderedRest] = lists:sort(Estimated),
    Heads = maps:from_list([
        {Spec, HeadRows}
     || {_N, Spec, HeadRows} <- Estimated, HeadRows =/= prefix
    ]),
    {Anchor, [Spec || {_N, Spec, _Rows} <- OrderedRest], Heads}.

client_state_doc_keys(States) ->
    maps:fold(
        fun(_Shard, State, Acc0) ->
            maps:fold(
                fun(DocKey, _Entry, Acc1) -> Acc1#{DocKey => true} end,
                Acc0,
                State
            )
        end,
        #{},
        States
    ).

client_read_query_pages(Bookie, Schema, TokenSpecs, Candidates, Heads) ->
    client_read_query_plane(
        Bookie, Schema, TokenSpecs, ?BOOLEAN_PLANE, Candidates, Heads
    ).

client_read_query_positions(Bookie, Schema, TokenSpecs, BooleanStates) ->
    lists:foldl(
        fun({Token, Prefix, Columns}, Acc0) ->
            ColumnIds = client_selector_column_ids(Columns, Schema),
            CandidatesByColumn = client_spec_column_candidates(
                BooleanStates, Token, Prefix, ColumnIds, Schema
            ),
            lists:foldl(
                fun(Column, Acc1) ->
                    Candidates = maps:get(Column, CandidatesByColumn, #{}),
                    case map_size(Candidates) of
                        0 ->
                            Acc1;
                        _ ->
                            TokenRows =
                                case Prefix of
                                    false ->
                                        #{
                                            Token => client_read_token_positions(
                                                Bookie,
                                                Schema,
                                                Token,
                                                Column,
                                                Candidates
                                            )
                                        };
                                    true ->
                                        client_fold_prefix_positions(
                                            Bookie,
                                            Schema,
                                            Token,
                                            Column,
                                            Candidates
                                        )
                                end,
                            maps:fold(
                                fun(ActualToken, PositionsByDoc, A) ->
                                    Shard = client_shard_id(
                                        ActualToken, maps:get(shards, Schema)
                                    ),
                                    A#{
                                        Shard => client_hydrate_state_positions(
                                            maps:get(Shard, A, #{}),
                                            ActualToken,
                                            Column,
                                            PositionsByDoc
                                        )
                                    }
                                end,
                                Acc1,
                                TokenRows
                            )
                    end
                end,
                Acc0,
                ColumnIds
            )
        end,
        BooleanStates,
        TokenSpecs
    ).

client_read_near_raw(
    Bookie,
    Schema,
    {near, Items, Distance, Columns},
    BooleanStates
) ->
    Raw0 = maps:fold(
        fun(_Shard, State, Acc) -> client_merge_shard_docs(State, Acc) end,
        #{},
        BooleanStates
    ),
    ColumnIds = client_selector_column_ids(Columns, Schema),
    Matches = lists:foldl(
        fun(Column, MatchAcc) ->
            Candidates = client_near_raw_candidates(Raw0, Items, Column),
            case map_size(Candidates) of
                0 ->
                    MatchAcc;
                _ ->
                    DocCandidates = maps:map(
                        fun(_DocKey, _Versions) -> true end, Candidates
                    ),
                    SpecRows = [
                        client_read_position_spec_rows(
                            Bookie, Schema, Spec, Column, DocCandidates
                        )
                     || Spec <- Items
                    ],
                    ColumnMatches = maps:fold(
                        fun(DocKey, Versions, DocAcc) ->
                            VersionMatches = maps:fold(
                                fun(Version, _True, VersionAcc) ->
                                    SpanLists = [
                                        client_position_spec_spans(
                                            Rows, DocKey, Version
                                        )
                                     || Rows <- SpecRows
                                    ],
                                    case
                                        lists:any(
                                            fun(Spans) -> Spans =:= [] end,
                                            SpanLists
                                        )
                                    of
                                        true ->
                                            VersionAcc;
                                        false ->
                                            case
                                                near_positions(
                                                    SpanLists, Distance
                                                )
                                            of
                                                [] ->
                                                    VersionAcc;
                                                Positions ->
                                                    VersionAcc#{
                                                        Version => Positions
                                                    }
                                            end
                                    end
                                end,
                                #{},
                                Versions
                            ),
                            case map_size(VersionMatches) of
                                0 -> DocAcc;
                                _ -> DocAcc#{DocKey => VersionMatches}
                            end
                        end,
                        #{},
                        Candidates
                    ),
                    client_merge_near_matches(MatchAcc, ColumnMatches)
            end
        end,
        #{},
        ColumnIds
    ),
    Raw = maps:fold(
        fun(DocKey, Versions, Acc) ->
            case maps:find(DocKey, Matches) of
                error ->
                    Acc;
                {ok, MatchVersions} ->
                    Kept = maps:with(maps:keys(MatchVersions), Versions),
                    case map_size(Kept) of
                        0 -> Acc;
                        _ -> Acc#{DocKey => Kept}
                    end
            end
        end,
        #{},
        Raw0
    ),
    Precomputed = maps:map(
        fun(_DocKey, Versions) -> lists:append(maps:values(Versions)) end,
        Matches
    ),
    {ok, Raw, Precomputed}.

client_near_raw_candidates(Raw, Specs, Column) ->
    maps:fold(
        fun(DocKey, Versions, Acc) ->
            Kept = maps:filter(
                fun(_Version, {_Length, Posting}) ->
                    Tokens = maps:get(Column, Posting, #{}),
                    lists:all(
                        fun({term, Token, Prefix, _Columns}) ->
                            lists:any(
                                fun(Actual) ->
                                    client_token_matches(
                                        Actual, Token, Prefix
                                    )
                                end,
                                maps:keys(Tokens)
                            )
                        end,
                        Specs
                    )
                end,
                Versions
            ),
            case map_size(Kept) of
                0 ->
                    Acc;
                _ ->
                    Acc#{
                        DocKey => maps:map(
                            fun(_Version, _Posting) -> true end, Kept
                        )
                    }
            end
        end,
        #{},
        Raw
    ).

client_merge_near_matches(A, B) ->
    maps:fold(
        fun(DocKey, Versions, Acc) ->
            Existing = maps:get(DocKey, Acc, #{}),
            Acc#{
                DocKey => maps:merge_with(
                    fun(_Version, PsA, PsB) -> PsA ++ PsB end,
                    Existing,
                    Versions
                )
            }
        end,
        A,
        B
    ).

client_read_position_spec_rows(
    Bookie,
    Schema,
    {term, Token, false, _Columns},
    Column,
    Candidates
) ->
    #{
        Token => client_read_token_positions(
            Bookie, Schema, Token, Column, Candidates
        )
    };
client_read_position_spec_rows(
    Bookie,
    Schema,
    {term, Prefix, true, _Columns},
    Column,
    Candidates
) ->
    client_fold_prefix_positions(
        Bookie, Schema, Prefix, Column, Candidates
    ).

client_position_spec_spans(TokenRows, DocKey, Version) ->
    [
        {Position, Position}
     || PositionsByDoc <- maps:values(TokenRows),
        {PositionVersion, Positions} <- [
            maps:get(
                DocKey, PositionsByDoc, {undefined, []}
            )
        ],
        PositionVersion =:= Version,
        Position <- Positions
    ].

client_read_token_positions(Bookie, Schema, Token, Column, Candidates) ->
    Bucket = maps:get(index, Schema),
    Key = client_token_key(Token),
    Head = client_read_page(
        Bookie, Bucket, Key, ?POSITION_PLANE, Column, 0
    ),
    client_read_position_column_pages(
        Bookie, Schema, Token, Column, Candidates, Head
    ).

client_read_position_column_pages(
    _Bookie,
    _Schema,
    _Token,
    _Column,
    _Candidates,
    not_found
) ->
    #{};
client_read_position_column_pages(
    Bookie,
    Schema,
    Token,
    Column,
    Candidates,
    {ok, Value0}
) ->
    Bucket = maps:get(index, Schema),
    Key = client_token_key(Token),
    Groups = client_page_candidate_groups(Value0, Candidates),
    OverflowPages = [PageNo || {PageNo, _} <- Groups, PageNo > 0],
    Folded =
        case length(OverflowPages) >= 3 of
            true ->
                client_fold_exact_plane_pages(
                    Bookie, Bucket, Key, ?POSITION_PLANE, Column, OverflowPages
                );
            false ->
                none
        end,
    lists:foldl(
        fun({PageNo, PageCandidates}, Acc) ->
            Value =
                case PageNo of
                    0 ->
                        Value0;
                    _ ->
                        case Folded of
                            none ->
                                {ok, PageValue} = client_read_page(
                                    Bookie,
                                    Bucket,
                                    Key,
                                    ?POSITION_PLANE,
                                    Column,
                                    PageNo
                                ),
                                PageValue;
                            PageValues ->
                                maps:get(PageNo, PageValues)
                        end
                end,
            {_IgnoredCount, Positions} = client_decode_plane_page(
                Token, Column, ?POSITION_PLANE, Value, PageCandidates, Schema
            ),
            client_merge_position_rows(Acc, Positions)
        end,
        #{},
        Groups
    ).

client_fold_prefix_positions(Bookie, Schema, Prefix, Column, Candidates) ->
    Bucket = maps:get(index, Schema),
    Start = client_token_key(Prefix),
    Finish = <<Start/binary, 255>>,
    Fold = fun
        (B, {<<"t:", Token/binary>>, SubKey}, Value, Acc) when
            B =:= Bucket
        ->
            case client_decode_page_row(SubKey, Value) of
                {page, ?POSITION_PLANE, Column, _PageNo} ->
                    case binary_prefix(Token, Prefix) of
                        true ->
                            {_PageCount, Positions} = client_decode_plane_page(
                                Token,
                                Column,
                                ?POSITION_PLANE,
                                Value,
                                Candidates,
                                Schema
                            ),
                            Acc#{
                                Token => client_merge_position_rows(
                                    maps:get(Token, Acc, #{}), Positions
                                )
                            };
                        false ->
                            Acc
                    end;
                {page, _Plane, _Column, _PageNo} ->
                    Acc;
                not_page ->
                    Acc
            end;
        (_B, _K, _V, Acc) ->
            Acc
    end,
    {async, Runner} = leveled_bookie:book_headfold(
        Bookie,
        ?HEAD_TAG,
        {range, Bucket, {{Start, <<>>}, {Finish, <<255>>}}},
        {Fold, #{}},
        false,
        true,
        false
    ),
    Runner().

client_spec_column_candidates(States, Token, Prefix, ColumnIds, Schema) ->
    Shards = client_token_shards(
        Token, Prefix, maps:get(shards, Schema)
    ),
    lists:foldl(
        fun(Shard, Acc0) ->
            State = maps:get(Shard, States, #{}),
            maps:fold(
                fun(DocKey, {_Version, _Length, Posting}, Acc1) ->
                    lists:foldl(
                        fun(Column, A) ->
                            Tokens = maps:get(Column, Posting, #{}),
                            case
                                lists:any(
                                    fun(Actual) ->
                                        client_token_matches(
                                            Actual, Token, Prefix
                                        )
                                    end,
                                    maps:keys(Tokens)
                                )
                            of
                                true ->
                                    A#{
                                        Column =>
                                            (maps:get(Column, A, #{}))#{
                                                DocKey => true
                                            }
                                    };
                                false ->
                                    A
                            end
                        end,
                        Acc1,
                        ColumnIds
                    )
                end,
                Acc0,
                State
            )
        end,
        #{},
        Shards
    ).

client_read_query_plane(Bookie, Schema, TokenSpecs, Plane, Candidates, Heads) ->
    lists:foldl(
        fun({Token, Prefix, Columns} = Spec, Acc) ->
            ColumnIds = client_selector_column_ids(Columns, Schema),
            TokenRows =
                case Prefix of
                    false ->
                        #{
                            Token => client_read_token_plane(
                                Bookie,
                                Schema,
                                Token,
                                Plane,
                                ColumnIds,
                                Candidates,
                                maps:get(Spec, Heads, [])
                            )
                        };
                    true ->
                        client_fold_prefix_plane(
                            Bookie, Schema, Token, Plane, ColumnIds, Candidates
                        )
                end,
            maps:fold(
                fun(ActualToken, Docs, A) ->
                    Shard = client_shard_id(
                        ActualToken, maps:get(shards, Schema)
                    ),
                    Existing = maps:get(Shard, A, #{}),
                    A#{Shard => client_merge_state(Existing, Docs)}
                end,
                Acc,
                TokenRows
            )
        end,
        #{},
        TokenSpecs
    ).

client_read_plane_heads(Bookie, Schema, Token, Plane, ColumnIds) ->
    Bucket = maps:get(index, Schema),
    Key = client_token_key(Token),
    [
        {Column,
            client_read_page(
                Bookie, Bucket, Key, Plane, Column, 0
            )}
     || Column <- ColumnIds
    ].

client_read_token_plane(
    Bookie,
    Schema,
    Token,
    Plane,
    ColumnIds,
    Candidates,
    Heads
) ->
    HeadMap = maps:from_list(Heads),
    lists:foldl(
        fun(Column, Acc) ->
            Head =
                case maps:find(Column, HeadMap) of
                    {ok, Result} ->
                        Result;
                    error ->
                        Bucket = maps:get(index, Schema),
                        client_read_page(
                            Bookie,
                            Bucket,
                            client_token_key(Token),
                            Plane,
                            Column,
                            0
                        )
                end,
            Docs = client_read_column_pages(
                Bookie, Schema, Token, Plane, Column, Candidates, Head
            ),
            client_merge_state(Acc, Docs)
        end,
        #{},
        ColumnIds
    ).

client_read_column_pages(
    _Bookie,
    _Schema,
    _Token,
    _Plane,
    _Column,
    _Candidates,
    not_found
) ->
    #{};
client_read_column_pages(
    Bookie,
    Schema,
    Token,
    Plane,
    Column,
    Candidates,
    {ok, Value0}
) ->
    Bucket = maps:get(index, Schema),
    Key = client_token_key(Token),
    lists:foldl(
        fun({PageNo, PageCandidates}, Acc) ->
            Value =
                case PageNo of
                    0 ->
                        Value0;
                    _ ->
                        {ok, PageValue} = client_read_page(
                            Bookie, Bucket, Key, Plane, Column, PageNo
                        ),
                        PageValue
                end,
            {_IgnoredCount, Docs} = client_decode_plane_page(
                Token, Column, Plane, Value, PageCandidates, Schema
            ),
            client_merge_state(Acc, Docs)
        end,
        #{},
        client_page_candidate_groups(Value0, Candidates)
    ).

client_fold_prefix_plane(Bookie, Schema, Prefix, Plane, ColumnIds, Candidates) ->
    Bucket = maps:get(index, Schema),
    Start = client_token_key(Prefix),
    Finish = <<Start/binary, 255>>,
    Fold = fun
        (B, {<<"t:", Token/binary>>, SubKey}, Value, Acc) when
            B =:= Bucket
        ->
            case client_decode_page_row(SubKey, Value) of
                {page, Plane, Column, _PageNo} ->
                    case
                        lists:member(Column, ColumnIds) andalso
                            binary_prefix(Token, Prefix)
                    of
                        true ->
                            {_PageCount, Docs} = client_decode_plane_page(
                                Token, Column, Plane, Value, Candidates, Schema
                            ),
                            Acc#{
                                Token => client_merge_state(
                                    maps:get(Token, Acc, #{}), Docs
                                )
                            };
                        false ->
                            Acc
                    end;
                {page, _OtherPlane, _Column, _PageNo} ->
                    Acc;
                not_page ->
                    Acc
            end;
        (_B, _K, _V, Acc) ->
            Acc
    end,
    {async, Runner} = leveled_bookie:book_headfold(
        Bookie,
        ?HEAD_TAG,
        {range, Bucket, {{Start, <<>>}, {Finish, <<255>>}}},
        {Fold, #{}},
        false,
        true,
        false
    ),
    Runner().

client_selector_column_ids(all, Schema) ->
    lists:seq(0, length(maps:get(columns, Schema)) - 1);
client_selector_column_ids({not_columns, Excluded}, Schema) ->
    lists:subtract(
        lists:seq(0, length(maps:get(columns, Schema)) - 1),
        client_selector_column_ids(Excluded, Schema)
    );
client_selector_column_ids(Columns, Schema) ->
    SchemaColumns = maps:get(columns, Schema),
    [
        ColumnId
     || {ColumnId, Column} <- lists:zip(
            lists:seq(0, length(SchemaColumns) - 1), SchemaColumns
        ),
        lists:member(Column, Columns)
    ].

client_specs_for_shard(TokenSpecs, Shard, Schema) ->
    [
        Spec
     || {Token, Prefix, _Columns} = Spec <- TokenSpecs,
        lists:member(
            Shard,
            client_token_shards(Token, Prefix, maps:get(shards, Schema))
        )
    ].

client_tailsum_admits_specs(Summary, Specs) ->
    lists:any(
        fun({Token, Prefix, _Columns}) ->
            client_tailsum_might_contain(Summary, Token, Prefix)
        end,
        Specs
    ).

client_apply_tail(_Bookie, _Schema, _Shard, [], PageState, _Hook, _Summary) ->
    PageState;
client_apply_tail(Bookie, Schema, Shard, Specs, PageState, Hook, SummaryRow) ->
    case SummaryRow of
        not_found ->
            PageState;
        {ok, Summary} ->
            case client_tailsum_admits_specs(Summary, Specs) of
                false ->
                    PageState;
                true ->
                    {Tail0, _Rows} = client_fold_shard_tail(
                        Bookie, Schema, Shard
                    ),
                    Tail = client_canonical_tail(Bookie, Schema, Tail0),
                    client_call_hook(Hook, {Shard, Tail}),
                    Masked = maps:without(
                        client_tail_doc_ids(Tail), PageState
                    ),
                    maps:fold(
                        fun
                            (
                                _DocKey,
                                {_V, _DocId, _RetiredIds, _L, _Base, remove,
                                    _Posting},
                                Acc
                            ) ->
                                Acc;
                            (
                                _DocKey,
                                {_V, DocId, _RetiredIds, L, _Base, live,
                                    Posting},
                                Acc
                            ) ->
                                Filtered = client_filter_posting(
                                    Posting, Specs, Schema
                                ),
                                case map_size(Filtered) of
                                    0 ->
                                        Acc;
                                    _ ->
                                        client_put_state(
                                            DocId, {DocId, L, Filtered}, Acc
                                        )
                                end
                        end,
                        Masked,
                        Tail
                    )
            end
    end.

client_tail_doc_ids(Tail) ->
    lists:usort(
        lists:append([
            [DocId | RetiredIds]
         || {_DocKey,
                {_Version, DocId, RetiredIds, _Length, _Base, _Kind, _Posting}} <- maps:to_list(
                Tail
            )
        ])
    ).

client_epoch_sqn(Bookie, Schema, Shard) ->
    leveled_bookie:book_sqn(
        Bookie,
        maps:get(index, Schema),
        {client_shard_key(Shard), <<"epoch">>},
        ?HEAD_TAG
    ).

client_fold_shard_tail(Bookie, Schema, Shard) ->
    Bucket = maps:get(index, Schema),
    ShardKey = client_shard_key(Shard),
    Fold = fun
        (
            B,
            {K, <<"d:", DocKey/binary>> = SubKey},
            Value,
            {Docs, Keys}
        ) when B =:= Bucket, K =:= ShardKey ->
            {Docs#{DocKey => client_decode_tail(Value)}, [SubKey | Keys]};
        (_B, _K, _V, Acc) ->
            Acc
    end,
    {async, Runner} = leveled_bookie:book_headfold(
        Bookie,
        ?HEAD_TAG,
        {range, Bucket, {{ShardKey, <<>>}, {ShardKey, <<255>>}}},
        {Fold, {#{}, []}},
        false,
        true,
        false
    ),
    Runner().

client_merge_shard_docs(State, Acc) ->
    maps:fold(
        fun(Key, {DocVersion, DocLength, Posting}, A) ->
            ByVersion = maps:get(Key, A, #{}),
            {Length, Existing} = maps:get(
                DocVersion, ByVersion, {DocLength, #{}}
            ),
            Merged = client_merge_posting(Existing, Posting),
            A#{Key => ByVersion#{DocVersion => {Length, Merged}}}
        end,
        Acc,
        State
    ).

client_raw_metas(Raw, Schema, AST, Precomputed) ->
    maps:fold(
        fun(DocKey, ByVersion, Acc) ->
            maps:fold(
                fun(Version, {DocLength, Posting}, A) ->
                    case maps:find(DocKey, Precomputed) of
                        {ok, MatchPositions} ->
                            Meta = client_meta(
                                DocKey, DocLength, Posting, Schema
                            ),
                            A#{
                                {DocKey, Version} => Meta#{
                                    precomputed_eval =>
                                        {true, #{near => MatchPositions}}
                                }
                            };
                        error ->
                            case client_posting_prefilter(AST, Posting) of
                                false ->
                                    A;
                                true ->
                                    A#{
                                        {DocKey, Version} => client_meta(
                                            DocKey, DocLength, Posting, Schema
                                        )
                                    };
                                {match, MatchPositions} ->
                                    Meta = client_meta(
                                        DocKey, DocLength, Posting, Schema
                                    ),
                                    A#{
                                        {DocKey, Version} => Meta#{
                                            precomputed_eval =>
                                                {true, #{
                                                    near => MatchPositions
                                                }}
                                        }
                                    }
                            end
                    end
                end,
                Acc,
                ByVersion
            )
        end,
        #{},
        Raw
    ).

client_posting_prefilter({near, Items, Distance, _Columns}, Posting) ->
    case client_posting_near_positions(Items, Distance, Posting) of
        unknown -> true;
        [] -> false;
        Positions -> {match, Positions}
    end;
client_posting_prefilter(_AST, _Posting) ->
    true.

client_posting_near_positions(Items, Distance, Posting) ->
    lists:foldl(
        fun
            (_ColumnPosting, unknown) ->
                unknown;
            ({_Column, Tokens}, Acc) ->
                case client_posting_near_column(Items, Distance, Tokens) of
                    unknown -> unknown;
                    Positions -> Acc ++ Positions
                end
        end,
        [],
        maps:to_list(Posting)
    ).

client_posting_near_column(Items, Distance, Tokens) ->
    SpanLists = [client_posting_item_spans(Item, Tokens) || Item <- Items],
    case lists:member(unknown, SpanLists) of
        true ->
            unknown;
        false ->
            case lists:any(fun(Spans) -> Spans =:= [] end, SpanLists) of
                true -> [];
                false -> near_positions(SpanLists, Distance)
            end
    end.

client_posting_item_spans({term, Token, Prefix, _Columns}, Tokens) ->
    Positions = lists:append([
        maps:get(positions, Entry)
     || {Actual, Entry} <- maps:to_list(Tokens),
        client_token_matches(Actual, Token, Prefix)
    ]),
    [{Position, Position} || Position <- Positions];
client_posting_item_spans({anchor, Item}, Tokens) ->
    case client_posting_item_spans(Item, Tokens) of
        unknown -> unknown;
        Spans -> [Span || {Start, _End} = Span <- Spans, Start =:= 0]
    end;
client_posting_item_spans(_Item, _Tokens) ->
    unknown.

client_merge_state(A, B) ->
    maps:fold(
        fun(DocKey, Entry, Acc) -> client_put_state(DocKey, Entry, Acc) end,
        A,
        B
    ).

client_merge_position_rows(A, B) ->
    maps:fold(
        fun(DocKey, {Version, Positions}, Acc) ->
            case maps:find(DocKey, Acc) of
                {ok, {Version, ExistingPositions}} ->
                    Acc#{
                        DocKey =>
                            {Version, lists:merge(ExistingPositions, Positions)}
                    };
                _ ->
                    Acc#{DocKey => {Version, Positions}}
            end
        end,
        A,
        B
    ).

client_put_state(DocKey, {Version, Length, Posting}, State) ->
    case maps:get(DocKey, State, none) of
        {Version, Length0, Existing} ->
            State#{
                DocKey =>
                    {Version, Length0, client_merge_posting(Existing, Posting)}
            };
        _ ->
            State#{DocKey => {Version, Length, Posting}}
    end.

client_hydrate_state_positions(State, Token, Column, PositionsByDoc) ->
    maps:fold(
        fun(DocKey, {PositionVersion, Positions}, Acc) ->
            case maps:find(DocKey, Acc) of
                error ->
                    Acc;
                {ok, {Version, Length, Posting}} when
                    Version =:= PositionVersion
                ->
                    ColumnTokens = maps:get(Column, Posting, #{}),
                    case maps:find(Token, ColumnTokens) of
                        error ->
                            Acc;
                        {ok, Entry} ->
                            Posting1 = Posting#{
                                Column => ColumnTokens#{
                                    Token => Entry#{positions => Positions}
                                }
                            },
                            Acc#{DocKey => {Version, Length, Posting1}}
                    end;
                {ok, {_OtherVersion, _Length, _Posting}} ->
                    Acc
            end
        end,
        State,
        PositionsByDoc
    ).

client_filter_posting(Posting, Specs, Schema) ->
    maps:fold(
        fun(Column, Tokens, Acc) ->
            Kept = maps:filter(
                fun(Token, _Entry) ->
                    lists:any(
                        fun({Wanted, Prefix, Columns}) ->
                            client_token_matches(Token, Wanted, Prefix) andalso
                                client_column_matches(Column, Columns, Schema)
                        end,
                        Specs
                    )
                end,
                Tokens
            ),
            case map_size(Kept) of
                0 -> Acc;
                _ -> Acc#{Column => Kept}
            end
        end,
        #{},
        Posting
    ).

client_token_matches(Token, Wanted, false) -> Token =:= Wanted;
client_token_matches(Token, Wanted, true) -> binary_prefix(Token, Wanted).

client_column_matches(_Column, all, _Schema) ->
    true;
client_column_matches(Column, {not_columns, Excluded}, Schema) ->
    not client_column_matches(Column, Excluded, Schema);
client_column_matches(Column, Columns, Schema) ->
    lists:member(lists:nth(Column + 1, maps:get(columns, Schema)), Columns).

client_merge_posting(A, B) ->
    maps:fold(
        fun(Col, Tokens, Acc) ->
            Acc#{Col => maps:merge(maps:get(Col, Acc, #{}), Tokens)}
        end,
        A,
        B
    ).

client_meta(Key, DocLength, Posting, Schema) ->
    Columns = maps:get(columns, Schema),
    Positions = maps:from_list([
        {
            lists:nth(ColId + 1, Columns),
            maps:map(
                fun(_Token, Entry) -> maps:get(positions, Entry) end, Tokens
            )
        }
     || {ColId, Tokens} <- maps:to_list(Posting)
    ]),
    Counts = maps:from_list([
        {
            lists:nth(ColId + 1, Columns),
            maps:map(fun(_Token, Entry) -> maps:get(count, Entry) end, Tokens)
        }
     || {ColId, Tokens} <- maps:to_list(Posting)
    ]),
    #{
        key => Key,
        doc_length => DocLength,
        positions => Positions,
        counts => Counts
    }.

client_empty_meta(Key, DocLength) ->
    #{
        key => Key,
        doc_length => DocLength,
        positions => #{},
        counts => #{}
    }.

client_ast_shards(AST, Schema) ->
    lists:usort(
        lists:append([
            client_token_shards(Token, Prefix, maps:get(shards, Schema))
         || {Token, Prefix} <- client_ast_tokens(AST)
        ])
    ).

client_ast_tokens({term, Token, Prefix, _Cols}) ->
    [{Token, Prefix}];
client_ast_tokens({phrase, Specs, _Cols}) ->
    [{T, P} || {T, P, _} <- Specs];
client_ast_tokens({near, Items, _D, _Cols}) ->
    lists:append([client_ast_tokens(I) || I <- Items]);
client_ast_tokens({anchor, A}) ->
    client_ast_tokens(A);
client_ast_tokens({'and', A, B}) ->
    client_ast_tokens(A) ++ client_ast_tokens(B);
client_ast_tokens({'or', A, B}) ->
    client_ast_tokens(A) ++ client_ast_tokens(B);
client_ast_tokens({'not', A, B}) ->
    client_ast_tokens(A) ++ client_ast_tokens(B);
client_ast_tokens(_) ->
    [].

client_token_shards(Token, false, Shards) ->
    [client_shard_id(Token, Shards)];
client_token_shards(<<>>, true, Shards) ->
    lists:seq(0, Shards - 1);
client_token_shards(Token, true, Shards) ->
    Lo = client_shard_id(Token, Shards),
    HiToken =
        case Token of
            <<B:8>> -> <<B, 255>>;
            <<B1:8, B2:8, _/binary>> -> <<B1, B2>>
        end,
    lists:seq(Lo, client_shard_id(HiToken, Shards)).

client_evaluate(AST, Metas, Bookie, Schema, Opts) ->
    Matches0 = lists:filtermap(
        fun({_K, Meta}) ->
            Evaluation =
                case maps:find(precomputed_eval, Meta) of
                    {ok, Precomputed} -> Precomputed;
                    error -> eval(AST, Meta)
                end,
            case Evaluation of
                false -> false;
                {true, Positions} -> {true, {Meta, Positions}}
            end
        end,
        maps:to_list(Metas)
    ),
    Matches = maps:values(
        maps:from_list([
            {maps:get(key, Meta), {Meta, Positions}}
         || {Meta, Positions} <- Matches0
        ])
    ),
    Ranked = maps:get(rank, Opts, none) =:= bm25,
    Hits0 =
        case Ranked of
            false ->
                [
                    client_hit(AST, Meta, Positions, Opts, 0.0)
                 || {Meta, Positions} <- Matches
                ];
            true ->
                Stats = client_corpus_stats(Bookie, Schema),
                Leaves = scoring_phrases(AST),
                Np = np_map(Leaves, maps:values(Metas)),
                {DocCount, TotalLength} = Stats,
                Idfs = maps:map(
                    fun(_Leaf, NHit) ->
                        client_bm25_idf(NHit, DocCount)
                    end,
                    Np
                ),
                Avg =
                    case DocCount of
                        0 -> 0.0;
                        _ -> TotalLength / DocCount
                    end,
                [
                    client_hit(
                        AST,
                        Meta,
                        Positions,
                        Opts,
                        bm25_score_precomputed(Meta, Leaves, Idfs, Avg)
                    )
                 || {Meta, Positions} <- Matches
                ]
        end,
    Hits1 =
        case Ranked of
            true ->
                client_bounded_ranked_hits(
                    Bookie, Schema, Hits0, client_ranked_window_size(Opts)
                );
            false ->
                lists:sort(
                    fun(A, B) -> maps:get(key, A) =< maps:get(key, B) end, Hits0
                )
        end,
    client_search_result(Bookie, Schema, Hits1, length(Matches), Opts).

client_hit(AST, Meta, MatchPositions, Opts, Score) ->
    Base = #{
        key => maps:get(key, Meta),
        score => Score,
        doc_length => maps:get(doc_length, Meta),
        match_count => client_match_count(AST, Meta, MatchPositions)
    },
    WithPositions =
        case maps:get(return_positions, Opts, false) of
            true -> Base#{positions => client_window_positions(MatchPositions)};
            false -> Base
        end,
    case maps:get(return_terms, Opts, false) of
        true -> WithPositions#{matched_terms => client_matched_terms(AST, Meta)};
        false -> WithPositions
    end.

client_match_count({term, Token, Prefix, Columns}, Meta, _MatchPositions) ->
    client_term_frequency(Meta, Token, Prefix, Columns);
client_match_count({'and', A, B}, Meta, _MatchPositions) ->
    client_matching_branch_count(A, Meta) + client_matching_branch_count(B, Meta);
client_match_count({'or', A, B}, Meta, _MatchPositions) ->
    client_matching_branch_count(A, Meta) + client_matching_branch_count(B, Meta);
client_match_count({'not', A, _B}, Meta, _MatchPositions) ->
    client_matching_branch_count(A, Meta);
client_match_count(_AST, _Meta, MatchPositions) ->
    position_count(MatchPositions).

client_matching_branch_count(AST, Meta) ->
    case eval(AST, Meta) of
        false -> 0;
        {true, MatchPositions} -> client_match_count(AST, Meta, MatchPositions)
    end.

client_matched_terms(AST, Meta) ->
    lists:usort(client_matched_terms_list(AST, Meta)).

client_matched_terms_list(
    {term, Token, Prefix, Columns}, #{counts := Counts}
) ->
    lists:append([
        [
            StoredToken
         || {StoredToken, Count} <- maps:to_list(maps:get(Column, Counts, #{})),
            Count > 0,
            client_token_matches(StoredToken, Token, Prefix)
        ]
     || Column <- concrete_columns(Columns)
    ]);
client_matched_terms_list({'and', A, B}, Meta) ->
    client_matched_branch_terms(A, Meta) ++ client_matched_branch_terms(B, Meta);
client_matched_terms_list({'or', A, B}, Meta) ->
    client_matched_branch_terms(A, Meta) ++ client_matched_branch_terms(B, Meta);
client_matched_terms_list({'not', A, _B}, Meta) ->
    client_matched_branch_terms(A, Meta);
client_matched_terms_list({anchor, AST}, Meta) ->
    client_matched_branch_terms(AST, Meta);
client_matched_terms_list(_AST, _Meta) ->
    [].

client_matched_branch_terms(AST, Meta) ->
    case eval(AST, Meta) of
        false -> [];
        {true, _MatchPositions} -> client_matched_terms_list(AST, Meta)
    end.

client_ranked_window_size(Opts) ->
    maps:get(offset, Opts, 0) + maps:get(limit, Opts, ?DEFAULT_LIMIT).

client_bounded_ranked_hits(_Hits, Limit) when Limit =< 0 ->
    [];
client_bounded_ranked_hits(Hits, Limit) ->
    Tree = lists:foldl(
        fun(Hit, Acc) ->
            client_ranked_heap_add(Hit, Limit, Acc)
        end,
        gb_trees:empty(),
        Hits
    ),
    [
        client_materialize_ranked_candidate(Hit)
     || {_RankKey, Hit} <- gb_trees:to_list(Tree)
    ].

client_bounded_ranked_hits(Bookie, Schema, Hits, Limit) ->
    client_bounded_ranked_live_hits(
        Bookie, Schema, Hits, Limit, erlang:min(Limit, length(Hits))
    ).

client_bounded_ranked_live_hits(
    _Bookie, _Schema, [], _Limit, _Capacity
) ->
    [];
client_bounded_ranked_live_hits(
    _Bookie, _Schema, _Hits, Limit, _Capacity
) when Limit =< 0 ->
    [];
client_bounded_ranked_live_hits(
    Bookie, Schema, Hits, Limit, Capacity
) ->
    Candidates = client_bounded_ranked_hits(Hits, Capacity),
    CutoffScore = client_ranked_candidate_score(lists:last(Candidates)),
    CutoffCandidates = [
        client_materialize_ranked_candidate(Hit)
     || Hit <- Hits,
        client_ranked_candidate_score(Hit) >= CutoffScore
    ],
    Resolved = client_resolve_hit_batch(
        Bookie, Schema, CutoffCandidates
    ),
    Live = client_bounded_ranked_hits(Resolved, Limit),
    Missing = Limit - length(Live),
    case Missing =< 0 orelse Capacity >= length(Hits) of
        true ->
            lists:sublist(Live, Limit);
        false ->
            client_bounded_ranked_live_hits(
                Bookie,
                Schema,
                Hits,
                Limit,
                erlang:min(length(Hits), Capacity + Missing)
            )
    end.

client_ranked_heap_add(Hit, Limit, Tree) ->
    RankKey = client_ranked_candidate_key(Hit),
    case gb_trees:size(Tree) < Limit of
        true ->
            gb_trees:enter(RankKey, Hit, Tree);
        false ->
            {WorstKey, _WorstHit} = gb_trees:largest(Tree),
            case RankKey < WorstKey of
                true ->
                    gb_trees:enter(
                        RankKey, Hit, gb_trees:delete(WorstKey, Tree)
                    );
                false ->
                    Tree
            end
    end.

client_ranked_candidate(Score, Key, DocLength, MatchCount) ->
    client_ranked_candidate(
        Score, Key, DocLength, MatchCount, none
    ).

client_ranked_candidate(
    Score, Key, DocLength, MatchCount, Extra
) ->
    {-Score, Key, DocLength, MatchCount, Extra}.

client_ranked_candidate_key(
    {NegativeScore, Key, _DocLength, _MatchCount, _Extra}
) ->
    {NegativeScore, Key};
client_ranked_candidate_key(Hit) when is_map(Hit) ->
    {-maps:get(score, Hit), maps:get(key, Hit)}.

client_ranked_candidate_score(
    {NegativeScore, _Key, _DocLength, _MatchCount, _Extra}
) ->
    -NegativeScore;
client_ranked_candidate_score(Hit) when is_map(Hit) ->
    maps:get(score, Hit).

client_materialize_ranked_candidate(Hit) when is_map(Hit) ->
    Hit;
client_materialize_ranked_candidate(
    {NegativeScore, Key, DocLength, MatchCount, Extra}
) ->
    Base = #{
        key => Key,
        score => -NegativeScore,
        doc_length => DocLength,
        match_count => MatchCount
    },
    case Extra of
        none ->
            Base;
        {positions, Positions} ->
            Base#{positions => Positions};
        {matched_terms, Terms} ->
            Base#{matched_terms => Terms};
        {internal_near, Version, MatchColumns} ->
            (maps:remove(match_count, Base))#{
                internal_version => Version,
                internal_columns => MatchColumns
            }
    end.

client_resolve_hits(Bookie, Schema, Hits, Opts) ->
    [
        maps:remove(doc_id, Hit)
     || Hit <- client_resolve_hits_with_ids(Bookie, Schema, Hits, Opts)
    ].

client_search_result(Bookie, Schema, Hits, Count, Opts) ->
    Resolved = client_resolve_hits(Bookie, Schema, Hits, Opts),
    case maps:get(return_count, Opts, false) of
        true -> {ok, #{hits => Resolved, count => Count}};
        false -> {ok, Resolved}
    end.

client_resolve_hits_with_ids(Bookie, Schema, Hits, Opts) ->
    Offset = maps:get(offset, Opts, 0),
    Limit = maps:get(limit, Opts, ?DEFAULT_LIMIT),
    Needed = Offset + Limit,
    Resolved = client_resolve_hit_batches(
        Bookie, Schema, Hits, Needed, 0, []
    ),
    Ordered =
        case maps:get(rank, Opts, none) of
            bm25 ->
                lists:sort(
                    fun(A, B) ->
                        {-maps:get(score, A), maps:get(key, A)} =<
                            {-maps:get(score, B), maps:get(key, B)}
                    end,
                    Resolved
                );
            none ->
                lists:sort(
                    fun(A, B) -> maps:get(key, A) =< maps:get(key, B) end,
                    Resolved
                )
        end,
    lists:sublist(drop(Offset, Ordered), Limit).

client_resolve_hit_batches(
    _Bookie, _Schema, _Hits, Needed, Count, Acc
) when
    Count >= Needed
->
    lists:reverse(Acc);
client_resolve_hit_batches(
    _Bookie, _Schema, [], _Needed, _Count, Acc
) ->
    lists:reverse(Acc);
client_resolve_hit_batches(
    Bookie, Schema, Hits, Needed, Count, Acc
) ->
    BatchSize = client_resolution_batch_size(Needed, Count),
    {Batch, Rest} = lists:split(erlang:min(BatchSize, length(Hits)), Hits),
    ResolvedBatch = client_resolve_hit_batch(
        Bookie, Schema, Batch
    ),
    BatchCount = length(ResolvedBatch),
    client_resolve_hit_batches(
        Bookie,
        Schema,
        Rest,
        Needed,
        Count + BatchCount,
        lists:reverse(ResolvedBatch, Acc)
    ).

client_resolution_batch_size(Needed, Resolved) ->
    erlang:max(0, Needed - Resolved).

client_resolve_hit_batch(_Bookie, _Schema, []) ->
    [];
client_resolve_hit_batch(Bookie, #{index := Bucket}, Hits) ->
    DocIds = lists:usort([
        DocId
     || #{key := DocId} <- Hits, is_integer(DocId)
    ]),
    Mappings = client_resolve_doc_id_mappings(Bookie, Bucket, DocIds),
    lists:filtermap(
        fun
            (#{key := DocKey} = Hit) when is_binary(DocKey) ->
                {true, Hit};
            (#{key := DocId} = Hit) when is_integer(DocId) ->
                case maps:get(DocId, Mappings, stale) of
                    stale ->
                        false;
                    {DocKey, DocVersion} ->
                        client_validate_doc_id_mapping(
                            DocId, DocKey, DocVersion
                        ),
                        {true, Hit#{key => DocKey, doc_id => DocId}}
                end
        end,
        Hits
    ).

client_resolve_doc_id_mappings(_Bookie, _Bucket, []) ->
    #{};
client_resolve_doc_id_mappings(Bookie, Bucket, DocIds) ->
    Rows = [
        {<<"id">>, client_doc_id_subkey(DocId)}
     || DocId <- DocIds
    ],
    Heads = leveled_bookie:book_mhead_sqn(Bookie, Bucket, Rows),
    lists:foldl(
        fun
            ({DocId, {_Row, not_found}}, Acc) ->
                Acc#{DocId => stale};
            ({DocId, {{<<"id">>, SubKey}, {ok, SQN}}}, Acc) ->
                Mapping = client_read_cached_doc_id_mapping(
                    Bookie, Bucket, SubKey, SQN
                ),
                Acc#{DocId => Mapping}
        end,
        #{},
        lists:zip(DocIds, Heads)
    ).

client_read_cached_doc_id_mapping(Bookie, Bucket, SubKey, SQN) ->
    CacheKey = {fts_decoded_id_row, direct_v1, Bucket, <<"id">>, SubKey},
    case leveled_bookie:book_valuecache_get(Bookie, CacheKey, SQN) of
        {ok, Mapping} ->
            Mapping;
        miss ->
            {ok, Value} = leveled_bookie:book_headonly(
                Bookie, Bucket, <<"id">>, SubKey
            ),
            Mapping = client_decode_doc_id_row(Value),
            ok = leveled_bookie:book_valuecache_put(
                Bookie, CacheKey, SQN, Mapping
            ),
            Mapping
    end.

client_validate_doc_id_mapping(DocId, DocKey, DocVersion) ->
    case client_transient_doc_id(DocId) of
        false ->
            ok;
        true ->
            case client_doc_id(DocKey, DocVersion) of
                DocId -> ok;
                _ -> erlang:error({invalid_fts_doc_id_mapping, DocId})
            end
    end.

client_fold_manifests(Bookie, Schema) ->
    Rows = client_fold_manifest_rows(Bookie, Schema),
    maps:map(
        fun(_DocKey, Manifest) ->
            {maps:get(version, Manifest), maps:get(doc_length, Manifest)}
        end,
        Rows
    ).

client_fold_manifest_rows(Bookie, Schema) ->
    Bucket = maps:get(index, Schema),
    Fingerprint = maps:get(fingerprint, Schema),
    Fold = fun
        (B, {<<"doc">>, DocKey}, Value, Acc) when B =:= Bucket ->
            M = client_decode_manifest_value(Value),
            case maps:get(fingerprint, M) =:= Fingerprint of
                true ->
                    Acc#{DocKey => M};
                false ->
                    Acc
            end;
        (_B, _K, _V, Acc) ->
            Acc
    end,
    %% Manifest rows live under the {<<"doc">>, DocKey} keyspace. Bound the
    %% fold there: an unbounded bucket fold walks every postings page row
    %% (~60x more rows than manifests) on EVERY search and consolidation
    %% batch — measured at ~1.7s/query on a 7.4K-doc corpus.
    {async, Runner} = leveled_bookie:book_headfold(
        Bookie,
        ?HEAD_TAG,
        {range, Bucket, {{<<"doc">>, <<>>}, {<<"doc;">>, <<>>}}},
        {Fold, #{}},
        false,
        true,
        false
    ),
    Runner().

client_corpus_stats(Bookie, Schema) ->
    Bucket = maps:get(index, Schema),
    Summary =
        case
            leveled_bookie:book_headonly(
                Bookie, Bucket, <<"stats">>, <<"summary">>
            )
        of
            {ok,
                <<?STATS_VERSION:8, 1:8, N:64/unsigned-big, L:64/unsigned-big>>} ->
                {N, L};
            not_found ->
                {0, 0}
        end,
    case
        leveled_bookie:book_headonly(
            Bookie, Bucket, <<"stats">>, <<"dirty">>
        )
    of
        not_found ->
            Summary;
        {ok, _Dirty} ->
            %% The clean summary is the consolidated accumulator.  Every doc
            %% batch also overwrites one stats-tail row carrying current and
            %% preserved-base lengths.  Folding that compact tail supplies
            %% exact deltas across repeated updates and partial shard
            %% consolidation, without manifest reads or a library cache.
            maps:fold(
                fun(_DocKey, Tail, Stats) ->
                    client_apply_stats_tail(Tail, Stats)
                end,
                Summary,
                client_fold_stats_tail(Bookie, Schema)
            )
    end.

client_fold_stats_tail(Bookie, Schema) ->
    Bucket = maps:get(index, Schema),
    Fold = fun
        (B, {<<"stats">>, <<"d:", DocKey/binary>>}, Value, Acc) when
            B =:= Bucket
        ->
            Acc#{DocKey => client_decode_stats_tail(Value)};
        (_B, _K, _V, Acc) ->
            Acc
    end,
    {async, Runner} = leveled_bookie:book_headfold(
        Bookie,
        ?HEAD_TAG,
        {range, Bucket, {{<<"stats">>, <<"d:">>}, {<<"stats">>, <<"d;">>}}},
        {Fold, #{}},
        false,
        true,
        false
    ),
    Runner().

client_apply_stats_tail({_Version, DocLength, none, live}, {N, L}) ->
    {N + 1, L + DocLength};
client_apply_stats_tail({_Version, DocLength, BaseLength, live}, {N, L}) ->
    {N, L + DocLength - BaseLength};
client_apply_stats_tail({_Version, _DocLength, none, remove}, Stats) ->
    Stats;
client_apply_stats_tail({_Version, _DocLength, BaseLength, remove}, {N, L}) ->
    {N - 1, L - BaseLength}.

client_encode_stats_summary(N, L) ->
    client_guard(stats_documents, N, ?MAX_U64),
    client_guard(stats_total_length, L, ?MAX_U64),
    <<?STATS_VERSION:8, 1:8, N:64/unsigned-big, L:64/unsigned-big>>.

client_encode_stats_tail(Version, DocLength, BaseLength, Kind) ->
    KindByte =
        case Kind of
            live -> 1;
            remove -> 0
        end,
    {BaseFlag, BaseValue} = client_encode_base_length(BaseLength),
    <<?STATS_VERSION:8, KindByte:8, BaseFlag:8, Version:8/binary,
        DocLength:64/unsigned-big, BaseValue:64/unsigned-big>>.

client_decode_stats_tail(
    <<?STATS_VERSION:8, KindByte:8, BaseFlag:8, Version:8/binary,
        DocLength:64/unsigned-big, BaseValue:64/unsigned-big>>
) when
    (KindByte =:= 0 orelse KindByte =:= 1) andalso
        (BaseFlag =:= 0 orelse BaseFlag =:= 1)
->
    Kind =
        case KindByte of
            0 -> remove;
            1 -> live
        end,
    {Version, DocLength, client_decode_base_length(BaseFlag, BaseValue), Kind};
client_decode_stats_tail(Bad) ->
    erlang:error({invalid_fts_stats_tail, Bad}).

client_encode_boolean_entry(DocId, DocLength, Count) ->
    client_guard(doc_id, DocId, ?MAX_DOC_ID),
    client_guard(doc_length, DocLength, ?MAX_U64),
    client_guard(true_occurrences, Count, ?MAX_U64),
    <<
        (client_encode_doc_id(DocId))/binary,
        (varint_append(DocLength, <<>>))/binary,
        (varint_append(Count, <<>>))/binary
    >>.

client_encode_position_entries(DocId, Positions) ->
    client_guard(doc_id, DocId, ?MAX_DOC_ID),
    DocIdBin = client_encode_doc_id(DocId),
    [
        <<DocIdBin/binary, (varint_append(byte_size(PosBin), <<>>))/binary,
            PosBin/binary>>
     || {_Count, PosBin} <-
            client_encode_position_chunks(Positions, ?PAGE_POSITION_BYTES)
    ].

%% Derived ids always carry the high-bit version namespace and therefore
%% occupy exactly ten bytes in the existing unsigned-varint page format.
%% Constructing that fixed shape once avoids ten growing-binary copies for
%% every boolean and position entry during a page rebuild.
client_encode_doc_id(DocId) when DocId >= ?TRANSIENT_DOC_ID_BIT ->
    <<
        ((DocId band 16#7F) bor 16#80):8,
        (((DocId bsr 7) band 16#7F) bor 16#80):8,
        (((DocId bsr 14) band 16#7F) bor 16#80):8,
        (((DocId bsr 21) band 16#7F) bor 16#80):8,
        (((DocId bsr 28) band 16#7F) bor 16#80):8,
        (((DocId bsr 35) band 16#7F) bor 16#80):8,
        (((DocId bsr 42) band 16#7F) bor 16#80):8,
        (((DocId bsr 49) band 16#7F) bor 16#80):8,
        (((DocId bsr 56) band 16#7F) bor 16#80):8,
        (DocId bsr 63):8
    >>;
client_encode_doc_id(DocId) ->
    varint_append(DocId, <<>>).

client_encode_token_pages(Docs) ->
    Columns = lists:usort(
        lists:append([
            maps:keys(Posting)
         || {_DocKey, {_V, _L, Posting}} <- maps:to_list(Docs)
        ])
    ),
    lists:append([client_encode_column_pages(Column, Docs) || Column <- Columns]).

client_canonical_tail(Bookie, #{index := Bucket}, Tail) ->
    maps:map(
        fun
            (
                _DocKey,
                {_Version, _DocId, _RetiredIds, _Length, _Base, remove,
                    _Posting} = Row
            ) ->
                Row;
            (
                DocKey,
                {Version, StoredDocId, RetiredIds, Length, Base, live, Posting}
            ) ->
                case
                    leveled_bookie:book_headonly(
                        Bookie, Bucket, <<"doc">>, DocKey
                    )
                of
                    {ok, Value} ->
                        case client_decode_manifest_value(Value) of
                            #{version := Version, doc_id := CurrentDocId} ->
                                {Version, CurrentDocId,
                                    lists:usort([StoredDocId | RetiredIds]),
                                    Length, Base, live, Posting};
                            _ ->
                                {Version, StoredDocId, RetiredIds, Length, Base,
                                    remove, #{}}
                        end;
                    not_found ->
                        {Version, StoredDocId, RetiredIds, Length, Base, remove,
                            #{}}
                end
        end,
        Tail
    ).

client_encode_column_pages(Column, Docs) ->
    GlobalDocs = map_size(Docs),
    DocRows = lists:filtermap(
        fun({DocId, {_Version, DocLength, Posting}}) ->
            case maps:find(Column, Posting) of
                error ->
                    false;
                {ok, Tokens} ->
                    [{_Token, #{count := Count, positions := Positions}}] =
                        maps:to_list(Tokens),
                    PositionRows = [
                        {DocId, {position, PosBin}}
                     || {_PositionCount, PosBin} <-
                            client_encode_position_chunks(
                                Positions, ?PAGE_POSITION_BYTES
                            )
                    ],
                    {true, {{DocId, {boolean, DocLength, Count}}, PositionRows}}
            end
        end,
        lists:sort(maps:to_list(Docs))
    ),
    BooleanRows = [Boolean || {Boolean, _Positions} <- DocRows],
    PositionRows = lists:append([
        Positions
     || {_Boolean, Positions} <- DocRows
    ]),
    client_encode_v8_plane_rows(
        ?BOOLEAN_PLANE, Column, GlobalDocs, BooleanRows
    ) ++
        client_encode_v8_plane_rows(
            ?POSITION_PLANE, Column, GlobalDocs, PositionRows
        ).

client_encode_v8_plane_rows(Plane, Column, GlobalDocs, Rows) ->
    Chunks = client_encode_v8_chunks(Rows, Plane),
    Pages = client_pack_v8_pages(Chunks),
    PageCount = length(Pages),
    client_guard(page_count, PageCount, ?MAX_U16),
    PageRanges = [
        {
            element(1, hd(PageChunks)),
            element(2, lists:last(PageChunks)),
            PageChunks
        }
     || PageChunks <- Pages
    ],
    PageBoundaries = client_encode_page_boundaries(
        GlobalDocs, PageRanges
    ),
    CollectionFrequency =
        case Plane of
            ?BOOLEAN_PLANE ->
                lists:sum([
                    Count
                 || {_DocId, {boolean, _DocLength, Count}} <- Rows
                ]);
            ?POSITION_PLANE ->
                0
        end,
    TermMaxBound =
        lists:max([0 | [element(3, Chunk) || Chunk <- Chunks]]),
    PageZeroBytes =
        32 +
            byte_size(PageBoundaries) +
            lists:sum([
                ?CHUNK_DIR_STRIDE + byte_size(element(5, Chunk))
             || Chunk <- hd(Pages)
            ]),
    {StoredPageBoundaries, BoundaryRows} =
        client_encode_boundary_overflow_rows(
            Plane,
            Column,
            PageBoundaries,
            PageZeroBytes
        ),
    PageRows = [
        begin
            {FirstDocId, LastDocId, _PageChunks} =
                lists:nth(PageNo + 1, PageRanges),
            Value = client_encode_v8_plane_page(
                Plane,
                PageNo,
                PageCount,
                GlobalDocs,
                CollectionFrequency,
                TermMaxBound,
                StoredPageBoundaries,
                PageChunks
            ),
            SubKey = client_v8_page_subkey(
                Plane, Column, PageNo, FirstDocId, LastDocId
            ),
            {Plane, Column, PageNo, SubKey, Value}
        end
     || {PageNo, PageChunks} <-
            lists:zip(lists:seq(0, PageCount - 1), Pages)
    ],
    PageRows ++ BoundaryRows.

client_encode_boundary_overflow_rows(
    _Plane, _Column, PageBoundaries, PageZeroBytes
) when PageZeroBytes =< ?PAGE_MAX_BYTES ->
    {PageBoundaries, []};
client_encode_boundary_overflow_rows(
    Plane,
    Column,
    <<GlobalDocs:32/unsigned-big, Boundaries/binary>>,
    _PageZeroBytes
) ->
    MaxPartBytes = ?PAGE_MAX_BYTES - ?BOUNDARY_OVERFLOW_HEADER_BYTES,
    Parts = client_split_boundary_overflow(Boundaries, MaxPartBytes, []),
    PartCount = length(Parts),
    client_guard(boundary_parts, PartCount, ?MAX_U16),
    TotalBoundaryBytes = byte_size(Boundaries),
    Marker =
        <<GlobalDocs:32/unsigned-big, ?BOUNDARY_MARKER:16/unsigned-big,
            PartCount:16/unsigned-big, TotalBoundaryBytes:32/unsigned-big>>,
    Rows = [
        begin
            SubKey = client_boundary_overflow_subkey(
                Plane, Column, PartNo
            ),
            Value =
                <<?PAGE_MAGIC:24/unsigned-big, ?PAGE_VERSION:8,
                    ?BOUNDARY_PLANE:8, Plane:8, Column:8,
                    PartNo:16/unsigned-big, PartCount:16/unsigned-big,
                    TotalBoundaryBytes:32/unsigned-big, Part/binary>>,
            true = byte_size(Value) =< ?PAGE_MAX_BYTES,
            {?BOUNDARY_PLANE, Column, PartNo, SubKey, Value}
        end
     || {PartNo, Part} <-
            lists:zip(lists:seq(0, PartCount - 1), Parts)
    ],
    {Marker, Rows}.

client_split_boundary_overflow(<<>>, _MaxPartBytes, Acc) ->
    lists:reverse(Acc);
client_split_boundary_overflow(Bin, MaxPartBytes, Acc) ->
    PartBytes = erlang:min(byte_size(Bin), MaxPartBytes),
    <<Part:PartBytes/binary, Rest/binary>> = Bin,
    client_split_boundary_overflow(Rest, MaxPartBytes, [Part | Acc]).

client_encode_v8_chunks([], _Plane) ->
    [];
client_encode_v8_chunks(Rows, Plane) ->
    {Chunk, Rest} = client_take_v8_chunk(Rows, Plane),
    [Chunk | client_encode_v8_chunks(Rest, Plane)].

client_take_v8_chunk(
    [{FirstDocId, Value} | Rest], Plane
) ->
    {FirstValue, FirstBound} = client_encode_v8_value(Plane, Value),
    client_take_v8_chunk(
        Rest,
        Plane,
        FirstDocId,
        FirstDocId,
        1,
        FirstBound,
        byte_size(FirstValue),
        [FirstValue]
    ).

client_take_v8_chunk(
    [], _Plane, First, Last, Count, MaxBound, _Bytes, Parts
) ->
    {
        {First, Last, MaxBound, Count, iolist_to_binary(lists:reverse(Parts))},
        []
    };
client_take_v8_chunk(
    [{DocId, Value} = Row | Rest],
    Plane,
    First,
    Previous,
    Count,
    MaxBound,
    Bytes,
    Parts
) ->
    true = DocId >= Previous,
    {ValueBin, Bound} = client_encode_v8_value(Plane, Value),
    DeltaBin = varint_append(DocId - Previous, <<>>),
    Entry = <<DeltaBin/binary, ValueBin/binary>>,
    NextBytes = Bytes + byte_size(Entry),
    case Count > 0 andalso NextBytes > ?CHUNK_TARGET_BYTES of
        true ->
            {
                {First, Previous, MaxBound, Count,
                    iolist_to_binary(lists:reverse(Parts))},
                [Row | Rest]
            };
        false ->
            client_take_v8_chunk(
                Rest,
                Plane,
                First,
                DocId,
                Count + 1,
                erlang:max(MaxBound, Bound),
                NextBytes,
                [Entry | Parts]
            )
    end.

client_encode_v8_value(
    ?BOOLEAN_PLANE, {boolean, DocLength, Count}
) ->
    client_guard(doc_length, DocLength, ?MAX_U64),
    client_guard(true_occurrences, Count, ?MAX_U64),
    {
        <<
            (varint_append(DocLength, <<>>))/binary,
            (varint_append(Count, <<>>))/binary
        >>,
        client_quantized_score_bound(Count)
    };
client_encode_v8_value(?POSITION_PLANE, {position, Positions}) ->
    {
        <<
            (varint_append(byte_size(Positions), <<>>))/binary,
            Positions/binary
        >>,
        0
    }.

client_quantized_score_bound(0) ->
    0;
client_quantized_score_bound(Tf) when Tf > 0 ->
    %% BM25's k1=1.2, b=0.75 contribution is always below the value
    %% obtained with the smallest possible length normalisation (0.25).
    %% Ceiling to a 1/64 unit makes the stored byte conservative.
    Numerator = Tf * 22 * ?SCORE_BOUND_SCALE,
    Denominator = Tf * 10 + 3,
    erlang:min(255, (Numerator + Denominator - 1) div Denominator).

client_pack_v8_pages(Chunks) ->
    client_pack_v8_pages(Chunks, [], [], 0).

client_pack_v8_pages([], [], Pages, _Bytes) ->
    lists:reverse(Pages);
client_pack_v8_pages([], Current, Pages, _Bytes) ->
    lists:reverse([lists:reverse(Current) | Pages]);
client_pack_v8_pages(
    [Chunk | Rest], Current, Pages, Bytes
) ->
    ChunkBytes = ?CHUNK_DIR_STRIDE + byte_size(element(5, Chunk)),
    NextBytes = Bytes + ChunkBytes,
    case Current =/= [] andalso NextBytes > ?PAGE_TARGET_BYTES of
        true ->
            client_pack_v8_pages(
                [Chunk | Rest], [], [lists:reverse(Current) | Pages], 0
            );
        false ->
            case NextBytes =< ?PAGE_TARGET_BYTES of
                true ->
                    client_pack_v8_pages(
                        Rest, [Chunk | Current], Pages, NextBytes
                    );
                false ->
                    erlang:error(
                        {fts_page_entry_too_large, element(1, Chunk)}
                    )
            end
    end.

%% Boundary entries are prefix-delta compressed 64-bit doc ids and the page
%% number is implicit in entry order: Min is delta-encoded against the
%% previous entry's Max (the first against <<>>), Max against its own Min.
client_encode_page_boundaries(GlobalDocs, Chunks) ->
    {_Prev, Parts} = lists:foldl(
        fun({MinDocId, MaxDocId, _Chunk}, {Prev, Acc}) ->
            MinDocKey = <<MinDocId:64/unsigned-big>>,
            MaxDocKey = <<MaxDocId:64/unsigned-big>>,
            {MaxDocKey, [
                [
                    client_encode_boundary_key(Prev, MinDocKey),
                    client_encode_boundary_key(MinDocKey, MaxDocKey)
                ]
                | Acc
            ]}
        end,
        {<<>>, []},
        Chunks
    ),
    <<GlobalDocs:32/unsigned-big,
        (iolist_to_binary(lists:reverse(Parts)))/binary>>.

client_encode_boundary_key(Base, Key) ->
    Shared = binary:longest_common_prefix([Base, Key]),
    Suffix = binary:part(Key, Shared, byte_size(Key) - Shared),
    <<Shared:16/unsigned-big, (byte_size(Suffix)):16/unsigned-big,
        Suffix/binary>>.

client_encode_v8_plane_page(
    Plane,
    PageNo,
    Count,
    TotalDocs,
    CollectionFrequency0,
    TermMaxBound0,
    PageBoundaries0,
    Chunks
) ->
    {ChunkDirectory, Payload, EntryCount} =
        client_v8_chunk_directory(Chunks),
    PageCount =
        case PageNo of
            0 -> Count;
            _ -> 0
        end,
    PageTotal =
        case PageNo of
            0 -> TotalDocs;
            _ -> 0
        end,
    PageBoundaries =
        case PageNo of
            0 -> PageBoundaries0;
            _ -> <<>>
        end,
    CollectionFrequency =
        case PageNo of
            0 -> CollectionFrequency0;
            _ -> 0
        end,
    TermMaxBound =
        case PageNo of
            0 -> TermMaxBound0;
            _ -> 0
        end,
    Value =
        <<?PAGE_MAGIC:24/unsigned-big, ?PAGE_VERSION:8, Plane:8,
            PageCount:16/unsigned-big, PageTotal:32/unsigned-big,
            CollectionFrequency:64/unsigned-big, TermMaxBound:8,
            EntryCount:32/unsigned-big,
            (byte_size(ChunkDirectory)):32/unsigned-big,
            (byte_size(PageBoundaries)):32/unsigned-big, ChunkDirectory/binary,
            PageBoundaries/binary, Payload/binary>>,
    case byte_size(Value) =< ?PAGE_MAX_BYTES of
        true -> Value;
        false -> erlang:error({fts_page_too_large, PageNo, byte_size(Value)})
    end.

client_v8_chunk_directory(Chunks) ->
    {Offset, DirectoryRows, PayloadRows, EntryCount} = lists:foldl(
        fun(
            {First, Last, MaxBound, Count, Payload},
            {PayloadOffset, DirAcc, PayloadAcc, N}
        ) ->
            PayloadBytes = byte_size(Payload),
            client_guard(chunk_bytes, PayloadBytes, ?MAX_U16),
            client_guard(chunk_entries, Count, ?MAX_U16),
            Row =
                <<First:64/unsigned-big, Last:64/unsigned-big,
                    PayloadOffset:32/unsigned-big, PayloadBytes:16/unsigned-big,
                    Count:16/unsigned-big, MaxBound:8>>,
            {
                PayloadOffset + PayloadBytes,
                [Row | DirAcc],
                [Payload | PayloadAcc],
                N + Count
            }
        end,
        {0, [], [], 0},
        Chunks
    ),
    true = Offset =< ?MAX_U32,
    {
        iolist_to_binary(lists:reverse(DirectoryRows)),
        iolist_to_binary(lists:reverse(PayloadRows)),
        EntryCount
    }.

client_plane_payload_entry(_Plane, Entry) ->
    Entry.

client_docid_hash(DocKey) ->
    erlang:phash2(DocKey, 16#100000000).

client_page_total_docs(
    <<?PAGE_MAGIC:24/unsigned-big, ?PAGE_VERSION:8, _Plane:8,
        PageCount:16/unsigned-big, TotalDocs:32/unsigned-big, _Rest/binary>>
) when PageCount > 0 ->
    TotalDocs;
client_page_total_docs(
    <<?PAGE_MAGIC:24/unsigned-big, ?PREVIOUS_PAGE_VERSION:8, _Plane:8,
        PageCount:16/unsigned-big, TotalDocs:32/unsigned-big, _Rest/binary>>
) when PageCount > 0 ->
    TotalDocs;
client_page_total_docs(Bad) ->
    erlang:error({invalid_fts_page_header, Bad}).

client_decode_plane_page(
    Token,
    Column,
    Plane,
    <<?PAGE_MAGIC:24/unsigned-big, ?PAGE_VERSION:8, Plane:8,
        PageCount:16/unsigned-big, _TotalDocs:32/unsigned-big,
        _CollectionFrequency:64/unsigned-big, _TermMaxBound:8,
        _N:32/unsigned-big, ChunkDirBytes:32/unsigned-big,
        PageDirBytes:32/unsigned-big, ChunkDirectory:ChunkDirBytes/binary,
        _PageDirectory:PageDirBytes/binary, Payload/binary>>,
    Candidates,
    _Schema
) when ChunkDirBytes rem ?CHUNK_DIR_STRIDE =:= 0 ->
    {
        PageCount,
        client_decode_v8_plane_chunks(
            ChunkDirectory,
            Payload,
            Token,
            Column,
            Plane,
            Candidates,
            #{}
        )
    };
client_decode_plane_page(
    Token,
    Column,
    Plane,
    <<?PAGE_MAGIC:24/unsigned-big, ?PREVIOUS_PAGE_VERSION:8, Plane:8,
        PageCount:16/unsigned-big, _TotalDocs:32/unsigned-big,
        N:32/unsigned-big, DirBytes:32/unsigned-big,
        PageDirBytes:32/unsigned-big, Directory:DirBytes/binary,
        _PageDirectory:PageDirBytes/binary, Payload/binary>>,
    Candidates,
    _Schema
) when DirBytes =:= N * ?PAGE_DIR_STRIDE ->
    TokenContext = Token,
    Docs =
        case {Plane, Candidates} of
            {_AnyPlane, all} ->
                client_decode_directory_all(
                    N, Directory, Payload, TokenContext, Column, Plane, #{}
                );
            {_AnyPlane, _} when
                map_size(Candidates) >= 8,
                map_size(Candidates) * 4 >= N
            ->
                HashCandidates = maps:fold(
                    fun(DocKey, _True, Acc) ->
                        Hash = client_docid_hash(DocKey),
                        Acc#{
                            Hash =>
                                (maps:get(Hash, Acc, #{}))#{DocKey => true}
                        }
                    end,
                    #{},
                    Candidates
                ),
                client_decode_directory_merge(
                    0,
                    N,
                    Directory,
                    Payload,
                    TokenContext,
                    Column,
                    Plane,
                    lists:sort(maps:keys(HashCandidates)),
                    HashCandidates,
                    #{}
                );
            {_AnyPlane, _} ->
                maps:fold(
                    fun(DocKey, _True, Acc) ->
                        client_decode_directory_candidate(
                            DocKey,
                            Directory,
                            N,
                            Payload,
                            TokenContext,
                            Column,
                            Plane,
                            Acc
                        )
                    end,
                    #{},
                    Candidates
                )
        end,
    {PageCount, Docs};
client_decode_plane_page(_Token, _Column, _Plane, Bad, _Candidates, _Schema) ->
    erlang:error({invalid_fts_page, Bad}).

client_decode_v8_plane_chunks(
    <<>>, _Payload, _Token, _Column, _Plane, _Candidates, Acc
) ->
    Acc;
client_decode_v8_plane_chunks(
    <<First:64/unsigned-big, Last:64/unsigned-big, Offset:32/unsigned-big,
        Bytes:16/unsigned-big, Count:16/unsigned-big, _MaxBound:8,
        RestDirectory/binary>>,
    Payload,
    Token,
    Column,
    Plane,
    Candidates,
    Acc
) ->
    ChunkCandidates =
        case Candidates of
            all ->
                all;
            _ ->
                maps:filter(
                    fun(DocId, _Value) ->
                        DocId >= First andalso DocId =< Last
                    end,
                    Candidates
                )
        end,
    Acc1 =
        case ChunkCandidates =:= all orelse map_size(ChunkCandidates) > 0 of
            true ->
                Chunk = binary:part(Payload, Offset, Bytes),
                client_decode_v8_plane_chunk(
                    Count,
                    First,
                    First,
                    Chunk,
                    true,
                    Token,
                    Column,
                    Plane,
                    ChunkCandidates,
                    Acc
                );
            false ->
                Acc
        end,
    client_decode_v8_plane_chunks(
        RestDirectory,
        Payload,
        Token,
        Column,
        Plane,
        Candidates,
        Acc1
    ).

client_decode_v8_plane_chunk(
    0,
    _First,
    _Previous,
    _Payload,
    _Initial,
    _Token,
    _Column,
    _Plane,
    _Candidates,
    Acc
) ->
    Acc;
client_decode_v8_plane_chunk(
    Count,
    First,
    Previous,
    Payload,
    Initial,
    Token,
    Column,
    Plane,
    Candidates,
    Acc
) ->
    {DocId, ValueBin} =
        case Initial of
            true ->
                {First, Payload};
            false ->
                {Delta, Rest0} = client_decode_page_varint(Payload),
                {Previous + Delta, Rest0}
        end,
    {Entry, Rest} =
        case Plane of
            ?BOOLEAN_PLANE ->
                {DocLength, CountBin} = client_decode_page_varint(ValueBin),
                {Tf, Tail} = client_decode_page_varint(CountBin),
                {
                    {DocId, DocLength, #{
                        Column => #{
                            Token => #{
                                count => Tf, positions => [0]
                            }
                        }
                    }},
                    Tail
                };
            ?POSITION_PLANE ->
                {PositionBytes, PositionsBin} =
                    client_decode_page_varint(ValueBin),
                <<PosBin:PositionBytes/binary, Tail/binary>> = PositionsBin,
                Positions =
                    case decode_positions(PosBin, 0, []) of
                        {ok, Ps} ->
                            Ps;
                        error ->
                            erlang:error(
                                {invalid_fts_positions, PosBin}
                            )
                    end,
                {{DocId, Positions}, Tail}
        end,
    Wanted =
        Candidates =:= all orelse maps:is_key(DocId, Candidates),
    Acc1 =
        case {Wanted, Plane} of
            {false, _} ->
                Acc;
            {true, ?BOOLEAN_PLANE} ->
                client_put_state(DocId, Entry, Acc);
            {true, ?POSITION_PLANE} ->
                client_merge_position_rows(Acc, #{DocId => Entry})
        end,
    client_decode_v8_plane_chunk(
        Count - 1,
        First,
        DocId,
        Rest,
        false,
        Token,
        Column,
        Plane,
        Candidates,
        Acc1
    ).

client_decode_directory_all(
    0,
    _Directory,
    _Payload,
    _Token,
    _Column,
    _Plane,
    Acc
) ->
    Acc;
client_decode_directory_all(N, Directory, Payload, Token, Column, Plane, Acc) ->
    Index = N - 1,
    <<_Hash:32/unsigned-big, Offset:32/unsigned-big>> =
        binary:part(Directory, Index * ?PAGE_DIR_STRIDE, ?PAGE_DIR_STRIDE),
    Decoded = client_decode_page_entry(
        Payload, Offset, Token, Column, Plane
    ),
    Acc1 =
        case Decoded of
            stale ->
                Acc;
            {DocKey, Entry} ->
                client_store_page_entry(
                    Plane, DocKey, Entry, Acc
                )
        end,
    client_decode_directory_all(
        Index,
        Directory,
        Payload,
        Token,
        Column,
        Plane,
        Acc1
    ).

client_decode_directory_candidate(
    DocKey,
    Directory,
    N,
    Payload,
    Token,
    Column,
    Plane,
    Acc
) ->
    Hash = client_docid_hash(DocKey),
    Index = client_directory_lower_bound(Directory, Hash, 0, N),
    client_decode_hash_matches(
        DocKey, Hash, Index, N, Directory, Payload, Token, Column, Plane, Acc
    ).

client_decode_directory_merge(
    _Index,
    _N,
    _Directory,
    _Payload,
    _Token,
    _Column,
    _Plane,
    [],
    _HashCandidates,
    Acc
) ->
    Acc;
client_decode_directory_merge(
    Index,
    N,
    _Directory,
    _Payload,
    _Token,
    _Column,
    _Plane,
    _Hashes,
    _HashCandidates,
    Acc
) when Index >= N ->
    Acc;
client_decode_directory_merge(
    Index,
    N,
    Directory,
    Payload,
    Token,
    Column,
    Plane,
    [Hash | Rest] = Hashes,
    HashCandidates,
    Acc
) ->
    <<RowHash:32/unsigned-big, Offset:32/unsigned-big>> =
        binary:part(Directory, Index * ?PAGE_DIR_STRIDE, ?PAGE_DIR_STRIDE),
    if
        RowHash < Hash ->
            client_decode_directory_merge(
                Index + 1,
                N,
                Directory,
                Payload,
                Token,
                Column,
                Plane,
                Hashes,
                HashCandidates,
                Acc
            );
        RowHash > Hash ->
            client_decode_directory_merge(
                Index,
                N,
                Directory,
                Payload,
                Token,
                Column,
                Plane,
                Rest,
                HashCandidates,
                Acc
            );
        true ->
            Acc1 =
                case
                    client_decode_page_entry(
                        Payload, Offset, Token, Column, Plane
                    )
                of
                    stale ->
                        Acc;
                    {StoredKey, Entry} ->
                        Wanted = maps:get(Hash, HashCandidates),
                        case
                            client_resolve_page_entry_key(
                                Plane, StoredKey, Wanted
                            )
                        of
                            {ok, DocKey} ->
                                client_store_page_entry(
                                    Plane, DocKey, Entry, Acc
                                );
                            error ->
                                Acc
                        end
                end,
            client_decode_directory_merge(
                Index + 1,
                N,
                Directory,
                Payload,
                Token,
                Column,
                Plane,
                Hashes,
                HashCandidates,
                Acc1
            )
    end.

client_directory_lower_bound(_Directory, _Hash, Lo, Lo) ->
    Lo;
client_directory_lower_bound(Directory, Hash, Lo, Hi) ->
    Mid = (Lo + Hi) div 2,
    DirectoryOffset = Mid * ?PAGE_DIR_STRIDE,
    <<_:DirectoryOffset/binary, MidHash:32/unsigned-big,
        _Offset:32/unsigned-big, _/binary>> = Directory,
    case MidHash < Hash of
        true -> client_directory_lower_bound(Directory, Hash, Mid + 1, Hi);
        false -> client_directory_lower_bound(Directory, Hash, Lo, Mid)
    end.

client_decode_hash_matches(
    _DocKey,
    _Hash,
    Index,
    N,
    _Directory,
    _Payload,
    _Token,
    _Column,
    _Plane,
    Acc
) when Index >= N ->
    Acc;
client_decode_hash_matches(
    DocKey,
    Hash,
    Index,
    N,
    Directory,
    Payload,
    Token,
    Column,
    Plane,
    Acc
) ->
    <<RowHash:32/unsigned-big, Offset:32/unsigned-big>> =
        binary:part(Directory, Index * ?PAGE_DIR_STRIDE, ?PAGE_DIR_STRIDE),
    case RowHash =:= Hash of
        false ->
            Acc;
        true ->
            Acc1 =
                case
                    client_decode_page_entry(
                        Payload, Offset, Token, Column, Plane
                    )
                of
                    stale ->
                        Acc;
                    {StoredKey, Entry} ->
                        case
                            client_page_entry_matches(
                                Plane, StoredKey, DocKey
                            )
                        of
                            true ->
                                client_store_page_entry(
                                    Plane, DocKey, Entry, Acc
                                );
                            false ->
                                Acc
                        end
                end,
            client_decode_hash_matches(
                DocKey,
                Hash,
                Index + 1,
                N,
                Directory,
                Payload,
                Token,
                Column,
                Plane,
                Acc1
            )
    end.

client_resolve_page_entry_key(_Plane, StoredKey, Wanted) ->
    case maps:is_key(StoredKey, Wanted) of
        true -> {ok, StoredKey};
        false -> error
    end.

client_page_entry_matches(_Plane, StoredKey, DocKey) ->
    StoredKey =:= DocKey.

client_decode_page_entry(
    Payload,
    Offset,
    Token,
    Column,
    ?BOOLEAN_PLANE
) ->
    Body = binary:part(Payload, Offset, byte_size(Payload) - Offset),
    {DocId, LengthBin} = client_decode_page_varint(Body),
    {DocLength, CountBin} = client_decode_page_varint(LengthBin),
    {Count, _Rest} = client_decode_page_varint(CountBin),
    {DocId,
        {DocId, DocLength, #{
            Column => #{
                Token => #{
                    count => Count, positions => [0]
                }
            }
        }}};
client_decode_page_entry(
    Payload,
    Offset,
    _Token,
    _Column,
    ?POSITION_PLANE
) ->
    Body = binary:part(Payload, Offset, byte_size(Payload) - Offset),
    {DocId, Rest} = client_decode_page_varint(Body),
    {PosBytes, PositionBin} = client_decode_page_varint(Rest),
    <<PosBin:PosBytes/binary, _/binary>> = PositionBin,
    Positions =
        case decode_positions(PosBin, 0, []) of
            {ok, Ps} -> Ps;
            error -> erlang:error({invalid_fts_positions, PosBin})
        end,
    {DocId, {DocId, Positions}}.

client_decode_page_varint(Bin) ->
    case decode_varint(Bin) of
        {ok, Value, Rest} -> {Value, Rest};
        error -> erlang:error({invalid_fts_page_varint, Bin})
    end.

client_store_page_entry(?BOOLEAN_PLANE, DocKey, Entry, Acc) ->
    client_put_state(DocKey, Entry, Acc);
client_store_page_entry(?POSITION_PLANE, DocKey, VersionPositions, Acc) ->
    client_merge_position_rows(Acc, #{DocKey => VersionPositions}).

-spec consolidate(pid(), map(), map() | list()) ->
    {ok, map()} | {error, term()}.
consolidate(Bookie, #{fingerprint := _} = Schema, Opts0) when is_pid(Bookie) ->
    try
        {Hook, Opts} = client_take_option(before_consolidate_commit, Opts0),
        Shards =
            case client_option(shards, Opts, all) of
                all -> lists:seq(0, maps:get(shards, Schema) - 1);
                L when is_list(L) -> L
            end,
        Result0 = lists:foldl(
            fun(Shard, Acc) ->
                client_consolidate_shard(Bookie, Schema, Shard, Hook, Acc)
            end,
            #{consolidated => [], skipped => []},
            Shards
        ),
        client_maybe_refresh_stats(Bookie, Schema, Shards, Result0),
        {ok, maps:map(fun(_K, V) -> lists:reverse(V) end, Result0)}
    catch
        error:Reason -> {error, Reason}
    end.

client_option(Key, Opts, Default) when is_map(Opts) ->
    maps:get(Key, Opts, Default);
client_option(Key, Opts, Default) when is_list(Opts) ->
    proplists:get_value(Key, Opts, Default).

client_consolidate_shard(Bookie, Schema, Shard, Hook, Acc) ->
    case client_epoch_sqn(Bookie, Schema, Shard) of
        not_found ->
            Acc;
        {ok, ObservedSQN} ->
            {Tail0, DocRows} = client_fold_shard_tail(Bookie, Schema, Shard),
            case DocRows of
                [] ->
                    Acc;
                _ ->
                    %% The shard tail is the authoritative latest row for
                    %% every document that touched this shard. Any concurrent
                    %% update also advances this shard's epoch, so the CAS
                    %% below rejects a page rebuild from a stale tail without
                    %% per-document manifest reads or id reassignment.
                    Existing = client_fold_shard_pages(
                        Bookie, Schema, Shard
                    ),
                    Inverted = client_invert_tail(Tail0),
                    client_call_hook(Hook, {Shard, ObservedSQN}),
                    Bucket = maps:get(index, Schema),
                    ShardKey = client_shard_key(Shard),
                    PageSpecs = client_merge_page_specs(
                        Bucket,
                        Existing,
                        Inverted,
                        client_tail_doc_ids(Tail0)
                    ),
                    Specs =
                        PageSpecs ++
                            [
                                {remove, Bucket, ShardKey, SubKey, <<>>}
                             || SubKey <- DocRows
                            ] ++
                            [
                                {add, Bucket, ShardKey, <<"tailsum">>,
                                    client_encode_tailsum(empty)},
                                client_epoch_spec(Bucket, Shard)
                            ],
                    Condition = {
                        Bucket, ShardKey, <<"epoch">>, {sqn, ObservedSQN}
                    },
                    case
                        leveled_bookie:book_casmput(
                            Bookie, Specs, [Condition]
                        )
                    of
                        ok ->
                            Acc#{
                                consolidated := [
                                    Shard
                                    | maps:get(consolidated, Acc)
                                ]
                            };
                        pause ->
                            Acc#{
                                consolidated := [
                                    Shard
                                    | maps:get(consolidated, Acc)
                                ]
                            };
                        {error, {precondition_failed, _}} ->
                            Acc#{
                                skipped := [
                                    Shard
                                    | maps:get(skipped, Acc)
                                ]
                            };
                        {error, Reason} ->
                            erlang:error({fts_consolidation_failed, Reason})
                    end
            end
    end.

client_fold_shard_pages(Bookie, Schema, Shard) ->
    Bucket = maps:get(index, Schema),
    {LoToken, HiToken} = client_shard_token_bounds(
        Shard, maps:get(shards, Schema)
    ),
    Start = client_token_key(LoToken),
    Finish = client_token_key(HiToken),
    Fold = fun
        (B, {<<"t:", Token/binary>>, SubKey}, Value, Acc) when
            B =:= Bucket
        ->
            case client_decode_page_row(SubKey, Value) of
                {page, Plane, Column, _PageNo} ->
                    {OldDocs, OldKeys, OldPositions} = maps:get(
                        Token, Acc, {#{}, [], #{}}
                    ),
                    {_Count, Decoded} = client_decode_plane_page(
                        Token, Column, Plane, Value, all, Schema
                    ),
                    {Docs, Positions} =
                        case Plane of
                            ?BOOLEAN_PLANE ->
                                {
                                    client_merge_state(OldDocs, Decoded),
                                    OldPositions
                                };
                            ?POSITION_PLANE ->
                                {
                                    OldDocs,
                                    OldPositions#{
                                        Column =>
                                            client_merge_position_rows(
                                                maps:get(
                                                    Column, OldPositions, #{}
                                                ),
                                                Decoded
                                            )
                                    }
                                }
                        end,
                    Acc#{
                        Token =>
                            {Docs, [SubKey | OldKeys], Positions}
                    };
                not_page ->
                    case client_decode_boundary_overflow_row(SubKey, Value) of
                        boundary_overflow ->
                            {OldDocs, OldKeys, OldPositions} = maps:get(
                                Token, Acc, {#{}, [], #{}}
                            ),
                            Acc#{
                                Token =>
                                    {OldDocs, [SubKey | OldKeys], OldPositions}
                            };
                        not_boundary_overflow ->
                            Acc
                    end
            end;
        (_B, _K, _V, Acc) ->
            Acc
    end,
    {async, Runner} = leveled_bookie:book_headfold(
        Bookie,
        ?HEAD_TAG,
        {range, Bucket, {{Start, <<>>}, {Finish, <<255>>}}},
        {Fold, #{}},
        false,
        true,
        false
    ),
    maps:map(
        fun(Token, {Docs, Keys, PositionsByColumn}) ->
            Hydrated = maps:fold(
                fun(Column, PositionsByDoc, Acc) ->
                    client_hydrate_state_positions(
                        Acc, Token, Column, PositionsByDoc
                    )
                end,
                Docs,
                PositionsByColumn
            ),
            {Hydrated, Keys}
        end,
        Runner()
    ).

client_shard_token_bounds(Shard, Shards) ->
    Lo = (Shard * 65536) div Shards,
    Hi = (((Shard + 1) * 65536) div Shards) - 1,
    {client_raw_token_lower(Lo), client_raw_token_upper(Hi)}.

client_raw_token_lower(Raw) ->
    B1 = Raw bsr 8,
    B2 = Raw band 255,
    case B2 of
        0 -> <<B1>>;
        _ -> <<B1, B2>>
    end.

client_raw_token_upper(Raw) ->
    B1 = Raw bsr 8,
    B2 = Raw band 255,
    <<B1, B2, 255>>.

client_invert_tail(Tail) ->
    maps:fold(
        fun
            (
                _DocKey,
                {_V, _DocId, _RetiredIds, _L, _Base, remove, _Posting},
                Acc
            ) ->
                Acc;
            (_DocKey, {_V, DocId, _RetiredIds, L, _Base, live, Posting}, Acc) ->
                maps:fold(
                    fun(Column, Tokens, A0) ->
                        maps:fold(
                            fun(Token, Entry, A1) ->
                                Docs = maps:get(Token, A1, #{}),
                                TokenPosting = #{Column => #{Token => Entry}},
                                A1#{
                                    Token => client_put_state(
                                        DocId, {DocId, L, TokenPosting}, Docs
                                    )
                                }
                            end,
                            A0,
                            Tokens
                        )
                    end,
                    Acc,
                    Posting
                )
        end,
        #{},
        Tail
    ).

client_merge_page_specs(Bucket, Existing, Inverted, TailDocIds) ->
    Tokens = lists:usort(maps:keys(Existing) ++ maps:keys(Inverted)),
    lists:append([
        client_token_page_specs(
            Bucket,
            Token,
            maps:get(Token, Existing, {#{}, []}),
            maps:get(Token, Inverted, #{}),
            TailDocIds
        )
     || Token <- Tokens
    ]).

client_token_page_specs(
    Bucket,
    Token,
    {OldDocs, OldSubKeys},
    Added,
    TailDocIds
) ->
    Affected =
        map_size(Added) > 0 orelse
            lists:any(
                fun(DocId) -> maps:is_key(DocId, OldDocs) end, TailDocIds
            ),
    case Affected of
        false ->
            [];
        true ->
            Docs = client_merge_state(
                maps:without(TailDocIds, OldDocs), Added
            ),
            NewRows =
                case map_size(Docs) of
                    0 -> [];
                    _ -> client_encode_token_pages(Docs)
                end,
            NewSubKeys = [
                SubKey
             || {_Plane, _Column, _PageNo, SubKey, _Value} <- NewRows
            ],
            Adds = [
                {add, Bucket, client_token_key(Token), SubKey, Value}
             || {_Plane, _Column, _PageNo, SubKey, Value} <- NewRows
            ],
            Removes = [
                {remove, Bucket, client_token_key(Token), SubKey, <<>>}
             || SubKey <- OldSubKeys,
                not lists:member(SubKey, NewSubKeys)
            ],
            Adds ++ Removes
    end.

client_maybe_refresh_stats(
    Bookie,
    Schema,
    Shards,
    #{skipped := []}
) ->
    AllShards = lists:seq(0, maps:get(shards, Schema) - 1),
    case lists:sort(Shards) =:= AllShards of
        false -> ok;
        true -> client_refresh_stats(Bookie, Schema, AllShards)
    end;
client_maybe_refresh_stats(_Bookie, _Schema, _Shards, _Result) ->
    ok.

client_refresh_stats(Bookie, Schema, Shards) ->
    Bucket = maps:get(index, Schema),
    Conditions = [
        client_epoch_condition(Bookie, Bucket, Shard)
     || Shard <- Shards
    ],
    Manifests = client_fold_manifest_rows(Bookie, Schema),
    {N, L} = maps:fold(
        fun(_DocKey, Manifest, {N0, L0}) ->
            {N0 + 1, L0 + maps:get(doc_length, Manifest)}
        end,
        {0, 0},
        Manifests
    ),
    ManifestSpecs = [
        {add, Bucket, <<"doc">>, DocKey,
            client_encode_manifest(
                maps:get(version, Manifest),
                maps:get(doc_id, Manifest),
                maps:get(shards, Manifest),
                maps:get(doc_length, Manifest),
                maps:get(doc_length, Manifest),
                maps:get(fingerprint, Manifest)
            )}
     || {DocKey, Manifest} <- maps:to_list(Manifests)
    ],
    StatsTail = client_fold_stats_tail(Bookie, Schema),
    StatsSpecs =
        [
            {add, Bucket, <<"stats">>, <<"summary">>,
                client_encode_stats_summary(N, L)},
            {remove, Bucket, <<"stats">>, <<"dirty">>, <<>>}
        ] ++
            [
                {remove, Bucket, <<"stats">>, client_doc_subkey(DocKey), <<>>}
             || DocKey <- maps:keys(StatsTail)
            ],
    case
        leveled_bookie:book_casmput(
            Bookie, ManifestSpecs ++ StatsSpecs, Conditions
        )
    of
        ok ->
            ok;
        pause ->
            ok;
        {error, {precondition_failed, _}} ->
            ok;
        {error, Reason} ->
            erlang:error({fts_stats_consolidation_failed, Reason})
    end.

client_epoch_condition(Bookie, Bucket, Shard) ->
    Key = client_shard_key(Shard),
    case
        leveled_bookie:book_sqn(
            Bookie, Bucket, {Key, <<"epoch">>}, ?HEAD_TAG
        )
    of
        not_found -> {Bucket, Key, <<"epoch">>, absent};
        {ok, SQN} -> {Bucket, Key, <<"epoch">>, {sqn, SQN}}
    end.

client_call_hook(undefined, _Arg) -> ok;
client_call_hook(Fun, Arg) when is_function(Fun, 1) -> Fun(Arg);
client_call_hook(Fun, _Arg) when is_function(Fun, 0) -> Fun().

%% Persisted page headers carry a 32-bit document count.

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
normalise_column_spec(#{name := Name, path := Path} = Spec) when
    is_list(Path)
->
    {
        normalise_column(Name),
        Path,
        normalise_column_mode(maps:get(mode, Spec, text))
    };
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
extract_path(Tuple, [N | Rest]) when
    is_tuple(Tuple), is_integer(N), N > 0, N =< tuple_size(Tuple)
->
    extract_path(element(N, Tuple), Rest);
extract_path(List, [N | Rest]) when
    is_list(List), is_integer(N), N > 0, N =< length(List)
->
    extract_path(lists:nth(N, List), Rest);
extract_path(_Value, _Path) ->
    <<>>.

alternate_map_key(Key) when is_atom(Key) ->
    atom_to_binary(Key, utf8);
alternate_map_key(Key) when is_binary(Key) ->
    try
        binary_to_existing_atom(Key, utf8)
    catch
        _:_ -> Key
    end;
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
        true ->
            ok;
        false ->
            {error, {invalid_fts_contract_change, columns, Existing, Columns}}
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

validate_schema_prefixes(
    #{prefixes := Existing}, #{prefixes := Prefixes0}, write
) ->
    Prefixes = normalise_prefixes(Prefixes0),
    case Prefixes =:= [] orelse Prefixes =:= Existing of
        true ->
            ok;
        false ->
            {error, {invalid_fts_contract_change, prefixes, Existing, Prefixes}}
    end;
validate_schema_prefixes(_Schema, _Opts, write) ->
    ok;
validate_schema_prefixes(
    #{prefixes := Existing}, #{prefixes := Prefixes0}, search
) ->
    Prefixes = normalise_prefixes(Prefixes0),
    case
        Prefixes =:= [] orelse
            lists:all(fun(P) -> lists:member(P, Existing) end, Prefixes)
    of
        true ->
            ok;
        false ->
            {error, {invalid_fts_contract_change, prefixes, Existing, Prefixes}}
    end;
validate_schema_prefixes(_Schema, _Opts, search) ->
    ok.

validate_schema_tokenizer(#{tokenizer := Existing}, Opts) ->
    Tokenizer = tokenizer_description(Opts),
    case Tokenizer =:= Existing of
        true ->
            ok;
        false ->
            {error,
                {invalid_fts_contract_change, tokenizer, Existing, Tokenizer}}
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
    [
        {Token, lists:reverse(Positions)}
     || {Token, Positions} <- maps:to_list(Acc)
    ];
group_positions([{Token, Pos} | Rest], Acc) ->
    group_positions(
        Rest, maps:update_with(Token, fun(Ps) -> [Pos | Ps] end, [Pos], Acc)
    ).

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
    lists:all(
        fun(ChosenSpan) -> span_distance(Span, ChosenSpan) =< Distance end,
        Chosen
    ).

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

drop(0, List) ->
    List;
drop(_N, []) ->
    [];
drop(N, [_ | Rest]) when N > 0 ->
    drop(N - 1, Rest).

normalise_search_options(Opts0, Schema) ->
    case options_map(Opts0) of
        {ok, Opts0Map} ->
            SearchOpts0 = maps:merge(
                schema_tokenizer_options(Schema), Opts0Map
            ),
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
                                columns => maps:get(
                                    columns, Opts1, maps:get(columns, Schema)
                                ),
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
    try
        {ok, maps:from_list(Opts)}
    catch
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
    case valid_columns(Columns) of
        true -> validate_search_option_list(Rest);
        false -> error
    end;
validate_search_option_list([{prefixes, Prefixes} | Rest]) ->
    case valid_prefixes(Prefixes) of
        true -> validate_search_option_list(Rest);
        false -> error
    end;
validate_search_option_list([{tokenizer, Tokenizer} | Rest]) ->
    case valid_tokenizer(Tokenizer) of
        true -> validate_search_option_list(Rest);
        false -> error
    end;
validate_search_option_list([{remove_diacritics, Value} | Rest]) ->
    case valid_remove_diacritics(Value) of
        true -> validate_search_option_list(Rest);
        false -> error
    end;
validate_search_option_list([{tokenchars, Value} | Rest]) ->
    case valid_char_option(Value) of
        true -> validate_search_option_list(Rest);
        false -> error
    end;
validate_search_option_list([{separators, Value} | Rest]) ->
    case valid_char_option(Value) of
        true -> validate_search_option_list(Rest);
        false -> error
    end;
validate_search_option_list([{stopwords, Words} | Rest]) when is_list(Words) ->
    validate_search_option_list(Rest);
validate_search_option_list([{rank, none} | Rest]) ->
    validate_search_option_list(Rest);
validate_search_option_list([{rank, bm25} | Rest]) ->
    validate_search_option_list(Rest);
validate_search_option_list([{rank, _Other} | _Rest]) ->
    {error, invalid_rank_option};
validate_search_option_list([{limit, Limit} | Rest]) when
    is_integer(Limit), Limit >= 0
->
    validate_search_option_list(Rest);
validate_search_option_list([{offset, Offset} | Rest]) when
    is_integer(Offset), Offset >= 0
->
    validate_search_option_list(Rest);
%% return_positions does not cap match_count.  It returns at most
%% MAX_RETURN_POSITIONS ordinals per hit as deterministic ordered prefixes;
%% phrase and NEAR ordinals are their match starts.
validate_search_option_list([{return_positions, Bool} | Rest]) when
    is_boolean(Bool)
->
    validate_search_option_list(Rest);
validate_search_option_list([{return_terms, Bool} | Rest]) when
    is_boolean(Bool)
->
    validate_search_option_list(Rest);
validate_search_option_list([{return_count, Bool} | Rest]) when
    is_boolean(Bool)
->
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
    Opts1 = Opts0#{
        stopwords => normalise_stopwords(maps:get(stopwords, Opts, []), Opts0)
    },
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
valid_char_option(_Other) ->
    false.

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
    [
        Token
     || Word <- Words, {Token, _Pos} <- tokenize(Word, Opts#{stopwords => []})
    ].

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

%% Return the index tokenizer's normalized token stream together with source
%% coordinates.  Coordinates address Text0 itself, before the SQLite UTF-8
%% compatibility rewrite and before token normalization.  In particular, the
%% F0 9F 92 truncation quirk maps the synthesized two-byte U+07D2 character
%% back to all three original input bytes.
-spec tokenize_with_offsets(binary(), map()) ->
    [{binary(), non_neg_integer(), non_neg_integer(), non_neg_integer()}].
tokenize_with_offsets(Text, Opts) when is_binary(Text), is_map(Opts) ->
    Stopwords = maps:get(stopwords, Opts, []),
    Tokens =
        case
            maps:get(tokenchars, Opts, []) =:= [] andalso
                maps:get(separators, Opts, []) =:= []
        of
            true ->
                fast_tokens_with_offsets(
                    Text, 0, Opts, Stopwords, <<>>, false, undefined, 0, []
                );
            false ->
                unicode_tokens_with_offsets(
                    Text, 0, Opts, Stopwords, <<>>, undefined, 0, []
                )
        end,
    [compat_source_range(Text, Token) || Token <- Tokens].

%% sqlite_utf8_compat/1 recognizes the malformed three-byte prefix only by
%% looking at the following non-continuation byte.  If a token ends at that
%% synthesized character, include the one-byte lookahead in its source range
%% so tokenizing binary:part(Input, Offset, Length) reproduces the same token.
%% When the token continues past the quirk its ordinary range already includes
%% the lookahead and no extension is needed.
compat_source_range(
    Text, {Token, Pos, Offset, Length} = WithOffset
) when Offset + Length < byte_size(Text), Length >= 3 ->
    End = Offset + Length,
    case binary:part(Text, End - 3, 3) of
        <<16#F0, 16#9F, 16#92>> ->
            {Token, Pos, Offset, Length + 1};
        _Other ->
            WithOffset
    end;
compat_source_range(_Text, WithOffset) ->
    WithOffset.

tokenize(Text0, Opts) ->
    Text = sqlite_utf8_compat(normalise_text(Text0)),
    case
        maps:get(tokenchars, Opts, []) =:= [] andalso
            maps:get(separators, Opts, []) =:= []
    of
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

sqlite_utf8_compat(<<16#F0, 16#9F, 16#92, Next, Rest/binary>>, Acc) when
    Next band 16#C0 =/= 16#80
->
    sqlite_utf8_compat(<<Next, Rest/binary>>, <<Acc/binary, 16#DF, 16#92>>);
sqlite_utf8_compat(<<Byte, Rest/binary>>, Acc) ->
    sqlite_utf8_compat(Rest, <<Acc/binary, Byte>>);
sqlite_utf8_compat(<<>>, Acc) ->
    Acc.

%% Offset-aware common path.  Token bytes follow sqlite_utf8_compat/1 while
%% Start/Offset remain coordinates in the original input binary.
fast_tokens_with_offsets(
    <<C, Rest/binary>>, Offset, Opts, SW, Tok, NA, Start, Pos, Acc
) when C >= $a, C =< $z ->
    fast_tokens_with_offsets(
        Rest,
        Offset + 1,
        Opts,
        SW,
        <<Tok/binary, C>>,
        NA,
        token_start(Start, Offset),
        Pos,
        Acc
    );
fast_tokens_with_offsets(
    <<C, Rest/binary>>, Offset, Opts, SW, Tok, NA, Start, Pos, Acc
) when C >= $0, C =< $9 ->
    fast_tokens_with_offsets(
        Rest,
        Offset + 1,
        Opts,
        SW,
        <<Tok/binary, C>>,
        NA,
        token_start(Start, Offset),
        Pos,
        Acc
    );
fast_tokens_with_offsets(
    <<C, Rest/binary>>, Offset, Opts, SW, Tok, NA, Start, Pos, Acc
) when C >= $A, C =< $Z ->
    fast_tokens_with_offsets(
        Rest,
        Offset + 1,
        Opts,
        SW,
        <<Tok/binary, (C bor 16#20)>>,
        NA,
        token_start(Start, Offset),
        Pos,
        Acc
    );
fast_tokens_with_offsets(
    <<C, Rest/binary>>, Offset, Opts, SW, Tok, NA, Start, Pos, Acc
) when C < 128 ->
    {Pos1, Acc1} = fast_offset_flush(
        Tok, NA, Start, Offset, Opts, SW, Pos, Acc
    ),
    fast_tokens_with_offsets(
        Rest, Offset + 1, Opts, SW, <<>>, false, undefined, Pos1, Acc1
    );
fast_tokens_with_offsets(
    <<16#F0, 16#9F, 16#92, Next, Rest/binary>>,
    Offset,
    Opts,
    SW,
    Tok,
    _NA,
    Start,
    Pos,
    Acc
) when Next band 16#C0 =/= 16#80 ->
    fast_tokens_with_offsets(
        <<Next, Rest/binary>>,
        Offset + 3,
        Opts,
        SW,
        <<Tok/binary, 16#DF, 16#92>>,
        true,
        token_start(Start, Offset),
        Pos,
        Acc
    );
fast_tokens_with_offsets(
    <<CP/utf8, Rest/binary>> = Bin,
    Offset,
    Opts,
    SW,
    Tok,
    NA,
    Start,
    Pos,
    Acc
) ->
    CharLen = byte_size(Bin) - byte_size(Rest),
    case unicode_token_char(CP, Opts) of
        true ->
            <<Char:CharLen/binary, _/binary>> = Bin,
            fast_tokens_with_offsets(
                Rest,
                Offset + CharLen,
                Opts,
                SW,
                <<Tok/binary, Char/binary>>,
                true,
                token_start(Start, Offset),
                Pos,
                Acc
            );
        false ->
            {Pos1, Acc1} = fast_offset_flush(
                Tok, NA, Start, Offset, Opts, SW, Pos, Acc
            ),
            fast_tokens_with_offsets(
                Rest,
                Offset + CharLen,
                Opts,
                SW,
                <<>>,
                false,
                undefined,
                Pos1,
                Acc1
            )
    end;
fast_tokens_with_offsets(
    <<_Bad, Rest/binary>>, Offset, Opts, SW, Tok, NA, Start, Pos, Acc
) ->
    {Pos1, Acc1} = fast_offset_flush(
        Tok, NA, Start, Offset, Opts, SW, Pos, Acc
    ),
    fast_tokens_with_offsets(
        Rest, Offset + 1, Opts, SW, <<>>, false, undefined, Pos1, Acc1
    );
fast_tokens_with_offsets(
    <<>>, Offset, Opts, SW, Tok, NA, Start, Pos, Acc
) ->
    {_Pos1, Acc1} = fast_offset_flush(
        Tok, NA, Start, Offset, Opts, SW, Pos, Acc
    ),
    lists:reverse(Acc1).

fast_offset_flush(<<>>, _NA, _Start, _End, _Opts, _SW, Pos, Acc) ->
    {Pos, Acc};
fast_offset_flush(Tok, false, Start, End, _Opts, SW, Pos, Acc) ->
    case lists:member(Tok, SW) of
        true -> {Pos + 1, Acc};
        false -> {Pos + 1, [{Tok, Pos, Start, End - Start} | Acc]}
    end;
fast_offset_flush(Tok, true, Start, End, Opts, SW, Pos, Acc) ->
    Norm = normalise_token(Tok, Opts),
    case {Norm =:= <<>>, lists:member(Norm, SW)} of
        {true, _} -> {Pos, Acc};
        {false, true} -> {Pos + 1, Acc};
        {false, false} -> {Pos + 1, [{Norm, Pos, Start, End - Start} | Acc]}
    end.

%% Custom tokenchars/separators use the same classifier and normalization as
%% tokenize_unicode/2, with the original byte range carried beside each token.
unicode_tokens_with_offsets(
    <<16#F0, 16#9F, 16#92, Next, Rest/binary>>,
    Offset,
    Opts,
    SW,
    Tok,
    Start,
    Pos,
    Acc
) when Next band 16#C0 =/= 16#80 ->
    unicode_offset_char(
        16#7D2,
        <<16#DF, 16#92>>,
        <<Next, Rest/binary>>,
        Offset,
        Offset + 3,
        Opts,
        SW,
        Tok,
        Start,
        Pos,
        Acc
    );
unicode_tokens_with_offsets(
    <<CP/utf8, Rest/binary>> = Bin,
    Offset,
    Opts,
    SW,
    Tok,
    Start,
    Pos,
    Acc
) ->
    CharLen = byte_size(Bin) - byte_size(Rest),
    <<Char:CharLen/binary, _/binary>> = Bin,
    unicode_offset_char(
        CP,
        Char,
        Rest,
        Offset,
        Offset + CharLen,
        Opts,
        SW,
        Tok,
        Start,
        Pos,
        Acc
    );
unicode_tokens_with_offsets(
    <<_Bad, Rest/binary>>, Offset, Opts, SW, Tok, Start, Pos, Acc
) ->
    {Pos1, Acc1} = unicode_offset_flush(
        Tok, Start, Offset, Opts, SW, Pos, Acc
    ),
    unicode_tokens_with_offsets(
        Rest, Offset + 1, Opts, SW, <<>>, undefined, Pos1, Acc1
    );
unicode_tokens_with_offsets(
    <<>>, Offset, Opts, SW, Tok, Start, Pos, Acc
) ->
    {_Pos1, Acc1} = unicode_offset_flush(
        Tok, Start, Offset, Opts, SW, Pos, Acc
    ),
    lists:reverse(Acc1).

unicode_offset_char(
    CP,
    Char,
    Rest,
    Offset,
    NextOffset,
    Opts,
    SW,
    Tok,
    Start,
    Pos,
    Acc
) ->
    case token_char(CP, Opts) of
        true ->
            unicode_tokens_with_offsets(
                Rest,
                NextOffset,
                Opts,
                SW,
                <<Tok/binary, Char/binary>>,
                token_start(Start, Offset),
                Pos,
                Acc
            );
        false ->
            {Pos1, Acc1} = unicode_offset_flush(
                Tok, Start, Offset, Opts, SW, Pos, Acc
            ),
            unicode_tokens_with_offsets(
                Rest, NextOffset, Opts, SW, <<>>, undefined, Pos1, Acc1
            )
    end.

unicode_offset_flush(<<>>, _Start, _End, _Opts, _SW, Pos, Acc) ->
    {Pos, Acc};
unicode_offset_flush(Tok, Start, End, Opts, SW, Pos, Acc) ->
    Norm = normalise_token(Tok, Opts),
    case {Norm =:= <<>>, lists:member(Norm, SW)} of
        {true, _} -> {Pos, Acc};
        {false, true} -> {Pos + 1, Acc};
        {false, false} -> {Pos + 1, [{Norm, Pos, Start, End - Start} | Acc]}
    end.

token_start(undefined, Offset) -> Offset;
token_start(Start, _Offset) -> Start.

tokenize_unicode(Text0, Opts) ->
    %% Keep malformed input explicit.  Dropping a bad byte before this fold
    %% concatenates the valid runs on either side and changes token identity.
    Text = unicode_chars_with_boundaries(normalise_text(Text0)),
    Stopwords = maps:get(stopwords, Opts, []),
    {Tokens, Current, Pos} =
        lists:foldl(
            fun
                (invalid_utf8, {Acc, Current, Pos}) ->
                    finish_token(Acc, Current, Pos, Stopwords, Opts);
                (Char, {Acc, Current, Pos}) ->
                    case token_char(Char, Opts) of
                        true ->
                            {Acc,
                                lists:reverse(normalise_char(Char, Opts)) ++
                                    Current,
                                Pos};
                        false ->
                            finish_token(Acc, Current, Pos, Stopwords, Opts)
                    end
            end,
            {[], [], 0},
            Text
        ),
    {Final, _Current2, _Pos2} = finish_token(
        Tokens, Current, Pos, Stopwords, Opts
    ),
    lists:reverse(Final).

%% Fast tokenizer for the common case where no custom tokenchars/separators are
%% configured. Runs of ASCII alphanumerics are classified and lowercased with
%% byte comparisons only (no per-character Unicode table lookups). Non-ASCII
%% characters fall back to the Unicode classifier, and any token that contained
%% a non-ASCII byte is normalised through normalise_token/2, so output is
%% byte-identical to tokenize_unicode/2 (unicode61 + remove_diacritics parity
%% with SQLite FTS5).
fast_tokens(<<C, Rest/binary>>, Opts, SW, Tok, NA, Pos, Acc) when
    C >= $a, C =< $z
->
    fast_tokens(Rest, Opts, SW, <<Tok/binary, C>>, NA, Pos, Acc);
fast_tokens(<<C, Rest/binary>>, Opts, SW, Tok, NA, Pos, Acc) when
    C >= $0, C =< $9
->
    fast_tokens(Rest, Opts, SW, <<Tok/binary, C>>, NA, Pos, Acc);
fast_tokens(<<C, Rest/binary>>, Opts, SW, Tok, NA, Pos, Acc) when
    C >= $A, C =< $Z
->
    fast_tokens(Rest, Opts, SW, <<Tok/binary, (C bor 16#20)>>, NA, Pos, Acc);
fast_tokens(<<C, Rest/binary>>, Opts, SW, Tok, NA, Pos, Acc) when C < 128 ->
    {Pos1, Acc1} = fast_flush(Tok, NA, Opts, SW, Pos, Acc),
    fast_tokens(Rest, Opts, SW, <<>>, false, Pos1, Acc1);
fast_tokens(<<CP/utf8, Rest/binary>> = Bin, Opts, SW, Tok, NA, Pos, Acc) ->
    case unicode_token_char(CP, Opts) of
        true ->
            CharLen = byte_size(Bin) - byte_size(Rest),
            <<Char:CharLen/binary, _/binary>> = Bin,
            fast_tokens(
                Rest, Opts, SW, <<Tok/binary, Char/binary>>, true, Pos, Acc
            );
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
    Token = normalise_token(
        unicode:characters_to_binary(lists:reverse(Current), utf8), Opts
    ),
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
        true ->
            true;
        false ->
            remove_diacritics_enabled(Opts) andalso sqlite_diacritic_mark(Char)
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
        48,
        57,
        65,
        90,
        97,
        122,
        170,
        170,
        178,
        179,
        181,
        181,
        185,
        186,
        188,
        190,
        192,
        214,
        216,
        246,
        248,
        705,
        710,
        721,
        736,
        740,
        748,
        748,
        750,
        750,
        880,
        884,
        886,
        893,
        895,
        899,
        902,
        902,
        904,
        1013,
        1015,
        1153,
        1162,
        1369,
        1376,
        1416,
        1419,
        1422,
        1424,
        1424,
        1480,
        1522,
        1525,
        1535,
        1541,
        1541,
        1564,
        1565,
        1568,
        1610,
        1632,
        1641,
        1646,
        1647,
        1649,
        1747,
        1749,
        1749,
        1765,
        1766,
        1774,
        1788,
        1791,
        1791,
        1806,
        1806,
        1808,
        1808,
        1810,
        1839,
        1867,
        1957,
        1969,
        2026,
        2036,
        2037,
        2042,
        2069,
        2074,
        2074,
        2084,
        2084,
        2088,
        2088,
        2094,
        2095,
        2111,
        2136,
        2140,
        2141,
        2143,
        2275,
        2303,
        2303,
        2308,
        2361,
        2365,
        2365,
        2384,
        2384,
        2392,
        2401,
        2406,
        2415,
        2417,
        2432,
        2436,
        2491,
        2493,
        2493,
        2501,
        2502,
        2505,
        2506,
        2510,
        2518,
        2520,
        2529,
        2532,
        2545,
        2548,
        2553,
        2556,
        2560,
        2564,
        2619,
        2621,
        2621,
        2627,
        2630,
        2633,
        2634,
        2638,
        2640,
        2642,
        2671,
        2674,
        2676,
        2678,
        2688,
        2692,
        2747,
        2749,
        2749,
        2758,
        2758,
        2762,
        2762,
        2766,
        2785,
        2788,
        2799,
        2802,
        2816,
        2820,
        2875,
        2877,
        2877,
        2885,
        2886,
        2889,
        2890,
        2894,
        2901,
        2904,
        2913,
        2916,
        2927,
        2929,
        2945,
        2947,
        3005,
        3011,
        3013,
        3017,
        3017,
        3022,
        3030,
        3032,
        3058,
        3067,
        3072,
        3076,
        3133,
        3141,
        3141,
        3145,
        3145,
        3150,
        3156,
        3159,
        3169,
        3172,
        3198,
        3200,
        3201,
        3204,
        3259,
        3261,
        3261,
        3269,
        3269,
        3273,
        3273,
        3278,
        3284,
        3287,
        3297,
        3300,
        3329,
        3332,
        3389,
        3397,
        3397,
        3401,
        3401,
        3406,
        3414,
        3416,
        3425,
        3428,
        3448,
        3450,
        3457,
        3460,
        3529,
        3531,
        3534,
        3541,
        3541,
        3543,
        3543,
        3552,
        3569,
        3573,
        3632,
        3634,
        3635,
        3643,
        3646,
        3648,
        3654,
        3664,
        3673,
        3676,
        3760,
        3762,
        3763,
        3770,
        3770,
        3773,
        3783,
        3790,
        3840,
        3872,
        3891,
        3904,
        3952,
        3976,
        3980,
        3992,
        3992,
        4029,
        4029,
        4045,
        4045,
        4059,
        4138,
        4159,
        4169,
        4176,
        4181,
        4186,
        4189,
        4193,
        4193,
        4197,
        4198,
        4206,
        4208,
        4213,
        4225,
        4238,
        4238,
        4240,
        4249,
        4256,
        4346,
        4348,
        4956,
        4969,
        5007,
        5018,
        5119,
        5121,
        5740,
        5743,
        5759,
        5761,
        5786,
        5789,
        5866,
        5870,
        5905,
        5909,
        5937,
        5943,
        5969,
        5972,
        6001,
        6004,
        6067,
        6103,
        6103,
        6108,
        6108,
        6110,
        6143,
        6159,
        6312,
        6314,
        6431,
        6444,
        6447,
        6460,
        6463,
        6465,
        6467,
        6470,
        6575,
        6593,
        6599,
        6602,
        6621,
        6656,
        6678,
        6684,
        6685,
        6688,
        6740,
        6751,
        6751,
        6781,
        6782,
        6784,
        6815,
        6823,
        6823,
        6830,
        6911,
        6917,
        6963,
        6981,
        7001,
        7037,
        7039,
        7043,
        7072,
        7086,
        7141,
        7156,
        7163,
        7168,
        7203,
        7224,
        7226,
        7232,
        7293,
        7296,
        7359,
        7368,
        7375,
        7401,
        7404,
        7406,
        7409,
        7413,
        7615,
        7655,
        7675,
        7680,
        8124,
        8126,
        8126,
        8130,
        8140,
        8144,
        8156,
        8160,
        8172,
        8176,
        8188,
        8191,
        8191,
        8293,
        8297,
        8304,
        8313,
        8319,
        8329,
        8335,
        8351,
        8378,
        8399,
        8433,
        8447,
        8450,
        8450,
        8455,
        8455,
        8458,
        8467,
        8469,
        8469,
        8473,
        8477,
        8484,
        8484,
        8486,
        8486,
        8488,
        8488,
        8490,
        8493,
        8495,
        8505,
        8508,
        8511,
        8517,
        8521,
        8526,
        8526,
        8528,
        8591,
        9204,
        9215,
        9255,
        9279,
        9291,
        9371,
        9450,
        9471,
        9984,
        9984,
        10102,
        10131,
        11085,
        11087,
        11098,
        11492,
        11499,
        11502,
        11506,
        11512,
        11517,
        11517,
        11520,
        11631,
        11633,
        11646,
        11648,
        11743,
        11823,
        11823,
        11836,
        11903,
        11930,
        11930,
        12020,
        12031,
        12246,
        12271,
        12284,
        12287,
        12293,
        12295,
        12321,
        12329,
        12337,
        12341,
        12344,
        12348,
        12352,
        12440,
        12445,
        12447,
        12449,
        12538,
        12540,
        12687,
        12690,
        12693,
        12704,
        12735,
        12772,
        12799,
        12831,
        12841,
        12872,
        12879,
        12881,
        12895,
        12928,
        12937,
        12977,
        12991,
        13055,
        13055,
        13312,
        19903,
        19968,
        42127,
        42183,
        42237,
        42240,
        42508,
        42512,
        42606,
        42623,
        42654,
        42656,
        42735,
        42744,
        42751,
        42775,
        42783,
        42786,
        42888,
        42891,
        43009,
        43011,
        43013,
        43015,
        43018,
        43020,
        43042,
        43052,
        43061,
        43066,
        43123,
        43128,
        43135,
        43138,
        43187,
        43205,
        43213,
        43216,
        43231,
        43250,
        43255,
        43259,
        43301,
        43312,
        43334,
        43348,
        43358,
        43360,
        43391,
        43396,
        43442,
        43470,
        43485,
        43488,
        43560,
        43575,
        43586,
        43588,
        43595,
        43598,
        43611,
        43616,
        43638,
        43642,
        43642,
        43644,
        43695,
        43697,
        43697,
        43701,
        43702,
        43705,
        43709,
        43712,
        43712,
        43714,
        43741,
        43744,
        43754,
        43762,
        43764,
        43767,
        44002,
        44014,
        55295,
        55297,
        56190,
        56193,
        56318,
        56321,
        57342,
        57344,
        64285,
        64287,
        64296,
        64298,
        64433,
        64450,
        64829,
        64832,
        65019,
        65022,
        65023,
        65050,
        65055,
        65063,
        65071,
        65107,
        65107,
        65127,
        65127,
        65132,
        65278,
        65280,
        65280,
        65296,
        65305,
        65313,
        65338,
        65345,
        65370,
        65382,
        65503,
        65511,
        65511,
        65519,
        65528,
        65534,
        65791,
        65795,
        65846,
        65856,
        65912,
        65930,
        65935,
        65948,
        65999,
        66046,
        66462,
        66464,
        66511,
        66513,
        67670,
        67672,
        67870,
        67872,
        67902,
        67904,
        68096,
        68100,
        68100,
        68103,
        68107,
        68112,
        68151,
        68155,
        68158,
        68160,
        68175,
        68185,
        68222,
        68224,
        68408,
        68416,
        69631,
        69635,
        69687,
        69710,
        69759,
        69763,
        69807,
        69826,
        69887,
        69891,
        69926,
        69941,
        69951,
        69956,
        70015,
        70019,
        70066,
        70081,
        70084,
        70089,
        71338,
        71352,
        74863,
        74868,
        94032,
        94079,
        94094,
        94099,
        118783,
        119030,
        119039,
        119079,
        119080,
        119262,
        119295,
        119366,
        119551,
        119639,
        120512,
        120514,
        120538,
        120540,
        120570,
        120572,
        120596,
        120598,
        120628,
        120630,
        120654,
        120656,
        120686,
        120688,
        120712,
        120714,
        120744,
        120746,
        120770,
        120772,
        126703,
        126706,
        126975,
        127020,
        127023,
        127124,
        127135,
        127151,
        127152,
        127167,
        127168,
        127184,
        127184,
        127200,
        127247,
        127279,
        127279,
        127340,
        127343,
        127387,
        127461,
        127491,
        127503,
        127547,
        127551,
        127561,
        127567,
        127570,
        127743,
        127777,
        127791,
        127798,
        127798,
        127869,
        127871,
        127892,
        127903,
        127941,
        127941,
        127947,
        127967,
        127985,
        127999,
        128063,
        128063,
        128065,
        128065,
        128248,
        128248,
        128253,
        128255,
        128318,
        128319,
        128324,
        128335,
        128360,
        128506,
        128577,
        128580,
        128592,
        128639,
        128710,
        128767,
        128884,
        917504,
        917506,
        917535,
        917632,
        917759,
        918000,
        1114111
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
    Lower = unicode:characters_to_binary(
        lower_chars(unicode_chars(Token0)), utf8
    ),
    case maps:get(remove_diacritics, Opts, false) of
        false -> Lower;
        0 -> Lower;
        true -> strip_diacritics(Lower, 1);
        1 -> strip_diacritics(Lower, 1);
        2 -> strip_diacritics(Lower, 2)
    end.

lower_chars(Chars) ->
    [
        client_simple_fold(Char)
     || Char <- lists:flatten(
            [unicode_util:lowercase([C]) || C <- lists:flatten(Chars)]
        )
    ].

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
    case sqlite_diacritic_mark(Char) of
        true -> [];
        false -> [Char]
    end;
sqlite_fold_diacritic(Char, Mode) ->
    Decomposed = lists:flatten(unicode_util:nfd([Char])),
    case Decomposed of
        [Base | Marks] when
            (Base >= $a andalso Base =< $z) orelse
                (Base >= $A andalso Base =< $Z)
        ->
            Removable = [M || M <- Marks, sqlite_diacritic_mark(M)],
            case
                {Mode, length(Removable), length(Removable) =:= length(Marks)}
            of
                {_Any, 0, _} ->
                    [Char];
                {1, N, true} when N > 1 -> [Char];
                {_Any, _N, true} ->
                    [Base];
                {_Any, _N, false} ->
                    [Base | [M || M <- Marks, not sqlite_diacritic_mark(M)]]
            end;
        _ ->
            [Char]
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
                true ->
                    {error, {fts_parse, empty_query}};
                false ->
                    case lex(Query, Opts) of
                        {ok, Tokens} ->
                            case length(Tokens) =< ?MAX_QUERY_TOKENS of
                                true ->
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
    case lex_chars(unicode_chars(Query), Opts, [], true) of
        {ok, Tokens} -> {ok, normalise_near_tokens(Tokens)};
        Error -> Error
    end.

lex_chars([], _Opts, Acc, _AfterSpace) ->
    {ok, lists:reverse(Acc)};
lex_chars([C | Rest], Opts, Acc, _AfterSpace) when
    C == 32; C == 9; C == 10; C == 13
->
    lex_chars(Rest, Opts, Acc, true);
lex_chars([$( | Rest], Opts, Acc, _AfterSpace) ->
    lex_chars(Rest, Opts, [lparen | Acc], false);
lex_chars([$) | Rest], Opts, Acc, _AfterSpace) ->
    lex_chars(Rest, Opts, [rparen | Acc], false);
lex_chars([$: | Rest], Opts, Acc, _AfterSpace) ->
    lex_chars(Rest, Opts, [colon | Acc], false);
lex_chars([$* | Rest], Opts, Acc, _AfterSpace) ->
    lex_chars(Rest, Opts, [star | Acc], false);
lex_chars([$+ | Rest], Opts, Acc, _AfterSpace) ->
    lex_chars(Rest, Opts, [plus | Acc], false);
lex_chars([$, | Rest], Opts, Acc, _AfterSpace) ->
    lex_chars(Rest, Opts, [comma | Acc], false);
lex_chars([$/ | Rest], Opts, [near_candidate | _] = Acc, _AfterSpace) ->
    lex_chars(Rest, Opts, [slash | Acc], false);
lex_chars([$/ | Rest], Opts, [near_case_variant | _] = Acc, _AfterSpace) ->
    lex_chars(Rest, Opts, [slash | Acc], false);
lex_chars([${ | Rest], Opts, Acc, _AfterSpace) ->
    lex_chars(Rest, Opts, [lbrace | Acc], false);
lex_chars([$} | Rest], Opts, Acc, _AfterSpace) ->
    lex_chars(Rest, Opts, [rbrace | Acc], false);
lex_chars([$- | Rest], Opts, Acc, _AfterSpace) ->
    lex_chars(Rest, Opts, [minus | Acc], false);
lex_chars([$^ | Rest], Opts, Acc, _AfterSpace) ->
    lex_chars(Rest, Opts, [caret | Acc], false);
lex_chars([$" | Rest], Opts, Acc, _AfterSpace) ->
    case collect_quote(Rest, []) of
        {ok, Phrase, Rest2} ->
            lex_chars(
                Rest2,
                Opts,
                [
                    {phrase, unicode:characters_to_binary(Phrase, utf8)} | Acc
                ],
                false
            );
        error ->
            {error, {fts_parse, unterminated_quote}}
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

collect_word([], _Opts, Acc) ->
    {Acc, []};
collect_word([$-, Next | Rest], Opts, Acc) ->
    case token_char(Next, Opts) of
        true -> collect_word(Rest, Opts, [Next, $- | Acc]);
        false -> {Acc, [$-, Next | Rest]}
    end;
collect_word([C | Rest], Opts, Acc) ->
    case token_char(C, Opts) of
        true -> collect_word(Rest, Opts, [C | Acc]);
        false -> {Acc, [C | Rest]}
    end.

classify_word(<<"AND">>, _Opts) ->
    'and';
classify_word(<<"OR">>, _Opts) ->
    'or';
classify_word(<<"NOT">>, _Opts) ->
    'not';
classify_word(<<"NEAR">>, _Opts) ->
    near_candidate;
classify_word(Word, Opts) ->
    case normalise_token(Word, Opts) of
        <<"near">> ->
            near_case_variant;
        _ ->
            case tokenize(Word, Opts) of
                [] -> skip;
                [{Token, _}] -> {word, Token};
                Tokens -> {phrase_tokens, [Token || {Token, _} <- Tokens]}
            end
    end.

%% NEAR syntax synonyms are resolved at the lexer seam.  The parser sees one
%% operator token for every infix spelling, while the long-standing function
%% form retains its grouped token shape.  Lowercase "near" remains an ordinary
%% prose term unless the spelling carries an explicit distance.
normalise_near_tokens([near_candidate, lparen | Rest]) ->
    [near, lparen | normalise_near_tokens(Rest)];
normalise_near_tokens([near_candidate, comma, {word, Distance} | Rest]) ->
    normalise_near_distance(Distance, Rest, near_candidate, comma);
normalise_near_tokens([near_candidate, slash, {word, Distance} | Rest]) ->
    normalise_near_distance(Distance, Rest, near_candidate, slash);
normalise_near_tokens([near_candidate | Rest]) ->
    [{near_op, ?DEFAULT_NEAR} | normalise_near_tokens(Rest)];
normalise_near_tokens([near_case_variant, comma, {word, Distance} | Rest]) ->
    normalise_near_distance(Distance, Rest, near_case_variant, comma);
normalise_near_tokens([near_case_variant, slash, {word, Distance} | Rest]) ->
    normalise_near_distance(Distance, Rest, near_case_variant, slash);
normalise_near_tokens([near_case_variant | Rest]) ->
    [{word, <<"near">>} | normalise_near_tokens(Rest)];
normalise_near_tokens([Token | Rest]) ->
    [Token | normalise_near_tokens(Rest)];
normalise_near_tokens([]) ->
    [].

normalise_near_distance(Distance, Rest, NearToken, Separator) ->
    try binary_to_integer(Distance) of
        N when N >= 0 ->
            [{near_op, N} | normalise_near_tokens(Rest)];
        _ ->
            [
                NearToken,
                Separator,
                {word, Distance}
                | normalise_near_tokens(Rest)
            ]
    catch
        _:_ ->
            [
                NearToken,
                Separator,
                {word, Distance}
                | normalise_near_tokens(Rest)
            ]
    end.

parse_or(Tokens, Opts) ->
    case parse_and(Tokens, Opts) of
        {ok, Left, Rest} -> parse_or_tail(Left, Rest, Opts);
        Error -> Error
    end.

parse_or_tail(Left, ['or' | Rest], Opts) ->
    case parse_and(Rest, Opts) of
        {ok, Right, Rest2} ->
            parse_or_tail({'or', ungroup(Left), ungroup(Right)}, Rest2, Opts);
        Error ->
            Error
    end;
parse_or_tail(Left, Rest, _Opts) ->
    {ok, Left, Rest}.

parse_and(Tokens, Opts) ->
    case parse_not(Tokens, Opts) of
        {ok, Left, Rest} -> parse_and_tail(Left, Rest, Opts);
        Error -> Error
    end.

parse_and_tail(Left, ['and' | Rest], Opts) ->
    case parse_not(Rest, Opts) of
        {ok, Right, Rest2} ->
            parse_and_tail({'and', ungroup(Left), ungroup(Right)}, Rest2, Opts);
        Error ->
            Error
    end;
parse_and_tail(Left, Rest, _Opts) ->
    {ok, Left, Rest}.

parse_not(Tokens, Opts) ->
    case parse_implicit(Tokens, Opts) of
        {ok, Left, Rest} -> parse_not_tail(Left, Rest, Opts);
        Error -> Error
    end.

parse_not_tail(Left, ['not' | Rest], Opts) ->
    case parse_implicit(Rest, Opts) of
        {ok, Right, Rest2} ->
            parse_not_tail({'not', ungroup(Left), ungroup(Right)}, Rest2, Opts);
        Error ->
            Error
    end;
parse_not_tail(Left, Rest, _Opts) ->
    {ok, Left, Rest}.

parse_implicit(Tokens, Opts) ->
    case parse_near_operator(Tokens, Opts) of
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
                    case parse_near_operator(Rest, Opts) of
                        {ok, Right, Rest2} ->
                            parse_implicit_tail(
                                {'and', ungroup(Left), ungroup(Right)},
                                Rest2,
                                Opts
                            );
                        Error ->
                            Error
                    end
            end;
        false ->
            {ok, Left, Rest}
    end.

parse_near_operator(Tokens, Opts) ->
    case parse_primary(Tokens, Opts) of
        {ok, Left, Rest} -> parse_near_operator_tail(Left, Rest, Opts);
        Error -> Error
    end.

parse_near_operator_tail(Left, [{near_op, Distance} | Rest], Opts) ->
    case parse_primary(Rest, Opts) of
        {ok, Right, Rest2} ->
            case append_near_item(ungroup(Left), ungroup(Right), Distance) of
                {ok, Near} ->
                    parse_near_operator_tail(Near, Rest2, Opts);
                Error ->
                    Error
            end;
        Error ->
            Error
    end;
parse_near_operator_tail(Left, Rest, _Opts) ->
    {ok, Left, Rest}.

append_near_item(
    {near, _Items, Distance, _Columns},
    {near, _RightItems, _RightDistance, _RightColumns},
    Distance
) ->
    {error, {fts_parse, invalid_near_nesting}};
append_near_item(
    {near, Items, Distance, Columns},
    Right,
    Distance
) ->
    {ok, {near, Items ++ [Right], Distance, Columns}};
append_near_item(
    {near, _Items, _OtherDistance, _Columns},
    _Right,
    _Distance
) ->
    {error, {fts_parse, mixed_near_distances}};
append_near_item(
    _Left,
    {near, _Items, _Distance, _Columns},
    _NearDistance
) ->
    {error, {fts_parse, invalid_near_nesting}};
append_near_item(Left, Right, Distance) ->
    {ok, {near, [Left, Right], Distance, all}}.

parse_primary([], _Opts) ->
    {error, {fts_parse, unexpected_end}};
parse_primary(Tokens, Opts) ->
    case parse_primary_base(Tokens, Opts) of
        {ok, AST, Rest} -> parse_concat_tail(AST, Rest, Opts);
        Error -> Error
    end.

parse_primary_base([], _Opts) ->
    {error, {fts_parse, unexpected_end}};
parse_primary_base([near, lparen | Rest], Opts) ->
    parse_near(Rest, Opts);
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
        {ok, Columns, [colon | Rest2]} ->
            parse_column_primary(Columns, Rest2, Opts);
        Error ->
            Error
    end;
parse_primary_base([minus, lbrace | Rest], Opts) ->
    case collect_columns(Rest, []) of
        {ok, Columns, [colon | Rest2]} ->
            parse_column_primary(
                {not_columns, Columns},
                Rest2,
                Opts
            );
        Error ->
            Error
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
        {ok, AST, Rest2} ->
            {ok, restrict_ast_columns(ungroup(AST), Columns), Rest2};
        Error ->
            Error
    end.

collect_columns([rbrace | _Rest], []) ->
    {error, {fts_parse, empty_column_list}};
collect_columns([rbrace | Rest], Acc) ->
    {ok, lists:reverse(Acc), Rest};
collect_columns([{word, Column} | Rest], Acc) ->
    collect_columns(Rest, [normalise_column(Column) | Acc]);
collect_columns([{phrase, Column} | Rest], Acc) ->
    collect_columns(Rest, [normalise_column(Column) | Acc]);
collect_columns(Other, _Acc) ->
    {error, {fts_parse, invalid_column_list, Other}}.

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
parse_near_items([comma | Rest], Opts, Acc) when Acc =/= [] ->
    case starts_primary(Rest) of
        true -> parse_near_items(Rest, Opts, Acc);
        false -> {error, {fts_parse, unexpected_token, comma}}
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
         || {Token, N} <- lists:zip(
                Tokens, lists:seq(0, max(0, length(Tokens) - 1))
            )
        ],
    {phrase, Specs, all}.

concat_phrase({term, Token, Prefix, Columns}, Right) ->
    concat_phrase({phrase, [{Token, Prefix, 0}], Columns}, Right);
concat_phrase(
    {phrase, LeftSpecs, LeftColumns}, {term, Token, Prefix, RightColumns}
) ->
    Offset = length(LeftSpecs),
    {ok,
        {phrase, LeftSpecs ++ [{Token, Prefix, Offset}],
            concat_columns(LeftColumns, RightColumns)}};
concat_phrase(
    {phrase, LeftSpecs, LeftColumns}, {phrase, RightSpecs, RightColumns}
) ->
    Offset = length(LeftSpecs),
    {ok,
        {phrase,
            LeftSpecs ++
                [
                    {Token, Prefix, Offset + Pos}
                 || {Token, Prefix, Pos} <- RightSpecs
                ],
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
    [
        Column
     || Column <- schema_columns(A), lists:member(Column, schema_columns(B))
    ].

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

ast_depth({'and', A, B}) ->
    1 + max(ast_depth(A), ast_depth(B));
ast_depth({'or', A, B}) ->
    1 + max(ast_depth(A), ast_depth(B));
ast_depth({'not', A, B}) ->
    1 + max(ast_depth(A), ast_depth(B));
ast_depth({anchor, A}) ->
    1 + ast_depth(A);
ast_depth({near, Items, _Distance, _Columns}) ->
    1 + lists:max([0 | [ast_depth(I) || I <- Items]]);
ast_depth(_Other) ->
    1.

validate_near_distance({near, Items, Distance, _Columns}) when
    is_integer(Distance), Distance >= 0, Distance =< ?MAX_NEAR_DISTANCE
->
    validate_ast_list(Items, fun validate_near_distance/1);
validate_near_distance({near, _Items, _Distance, _Columns}) ->
    {error, fts_query_near_distance_exceeded};
validate_near_distance(AST) ->
    validate_children(AST, fun validate_near_distance/1).

validate_prefix_bytes({term, Token, true, _Columns}) when
    byte_size(Token) > ?MAX_PREFIX_BYTES
->
    {error, fts_query_prefix_too_large};
validate_prefix_bytes({phrase, Specs, _Columns}) ->
    case
        lists:any(
            fun
                ({Token, true, _Pos}) -> byte_size(Token) > ?MAX_PREFIX_BYTES;
                (_) -> false
            end,
            Specs
        )
    of
        true -> {error, fts_query_prefix_too_large};
        false -> ok
    end;
validate_prefix_bytes(AST) ->
    validate_children(AST, fun validate_prefix_bytes/1).

validate_children({'and', A, B}, Fun) ->
    validate_pair(A, B, Fun);
validate_children({'or', A, B}, Fun) ->
    validate_pair(A, B, Fun);
validate_children({'not', A, B}, Fun) ->
    validate_pair(A, B, Fun);
validate_children({anchor, A}, Fun) ->
    Fun(A);
validate_children({near, Items, _Distance, _Columns}, Fun) ->
    validate_ast_list(Items, Fun);
validate_children(_Other, _Fun) ->
    ok.

validate_pair(A, B, Fun) ->
    case Fun(A) of
        ok -> Fun(B);
        Error -> Error
    end.

validate_ast_list([], _Fun) ->
    ok;
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
    case
        [
            Column
         || Column <- ast_columns(AST), not lists:member(Column, Columns)
        ]
    of
        [] -> ok;
        [Unknown | _Rest] -> {error, {fts_parse, unknown_column, Unknown}}
    end.

ast_columns({term, _T, _P, Columns}) ->
    selector_columns(Columns);
ast_columns({phrase, _Specs, Columns}) ->
    selector_columns(Columns);
ast_columns({near, Items, _Distance, Columns}) ->
    lists:usort(
        selector_columns(Columns) ++
            lists:append([ast_columns(Item) || Item <- Items])
    );
ast_columns({anchor, AST}) ->
    ast_columns(AST);
ast_columns({'and', A, B}) ->
    lists:usort(ast_columns(A) ++ ast_columns(B));
ast_columns({'or', A, B}) ->
    lists:usort(ast_columns(A) ++ ast_columns(B));
ast_columns({'not', A, B}) ->
    lists:usort(ast_columns(A) ++ ast_columns(B));
ast_columns(_Other) ->
    [].

selector_columns(all) -> [];
selector_columns({not_columns, Columns}) -> schema_columns(Columns);
selector_columns(Columns) -> schema_columns(Columns).

restrict_ast_columns(AST, all) ->
    AST;
restrict_ast_columns(_AST, []) ->
    {empty};
restrict_ast_columns({term, T, P, Cols0}, Cols) ->
    case combine_columns(Cols0, Cols) of
        [] -> {empty};
        Cols1 -> {term, T, P, Cols1}
    end;
restrict_ast_columns({phrase, Specs, Cols0}, Cols) ->
    case combine_columns(Cols0, Cols) of
        [] -> {empty};
        Cols1 -> {phrase, Specs, Cols1}
    end;
restrict_ast_columns({near, Items, Distance, Cols0}, Cols) ->
    case combine_columns(Cols0, Cols) of
        [] ->
            {empty};
        Cols1 ->
            {near, [restrict_ast_columns(I, Cols1) || I <- Items], Distance,
                Cols1}
    end;
restrict_ast_columns({anchor, AST}, Cols) ->
    case restrict_ast_columns(AST, Cols) of
        {empty} -> {empty};
        AST1 -> {anchor, AST1}
    end;
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

combine_columns(all, Cols) ->
    normalise_column_selector(Cols);
combine_columns(Cols, all) ->
    normalise_column_selector(Cols);
combine_columns({not_columns, ExcludedA}, {not_columns, ExcludedB}) ->
    %% Nested negative selectors compose by union: the term must avoid
    %% every excluded column from both levels.
    {not_columns,
        lists:usort(
            schema_columns(ExcludedA) ++ schema_columns(ExcludedB)
        )};
combine_columns({not_columns, Excluded}, Cols) ->
    lists:subtract(
        normalise_column_selector(Cols), normalise_column_selector(Excluded)
    );
combine_columns(Cols, {not_columns, Excluded}) ->
    lists:subtract(
        normalise_column_selector(Cols), normalise_column_selector(Excluded)
    );
combine_columns(A, B) ->
    [
        C
     || C <- normalise_column_selector(A),
        lists:member(C, normalise_column_selector(B))
    ].

normalise_column_selector({not_columns, Columns}) ->
    {not_columns, schema_columns(Columns)};
normalise_column_selector(Columns) ->
    schema_columns(Columns).

binary_prefix(Bin, Prefix) when
    is_binary(Bin), is_binary(Prefix), byte_size(Bin) >= byte_size(Prefix)
->
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

scoring_phrases({term, _Token, _Prefix, _Cols} = Leaf) ->
    [Leaf];
scoring_phrases({phrase, _Specs, _Cols} = Leaf) ->
    [Leaf];
scoring_phrases({near, Items, Distance, Cols}) ->
    [
        {near_member, Index, Items, Distance, Cols}
     || Index <- lists:seq(1, length(Items))
    ];
scoring_phrases({anchor, AST}) ->
    scoring_phrases(AST);
scoring_phrases({'and', A, B}) ->
    scoring_phrases(A) ++ scoring_phrases(B);
scoring_phrases({'or', A, B}) ->
    scoring_phrases(A) ++ scoring_phrases(B);
scoring_phrases({'not', A, _B}) ->
    scoring_phrases(A);
scoring_phrases(_Other) ->
    [].

leaf_tf(Meta, {term, Token, Prefix, Cols}) ->
    client_term_frequency(Meta, Token, Prefix, Cols);
leaf_tf(Meta, {phrase, Specs, Cols}) ->
    length(phrase_match_positions(Meta, Specs, Cols));
leaf_tf(Meta, {near_member, Index, Items, Distance, Cols}) ->
    near_member_tf(Meta, Items, Index, Distance, Cols, filtered).

%% Counts remain a separate scoring field, but newly written postings also
%% carry the complete position list.  Legacy in-memory metas (used only by
%% retained private helpers) have no counts and retain their former behaviour.
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
        length(
            near_member_column_spans(
                Meta, Items, Index, Member, Distance, Column, Mode
            )
        )
     || Column <- concrete_columns(Cols)
    ]).

near_member_column_spans(
    Meta, _Items, _Index, Member, _Distance, Column, standalone
) ->
    item_spans_in_column(Meta, Member, Column);
near_member_column_spans(
    Meta, Items, Index, _Member, Distance, Column, filtered
) ->
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
                    N = length([
                        ok
                     || Meta <- MetaList, leaf_df_tf(Meta, Leaf) > 0
                    ]),
                    Acc#{Leaf => N}
            end
        end,
        #{},
        Leaves
    ).

bm25_score_precomputed(Meta, Leaves, Idfs, AvgDl) ->
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
                    Idf = maps:get(Leaf, Idfs),
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

client_window_positions(Value) ->
    {Windowed, _Remaining} = client_window_positions(
        Value, ?MAX_RETURN_POSITIONS
    ),
    Windowed.

client_window_positions(Value, Remaining) when is_map(Value) ->
    lists:foldl(
        fun({Key, Nested}, {Acc, Left}) ->
            {Windowed, NextLeft} = client_window_positions(Nested, Left),
            {Acc#{Key => Windowed}, NextLeft}
        end,
        {#{}, Remaining},
        lists:sort(maps:to_list(Value))
    );
client_window_positions(Value, Remaining) when is_list(Value) ->
    Ordered = lists:sort(Value),
    Kept = lists:sublist(Ordered, Remaining),
    {Kept, Remaining - length(Kept)};
client_window_positions(Value, Remaining) ->
    {Value, Remaining}.

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
phrase_column_match_positions(
    Meta, [{FirstToken, FirstPrefix, FirstOffset} | Rest], Column
) ->
    FirstPositions = column_term_positions(
        Meta, Column, FirstToken, FirstPrefix
    ),
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
        true ->
            [{P, P} || P <- column_term_positions(Meta, Column, Token, Prefix)];
        false ->
            []
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
    {near, [canonicalise_ast_columns(I) || I <- Items], D,
        canonical_selector(Cols)};
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
phrase_column_match_spans(
    Meta, [{FirstToken, FirstPrefix, FirstOffset} | Rest] = Specs, Column
) ->
    FirstPositions = column_term_positions(
        Meta, Column, FirstToken, FirstPrefix
    ),
    LastOffset = phrase_last_offset(Specs),
    [
        {Pos - FirstOffset, Pos - FirstOffset + LastOffset}
     || Pos <- FirstPositions,
        phrase_rest_matches(Meta, Rest, Column, Pos - FirstOffset)
    ].

phrase_last_offset(Specs) ->
    lists:max([Offset || {_Token, _Prefix, Offset} <- Specs]).

-ifdef(TEST).

client_test_tailsum(Tokens, Count) ->
    Bloom0 = <<0:(?TAIL_BLOOM_BYTES * 8)>>,
    Bloom = lists:foldl(
        fun(Token, BloomAcc0) ->
            lists:foldl(
                fun client_test_set_bloom_bit/2,
                BloomAcc0,
                client_tail_bloom_positions(Token)
            )
        end,
        Bloom0,
        Tokens
    ),
    <<?TAILSUM_VERSION:8, Count:32/unsigned-big, Bloom/binary>>.

client_test_set_bloom_bit(Position, Bloom) ->
    ByteOffset = Position bsr 3,
    <<Head:ByteOffset/binary, Byte:8, Tail/binary>> = Bloom,
    Mask = 1 bsl (Position band 7),
    <<Head/binary, (Byte bor Mask):8, Tail/binary>>.

tail_summary_bloom_probe_test() ->
    Summary = client_test_tailsum([<<"alpha">>, <<"beta">>], 2),
    ?assert(client_tailsum_nonempty(Summary)),
    ?assert(client_tailsum_might_contain(Summary, <<"alpha">>, false)),
    ?assert(client_tailsum_might_contain(Summary, <<"beta">>, false)),
    ?assertNot(client_tailsum_might_contain(Summary, <<"absent">>, false)),
    %% Prefixes cannot be disproved by an exact-token bloom.
    ?assert(client_tailsum_might_contain(Summary, <<"abs">>, true)),
    %% Today's dirty writer is deliberately all ones, hence conservative.
    Dirty = client_encode_tailsum(dirty),
    ?assert(client_tailsum_might_contain(Dirty, <<"absent">>, false)),
    Empty = client_encode_tailsum(empty),
    ?assertNot(client_tailsum_might_contain(Empty, <<"alpha">>, false)),
    Shards = 8,
    [AlphaShard] = client_token_shards(<<"alpha">>, false, Shards),
    Summaries = #{AlphaShard => {ok, Summary}},
    Schema = #{shards => Shards},
    ?assert(
        client_has_dirty_tail(
            Summaries, [{<<"alpha">>, false, all}], Schema
        )
    ),
    ?assertNot(
        client_has_dirty_tail(
            Summaries, [{<<"absent">>, false, all}], Schema
        )
    ).

hyphenated_word_query_test() ->
    Opts = #{
        stopwords => [],
        tokenchars => [],
        separators => [],
        remove_diacritics => false
    },
    ?assertEqual(
        {ok,
            {phrase,
                [
                    {<<"cross">>, false, 0},
                    {<<"claim">>, false, 1}
                ],
                all}},
        parse(<<"cross-claim">>, Opts)
    ),
    ?assertMatch(
        {ok, _},
        parse(<<"Fairline JV BSA cross-claim">>, Opts)
    ),
    ?assertMatch(
        {error, {fts_parse, trailing_tokens, [minus | _]}},
        parse(<<"cross - claim">>, Opts)
    ).

near_syntax_synonym_lexer_test() ->
    Opts = #{
        stopwords => [],
        tokenchars => [],
        separators => [],
        remove_diacritics => false
    },
    Alpha = {term, <<"alpha">>, false, all},
    Beta = {term, <<"beta">>, false, all},
    Expected3 = {ok, {near, [Alpha, Beta], 3, all}},
    ?assertEqual(
        {ok, [
            {word, <<"alpha">>},
            {near_op, 3},
            {word, <<"beta">>}
        ]},
        lex(<<"alpha Near,3 beta">>, Opts)
    ),
    ?assertEqual(
        {ok, [
            {word, <<"alpha">>},
            {word, <<"near">>},
            {word, <<"beta">>}
        ]},
        lex(<<"alpha near beta">>, Opts)
    ),
    [
        ?assertEqual(Expected3, parse(Query, Opts))
     || Query <- [
            <<"NEAR(alpha beta, 3)">>,
            <<"NEAR(alpha, beta, 3)">>,
            <<"alpha NEAR,3 beta">>,
            <<"alpha NEAR/3 beta">>,
            <<"alpha Near,3 beta">>,
            <<"alpha near,3 beta">>,
            <<"alpha nEaR,3 beta">>
        ]
    ],
    ?assertEqual(
        {ok, {near, [Alpha, Beta], ?DEFAULT_NEAR, all}},
        parse(<<"alpha NEAR beta">>, Opts)
    ),
    ?assertEqual(
        {ok,
            {near,
                [
                    {phrase,
                        [
                            {<<"alpha">>, false, 0},
                            {<<"one">>, false, 1}
                        ],
                        all},
                    {phrase,
                        [
                            {<<"beta">>, false, 0},
                            {<<"two">>, false, 1}
                        ],
                        all}
                ],
                3, all}},
        parse(<<"\"alpha one\" NEAR,3 \"beta two\"">>, Opts)
    ),
    {ok, ProseAST} = parse(
        <<"land and order not notice or claim">>, Opts
    ),
    ?assertEqual(
        [
            <<"and">>,
            <<"claim">>,
            <<"land">>,
            <<"not">>,
            <<"notice">>,
            <<"or">>,
            <<"order">>
        ],
        lists:sort([
            Token
         || {Token, false, all} <- client_ast_token_specs(ProseAST)
        ])
    ).

near_measure3_probe_variants_test_() ->
    {timeout, 60, fun near_measure3_probe_variants_tester/0}.

near_measure3_probe_variants_tester() ->
    client_with_test_bookie(fun(Bookie) ->
        {ok, Schema} = schema(#{
            index => <<"near-syntax-synonyms">>, columns => [body]
        }),
        ok = client_test_put(
            Bookie,
            Schema,
            <<"probe-hit">>,
            <<"Point Duty correspondence about the cross-claim">>
        ),
        ok = client_test_put(
            Bookie,
            Schema,
            <<"syntax-hit">>,
            <<"alpha one middle beta two">>
        ),
        Queries = [
            <<"NEAR(\"Point Duty\" cross claim, 20)">>,
            <<"NEAR(\"Point Duty\" \"cross claim\", 20)">>,
            <<"NEAR(\"Point Duty\" \"cross claim\", 20)">>,
            <<"NEAR(\"Point Duty\" \"cross-claim\", 20)">>,
            <<"NEAR(\"Point Duty\" cross-claim, 20)">>,
            <<"NEAR(Point Duty cross claim, 20)">>,
            <<"NEAR(Point Duty cross-claim, 20)">>,
            <<"NEAR(Point Duty Cross Claim, 20)">>
        ],
        [
            ?assertMatch(
                {ok, [#{key := <<"probe-hit">>}]},
                search(Bookie, Schema, Query, #{})
            )
         || Query <- Queries
        ],
        SyntaxQueries = [
            <<"NEAR(alpha, beta, 3)">>,
            <<"alpha NEAR,3 beta">>,
            <<"alpha NEAR/3 beta">>,
            <<"alpha NEAR beta">>,
            <<"alpha Near,3 beta">>,
            <<"\"alpha one\" NEAR,3 \"beta two\"">>
        ],
        [
            ?assertMatch(
                {ok, [#{key := <<"syntax-hit">>}]},
                search(Bookie, Schema, Query, #{})
            )
         || Query <- SyntaxQueries
        ]
    end).

client_codec_and_capacity_test() ->
    Positions = lists:seq(0, 69999),
    Posting = #{
        0 => #{
            <<"alpha">> => #{
                count => 70000,
                positions => Positions
            }
        }
    },
    V = <<1, 2, 3, 4, 5, 6, 7, 8>>,
    Encoded = client_encode_posting({V, Posting}),
    {V, Decoded} = client_decode_posting(Encoded),
    #{
        0 := #{
            <<"alpha">> := #{
                count := 70000,
                positions := DecodedPositions
            }
        }
    } = Decoded,
    ?assertEqual(Positions, DecodedPositions),
    ?assertEqual(70000, length(DecodedPositions)),
    ?assertEqual(
        70000, maps:get(count, maps:get(<<"alpha">>, maps:get(0, Decoded)))
    ),
    Tail = client_encode_tail(V, 42, [41], 70000, none, live, Posting),
    {V, 42, [41], 70000, none, live, TailPosting} =
        client_decode_tail(Tail),
    ?assertEqual(Posting, TailPosting),
    Legacy =
        <<?POSTING_VERSION:8, V:8/binary, 1:8, 0:8, 1:32/unsigned-big,
            5:16/unsigned-big, "alpha", 2:64/unsigned-big, 1:16/unsigned-big,
            0:8>>,
    {V, #{
        0 := #{
            <<"alpha">> := #{
                count := 2,
                positions := [0]
            }
        }
    }} = client_decode_posting(Legacy),
    Columns255 = [integer_to_binary(I) || I <- lists:seq(1, 255)],
    ?assertMatch(
        {ok, _}, schema(#{index => <<"cap255">>, columns => Columns255})
    ),
    Columns256 = [integer_to_binary(I) || I <- lists:seq(1, 256)],
    ?assertMatch(
        {error, {fts_capacity_exceeded, columns, 256, 255}},
        schema(#{index => <<"cap256">>, columns => Columns256})
    ),
    ?assertError(
        {fts_capacity_exceeded, column_id, 255, 254},
        client_encode_posting(
            {<<0:64>>, #{255 => #{<<"x">> => #{count => 1, positions => [0]}}}}
        )
    ).

v7_page_entry_roundtrip_test() ->
    DerivedDocId = ?TRANSIENT_DOC_ID_BIT + 16#123456789ABC,
    ?assertEqual(
        varint_append(DerivedDocId, <<>>),
        client_encode_doc_id(DerivedDocId)
    ),
    ?assertEqual(
        {ok, DerivedDocId, <<>>},
        decode_varint(client_encode_doc_id(DerivedDocId))
    ),
    Boolean = client_encode_boolean_entry(300, 9876, 70000),
    ?assertEqual(
        {300,
            {300, 9876, #{
                4 => #{
                    <<"alpha">> => #{
                        count => 70000, positions => [0]
                    }
                }
            }}},
        client_decode_page_entry(
            Boolean, 0, <<"alpha">>, 4, ?BOOLEAN_PLANE
        )
    ),
    [PositionRaw] = client_encode_position_entries(
        300, [0, 1, 128, 65536]
    ),
    Position = client_plane_payload_entry(?POSITION_PLANE, PositionRaw),
    ?assertEqual(
        {300, {300, [0, 1, 128, 65536]}},
        client_decode_page_entry(
            Position, 0, <<"alpha">>, 4, ?POSITION_PLANE
        )
    ).

v8_ordered_chunk_bound_and_probe_test() ->
    Rows = [
        {
            ?TRANSIENT_DOC_ID_BIT + I,
            {boolean, 1,
                case I =< 700 of
                    true -> 100;
                    false -> 1
                end}
        }
     || I <- lists:seq(1, 5000)
    ],
    Encoded = client_encode_v8_plane_rows(
        ?BOOLEAN_PLANE, 0, length(Rows), Rows
    ),
    {_Tree, SkippedBound, Seen} = lists:foldl(
        fun({_Plane, _Column, _PageNo, SubKey, Value}, State) ->
            ?assertEqual(
                {page, ?BOOLEAN_PLANE, 0, client_v8_subkey_page_number(SubKey)},
                client_decode_page_row(SubKey, Value)
            ),
            client_scan_ranked_v8_page(
                Value, 10, [1.0], 1.0, 1.0, State
            )
        end,
        {gb_trees:empty(), none, []},
        Encoded
    ),
    ?assert(is_float(SkippedBound)),
    ?assert(length(Seen) < length(Rows)),
    ProbeId = ?TRANSIENT_DOC_ID_BIT + 4500,
    [{_Plane, _Column, _PageNo, _SubKey, ProbePage}] = [
        Row
     || Row =
            {_P0, _C0, _N0,
                <<_P:8, _C:8, Last:64/unsigned-big, First:64/unsigned-big,
                    _N:16/unsigned-big>>,
                _Value} <-
            Encoded,
        ProbeId >= First,
        ProbeId =< Last
    ],
    ?assertMatch(
        #{ProbeId := {ProbeId, 1, 1}},
        client_decode_v8_direct_term_candidates(
            ProbePage, [{ProbeId, true}]
        )
    ),
    lists:foreach(
        fun({Tf, DocLength, AvgLength}) ->
            Bound = client_quantized_score_bound(Tf) / ?SCORE_BOUND_SCALE,
            Actual = client_direct_bm25_score_precomputed(
                [Tf], [1.0], AvgLength, DocLength
            ),
            ?assert(Bound >= Actual)
        end,
        [
            {1, 1, 1.0},
            {3, 1, 1000.0},
            {100, 50, 10.0},
            {?MAX_U16, 1, 1.0}
        ]
    ).

hot_term_boundary_overflow_test_() ->
    {timeout, 60, fun hot_term_boundary_overflow_tester/0}.

hot_term_boundary_overflow_tester() ->
    client_with_test_bookie(fun(Bookie) ->
        Bucket = <<"boundary-overflow-unit">>,
        Key = client_token_key(<<"hot">>),
        PageRanges = [{I, I, []} || I <- lists:seq(1, 5000)],
        PageBoundaries = client_encode_page_boundaries(
            5000, PageRanges
        ),
        Chunk = {1, 1, 141, 1, <<1, 1>>},
        PageZeroBytes =
            32 +
                byte_size(PageBoundaries) +
                ?CHUNK_DIR_STRIDE +
                byte_size(element(5, Chunk)),
        {Marker, OverflowRows} =
            client_encode_boundary_overflow_rows(
                ?BOOLEAN_PLANE,
                0,
                PageBoundaries,
                PageZeroBytes
            ),
        ?assert(length(OverflowRows) > 1),
        ?assertMatch(
            <<5000:32/unsigned-big, ?BOUNDARY_MARKER:16/unsigned-big,
                _/binary>>,
            Marker
        ),
        Page0 = client_encode_v8_plane_page(
            ?BOOLEAN_PLANE,
            0,
            5000,
            5000,
            1,
            141,
            Marker,
            [Chunk]
        ),
        ?assert(byte_size(Page0) =< ?PAGE_MAX_BYTES),
        SubKey0 = client_v8_page_subkey(
            ?BOOLEAN_PLANE, 0, 0, 1, 1
        ),
        Specs =
            [{add, Bucket, Key, SubKey0, Page0}] ++
                [
                    {add, Bucket, Key, SubKey, Value}
                 || {
                        ?BOUNDARY_PLANE,
                        0,
                        _PartNo,
                        SubKey,
                        Value
                    } <- OverflowRows,
                    byte_size(Value) =< ?PAGE_MAX_BYTES,
                    client_decode_boundary_overflow_row(SubKey, Value) =:=
                        boundary_overflow
                ],
        ?assertEqual(length(OverflowRows) + 1, length(Specs)),
        ok = leveled_bookie:book_mput(Bookie, Specs),
        {ok, Expanded} = client_read_page(
            Bookie, Bucket, Key, ?BOOLEAN_PLANE, 0, 0
        ),
        Boundaries = client_page_boundaries(Expanded),
        ?assertEqual(5000, tuple_size(Boundaries)),
        ?assertEqual({0, 1, 1}, element(1, Boundaries)),
        ?assertEqual({4999, 5000, 5000}, element(5000, Boundaries)),
        ?assertEqual(5000, client_page_global_docs(Expanded)),
        {ok, Schema} = schema(#{index => Bucket, columns => [body]}),
        [Shard] = client_token_shards(<<"hot">>, false, 256),
        #{<<"hot">> := {OldDocs, OldSubKeys}} =
            client_fold_shard_pages(Bookie, Schema, Shard),
        ?assertEqual(length(OverflowRows) + 1, length(OldSubKeys)),
        RemoveSpecs = client_token_page_specs(
            Bucket, <<"hot">>, {OldDocs, OldSubKeys}, #{}, [1]
        ),
        ?assertEqual(length(OldSubKeys), length(RemoveSpecs)),
        ok = leveled_bookie:book_mput(Bookie, RemoveSpecs),
        ?assert(
            lists:all(
                fun(SubKey) ->
                    not_found =:=
                        leveled_bookie:book_headonly(
                            Bookie, Bucket, Key, SubKey
                        )
                end,
                OldSubKeys
            )
        )
    end).

v7_representative_entry_size_test() ->
    Sample = test_v7_entry_size_sample(),
    ?assertEqual(
        #{
            documents => 7379,
            doc_key_bytes => 82,
            count_range => {1, 7},
            boolean => #{v6 => 826448, v7 => 36768},
            position_unkeyed => #{v6 => 140199, v7 => 51524},
            position_keyed => #{v6 => 760035, v7 => 51524}
        },
        Sample
    ),
    #{
        boolean := #{v6 := BooleanV6, v7 := BooleanV7},
        position_unkeyed := #{v6 := PositionV6, v7 := PositionV7},
        position_keyed := #{v6 := KeyedPositionV6}
    } = Sample,
    ?assert(BooleanV7 < BooleanV6),
    ?assert(PositionV7 < PositionV6),
    ?assert(PositionV7 < KeyedPositionV6).

test_v7_entry_size_sample() ->
    DocKey = binary:copy(<<"k">>, 82),
    Version = <<1, 2, 3, 4, 5, 6, 7, 8>>,
    Totals = lists:foldl(
        fun(DocId, Acc) ->
            Count = 1 + (DocId rem 7),
            Positions = lists:seq(0, Count - 1),
            PosBin = client_encode_positions(Positions),
            BooleanV6Body =
                <<82:16/unsigned-big, DocKey/binary, Version/binary,
                    1000:64/unsigned-big, Count:64/unsigned-big>>,
            BooleanV6 = <<
                (byte_size(BooleanV6Body)):32/unsigned-big, BooleanV6Body/binary
            >>,
            BooleanV7 = client_encode_boolean_entry(DocId, 1000, Count),
            PositionV6Body =
                <<0:8, Version/binary, (byte_size(PosBin)):16/unsigned-big,
                    PosBin/binary>>,
            PositionV6 = <<
                (byte_size(PositionV6Body)):32/unsigned-big,
                PositionV6Body/binary
            >>,
            KeyedPositionV6Body =
                <<1:8, 82:16/unsigned-big, DocKey/binary, Version/binary,
                    (byte_size(PosBin)):16/unsigned-big, PosBin/binary>>,
            KeyedPositionV6 = <<
                (byte_size(KeyedPositionV6Body)):32/unsigned-big,
                KeyedPositionV6Body/binary
            >>,
            [PositionV7Raw] = client_encode_position_entries(
                DocId, Positions
            ),
            PositionV7 = client_plane_payload_entry(
                ?POSITION_PLANE, PositionV7Raw
            ),
            Acc#{
                boolean_v6 := maps:get(boolean_v6, Acc) +
                    byte_size(BooleanV6),
                boolean_v7 := maps:get(boolean_v7, Acc) +
                    byte_size(BooleanV7),
                position_v6 := maps:get(position_v6, Acc) +
                    byte_size(PositionV6),
                keyed_position_v6 := maps:get(keyed_position_v6, Acc) +
                    byte_size(KeyedPositionV6),
                position_v7 := maps:get(position_v7, Acc) +
                    byte_size(PositionV7)
            }
        end,
        #{
            boolean_v6 => 0,
            boolean_v7 => 0,
            position_v6 => 0,
            keyed_position_v6 => 0,
            position_v7 => 0
        },
        lists:seq(1, 7379)
    ),
    #{
        documents => 7379,
        doc_key_bytes => 82,
        count_range => {1, 7},
        boolean => #{
            v6 => maps:get(boolean_v6, Totals),
            v7 => maps:get(boolean_v7, Totals)
        },
        position_unkeyed => #{
            v6 => maps:get(position_v6, Totals),
            v7 => maps:get(position_v7, Totals)
        },
        position_keyed => #{
            v6 => maps:get(keyed_position_v6, Totals),
            v7 => maps:get(position_v7, Totals)
        }
    }.

doc_id_row_lookup_direction_test_() ->
    {timeout, 60, fun doc_id_row_lookup_direction_tester/0}.

doc_id_row_lookup_direction_tester() ->
    client_with_test_bookie(fun(Bookie) ->
        Bucket = <<"doc-id-row-lookup">>,
        {ok, Schema} = schema(#{
            index => Bucket, columns => [body]
        }),
        ok = client_test_put(Bookie, Schema, <<"doc">>, <<"alpha">>),
        {ok, ManifestBin} = leveled_bookie:book_headonly(
            Bookie, Bucket, <<"doc">>, <<"doc">>
        ),
        #{doc_id := DocId, version := Version} =
            client_decode_manifest_value(ManifestBin),
        {ok, IdRow} = leveled_bookie:book_headonly(
            Bookie, Bucket, <<"id">>, client_doc_id_subkey(DocId)
        ),
        ?assertEqual({<<"doc">>, Version}, client_decode_doc_id_row(IdRow)),
        ?assertMatch({ok, [_]}, search(Bookie, Schema, <<"alpha">>, #{})),
        {ok, #{skipped := []}} = consolidate(Bookie, Schema, #{}),
        {ok, ConsolidatedManifestBin} = leveled_bookie:book_headonly(
            Bookie, Bucket, <<"doc">>, <<"doc">>
        ),
        #{doc_id := DocId} =
            client_decode_manifest_value(ConsolidatedManifestBin),
        {ok, ConsolidatedIdRow} = leveled_bookie:book_headonly(
            Bookie, Bucket, <<"id">>, client_doc_id_subkey(DocId)
        ),
        ?assertEqual(
            {<<"doc">>, Version},
            client_decode_doc_id_row(ConsolidatedIdRow)
        ),
        ?assertEqual(
            not_found,
            leveled_bookie:book_headonly(
                Bookie, Bucket, <<"id">>, <<"counter">>
            )
        )
    end).

search_skips_manifest_fold_test_() ->
    {timeout, 60, fun search_skips_manifest_fold_tester/0}.

search_skips_manifest_fold_tester() ->
    client_with_test_bookie(fun(Bookie) ->
        Bucket = <<"no-manifest-search-prelude">>,
        {ok, Schema} = schema(#{index => Bucket, columns => [body]}),
        ok = client_test_put(Bookie, Schema, <<"doc">>, <<"alpha">>),
        {ok, #{skipped := []}} = consolidate(Bookie, Schema, #{}),
        ok = leveled_bookie:book_mput(Bookie, [
            {add, Bucket, <<"doc">>, <<"doc">>, <<0>>}
        ]),
        ?assertMatch(
            {ok, [#{key := <<"doc">>}]},
            search(Bookie, Schema, <<"alpha">>, #{})
        ),
        ?assertMatch({error, _}, search(Bookie, Schema, all_docs, #{}))
    end).

missing_id_row_backfills_ranked_window_test_() ->
    {timeout, 60, fun missing_id_row_backfills_ranked_window_tester/0}.

missing_id_row_backfills_ranked_window_tester() ->
    client_with_test_bookie(fun(Bookie) ->
        Bucket = <<"id-row-backfill">>,
        {ok, Schema} = schema(#{index => Bucket, columns => [body]}),
        [
            ok = client_test_put(Bookie, Schema, Key, <<"alpha">>)
         || Key <- [<<"a">>, <<"b">>, <<"c">>]
        ],
        {ok, #{skipped := []}} = consolidate(Bookie, Schema, #{}),
        {ok, ManifestA} = leveled_bookie:book_headonly(
            Bookie, Bucket, <<"doc">>, <<"a">>
        ),
        #{doc_id := RetiredId} = client_decode_manifest_value(ManifestA),
        ok = leveled_bookie:book_mput(Bookie, [
            client_remove_doc_id_spec(Bucket, RetiredId)
        ]),
        {ok, Hits} = search(
            Bookie, Schema, <<"alpha">>, #{rank => bm25, limit => 2}
        ),
        ?assertEqual(
            [<<"b">>, <<"c">>], lists:sort([maps:get(key, Hit) || Hit <- Hits])
        )
    end).

retired_doc_ids_exclude_stale_pages_test_() ->
    {timeout, 60, fun retired_doc_ids_exclude_stale_pages_tester/0}.

retired_doc_ids_exclude_stale_pages_tester() ->
    client_with_test_bookie(fun(Bookie) ->
        Bucket = <<"retired-doc-ids">>,
        {ok, Schema} = schema(#{index => Bucket, columns => [body]}),
        ok = client_test_put(
            Bookie, Schema, <<"doc">>, <<"alpha zulu">>
        ),
        {ok, #{skipped := []}} = consolidate(Bookie, Schema, #{}),
        {ok, OldManifestBin} = leveled_bookie:book_headonly(
            Bookie, Bucket, <<"doc">>, <<"doc">>
        ),
        OldManifest = client_decode_manifest_value(OldManifestBin),
        OldDocId = maps:get(doc_id, OldManifest),
        AlphaShard = client_shard_id(<<"alpha">>, maps:get(shards, Schema)),
        YonderShard = client_shard_id(
            <<"yonder">>, maps:get(shards, Schema)
        ),
        %% The update removes the old id row atomically while the old zulu
        %% page remains physically present until its shard consolidates.
        ok = client_test_put(
            Bookie, Schema, <<"doc">>, <<"alpha yonder">>
        ),
        {ok, NewManifestBin} = leveled_bookie:book_headonly(
            Bookie, Bucket, <<"doc">>, <<"doc">>
        ),
        NewManifest = client_decode_manifest_value(NewManifestBin),
        NewDocId = maps:get(doc_id, NewManifest),
        ?assert(NewDocId =/= OldDocId),
        ?assertEqual(
            not_found,
            leveled_bookie:book_headonly(
                Bookie, Bucket, <<"id">>, client_doc_id_subkey(OldDocId)
            )
        ),
        ?assertMatch(
            {ok, _},
            leveled_bookie:book_headonly(
                Bookie, Bucket, <<"id">>, client_doc_id_subkey(NewDocId)
            )
        ),
        ?assertMatch(
            {ok, _},
            client_read_page(
                Bookie,
                Bucket,
                client_token_key(<<"zulu">>),
                ?BOOLEAN_PLANE,
                0,
                0
            )
        ),
        ?assertEqual(
            {ok, []},
            search(
                Bookie, Schema, <<"alpha AND zulu">>, #{}
            )
        ),
        ?assertEqual({ok, []}, search(Bookie, Schema, <<"zulu">>, #{})),

        {ok, #{skipped := []}} = consolidate(
            Bookie,
            Schema,
            #{shards => lists:usort([AlphaShard, YonderShard])}
        ),
        ?assertEqual(
            {ok, []},
            search(
                Bookie, Schema, <<"alpha AND zulu">>, #{}
            )
        ),
        {ok, [Hit]} = search(Bookie, Schema, <<"alpha AND yonder">>, #{}),
        ?assertEqual(<<"doc">>, maps:get(key, Hit))
    end).

v8_cross_chunk_hot_token_test_() ->
    {timeout, 60, fun v8_cross_chunk_hot_token_tester/0}.

v8_cross_chunk_hot_token_tester() ->
    client_with_test_bookie(fun(Bookie) ->
        Bucket = <<"v7-hot-token">>,
        {ok, Schema} = schema(#{index => Bucket, columns => [body]}),
        Specs = client_dedupe_specs(
            lists:append([
                begin
                    Key = <<
                        (binary:copy(<<"k">>, 74))/binary,
                        (integer_to_binary(I))/binary
                    >>,
                    {ok, DocSpecs} = derive(
                        Schema, Key, #{body => <<"common phrase target">>}
                    ),
                    DocSpecs
                end
             || I <- lists:seq(1, 2200)
            ])
        ),
        ok = leveled_bookie:book_mput(Bookie, Specs),
        {ok, #{skipped := []}} = consolidate(Bookie, Schema, #{}),
        {ok, BooleanHead} = client_read_page(
            Bookie,
            Bucket,
            client_token_key(<<"common">>),
            ?BOOLEAN_PLANE,
            0,
            0
        ),
        ?assert(client_page_chunk_count(BooleanHead) > 1),
        {ok, Hits} = search(Bookie, Schema, <<"common">>, #{}),
        ?assertEqual(2200, length(Hits)),
        {ok, PrefixHits} = search(Bookie, Schema, <<"comm*">>, #{}),
        ?assertEqual(2200, length(PrefixHits)),
        {ok, PhraseHits} = search(
            Bookie, Schema, <<"\"common phrase\"">>, #{}
        ),
        ?assertEqual(2200, length(PhraseHits)),
        {ok, NearHits} = search(
            Bookie, Schema, <<"NEAR(common target, 1)">>, #{}
        ),
        ?assertEqual(2200, length(NearHits))
    end).

deferred_ranked_position_winners_match_eager_test_() ->
    {timeout, 60, fun deferred_ranked_position_winners_match_eager_tester/0}.

clean_posting_caches_yield_to_dirty_tail_test_() ->
    {timeout, 60, fun clean_posting_caches_yield_to_dirty_tail_tester/0}.

ranked_missing_term_is_complete_without_fallback_test_() ->
    {timeout, 60, fun ranked_missing_term_is_complete_without_fallback_tester/0}.

ranked_missing_term_is_complete_without_fallback_tester() ->
    client_with_test_bookie(fun(Bookie) ->
        Bucket = <<"ranked-missing-complete">>,
        {ok, Schema} = schema(#{index => Bucket, columns => [body]}),
        ok = client_test_put(Bookie, Schema, <<"present">>, <<"alpha">>),
        {ok, #{skipped := []}} = consolidate(Bookie, Schema, #{}),
        {ok, Prepared} = prepare(
            Schema, <<"missing">>, #{columns => [body]}
        ),
        {leveled_fts_prepared_v1, _Fingerprint, _Columns, AST} = Prepared,
        Shards = client_ast_shards(AST, Schema),
        TailSummaries = client_read_tail_summaries(Bookie, Schema, Shards),
        ?assertEqual(
            {ok, []},
            client_try_ranked_v8_direct_term(
                Bookie,
                Schema,
                AST,
                #{rank => bm25, limit => 20},
                Shards,
                TailSummaries
            )
        )
    end).

ranked_boolean_tree_matches_generic_test_() ->
    {timeout, 60, fun ranked_boolean_tree_matches_generic_tester/0}.

ranked_boolean_tree_matches_generic_tester() ->
    client_with_test_bookie(fun(Bookie) ->
        Bucket = <<"ranked-boolean-tree">>,
        {ok, Schema} = schema(#{index => Bucket, columns => [body]}),
        Docs = [
            {<<"a">>, <<"alpha gamma">>},
            {<<"b">>, <<"beta gamma">>},
            {<<"c">>, <<"alpha beta gamma">>},
            {<<"d">>, <<"gamma">>},
            {<<"e">>, <<"alpha">>},
            {<<"f">>, <<"beta">>}
        ],
        [
            ok = client_test_put(Bookie, Schema, Key, Body)
         || {Key, Body} <- Docs
        ],
        {ok, #{skipped := []}} = consolidate(Bookie, Schema, #{}),
        BaseOpts = #{
            rank => bm25,
            limit => 20,
            return_count => true,
            return_terms => true
        },
        lists:foreach(
            fun(Query) ->
                {ok, #{count := DirectCount, hits := DirectHits}} =
                    search(Bookie, Schema, Query, BaseOpts),
                {ok, #{count := GenericCount, hits := GenericHits}} =
                    search(
                        Bookie,
                        Schema,
                        Query,
                        BaseOpts#{return_positions => true}
                    ),
                ?assertEqual(GenericCount, DirectCount),
                ?assertEqual(
                    [maps:remove(positions, Hit) || Hit <- GenericHits],
                    DirectHits
                )
            end,
            [
                <<"alpha OR beta">>,
                <<"gamma AND (alpha OR beta)">>,
                <<"gamma NOT (alpha OR beta)">>
            ]
        )
    end).

clean_posting_caches_yield_to_dirty_tail_tester() ->
    client_with_test_bookie(fun(Bookie) ->
        Bucket = <<"clean-cache-dirty-tail">>,
        {ok, Schema} = schema(#{index => Bucket, columns => [body]}),
        ok = client_test_put(Bookie, Schema, <<"old">>, <<"alpha beta">>),
        {ok, #{skipped := []}} = consolidate(Bookie, Schema, #{}),

        {ok, [_OldTerm]} = search(
            Bookie, Schema, <<"alpha">>, #{rank => bm25}
        ),
        {ok, [_OldPhrase]} = search(
            Bookie, Schema, <<"\"alpha beta\"">>, #{rank => bm25}
        ),

        ok = client_test_put(
            Bookie,
            Schema,
            <<"new">>,
            <<"alpha beta alpha beta alpha beta">>
        ),

        {ok, TermHits} = search(
            Bookie, Schema, <<"alpha">>, #{rank => bm25}
        ),
        {ok, PhraseHits} = search(
            Bookie, Schema, <<"\"alpha beta\"">>, #{rank => bm25}
        ),
        ?assertEqual(
            [<<"new">>, <<"old">>],
            lists:sort([maps:get(key, Hit) || Hit <- TermHits])
        ),
        ?assertEqual(
            [<<"new">>, <<"old">>],
            lists:sort([maps:get(key, Hit) || Hit <- PhraseHits])
        )
    end).

deferred_ranked_position_winners_match_eager_tester() ->
    client_with_test_bookie(fun(Bookie) ->
        Bucket = <<"deferred-position-winners">>,
        {ok, Schema} = schema(#{index => Bucket, columns => [body]}),
        Docs = [
            {<<"a">>,
                <<"alpha alpha alpha alpha x x x x beta beta beta beta">>},
            {<<"b">>, <<"alpha beta">>},
            {<<"c">>, <<"alpha beta alpha beta">>},
            {<<"d">>, <<"alpha x beta">>}
        ],
        [
            ok = client_test_put(Bookie, Schema, Key, Body)
         || {Key, Body} <- Docs
        ],
        {ok, #{skipped := []}} = consolidate(Bookie, Schema, #{}),
        lists:foreach(
            fun(Query) ->
                {ok, Deferred} = search(
                    Bookie,
                    Schema,
                    Query,
                    #{rank => bm25, limit => 2}
                ),
                {ok, #{hits := Eager}} = search(
                    Bookie,
                    Schema,
                    Query,
                    #{rank => bm25, limit => 2, return_count => true}
                ),
                ?assertEqual(
                    [maps:get(key, Hit) || Hit <- Eager],
                    [maps:get(key, Hit) || Hit <- Deferred]
                )
            end,
            [<<"\"alpha beta\"">>, <<"NEAR(alpha beta, 0)">>]
        )
    end).

raw_near_zipper_equivalence_test() ->
    Cases = [
        {[], [], 10},
        {[1], [13], 10},
        {[1], [12], 10},
        {[1, 20, 40], [8, 32, 60], 6},
        {[0, 130, 260, 1000], [128, 261, 900], 1},
        {lists:seq(0, 200, 7), lists:seq(3, 200, 11), 4}
    ],
    lists:foreach(
        fun({PositionsA, PositionsB, Distance}) ->
            BinaryA = client_encode_positions(PositionsA),
            BinaryB = client_encode_positions(PositionsB),
            ExpectedPositions = client_direct_near_positions(
                [PositionsA, PositionsB], Distance
            ),
            ExpectedTfs = client_direct_near_tfs(
                [PositionsA, PositionsB], Distance
            ),
            Matched = client_raw_near_any(
                BinaryA, BinaryB, Distance
            ),
            ?assertEqual(ExpectedPositions =/= [], Matched),
            {TfMatched, TfA, TfB} = client_raw_near_tfs(
                BinaryA, BinaryB, Distance
            ),
            ?assertEqual(ExpectedPositions =/= [], TfMatched),
            ?assertEqual(ExpectedTfs, [TfA, TfB])
        end,
        Cases
    ).

derive_remove_update_shape_test() ->
    {ok, Schema} = schema(#{index => <<"shape-unit">>, columns => [body]}),
    {ok, Specs} = derive(Schema, <<"doc">>, #{body => <<"alpha beta">>}),
    ?assert(
        lists:any(
            fun
                ({add, <<"shape-unit">>, <<_Shard:16>>, <<"d:doc">>, _}) ->
                    true;
                (_) ->
                    false
            end,
            Specs
        )
    ),
    {add, <<"shape-unit">>, <<"doc">>, <<"doc">>, Manifest} =
        lists:keyfind(<<"doc">>, 3, Specs),
    Removes = remove(Schema, <<"doc">>, Manifest),
    ?assert(
        lists:member(
            {remove, <<"shape-unit">>, <<"doc">>, <<"doc">>, <<>>},
            Removes
        )
    ),
    ?assert(
        lists:any(
            fun
                ({add, <<"shape-unit">>, <<_Shard:16>>, <<"d:doc">>, _}) ->
                    true;
                (_) ->
                    false
            end,
            Removes
        )
    ),
    {ok, Updated} = update(Schema, <<"doc">>, #{body => <<"gamma">>}, Manifest),
    Ids = [{B, K, SK} || {_, B, K, SK, _} <- Updated],
    ?assertEqual(length(Ids), length(lists:usort(Ids))).

page_format_stamp_and_v6_rejection_test_() ->
    {timeout, 60, fun page_format_stamp_and_v6_rejection_tester/0}.

page_format_stamp_and_v6_rejection_tester() ->
    client_with_test_bookie(fun(Bookie) ->
        Bucket = <<"page-format-unit">>,
        {ok, Schema} = schema(#{index => Bucket, columns => [body]}),
        ok = client_test_put(Bookie, Schema, <<"one">>, <<"alpha">>),
        ok = client_test_put(Bookie, Schema, <<"two">>, <<"beta">>),
        {ok, #{skipped := []}} = consolidate(Bookie, Schema, #{}),

        %% Current pages round-trip and every row value carries the explicit
        %% format magic and version before its page header.
        {ok, [AlphaHit]} = search(Bookie, Schema, <<"alpha">>, #{}),
        ?assertEqual(<<"one">>, maps:get(key, AlphaHit)),
        AlphaKey = client_token_key(<<"alpha">>),
        #{0 := {AlphaPageKey, AlphaPage}} = client_read_v8_plane_pages(
            Bookie, Bucket, AlphaKey, ?BOOLEAN_PLANE, 0, [0]
        ),
        {ok,
            <<?PAGE_MAGIC:24/unsigned-big, ?PAGE_VERSION:8,
                AlphaPageRest/binary>>} =
            leveled_bookie:book_headonly(
                Bookie, Bucket, AlphaKey, AlphaPageKey
            ),

        %% A v6 page is a migration error, not
        %% an absent token and not a function-clause crash.
        ok = leveled_bookie:book_mput(Bookie, [
            {add, Bucket, AlphaKey, AlphaPageKey,
                <<?PAGE_MAGIC:24/unsigned-big, 6:8, AlphaPageRest/binary>>}
        ]),
        ?assertEqual(
            {error, {fts_page_format, 6, ?PAGE_VERSION}},
            search(Bookie, Schema, <<"alpha">>, #{})
        ),

        %% A pre-envelope v5 payload is also rejected as unstamped.
        BetaKey = client_token_key(<<"beta">>),
        #{0 := {BetaPageKey, _BetaPage}} = client_read_v8_plane_pages(
            Bookie, Bucket, BetaKey, ?BOOLEAN_PLANE, 0, [0]
        ),
        {ok,
            <<?PAGE_MAGIC:24/unsigned-big, ?PAGE_VERSION:8,
                BetaPageRest/binary>>} = leveled_bookie:book_headonly(
            Bookie, Bucket, BetaKey, BetaPageKey
        ),
        ok = leveled_bookie:book_mput(Bookie, [
            {add, Bucket, BetaKey, BetaPageKey,
                <<?PAGE_VERSION:8, BetaPageRest/binary>>}
        ]),
        ?assertEqual(
            {error, {fts_page_format, unstamped, ?PAGE_VERSION}},
            search(Bookie, Schema, <<"beta">>, #{})
        ),
        ?assertMatch(
            <<?PAGE_MAGIC:24/unsigned-big, ?PAGE_VERSION:8, _/binary>>,
            AlphaPage
        )
    end).

v7_pages_remain_readable_test_() ->
    {timeout, 60, fun v7_pages_remain_readable_tester/0}.

v7_pages_remain_readable_tester() ->
    client_with_test_bookie(fun(Bookie) ->
        Bucket = <<"v7-readable-unit">>,
        {ok, Schema} = schema(#{index => Bucket, columns => [body]}),
        Token = <<"legacy">>,
        DocKey = <<"legacy-doc">>,
        Version = <<1, 2, 3, 4, 5, 6, 7, 8>>,
        DocId = client_doc_id(DocKey, Version),
        Boolean = client_encode_boolean_entry(DocId, 3, 3),
        Positions = client_encode_positions([0, 1, 2]),
        Position = <<
            (client_encode_doc_id(DocId))/binary,
            (varint_append(byte_size(Positions), <<>>))/binary,
            Positions/binary
        >>,
        Boundaries = client_encode_page_boundaries(
            1, [{DocId, DocId, legacy}]
        ),
        BooleanPage = client_test_v7_page(
            ?BOOLEAN_PLANE, Boolean, DocId, Boundaries
        ),
        PositionPage = client_test_v7_page(
            ?POSITION_PLANE, Position, DocId, Boundaries
        ),
        ok = leveled_bookie:book_mput(Bookie, [
            {add, Bucket, client_token_key(Token),
                client_page_subkey(?BOOLEAN_PLANE, 0, 0), BooleanPage},
            {add, Bucket, client_token_key(Token),
                client_page_subkey(?POSITION_PLANE, 0, 0), PositionPage},
            client_doc_id_spec(Bucket, DocId, DocKey, Version)
        ]),
        {ok, [Hit]} = search(
            Bookie, Schema, Token, #{return_positions => true}
        ),
        ?assertEqual(DocKey, maps:get(key, Hit)),
        ?assertEqual(3, maps:get(match_count, Hit)),
        ?assertEqual(
            [0, 1, 2],
            maps:get(Token, maps:get(positions, Hit))
        )
    end).

client_test_v7_page(Plane, Entry, DocId, PageBoundaries) ->
    Directory = <<
        (client_docid_hash(DocId)):32/unsigned-big, 0:32/unsigned-big
    >>,
    <<?PAGE_MAGIC:24/unsigned-big, ?PREVIOUS_PAGE_VERSION:8, Plane:8,
        1:16/unsigned-big, 1:32/unsigned-big, 1:32/unsigned-big,
        (byte_size(Directory)):32/unsigned-big,
        (byte_size(PageBoundaries)):32/unsigned-big, Directory/binary,
        PageBoundaries/binary, Entry/binary>>.

tail_fold_interleaving_test_() ->
    {timeout, 60, fun tail_fold_interleaving_tester/0}.

tail_fold_interleaving_tester() ->
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
        {ok, Before} = search(
            Bookie,
            Schema,
            <<"common">>,
            #{tail_fold_hook => Hook}
        ),
        %% The fold is a store snapshot: the pre-write result is the exact
        %% legal outcome for the forced interleaving.  The next direct read
        %% observes the committed tail without any library cache to admit.
        ?assertEqual([<<"seed">>], [maps:get(key, H) || H <- Before]),
        {ok, After} = search(Bookie, Schema, <<"common">>, #{}),
        ?assertEqual(
            [<<"racer">>, <<"seed">>],
            [maps:get(key, H) || H <- After]
        )
    end).

bm25_true_count_at_cap_test_() ->
    {timeout, 60, fun bm25_true_count_at_cap_tester/0}.

bm25_true_count_at_cap_tester() ->
    client_with_test_bookie(fun(Bookie) ->
        {ok, Schema} = schema(#{index => <<"bm25-cap">>, columns => [body]}),
        ok = client_test_put(
            Bookie,
            Schema,
            <<"65000">>,
            binary:copy(<<"hot ">>, 65000)
        ),
        ok = client_test_put(
            Bookie,
            Schema,
            <<"70000">>,
            binary:copy(<<"hot ">>, 70000)
        ),
        {ok, [High, Low]} = search(Bookie, Schema, <<"hot">>, #{rank => bm25}),
        ?assertEqual(<<"70000">>, maps:get(key, High)),
        ?assertEqual(<<"65000">>, maps:get(key, Low)),
        ?assert(maps:get(score, High) > maps:get(score, Low))
    end).

tokenize_with_offsets_sqlite_oracle_corpus_test() ->
    Oracle = tokenizer_oracle_path(),
    {ok, [Cases]} = file:consult(Oracle),
    lists:foreach(
        fun(#{id := Id, tokenizer_opts := DefinitionOpts, doc := Text}) ->
            {ok, Schema} = schema(DefinitionOpts#{
                index => atom_to_binary(Id, utf8),
                columns => [body]
            }),
            Opts = schema_tokenizer_options(Schema),
            Expected = tokenize(Text, Opts),
            WithOffsets = tokenize_with_offsets(Text, Opts),
            ?assertEqual(
                Expected,
                [{Token, Pos} || {Token, Pos, _Offset, _Length} <- WithOffsets]
            ),
            lists:foreach(
                fun({Token, _Pos, Offset, Length}) ->
                    SourceToken = binary:part(Text, Offset, Length),
                    ?assertEqual(
                        [{Token, 0}],
                        tokenize(SourceToken, Opts#{stopwords => []})
                    )
                end,
                WithOffsets
            )
        end,
        Cases
    ).

tokenize_with_offsets_stopword_ordinals_test() ->
    {ok, Schema} = schema(#{
        index => <<"offset-stopwords">>,
        columns => [body],
        remove_diacritics => true,
        stopwords => [<<"skip">>]
    }),
    Opts = schema_tokenizer_options(Schema),
    ?assertEqual(
        [
            {<<"alpha">>, 0, 0, 5},
            {<<"cafe">>, 2, 11, 5},
            {<<"omega">>, 3, 17, 5}
        ],
        tokenize_with_offsets(<<"Alpha skip caf\xC3\xA9 omega">>, Opts)
    ),
    ?assertEqual(
        [{<<"ab\xDF\x92cd">>, 0, 0, 7}],
        tokenize_with_offsets(<<"ab", 16#F0, 16#9F, 16#92, "cd">>, Opts)
    ).

tokenizer_oracle_path() ->
    Name = "fts_sqlite_oracle_corpus.eterm",
    Candidates = [
        filename:absname(
            filename:join([
                code:lib_dir(leveled), "..", "..", "..", "..", "test", Name
            ])
        ),
        filename:absname(
            filename:join([
                filename:dirname(?FILE), "..", "test", Name
            ])
        )
    ],
    hd([Path || Path <- Candidates, filelib:is_file(Path)]).

exact_positions_pathological_scale_test_() ->
    {timeout, 60, fun exact_positions_pathological_scale_tester/0}.

exact_positions_pathological_scale_tester() ->
    client_with_test_bookie(fun(Bookie) ->
        Bucket = <<"exact-position-scale">>,
        {ok, Schema} = schema(#{index => Bucket, columns => [body]}),
        Text = <<(binary:copy(<<"hot ">>, 100000))/binary, "hot needle">>,
        ok = client_test_put(Bookie, Schema, <<"doc">>, Text),

        {ok, [TailPhrase]} = search(
            Bookie,
            Schema,
            <<"\"hot needle\"">>,
            #{return_positions => true}
        ),
        ?assertEqual(1, maps:get(match_count, TailPhrase)),
        ?assertEqual(
            [100000], maps:get(phrase, maps:get(positions, TailPhrase))
        ),
        {ok, [TailNear]} = search(
            Bookie,
            Schema,
            <<"NEAR(hot needle, 0)">>,
            #{return_positions => true}
        ),
        ?assertEqual(1, maps:get(match_count, TailNear)),
        ?assertEqual(
            [100000], maps:get(near, maps:get(positions, TailNear))
        ),
        {ok, [TailHot]} = search(
            Bookie, Schema, <<"hot">>, #{return_positions => true}
        ),
        TailHotPositions = maps:get(
            <<"hot">>, maps:get(positions, TailHot)
        ),
        ?assertEqual(100001, maps:get(match_count, TailHot)),
        ?assertEqual(?MAX_RETURN_POSITIONS, length(TailHotPositions)),
        ?assertEqual(
            lists:seq(0, ?MAX_RETURN_POSITIONS - 1), TailHotPositions
        ),

        {ok, #{skipped := []}} = consolidate(Bookie, Schema, #{}),
        {ok, PositionHead} = client_read_page(
            Bookie,
            Bucket,
            client_token_key(<<"hot">>),
            ?POSITION_PLANE,
            0,
            0
        ),
        ?assert(client_page_count(PositionHead) > 1),
        {ok, ManifestBin} = leveled_bookie:book_headonly(
            Bookie, Bucket, <<"doc">>, <<"doc">>
        ),
        #{doc_id := DocId} = client_decode_manifest_value(ManifestBin),
        #{DocId := {DocId, FullPagePositions}} =
            client_read_token_positions(
                Bookie, Schema, <<"hot">>, 0, #{DocId => true}
            ),
        ?assertEqual(100001, length(FullPagePositions)),
        ?assertEqual(100000, lists:last(FullPagePositions)),
        {ok, [PagePhrase]} = search(
            Bookie,
            Schema,
            <<"\"hot needle\"">>,
            #{return_positions => true}
        ),
        ?assertEqual(1, maps:get(match_count, PagePhrase)),
        ?assertEqual(
            [100000], maps:get(phrase, maps:get(positions, PagePhrase))
        ),
        {ok, [PageNear]} = search(
            Bookie,
            Schema,
            <<"NEAR(hot needle, 0)">>,
            #{return_positions => true}
        ),
        ?assertEqual(1, maps:get(match_count, PageNear)),
        ?assertEqual(
            [100000], maps:get(near, maps:get(positions, PageNear))
        ),
        {ok, [HotHit]} = search(
            Bookie, Schema, <<"hot">>, #{return_positions => true}
        ),
        HotPositions = maps:get(<<"hot">>, maps:get(positions, HotHit)),
        ?assertEqual(100001, maps:get(match_count, HotHit)),
        ?assertEqual(?MAX_RETURN_POSITIONS, length(HotPositions)),
        ?assertEqual(lists:seq(0, ?MAX_RETURN_POSITIONS - 1), HotPositions)
    end).

phrase_near_position_window_test_() ->
    {timeout, 60, fun phrase_near_position_window_tester/0}.

phrase_near_position_window_tester() ->
    client_with_test_bookie(fun(Bookie) ->
        {ok, Schema} = schema(#{
            index => <<"phrase-near-window">>, columns => [body]
        }),
        ok = client_test_put(
            Bookie,
            Schema,
            <<"doc">>,
            binary:copy(<<"hot needle ">>, 5000)
        ),
        {ok, #{skipped := []}} = consolidate(Bookie, Schema, #{}),
        ExpectedPrefix = lists:seq(0, 8190, 2),
        {ok, [PhraseHit]} = search(
            Bookie,
            Schema,
            <<"\"hot needle\"">>,
            #{return_positions => true}
        ),
        ?assertEqual(5000, maps:get(match_count, PhraseHit)),
        ?assertEqual(
            ExpectedPrefix,
            maps:get(phrase, maps:get(positions, PhraseHit))
        ),
        {ok, [NearHit]} = search(
            Bookie,
            Schema,
            <<"NEAR(hot needle, 0)">>,
            #{return_positions => true}
        ),
        ?assertEqual(5000, maps:get(match_count, NearHit)),
        ?assertEqual(
            ExpectedPrefix,
            maps:get(near, maps:get(positions, NearHit))
        )
    end).

posting_read_targets_only_requested_documents_test_() ->
    {timeout, 60, fun posting_read_targets_only_requested_documents_tester/0}.

posting_read_targets_only_requested_documents_tester() ->
    client_with_test_bookie(fun(Bookie) ->
        Bucket = <<"posting-read-targets">>,
        {ok, Schema} = schema(#{
            index => Bucket, columns => [body]
        }),
        ok = client_test_put(
            Bookie,
            Schema,
            <<"alpha">>,
            <<"invoice invoice agreement">>
        ),
        ok = client_test_put(Bookie, Schema, <<"beta">>, <<"invoice">>),
        ok = client_test_put(Bookie, Schema, <<"gamma">>, <<"other">>),
        {ok, _} = consolidate(Bookie, Schema, #{}),
        {ok, [Hit]} = posting_read(
            Bookie,
            Schema,
            <<"invoice">>,
            [<<"alpha">>, <<"gamma">>],
            #{columns => [body]}
        ),
        ?assertEqual(<<"alpha">>, maps:get(key, Hit)),
        ?assertEqual(2, maps:get(match_count, Hit)),
        ?assertEqual(
            #{<<"invoice">> => [0, 1]},
            maps:get(positions, Hit)
        )
    end).

boolean_posting_merge_engagement_test() ->
    Columns = {include, [body]},
    AndAST =
        {'and', {term, <<"invoice">>, false, Columns},
            {term, <<"agreement">>, false, Columns}},
    NotAST =
        {'not', {term, <<"invoice">>, false, Columns},
            {term, <<"agreement">>, false, Columns}},
    Opts = #{rank => bm25, return_positions => false},
    ?assert(client_can_direct_boolean(AndAST, Opts, false)),
    ?assert(client_can_direct_boolean(NotAST, Opts, false)),
    ?assertEqual(
        [{rare, 1, 3}, {common, 0, 300}],
        client_order_ranked_boolean_terms([
            {common, 0, 300},
            {rare, 1, 3}
        ])
    ),
    ?assertNot(
        client_can_direct_boolean(
            {'and', {term, <<"invo">>, true, Columns},
                {term, <<"agreement">>, false, Columns}},
            Opts,
            false
        )
    ).

boolean_posting_merge_ranked_semantics_test_() ->
    {timeout, 60, fun boolean_posting_merge_ranked_semantics_tester/0}.

boolean_posting_merge_ranked_semantics_tester() ->
    client_with_test_bookie(fun(Bookie) ->
        {ok, Schema} = schema(#{
            index => <<"boolean-posting-merge">>, columns => [body]
        }),
        ok = client_test_put(
            Bookie, Schema, <<"both">>, <<"invoice agreement">>
        ),
        ok = client_test_put(Bookie, Schema, <<"invoice">>, <<"invoice">>),
        ok = client_test_put(Bookie, Schema, <<"agreement">>, <<"agreement">>),
        {ok, _} = consolidate(Bookie, Schema, #{}),
        {ok, [AndHit]} = search(
            Bookie,
            Schema,
            <<"invoice agreement">>,
            #{rank => bm25, return_positions => false}
        ),
        ?assertEqual(<<"both">>, maps:get(key, AndHit)),
        ?assert(
            abs(
                maps:get(score, AndHit) -
                    client_direct_bm25_score(
                        [1, 1], [2, 2], 3, 4 / 3, 2
                    )
            ) < 1.0e-12
        ),
        {ok, [NotHit]} = search(
            Bookie,
            Schema,
            <<"invoice NOT agreement">>,
            #{rank => bm25, return_positions => false}
        ),
        ?assertEqual(<<"invoice">>, maps:get(key, NotHit)),
        ?assert(
            abs(
                maps:get(score, NotHit) -
                    client_direct_bm25_score([1], [2], 3, 4 / 3, 1)
            ) < 1.0e-12
        ),
        lists:foreach(
            fun(Query) ->
                {ok, Direct} = search(
                    Bookie,
                    Schema,
                    Query,
                    #{rank => bm25, return_positions => false}
                ),
                {ok, Generic} = search(
                    Bookie,
                    Schema,
                    Query,
                    #{rank => bm25, return_positions => true}
                ),
                ?assertEqual(
                    [maps:get(key, Hit) || Hit <- Generic],
                    [maps:get(key, Hit) || Hit <- Direct]
                )
            end,
            [<<"invoice AND agreement">>, <<"invoice NOT agreement">>]
        )
    end).

bounded_top_k_stability_test_() ->
    {timeout, 60, fun bounded_top_k_stability_tester/0}.

exact_resolution_batch_size_test() ->
    ?assertEqual(21, client_resolution_batch_size(21, 0)),
    ?assertEqual(1, client_resolution_batch_size(21, 20)),
    ?assertEqual(0, client_resolution_batch_size(21, 21)).

bm25_idf_hoist_test() ->
    Idfs = client_direct_bm25_idfs([2, 5], 12),
    ?assertEqual(2, length(Idfs)),
    ?assert(
        abs(
            client_direct_bm25_score_precomputed(
                [3, 1], Idfs, 20.0, 15
            ) -
                client_direct_bm25_score([3, 1], [2, 5], 12, 20.0, 15)
        ) < 1.0e-12
    ).

ranked_tuple_candidate_test() ->
    Candidate = client_ranked_candidate(2.5, 17, 9, 3),
    ?assertEqual(
        #{
            key => 17,
            score => 2.5,
            doc_length => 9,
            match_count => 3
        },
        client_materialize_ranked_candidate(Candidate)
    ).

constant_time_merge_cap_test() ->
    ?assertNot(client_merge_cap_reached(10, 9)),
    ?assert(client_merge_cap_reached(10, 10)),
    ?assertNot(client_merge_cap_reached(infinity, 10)).

prepared_query_ast_test() ->
    {ok, PreparedSchema} = schema(#{
        index => <<"prepared-query">>,
        columns => [body]
    }),
    {ok, Prepared} = prepare(
        PreparedSchema, <<"invoice agreement">>, #{columns => [body]}
    ),
    ?assertMatch(
        {leveled_fts_prepared_v1, _Fingerprint, [<<"body">>],
            {'and', {term, <<"invoice">>, false, [<<"body">>]},
                {term, <<"agreement">>, false, [<<"body">>]}}},
        Prepared
    ).

bounded_top_k_stability_tester() ->
    Rows = [
        #{key => <<"d">>, score => 2.0},
        #{key => <<"b">>, score => 3.0},
        #{key => <<"a">>, score => 3.0},
        #{key => <<"e">>, score => 1.0},
        #{key => <<"c">>, score => 2.0}
    ],
    FullSort = lists:sort(
        fun(A, B) ->
            {-maps:get(score, A), maps:get(key, A)} =<
                {-maps:get(score, B), maps:get(key, B)}
        end,
        Rows
    ),
    lists:foreach(
        fun(K) ->
            ?assertEqual(
                lists:sublist(FullSort, K),
                client_bounded_ranked_hits(Rows, K)
            )
        end,
        lists:seq(1, length(Rows))
    ),
    client_with_test_bookie(fun(Bookie) ->
        {ok, Schema} = schema(#{
            index => <<"bounded-top-k-stability">>, columns => [body]
        }),
        Docs = [
            {<<"a">>, <<"alpha invoice agreement">>},
            {<<"b">>, <<"alpha alpha invoice">>},
            {<<"c">>, <<"alpha agreement agreement">>},
            {<<"d">>, <<"alpha invoice agreement agreement">>},
            {<<"e">>, <<"invoice">>},
            {<<"f">>, <<"agreement">>},
            {<<"z-tie">>, <<"tieonly">>},
            {<<"a-tie">>, <<"tieonly">>},
            {<<"m-tie">>, <<"tieonly">>}
        ],
        [
            ok = client_test_put(Bookie, Schema, Key, Body)
         || {Key, Body} <- Docs
        ],
        {ok, _} = consolidate(Bookie, Schema, #{}),
        lists:foreach(
            fun(Query) ->
                {ok, Full} = search(
                    Bookie,
                    Schema,
                    Query,
                    #{rank => bm25, limit => 100}
                ),
                lists:foreach(
                    fun({Offset, Limit}) ->
                        {ok, Window} = search(
                            Bookie,
                            Schema,
                            Query,
                            #{
                                rank => bm25,
                                offset => Offset,
                                limit => Limit
                            }
                        ),
                        ?assertEqual(
                            lists:sublist(drop(Offset, Full), Limit),
                            Window
                        )
                    end,
                    [{0, 1}, {0, 3}, {1, 2}, {2, 2}]
                )
            end,
            [
                <<"alpha">>,
                <<"alpha AND invoice">>,
                <<"alpha NOT agreement">>,
                <<"tieonly">>
            ]
        )
    end).

decoded_page_value_cache_test_() ->
    {timeout, 60, fun decoded_page_value_cache_tester/0}.

decoded_page_value_cache_tester() ->
    client_with_test_bookie(fun(Bookie) ->
        {ok, Schema} = schema(#{
            index => <<"decoded-page-value-cache">>, columns => [body]
        }),
        [
            ok = client_test_put(
                Bookie,
                Schema,
                integer_to_binary(I),
                <<"common term">>
            )
         || I <- lists:seq(1, 100)
        ],
        {ok, _} = consolidate(Bookie, Schema, #{}),
        erlang:put({?MODULE, direct_page_decodes}, 0),
        {ok, _} = search(
            Bookie, Schema, <<"common">>, #{rank => none, limit => 10}
        ),
        FirstCount = erlang:get({?MODULE, direct_page_decodes}),
        ?assert(FirstCount > 0),
        {ok, _} = search(
            Bookie, Schema, <<"common">>, #{rank => none, limit => 10}
        ),
        ?assertEqual(
            FirstCount, erlang:get({?MODULE, direct_page_decodes})
        ),
        ok = client_test_put(
            Bookie, Schema, <<"1">>, <<"different term">>
        ),
        {ok, _} = consolidate(Bookie, Schema, #{}),
        {ok, #{count := 99}} = search(
            Bookie,
            Schema,
            <<"common">>,
            #{rank => none, limit => 10, return_count => true}
        ),
        ?assert(
            erlang:get({?MODULE, direct_page_decodes}) > FirstCount
        ),
        erlang:erase({?MODULE, direct_page_decodes})
    end).

single_pass_match_count_test_() ->
    {timeout, 60, fun single_pass_match_count_tester/0}.

single_pass_match_count_tester() ->
    client_with_test_bookie(fun(Bookie) ->
        {ok, Schema} = schema(#{
            index => <<"single-pass-match-count">>, columns => [body]
        }),
        ok = client_test_put(Bookie, Schema, <<"one">>, <<"needle">>),
        ok = client_test_put(Bookie, Schema, <<"two">>, <<"needle needle">>),
        ok = client_test_put(Bookie, Schema, <<"other">>, <<"haystack">>),
        {ok, _} = consolidate(Bookie, Schema, #{}),
        {ok, #{count := 2, hits := [TopHit]}} =
            search(
                Bookie,
                Schema,
                <<"needle">>,
                #{
                    rank => bm25,
                    limit => 1,
                    return_count => true,
                    return_positions => false
                }
            ),
        ?assertEqual(<<"two">>, maps:get(key, TopHit)),
        ?assertEqual(2, maps:get(match_count, TopHit)),
        {ok, EvidenceHits} =
            search(
                Bookie,
                Schema,
                <<"needle OR haystack">>,
                #{
                    rank => bm25,
                    limit => 3,
                    return_count => true,
                    return_positions => false,
                    return_terms => true
                }
            ),
        One = hd([
            Hit
         || Hit <- maps:get(hits, EvidenceHits),
            maps:get(key, Hit) =:= <<"one">>
        ]),
        ?assertEqual([<<"needle">>], maps:get(matched_terms, One))
    end).

store_direct_layout_and_stats_tail_test_() ->
    {timeout, 60, fun store_direct_layout_and_stats_tail_tester/0}.

store_direct_layout_and_stats_tail_tester() ->
    client_with_test_bookie(fun(Bookie) ->
        {ok, Schema} = schema(#{index => <<"layout-unit">>, columns => [body]}),
        ok = client_test_put(Bookie, Schema, <<"one">>, <<"alpha">>),
        ok = client_test_put(Bookie, Schema, <<"two">>, <<"beta beta">>),
        ?assertEqual({2, 3}, client_corpus_stats(Bookie, Schema)),
        {ok, #{skipped := []}} = consolidate(Bookie, Schema, #{}),
        ?assertEqual({2, 3}, client_corpus_stats(Bookie, Schema)),
        {Pages, LegacyBases, TailRows, Oversized} = client_layout_counts(
            Bookie, <<"layout-unit">>
        ),
        ?assert(Pages > 0),
        ?assertEqual(0, LegacyBases),
        ?assertEqual(0, TailRows),
        ?assertEqual(0, Oversized),
        {ok, Manifest1} = leveled_bookie:book_headonly(
            Bookie, <<"layout-unit">>, <<"doc">>, <<"one">>
        ),
        {ok, Update1} = update(
            Schema,
            <<"one">>,
            #{body => <<"alpha alpha alpha">>},
            Manifest1
        ),
        ok = leveled_bookie:book_mput(Bookie, Update1),
        ?assertEqual({2, 5}, client_corpus_stats(Bookie, Schema)),
        {ok, Manifest2} = leveled_bookie:book_headonly(
            Bookie, <<"layout-unit">>, <<"doc">>, <<"one">>
        ),
        {ok, Update2} = update(
            Schema, <<"one">>, #{body => <<"alpha alpha">>}, Manifest2
        ),
        ok = leveled_bookie:book_mput(Bookie, Update2),
        ?assertEqual({2, 4}, client_corpus_stats(Bookie, Schema)),
        {ok, ManifestTwo} = leveled_bookie:book_headonly(
            Bookie, <<"layout-unit">>, <<"doc">>, <<"two">>
        ),
        ok = leveled_bookie:book_mput(
            Bookie, remove(Schema, <<"two">>, ManifestTwo)
        ),
        ?assertEqual({1, 2}, client_corpus_stats(Bookie, Schema))
    end).

client_layout_counts(Bookie, Bucket) ->
    Fold = fun
        (
            B,
            {<<"t:", _Token/binary>>, SubKey},
            Value,
            {P, Bases, Tails, Big}
        ) when B =:= Bucket ->
            case client_decode_page_row(SubKey, Value) of
                {page, _Plane, _Column, _Page} ->
                    {
                        P + 1,
                        Bases,
                        Tails,
                        Big +
                            case byte_size(Value) > ?PAGE_MAX_BYTES of
                                true -> 1;
                                false -> 0
                            end
                    };
                not_page ->
                    {P, Bases, Tails, Big}
            end;
        (B, {_Shard, <<"base">>}, _Value, {P, Bases, Tails, Big}) when
            B =:= Bucket
        ->
            {P, Bases + 1, Tails, Big};
        (
            B,
            {_Shard, <<"d:", _Doc/binary>>},
            _Value,
            {P, Bases, Tails, Big}
        ) when B =:= Bucket ->
            {P, Bases, Tails + 1, Big};
        (_B, _K, _V, Acc) ->
            Acc
    end,
    {async, Runner} = leveled_bookie:book_headfold(
        Bookie,
        ?HEAD_TAG,
        {range, Bucket, all},
        {Fold, {0, 0, 0, 0}},
        false,
        true,
        false
    ),
    Runner().

client_test_put(Bookie, Schema, Key, Text) ->
    Bucket = maps:get(index, Schema),
    {ok, Specs} =
        case
            leveled_bookie:book_headonly(
                Bookie, Bucket, <<"doc">>, Key
            )
        of
            not_found -> derive(Schema, Key, #{body => Text});
            {ok, Manifest} -> update(Schema, Key, #{body => Text}, Manifest)
        end,
    leveled_bookie:book_mput(Bookie, Specs).

client_with_test_bookie(Fun) ->
    Root = filename:join(
        "/tmp",
        "leveled_fts_" ++
            integer_to_list(erlang:unique_integer([positive]))
    ),
    _ = os:cmd("rm -rf " ++ Root),
    {ok, Bookie} = leveled_bookie:book_start([
        {root_path, Root},
        {compression_method, none},
        {ledger_compression, none},
        {value_cache_size, 16 * 1024 * 1024}
    ]),
    try
        Fun(Bookie)
    after
        try
            leveled_bookie:book_destroy(Bookie)
        catch
            _:_ -> ok
        end
    end.

-endif.

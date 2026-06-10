%% -------- Native full-text search ---------
%%
%% FTS is implemented as ordinary secondary index rows on the indexed object.
%% There are no companion objects, hidden buckets, segments, or side stores.

-module(leveled_fts).

-include("leveled.hrl").

-export([
    normalise_indexes/1,
    has_matching_index/3,
    augment_object_changes/3,
    spec_token_entries/3,
    book_ftssearch/5,
    cached_search/5,
    search/6,
    search/7
]).

-define(VERSION, 1).
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

normalise_indexes(Indexes) when is_list(Indexes) ->
    normalise_indexes(Indexes, []);
normalise_indexes(_Indexes) ->
    {error, invalid_fts_indexes}.

normalise_indexes([], Acc) ->
    Normal = lists:reverse(Acc),
    case duplicate_search_names(Normal) of
        false -> {ok, Normal};
        true -> {error, ambiguous_fts_schema}
    end;
normalise_indexes([Index | Rest], Acc) ->
    case normalise_index_definition(Index) of
        {ok, Normal} -> normalise_indexes(Rest, [Normal | Acc]);
        {error, Reason} -> {error, Reason}
    end.

%% Two definitions sharing an index name must not be able to match the same
%% bucket, whatever their tags: searches address {Bucket, Index} without a
%% tag, and same-tag overlap would also make write-side derivation emit
%% duplicate marker and page rows under one {Index, Tag} field. Exact
%% duplicates, a prefix covering an exact bucket, and nested prefixes are
%% all rejected.
duplicate_search_names(Schemas) ->
    length(Schemas) =/= length(lists:usort(Schemas)) orelse
        overlapping_search_names(Schemas).

overlapping_search_names(Schemas) ->
    Pairs = [
        {A, B}
     || A <- Schemas,
        B <- Schemas,
        A =/= B,
        maps:get(index, A) =:= maps:get(index, B)
    ],
    lists:any(
        fun({#{bucket := BA}, #{bucket := BB}}) ->
            buckets_overlap(BA, BB)
        end,
        Pairs
    ).

buckets_overlap(Bucket, Bucket) ->
    true;
buckets_overlap({prefix, P1}, {prefix, P2}) ->
    bucket_matches({prefix, P1}, P2) orelse bucket_matches({prefix, P2}, P1);
buckets_overlap({prefix, P}, Bucket) ->
    bucket_matches({prefix, P}, Bucket);
buckets_overlap(Bucket, {prefix, P}) ->
    bucket_matches({prefix, P}, Bucket);
buckets_overlap(_BucketA, _BucketB) ->
    false.

book_ftssearch(Pid, Bucket, Index0, Query, Opts) ->
    Index = normalise_index(Index0),
    leveled_bookie:book_returnfolder(Pid, {fts_query, Bucket, Index, Query, Opts}).

%% Cheap membership check used by the Bookie write path to decide whether an
%% object's bucket/tag has any configured FTS index. Writes to non-matching
%% bucket/tag pairs skip the read-before-write entirely and behave exactly as a
%% non-FTS Leveled store.
has_matching_index(Bucket, Tag, Indexes) ->
    lists:any(
        fun(#{bucket := Bucket0, tag := Tag0}) ->
            Tag0 =:= Tag andalso bucket_matches(Bucket0, Bucket)
        end,
        Indexes
    ).

-define(FTS_PMAP_MIN, 8).
-define(FTS_CHUNKS, 16).
-define(FTS_PAGE_TARGET_BYTES, 8192).
%% Persisted page entries carry a 16-bit doc count.
-define(FTS_MAX_ENTRY_DOCS, 65535).
-define(FTS_MARKER_SEEK_MAX, 64).
-define(FTS_RESULT_CACHE_MAX, 1024).
-define(FTS_CACHE_MAX_WORDS, 33554432).

%% ----------------------------------------------------------------------------
%% Write-side derivation.
%%
%% Documents in a write batch are tokenised, grouped into token-sorted pages
%% per column, and attached to the batch as payload-bearing secondary index
%% specs alongside a per-document marker:
%%
%%   {fts_doc, Index, Tag} / doc / Key            -> <<BatchSeq:64, DocLength:32>>
%%   {fts_term, Index, Tag} / <<0, BatchSeq:64>>  -> page directory for batch
%%   {fts_term, Index, Tag} / <<1, BatchSeq:64, PageNo:16>> -> packed page
%%
%% A document's live postings are exactly those written in the batch recorded
%% by its marker; entries from older batches are filtered out at read time, so
%% updates and deletes never read or rewrite earlier postings.
%% ----------------------------------------------------------------------------

augment_object_changes(ObjectChanges, Indexes, BatchSeq) ->
    Matching =
        lists:any(
            fun({{Tag, Bucket, _Key, null}, _Obj, _SpecsTTL}) ->
                has_matching_index(Bucket, Tag, Indexes)
            end,
            ObjectChanges
        ),
    case Matching of
        false ->
            {ok, ObjectChanges};
        true ->
            %% Run the whole derivation in a short-lived coordinator process:
            %% tokenisation, merging, and page encoding allocate heavily, and
            %% doing that on the long-lived caller heap (the bookie) costs
            %% far more in garbage collection than the one result copy.
            [Result] = fts_pmap(
                fun(Cs) -> augment_matching(Cs, Indexes, BatchSeq) end,
                [ObjectChanges]
            ),
            Result
    end.

augment_matching(ObjectChanges, Indexes, BatchSeq) ->
    Flagged = flag_superseded(ObjectChanges),
            Chunks = chunk_changes(Flagged),
            Results =
                case Chunks of
                    [Single] ->
                        [derive_chunk(Single, Indexes, BatchSeq)];
                    _ ->
                        fts_pmap(
                            fun(Chunk) -> derive_chunk(Chunk, Indexes, BatchSeq) end,
                            Chunks
                        )
                end,
            Changes1 = lists:append([Cs || {Cs, _Rows} <- Results]),
            MergedRows = merge_chunk_rows([Rows || {_Cs, Rows} <- Results]),
            PageSpecsByBucket =
                maps:fold(
                    fun({Bucket, Ref}, ByCol, Acc) ->
                        Specs = pack_index_specs(Ref, ByCol, BatchSeq),
                        maps:update_with(
                            Bucket, fun(S) -> S ++ Specs end, Specs, Acc
                        )
                    end,
                    #{},
                    MergedRows
                ),
            {ok, attach_page_specs(Changes1, PageSpecsByBucket)}.

%% Within one batch, a later write to the same key supersedes earlier ones.
%% Earlier occurrences must not contribute page rows: they share the batch
%% sequence, so their tokens would wrongly read as live for the final version.
%% One reversed pass flags superseded occurrences.
flag_superseded(ObjectChanges) ->
    {Flagged, _Seen} =
        lists:foldl(
            fun({{Tag, Bucket, Key, null}, _Obj, _SpecsTTL} = Change, {Acc, Seen}) ->
                Id = {Tag, Bucket, Key},
                case sets:is_element(Id, Seen) of
                    true -> {[{Change, true} | Acc], Seen};
                    false -> {[{Change, false} | Acc], sets:add_element(Id, Seen)}
                end
            end,
            {[], sets:new()},
            lists:reverse(ObjectChanges)
        ),
    Flagged.

chunk_changes(Flagged) ->
    N = length(Flagged),
    case N >= ?FTS_PMAP_MIN of
        false ->
            [Flagged];
        true ->
            Size = max(1, (N + ?FTS_CHUNKS - 1) div ?FTS_CHUNKS),
            split_chunks(Flagged, Size)
    end.

split_chunks([], _Size) ->
    [];
split_chunks(L, Size) when length(L) =< Size ->
    [L];
split_chunks(L, Size) ->
    {Chunk, Rest} = lists:split(Size, L),
    [Chunk | split_chunks(Rest, Size)].

%% A chunk worker tokenises its documents, appends marker specs to each
%% change, and returns its page rows already sorted per (bucket, ref, column),
%% so the caller only merges a handful of sorted lists.
derive_chunk(Flagged, Indexes, BatchSeq) ->
    {ChangesRev, Flat} =
        lists:foldl(
            fun({Change, Skip}, {CsAcc, FlatAcc}) ->
                {{Tag, Bucket, Key, null} = LK, Object, {Specs, TTL}} = Change,
                Schemas =
                    [
                        Schema
                     || #{bucket := B0, tag := T0} = Schema <- Indexes,
                        T0 =:= Tag,
                        bucket_matches(B0, Bucket)
                    ],
                PerSchema = [derive_doc(Schema, Object, BatchSeq) || Schema <- Schemas],
                MarkerSpecs = [Marker || {_Ref, Marker, _Rows} <- PerSchema],
                HasMatch = Schemas =/= [],
                Change1 = {{LK, Object, {Specs ++ MarkerSpecs, TTL}}, Bucket, HasMatch},
                FlatAcc1 =
                    case Skip of
                        true ->
                            FlatAcc;
                        false ->
                            lists:foldl(
                                fun({Ref, _Marker, RowList}, A) ->
                                    BR = {Bucket, Ref},
                                    lists:foldl(
                                        fun({ColId, Token, PosBin}, A1) ->
                                            [{BR, ColId, Token, Key, PosBin} | A1]
                                        end,
                                        A,
                                        RowList
                                    )
                                end,
                                FlatAcc,
                                PerSchema
                            )
                    end,
                {[Change1 | CsAcc], FlatAcc1}
            end,
            {[], []},
            Flagged
        ),
    %% One sort orders by {Bucket, Ref}, column, token, key -- giving both the
    %% per-(bucket, ref, column) grouping and the token order pages need.
    {lists:reverse(ChangesRev), group_flat(lists:sort(Flat))}.

%% Sorted flat rows -> #{{BucketRef, ColId} => [{Token, Key, PosBin}]} with the
%% group lists in ascending order (built by prepending over the reversed
%% input).
%% Sorted flat rows -> #{{BucketRef, ColId} => RunBin} where RunBin is a
%% sorted, framed sequence of token entries built in the chunk worker:
%%   <<EntryLen:32, TokLen:16, Token, NDocs:32, Docs>>
%% with Docs as in page entries. Run binaries are large refc binaries, so
%% returning them to the coordinator does not copy the row data.
group_flat(SortedFlat) ->
    group_flat(SortedFlat, none, none, 0, <<>>, <<>>, #{}).

group_flat([], none, _Tok, _ND, _Docs, _Run, Acc) ->
    Acc;
group_flat([], GKey, Tok, ND, Docs, Run, Acc) ->
    Acc#{GKey => flush_run_entry(Tok, ND, Docs, Run)};
group_flat([{BR, ColId, Token, Key, PosBin} | Rest], GKey, Tok, ND, Docs, Run, Acc) ->
    RowKey = {BR, ColId},
    DocBin =
        <<(byte_size(Key)):16/unsigned-big, Key/binary,
            (byte_size(PosBin)):16/unsigned-big, PosBin/binary>>,
    case {RowKey, Token} of
        {GKey, Tok} ->
            group_flat(Rest, GKey, Tok, ND + 1, <<Docs/binary, DocBin/binary>>, Run, Acc);
        {GKey, _NewTok} ->
            Run1 = flush_run_entry(Tok, ND, Docs, Run),
            group_flat(Rest, GKey, Token, 1, DocBin, Run1, Acc);
        {_NewKey, _} when GKey =:= none ->
            group_flat(Rest, RowKey, Token, 1, DocBin, <<>>, Acc);
        {_NewKey, _} ->
            Acc1 = Acc#{GKey => flush_run_entry(Tok, ND, Docs, Run)},
            group_flat(Rest, RowKey, Token, 1, DocBin, <<>>, Acc1)
    end.

flush_run_entry(Token, NDocs, Docs, Run) ->
    EntryLen = 2 + byte_size(Token) + 4 + byte_size(Docs),
    <<Run/binary, EntryLen:32/unsigned-big, (byte_size(Token)):16/unsigned-big,
        Token/binary, NDocs:32/unsigned-big, Docs/binary>>.

%% Gather the chunks' runs per (bucket, ref, column); merging happens during
%% page packing.
merge_chunk_rows(ChunkMaps) ->
    lists:foldl(
        fun(ChunkMap, Acc) ->
            maps:fold(
                fun({BucketRef, ColId}, RunBin, A) ->
                    ByCol = maps:get(BucketRef, A, #{}),
                    Runs = maps:get(ColId, ByCol, []),
                    A#{BucketRef => ByCol#{ColId => [RunBin | Runs]}}
                end,
                Acc,
                ChunkMap
            )
        end,
        #{},
        ChunkMaps
    ).

derive_doc(Schema, delete, _BatchSeq) ->
    Ref = index_ref(Schema),
    {Ref, {remove, doc_field(Ref), doc}, []};
derive_doc(Schema, Object0, BatchSeq) ->
    Ref = index_ref(Schema),
    Object = maybe_decode_object(Object0, Schema),
    Fields = extract_fields(Object, maps:get(column_specs, Schema)),
    ColTerms = build_column_terms(Fields, maps:get(options, Schema)),
    DocLength =
        lists:sum([
            length(Positions)
         || {_Col, TokenPositions} <- ColTerms, {_Token, Positions} <- TokenPositions
        ]),
    Marker = {add_payload, doc_field(Ref), doc, encode_marker(BatchSeq, DocLength)},
    %% build_column_terms returns columns in schema column order, so the
    %% position is the column id used by pages and directories. Positions are
    %% encoded here so the cost lands in the parallel derive workers.
    Rows =
        [
            {ColId, Token, encode_positions(Positions)}
         || {ColId, {_Col, TokenPositions}} <-
                lists:zip(lists:seq(0, length(ColTerms) - 1), ColTerms),
            {Token, Positions} <- TokenPositions
        ],
    {Ref, Marker, Rows}.

%% Each bucket's page and directory specs are attached to that bucket's last
%% matching change, since index rows are scoped by the carrier's bucket.
attach_page_specs(Changes, PageSpecsByBucket) ->
    attach_page_specs_rev(lists:reverse(Changes), PageSpecsByBucket, []).

attach_page_specs_rev([], _PageSpecsByBucket, Acc) ->
    Acc;
attach_page_specs_rev(
    [{{LK, Obj, {Specs, TTL}}, Bucket, true} | Rest], PageSpecsByBucket, Acc
) when is_map_key(Bucket, PageSpecsByBucket) ->
    PageSpecs = maps:get(Bucket, PageSpecsByBucket),
    attach_page_specs_rev(
        Rest,
        maps:remove(Bucket, PageSpecsByBucket),
        [{LK, Obj, {Specs ++ PageSpecs, TTL}} | Acc]
    );
attach_page_specs_rev([{Change, _Bucket, _HasMatch} | Rest], PageSpecsByBucket, Acc) ->
    attach_page_specs_rev(Rest, PageSpecsByBucket, [Change | Acc]).

pack_index_specs(Ref, ByCol, BatchSeq) ->
    Pages =
        lists:append(
            [
                merge_runs_to_pages(ColId, Runs)
             || {ColId, Runs} <- lists:sort(maps:to_list(ByCol)),
                Runs =/= []
            ]
        ),
    case Pages of
        [] ->
            [];
        _ ->
            Numbered = lists:zip(lists:seq(0, length(Pages) - 1), Pages),
            Field = seg_field(Ref),
            [
                {add_payload, Field, dir_term(BatchSeq), encode_dir(Numbered)}
                | [
                    {add_payload, Field, page_term(BatchSeq, PageNo), Payload}
                 || {PageNo, {_ColId, _First, _Last, Payload}} <- Numbered
                ]
            ]
    end.

%% Fused k-way merge and page pack over the chunks' sorted run binaries. Each
%% step takes the minimum head token across runs, concatenates those runs'
%% doc fragments into one page entry, and flushes a page at roughly
%% ?FTS_PAGE_TARGET_BYTES. Only binaries are appended; row data is never
%% reconstructed as terms.
merge_runs_to_pages(ColId, Runs) ->
    Heads = [parse_run_head(Run) || Run <- Runs, Run =/= <<>>],
    merge_runs(Heads, ColId, none, [], 0, 0, []).

parse_run_head(<<EntryLen:32/unsigned-big, Rest/binary>>) ->
    <<Entry:EntryLen/binary, Tail/binary>> = Rest,
    <<TokLen:16/unsigned-big, Token:TokLen/binary, NDocs:32/unsigned-big,
        Docs/binary>> = Entry,
    {Token, NDocs, Docs, Tail}.

merge_runs([], ColId, First, Entries, _Bytes, NTok, Pages) ->
    FinalPages =
        case NTok of
            0 -> Pages;
            _ -> [finish_run_page(ColId, First, Entries, NTok) | Pages]
        end,
    lists:reverse(FinalPages);
merge_runs(Heads, ColId, First, Entries, Bytes, NTok, Pages) ->
    MinTok =
        lists:foldl(
            fun({Token, _ND, _Docs, _Tail}, Min) ->
                case Min =:= none orelse Token < Min of
                    true -> Token;
                    false -> Min
                end
            end,
            none,
            Heads
        ),
    {NDSum, DocsList, Heads1} = take_token(Heads, MinTok, 0, [], []),
    %% The persisted page entry carries a 16-bit doc count, so a token with
    %% more postings than that in one batch is split into several entries
    %% (the reader collects every entry for a token across and within
    %% pages, and split chunks partition the docs).
    Chunks =
        case NDSum =< ?FTS_MAX_ENTRY_DOCS of
            true ->
                DocsBin =
                    lists:foldl(
                        fun(Docs, Acc) -> <<Acc/binary, Docs/binary>> end,
                        <<>>,
                        DocsList
                    ),
                [{NDSum, DocsBin}];
            false ->
                split_docs(iolist_to_binary(DocsList), ?FTS_MAX_ENTRY_DOCS)
        end,
    {First1, Entries1, Bytes1, NTok1, Pages1} =
        lists:foldl(
            fun({NDocs, DocsBin}, {FirstA, EntriesA, BytesA, NTokA, PagesA}) ->
                EntryBin =
                    <<(byte_size(MinTok)):16/unsigned-big, MinTok/binary,
                        NDocs:16/unsigned-big, DocsBin/binary>>,
                EntrySize = byte_size(EntryBin),
                case
                    BytesA + EntrySize > ?FTS_PAGE_TARGET_BYTES andalso NTokA > 0
                of
                    true ->
                        {MinTok, [{MinTok, EntryBin}], EntrySize, 1, [
                            finish_run_page(ColId, FirstA, EntriesA, NTokA)
                            | PagesA
                        ]};
                    false ->
                        F =
                            case NTokA of
                                0 -> MinTok;
                                _ -> FirstA
                            end,
                        {F, [{MinTok, EntryBin} | EntriesA], BytesA + EntrySize,
                            NTokA + 1, PagesA}
                end
            end,
            {First, Entries, Bytes, NTok, Pages},
            Chunks
        ),
    merge_runs(Heads1, ColId, First1, Entries1, Bytes1, NTok1, Pages1).

%% Cut a concatenated docs binary at doc boundaries into chunks of at most
%% Cap docs each.
split_docs(DocsBin, Cap) ->
    split_docs(DocsBin, Cap, DocsBin, 0, 0, []).

split_docs(<<>>, _Cap, ChunkStart, ChunkBytes, Count, Acc) ->
    Tail =
        case Count of
            0 -> [];
            _ -> [{Count, binary_part(ChunkStart, 0, ChunkBytes)}]
        end,
    lists:reverse(Tail ++ Acc);
split_docs(
    <<KeyLen:16/unsigned-big, _Key:KeyLen/binary, PosLen:16/unsigned-big,
        _Pos:PosLen/binary, Rest/binary>> = Bin,
    Cap,
    ChunkStart,
    ChunkBytes,
    Count,
    Acc
) ->
    DocSize = 4 + KeyLen + PosLen,
    case Count + 1 of
        Cap ->
            Chunk = binary_part(ChunkStart, 0, ChunkBytes + DocSize),
            split_docs(Rest, Cap, Rest, 0, 0, [{Cap, Chunk} | Acc]);
        Count1 ->
            _ = Bin,
            split_docs(Rest, Cap, ChunkStart, ChunkBytes + DocSize, Count1, Acc)
    end.

take_token([], _Tok, NDSum, DocsList, HeadsAcc) ->
    {NDSum, lists:reverse(DocsList), lists:reverse(HeadsAcc)};
take_token([{Tok, ND, Docs, Tail} | Rest], Tok, NDSum, DocsList, HeadsAcc) ->
    HeadsAcc1 =
        case Tail of
            <<>> -> HeadsAcc;
            _ -> [parse_run_head(Tail) | HeadsAcc]
        end,
    take_token(Rest, Tok, NDSum + ND, [Docs | DocsList], HeadsAcc1);
take_token([Head | Rest], Tok, NDSum, DocsList, HeadsAcc) ->
    take_token(Rest, Tok, NDSum, DocsList, [Head | HeadsAcc]).

finish_run_page(ColId, First, EntriesRev, NTok) ->
    {Last, _LastBin} = hd(EntriesRev),
    Payload =
        lists:foldl(
            fun({_Tok, EntryBin}, Acc) -> <<Acc/binary, EntryBin/binary>> end,
            <<NTok:16/unsigned-big>>,
            lists:reverse(EntriesRev)
        ),
    {ColId, First, Last, Payload}.

fts_pmap(Fun, List) ->
    Ref = make_ref(),
    Parent = self(),
    %% Workers allocate run binaries immediately; pre-sizing the heap avoids
    %% several growth collections per worker.
    Pids =
        [
            spawn_opt(
                fun() -> Parent ! {Ref, self(), Fun(Item)} end,
                [link, {min_heap_size, 8192}]
            )
         || Item <- List
        ],
    [
        receive
            {Ref, Pid, Result} -> Result
        end
     || Pid <- Pids
    ].

%% Test/debug introspection: extract {Token, Key} pairs carried by page specs
%% for the given index name and tag (e.g. from journal key changes).
spec_token_entries(Specs, Index, Tag) ->
    Field = {fts_term, Index, Tag},
    lists:append(
        [
            [
                {Token, Key}
             || {Token, Docs} <- decode_page(Payload), {Key, _Positions} <- Docs
            ]
         || {add_payload, Field0, <<1:8, _Rest/binary>>, Payload} <- Specs,
            Field0 =:= Field
        ]
    ).

seg_field({Index, Tag}) ->
    {fts_term, Index, Tag}.

search(FoldSource, Bucket, Index, Query, Opts0, Indexes) ->
    search(FoldSource, Bucket, Index, Query, Opts0, Indexes, undefined).

%% Pre-snapshot probe of the result cache, so cache hits skip snapshot setup.
cached_search(undefined, _Bucket, _Index, _Query, _Opts0) ->
    miss;
cached_search({Ets, Seq}, Bucket, Index, Query, Opts0) ->
    ResKey = {res, Bucket, Index, result_query_key(Query), Opts0, Seq},
    case ets:lookup(Ets, ResKey) of
        [{_K, Result}] -> {ok, Result};
        [] -> miss
    end.

%% Cache is undefined or {EtsTable, WriteSeq}. All cached artefacts are either
%% immutable for the lifetime of the store instance (page directories, page
%% entries) or keyed by the FTS write sequence (batch lists, query results),
%% which changes on every FTS write, so cached data is always exact.
search(FoldSource, Bucket, Index, Query, Opts0, Indexes, Cache) ->
    case Cache of
        {Ets, Seq} ->
            ResKey = {res, Bucket, Index, result_query_key(Query), Opts0, Seq},
            case ets:lookup(Ets, ResKey) of
                [{_K, Result}] ->
                    Result;
                [] ->
                    Result = search_uncached(
                        FoldSource, Bucket, Index, Query, Opts0, Indexes, Cache
                    ),
                    cache_result(Ets, ResKey, Result),
                    Result
            end;
        undefined ->
            search_uncached(FoldSource, Bucket, Index, Query, Opts0, Indexes, Cache)
    end.

result_query_key(Query) when is_binary(Query) -> Query;
result_query_key(all_docs) -> all_docs;
result_query_key(Query) ->
    try unicode:characters_to_binary(Query, utf8) of
        Bin when is_binary(Bin) -> Bin;
        _ -> Query
    catch
        _:_ -> Query
    end.

cache_result(Ets, ResKey, Result) ->
    case ets:info(Ets, size) > ?FTS_RESULT_CACHE_MAX of
        true ->
            case ets:first(Ets) of
                '$end_of_table' -> ok;
                First -> ets:delete(Ets, First)
            end;
        false ->
            ok
    end,
    ets:insert(Ets, {ResKey, Result}).

search_uncached(FoldSource, Bucket, Index, Query, Opts0, Indexes, Cache) ->
    case find_schema(Bucket, Index, Indexes) of
        {ok, Schema} ->
            IndexRef = index_ref(Schema),
            case normalise_search_options(Opts0, Schema) of
                {ok, Opts} ->
                    case parse(Query, Opts) of
                        {ok, AST0} ->
                            Columns = option_columns(Opts, Schema),
                            case validate_ast_columns(AST0, Columns) of
                                ok ->
                                    AST = restrict_ast_columns(AST0, Columns),
                                    try
                                        Metas =
                                            collect_metas(
                                                FoldSource,
                                                Bucket,
                                                Schema,
                                                AST,
                                                Columns,
                                                Cache,
                                                maps:get(return_positions, Opts, false)
                                            ),
                                        evaluate_payload_candidates(
                                            IndexRef, AST, Opts, Metas
                                        )
                                    catch
                                        throw:{fts_error, Reason} ->
                                            {error, Reason}
                                    end;
                                {error, Reason} ->
                                    {error, Reason}
                            end;
                        {error, Reason} ->
                            {error, Reason}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        not_found ->
            {error, missing_fts_schema};
        {error, Reason} ->
            {error, Reason}
    end.

normalise_index_definition(#{bucket_prefix := Prefix} = Def) when
    is_binary(Prefix), Prefix =/= <<>>, not is_map_key(bucket, Def)
->
    normalise_index_definition(
        maps:put(bucket, {prefix, Prefix}, maps:remove(bucket_prefix, Def))
    );
normalise_index_definition(#{bucket_prefix := _Prefix} = _Def) ->
    {error, invalid_fts_index};
normalise_index_definition(#{decode := Decode} = _Def) when
    Decode =/= external_term
->
    {error, invalid_fts_decode_option};
normalise_index_definition(#{bucket := Bucket, index := Index0, columns := Columns0} = Def) ->
    Opts = normalise_options(Def),
    case normalise_column_specs(Columns0) of
        {ok, ColumnSpecs} ->
            Columns = [Column || {Column, _Path} <- ColumnSpecs],
            Index = normalise_index(Index0),
            {ok, #{
                bucket => Bucket,
                tag => maps:get(tag, Def, ?STD_TAG),
                index => Index,
                columns => Columns,
                column_specs => ColumnSpecs,
                prefixes => maps:get(prefixes, Opts, []),
                tokenizer => tokenizer_description(Opts),
                options => Opts
            }};
        {error, Reason} ->
            {error, Reason}
    end;
normalise_index_definition(_Def) ->
    {error, invalid_fts_index}.

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

normalise_column_spec(#{name := Name, path := Path}) when is_list(Path) ->
    {normalise_column(Name), Path};
normalise_column_spec({Name, Path}) when is_list(Path) ->
    {normalise_column(Name), Path};
normalise_column_spec(Name) ->
    Column = normalise_column(Name),
    {Column, [Name]}.

duplicate_columns(Specs) ->
    Columns = [Column || {Column, _Path} <- Specs],
    length(Columns) =/= length(lists:usort(Columns)).

find_schema(Bucket, Index, Indexes) ->
    Matching =
        [
            Schema
         || #{bucket := Bucket0, index := Index0} = Schema <- Indexes,
            Index0 =:= Index,
            bucket_matches(Bucket0, Bucket)
        ],
    case lists:partition(fun(#{bucket := B}) -> B =:= Bucket end, Matching) of
        {[Schema], _Prefixed} ->
            {ok, Schema};
        {[], []} ->
            not_found;
        {[], Prefixed} ->
            %% Distinct prefixes matching the same bucket cannot share a
            %% length, so taking the longest prefix is deterministic.
            [{_Len, Schema} | _Rest] =
                lists:reverse(
                    lists:keysort(1, [
                        {byte_size(P), S}
                     || #{bucket := {prefix, P}} = S <- Prefixed
                    ])
                ),
            {ok, Schema};
        {_Exact, _Prefixed} ->
            {error, ambiguous_fts_schema}
    end.

%% A schema bucket is either an exact bucket term or {prefix, Prefix},
%% authored as bucket_prefix, matching every binary bucket sharing the
%% prefix (one schema covering all tenant-prefixed buckets). Postings,
%% markers and caches are still kept per actual bucket, so tenants stay
%% isolated at both write and query time.
bucket_matches({prefix, Prefix}, Bucket) when
    is_binary(Prefix), is_binary(Bucket)
->
    Size = byte_size(Prefix),
    case Bucket of
        <<Head:Size/binary, _Rest/binary>> -> Head =:= Prefix;
        _Shorter -> false
    end;
bucket_matches(Bucket, Bucket) ->
    true;
bucket_matches(_SchemaBucket, _Bucket) ->
    false.


extract_fields(Object, ColumnSpecs) ->
    [
        {Column, normalise_text(extract_path(Object, Path))}
     || {Column, Path} <- ColumnSpecs
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
        {Column, group_positions(tokenize(Text, Opts))}
     || {Column, Text} <- Fields
    ].

group_positions(Tokens) ->
    group_positions(Tokens, #{}).

group_positions([], Acc) ->
    %% Positions are appended in increasing order during tokenisation and
    %% prepended here, so each list is descending; a single reverse yields the
    %% ascending order encode_positions/1 expects (no per-token sort needed).
    [{Token, lists:reverse(Positions)} || {Token, Positions} <- maps:to_list(Acc)];
group_positions([{Token, Pos} | Rest], Acc) ->
    group_positions(Rest, maps:update_with(Token, fun(Ps) -> [Pos | Ps] end, [Pos], Acc)).

%% ----------------------------------------------------------------------------
%% Query-side reading of the packed posting representation (layout described
%% above augment_object_changes/3). Query terms are located through the page
%% directories, pages are point-read and cached for the query, and entries are
%% filtered against each document's marker batch sequence so stale postings
%% from superseded writes are invisible.
%% ----------------------------------------------------------------------------

collect_metas(FoldSource, Bucket, Schema, AST, Columns, Cache, ReturnPositions) ->
    Ctx0 = #{
        fold => FoldSource,
        bucket => Bucket,
        ref => index_ref(Schema),
        columns => maps:get(columns, Schema),
        dirs => undefined,
        cache => Cache,
        return_positions_opt => ReturnPositions,
        pages => #{}
    },
    case AST of
        {all_docs} ->
            all_doc_metas(Ctx0);
        _ ->
            term_metas(Ctx0, AST, query_terms(AST, Columns))
    end.

all_doc_metas(#{fold := FoldSource, bucket := Bucket, ref := Ref}) ->
    Fold =
        fun(_B, {_Term, Key, Payload}, Acc) ->
            case decode_marker(Payload) of
                {ok, _BatchSeq, DocLength} ->
                    Acc#{Key => empty_meta(Ref, Key, DocLength)};
                error ->
                    Acc
            end
        end,
    index_fold(
        FoldSource,
        {Bucket, null},
        {Fold, #{}},
        {doc_field(Ref), doc, doc},
        {payload, undefined}
    ).

%% Query terms are loaded in two phases. The driver terms -- a minimal set of
%% leaves that every possible match must satisfy, chosen rarest-first by
%% covering-page count -- are loaded in full. The remaining terms then only
%% need postings for the surviving candidate documents, so their page reads
%% are restricted to the batches named by the candidates' markers. This gives
%% phrase, NEAR, and AND queries over hot tokens a cost proportional to the
%% rarest leg rather than the hottest.
term_metas(_Ctx, _AST, []) ->
    #{};
term_metas(Ctx0, AST, Terms) ->
    PosNeeded =
        case maps:get(return_positions_opt, Ctx0, false) of
            true -> all;
            false -> positional_terms(AST, Ctx0)
        end,
    {Ctx1, Dirs} = load_dirs(Ctx0),
    Columns = maps:get(columns, Ctx1),
    Cost =
        fun({Col, Token, Prefix}) ->
            ColId = column_id(Columns, Col, 0),
            case Prefix of
                false -> length(covering_pages(Dirs, ColId, Token));
                true -> length(covering_prefix_pages(Dirs, ColId, Token))
            end
        end,
    Drivers = lists:usort(driver_terms(AST, Cost)),
    Rest = [T || T <- Terms, not lists:member(T, Drivers)],
    {Ctx2, RawDrivers} =
        lists:foldl(
            fun({Col, Token, Prefix} = T, {CtxA, RawA}) ->
                load_term(CtxA, Col, Token, Prefix, all, need_pos(T, PosNeeded), RawA)
            end,
            {Ctx1, #{}},
            Drivers
        ),
    case maps:size(RawDrivers) of
        0 ->
            #{};
        _ ->
            {Ctx3, Markers} = load_markers(Ctx2, maps:keys(RawDrivers)),
            DriverMetas = build_metas(RawDrivers, Markers, maps:get(ref, Ctx3)),
            case maps:size(DriverMetas) of
                0 ->
                    #{};
                _ ->
                    CandKeys = sets:from_list(maps:keys(DriverMetas)),
                    CandSeqs =
                        sets:from_list([
                            element(1, maps:get(Key, Markers))
                         || Key <- maps:keys(DriverMetas)
                        ]),
                    {_Ctx4, RawAll} =
                        lists:foldl(
                            fun({Col, Token, Prefix} = T, {CtxA, RawA}) ->
                                load_term(
                                    CtxA,
                                    Col,
                                    Token,
                                    Prefix,
                                    {CandSeqs, CandKeys},
                                    need_pos(T, PosNeeded),
                                    RawA
                                )
                            end,
                            {Ctx3, RawDrivers},
                            Rest
                        ),
                    build_metas(
                        maps:with(sets:to_list(CandKeys), RawAll),
                        Markers,
                        maps:get(ref, Ctx3)
                    )
            end
    end.

%% A minimal set of leaves such that every document matching the AST matches
%% at least one driver leaf: both sides of OR, the cheaper side of AND, the
%% left of NOT, the rarest leg of phrase/NEAR.
driver_terms({term, Token, Prefix, Cols}, _Cost) ->
    [{Col, Token, Prefix} || Col <- concrete_columns(Cols)];
driver_terms({phrase, Specs, Cols}, Cost) ->
    Legs =
        [
            [{Col, Token, Prefix} || Col <- concrete_columns(Cols)]
         || {Token, Prefix, _Offset} <- lists:ukeysort(1, Specs)
        ],
    cheapest_leg(Legs, Cost);
driver_terms({near, Items, _Distance, _Cols}, Cost) ->
    Legs = [driver_terms(Item, Cost) || Item <- Items],
    cheapest_leg([Leg || Leg <- Legs, Leg =/= []], Cost);
driver_terms({anchor, AST}, Cost) ->
    driver_terms(AST, Cost);
driver_terms({'and', A, B}, Cost) ->
    cheapest_leg([driver_terms(A, Cost), driver_terms(B, Cost)], Cost);
driver_terms({'or', A, B}, Cost) ->
    driver_terms(A, Cost) ++ driver_terms(B, Cost);
driver_terms({'not', A, _B}, Cost) ->
    driver_terms(A, Cost);
driver_terms(_Other, _Cost) ->
    [].

cheapest_leg([], _Cost) ->
    [];
cheapest_leg(Legs, Cost) ->
    {_C, Leg} =
        lists:min([
            {lists:sum([Cost(T) || T <- Leg]), Leg}
         || Leg <- Legs
        ]),
    Leg.

%% Terms that appear under phrase, NEAR, or anchor need their positions for
%% evaluation; terms in purely boolean context only need membership, so their
%% position decode is skipped (a sentinel keeps the membership check truthy).
positional_terms(AST, Ctx) ->
    Columns = maps:get(columns, Ctx),
    Sub = positional_subtrees(AST),
    sets:from_list(
        lists:append([query_terms(S, Columns) || S <- Sub])
    ).

positional_subtrees({phrase, _Specs, _Cols} = AST) -> [AST];
positional_subtrees({near, _Items, _D, _Cols} = AST) -> [AST];
positional_subtrees({anchor, A}) -> [{anchor, A}];
positional_subtrees({'and', A, B}) -> positional_subtrees(A) ++ positional_subtrees(B);
positional_subtrees({'or', A, B}) -> positional_subtrees(A) ++ positional_subtrees(B);
positional_subtrees({'not', A, B}) -> positional_subtrees(A) ++ positional_subtrees(B);
positional_subtrees(_Other) -> [].

need_pos(_Term, all) -> true;
need_pos(Term, PosNeeded) -> sets:is_element(Term, PosNeeded).

%% Raw :: #{Key => #{Column => #{Token => {BatchSeq, Positions | present}}}}
%% Restrict is `all` or {CandSeqs, CandKeys}: when restricted, only pages in
%% candidate batches are read and only candidate keys are accumulated.
load_term(Ctx0, Col, Token, Prefix, Restrict, NeedPos, Raw0) ->
    {Ctx1, Dirs} = load_dirs(Ctx0),
    ColId = column_id(maps:get(columns, Ctx1), Col, 0),
    Pages0 =
        case Prefix of
            false -> covering_pages(Dirs, ColId, Token);
            true -> covering_prefix_pages(Dirs, ColId, Token)
        end,
    Pages =
        case Restrict of
            all ->
                Pages0;
            {CandSeqs, _CandKeys} ->
                [P || {BS, _PN} = P <- Pages0, sets:is_element(BS, CandSeqs)]
        end,
    KeyFilter =
        case Restrict of
            all -> all;
            {_Seqs, CandKeys} -> CandKeys
        end,
    lists:foldl(
        fun({BatchSeq, PageNo}, {CtxA, RawA}) ->
            {CtxB, Entries} = read_page(CtxA, BatchSeq, PageNo),
            TokenEntries =
                case Prefix of
                    false ->
                        %% Oversized tokens are split into several entries,
                        %% possibly within one page; collect them all.
                        [E || {T, _DocsBin} = E <- Entries, T =:= Token];
                    true ->
                        [E || {T, _DocsBin} = E <- Entries, binary_prefix(T, Token)]
                end,
            Wanted =
                [
                    {T, extract_docs(DocsBin, KeyFilter, NeedPos)}
                 || {T, DocsBin} <- TokenEntries
                ],
            {CtxB, add_raw_entries(RawA, Col, BatchSeq, Wanted)}
        end,
        {Ctx1, Raw0},
        Pages
    ).

%% Walk a raw docs binary, decoding positions only for wanted keys and only
%% when the term is positional; other docs are skipped without decoding.
extract_docs(DocsBin, KeyFilter, NeedPos) ->
    extract_docs(DocsBin, KeyFilter, NeedPos, []).

extract_docs(<<>>, _KeyFilter, _NeedPos, Acc) ->
    lists:reverse(Acc);
extract_docs(
    <<KeyLen:16/unsigned-big, Key:KeyLen/binary, PosLen:16/unsigned-big,
        PosBin:PosLen/binary, Rest/binary>>,
    KeyFilter,
    NeedPos,
    Acc
) ->
    Wanted = KeyFilter =:= all orelse sets:is_element(Key, KeyFilter),
    case Wanted of
        false ->
            extract_docs(Rest, KeyFilter, NeedPos, Acc);
        true ->
            Value =
                case NeedPos of
                    true ->
                        case decode_positions(PosBin, 0, []) of
                            {ok, Positions} -> Positions;
                            error -> throw({fts_error, invalid_fts_payload})
                        end;
                    false ->
                        present
                end,
            extract_docs(Rest, KeyFilter, NeedPos, [{Key, Value} | Acc])
    end;
extract_docs(_Bad, _KeyFilter, _NeedPos, _Acc) ->
    throw({fts_error, invalid_fts_payload}).

add_raw_entries(Raw, Col, BatchSeq, TokenEntries) ->
    lists:foldl(
        fun({Token, Docs}, RawA) ->
            lists:foldl(
                fun({Key, Positions}, RawB) ->
                    ByCol = maps:get(Key, RawB, #{}),
                    ByTok = maps:get(Col, ByCol, #{}),
                    case maps:get(Token, ByTok, undefined) of
                        {BatchSeq0, _P} when BatchSeq0 >= BatchSeq ->
                            RawB;
                        _Stale ->
                            RawB#{
                                Key =>
                                    ByCol#{Col => ByTok#{Token => {BatchSeq, Positions}}}
                            }
                    end
                end,
                RawA,
                Docs
            )
        end,
        Raw,
        TokenEntries
    ).

build_metas(Raw, Markers, Ref) ->
    maps:fold(
        fun(Key, ByCol, Acc) ->
            case maps:get(Key, Markers, undefined) of
                undefined ->
                    Acc;
                {MarkerSeq, DocLength} ->
                    Positions =
                        maps:filtermap(
                            fun(_Col, ByTok) ->
                                Live =
                                    maps:filtermap(
                                        fun
                                            (_T, {BS, present}) when BS =:= MarkerSeq ->
                                                %% membership-only term; any
                                                %% non-empty value satisfies
                                                %% boolean evaluation
                                                {true, [0]};
                                            (_T, {BS, P}) when BS =:= MarkerSeq ->
                                                {true, P};
                                            (_T, _Stale) ->
                                                false
                                        end,
                                        ByTok
                                    ),
                                case maps:size(Live) of
                                    0 -> false;
                                    _ -> {true, Live}
                                end
                            end,
                            ByCol
                        ),
                    case maps:size(Positions) of
                        0 ->
                            Acc;
                        _ ->
                            Meta = empty_meta(Ref, Key, DocLength),
                            Acc#{Key => Meta#{positions => Positions}}
                    end
            end
        end,
        #{},
        Raw
    ).

column_id([Col | _Rest], Col, N) -> N;
column_id([_Other | Rest], Col, N) -> column_id(Rest, Col, N + 1);
column_id([], _Col, _N) -> -1.

load_dirs(#{dirs := Dirs} = Ctx) when Dirs =/= undefined ->
    {Ctx, Dirs};
load_dirs(#{fold := FoldSource, bucket := Bucket, ref := Ref, cache := Cache} = Ctx) ->
    %% The batch list is keyed by the write sequence (exact invalidation);
    %% decoded directories are immutable per batch sequence, so they are
    %% cached without invalidation. On a cold cache one term-only fold
    %% discovers the batches and one point fold per batch fetches each
    %% directory payload.
    Seqs =
        case Cache of
            {Ets, Seq} ->
                SeqsKey = {seqs, Bucket, Ref, Seq},
                case ets:lookup(Ets, SeqsKey) of
                    [{_K, CachedSeqs}] ->
                        CachedSeqs;
                    [] ->
                        Discovered = discover_seqs(FoldSource, Bucket, Ref),
                        ets:insert(Ets, {SeqsKey, Discovered}),
                        Discovered
                end;
            undefined ->
                discover_seqs(FoldSource, Bucket, Ref)
        end,
    Dirs =
        [
            {BatchSeq, resolve_dir(FoldSource, Bucket, Ref, BatchSeq, Cache)}
         || BatchSeq <- Seqs
        ],
    {Ctx#{dirs := Dirs}, Dirs}.

discover_seqs(FoldSource, Bucket, Ref) ->
    Fold =
        fun(_B, {Term, _Key}, Acc) ->
            case Term of
                <<0:8, BatchSeq:64/unsigned-big>> -> [BatchSeq | Acc];
                _Other -> Acc
            end
        end,
    lists:reverse(
        index_fold(
            FoldSource,
            {Bucket, null},
            {Fold, []},
            {seg_field(Ref), <<0:8, 0:64/unsigned-big>>,
                <<0:8, 16#FFFFFFFFFFFFFFFF:64/unsigned-big>>},
            {true, undefined}
        )
    ).

resolve_dir(FoldSource, Bucket, Ref, BatchSeq, Cache) ->
    CacheKey = {dir, Bucket, Ref, BatchSeq},
    Ets =
        case Cache of
            {Tab, _Seq} -> Tab;
            undefined -> undefined
        end,
    Cached =
        case Ets of
            undefined -> [];
            _ -> ets:lookup(Ets, CacheKey)
        end,
    case Cached of
        [{_K, ColMap}] ->
            ColMap;
        [] ->
            Term = dir_term(BatchSeq),
            Fold = fun(_B, {_Term, _Key, Payload}, _Acc) -> Payload end,
            ColMap =
                case
                    index_fold(
                        FoldSource,
                        {Bucket, null},
                        {Fold, not_found},
                        {seg_field(Ref), Term, Term},
                        {payload, undefined}
                    )
                of
                    not_found ->
                        #{};
                    Payload ->
                        case decode_dir(Payload) of
                            {ok, Entries} -> dir_col_map(Entries);
                            error -> throw({fts_error, invalid_fts_payload})
                        end
                end,
            case Ets of
                undefined -> ok;
                _ -> ets:insert(Ets, {CacheKey, ColMap})
            end,
            ColMap
    end.

%% Convert directory entries to, per column, a fixed-width probe binary plus a
%% strings blob. Pages within a column are token-ordered and disjoint, so page
%% location is a binary search over the probe rows; both parts are refc
%% binaries, so handing the structure out of the ETS cache copies no row data.
%% Probe row: <<FirstOff:32, FirstLen:16, LastOff:32, LastLen:16, PageNo:16>>.
-define(FTS_DIR_PROBE_BYTES, 14).

dir_col_map(Entries) ->
    Grouped =
        lists:foldl(
            fun({ColId, PageNo, First, Last}, Acc) ->
                maps:update_with(
                    ColId,
                    fun(L) -> [{First, Last, PageNo} | L] end,
                    [{First, Last, PageNo}],
                    Acc
                )
            end,
            #{},
            Entries
        ),
    maps:map(
        fun(_ColId, L) ->
            build_dir_probe(lists:sort(L))
        end,
        Grouped
    ).

build_dir_probe(Sorted) ->
    {Probe, Str, _Off} =
        lists:foldl(
            fun({First, Last, PageNo}, {ProbeAcc, StrAcc, Off}) ->
                FLen = byte_size(First),
                LLen = byte_size(Last),
                {
                    <<ProbeAcc/binary, Off:32/unsigned-big, FLen:16/unsigned-big,
                        (Off + FLen):32/unsigned-big, LLen:16/unsigned-big,
                        PageNo:16/unsigned-big>>,
                    <<StrAcc/binary, First/binary, Last/binary>>,
                    Off + FLen + LLen
                }
            end,
            {<<>>, <<>>, 0},
            Sorted
        ),
    {Probe, Str}.

probe_row({Probe, Str}, Idx) ->
    <<FOff:32/unsigned-big, FLen:16/unsigned-big, LOff:32/unsigned-big,
        LLen:16/unsigned-big, PageNo:16/unsigned-big>> =
        binary:part(Probe, (Idx - 1) * ?FTS_DIR_PROBE_BYTES, ?FTS_DIR_PROBE_BYTES),
    {binary:part(Str, FOff, FLen), binary:part(Str, LOff, LLen), PageNo}.

probe_size({Probe, _Str}) ->
    byte_size(Probe) div ?FTS_DIR_PROBE_BYTES.

covering_pages(Dirs, ColId, Token) ->
    %% A token split across entries spans a contiguous run of pages, so the
    %% exact probe is the range [Token, Token ++ <<0>>) over the directory.
    lists:append(
        [
            case maps:get(ColId, ColMap, undefined) of
                undefined ->
                    [];
                PS ->
                    [
                        {BatchSeq, PageNo}
                     || PageNo <-
                            pages_for_range(PS, Token, <<Token/binary, 0>>)
                    ]
            end
         || {BatchSeq, ColMap} <- Dirs
        ]
    ).

covering_prefix_pages(Dirs, ColId, Prefix) ->
    End =
        case next_prefix(Prefix) of
            {ok, Next} -> Next;
            none -> none
        end,
    lists:append(
        [
            case maps:get(ColId, ColMap, undefined) of
                undefined ->
                    [];
                PS ->
                    [
                        {BatchSeq, PageNo}
                     || PageNo <- pages_for_range(PS, Prefix, End)
                    ]
            end
         || {BatchSeq, ColMap} <- Dirs
        ]
    ).

%% Pages overlapping [Start, End): both Firsts and Lasts ascend, so the run is
%% from the first page with Last >= Start through the last page with
%% First < End (End =:= none means unbounded).
pages_for_range(PS, Start, End) ->
    Size = probe_size(PS),
    Lo = leftmost_last_geq(PS, Start, 1, Size, none),
    case Lo of
        none ->
            [];
        _ ->
            Hi =
                case End of
                    none -> Size;
                    _ -> rightmost_first_lt(PS, End, 1, Size, none)
                end,
            case Hi =:= none orelse Hi < Lo of
                true ->
                    [];
                false ->
                    [element(3, probe_row(PS, I)) || I <- lists:seq(Lo, Hi)]
            end
    end.

leftmost_last_geq(_PS, _Token, Lo, Hi, Best) when Lo > Hi ->
    Best;
leftmost_last_geq(PS, Token, Lo, Hi, Best) ->
    Mid = (Lo + Hi) div 2,
    {_First, Last, _PageNo} = probe_row(PS, Mid),
    case Last >= Token of
        true -> leftmost_last_geq(PS, Token, Lo, Mid - 1, Mid);
        false -> leftmost_last_geq(PS, Token, Mid + 1, Hi, Best)
    end.

rightmost_first_lt(_PS, _Token, Lo, Hi, Best) when Lo > Hi ->
    Best;
rightmost_first_lt(PS, Token, Lo, Hi, Best) ->
    Mid = (Lo + Hi) div 2,
    {First, _Last, _PageNo} = probe_row(PS, Mid),
    case First < Token of
        true -> rightmost_first_lt(PS, Token, Mid + 1, Hi, Mid);
        false -> rightmost_first_lt(PS, Token, Lo, Mid - 1, Best)
    end.

read_page(#{pages := Pages} = Ctx, BatchSeq, PageNo) ->
    case maps:get({BatchSeq, PageNo}, Pages, undefined) of
        undefined ->
            #{fold := FoldSource, bucket := Bucket, ref := Ref, cache := Cache} = Ctx,
            PageKey = {page, Bucket, Ref, BatchSeq, PageNo},
            Ets =
                case Cache of
                    {Tab, _Seq} -> Tab;
                    undefined -> undefined
                end,
            CachedEntries =
                case Ets of
                    undefined -> undefined;
                    _ ->
                        case ets:lookup(Ets, PageKey) of
                            [{_K, E}] -> E;
                            [] -> undefined
                        end
                end,
            Entries =
                case CachedEntries of
                    undefined ->
                        Term = page_term(BatchSeq, PageNo),
                        Fold = fun(_B, {_Term, _Key, Payload}, _Acc) -> Payload end,
                        Read =
                            case
                                index_fold(
                                    FoldSource,
                                    {Bucket, null},
                                    {Fold, not_found},
                                    {seg_field(Ref), Term, Term},
                                    {payload, undefined}
                                )
                            of
                                not_found -> [];
                                Payload -> page_entries(Payload)
                            end,
                        case
                            Ets =/= undefined andalso
                                ets:info(Ets, memory) < ?FTS_CACHE_MAX_WORDS
                        of
                            true -> ets:insert(Ets, {PageKey, Read});
                            false -> ok
                        end,
                        Read;
                    _ ->
                        CachedEntries
                end,
            {Ctx#{pages := Pages#{{BatchSeq, PageNo} => Entries}}, Entries};
        Entries ->
            {Ctx, Entries}
    end.

load_markers(#{fold := FoldSource, bucket := Bucket, ref := Ref} = Ctx, Keys) ->
    Field = doc_field(Ref),
    Markers =
        case length(Keys) =< ?FTS_MARKER_SEEK_MAX of
            true ->
                lists:foldl(
                    fun(Key, Acc) ->
                        case seek_marker(FoldSource, Bucket, Field, Key) of
                            {ok, BatchSeq, DocLength} ->
                                Acc#{Key => {BatchSeq, DocLength}};
                            not_found ->
                                Acc
                        end
                    end,
                    #{},
                    Keys
                );
            false ->
                Sorted = lists:sort(Keys),
                MinKey = hd(Sorted),
                MaxKey = lists:last(Sorted),
                KeySet = sets:from_list(Keys),
                Fold =
                    fun(_B, {_Term, Key, Payload}, Acc) ->
                        case Key > MaxKey of
                            true ->
                                throw({fts_fold_stop, Acc});
                            false ->
                                case sets:is_element(Key, KeySet) of
                                    true ->
                                        case decode_marker(Payload) of
                                            {ok, BatchSeq, DocLength} ->
                                                Acc#{Key => {BatchSeq, DocLength}};
                                            error ->
                                                Acc
                                        end;
                                    false ->
                                        Acc
                                end
                        end
                    end,
                try
                    index_fold(
                        FoldSource,
                        {Bucket, MinKey},
                        {Fold, #{}},
                        {Field, doc, doc},
                        {payload, undefined}
                    )
                catch
                    throw:{fts_fold_stop, Acc} -> Acc
                end
        end,
    {Ctx, Markers}.

seek_marker(FoldSource, Bucket, Field, Key) ->
    Fold =
        fun(_B, {_Term, Key0, Payload}, _Acc) ->
            throw({fts_fold_stop, {Key0, Payload}})
        end,
    Found =
        try
            index_fold(
                FoldSource,
                {Bucket, Key},
                {Fold, not_found},
                {Field, doc, doc},
                {payload, undefined}
            )
        catch
            throw:{fts_fold_stop, KeyPayload} -> KeyPayload
        end,
    case Found of
        {Key, Payload} ->
            case decode_marker(Payload) of
                {ok, BatchSeq, DocLength} -> {ok, BatchSeq, DocLength};
                error -> not_found
            end;
        _Other ->
            not_found
    end.

index_fold(FoldFun, BucketKey, FoldAccT, Range, TermHandling) when is_function(FoldFun, 4) ->
    FoldFun(BucketKey, FoldAccT, Range, TermHandling);
index_fold(Pid, BucketKey, FoldAccT, Range, TermHandling) when is_pid(Pid) ->
    {async, Runner} =
        leveled_bookie:book_indexfold(
            Pid, BucketKey, FoldAccT, Range, TermHandling
        ),
    Runner().



empty_meta(Index, Key, DocLength) ->
    #{
        version => ?VERSION,
        kind => fts_doc,
        key => Key,
        index => Index,
        doc_length => DocLength,
        positions => #{}
    }.

evaluate_payload_candidates(Index, AST, Opts, Metas) ->
    try
        Hits =
            lists:foldl(
                fun
                    ({_Key, #{version := ?VERSION, index := MetaIndex} = Meta}, Acc) when
                        MetaIndex =:= Index
                    ->
                        case eval(AST, Meta) of
                            {true, Positions} ->
                                case public_hit(Meta, Positions, Opts) of
                                    {ok, Hit} -> [Hit | Acc];
                                    {error, Reason} -> throw({fts_error, Reason})
                                end;
                            false ->
                                Acc
                        end;
                    (_Other, Acc) ->
                        Acc
                end,
                [],
                maps:to_list(Metas)
            ),
        Sorted = lists:sort(fun(A, B) -> maps:get(key, A) =< maps:get(key, B) end, Hits),
        {ok, hit_list_result(Sorted, Opts)}
    catch
        throw:{fts_error, Reason} -> {error, Reason}
    end.

query_terms({empty}, _Columns) ->
    [];
query_terms({all_docs}, _Columns) ->
    [];
query_terms({term, Token, Prefix, Columns}, _SearchColumns) ->
    [{Column, Token, Prefix} || Column <- concrete_columns(Columns)];
query_terms({phrase, Specs, Columns}, _SearchColumns) ->
    lists:usort([
        {Column, Token, Prefix}
     || Column <- concrete_columns(Columns),
        {Token, Prefix, _Offset} <- Specs
    ]);
query_terms({near, Items, _Distance, Columns}, SearchColumns) ->
    lists:usort(lists:append([query_terms(restrict_ast_columns(Item, Columns), SearchColumns)
        || Item <- Items]));
query_terms({anchor, AST}, Columns) ->
    query_terms(AST, Columns);
query_terms({'and', A, B}, Columns) ->
    lists:usort(query_terms(A, Columns) ++ query_terms(B, Columns));
query_terms({'or', A, B}, Columns) ->
    lists:usort(query_terms(A, Columns) ++ query_terms(B, Columns));
query_terms({'not', A, B}, Columns) ->
    lists:usort(query_terms(A, Columns) ++ query_terms(B, Columns)).

%% Marker, directory, and page codecs for the packed posting representation.
encode_marker(BatchSeq, DocLength) ->
    <<BatchSeq:64/unsigned-big, DocLength:32/unsigned-big>>.

decode_marker(<<BatchSeq:64/unsigned-big, DocLength:32/unsigned-big>>) ->
    {ok, BatchSeq, DocLength};
decode_marker(_Payload) ->
    error.

dir_term(BatchSeq) ->
    <<0:8, BatchSeq:64/unsigned-big>>.

page_term(BatchSeq, PageNo) ->
    <<1:8, BatchSeq:64/unsigned-big, PageNo:16/unsigned-big>>.

encode_dir(NumberedPages) ->
    iolist_to_binary([
        <<(length(NumberedPages)):16/unsigned-big>>,
        [
            <<ColId:8/unsigned-big, PageNo:16/unsigned-big,
                (byte_size(First)):16/unsigned-big, First/binary,
                (byte_size(Last)):16/unsigned-big, Last/binary>>
         || {PageNo, {ColId, First, Last, _Payload}} <- NumberedPages
        ]
    ]).

decode_dir(<<Count:16/unsigned-big, Rest/binary>>) ->
    decode_dir_entries(Count, Rest, []);
decode_dir(_Payload) ->
    error.

decode_dir_entries(0, <<>>, Acc) ->
    {ok, lists:reverse(Acc)};
decode_dir_entries(Count, Bin, Acc) when Count > 0 ->
    case Bin of
        <<ColId:8/unsigned-big, PageNo:16/unsigned-big, FirstLen:16/unsigned-big,
            First:FirstLen/binary, LastLen:16/unsigned-big, Last:LastLen/binary,
            Rest/binary>> ->
            decode_dir_entries(Count - 1, Rest, [{ColId, PageNo, First, Last} | Acc]);
        _Other ->
            error
    end;
decode_dir_entries(_Count, _Bin, _Acc) ->
    error.

%% Lazily parses a page payload to [{Token, DocsBin}] in token order: only
%% entry boundaries are walked; doc keys and positions stay as binary slices
%% for extract_docs/3 to decode on demand.
page_entries(<<Count:16/unsigned-big, Rest/binary>>) ->
    page_entries(Count, Rest, []);
page_entries(_Payload) ->
    throw({fts_error, invalid_fts_payload}).

page_entries(0, <<>>, Acc) ->
    lists:reverse(Acc);
page_entries(Count, Bin, Acc) when Count > 0 ->
    case Bin of
        <<TokenLen:16/unsigned-big, Token:TokenLen/binary, DocCount:16/unsigned-big,
            Rest/binary>> ->
            DocsLen = docs_byte_length(DocCount, Rest, 0),
            <<DocsBin:DocsLen/binary, Rest1/binary>> = Rest,
            page_entries(Count - 1, Rest1, [{Token, DocsBin} | Acc]);
        _Other ->
            throw({fts_error, invalid_fts_payload})
    end;
page_entries(_Count, _Bin, _Acc) ->
    throw({fts_error, invalid_fts_payload}).

docs_byte_length(0, _Bin, Len) ->
    Len;
docs_byte_length(Count, Bin, Len) ->
    case Bin of
        <<KeyLen:16/unsigned-big, _Key:KeyLen/binary, PosLen:16/unsigned-big,
            _PosBin:PosLen/binary, Rest/binary>> ->
            docs_byte_length(Count - 1, Rest, Len + 4 + KeyLen + PosLen);
        _Other ->
            throw({fts_error, invalid_fts_payload})
    end.

%% Decodes a page payload to [{Token, [{Key, Positions}]}] in token order.
%% Malformed payloads raise {fts_error, invalid_fts_payload}.
decode_page(<<Count:16/unsigned-big, Rest/binary>>) ->
    decode_page_tokens(Count, Rest, []);
decode_page(_Payload) ->
    throw({fts_error, invalid_fts_payload}).

decode_page_tokens(0, <<>>, Acc) ->
    lists:reverse(Acc);
decode_page_tokens(Count, Bin, Acc) when Count > 0 ->
    case Bin of
        <<TokenLen:16/unsigned-big, Token:TokenLen/binary, DocCount:16/unsigned-big,
            Rest/binary>> ->
            {Docs, Rest1} = decode_page_docs(DocCount, Rest, []),
            decode_page_tokens(Count - 1, Rest1, [{Token, Docs} | Acc]);
        _Other ->
            throw({fts_error, invalid_fts_payload})
    end;
decode_page_tokens(_Count, _Bin, _Acc) ->
    throw({fts_error, invalid_fts_payload}).

decode_page_docs(0, Rest, Acc) ->
    {lists:reverse(Acc), Rest};
decode_page_docs(Count, Bin, Acc) when Count > 0 ->
    case Bin of
        <<KeyLen:16/unsigned-big, Key:KeyLen/binary, PosLen:16/unsigned-big,
            PosBin:PosLen/binary, Rest/binary>> ->
            case decode_positions(PosBin, 0, []) of
                {ok, Positions} ->
                    decode_page_docs(Count - 1, Rest, [{Key, Positions} | Acc]);
                error ->
                    throw({fts_error, invalid_fts_payload})
            end;
        _Other ->
            throw({fts_error, invalid_fts_payload})
    end.
%% Positions are supplied already ascending (see group_positions/1), so no
%% sort; deltas are appended directly onto the accumulator binary.
encode_positions(Positions) ->
    encode_positions(Positions, 0, <<>>).

encode_positions([], _Last, Acc) ->
    Acc;
encode_positions([Pos | Rest], Last, Acc) ->
    encode_positions(Rest, Pos, varint_append(Pos - Last, Acc)).

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

public_hit(Meta, Positions, Opts) ->
    Base = #{
        key => maps:get(key, Meta),
        rank => 0.0,
        score => 0.0,
        doc_length => maps:get(doc_length, Meta, 0)
    },
    case maps:get(return_positions, Opts, false) of
        true ->
            case position_count(Positions) =< ?MAX_RETURN_POSITIONS of
                true -> {ok, Base#{positions => Positions}};
                false -> {error, fts_query_positions_limit_exceeded}
            end;
        false ->
            {ok, Base}
    end.

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

item_allows_column(all, _Column) ->
    true;
item_allows_column({not_columns, Columns}, Column) ->
    not lists:member(Column, schema_columns(Columns));
item_allows_column(Columns, Column) ->
    lists:member(Column, schema_columns(Columns)).

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

near_positions([First | Rest], Distance) ->
    [
        Start
     || {Start, _End} = Span <- First,
        near_position_matches([Span], Rest, Distance)
    ].

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

hit_list_result(Hits, #{result := summary}) ->
    key_summary([maps:get(key, Hit) || Hit <- Hits]);
hit_list_result(Hits, Opts) ->
    page_hits(Hits, Opts).

key_summary(Keys) ->
    {Count, Hash} = json_key_list_sha256(Keys),
    #{total_count => Count, full_keys_sha256 => Hash}.

json_key_list_sha256(Keys) ->
    Ctx0 = crypto:hash_init(sha256),
    Ctx1 = crypto:hash_update(Ctx0, <<"[">>),
    {Count, Ctx2} = json_key_items_sha256(Keys, 0, Ctx1),
    {Count, hash_hex(crypto:hash_final(crypto:hash_update(Ctx2, <<"]">>)))}.

json_key_items_sha256([], Count, Ctx) ->
    {Count, Ctx};
json_key_items_sha256([Key | Rest], 0, Ctx0) ->
    Ctx1 = crypto:hash_update(Ctx0, json_key(Key)),
    json_key_items_sha256(Rest, 1, Ctx1);
json_key_items_sha256([Key | Rest], Count, Ctx0) ->
    Ctx1 = crypto:hash_update(crypto:hash_update(Ctx0, <<",">>), json_key(Key)),
    json_key_items_sha256(Rest, Count + 1, Ctx1).

json_key(Key) ->
    %% Keys that are not valid UTF-8 (e.g. order-preserving encoded
    %% composite keys) cannot be JSON text; hash them as framed base64
    %% instead. Valid UTF-8 keys keep the JSON form, preserving the
    %% cross-engine summary hash for text-keyed corpora.
    case unicode:characters_to_binary(Key) of
        Bin when is_binary(Bin) ->
            [$", json_key_chars(Bin, []), $"];
        _NotUnicode ->
            [<<"b64:">>, base64:encode(iolist_to_binary([Key]))]
    end.

json_key_chars(<<>>, Acc) ->
    lists:reverse(Acc);
json_key_chars(<<$", Rest/binary>>, Acc) ->
    json_key_chars(Rest, [$", $\\ | Acc]);
json_key_chars(<<$\\, Rest/binary>>, Acc) ->
    json_key_chars(Rest, [$\\, $\\ | Acc]);
json_key_chars(<<$\b, Rest/binary>>, Acc) ->
    json_key_chars(Rest, [$b, $\\ | Acc]);
json_key_chars(<<$\f, Rest/binary>>, Acc) ->
    json_key_chars(Rest, [$f, $\\ | Acc]);
json_key_chars(<<$\n, Rest/binary>>, Acc) ->
    json_key_chars(Rest, [$n, $\\ | Acc]);
json_key_chars(<<$\r, Rest/binary>>, Acc) ->
    json_key_chars(Rest, [$r, $\\ | Acc]);
json_key_chars(<<$\t, Rest/binary>>, Acc) ->
    json_key_chars(Rest, [$t, $\\ | Acc]);
json_key_chars(<<C, Rest/binary>>, Acc) when C < 16#20 ->
    Hex = io_lib:format("\\u~4.16.0B", [C]),
    json_key_chars(Rest, lists:reverse(Hex) ++ Acc);
json_key_chars(<<C, Rest/binary>>, Acc) ->
    json_key_chars(Rest, [C | Acc]).

hash_hex(Hash) ->
    Hex = [
        begin
            <<Hi:4, Lo:4>> = <<Byte>>,
            [hex_digit(Hi), hex_digit(Lo)]
        end
     || <<Byte>> <= Hash
    ],
    list_to_binary(Hex).

hex_digit(N) when N < 10 ->
    $0 + N;
hex_digit(N) ->
    $a + (N - 10).

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
validate_search_option_list([{rank, _Other} | _Rest]) ->
    {error, invalid_rank_option};
validate_search_option_list([{limit, Limit} | Rest]) when is_integer(Limit), Limit >= 0 ->
    validate_search_option_list(Rest);
validate_search_option_list([{offset, Offset} | Rest]) when is_integer(Offset), Offset >= 0 ->
    validate_search_option_list(Rest);
validate_search_option_list([{return_positions, Bool} | Rest]) when is_boolean(Bool) ->
    validate_search_option_list(Rest);
validate_search_option_list([{result, summary} | Rest]) ->
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
    leveled_util:t2b(T).

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
    normalise_column_binary(leveled_util:t2b(C)).

normalise_column_binary(Bin) ->
    unicode:characters_to_binary(lower_chars(unicode_chars(Bin)), utf8).

normalise_index(I) when is_binary(I) -> I;
normalise_index(I) when is_atom(I) -> atom_to_binary(I, utf8);
normalise_index(I) when is_list(I) -> unicode:characters_to_binary(I, utf8);
normalise_index(I) -> leveled_util:t2b(I).

first_unknown([], _Known) ->
    none;
first_unknown([Item | Rest], Known) ->
    case lists:member(Item, Known) of
        true -> first_unknown(Rest, Known);
        false -> {unknown, Item}
    end.

index_ref(#{index := Index, tag := Tag}) ->
    {Index, Tag}.

doc_field({Index, Tag}) ->
    {fts_doc, Index, Tag};
doc_field(Index) ->
    {fts_doc, Index}.

tokenize(Text0, Opts) ->
    case maps:get(tokenchars, Opts, []) =:= [] andalso maps:get(separators, Opts, []) =:= [] of
        true ->
            Stopwords = maps:get(stopwords, Opts, []),
            fast_tokens(normalise_text(Text0), Opts, Stopwords, <<>>, false, 0, []);
        false ->
            tokenize_unicode(Text0, Opts)
    end.

tokenize_unicode(Text0, Opts) ->
    Text = unicode_chars(normalise_text(Text0)),
    Stopwords = maps:get(stopwords, Opts, []),
    {Tokens, Current, Pos} =
        lists:foldl(
            fun(Char, {Acc, Current, Pos}) ->
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
    case Norm =:= <<>> orelse lists:member(Norm, SW) of
        true -> {Pos + 1, Acc};
        false -> {Pos + 1, [{Norm, Pos} | Acc]}
    end.

finish_token(Acc, [], Pos, _Stopwords, _Opts) ->
    {Acc, [], Pos};
finish_token(Acc, Current, Pos, Stopwords, Opts) ->
    Token = normalise_token(unicode:characters_to_binary(lists:reverse(Current), utf8), Opts),
    case Token =:= <<>> orelse lists:member(Token, Stopwords) of
        true -> {Acc, [], Pos + 1};
        false -> {[{Token, Pos} | Acc], [], Pos + 1}
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

%% Generated from SQLite ext/fts5/fts5_unicode2.c (categories "L* N* Co").
%% Codepoint classification therefore matches SQLite FTS5 unicode61 exactly
%% and does not depend on the OTP unicode_util data version.
fts5_token_ranges() ->
    {
        48, 57, 65, 90, 97, 122, 170, 170, 178, 179, 181, 181,
        185, 186, 188, 190, 192, 214, 216, 246, 248, 705, 710, 721,
        736, 740, 748, 748, 750, 750, 880, 884, 886, 887, 890, 893,
        902, 902, 904, 906, 908, 908, 910, 929, 931, 1013, 1015, 1153,
        1162, 1319, 1329, 1366, 1369, 1369, 1377, 1415, 1488, 1514, 1520, 1522,
        1568, 1610, 1632, 1641, 1646, 1647, 1649, 1747, 1749, 1749, 1765, 1766,
        1774, 1788, 1791, 1791, 1808, 1808, 1810, 1839, 1869, 1957, 1969, 1969,
        1984, 2026, 2036, 2037, 2042, 2042, 2048, 2069, 2074, 2074, 2084, 2084,
        2088, 2088, 2112, 2136, 2208, 2208, 2210, 2220, 2308, 2361, 2365, 2365,
        2384, 2384, 2392, 2401, 2406, 2415, 2417, 2423, 2425, 2431, 2437, 2444,
        2447, 2448, 2451, 2472, 2474, 2480, 2482, 2482, 2486, 2489, 2493, 2493,
        2510, 2510, 2524, 2525, 2527, 2529, 2534, 2545, 2548, 2553, 2565, 2570,
        2575, 2576, 2579, 2600, 2602, 2608, 2610, 2611, 2613, 2614, 2616, 2617,
        2649, 2652, 2654, 2654, 2662, 2671, 2674, 2676, 2693, 2701, 2703, 2705,
        2707, 2728, 2730, 2736, 2738, 2739, 2741, 2745, 2749, 2749, 2768, 2768,
        2784, 2785, 2790, 2799, 2821, 2828, 2831, 2832, 2835, 2856, 2858, 2864,
        2866, 2867, 2869, 2873, 2877, 2877, 2908, 2909, 2911, 2913, 2918, 2927,
        2929, 2935, 2947, 2947, 2949, 2954, 2958, 2960, 2962, 2965, 2969, 2970,
        2972, 2972, 2974, 2975, 2979, 2980, 2984, 2986, 2990, 3001, 3024, 3024,
        3046, 3058, 3077, 3084, 3086, 3088, 3090, 3112, 3114, 3123, 3125, 3129,
        3133, 3133, 3160, 3161, 3168, 3169, 3174, 3183, 3192, 3198, 3205, 3212,
        3214, 3216, 3218, 3240, 3242, 3251, 3253, 3257, 3261, 3261, 3294, 3294,
        3296, 3297, 3302, 3311, 3313, 3314, 3333, 3340, 3342, 3344, 3346, 3386,
        3389, 3389, 3406, 3406, 3424, 3425, 3430, 3445, 3450, 3455, 3461, 3478,
        3482, 3505, 3507, 3515, 3517, 3517, 3520, 3526, 3585, 3632, 3634, 3635,
        3648, 3654, 3664, 3673, 3713, 3714, 3716, 3716, 3719, 3720, 3722, 3722,
        3725, 3725, 3732, 3735, 3737, 3743, 3745, 3747, 3749, 3749, 3751, 3751,
        3754, 3755, 3757, 3760, 3762, 3763, 3773, 3773, 3776, 3780, 3782, 3782,
        3792, 3801, 3804, 3807, 3840, 3840, 3872, 3891, 3904, 3911, 3913, 3948,
        3976, 3980, 4096, 4138, 4159, 4169, 4176, 4181, 4186, 4189, 4193, 4193,
        4197, 4198, 4206, 4208, 4213, 4225, 4238, 4238, 4240, 4249, 4256, 4293,
        4295, 4295, 4301, 4301, 4304, 4346, 4348, 4680, 4682, 4685, 4688, 4694,
        4696, 4696, 4698, 4701, 4704, 4744, 4746, 4749, 4752, 4784, 4786, 4789,
        4792, 4798, 4800, 4800, 4802, 4805, 4808, 4822, 4824, 4880, 4882, 4885,
        4888, 4954, 4969, 4988, 4992, 5007, 5024, 5108, 5121, 5740, 5743, 5759,
        5761, 5786, 5792, 5866, 5870, 5872, 5888, 5900, 5902, 5905, 5920, 5937,
        5952, 5969, 5984, 5996, 5998, 6000, 6016, 6067, 6103, 6103, 6108, 6108,
        6112, 6121, 6128, 6137, 6160, 6169, 6176, 6263, 6272, 6312, 6314, 6314,
        6320, 6389, 6400, 6428, 6470, 6509, 6512, 6516, 6528, 6571, 6593, 6599,
        6608, 6618, 6656, 6678, 6688, 6740, 6784, 6793, 6800, 6809, 6823, 6823,
        6917, 6963, 6981, 6987, 6992, 7001, 7043, 7072, 7086, 7141, 7168, 7203,
        7232, 7241, 7245, 7293, 7401, 7404, 7406, 7409, 7413, 7414, 7424, 7615,
        7680, 7957, 7960, 7965, 7968, 8005, 8008, 8013, 8016, 8023, 8025, 8025,
        8027, 8027, 8029, 8029, 8031, 8061, 8064, 8116, 8118, 8124, 8126, 8126,
        8130, 8132, 8134, 8140, 8144, 8147, 8150, 8155, 8160, 8172, 8178, 8180,
        8182, 8188, 8304, 8305, 8308, 8313, 8319, 8329, 8336, 8348, 8450, 8450,
        8455, 8455, 8458, 8467, 8469, 8469, 8473, 8477, 8484, 8484, 8486, 8486,
        8488, 8488, 8490, 8493, 8495, 8505, 8508, 8511, 8517, 8521, 8526, 8526,
        8528, 8585, 9312, 9371, 9450, 9471, 10102, 10131, 11264, 11310, 11312, 11358,
        11360, 11492, 11499, 11502, 11506, 11507, 11517, 11517, 11520, 11557, 11559, 11559,
        11565, 11565, 11568, 11623, 11631, 11631, 11648, 11670, 11680, 11686, 11688, 11694,
        11696, 11702, 11704, 11710, 11712, 11718, 11720, 11726, 11728, 11734, 11736, 11742,
        11823, 11823, 12293, 12295, 12321, 12329, 12337, 12341, 12344, 12348, 12353, 12438,
        12445, 12447, 12449, 12538, 12540, 12543, 12549, 12589, 12593, 12686, 12690, 12693,
        12704, 12730, 12784, 12799, 12832, 12841, 12872, 12879, 12881, 12895, 12928, 12937,
        12977, 12991, 13312, 13312, 19893, 19893, 19968, 19968, 40908, 40908, 40960, 42124,
        42192, 42237, 42240, 42508, 42512, 42539, 42560, 42606, 42623, 42647, 42656, 42735,
        42775, 42783, 42786, 42888, 42891, 42894, 42896, 42899, 42912, 42922, 43000, 43009,
        43011, 43013, 43015, 43018, 43020, 43042, 43056, 43061, 43072, 43123, 43138, 43187,
        43216, 43225, 43250, 43255, 43259, 43259, 43264, 43301, 43312, 43334, 43360, 43388,
        43396, 43442, 43471, 43481, 43520, 43560, 43584, 43586, 43588, 43595, 43600, 43609,
        43616, 43638, 43642, 43642, 43648, 43695, 43697, 43697, 43701, 43702, 43705, 43709,
        43712, 43712, 43714, 43714, 43739, 43741, 43744, 43754, 43762, 43764, 43777, 43782,
        43785, 43790, 43793, 43798, 43808, 43814, 43816, 43822, 43968, 44002, 44016, 44025,
        44032, 44032, 55203, 55203, 55216, 55238, 55243, 55291, 57344, 57344, 63743, 64109,
        64112, 64217, 64256, 64262, 64275, 64279, 64285, 64285, 64287, 64296, 64298, 64310,
        64312, 64316, 64318, 64318, 64320, 64321, 64323, 64324, 64326, 64433, 64467, 64829,
        64848, 64911, 64914, 64967, 65008, 65019, 65136, 65140, 65142, 65276, 65296, 65305,
        65313, 65338, 65345, 65370, 65382, 65470, 65474, 65479, 65482, 65487, 65490, 65495,
        65498, 65500, 65536, 65547, 65549, 65574, 65576, 65594, 65596, 65597, 65599, 65613,
        65616, 65629, 65664, 65786, 65799, 65843, 65856, 65912, 65930, 65930, 66176, 66204,
        66208, 66256, 66304, 66334, 66336, 66339, 66352, 66378, 66432, 66461, 66464, 66499,
        66504, 66511, 66513, 66517, 66560, 66717, 66720, 66729, 67584, 67589, 67592, 67592,
        67594, 67637, 67639, 67640, 67644, 67644, 67647, 67669, 67672, 67679, 67840, 67867,
        67872, 67897, 67968, 68023, 68030, 68031, 68096, 68096, 68112, 68115, 68117, 68119,
        68121, 68147, 68160, 68167, 68192, 68222, 68352, 68405, 68416, 68437, 68440, 68466,
        68472, 68479, 68608, 68680, 69216, 69246, 69635, 69687, 69714, 69743, 69763, 69807,
        69840, 69864, 69872, 69881, 69891, 69926, 69942, 69951, 70019, 70066, 70081, 70084,
        70096, 70105, 71296, 71338, 71360, 71369, 73728, 74606, 74752, 74850, 77824, 78894,
        92160, 92728, 93952, 94020, 94032, 94032, 94099, 94111, 110592, 110593, 119648, 119665,
        119808, 119892, 119894, 119964, 119966, 119967, 119970, 119970, 119973, 119974, 119977, 119980,
        119982, 119993, 119995, 119995, 119997, 120003, 120005, 120069, 120071, 120074, 120077, 120084,
        120086, 120092, 120094, 120121, 120123, 120126, 120128, 120132, 120134, 120134, 120138, 120144,
        120146, 120485, 120488, 120512, 120514, 120538, 120540, 120570, 120572, 120596, 120598, 120628,
        120630, 120654, 120656, 120686, 120688, 120712, 120714, 120744, 120746, 120770, 120772, 120779,
        120782, 120831, 126464, 126467, 126469, 126495, 126497, 126498, 126500, 126500, 126503, 126503,
        126505, 126514, 126516, 126519, 126521, 126521, 126523, 126523, 126530, 126530, 126535, 126535,
        126537, 126537, 126539, 126539, 126541, 126543, 126545, 126546, 126548, 126548, 126551, 126551,
        126553, 126553, 126555, 126555, 126557, 126557, 126559, 126559, 126561, 126562, 126564, 126564,
        126567, 126570, 126572, 126578, 126580, 126583, 126585, 126588, 126590, 126590, 126592, 126601,
        126603, 126619, 126625, 126627, 126629, 126633, 126635, 126651, 127232, 127242, 131072, 131072,
        173782, 173782, 173824, 173824, 177972, 177972, 177984, 177984, 178205, 178205, 194560, 195101,
        983040, 983040, 1048573, 1048573
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
        _ -> strip_diacritics(Lower)
    end.

lower_chars(Chars) ->
    lists:flatten([unicode_util:lowercase([Char]) || Char <- lists:flatten(Chars)]).

strip_diacritics(Token) ->
    Chars =
        lists:flatten([
            unicode_util:nfd([Char])
         || Char <- lists:flatten(unicode_chars(Token))
        ]),
    Kept =
        [
            Char
         || Char <- Chars,
            not sqlite_diacritic_mark(Char)
        ],
    unicode:characters_to_binary(Kept, utf8).

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
    bounded_query(leveled_util:t2b(Query)).

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

next_prefix(Prefix) when is_binary(Prefix) ->
    next_prefix(Prefix, byte_size(Prefix) - 1).

next_prefix(_Prefix, Pos) when Pos < 0 ->
    none;
next_prefix(Prefix, Pos) ->
    case binary:at(Prefix, Pos) of
        255 ->
            next_prefix(Prefix, Pos - 1);
        Byte ->
            {ok, <<(binary:part(Prefix, 0, Pos))/binary, (Byte + 1)>>}
    end.

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

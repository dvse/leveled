%% FTS2 immutable generation builder.
%%
%% The migration input is the canonical consolidated posting state.  The
%% builder assigns group-contiguous chunk ids, writes every generation row,
%% and publishes the root last with compare-and-swap.

-module(leveled_fts2_build).

-include("leveled.hrl").

-export([publish/5, publish_documents/3, consolidate/2, consolidate/3]).

-define(CHUNK_BITS, 12).
-define(CHUNKS_PER_GROUP, 1 bsl ?CHUNK_BITS).
-define(IDENTITY_PAGE_SHIFT, 8).
-define(IDENTITY_PAGE_SIZE, 1 bsl ?IDENTITY_PAGE_SHIFT).
-define(HEAD_WINDOW, 256).
-define(WRITE_SLICE, 192).

publish(Bookie, Schema, TokenDocs, CandidateRecords, HitRecords) when
    is_pid(Bookie),
    is_map(TokenDocs),
    is_map(CandidateRecords),
    is_map(HitRecords)
->
    publish(
        Bookie,
        Schema,
        TokenDocs,
        CandidateRecords,
        HitRecords,
        [],
        []
    ).

publish(
    Bookie,
    Schema,
    TokenDocs,
    CandidateRecords,
    HitRecords,
    CommitSpecs,
    CommitConditions
) ->
    Bucket = maps:get(index, Schema),
    Generation = generation_id(),
    Previous = current_root(Bookie, Bucket),
    SourceLengths = source_lengths(TokenDocs),
    {SourceMap, Groups} = group_sources(
        Schema, CandidateRecords, HitRecords, SourceLengths
    ),
    SelectedTotalLength = lists:sum([
        maps:get(doc_length, Source)
     || Source <- maps:values(SourceMap)
    ]),
    {TermRows, ByChunk} = build_term_rows(TokenDocs, SourceMap),
    BigramRows = build_bigram_rows(ByChunk),
    IdentitySpecs = identity_specs(Bucket, Generation, Groups),
    TermSpecs = term_specs(Bucket, Generation, TermRows),
    BigramSpecs = bigram_specs(Bucket, Generation, BigramRows),
    PlaneSpecs = TermSpecs ++ BigramSpecs,
    ok = write_slices(identity_bookie(Bookie, Schema), IdentitySpecs),
    ok = write_slices(Bookie, PlaneSpecs),
    Root = #{
        version => 1,
        generation => Generation,
        fingerprint => maps:get(fingerprint, Schema),
        chunk_bits => ?CHUNK_BITS,
        group_count => length(Groups),
        chunk_count => map_size(SourceMap),
        term_count => map_size(TermRows),
        bigram_count => map_size(BigramRows),
        identity_page_count => identity_page_count(length(Groups)),
        total_length => SelectedTotalLength,
        phrase_strategy => bigram,
        previous_generation => previous_generation(Previous)
    },
    try
        publish_root(
            Bookie, Bucket, Previous, Root, CommitSpecs, CommitConditions
        )
    catch
        Class:Reason:Stacktrace ->
            cleanup_generation(Bookie, Schema, Generation),
            erlang:raise(Class, Reason, Stacktrace)
    end,
    case previous_generation(Previous) of
        undefined -> ok;
        PreviousGeneration ->
            cleanup_generation(Bookie, Schema, PreviousGeneration)
    end,
    {ok, Root#{row_count => length(IdentitySpecs) + length(PlaneSpecs)}}.

publish_documents(Bookie, Schema, Documents) when is_map(Documents) ->
    publish_documents(Bookie, Schema, Documents, [], []).

publish_documents(Bookie, Schema, Documents, CommitSpecs, CommitConditions) when
    is_map(Documents)
->
    {TokenDocs, CandidateRecords, HitRecords} = maps:fold(
        fun(SourceId, Document, {Terms, Candidates, Hits}) ->
            Candidate = {
                maps:get(doc_key, Document),
                maps:get(doc_version, Document),
                maps:get(candidate_record, Document)
            },
            Hit = {
                maps:get(doc_key, Document),
                maps:get(doc_version, Document),
                maps:get(hit_record, Document)
            },
            Posting = maps:get(posting, Document, #{}),
            NextTerms = maps:fold(
                fun(Column, Tokens, TermAcc) ->
                    maps:fold(
                        fun(Token, Entry, Inner) ->
                            Docs = maps:get(Token, Inner, #{}),
                            TokenPosting = #{Column => #{Token => Entry}},
                            Inner#{Token => Docs#{SourceId => {
                                SourceId,
                                maps:get(doc_length, Document),
                                TokenPosting
                            }}}
                        end,
                        TermAcc,
                        Tokens
                    )
                end,
                Terms,
                Posting
            ),
            {
                NextTerms,
                Candidates#{SourceId => Candidate},
                Hits#{SourceId => Hit}
            }
        end,
        {#{}, #{}, #{}},
        Documents
    ),
    publish(
        Bookie,
        Schema,
        TokenDocs,
        CandidateRecords,
        HitRecords,
        CommitSpecs,
        CommitConditions
    ).

consolidate(Bookie, Schema) ->
    consolidate(Bookie, Schema, undefined).

consolidate(Bookie, Schema, Hook) ->
    EpochConditions = epoch_conditions(Bookie, Schema),
    Root = case leveled_fts2_search:root(Bookie, Schema) of
        {ok, ExistingRoot} -> ExistingRoot;
        not_found -> undefined
    end,
    Existing = case Root of
        undefined -> #{};
        _ -> leveled_fts2_search:export_documents(Bookie, Schema, Root)
    end,
    Deltas = read_deltas(Bookie, Schema),
    Documents = apply_deltas(Existing, Deltas),
    case Hook of
        undefined -> ok;
        Fun when is_function(Fun, 1) -> Fun({fts2, EpochConditions});
        Fun when is_function(Fun, 0) -> Fun()
    end,
    Result = publish_documents(
        Bookie,
        Schema,
        Documents,
        [
            {remove, maps:get(index, Schema), <<"stats">>, <<"dirty">>, <<>>},
            {remove, maps:get(index, Schema), <<"record-tail">>, <<"dirty">>,
                <<>>}
        ],
        EpochConditions
    ),
    remove_deltas(Bookie, maps:get(index, Schema), Deltas),
    trim_journals(Bookie, Schema),
    Result.

generation_id() ->
    <<Generation:64/unsigned-big, _/binary>> = crypto:hash(
        sha256,
        term_to_binary(
            {erlang:system_time(nanosecond), erlang:unique_integer([positive])},
            [deterministic]
        )
    ),
    Generation.

current_root(Bookie, Bucket) ->
    {Key, SubKey} = leveled_fts2_codec:root_key(),
    case leveled_bookie:book_sqn(Bookie, Bucket, {Key, SubKey}, ?HEAD_TAG) of
        not_found ->
            absent;
        {ok, SQN} ->
            case leveled_bookie:book_headonly(Bookie, Bucket, Key, SubKey) of
                {ok, Value} -> {SQN, leveled_fts2_codec:decode_root(Value)};
                not_found -> absent
            end
    end.

previous_generation(absent) -> undefined;
previous_generation({_SQN, Root}) -> maps:get(generation, Root).

publish_root(
    Bookie, Bucket, Previous, Root, CommitSpecs, CommitConditions
) ->
    {Key, SubKey} = leveled_fts2_codec:root_key(),
    Condition =
        case Previous of
            absent -> {Bucket, Key, SubKey, absent};
            {SQN, _Root} -> {Bucket, Key, SubKey, {sqn, SQN}}
        end,
    Spec = {add, Bucket, Key, SubKey, leveled_fts2_codec:encode_root(Root)},
    case
        leveled_bookie:book_casmput(
            Bookie,
            [Spec | CommitSpecs],
            [Condition | CommitConditions]
        )
    of
        ok ->
            ok;
        pause ->
            ok;
        {error, {precondition_failed, _}} ->
            erlang:error(fts2_generation_raced);
        {error, Reason} ->
            erlang:error({fts2_root_publish_failed, Reason})
    end.

epoch_conditions(Bookie, #{index := Bucket, shards := Shards}) ->
    [
        begin
            Key = <<Shard:16/unsigned-big>>,
            case leveled_bookie:book_sqn(
                Bookie, Bucket, {Key, <<"epoch">>}, ?HEAD_TAG
            ) of
                not_found -> {Bucket, Key, <<"epoch">>, absent};
                {ok, SQN} -> {Bucket, Key, <<"epoch">>, {sqn, SQN}}
            end
        end
     || Shard <- lists:seq(0, Shards - 1)
    ].

read_deltas(Bookie, #{index := Bucket}) ->
    Fold = fun
        (B, {<<"f2:d">>, <<SourceId:64/unsigned-big>>}, _Value, Acc) when
            B =:= Bucket
        ->
            [SourceId | Acc];
        (_B, _Key, _Value, Acc) ->
            Acc
    end,
    {async, Runner} = leveled_bookie:book_headfold(
        Bookie,
        ?HEAD_TAG,
        {range, Bucket, {
            {<<"f2:d">>, <<>>},
            {<<"f2:d">>, <<16#FFFFFFFFFFFFFFFF:64/unsigned-big>>}
        }},
        {Fold, []},
        false,
        true,
        false
    ),
    lists:filtermap(
        fun(SourceId) ->
            SubKey = <<SourceId:64/unsigned-big>>,
            case leveled_bookie:book_sqn(
                Bookie, Bucket, {<<"f2:d">>, SubKey}, ?HEAD_TAG
            ) of
                {ok, SQN} ->
                    case leveled_bookie:book_headonly(
                        Bookie, Bucket, <<"f2:d">>, SubKey
                    ) of
                        {ok, Value} ->
                            {true, {SourceId, SQN,
                                leveled_fts2_codec:decode(delta, Value)}};
                        not_found ->
                            false
                    end;
                not_found ->
                    false
            end
        end,
        lists:usort(Runner())
    ).

apply_deltas(Existing, Deltas) ->
    lists:foldl(
        fun({_SourceId, _SQN, Delta}, Acc) ->
            WithoutRetired = maps:without(
                maps:get(retired_ids, Delta, []), Acc
            ),
            SourceId = maps:get(source_id, Delta),
            case maps:get(status, Delta) of
                live -> WithoutRetired#{SourceId => Delta};
                remove -> maps:remove(SourceId, WithoutRetired)
            end
        end,
        Existing,
        Deltas
    ).

remove_deltas(_Bookie, _Bucket, []) ->
    ok;
remove_deltas(Bookie, Bucket, Deltas) ->
    Count = erlang:min(128, length(Deltas)),
    {Batch, Rest} = lists:split(Count, Deltas),
    Specs = [
        {remove, Bucket, <<"f2:d">>, <<SourceId:64/unsigned-big>>, <<>>}
     || {SourceId, _SQN, _Delta} <- Batch
    ],
    Conditions = [
        {Bucket, <<"f2:d">>, <<SourceId:64/unsigned-big>>, {sqn, SQN}}
     || {SourceId, SQN, _Delta} <- Batch
    ],
    case leveled_bookie:book_casmput(Bookie, Specs, Conditions) of
        ok -> remove_deltas(Bookie, Bucket, Rest);
        pause -> remove_deltas(Bookie, Bucket, Rest);
        {error, {precondition_failed, _}} ->
            remove_deltas(Bookie, Bucket, Rest);
        {error, Reason} ->
            erlang:error({fts2_delta_cleanup_failed, Reason})
    end.

source_lengths(TokenDocs) ->
    maps:fold(
        fun(_Token, Docs, Acc) ->
            maps:fold(
                fun(SourceId, {_StoredId, Length, _Posting}, Inner) ->
                    Inner#{SourceId => Length}
                end,
                Acc,
                Docs
            )
        end,
        #{},
        TokenDocs
    ).

group_sources(Schema, CandidateRecords, HitRecords, SourceLengths) ->
    GroupFields = maps:get(candidate_group_fields, Schema, []),
    VersionField = maps:get(candidate_version_field, Schema, undefined),
    Grouped0 = maps:fold(
        fun
            (_SourceId, deleted, Acc) ->
                Acc;
            (SourceId, {DocKey, DocVersion, Candidate}, Acc) ->
                case maps:find(SourceId, HitRecords) of
                    {ok, {DocKey, DocVersion, HitRecord}} ->
                        GroupKey = group_key(GroupFields, SourceId, Candidate),
                        Version = group_version(VersionField, Candidate),
                        Row = #{
                            source_id => SourceId,
                            doc_key => DocKey,
                            doc_version => DocVersion,
                            candidate_record => Candidate,
                            hit_record => HitRecord,
                            doc_length => maps:get(SourceId, SourceLengths, 0)
                        },
                        keep_group_version(GroupKey, Version, Row, Acc);
                    _ ->
                        Acc
                end
        end,
        #{},
        CandidateRecords
    ),
    OrderedGroups = lists:sort(
        fun({KeyA, _}, {KeyB, _}) ->
            term_to_binary(KeyA, [deterministic]) =<
                term_to_binary(KeyB, [deterministic])
        end,
        maps:to_list(Grouped0)
    ),
    {SourceMap, Groups, _NextGroup} = lists:foldl(
        fun({GroupKey, {_Version, Rows0}}, {Sources, Acc, GroupId}) ->
            Rows = lists:sort(
                fun(A, B) ->
                    {maps:get(doc_key, A), maps:get(source_id, A)} =<
                        {maps:get(doc_key, B), maps:get(source_id, B)}
                end,
                Rows0
            ),
            true = length(Rows) =< ?CHUNKS_PER_GROUP,
            {ChunkRows, NextSources, _Local} = lists:foldl(
                fun(Row, {ChunkAcc, SourceAcc, Local}) ->
                    ChunkId = (GroupId bsl ?CHUNK_BITS) bor Local,
                    Chunk = Row#{chunk_id => ChunkId, group_id => GroupId},
                    {
                        [Chunk | ChunkAcc],
                        SourceAcc#{maps:get(source_id, Row) => Chunk},
                        Local + 1
                    }
                end,
                {[], Sources, 0},
                Rows
            ),
            Group = #{
                group_id => GroupId,
                group_key => GroupKey,
                chunks => lists:reverse(ChunkRows)
            },
            {NextSources, [Group | Acc], GroupId + 1}
        end,
        {#{}, [], 0},
        OrderedGroups
    ),
    {SourceMap, lists:reverse(Groups)}.

group_key([], SourceId, _Candidate) ->
    {source, SourceId};
group_key(Fields, _SourceId, Candidate) ->
    {group, [maps:get(Field, Candidate, undefined) || Field <- Fields]}.

group_version(undefined, _Candidate) -> 0;
group_version(Field, Candidate) -> maps:get(Field, Candidate, 0).

keep_group_version(Key, Version, Row, Acc) ->
    case maps:find(Key, Acc) of
        error ->
            Acc#{Key => {Version, [Row]}};
        {ok, {Older, _Rows}} when Version > Older ->
            Acc#{Key => {Version, [Row]}};
        {ok, {Version, Rows}} ->
            Acc#{Key => {Version, [Row | Rows]}};
        {ok, {_Newer, _Rows}} ->
            Acc
    end.

build_term_rows(TokenDocs, SourceMap) ->
    maps:fold(
        fun(Token, Docs, {TermAcc, ChunkAcc}) ->
            maps:fold(
                fun(SourceId, {_StoredId, _Length, Posting}, {Terms, Chunks}) ->
                    case maps:find(SourceId, SourceMap) of
                        error ->
                            {Terms, Chunks};
                        {ok, Chunk} ->
                            maps:fold(
                                fun(Column, Tokens, {TA, CA}) ->
                                    case maps:find(Token, Tokens) of
                                        error ->
                                            {TA, CA};
                                        {ok, Entry} ->
                                            add_term_entry(
                                                Column,
                                                Token,
                                                Entry,
                                                Chunk,
                                                TA,
                                                CA
                                            )
                                    end
                                end,
                                {Terms, Chunks},
                                Posting
                            )
                    end
                end,
                {TermAcc, ChunkAcc},
                Docs
            )
        end,
        {#{}, #{}},
        TokenDocs
    ).

add_term_entry(Column, Token, Entry, Chunk, Terms, Chunks) ->
    ChunkId = maps:get(chunk_id, Chunk),
    GroupId = maps:get(group_id, Chunk),
    SourceId = maps:get(source_id, Chunk),
    Length = maps:get(doc_length, Chunk),
    Tf = maps:get(count, Entry),
    Positions = maps:get(positions, Entry),
    PlaneEntry = {ChunkId, GroupId, SourceId, Length, Tf},
    Key = {Column, Token},
    Row0 = maps:get(Key, Terms, #{entries => [], positions => []}),
    Row = Row0#{
        entries := [PlaneEntry | maps:get(entries, Row0)],
        positions := [{ChunkId, Positions} | maps:get(positions, Row0)]
    },
    ChunkTerms = maps:get({Column, ChunkId}, Chunks, []),
    {
        Terms#{Key => Row},
        Chunks#{
            {Column, ChunkId} => [{Token, Positions, PlaneEntry} | ChunkTerms]
        }
    }.

build_bigram_rows(ByChunk) ->
    maps:fold(
        fun({Column, _ChunkId}, Terms, Acc) ->
            PositionTokens = lists:foldl(
                fun({Token, Positions, PlaneEntry}, PosAcc) ->
                    lists:foldl(
                        fun(Pos, PA) -> PA#{Pos => {Token, PlaneEntry}} end,
                        PosAcc,
                        Positions
                    )
                end,
                #{},
                Terms
            ),
            lists:foldl(
                fun(Pos, BigramAcc) ->
                    case
                        {
                            maps:find(Pos, PositionTokens),
                            maps:find(Pos + 1, PositionTokens)
                        }
                    of
                        {{ok, {First, PlaneEntry}}, {ok, {Second, _}}} ->
                            add_bigram(
                                Column,
                                First,
                                Second,
                                Pos,
                                PlaneEntry,
                                BigramAcc
                            );
                        _ ->
                            BigramAcc
                    end
                end,
                Acc,
                lists:sort(maps:keys(PositionTokens))
            )
        end,
        #{},
        ByChunk
    ).

add_bigram(Column, First, Second, Position, PlaneEntry, Acc) ->
    Key = {Column, First, Second},
    Row0 = maps:get(Key, Acc, #{}),
    ChunkId = element(1, PlaneEntry),
    case maps:find(ChunkId, Row0) of
        error ->
            Acc#{Key => Row0#{ChunkId => {PlaneEntry, [Position]}}};
        {ok, {Existing, Positions}} ->
            Acc#{Key => Row0#{ChunkId => {Existing, [Position | Positions]}}}
    end.

identity_specs(Bucket, Generation, Groups) ->
    Pages = lists:foldl(
        fun(Group, Acc) ->
            PageNo = maps:get(group_id, Group) bsr ?IDENTITY_PAGE_SHIFT,
            Acc#{PageNo => [Group | maps:get(PageNo, Acc, [])]}
        end,
        #{},
        Groups
    ),
    Key = leveled_fts2_codec:identity_key(Generation),
    [
        {add, Bucket, Key, leveled_fts2_codec:identity_subkey(PageNo),
            leveled_fts2_codec:encode(identity, lists:reverse(PageGroups))}
     || {PageNo, PageGroups} <- lists:sort(maps:to_list(Pages))
    ].

term_specs(Bucket, Generation, TermRows) ->
    lists:append([
        term_row_specs(Bucket, Generation, Column, Token, Row)
     || {{Column, Token}, Row} <- lists:sort(maps:to_list(TermRows))
    ]).

term_row_specs(Bucket, Generation, Column, Token, Row) ->
    Entries = lists:sort(maps:get(entries, Row)),
    Positions = lists:sort(maps:get(positions, Row)),
    Header = header(Entries),
    Anchor = anchor_entries(Entries),
    {Key, _} = leveled_fts2_codec:term_key(Generation, Column, Token),
    [
        {add, Bucket, Key, <<"h">>, leveled_fts2_codec:encode(header, Header)},
        {add, Bucket, Key, <<"b">>, leveled_fts2_codec:encode_plane(Entries)},
        {add, Bucket, Key, <<"a">>, leveled_fts2_codec:encode_plane(Anchor)},
        {add, Bucket, Key, <<"p">>,
            leveled_fts2_codec:encode_positions(Positions)}
    ].

bigram_specs(Bucket, Generation, BigramRows) ->
    lists:append([
        bigram_row_specs(Bucket, Generation, Column, First, Second, ByChunk)
     || {{Column, First, Second}, ByChunk} <- lists:sort(
            maps:to_list(BigramRows)
        )
    ]).

bigram_row_specs(Bucket, Generation, Column, First, Second, ByChunk) ->
    Entries = lists:sort([
        setelement(5, PlaneEntry, length(Positions))
     || {_ChunkId, {PlaneEntry, Positions}} <- maps:to_list(ByChunk)
    ]),
    PositionEntries = lists:sort([
        {ChunkId, lists:sort(Positions)}
     || {ChunkId, {_PlaneEntry, Positions}} <- maps:to_list(ByChunk)
    ]),
    Header = header(Entries),
    {Key, _} = leveled_fts2_codec:bigram_key(
        Generation, Column, First, Second
    ),
    [
        {add, Bucket, Key, <<"h">>,
            leveled_fts2_codec:encode(bigram_header, Header)},
        {add, Bucket, Key, <<"b">>,
            leveled_fts2_codec:encode(bigram_plane, Entries)},
        {add, Bucket, Key, <<"p">>,
            leveled_fts2_codec:encode_positions(PositionEntries)}
    ].

header(Entries) ->
    Anchors = anchor_entries(Entries),
    First = lists:sublist(Anchors, ?HEAD_WINDOW),
    Champions = lists:sublist(
        lists:sort(
            fun(A, B) ->
                {-element(5, A), element(2, A), element(3, A)} =<
                    {-element(5, B), element(2, B), element(3, B)}
            end,
            Anchors
        ),
        ?HEAD_WINDOW
    ),
    #{
        group_df => length(Anchors),
        chunk_df => length(Entries),
        collection_frequency => lists:sum([element(5, E) || E <- Entries]),
        first => First,
        champions => Champions
    }.

anchor_entries(Entries) ->
    Best = lists:foldl(
        fun(Entry, Acc) ->
            GroupId = element(2, Entry),
            case maps:find(GroupId, Acc) of
                error ->
                    Acc#{GroupId => Entry};
                {ok, Existing} when element(5, Entry) > element(5, Existing) ->
                    Acc#{GroupId => Entry};
                {ok, _Existing} ->
                    Acc
            end
        end,
        #{},
        Entries
    ),
    lists:sort(
        fun(A, B) -> element(2, A) =< element(2, B) end,
        maps:values(Best)
    ).

identity_page_count(0) ->
    0;
identity_page_count(GroupCount) ->
    ((GroupCount - 1) div ?IDENTITY_PAGE_SIZE) + 1.

write_slices(_Bookie, []) ->
    ok;
write_slices(Bookie, Specs) ->
    Count = erlang:min(?WRITE_SLICE, length(Specs)),
    {Batch, Rest} = lists:split(Count, Specs),
    case leveled_bookie:book_mput(Bookie, Batch) of
        ok -> write_slices(Bookie, Rest);
        pause -> write_slices(Bookie, Rest);
        {error, Reason} -> erlang:error({fts2_generation_write_failed, Reason})
    end.

identity_bookie(Bookie, Schema) ->
    maps:get(identity_bookie, Schema, Bookie).

cleanup_generation(Bookie, Schema, Generation) ->
    Bucket = maps:get(index, Schema),
    remove_generation_rows(Bookie, Bucket, term, Generation),
    remove_generation_rows(Bookie, Bucket, bigram, Generation),
    remove_generation_rows(
        identity_bookie(Bookie, Schema), Bucket, identity, Generation
    ).

remove_generation_rows(Bookie, Bucket, Kind, Generation) ->
    {Start, Finish} = case Kind of
        term -> {<<"f2:t:">>, <<"f2:u">>};
        bigram -> {<<"f2:g:">>, <<"f2:h">>};
        identity ->
            Key = leveled_fts2_codec:identity_key(Generation),
            {Key, Key}
    end,
    Fold = fun(B, {Key, SubKey}, _Value, Acc) when B =:= Bucket ->
        case generation_key(Kind, Key, Generation) of
            true -> [{remove, Bucket, Key, SubKey, <<>>} | Acc];
            false -> Acc
        end;
        (_B, _Key, _Value, Acc) -> Acc
    end,
    {async, Runner} = leveled_bookie:book_headfold(
        Bookie,
        ?HEAD_TAG,
        {range, Bucket, {{Start, <<>>}, {Finish, <<255, 255, 255, 255>>}}},
        {Fold, []},
        false,
        true,
        false
    ),
    remove_specs(Bookie, Runner()).

generation_key(term, <<"f2:t:", Generation:64/unsigned-big, _/binary>>,
    Generation) -> true;
generation_key(bigram, <<"f2:g:", Generation:64/unsigned-big, _/binary>>,
    Generation) -> true;
generation_key(identity, <<"f2:i:", Generation:64/unsigned-big>>, Generation) ->
    true;
generation_key(_Kind, _Key, _Generation) -> false.

remove_specs(_Bookie, []) -> ok;
remove_specs(Bookie, Specs) ->
    Count = erlang:min(?WRITE_SLICE, length(Specs)),
    {Batch, Rest} = lists:split(Count, Specs),
    case leveled_bookie:book_mput(Bookie, Batch) of
        ok -> remove_specs(Bookie, Rest);
        pause -> remove_specs(Bookie, Rest);
        {error, Reason} -> erlang:error({fts2_generation_cleanup_failed, Reason})
    end.

trim_journals(Bookie, Schema) ->
    case maps:get(trim_journal, Schema, false) of
        true ->
            lists:foreach(
                fun leveled_bookie:book_trimjournal/1,
                lists:usort([Bookie, identity_bookie(Bookie, Schema)])
            );
        false ->
            ok
    end.

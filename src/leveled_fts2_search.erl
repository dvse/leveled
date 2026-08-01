%% FTS2 generation reader and chunk-grained evaluator.

-module(leveled_fts2_search).

-include("leveled.hrl").

-export([
    root/2,
    search/5,
    search_dirty/5,
    search_dirty/6,
    posting_read/6,
    posting_read_dirty/6,
    export_documents/3,
    lookup_documents/3
]).

-define(DEFAULT_LIMIT, 10000).
-define(IDENTITY_PAGE_SHIFT, 8).
-define(MAX_RETURN_POSITIONS, 4096).

root(Bookie, #{index := Bucket, fingerprint := Fingerprint}) ->
    {Key, SubKey} = leveled_fts2_codec:root_key(),
    case leveled_bookie:book_headonly(Bookie, Bucket, Key, SubKey) of
        {ok, Value} ->
            Root = leveled_fts2_codec:decode_root(Value),
            case maps:get(fingerprint, Root) of
                Fingerprint -> {ok, Root};
                _ -> not_found
            end;
        not_found ->
            not_found
    end.

search(Bookie, Schema, Root, AST, Opts) ->
    run(Bookie, Schema, Root, AST, Opts, all, []).

search_dirty(Bookie, Schema, Root, AST, Opts) ->
    search_dirty(Bookie, Schema, Root, AST, Opts, undefined).

search_dirty(Bookie, Schema, Root, AST, Opts, Hook) ->
    Deltas = leveled_fts2_delta:read(Bookie, Schema, Hook),
    Affected = leveled_fts2_delta:affected_sources(Deltas),
    run(
        Bookie,
        Schema,
        Root,
        AST,
        Opts,
        {all_except, Affected},
        Deltas
    ).

posting_read(Bookie, Schema, Root, AST, SourceIds, Opts) ->
    run(
        Bookie,
        Schema,
        Root,
        AST,
        Opts#{
            offset => 0, limit => length(SourceIds), return_positions => true
        },
        maps:from_list([{SourceId, true} || SourceId <- SourceIds]),
        []
    ).

posting_read_dirty(Bookie, Schema, Root, AST, SourceIds, Opts) ->
    Deltas = leveled_fts2_delta:read(Bookie, Schema),
    Wanted = maps:from_list([{SourceId, true} || SourceId <- SourceIds]),
    run(
        Bookie,
        Schema,
        Root,
        AST,
        Opts#{
            offset => 0, limit => length(SourceIds), return_positions => true
        },
        Wanted,
        Deltas
    ).

export_documents(Bookie, #{index := Bucket} = Schema, Root) ->
    GroupCount = maps:get(group_count, Root),
    GroupIds = case GroupCount of
        0 -> [];
        _ -> lists:seq(0, GroupCount - 1)
    end,
    Identities = read_identities(Bookie, Schema, Root, GroupIds),
    {Documents0, ByChunk} = maps:fold(
        fun(_GroupId, Group, {Docs, Chunks}) ->
            lists:foldl(
                fun(Chunk, {DocAcc, ChunkAcc}) ->
                    SourceId = maps:get(source_id, Chunk),
                    Document = maps:with(
                        [
                            source_id,
                            doc_key,
                            doc_version,
                            doc_length,
                            candidate_record,
                            hit_record
                        ],
                        Chunk
                    ),
                    {
                        DocAcc#{SourceId => Document#{
                            status => live, retired_ids => [], posting => #{}
                        }},
                        ChunkAcc#{maps:get(chunk_id, Chunk) => SourceId}
                    }
                end,
                {Docs, Chunks},
                maps:get(chunks, Group)
            )
        end,
        {#{}, #{}},
        Identities
    ),
    Generation = maps:get(generation, Root),
    Prefix = <<"f2:t:", Generation:64/unsigned-big>>,
    Fold = fun
        (B, {Key, <<"b">>}, Value, Acc) when B =:= Bucket ->
            case Key of
                <<"f2:t:", Generation:64/unsigned-big, Column:8, Token/binary>> ->
                    Row = maps:get({Column, Token}, Acc, #{}),
                    Acc#{{Column, Token} => Row#{
                        entries => leveled_fts2_codec:decode_plane(Value)
                    }};
                _ -> Acc
            end;
        (B, {Key, <<"p">>}, Value, Acc) when B =:= Bucket ->
            case Key of
                <<"f2:t:", Generation:64/unsigned-big, Column:8, Token/binary>> ->
                    Row = maps:get({Column, Token}, Acc, #{}),
                    Acc#{{Column, Token} => Row#{
                        positions => leveled_fts2_codec:decode_positions(Value)
                    }};
                _ -> Acc
            end;
        (_B, _Key, _Value, Acc) ->
            Acc
    end,
    {async, Runner} = leveled_bookie:book_headfold(
        Bookie,
        ?HEAD_TAG,
        {range, Bucket, {{Prefix, <<>>}, {<<Prefix/binary, 255>>, <<255>>}}},
        {Fold, #{}},
        false,
        true,
        false
    ),
    TermRows = Runner(),
    maps:fold(
        fun({Column, Token}, Row, Docs) ->
            Positions = maps:get(positions, Row, #{}),
            lists:foldl(
                fun({ChunkId, _GroupId, _StoredSourceId, _Length, Tf}, Acc) ->
                    SourceId = maps:get(ChunkId, ByChunk),
                    Document = maps:get(SourceId, Acc),
                    Posting = maps:get(posting, Document),
                    Tokens = maps:get(Column, Posting, #{}),
                    Entry = #{
                        count => Tf,
                        positions => maps:get(ChunkId, Positions, [])
                    },
                    Acc#{SourceId => Document#{posting => Posting#{
                        Column => Tokens#{Token => Entry}
                    }}}
                end,
                Docs,
                maps:get(entries, Row, [])
            )
        end,
        Documents0,
        TermRows
    ).

lookup_documents(Bookie, Schema, SourceIds) ->
    Existing = case root(Bookie, Schema) of
        {ok, Root} -> export_documents(Bookie, Schema, Root);
        not_found -> #{}
    end,
    Current = leveled_fts2_delta:overlay_documents(
        Existing, leveled_fts2_delta:read(Bookie, Schema)
    ),
    maps:with(SourceIds, Current).

run(Bookie, Schema, Root, AST, Opts, WantedSources, Deltas) ->
    check_cancellation(),
    NeedPositions = maps:get(return_positions, Opts, false),
    BaseMatches0 = case Root of
        undefined -> #{};
        _ -> eval(Bookie, Schema, Root, AST, NeedPositions, WantedSources)
    end,
    BaseMatches = case Deltas of
        [] -> BaseMatches0;
        _ -> enrich_base_groups(Bookie, Schema, Root, BaseMatches0)
    end,
    DeltaMatches0 = leveled_fts2_delta:evaluate(Deltas, Schema, AST),
    DeltaMatches = maps:filter(
        fun(_Key, Meta) ->
            wanted_delta(maps:get(source_id, Meta), WantedSources)
        end,
        DeltaMatches0
    ),
    Matches0 = maps:merge(BaseMatches, DeltaMatches),
    Ranked = maps:get(rank, Opts, none) =:= bm25,
    ScoreRoot = score_root(Root, Deltas),
    Scored = collapse_scored_groups(
        score_matches(Matches0, ScoreRoot, Ranked), Ranked
    ),
    BaseGroupIds = [
        maps:get(group_id, M)
     || M <- Scored, maps:is_key(group_id, M)
    ],
    Identity = case Root of
        undefined -> #{};
        _ -> read_identities(Bookie, Schema, Root, BaseGroupIds)
    end,
    Hydrated = lists:filtermap(
        fun(Match) ->
            Group = case maps:find(group_id, Match) of
                {ok, GroupId} -> maps:get(GroupId, Identity);
                error -> undefined
            end,
            hydrate_hit(Match, Group, Opts)
        end,
        Scored
    ),
    Hits0 = [
        Hit
     || Hit <- Hydrated,
        facet_matches(Hit, Schema, maps:get(impact_facet, Opts, nil))
    ],
    Count = length(Hits0),
    check_cancellation(),
    Hits1 = order_hits(Hits0, Ranked, maps:get(rank_tie_fields, Opts, [])),
    Offset = maps:get(offset, Opts, 0),
    Limit = maps:get(limit, Opts, ?DEFAULT_LIMIT),
    Hits = lists:sublist(drop(Offset, Hits1), Limit),
    case maps:get(return_count, Opts, false) of
        true -> {ok, #{hits => Hits, count => Count, count_kind => grouped}};
        false -> {ok, Hits}
    end.

enrich_base_groups(_Bookie, _Schema, undefined, Matches) ->
    Matches;
enrich_base_groups(Bookie, Schema, Root, Matches) ->
    GroupIds = lists:usort([
        maps:get(group_id, Meta)
     || Meta <- maps:values(Matches)
    ]),
    Identities = read_identities(Bookie, Schema, Root, GroupIds),
    VersionField = maps:get(candidate_version_field, Schema, undefined),
    maps:map(
        fun(_ChunkId, Meta) ->
            Group = maps:get(maps:get(group_id, Meta), Identities),
            SourceId = maps:get(source_id, Meta),
            [Chunk] = [
                C
             || C <- maps:get(chunks, Group),
                maps:get(source_id, C) =:= SourceId
            ],
            Candidate = maps:get(candidate_record, Chunk),
            Version = case VersionField of
                undefined -> 0;
                _ -> maps:get(VersionField, Candidate, 0)
            end,
            Meta#{
                logical_group => maps:get(group_key, Group),
                group_version => Version
            }
        end,
        Matches
    ).

score_root(undefined, Deltas) ->
    Live = leveled_fts2_delta:live_documents(Deltas),
    #{
        chunk_count => map_size(Live),
        group_count => map_size(Live),
        total_length => lists:sum([
            maps:get(doc_length, Document)
         || Document <- maps:values(Live)
        ])
    };
score_root(Root, []) ->
    Root;
score_root(Root, Deltas) ->
    Live = leveled_fts2_delta:live_documents(Deltas),
    Removed = [
        Delta
     || {_RowId, Delta} <- Deltas,
        maps:get(status, Delta) =:= remove orelse
            (maps:get(status, Delta) =:= live andalso
                maps:get(retired_ids, Delta, []) =:= [] andalso
                is_integer(maps:get(base_length, Delta, none)))
    ],
    Root#{
        chunk_count => erlang:max(
            0,
            maps:get(chunk_count, Root) - length(Removed) + map_size(Live)
        ),
        total_length => erlang:max(
            0,
            maps:get(total_length, Root) -
                lists:sum([maps:get(doc_length, D) || D <- Removed]) +
                lists:sum([
                    maps:get(doc_length, Document)
                 || Document <- maps:values(Live)
                ])
        )
    }.

eval(_Bookie, _Schema, _Root, {empty}, _NeedPositions, _Wanted) ->
    #{};
eval(Bookie, Schema, Root, {all_docs}, _NeedPositions, Wanted) ->
    all_chunks(Bookie, Schema, Root, Wanted);
eval(
    Bookie, Schema, Root, {term, Token, Prefix, Columns}, NeedPositions, Wanted
) ->
    read_term(
        Bookie, Schema, Root, Token, Prefix, Columns, NeedPositions, Wanted
    );
eval(Bookie, Schema, Root, {phrase, Specs, Columns}, _NeedPositions, Wanted) ->
    eval_phrase(Bookie, Schema, Root, Specs, Columns, Wanted);
eval(
    Bookie,
    Schema,
    Root,
    {near, Items, Distance, Columns},
    _NeedPositions,
    Wanted
) ->
    eval_near(Bookie, Schema, Root, Items, Distance, Columns, Wanted);
eval(Bookie, Schema, Root, {anchor, Child}, NeedPositions, Wanted) ->
    maps:filter(
        fun(_ChunkId, Meta) ->
            lists:member(
                0, flatten_positions(maps:get(match_positions, Meta, []))
            )
        end,
        eval(Bookie, Schema, Root, Child, NeedPositions, Wanted)
    );
eval(Bookie, Schema, Root, {'and', A, B}, NeedPositions, Wanted) ->
    intersect(
        eval(Bookie, Schema, Root, A, NeedPositions, Wanted),
        eval(Bookie, Schema, Root, B, NeedPositions, Wanted)
    );
eval(Bookie, Schema, Root, {'or', A, B}, NeedPositions, Wanted) ->
    union(
        eval(Bookie, Schema, Root, A, NeedPositions, Wanted),
        eval(Bookie, Schema, Root, B, NeedPositions, Wanted)
    );
eval(Bookie, Schema, Root, {'not', A, B}, NeedPositions, Wanted) ->
    Positive = eval(Bookie, Schema, Root, A, NeedPositions, Wanted),
    Negative = eval(Bookie, Schema, Root, B, false, Wanted),
    maps:without(maps:keys(Negative), Positive).

read_term(Bookie, Schema, Root, Token, false, Columns, NeedPositions, Wanted) ->
    lists:foldl(
        fun(Column, Acc) ->
            merge_term_row(
                Bookie, Schema, Root, Column, Token, NeedPositions, Wanted, Acc
            )
        end,
        #{},
        selector_ids(Columns, Schema)
    );
read_term(Bookie, Schema, Root, Prefix, true, Columns, NeedPositions, Wanted) ->
    lists:foldl(
        fun(Column, Acc) ->
            fold_prefix_rows(
                Bookie, Schema, Root, Column, Prefix, NeedPositions, Wanted, Acc
            )
        end,
        #{},
        selector_ids(Columns, Schema)
    ).

merge_term_row(
    Bookie, #{index := Bucket}, Root, Column, Token, NeedPositions, Wanted, Acc
) ->
    Generation = maps:get(generation, Root),
    {Key, _} = leveled_fts2_codec:term_key(Generation, Column, Token),
    case
        leveled_bookie:book_headonly_many(
            Bookie, Bucket, [{Key, <<"h">>}, {Key, <<"b">>}]
        )
    of
        [not_found, not_found] ->
            Acc;
        [{ok, HeaderValue}, {ok, Value}] ->
            Header = leveled_fts2_codec:decode(header, HeaderValue),
            Entries = leveled_fts2_codec:decode_plane(Value),
            note_plane_decode(),
            note_term_read(Token, NeedPositions),
            PositionMap =
                case NeedPositions of
                    true -> read_positions(Bookie, Bucket, Key);
                    false -> #{}
                end,
            add_entries(
                Entries,
                Column,
                Token,
                maps:get(chunk_df, Header),
                PositionMap,
                Wanted,
                Acc
            );
        Bad ->
            erlang:error({invalid_fts2_term_rows, Token, Bad})
    end.

fold_prefix_rows(
    Bookie,
    #{index := Bucket},
    Root,
    Column,
    Prefix,
    NeedPositions,
    Wanted,
    Acc0
) ->
    Generation = maps:get(generation, Root),
    {Start, Finish} = leveled_fts2_codec:term_range(Generation, Column, Prefix),
    Fold = fun
        (B, {Key, <<"b">>}, Value, Acc) when B =:= Bucket ->
            case Key of
                <<"f2:t:", Generation:64/unsigned-big, Column:8, Token/binary>> ->
                    PositionMap =
                        case NeedPositions of
                            true -> read_positions(Bookie, Bucket, Key);
                            false -> #{}
                        end,
                    add_entries(
                        leveled_fts2_codec:decode_plane(Value),
                        Column,
                        Token,
                        undefined,
                        PositionMap,
                        Wanted,
                        Acc
                    );
                _ ->
                    Acc
            end;
        (_B, _Key, _Value, Acc) ->
            Acc
    end,
    {async, Runner} = leveled_bookie:book_headfold(
        Bookie,
        ?HEAD_TAG,
        {range, Bucket, {{Start, <<>>}, {Finish, <<255>>}}},
        {Fold, Acc0},
        false,
        true,
        false
    ),
    Result = Runner(),
    check_cancellation(),
    Result.

read_positions(Bookie, Bucket, Key) ->
    case leveled_bookie:book_headonly(Bookie, Bucket, Key, <<"p">>) of
        {ok, Value} -> leveled_fts2_codec:decode_positions(Value);
        not_found -> #{}
    end.

add_entries(Entries, Column, Token, Df0, PositionMap, Wanted, Acc) ->
    Selected = [
        Entry
     || {_ChunkId, _GroupId, SourceId, _Length, _Tf} = Entry <- Entries,
        wanted(SourceId, Wanted)
    ],
    Df =
        case {Df0, Wanted} of
            {StoredDf, all} when is_integer(StoredDf) -> StoredDf;
            _ -> length(Selected)
        end,
    note_plane_decode(),
    lists:foldl(
        fun({ChunkId, GroupId, SourceId, Length, Tf} = Entry, Inner) ->
            Positions = maps:get(ChunkId, PositionMap, []),
            Term = {Column, Token},
            Meta = #{
                entry => Entry,
                chunk_id => ChunkId,
                group_id => GroupId,
                source_id => SourceId,
                doc_length => Length,
                tf => Tf,
                term_tfs => #{Term => Tf},
                term_dfs => #{Term => Df},
                term_df_parts => #{{base, Term} => Df},
                terms => [Token],
                columns => #{Column => #{Token => Positions}},
                match_positions => #{Token => Positions}
            },
            case maps:find(ChunkId, Inner) of
                error ->
                    Inner#{ChunkId => Meta};
                {ok, Existing} ->
                    Inner#{ChunkId => merge_meta(Existing, Meta)}
            end
        end,
        Acc,
        Selected
    ).

eval_phrase(Bookie, Schema, Root, Specs, Columns, Wanted) ->
    lists:foldl(
        fun(Column, Acc) ->
            TermMaps = [
                read_term(
                    Bookie,
                    Schema,
                    Root,
                    Token,
                    Prefix,
                    [column_name(Column, Schema)],
                    true,
                    Wanted
                )
             || {Token, Prefix, _Offset} <- Specs
            ],
            Candidates = intersect_many(TermMaps),
            maps:fold(
                fun(ChunkId, Meta, Inner) ->
                    Starts = phrase_starts(Meta, Column, Specs),
                    case Starts of
                        [] ->
                            Inner;
                        _ ->
                            Match = Meta#{
                                match_positions => #{phrase => Starts},
                                match_count => length(Starts)
                            },
                            case maps:find(ChunkId, Inner) of
                                error ->
                                    Inner#{ChunkId => Match};
                                {ok, Existing} ->
                                    Inner#{
                                        ChunkId => merge_meta(Existing, Match)
                                    }
                            end
                    end
                end,
                Acc,
                Candidates
            )
        end,
        #{},
        selector_ids(Columns, Schema)
    ).

phrase_starts(_Meta, _Column, []) ->
    [];
phrase_starts(Meta, Column, [{First, _Prefix, FirstOffset} | Rest]) ->
    Positions = column_token_positions(Meta, Column, First),
    [
        Position - FirstOffset
     || Position <- Positions,
        phrase_rest(Meta, Column, Rest, Position - FirstOffset)
    ].

phrase_rest(_Meta, _Column, [], _Start) ->
    true;
phrase_rest(Meta, Column, [{Token, _Prefix, Offset} | Rest], Start) ->
    lists:member(Start + Offset, column_token_positions(Meta, Column, Token)) andalso
        phrase_rest(Meta, Column, Rest, Start).

eval_near(Bookie, Schema, Root, Items, Distance, Columns, Wanted) ->
    lists:foldl(
        fun(Column, Acc) ->
            ColumnName = column_name(Column, Schema),
            ItemMaps = [
                eval(
                    Bookie,
                    Schema,
                    Root,
                    restrict_columns(Item, [ColumnName]),
                    true,
                    Wanted
                )
             || Item <- Items
            ],
            Candidates = intersect_many(ItemMaps),
            maps:fold(
                fun(ChunkId, Meta, Inner) ->
                    SpanLists = [
                        item_spans(Meta, Column, Item)
                     || Item <- Items
                    ],
                    Starts = near_positions(SpanLists, Distance),
                    case Starts of
                        [] ->
                            Inner;
                        _ ->
                            Match = Meta#{
                                match_positions => #{near => Starts},
                                match_count => length(Starts)
                            },
                            case maps:find(ChunkId, Inner) of
                                error ->
                                    Inner#{ChunkId => Match};
                                {ok, Existing} ->
                                    Inner#{
                                        ChunkId => merge_meta(Existing, Match)
                                    }
                            end
                    end
                end,
                Acc,
                Candidates
            )
        end,
        #{},
        selector_ids(Columns, Schema)
    ).

item_spans(Meta, Column, {term, Token, _Prefix, _Columns}) ->
    [{P, P} || P <- column_token_positions(Meta, Column, Token)];
item_spans(Meta, Column, {phrase, Specs, _Columns}) ->
    Starts = phrase_starts(Meta, Column, Specs),
    LastOffset = lists:max([Offset || {_Token, _Prefix, Offset} <- Specs]),
    [{Start, Start + LastOffset} || Start <- Starts];
item_spans(Meta, Column, {anchor, Item}) ->
    [
        {Start, End}
     || {Start, End} <- item_spans(Meta, Column, Item), Start =:= 0
    ];
item_spans(Meta, _Column, _Other) ->
    [{P, P} || P <- flatten_positions(maps:get(match_positions, Meta, []))].

near_positions(SpanLists, _Distance) when SpanLists =:= [] -> [];
near_positions(SpanLists, _Distance) when
    length(SpanLists) > 0,
    hd(SpanLists) =:= []
->
    [];
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
            lists:all(
                fun(Other) -> span_distance(Span, Other) =< Distance end,
                Chosen
            ) andalso near_position_matches([Span | Chosen], Rest, Distance)
        end,
        Spans
    ).

span_distance({_SA, EA}, {SB, _EB}) when EA < SB -> SB - EA - 1;
span_distance({SA, _EA}, {_SB, EB}) when EB < SA -> SA - EB - 1;
span_distance(_A, _B) -> 0.

intersect_many([]) -> #{};
intersect_many([First | Rest]) -> lists:foldl(fun intersect/2, First, Rest).

intersect(A, B) ->
    maps:fold(
        fun(ChunkId, MetaA, Acc) ->
            case maps:find(ChunkId, B) of
                {ok, MetaB} -> Acc#{ChunkId => merge_meta(MetaA, MetaB)};
                error -> Acc
            end
        end,
        #{},
        A
    ).

union(A, B) ->
    maps:fold(
        fun(ChunkId, Meta, Acc) ->
            case maps:find(ChunkId, Acc) of
                error -> Acc#{ChunkId => Meta};
                {ok, Existing} -> Acc#{ChunkId => merge_meta(Existing, Meta)}
            end
        end,
        A,
        B
    ).

merge_meta(A, B) ->
    Columns = maps:fold(
        fun(Column, Tokens, Acc) ->
            Acc#{Column => maps:merge(maps:get(Column, Acc, #{}), Tokens)}
        end,
        maps:get(columns, A, #{}),
        maps:get(columns, B, #{})
    ),
    A#{
        columns => Columns,
        terms => lists:usort(maps:get(terms, A, []) ++ maps:get(terms, B, [])),
        term_tfs => maps:merge(
            maps:get(term_tfs, A, #{}), maps:get(term_tfs, B, #{})
        ),
        term_dfs => maps:merge(
            maps:get(term_dfs, A, #{}), maps:get(term_dfs, B, #{})
        ),
        term_df_parts => maps:merge(
            maps:get(term_df_parts, A, #{}),
            maps:get(term_df_parts, B, #{})
        ),
        tf => maps:get(tf, A, 0) + maps:get(tf, B, 0),
        match_positions => maps:merge(
            maps:get(match_positions, A, #{}),
            maps:get(match_positions, B, #{})
        )
    }.

collapse_scored_groups(Chunks, Ranked) ->
    Groups = lists:foldl(
        fun(Meta, Acc) ->
            GroupKey = case maps:find(logical_group, Meta) of
                {ok, LogicalGroup} -> LogicalGroup;
                error -> {generation, maps:get(group_id, Meta)}
            end,
            Version = maps:get(group_version, Meta, 0),
            case maps:find(GroupKey, Acc) of
                error ->
                    Acc#{GroupKey => Meta};
                {ok, Existing} ->
                    ExistingVersion = maps:get(group_version, Existing, 0),
                    if
                        Version > ExistingVersion ->
                            Acc#{GroupKey => Meta};
                        Version < ExistingVersion ->
                            Acc;
                        true ->
                            CombinedCount =
                                match_count(Existing) + match_count(Meta),
                            Winner = case better_chunk(Meta, Existing, Ranked) of
                                true -> Meta;
                                false -> Existing
                            end,
                            Acc#{GroupKey => Winner#{
                                group_match_count => CombinedCount
                            }}
                    end
            end
        end,
        #{},
        Chunks
    ),
    maps:values(Groups).

better_chunk(A, B, true) ->
    {-maps:get(score, A), maps:get(source_id, A)} <
        {-maps:get(score, B), maps:get(source_id, B)};
better_chunk(A, B, false) ->
    maps:get(source_id, A) < maps:get(source_id, B).

match_count(Meta) ->
    maps:get(match_count, Meta, maps:get(tf, Meta, 0)).

score_matches(Matches, _Root, false) ->
    lists:sort(
        fun(A, B) -> maps:get(source_id, A) =< maps:get(source_id, B) end,
        [Meta#{score => 0.0} || Meta <- maps:values(Matches)]
    );
score_matches(Matches, Root, true) ->
    DocCount = erlang:max(maps:get(chunk_count, Root), 1),
    Avg = maps:get(total_length, Root, 0) / DocCount,
    Terms = lists:usort(
        lists:append([
            maps:keys(maps:get(term_tfs, Meta, #{}))
         || Meta <- maps:values(Matches)
        ])
    ),
    Idfs = maps:from_list([
        begin
            Parts = lists:foldl(
                fun(Meta, Acc) ->
                    maps:merge(Acc, maps:get(term_df_parts, Meta, #{}))
                end,
                #{},
                maps:values(Matches)
            ),
            NHit0 = lists:sum([
                Df
             || {{_Source, PartTerm}, Df} <- maps:to_list(Parts),
                PartTerm =:= Term
            ]),
            NHit = erlang:max(1, NHit0),
            Idf0 = math:log((DocCount - NHit + 0.5) / (NHit + 0.5)),
            {Term, erlang:max(Idf0, 1.0e-6)}
        end
     || Term <- Terms
    ]),
    lists:sort(
        fun(A, B) ->
            {-maps:get(score, A), maps:get(source_id, A)} =<
                {-maps:get(score, B), maps:get(source_id, B)}
        end,
        [
            Meta#{
                score => lists:sum([
                    bm25(
                        Tf,
                        maps:get(Term, Idfs),
                        Avg,
                        maps:get(doc_length, Meta, 0)
                    )
                 || {Term, Tf} <- maps:to_list(maps:get(term_tfs, Meta, #{})),
                    Tf > 0
                ])
            }
         || Meta <- maps:values(Matches)
        ]
    ).

bm25(Tf, Idf, Avg, Length) ->
    Ratio =
        case Avg > 0.0 of
            true -> Length / Avg;
            false -> 1.0
        end,
    Idf * (Tf * 2.2) / (Tf + 1.2 * (0.25 + 0.75 * Ratio)).

read_identities(_Bookie, _Schema, _Root, []) ->
    #{};
read_identities(Bookie, #{index := Bucket} = Schema, Root, GroupIds) ->
    Generation = maps:get(generation, Root),
    Key = leveled_fts2_codec:identity_key(Generation),
    Pages = lists:usort([
        GroupId bsr ?IDENTITY_PAGE_SHIFT
     || GroupId <- GroupIds
    ]),
    Values = leveled_bookie:book_headonly_many(
        maps:get(identity_bookie, Schema, Bookie),
        Bucket,
        [{Key, leveled_fts2_codec:identity_subkey(Page)} || Page <- Pages]
    ),
    lists:foldl(
        fun
            ({ok, Value}, Acc) ->
                lists:foldl(
                    fun(Group, Inner) ->
                        Inner#{maps:get(group_id, Group) => Group}
                    end,
                    Acc,
                    leveled_fts2_codec:decode(identity, Value)
                );
            (not_found, Acc) ->
                Acc
        end,
        #{},
        Values
    ).

hydrate_hit(#{delta_document := Document} = Match, undefined, Opts) ->
    Chunk = Document#{chunk_id => maps:get(chunk_id, Match)},
    hydrate_hit(Match, #{chunks => [Chunk]}, Opts);
hydrate_hit(Match, Group, Opts) ->
    ChunkId = maps:get(chunk_id, Match),
    case
        lists:filter(
            fun(Chunk) -> maps:get(chunk_id, Chunk) =:= ChunkId end,
            maps:get(chunks, Group)
        )
    of
        [] ->
            false;
        [Chunk] ->
            SourceId = maps:get(source_id, Chunk),
            Candidate = maps:get(candidate_record, Chunk),
            Base0 = #{
                key => SourceId,
                score => maps:get(score, Match),
                doc_length => maps:get(doc_length, Match),
                match_count => maps:get(
                    group_match_count, Match, match_count(Match)
                ),
                candidate_key => maps:get(doc_key, Chunk),
                candidate_version => maps:get(doc_version, Chunk),
                candidate_record => Candidate
            },
            Base1 =
                case maps:get(return_positions, Opts, false) of
                    true ->
                        Base0#{
                            positions => window_positions(
                                maps:get(match_positions, Match, #{})
                            )
                        };
                    false ->
                        Base0
                end,
            Base2 =
                case maps:get(return_terms, Opts, false) of
                    true -> Base1#{matched_terms => maps:get(terms, Match, [])};
                    false -> Base1
                end,
            Base3 =
                case
                    {
                        maps:find('$fts_text_blocks', Candidate),
                        maps:find('$fts_text_bytes', Candidate)
                    }
                of
                    {{ok, Blocks}, {ok, Bytes}} ->
                        Base2#{
                            text_blocks => Blocks,
                            text_bytes => Bytes,
                            index_resident_complete => true
                        };
                    _ ->
                        Base2
                end,
            case maps:get(resolve_hits, Opts, true) of
                false ->
                    {true, Base3};
                true ->
                    HitRecord = maps:get(hit_record, Chunk),
                    Resolved0 = Base3#{
                        key => maps:get(doc_key, Chunk), doc_id => SourceId
                    },
                    Resolved =
                        case HitRecord of
                            #{
                                record := Record,
                                text_blocks := HitBlocks,
                                text_bytes := HitBytes
                            } ->
                                WithText = Resolved0#{
                                    text_blocks => HitBlocks,
                                    text_bytes => HitBytes
                                },
                                case map_size(Record) of
                                    0 -> WithText;
                                    _ -> WithText#{record => Record}
                                end;
                            _ ->
                                Resolved0
                        end,
                    {true, Resolved}
            end
    end.

order_hits(Hits, false, _TieFields) ->
    lists:sort(fun(A, B) -> maps:get(key, A) =< maps:get(key, B) end, Hits);
order_hits(Hits, true, TieFields) ->
    lists:sort(
        fun(A, B) ->
            {-maps:get(score, A), tie_key(A, TieFields)} =<
                {-maps:get(score, B), tie_key(B, TieFields)}
        end,
        Hits
    ).

tie_key(Hit, []) ->
    maps:get(candidate_key, Hit, maps:get(key, Hit));
tie_key(Hit, Fields) ->
    Candidate = maps:get(candidate_record, Hit, #{}),
    {
        [maps:get(Field, Candidate, nil) || Field <- Fields],
        maps:get(candidate_key, Hit)
    }.

facet_matches(_Hit, _Schema, nil) ->
    true;
facet_matches(Hit, Schema, Facet) when is_list(Facet) ->
    Candidate = maps:get(candidate_record, Hit, #{}),
    Fields = maps:get(candidate_filter_fields, Schema, []),
    Facet =:=
        [maps:get(Field, Candidate, undefined) || {_Column, Field} <- Fields];
facet_matches(_Hit, _Schema, _Facet) ->
    false.

all_chunks(Bookie, Schema, Root, Wanted) ->
    GroupCount = maps:get(group_count, Root),
    Identity = read_identities(
        Bookie, Schema, Root, lists:seq(0, GroupCount - 1)
    ),
    maps:fold(
        fun(_GroupId, Group, Acc) ->
            lists:foldl(
                fun(Chunk, Inner) ->
                    SourceId = maps:get(source_id, Chunk),
                    case wanted(SourceId, Wanted) of
                        false ->
                            Inner;
                        true ->
                            ChunkId = maps:get(chunk_id, Chunk),
                            Inner#{
                                ChunkId => #{
                                    entry =>
                                        {ChunkId, maps:get(group_id, Chunk),
                                            SourceId,
                                            maps:get(doc_length, Chunk), 0},
                                    chunk_id => ChunkId,
                                    group_id => maps:get(group_id, Chunk),
                                    source_id => SourceId,
                                    doc_length => maps:get(doc_length, Chunk),
                                    tf => 0,
                                    terms => [],
                                    columns => #{},
                                    match_positions => #{}
                                }
                            }
                    end
                end,
                Acc,
                maps:get(chunks, Group)
            )
        end,
        #{},
        Identity
    ).

column_token_positions(Meta, Column, Token) ->
    maps:get(Token, maps:get(Column, maps:get(columns, Meta, #{}), #{}), []).

selector_ids(all, Schema) ->
    lists:seq(0, length(maps:get(columns, Schema)) - 1);
selector_ids({not_columns, Excluded}, Schema) ->
    selector_ids(
        [C || C <- maps:get(columns, Schema), not lists:member(C, Excluded)],
        Schema
    );
selector_ids(Columns, Schema) ->
    Names = maps:get(columns, Schema),
    [
        Index
     || {Name, Index} <- lists:zip(Names, lists:seq(0, length(Names) - 1)),
        lists:member(Name, Columns)
    ].

column_name(Column, Schema) -> lists:nth(Column + 1, maps:get(columns, Schema)).

restrict_columns({term, T, P, _}, Columns) ->
    {term, T, P, Columns};
restrict_columns({phrase, Specs, _}, Columns) ->
    {phrase, Specs, Columns};
restrict_columns({near, Items, D, _}, Columns) ->
    {near, [restrict_columns(I, Columns) || I <- Items], D, Columns};
restrict_columns({anchor, A}, Columns) ->
    {anchor, restrict_columns(A, Columns)};
restrict_columns({'and', A, B}, Columns) ->
    {'and', restrict_columns(A, Columns), restrict_columns(B, Columns)};
restrict_columns({'or', A, B}, Columns) ->
    {'or', restrict_columns(A, Columns), restrict_columns(B, Columns)};
restrict_columns({'not', A, B}, Columns) ->
    {'not', restrict_columns(A, Columns), restrict_columns(B, Columns)};
restrict_columns(Other, _Columns) ->
    Other.

wanted(_SourceId, all) -> true;
wanted(SourceId, {all_except, Excluded}) -> not maps:is_key(SourceId, Excluded);
wanted(SourceId, Wanted) -> maps:is_key(SourceId, Wanted).

wanted_delta(_SourceId, all) -> true;
wanted_delta(_SourceId, {all_except, _Excluded}) -> true;
wanted_delta(SourceId, Wanted) -> maps:is_key(SourceId, Wanted).

window_positions(Value) ->
    element(1, window_positions(Value, ?MAX_RETURN_POSITIONS)).

window_positions(Value, Remaining) when is_map(Value) ->
    lists:foldl(
        fun({Key, Nested}, {Acc, Left}) ->
            {Windowed, Next} = window_positions(Nested, Left),
            {Acc#{Key => Windowed}, Next}
        end,
        {#{}, Remaining},
        lists:sort(maps:to_list(Value))
    );
window_positions(Value, Remaining) when is_list(Value) ->
    Kept = lists:sublist(lists:sort(Value), Remaining),
    {Kept, Remaining - length(Kept)};
window_positions(Value, Remaining) ->
    {Value, Remaining}.

flatten_positions(Value) when is_map(Value) ->
    lists:append([flatten_positions(V) || V <- maps:values(Value)]);
flatten_positions(Value) when is_list(Value) -> Value;
flatten_positions(_Value) ->
    [].

drop(0, Values) -> Values;
drop(_Count, []) -> [];
drop(Count, [_ | Rest]) -> drop(Count - 1, Rest).

note_plane_decode() ->
    case erlang:get({leveled_fts, direct_page_decodes}) of
        Count when is_integer(Count) ->
            erlang:put({leveled_fts, direct_page_decodes}, Count + 1);
        _ ->
            ok
    end.

note_term_read(Token, true) ->
    case erlang:get({leveled_fts, term_run_folds}) of
        Counts when is_map(Counts) ->
            erlang:put(
                {leveled_fts, term_run_folds},
                Counts#{Token => maps:get(Token, Counts, 0) + 1}
            );
        _ ->
            ok
    end;
note_term_read(_Token, false) ->
    ok.

check_cancellation() ->
    case erlang:get(ash_leveled_search_cancellation) of
        #{owner := Owner, deadline_at := DeadlineAt, cancel_token := Token} ->
            receive
                {ash_leveled_cancel, Token, Reason} ->
                    throw({fts_error, {search_cancelled, Reason}})
            after 0 ->
                case erlang:is_process_alive(Owner) of
                    false ->
                        throw({fts_error, {search_cancelled, caller_down}});
                    true ->
                        case
                            DeadlineAt =/= infinity andalso
                                erlang:monotonic_time(millisecond) >= DeadlineAt
                        of
                            true ->
                                throw(
                                    {fts_error, {search_cancelled, deadline}}
                                );
                            false ->
                                ok
                        end
                end
            end;
        _ ->
            ok
    end.

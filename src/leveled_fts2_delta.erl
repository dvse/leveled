%% Exact doc-major FTS2 delta reader/evaluator.

-module(leveled_fts2_delta).

-include("leveled.hrl").

-export([
    read/2,
    read/3,
    affected_sources/1,
    live_documents/1,
    overlay_documents/2,
    evaluate/3
]).

read(Bookie, #{index := Bucket}) ->
    read(Bookie, #{index => Bucket}, undefined).

read(Bookie, #{index := Bucket}, Hook) ->
    Fold = fun
        (B, {<<"f2:d">>, <<SourceId:64/unsigned-big>>}, Value, Acc) when
            B =:= Bucket
        ->
            [{SourceId, leveled_fts2_codec:decode(delta, Value)} | Acc];
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
    Deltas = lists:reverse(Runner()),
    case Hook of
        undefined -> ok;
        Fun when is_function(Fun, 1) -> Fun({fts2, Deltas});
        Fun when is_function(Fun, 0) -> Fun()
    end,
    Deltas.

affected_sources(Deltas) ->
    maps:from_list([
        {SourceId, true}
     || {_RowId, Delta} <- Deltas,
        SourceId <- [maps:get(source_id, Delta) | maps:get(retired_ids, Delta, [])]
    ]).

live_documents(Deltas) ->
    overlay_documents(#{}, Deltas).

overlay_documents(Documents, Deltas) ->
    lists:foldl(
        fun({_RowId, Delta}, Acc) ->
            WithoutRetired = maps:without(maps:get(retired_ids, Delta, []), Acc),
            SourceId = maps:get(source_id, Delta),
            case maps:get(status, Delta) of
                live -> WithoutRetired#{SourceId => Delta};
                remove -> maps:remove(SourceId, WithoutRetired)
            end
        end,
        Documents,
        Deltas
    ).

evaluate(Deltas, Schema, AST) ->
    Documents = live_documents(Deltas),
    Dfs = delta_dfs(Documents, Schema, AST),
    maps:fold(
        fun(SourceId, Document, Acc) ->
            Posting = maps:get(posting, Document, #{}),
            case eval(AST, Posting, Schema) of
                false ->
                    Acc;
                {true, Positions, TermTfs, Terms} ->
                    Candidate = maps:get(candidate_record, Document),
                    GroupFields = maps:get(candidate_group_fields, Schema, []),
                    VersionField = maps:get(
                        candidate_version_field, Schema, undefined
                    ),
                    LogicalGroup = case GroupFields of
                        [] -> {source, SourceId};
                        _ -> {group, [
                            maps:get(Field, Candidate, undefined)
                         || Field <- GroupFields
                        ]}
                    end,
                    GroupVersion = case VersionField of
                        undefined -> 0;
                        _ -> maps:get(VersionField, Candidate, 0)
                    end,
                    Acc#{{delta, SourceId} => #{
                        chunk_id => {delta, SourceId},
                        source_id => SourceId,
                        doc_length => maps:get(doc_length, Document),
                        tf => lists:sum(maps:values(TermTfs)),
                        term_tfs => TermTfs,
                        term_df_parts => maps:from_list([
                            {{delta, Term}, maps:get(Term, Dfs, 0)}
                         || Term <- maps:keys(TermTfs)
                        ]),
                        terms => Terms,
                        match_positions => Positions,
                        match_count => position_count(Positions),
                        logical_group => LogicalGroup,
                        group_version => GroupVersion,
                        delta_document => Document
                    }}
            end
        end,
        #{},
        Documents
    ).

delta_dfs(Documents, Schema, AST) ->
    Terms = lists:usort(ast_terms(AST, Schema)),
    maps:from_list([
        {Term, length([
            ok
         || Document <- maps:values(Documents),
            term_tf(Term, maps:get(posting, Document, #{})) > 0
        ])}
     || Term <- Terms
    ]).

ast_terms({term, Token, Prefix, Columns}, Schema) ->
    [
        {Column, Token, Prefix}
     || Column <- selector_ids(Columns, Schema)
    ];
ast_terms({phrase, Specs, Columns}, Schema) ->
    lists:append([
        [{Column, Token, Prefix} || {Token, Prefix, _Offset} <- Specs]
     || Column <- selector_ids(Columns, Schema)
    ]);
ast_terms({near, Items, _Distance, _Columns}, Schema) ->
    lists:append([ast_terms(Item, Schema) || Item <- Items]);
ast_terms({anchor, Child}, Schema) -> ast_terms(Child, Schema);
ast_terms({_Op, A, B}, Schema) -> ast_terms(A, Schema) ++ ast_terms(B, Schema);
ast_terms(_Other, _Schema) -> [].

term_tf({Column, Token, false}, Posting) ->
    case maps:find(Token, maps:get(Column, Posting, #{})) of
        {ok, Entry} -> maps:get(count, Entry);
        error -> 0
    end;
term_tf({Column, Prefix, true}, Posting) ->
    lists:sum([
        maps:get(count, Entry)
     || {Token, Entry} <- maps:to_list(maps:get(Column, Posting, #{})),
        binary_prefix(Token, Prefix)
    ]).

eval({empty}, _Posting, _Schema) -> false;
eval({all_docs}, _Posting, _Schema) -> {true, #{}, #{}, []};
eval({term, Token, Prefix, Columns}, Posting, Schema) ->
    Matches = lists:append([
        [
            {{Column, Actual, false}, Actual, maps:get(positions, Entry)}
         || {Actual, Entry} <- maps:to_list(maps:get(Column, Posting, #{})),
            token_matches(Actual, Token, Prefix)
        ]
     || Column <- selector_ids(Columns, Schema)
    ]),
    case Matches of
        [] -> false;
        _ ->
            TermTfs = maps:from_list([
                {Term, length(Positions)}
             || {Term, _Actual, Positions} <- Matches
            ]),
            Positions = lists:append([Ps || {_Term, _Actual, Ps} <- Matches]),
            {true, #{Token => Positions}, TermTfs,
                lists:usort([Actual || {_Term, Actual, _Ps} <- Matches])}
    end;
eval({phrase, Specs, Columns}, Posting, Schema) ->
    Starts = lists:append([
        phrase_starts(Posting, Column, Specs)
     || Column <- selector_ids(Columns, Schema)
    ]),
    case Starts of
        [] -> false;
        _ ->
            Terms = phrase_term_tfs(Posting, Schema, Specs, Columns),
            {true, #{phrase => Starts}, Terms,
                [Token || {Token, _Prefix, _Offset} <- Specs]}
    end;
eval({near, Items, Distance, Columns}, Posting, Schema) ->
    Starts = lists:append([
        near_positions(
            [item_spans(Posting, Schema, Column, Item) || Item <- Items],
            Distance
        )
     || Column <- selector_ids(Columns, Schema)
    ]),
    case Starts of
        [] -> false;
        _ ->
            Tfs = maps:from_list([
                {Term, term_tf(Term, Posting)}
             || Term <- ast_terms({near, Items, Distance, Columns}, Schema)
            ]),
            {true, #{near => Starts}, Tfs,
                lists:usort([Token || {_Column, Token, _Prefix} <- maps:keys(Tfs)])}
    end;
eval({anchor, Child}, Posting, Schema) ->
    case eval(Child, Posting, Schema) of
        {true, Positions, Tfs, Terms} ->
            case lists:member(0, flatten_positions(Positions)) of
                true -> {true, Positions, Tfs, Terms};
                false -> false
            end;
        false -> false
    end;
eval({'and', A, B}, Posting, Schema) ->
    case {eval(A, Posting, Schema), eval(B, Posting, Schema)} of
        {{true, PA, TA, TermsA}, {true, PB, TB, TermsB}} ->
            {true, maps:merge(PA, PB), maps:merge(TA, TB),
                lists:usort(TermsA ++ TermsB)};
        _ -> false
    end;
eval({'or', A, B}, Posting, Schema) ->
    case {eval(A, Posting, Schema), eval(B, Posting, Schema)} of
        {{true, PA, TA, TermsA}, {true, PB, TB, TermsB}} ->
            {true, maps:merge(PA, PB), maps:merge(TA, TB),
                lists:usort(TermsA ++ TermsB)};
        {{true, PA, TA, TermsA}, false} -> {true, PA, TA, TermsA};
        {false, {true, PB, TB, TermsB}} -> {true, PB, TB, TermsB};
        _ -> false
    end;
eval({'not', A, B}, Posting, Schema) ->
    case {eval(A, Posting, Schema), eval(B, Posting, Schema)} of
        {{true, PA, TA, TermsA}, false} -> {true, PA, TA, TermsA};
        _ -> false
    end.

phrase_term_tfs(Posting, Schema, Specs, Columns) ->
    maps:from_list([
        begin
            Term = {Column, Token, Prefix},
            {Term, term_tf(Term, Posting)}
        end
     || Column <- selector_ids(Columns, Schema),
        {Token, Prefix, _Offset} <- Specs
    ]).

phrase_starts(_Posting, _Column, []) -> [];
phrase_starts(Posting, Column, [{First, FirstPrefix, FirstOffset} | Rest]) ->
    FirstPositions = token_positions(Posting, Column, First, FirstPrefix),
    [
        Position - FirstOffset
     || Position <- FirstPositions,
        phrase_rest(Posting, Column, Rest, Position - FirstOffset)
    ].

phrase_rest(_Posting, _Column, [], _Start) -> true;
phrase_rest(Posting, Column, [{Token, Prefix, Offset} | Rest], Start) ->
    lists:member(Start + Offset, token_positions(Posting, Column, Token, Prefix))
        andalso phrase_rest(Posting, Column, Rest, Start).

item_spans(Posting, _Schema, Column, {term, Token, Prefix, _Columns}) ->
    [{P, P} || P <- token_positions(Posting, Column, Token, Prefix)];
item_spans(Posting, _Schema, Column, {phrase, Specs, _Columns}) ->
    Starts = phrase_starts(Posting, Column, Specs),
    Last = lists:max([Offset || {_Token, _Prefix, Offset} <- Specs]),
    [{Start, Start + Last} || Start <- Starts];
item_spans(Posting, Schema, Column, {anchor, Item}) ->
    [Span || {Start, _End} = Span <- item_spans(Posting, Schema, Column, Item),
        Start =:= 0];
item_spans(_Posting, _Schema, _Column, _Item) -> [].

near_positions([], _Distance) -> [];
near_positions([[] | _], _Distance) -> [];
near_positions([First | Rest], Distance) ->
    [
        Start
     || {Start, _End} = Span <- First,
        near_match([Span], Rest, Distance)
    ].

near_match(_Chosen, [], _Distance) -> true;
near_match(Chosen, [Spans | Rest], Distance) ->
    lists:any(
        fun(Span) ->
            lists:all(fun(Other) -> span_distance(Span, Other) =< Distance end,
                Chosen) andalso near_match([Span | Chosen], Rest, Distance)
        end,
        Spans
    ).

span_distance({_SA, EA}, {SB, _EB}) when EA < SB -> SB - EA - 1;
span_distance({SA, _EA}, {_SB, EB}) when EB < SA -> SA - EB - 1;
span_distance(_A, _B) -> 0.

token_positions(Posting, Column, Token, false) ->
    case maps:find(Token, maps:get(Column, Posting, #{})) of
        {ok, Entry} -> maps:get(positions, Entry);
        error -> []
    end;
token_positions(Posting, Column, Prefix, true) ->
    lists:append([
        maps:get(positions, Entry)
     || {Token, Entry} <- maps:to_list(maps:get(Column, Posting, #{})),
        binary_prefix(Token, Prefix)
    ]).

token_matches(Actual, Token, false) -> Actual =:= Token;
token_matches(Actual, Prefix, true) -> binary_prefix(Actual, Prefix).

binary_prefix(Binary, Prefix) when byte_size(Binary) >= byte_size(Prefix) ->
    binary:part(Binary, 0, byte_size(Prefix)) =:= Prefix;
binary_prefix(_Binary, _Prefix) -> false.

selector_ids(all, Schema) -> lists:seq(0, length(maps:get(columns, Schema)) - 1);
selector_ids({not_columns, Excluded}, Schema) ->
    selector_ids([C || C <- maps:get(columns, Schema), not lists:member(C, Excluded)],
        Schema);
selector_ids(Columns, Schema) ->
    Names = maps:get(columns, Schema),
    [Index || {Name, Index} <- lists:zip(Names, lists:seq(0, length(Names) - 1)),
        lists:member(Name, Columns)].

position_count(Value) when is_map(Value) ->
    lists:sum([position_count(V) || V <- maps:values(Value)]);
position_count(Value) when is_list(Value) -> length(Value);
position_count(_Value) -> 0.

flatten_positions(Value) when is_map(Value) ->
    lists:append([flatten_positions(V) || V <- maps:values(Value)]);
flatten_positions(Value) when is_list(Value) -> Value;
flatten_positions(_Value) -> [].

%% -------- Native full-text search ---------
%%
%% FTS is implemented as ordinary secondary index rows on the indexed object.
%% There are no companion objects, hidden buckets, segments, or side stores.

-module(leveled_fts).

-include("leveled.hrl").

-export([
    normalise_indexes/1,
    augment_index_specs/7,
    book_ftssearch/5,
    search/6
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
    {ok, lists:reverse(Acc)};
normalise_indexes([Index | Rest], Acc) ->
    case normalise_index_definition(Index) of
        {ok, Normal} -> normalise_indexes(Rest, [Normal | Acc]);
        {error, Reason} -> {error, Reason}
    end.

book_ftssearch(Pid, Bucket, Index0, Query, Opts) ->
    Index = normalise_index(Index0),
    leveled_bookie:book_returnfolder(Pid, {fts_query, Bucket, Index, Query, Opts}).

augment_index_specs(Bucket, Key, Tag, Object, OldObject, IndexSpecs, Indexes) ->
    Matching = [
        Schema
     || #{bucket := Bucket0, tag := Tag0} = Schema <- Indexes,
        Bucket0 =:= Bucket,
        Tag0 =:= Tag
    ],
    Generated =
        lists:append([
            schema_delta_specs(Bucket, Key, Object, OldObject, Schema)
         || Schema <- Matching
        ]),
    {ok, IndexSpecs ++ Generated}.

search(Pid, Bucket, Index, Query, Opts0, Indexes) ->
    case find_schema(Bucket, Index, Indexes) of
        {ok, Schema} ->
            case normalise_search_options(Opts0, Schema) of
                {ok, Opts} ->
                    case parse(Query, Opts) of
                        {ok, AST0} ->
                            Columns = option_columns(Opts, Schema),
                            case validate_ast_columns(AST0, Columns) of
                                ok ->
                                    AST = restrict_ast_columns(AST0, Columns),
                                    case needs_payload_positions(AST, Opts) of
                                        false ->
                                            case candidate_keys(Pid, Bucket, Index, AST, Columns) of
                                                {ok, Keys} -> {ok, page_key_hits(Keys, Opts)};
                                                {error, Reason} -> {error, Reason}
                                            end;
                                        true ->
                                            case fast_payload_search(Pid, Bucket, Index, AST, Opts) of
                                                {ok, Hits} ->
                                                    {ok, Hits};
                                                not_supported ->
                                                    case load_payload_candidate_metas(
                                                        Pid, Bucket, Index, AST, Columns
                                                    ) of
                                                        {ok, Metas} ->
                                                            evaluate_payload_candidates(
                                                                Index, AST, Opts, Metas
                                                            );
                                                        {error, Reason} ->
                                                            {error, Reason}
                                                    end;
                                                {error, Reason} ->
                                                    {error, Reason}
                                            end
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
            {error, missing_fts_schema}
    end.

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
    case [
        Schema
     || #{bucket := Bucket0, index := Index0} = Schema <- Indexes,
        Bucket0 =:= Bucket,
        Index0 =:= Index
    ] of
        [Schema | _] -> {ok, Schema};
        [] -> not_found
    end.

schema_delta_specs(Bucket, Key, Object, OldObject, Schema) ->
    OldRows =
        case OldObject of
            {ok, Old} -> doc_rows(Bucket, Key, Old, Schema);
            _ -> #{}
        end,
    NewRows =
        case Object of
            delete -> #{};
            _ -> doc_rows(Bucket, Key, Object, Schema)
        end,
    OldKeys = maps:keys(OldRows),
    Removes = [
        {remove, Field, Term}
     || {Field, Term} <- OldKeys,
        not maps:is_key({Field, Term}, NewRows)
    ],
    Adds = [
        {add_payload, Field, Term, Payload}
     || {{Field, Term}, Payload} <- maps:to_list(NewRows),
        maps:get({Field, Term}, OldRows, undefined) =/= Payload
    ],
    Removes ++ Adds.

doc_rows(_Bucket, _Key, Object, Schema) ->
    Fields = extract_fields(Object, maps:get(column_specs, Schema)),
    Meta = build_doc_meta_from_fields(Fields, Schema),
    Index = maps:get(index, Schema),
    Positions = maps:get(positions, Meta),
    DocLength = maps:get(doc_length, Meta),
    TermRows =
        lists:append([
            [
                {{term_field(Index, Column), Token},
                    encode_term_payload(
                        DocLength,
                        maps:get(Column, maps:get(column_lengths, Meta), 0),
                        Positions0
                    )}
             || {Token, Positions0} <- maps:to_list(TokenMap)
            ]
         || {Column, TokenMap} <- maps:to_list(Positions)
        ]),
    maps:from_list([
        {{doc_field(Index), doc}, encode_doc_payload(DocLength)}
        | TermRows
    ]).

extract_fields(Object, ColumnSpecs) ->
    [
        {Column, normalise_text(extract_path(Object, Path))}
     || {Column, Path} <- ColumnSpecs
    ].

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

build_doc_meta_from_fields(Fields, Schema) ->
    Opts = maps:get(options, Schema),
    ColumnTerms = build_column_terms(Fields, Opts),
    Positions =
        maps:from_list([
            {Column, maps:from_list(Terms)}
         || {Column, Terms} <- ColumnTerms
        ]),
    ColumnLengths =
        maps:from_list([
            {Column, lists:sum([length(Pos) || {_Token, Pos} <- Terms])}
         || {Column, Terms} <- ColumnTerms
        ]),
    DocLength = lists:sum([Len || {_Column, Len} <- maps:to_list(ColumnLengths)]),
    #{
        version => ?VERSION,
        kind => fts_doc,
        index => maps:get(index, Schema),
        columns => maps:get(columns, Schema),
        prefixes => maps:get(prefixes, Schema),
        tokenizer => maps:get(tokenizer, Schema),
        doc_length => DocLength,
        column_lengths => ColumnLengths,
        positions => Positions
    }.

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
    lists:sort([{Token, lists:sort(Positions)} || {Token, Positions} <- maps:to_list(Acc)]);
group_positions([{Token, Pos} | Rest], Acc) ->
    group_positions(Rest, maps:update_with(Token, fun(Ps) -> [Pos | Ps] end, [Pos], Acc)).

candidate_keys(_Pid, _Bucket, _Index, {empty}, _Columns) ->
    {ok, sets:new()};
candidate_keys(Pid, Bucket, Index, {all_docs}, _Columns) ->
    index_key_set(Pid, Bucket, doc_field(Index), doc, doc);
candidate_keys(Pid, Bucket, Index, {term, Token, Prefix, Columns}, _SearchColumns) ->
    token_candidate_keys(Pid, Bucket, Index, Token, Prefix, Columns);
candidate_keys(Pid, Bucket, Index, {phrase, Specs, Columns}, _SearchColumns) ->
    phrase_candidate_keys(Pid, Bucket, Index, Specs, Columns);
candidate_keys(Pid, Bucket, Index, {near, Items, _Distance, Columns}, SearchColumns) ->
    intersect_ast_candidate_keys(
        Pid,
        Bucket,
        Index,
        [restrict_ast_columns(Item, Columns) || Item <- Items],
        SearchColumns
    );
candidate_keys(Pid, Bucket, Index, {anchor, AST}, Columns) ->
    candidate_keys(Pid, Bucket, Index, AST, Columns);
candidate_keys(Pid, Bucket, Index, {'and', A, B}, Columns) ->
    with_sets(
        candidate_keys(Pid, Bucket, Index, A, Columns),
        candidate_keys(Pid, Bucket, Index, B, Columns),
        fun sets:intersection/2
    );
candidate_keys(Pid, Bucket, Index, {'or', A, B}, Columns) ->
    with_sets(
        candidate_keys(Pid, Bucket, Index, A, Columns),
        candidate_keys(Pid, Bucket, Index, B, Columns),
        fun sets:union/2
    );
candidate_keys(Pid, Bucket, Index, {'not', A, B}, Columns) ->
    with_sets(
        candidate_keys(Pid, Bucket, Index, A, Columns),
        candidate_keys(Pid, Bucket, Index, B, Columns),
        fun sets:subtract/2
    ).

with_sets({ok, A}, {ok, B}, Fun) ->
    {ok, Fun(A, B)};
with_sets({error, Reason}, _Other, _Fun) ->
    {error, Reason};
with_sets(_Other, {error, Reason}, _Fun) ->
    {error, Reason}.

phrase_candidate_keys(_Pid, _Bucket, _Index, [], _Columns) ->
    {ok, sets:new()};
phrase_candidate_keys(Pid, Bucket, Index, Specs, Columns) ->
    intersect_candidate_sets([
        fun() -> token_candidate_keys(Pid, Bucket, Index, Token, Prefix, Columns) end
     || {Token, Prefix} <- lists:usort([{T, P} || {T, P, _Offset} <- Specs])
    ]).

intersect_ast_candidate_keys(_Pid, _Bucket, _Index, [], _Columns) ->
    {ok, sets:new()};
intersect_ast_candidate_keys(Pid, Bucket, Index, ASTs, Columns) ->
    intersect_candidate_sets([
        fun() -> candidate_keys(Pid, Bucket, Index, AST, Columns) end
     || AST <- ASTs
    ]).

intersect_candidate_sets([]) ->
    {ok, sets:new()};
intersect_candidate_sets([Fun | Rest]) ->
    case Fun() of
        {ok, Set} -> intersect_candidate_sets(Rest, Set);
        Error -> Error
    end.

intersect_candidate_sets([], Acc) ->
    {ok, Acc};
intersect_candidate_sets([Fun | Rest], Acc) ->
    case Fun() of
        {ok, Set} -> intersect_candidate_sets(Rest, sets:intersection(Acc, Set));
        Error -> Error
    end.

token_candidate_keys(Pid, Bucket, Index, Token, false, Columns) ->
    fold_column_sets(
        fun(Column) -> index_key_set(Pid, Bucket, term_field(Index, Column), Token,
            Token)
        end,
        concrete_columns(Columns)
    );
token_candidate_keys(Pid, Bucket, Index, Prefix, true, Columns) ->
    fold_column_sets(
        fun(Column) -> prefix_key_set(Pid, Bucket, term_field(Index, Column), Prefix)
        end,
        concrete_columns(Columns)
    ).

fold_column_sets(_Fun, []) ->
    {ok, sets:new()};
fold_column_sets(Fun, [Column | Rest]) ->
    case Fun(Column) of
        {ok, Set} -> fold_column_sets(Fun, Rest, Set);
        Error -> Error
    end.

fold_column_sets(_Fun, [], Acc) ->
    {ok, Acc};
fold_column_sets(Fun, [Column | Rest], Acc) ->
    case Fun(Column) of
        {ok, Set} -> fold_column_sets(Fun, Rest, sets:union(Acc, Set));
        Error -> Error
    end.

index_key_set(Pid, Bucket, Field, Start, End) ->
    Fold = fun(_Bucket, Key, Acc) -> sets:add_element(Key, Acc) end,
    {async, Runner} =
        leveled_bookie:book_indexfold(
            Pid, {Bucket, null}, {Fold, sets:new()}, {Field, Start, End}, {false, undefined}
        ),
    {ok, Runner()}.

prefix_key_set(Pid, Bucket, Field, Prefix) ->
    End =
        case next_prefix(Prefix) of
            {ok, Next} -> Next;
            none -> <<255>>
        end,
    Fold =
        fun(_Bucket, {Term, Key}, Acc) ->
            case binary_prefix(Term, Prefix) of
                true -> sets:add_element(Key, Acc);
                false -> Acc
            end
        end,
    {async, Runner} =
        leveled_bookie:book_indexfold(
            Pid, {Bucket, null}, {Fold, sets:new()}, {Field, Prefix, End}, {true, undefined}
        ),
    {ok, Runner()}.

load_candidate_metas(Pid, Bucket, Index, {all_docs}, _Columns, Keys) ->
    load_doc_metas(Pid, Bucket, Index, Keys);
load_candidate_metas(Pid, Bucket, Index, AST, Columns, Keys) ->
    Terms = query_terms(AST, Columns),
    load_term_metas(Pid, Bucket, Index, sets:to_list(Keys), Terms).

fast_payload_search(
    Pid,
    Bucket,
    Index,
    {near, [{term, TokenA, false, _}, {term, TokenB, false, _}], Distance, Columns},
    #{return_positions := false} = Opts
) when TokenA =/= TokenB ->
    fast_two_term_near(Pid, Bucket, Index, TokenA, TokenB, Distance, concrete_columns(Columns), Opts);
fast_payload_search(
    Pid,
    Bucket,
    Index,
    {near, [{term, TokenA, false, _}, {term, TokenB, false, _}], Distance, Columns},
    Opts
) when TokenA =/= TokenB, not is_map_key(return_positions, Opts) ->
    fast_two_term_near(Pid, Bucket, Index, TokenA, TokenB, Distance, concrete_columns(Columns), Opts);
fast_payload_search(_Pid, _Bucket, _Index, _AST, _Opts) ->
    not_supported.

fast_two_term_near(_Pid, _Bucket, _Index, _TokenA, _TokenB, _Distance, [], Opts) ->
    {ok, page_key_hits(sets:new(), Opts)};
fast_two_term_near(Pid, Bucket, Index, TokenA, TokenB, Distance, Columns, Opts) ->
    try
        Matches =
            lists:foldl(
                fun(Column, Acc) ->
                    PositionsByKey =
                        term_positions_by_key(Pid, Bucket, term_field(Index, Column), TokenA),
                    fold_near_term_matches(
                        Pid,
                        Bucket,
                        term_field(Index, Column),
                        TokenB,
                        PositionsByKey,
                        Distance,
                        Acc
                    )
                end,
                sets:new(),
                Columns
            ),
        {ok, page_key_hits(Matches, Opts)}
    catch
        throw:{fts_error, Reason} -> {error, Reason}
    end.

term_positions_by_key(Pid, Bucket, Field, Token) ->
    Fold =
        fun(_Bucket, {_Term, Key, Payload}, Acc) ->
            case decode_term_payload(Payload) of
                {ok, _DocLength, _ColLength, Positions} -> Acc#{Key => Positions};
                error -> throw({fts_error, invalid_fts_payload})
            end
        end,
    {async, Runner} =
        leveled_bookie:book_indexfold(
            Pid, {Bucket, null}, {Fold, #{}}, {Field, Token, Token}, {payload, undefined}
        ),
    Runner().

fold_near_term_matches(Pid, Bucket, Field, Token, PositionsByKey, Distance, Acc0) ->
    Fold =
        fun(_Bucket, {_Term, Key, Payload}, Acc) ->
            case maps:find(Key, PositionsByKey) of
                {ok, PositionsA} ->
                    case decode_term_payload(Payload) of
                        {ok, _DocLength, _ColLength, PositionsB} ->
                            case position_lists_near(PositionsA, PositionsB, Distance + 1) of
                                true -> sets:add_element(Key, Acc);
                                false -> Acc
                            end;
                        error ->
                            throw({fts_error, invalid_fts_payload})
                    end;
                error ->
                    Acc
            end
        end,
    {async, Runner} =
        leveled_bookie:book_indexfold(
            Pid, {Bucket, null}, {Fold, Acc0}, {Field, Token, Token}, {payload, undefined}
        ),
    Runner().

position_lists_near([], _PositionsB, _Window) ->
    false;
position_lists_near(_PositionsA, [], _Window) ->
    false;
position_lists_near([A | RestA] = PositionsA, [B | RestB] = PositionsB, Window) ->
    if
        B < A - Window -> position_lists_near(PositionsA, RestB, Window);
        A < B - Window -> position_lists_near(RestA, PositionsB, Window);
        true -> true
    end.

load_payload_candidate_metas(Pid, Bucket, Index, {all_docs}, Columns) ->
    case candidate_keys(Pid, Bucket, Index, {all_docs}, Columns) of
        {ok, Keys} -> load_doc_metas(Pid, Bucket, Index, Keys);
        {error, Reason} -> {error, Reason}
    end;
load_payload_candidate_metas(Pid, Bucket, Index, AST, Columns) ->
    Terms = query_terms(AST, Columns),
    case Terms of
        [] ->
            case candidate_keys(Pid, Bucket, Index, AST, Columns) of
                {ok, Keys} -> load_candidate_metas(Pid, Bucket, Index, AST, Columns, Keys);
                {error, Reason} -> {error, Reason}
            end;
        [SeedTerm | Rest] ->
            case seedable_payload_ast(AST) of
                true -> load_seeded_term_metas(Pid, Bucket, Index, SeedTerm, Rest);
                false -> load_unfiltered_term_metas(Pid, Bucket, Index, Terms)
            end
    end.

load_doc_metas(Pid, Bucket, Index, Keys) ->
    KeySet = Keys,
    Fold =
        fun(_Bucket, {_Term, Key, Payload}, Acc) ->
            case sets:is_element(Key, KeySet) of
                true ->
                    case decode_doc_payload(Payload) of
                        {ok, DocLength} ->
                            Acc#{Key => empty_meta(Index, Key, DocLength)};
                        error ->
                            Acc
                    end;
                false ->
                    Acc
            end
        end,
    {async, Runner} =
        leveled_bookie:book_indexfold(
            Pid, {Bucket, null}, {Fold, #{}}, {doc_field(Index), doc, doc}, {payload, undefined}
        ),
    {ok, Runner()}.

load_term_metas(_Pid, _Bucket, Index, Keys, []) ->
    {ok, maps:from_list([{Key, empty_meta(Index, Key, 0)} || Key <- Keys])};
load_term_metas(Pid, Bucket, Index, Keys, Terms) ->
    KeySet = sets:from_list(Keys),
    Init = maps:from_list([{Key, empty_meta(Index, Key, 0)} || Key <- Keys]),
    Metas = lists:foldl(
        fun({Column, Token, Prefix}, Acc0) ->
            fold_postings(Pid, Bucket, Index, Column, Token, Prefix, {set, KeySet}, Acc0)
        end,
        Init,
        Terms
    ),
    {ok, Metas}.

load_unfiltered_term_metas(Pid, Bucket, Index, Terms) ->
    Metas = lists:foldl(
        fun({Column, Token, Prefix}, Acc0) ->
            fold_postings(Pid, Bucket, Index, Column, Token, Prefix, all, Acc0)
        end,
        #{},
        Terms
    ),
    {ok, Metas}.

load_seeded_term_metas(Pid, Bucket, Index, {Column, Token, Prefix}, Rest) ->
    SeedMetas = fold_postings(Pid, Bucket, Index, Column, Token, Prefix, all, #{}),
    KeySet = sets:from_list(maps:keys(SeedMetas)),
    Metas = lists:foldl(
        fun({Column0, Token0, Prefix0}, Acc0) ->
            fold_postings(Pid, Bucket, Index, Column0, Token0, Prefix0, {set, KeySet}, Acc0)
        end,
        SeedMetas,
        Rest
    ),
    {ok, Metas}.

seedable_payload_ast({phrase, _Specs, _Columns}) ->
    true;
seedable_payload_ast({near, _Items, _Distance, _Columns}) ->
    true;
seedable_payload_ast({anchor, AST}) ->
    seedable_payload_ast(AST);
seedable_payload_ast(_AST) ->
    false.

fold_postings(Pid, Bucket, Index, Column, Token, false, KeySet, Acc0) ->
    fold_postings_range(Pid, Bucket, Index, term_field(Index, Column), Token, Token, Token, false,
        Column, KeySet, Acc0);
fold_postings(Pid, Bucket, Index, Column, Prefix, true, KeySet, Acc0) ->
    End =
        case next_prefix(Prefix) of
            {ok, Next} -> Next;
            none -> <<255>>
        end,
    fold_postings_range(Pid, Bucket, Index, term_field(Index, Column), Prefix, End, Prefix, true,
        Column, KeySet, Acc0).

fold_postings_range(Pid, Bucket, Index, Field, Start, End, Wanted, Prefix, Column, KeySet, Acc0) ->
    Fold =
        fun(_Bucket, {Term, Key, Payload}, Acc) ->
            case payload_key_wanted(Key, KeySet) andalso
                (Prefix =:= false orelse binary_prefix(Term, Wanted))
            of
                true ->
                    case decode_term_payload(Payload) of
                        {ok, DocLength, _ColLength, Positions} ->
                            add_payload_positions(
                                Acc, Index, Key, DocLength, Column, Term, Positions
                            );
                        error ->
                            Acc
                    end;
                false ->
                    Acc
            end
        end,
    {async, Runner} =
        leveled_bookie:book_indexfold(
            Pid, {Bucket, null}, {Fold, Acc0}, {Field, Start, End}, {payload, undefined}
        ),
    Runner().

payload_key_wanted(_Key, all) ->
    true;
payload_key_wanted(Key, {set, KeySet}) ->
    sets:is_element(Key, KeySet).

empty_meta(Index, Key, DocLength) ->
    #{
        version => ?VERSION,
        kind => fts_doc,
        key => Key,
        index => Index,
        doc_length => DocLength,
        positions => #{}
    }.

add_payload_positions(Metas, Index, Key, DocLength, Column, Term, Positions) ->
    Meta0 = maps:get(Key, Metas, empty_meta(Index, Key, DocLength)),
    Meta1 = Meta0#{doc_length => max(DocLength, maps:get(doc_length, Meta0, 0))},
    Positions0 = maps:get(positions, Meta1, #{}),
    ColumnMap0 = maps:get(Column, Positions0, #{}),
    ColumnMap = ColumnMap0#{Term => Positions},
    Metas#{Key => Meta1#{positions => Positions0#{Column => ColumnMap}}}.

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
        {ok, page_hits(Sorted, Opts)}
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

encode_doc_payload(DocLength) ->
    <<2:8, DocLength:32/unsigned-big>>.

decode_doc_payload(<<2:8, DocLength:32/unsigned-big>>) ->
    {ok, DocLength};
decode_doc_payload(_Payload) ->
    error.

encode_term_payload(DocLength, ColLength, Positions) ->
    PosBin = encode_positions(Positions),
    Tf = length(Positions),
    <<1:8, DocLength:32/unsigned-big, ColLength:32/unsigned-big, Tf:32/unsigned-big,
        PosBin/binary>>.

decode_term_payload(
    <<1:8, DocLength:32/unsigned-big, ColLength:32/unsigned-big, Tf:32/unsigned-big,
        PosBin/binary>>
) ->
    case decode_positions(PosBin, Tf) of
        {ok, Positions} -> {ok, DocLength, ColLength, Positions};
        error -> error
    end;
decode_term_payload(_Payload) ->
    error.

encode_positions(Positions) ->
    {Parts, _Last} =
        lists:foldl(
            fun(Pos, {Acc, Last}) ->
                {[encode_varint(Pos - Last) | Acc], Pos}
            end,
            {[], 0},
            lists:sort(Positions)
        ),
    iolist_to_binary(lists:reverse(Parts)).

decode_positions(Bin, Count) ->
    decode_positions(Bin, Count, 0, []).

decode_positions(<<>>, 0, _Last, Acc) ->
    {ok, lists:reverse(Acc)};
decode_positions(Bin, Count, Last, Acc) when Count > 0 ->
    case decode_varint(Bin) of
        {ok, Delta, Rest} ->
            Pos = Last + Delta,
            decode_positions(Rest, Count - 1, Pos, [Pos | Acc]);
        error ->
            error
    end;
decode_positions(_Bin, _Count, _Last, _Acc) ->
    error.

encode_varint(N) when is_integer(N), N >= 0 ->
    iolist_to_binary(encode_varint_parts(N)).

encode_varint_parts(N) when N < 128 ->
    [N];
encode_varint_parts(N) ->
    [16#80 bor (N band 16#7F) | encode_varint_parts(N bsr 7)].

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

item_positions_in_column(Meta, {term, Token, Prefix, _Columns}, Column) ->
    column_term_positions(Meta, Column, Token, Prefix);
item_positions_in_column(Meta, {phrase, Specs, _Columns}, Column) ->
    phrase_column_match_positions(Meta, Specs, Column);
item_positions_in_column(Meta, {anchor, AST}, Column) ->
    [P || P <- item_positions_in_column(Meta, AST, Column), P =:= 0];
item_positions_in_column(Meta, Other, Column) ->
    case eval(restrict_ast_columns(Other, [Column]), Meta) of
        {true, PosMap} -> flatten_position_map(PosMap);
        false -> []
    end.

item_spans_in_column(Meta, {term, Token, Prefix, _Columns}, Column) ->
    [{P, P} || P <- column_term_positions(Meta, Column, Token, Prefix)];
item_spans_in_column(Meta, {phrase, Specs, _Columns}, Column) ->
    phrase_column_match_spans(Meta, Specs, Column);
item_spans_in_column(Meta, {anchor, AST}, Column) ->
    [{P, P} || P <- item_positions_in_column(Meta, AST, Column), P =:= 0];
item_spans_in_column(Meta, Other, Column) ->
    [{P, P} || P <- item_positions_in_column(Meta, Other, Column)].

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
        near_position_matches(Span, Rest, Distance)
    ].

near_position_matches(Span, Rest, Distance) ->
    lists:all(
        fun(Spans) ->
            span_list_matches(Span, Spans, Distance)
        end,
        Rest
    ).

span_list_matches(_Span, [], _Distance) ->
    false;
span_list_matches({StartA, EndA} = Span, [{StartB, EndB} = OtherSpan | Rest], Distance) ->
    BeforeWindow = EndB < StartA - Distance - 1,
    AfterWindow = StartB > EndA + Distance + 1,
    case {BeforeWindow, AfterWindow} of
        {true, false} ->
            span_list_matches(Span, Rest, Distance);
        {false, true} ->
            false;
        {false, false} ->
            span_distance(Span, OtherSpan) =< Distance orelse
                span_list_matches(Span, Rest, Distance);
        {true, true} ->
            false
    end.

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

page_key_hits(Keys, Opts) ->
    Hits = [
        #{key => Key, rank => 0.0, score => 0.0, doc_length => 0}
     || Key <- lists:sort(sets:to_list(Keys))
    ],
    page_hits(Hits, Opts).

needs_payload_positions(_AST, #{return_positions := true}) ->
    true;
needs_payload_positions({phrase, _Specs, _Columns}, _Opts) ->
    true;
needs_payload_positions({near, _Items, _Distance, _Columns}, _Opts) ->
    true;
needs_payload_positions({anchor, _AST}, _Opts) ->
    true;
needs_payload_positions({'and', A, B}, Opts) ->
    needs_payload_positions(A, Opts) orelse needs_payload_positions(B, Opts);
needs_payload_positions({'or', A, B}, Opts) ->
    needs_payload_positions(A, Opts) orelse needs_payload_positions(B, Opts);
needs_payload_positions({'not', A, B}, Opts) ->
    needs_payload_positions(A, Opts) orelse needs_payload_positions(B, Opts);
needs_payload_positions(_AST, _Opts) ->
    false.

drop(0, List) ->
    List;
drop(_N, []) ->
    [];
drop(N, [_ | Rest]) when N > 0 ->
    drop(N - 1, Rest).

normalise_search_options(Opts0, Schema) ->
    case options_map(Opts0) of
        {ok, Opts0Map} ->
            case validate_search_options(Opts0Map) of
                {ok, Opts1} ->
                    case validate_schema_contract(Schema, Opts1, search) of
                        ok ->
                            {ok, Opts1#{
                                columns => maps:get(columns, Opts1, maps:get(columns, Schema)),
                                prefixes => maps:get(prefixes, Opts1, maps:get(prefixes, Schema))
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
    Opts1 = Opts#{
        prefixes => normalise_prefixes(maps:get(prefixes, Opts, [])),
        tokenchars => normalise_char_list(maps:get(tokenchars, Opts, [])),
        separators => normalise_char_list(maps:get(separators, Opts, [])),
        stopwords => normalise_stopwords(maps:get(stopwords, Opts, []))
    }#{
        tokenizer => normalise_tokenizer(maps:get(tokenizer, Opts, unicode61))
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

normalise_stopwords(Words) ->
    [Token || Word <- Words, {Token, _Pos} <- tokenize(Word, #{stopwords => []})].

normalise_tokenizer(unicode61) -> unicode61;
normalise_tokenizer(<<"unicode61">>) -> unicode61;
normalise_tokenizer("unicode61") -> unicode61.

tokenizer_description(Opts) ->
    #{
        tokenizer => normalise_tokenizer(maps:get(tokenizer, Opts, unicode61)),
        remove_diacritics => maps:get(remove_diacritics, Opts, false),
        tokenchars => normalise_char_list(maps:get(tokenchars, Opts, [])),
        separators => normalise_char_list(maps:get(separators, Opts, [])),
        stopwords => normalise_stopwords(maps:get(stopwords, Opts, []))
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

doc_field(Index) ->
    {fts_doc, Index}.

term_field(Index, Column) ->
    {fts_term, Index, Column}.

tokenize(Text0, Opts) ->
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
    (unicode_token_char(Char) orelse lists:member(Char, TokenChars)) andalso
        not lists:member(Char, Separators).

unicode_token_char(Char) ->
    case unicode_util:category(Char) of
        {letter, _} -> true;
        {number, _} -> true;
        {mark, _} -> true;
        {other, private} -> true;
        _ -> false
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
            not unicode_mark(Char)
        ],
    unicode:characters_to_binary(Kept, utf8).

unicode_mark(Char) ->
    case unicode_util:category(Char) of
        {mark, _} -> true;
        _ -> false
    end.

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
        {ok, Item, Rest} -> parse_near_items(Rest, Opts, [ungroup(Item) | Acc]);
        Error -> Error
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

concat_phrase({term, Token, Prefix, _Columns}, Right) ->
    concat_phrase({phrase, [{Token, Prefix, 0}], all}, Right);
concat_phrase({phrase, LeftSpecs, _Columns}, {term, Token, Prefix, _RightColumns}) ->
    Offset = length(LeftSpecs),
    {ok, {phrase, LeftSpecs ++ [{Token, Prefix, Offset}], all}};
concat_phrase({phrase, LeftSpecs, _Columns}, {phrase, RightSpecs, _RightColumns}) ->
    Offset = length(LeftSpecs),
    {ok, {phrase, LeftSpecs ++ [{Token, Prefix, Offset + Pos}
        || {Token, Prefix, Pos} <- RightSpecs], all}};
concat_phrase(_Left, _Right) ->
    {error, {fts_parse, invalid_phrase_concat}}.

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
    case first_ast_column(AST) of
        none -> ok;
        {column, Column} ->
            case lists:member(Column, Columns) of
                true -> ok;
                false -> {error, {fts_parse, unknown_column, Column}}
            end
    end.

first_ast_column({term, _T, _P, Columns}) -> first_column(Columns);
first_ast_column({phrase, _Specs, Columns}) -> first_column(Columns);
first_ast_column({near, Items, _Distance, Columns}) ->
    case first_column(Columns) of
        none -> first_ast_column_list(Items);
        Column -> Column
    end;
first_ast_column({anchor, AST}) -> first_ast_column(AST);
first_ast_column({'and', A, B}) -> first_ast_column_pair(A, B);
first_ast_column({'or', A, B}) -> first_ast_column_pair(A, B);
first_ast_column({'not', A, B}) -> first_ast_column_pair(A, B);
first_ast_column(_Other) -> none.

first_ast_column_pair(A, B) ->
    case first_ast_column(A) of
        none -> first_ast_column(B);
        Column -> Column
    end.

first_ast_column_list([]) -> none;
first_ast_column_list([Item | Rest]) ->
    case first_ast_column(Item) of
        none -> first_ast_column_list(Rest);
        Column -> Column
    end.

first_column(all) -> none;
first_column({not_columns, Columns}) -> first_column(Columns);
first_column([]) -> none;
first_column([Column | _Rest]) -> {column, Column}.

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

next_prefix(<<>>) ->
    none;
next_prefix(Bin) ->
    case bump_prefix(lists:reverse(binary_to_list(Bin)), []) of
        none -> none;
        Bytes -> {ok, list_to_binary(Bytes)}
    end.

bump_prefix([], _Suffix) ->
    none;
bump_prefix([B | Rest], Suffix) when B < 255 ->
    lists:reverse(Rest) ++ [B + 1 | Suffix];
bump_prefix([255 | Rest], Suffix) ->
    bump_prefix(Rest, [0 | Suffix]).

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

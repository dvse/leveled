-module(leveled_fts2_tests).

-include("leveled.hrl").
-include_lib("eunit/include/eunit.hrl").

delta_write_shape_test() ->
    Schema = schema(<<"fts2-shape">>),
    {ok, Specs} = leveled_fts:derive(
        Schema, <<"doc">>, #{body => <<"alpha beta">>, title => <<"Doc">>}
    ),
    Keys = [{Key, SubKey} || {add, _Bucket, Key, SubKey, _Value} <- Specs],
    ?assert(lists:member({<<"doc">>, <<"doc">>}, Keys)),
    ?assert(lists:any(fun({<<"f2:d">>, <<_:64>>}) -> true;
        (_) -> false end, Keys)),
    ?assertNot(lists:any(fun({<<"r">>, _}) -> true;
        ({<<"c">>, _}) -> true;
        ({<<_Shard:16>>, <<"d:", _/binary>>}) -> true;
        (_) -> false end, Keys)).

codec_roundtrip_test() ->
    Plane = [{1, 2, 3, 4, 5}, {9, 10, 11, 12, 13}],
    Positions = [{1, [0, 3, 21]}, {9, [2, 8]}],
    ?assertEqual(Plane,
        leveled_fts2_codec:decode_plane(leveled_fts2_codec:encode_plane(Plane))),
    ?assertEqual(maps:from_list(Positions),
        leveled_fts2_codec:decode_positions(
            leveled_fts2_codec:encode_positions(Positions)
        )).

generation_and_identity_bookie_test_() ->
    {timeout, 60, fun generation_and_identity_bookie/0}.

generation_and_identity_bookie() ->
    with_bookies(fun(Main, Identity) ->
        Schema = (schema(<<"fts2-generation">>))#{identity_bookie => Identity},
        ok = put_doc(Main, Schema, <<"a">>, <<"alpha thank you alpha">>),
        ok = put_doc(Main, Schema, <<"b">>, <<"alpha beta">>),
        {ok, #{skipped := []}} = leveled_fts:consolidate(Main, Schema, #{}),
        {ok, Root1} = leveled_fts2:available(Main, Schema),
        Generation1 = maps:get(generation, Root1),
        IdentityKey1 = leveled_fts2_codec:identity_key(Generation1),
        not_found = leveled_bookie:book_headonly(
            Main, maps:get(index, Schema), IdentityKey1, <<0:32>>
        ),
        {ok, _} = leveled_bookie:book_headonly(
            Identity, maps:get(index, Schema), IdentityKey1, <<0:32>>
        ),
        #{count := 2} = search(Main, Schema, <<"alpha">>, #{return_count => true}),
        #{count := 1} = search(Main, Schema, <<"\"thank you\"">>,
            #{return_count => true}),
        {ok, Manifest} = leveled_bookie:book_headonly(
            Main, maps:get(index, Schema), <<"doc">>, <<"a">>
        ),
        {ok, Specs} = leveled_fts:update(
            Schema, <<"a">>, #{body => <<"gamma">>, title => <<"a">>}, Manifest
        ),
        ok = leveled_bookie:book_mput(Main, Specs),
        #{count := 1} = search(Main, Schema, <<"alpha">>, #{return_count => true}),
        {ok, #{skipped := []}} = leveled_fts:consolidate(Main, Schema, #{}),
        {ok, Root2} = leveled_fts2:available(Main, Schema),
        ?assertNotEqual(Generation1, maps:get(generation, Root2)),
        {OldTermKey, _} = leveled_fts2_codec:term_key(Generation1, 0, <<"alpha">>),
        not_found = leveled_bookie:book_headonly(
            Main, maps:get(index, Schema), OldTermKey, <<"b">>
        ),
        not_found = leveled_bookie:book_headonly(
            Identity, maps:get(index, Schema), IdentityKey1, <<0:32>>
        ),
        #{count := 1} = search(Main, Schema, <<"gamma">>, #{return_count => true})
    end).

fetches_cover_dirty_and_clean_test_() ->
    {timeout, 60, fun fetches_cover_dirty_and_clean/0}.

fetches_cover_dirty_and_clean() ->
    with_bookies(fun(Main, Identity) ->
        Schema = (schema(<<"fts2-fetch">>))#{identity_bookie => Identity},
        ok = put_doc(Main, Schema, <<"a">>, <<"alpha">>),
        [DirtyHit] = maps:get(hits,
            search(Main, Schema, <<"alpha">>, #{return_count => true})),
        DocId = maps:get(doc_id, DirtyHit),
        {ok, #{DocId := {<<"a">>, _Version, _Record}}} =
            leveled_fts:record_fetch(Main, Schema, [DocId], points),
        {ok, #{DocId := {<<"a">>, _Version2, Candidate}}} =
            leveled_fts:candidate_fetch(Main, Schema, [DocId], fold),
        ?assertMatch(#{'$fts_text_blocks' := _}, Candidate),
        {ok, #{skipped := []}} = leveled_fts:consolidate(Main, Schema, #{}),
        {ok, #{DocId := {<<"a">>, _Version3, _Record2}}} =
            leveled_fts:record_fetch(Main, Schema, [DocId], fold),
        {ok, #{DocId := {<<"a">>, _Version4, _Candidate2}}} =
            leveled_fts:candidate_fetch(Main, Schema, [DocId], points)
    end).

schema(Index) ->
    {ok, Schema} = leveled_fts:schema(#{
        index => Index,
        columns => [body],
        text_field => [body],
        hit_fields => [title],
        candidate_fields => [title]
    }),
    Schema.

put_doc(Bookie, Schema, Key, Body) when is_binary(Body) ->
    put_doc(Bookie, Schema, Key, #{body => Body, title => Key});
put_doc(Bookie, Schema, Key, Object) ->
    {ok, Specs} = leveled_fts:derive(Schema, Key, Object),
    leveled_bookie:book_mput(Bookie, Specs).

search(Bookie, Schema, Query, Opts) ->
    {ok, Result} = leveled_fts:search(Bookie, Schema, Query, Opts),
    Result.

with_bookies(Fun) ->
    Suffix = integer_to_list(erlang:unique_integer([positive])),
    MainRoot = testutil:reset_filestructure("test/test_fts2_main_" ++ Suffix),
    IdentityRoot = testutil:reset_filestructure(
        "test/test_fts2_identity_" ++ Suffix
    ),
    {ok, Main} = leveled_bookie:book_start(start_opts(MainRoot)),
    {ok, Identity} = leveled_bookie:book_start(start_opts(IdentityRoot)),
    try Fun(Main, Identity)
    after
        try leveled_bookie:book_destroy(Main) catch _:_ -> ok end,
        try leveled_bookie:book_destroy(Identity) catch _:_ -> ok end
    end.

start_opts(Root) ->
    [
        {root_path, Root},
        {sync_strategy, testutil:sync_strategy()},
        {compression_method, none},
        {ledger_compression, none},
        {log_level, warning}
    ].

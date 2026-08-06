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
    Plane = [{1, 2, 3, 4, 5, 0}, {9, 10, 11, 12, 13, 1}],
    Positions = [{1, [0, 3, 21]}, {9, [2, 8]}],
    ?assertEqual(Plane,
        leveled_fts:fts2_codec_decode_plane(
            leveled_fts:fts2_codec_encode_plane(Plane)
        )),
    ?assertEqual(maps:from_list(Positions),
        leveled_fts:fts2_codec_decode_positions(
            leveled_fts:fts2_codec_encode_positions(Positions)
        )).

packed_identity_page_selective_roundtrip_test() ->
    First = identity_group(
        0,
        0,
        7,
        <<"first">>,
        #{title => <<"First">>, active => true, ordinal => 1}
    ),
    Second = identity_group(
        1,
        1,
        8,
        <<"second">>,
        #{title => <<"Second">>, active => false, ordinal => 2}
    ),
    Encoded = leveled_fts:fts2_codec_encode_identity_page([First, Second]),
    ?assertMatch(<<4, _/binary>>, Encoded),
    [{1, {group, [<<"second">>, [8], nil]}, 1, 8, <<"second">>,
        <<8:64/unsigned-big>>, 12, Candidate, Hit}] =
        leveled_fts:fts2_codec_decode_identity_page(Encoded, [{1, 1}]),
    ?assertEqual(maps:get(candidate_record, hd(maps:get(chunks, Second))), Candidate),
    ?assertEqual(maps:get(hit_record, hd(maps:get(chunks, Second))), Hit).

packed_identity_serving_row_roundtrip_test() ->
    Key = <<"fast-row">>,
    Blocks = [{0, 0, 0}, {1, 512, 4096}],
    Candidate = #{
        '$fts_text_blocks' => Blocks,
        '$fts_text_bytes' => 5000,
        udi => <<"document-identity">>,
        content_version => 42
    },
    Hit = #{
        doc_key => Key,
        record => #{udi => <<"document-identity">>},
        text_blocks => Blocks,
        text_bytes => 5000
    },
    Group = #{
        group_id => 7,
        group_key => {group, [<<"document-identity">>]},
        chunks => [#{
            chunk_id => 11,
            group_id => 7,
            source_id => 99,
            doc_key => Key,
            doc_version => <<42:64/unsigned-big>>,
            doc_length => 512,
            candidate_record => Candidate,
            hit_record => Hit
        }]
    },
    Encoded = leveled_fts:fts2_codec_encode_identity_page([Group]),
    [{7, undefined, 11, 99, Key, <<42:64/unsigned-big>>, 512,
        Candidate, Hit}] = leveled_fts:fts2_codec_decode_identity_page(
            Encoded, [{serve, 7, 11}]
        ).

tokenizer_scanner_parity_test() ->
    Inputs = tokenizer_parity_inputs(),
    Configs = [
        #{tokenizer => unicode61, remove_diacritics => 1},
        #{tokenizer => unicode61, remove_diacritics => 0},
        #{tokenizer => unicode61, remove_diacritics => 2},
        #{
            tokenizer => unicode61,
            remove_diacritics => 1,
            stopwords => [<<"the">>, <<"and">>, <<"ab">>]
        },
        #{
            tokenizer => unicode61,
            remove_diacritics => 1,
            tokenchars => [$_, $-]
        },
        #{
            tokenizer => unicode61,
            remove_diacritics => 1,
            separators => [$e]
        }
    ],
    lists:foreach(
        fun({Input, Opts}) ->
            ?assertEqual(
                leveled_fts_tokenizer_reference:tokenize_with_offsets(
                    Input, Opts
                ),
                leveled_fts:tokenize_with_offsets(Input, Opts)
            )
        end,
        [{Input, Opts} || Input <- Inputs, Opts <- Configs]
    ).

tokenizer_stream_fold_parity_test() ->
    Opts = #{tokenizer => unicode61, remove_diacritics => 1},
    lists:foreach(
        fun(Input) ->
            Expected = leveled_fts:tokenize_with_offsets(Input, Opts),
            Actual = lists:reverse(
                leveled_fts:tokenize_fold_for_test(
                    fun(Token, Ordinal, Offset, Length, Acc) ->
                        [{Token, Ordinal, Offset, Length} | Acc]
                    end,
                    [],
                    Input,
                    Opts
                )
            ),
            ?assertEqual(Expected, Actual)
        end,
        tokenizer_parity_inputs()
    ).

generation_and_identity_bookie_test_() ->
    {timeout, 60, fun generation_and_identity_bookie/0}.

generation_residency_budget_fallback_test_() ->
    {timeout, 60, fun generation_residency_budget_fallback/0}.

cold_reopen_result_parity_test_() ->
    {timeout, 180, fun cold_reopen_result_parity/0}.

residency_failure_is_sticky_test_() ->
    {timeout, 60, fun residency_failure_is_sticky/0}.

cold_reopen_result_parity() ->
    Suffix = integer_to_list(erlang:unique_integer([positive])),
    MainRoot = testutil:reset_filestructure(
        "test/test_fts2_cold_reopen_main_" ++ Suffix
    ),
    IdentityRoot = testutil:reset_filestructure(
        "test/test_fts2_cold_reopen_identity_" ++ Suffix
    ),
    {ok, Main0} = leveled_bookie:book_plainstart(start_opts(MainRoot)),
    {ok, Identity0} = leveled_bookie:book_plainstart(start_opts(IdentityRoot)),
    Schema0 = (seam_schema(<<"fts2-cold-reopen">>))#{
        identity_bookie => Identity0
    },
    try
        lists:foreach(
            fun(Group) ->
                Document = <<"file-", Group:16/unsigned-big>>,
                lists:foreach(
                    fun(Chunk) ->
                        Key = <<Group:16/unsigned-big, Chunk:16/unsigned-big>>,
                        Body = iolist_to_binary([
                            binary:copy(<<"spitfire ">>, 1 + Group rem 7),
                            binary:copy(<<"filler ">>, Chunk + Group rem 5)
                        ]),
                        ok = seam_put(
                            Main0, Schema0, Key, Document, <<"default">>, Body
                        )
                    end,
                    lists:seq(1, 1 + Group rem 4)
                )
            end,
            lists:seq(1, 32)
        ),
        Opts = #{
            columns => [content],
            limit => 20,
            rank => bm25,
            resolve_hits => false,
            return_count => true
        },
        Dirty = search(Main0, Schema0, <<"spitfire">>, Opts),
        {ok, _} = leveled_fts:consolidate(Main0, Schema0, #{}),
        Clean = search(Main0, Schema0, <<"spitfire">>, Opts),
        ?assertEqual(
            cold_reopen_result_projection(Dirty),
            cold_reopen_result_projection(Clean)
        ),
        lists:foreach(
            fun(Group) ->
                Document = <<"file-", Group:16/unsigned-big>>,
                lists:foreach(
                    fun(Chunk) ->
                        Key = <<Group:16/unsigned-big, Chunk:16/unsigned-big>>,
                        {ok, Manifest} = leveled_bookie:book_headonly(
                            Main0, maps:get(index, Schema0), <<"doc">>, Key
                        ),
                        {ok, Specs} = leveled_fts:update(
                            Schema0,
                            Key,
                            #{
                                content => <<"renamed filler">>,
                                tenant => <<"default">>,
                                udi => Document,
                                path => <<"/", Document/binary>>,
                                ipath_vec => [<<"root">>, Document],
                                content_version => 2,
                                chunk_key => Key
                            },
                            Manifest
                        ),
                        ok = leveled_bookie:book_mput(Main0, Specs)
                    end,
                    lists:seq(1, 1 + Group rem 4)
                )
            end,
            lists:seq(1, 12)
        ),
        lists:foreach(
            fun(Group) ->
                Document = <<"file-", Group:16/unsigned-big>>,
                Key = <<Group:16/unsigned-big, 1:16/unsigned-big>>,
                ok = seam_put(
                    Main0,
                    Schema0,
                    Key,
                    Document,
                    <<"default">>,
                    <<"spitfire spitfire">>
                )
            end,
            lists:seq(33, 44)
        ),
        {DirtyErrorUs, DirtyError} = timer:tc(
            leveled_fts,
            search,
            [Main0, Schema0, <<"spitfire">>, Opts]
        ),
        ?assertMatch(
            {error, {fts_index_dirty,
                grouped_search_requires_consolidation, _}},
            DirtyError
        ),
        ?assert(DirtyErrorUs < 50000),
        {ok, _} = leveled_fts:consolidate(Main0, Schema0, #{}),
        CleanOverlay = search(Main0, Schema0, <<"spitfire">>, Opts),
        exit(Main0, kill),
        exit(Identity0, kill),
        {ok, Main1} = leveled_bookie:book_start(start_opts(MainRoot)),
        {ok, Identity1} = leveled_bookie:book_start(start_opts(IdentityRoot)),
        Schema1 = Schema0#{identity_bookie := Identity1},
        try
            {FirstUs, First} = timer:tc(
                fun() -> search(Main1, Schema1, <<"spitfire">>, Opts) end
            ),
            {SecondUs, Second} = timer:tc(
                fun() -> search(Main1, Schema1, <<"spitfire">>, Opts) end
            ),
            WarmSamples = [
                timer:tc(
                    fun() ->
                        search(Main1, Schema1, <<"spitfire">>, Opts)
                    end
                )
             || _ <- lists:seq(1, 5)
            ],
            {WarmTimes, Warms} = lists:unzip(WarmSamples),
            WarmUs = lists:nth(3, lists:sort(WarmTimes)),
            Projection = cold_reopen_result_projection(CleanOverlay),
            ?assertEqual(Projection, cold_reopen_result_projection(First)),
            ?assertEqual(Projection, cold_reopen_result_projection(Second)),
            lists:foreach(
                fun(Warm) ->
                    ?assertEqual(
                        Projection, cold_reopen_result_projection(Warm)
                    )
                end,
                Warms
            ),
            ?assert(SecondUs =< 5 * erlang:max(WarmUs, 1)),
            Status = leveled_bookie:book_status(Main1),
            ?assert(is_integer(maps:get(fts_resident_generation, Status))),
            ?assert(maps:get(fts_resident_rows, Status) > 0),
            io:format(
                user,
                "coldnames dirty_error_us=~p first_us=~p second_us=~p "
                "warm_median_us=~p~n",
                [DirtyErrorUs, FirstUs, SecondUs, WarmUs]
            )
        after
            try leveled_bookie:book_destroy(Main1) catch _:_ -> ok end,
            try leveled_bookie:book_destroy(Identity1) catch _:_ -> ok end
        end
    catch
        Class:Reason:Stacktrace ->
            try leveled_bookie:book_destroy(Main0) catch _:_ -> ok end,
            try leveled_bookie:book_destroy(Identity0) catch _:_ -> ok end,
            erlang:raise(Class, Reason, Stacktrace)
    end.

residency_failure_is_sticky() ->
    Suffix = integer_to_list(erlang:unique_integer([positive])),
    MainRoot = testutil:reset_filestructure(
        "test/test_fts2_resident_failure_main_" ++ Suffix
    ),
    IdentityRoot = testutil:reset_filestructure(
        "test/test_fts2_resident_failure_identity_" ++ Suffix
    ),
    {ok, Main0} = leveled_bookie:book_start(start_opts(MainRoot)),
    {ok, Identity0} = leveled_bookie:book_start(start_opts(IdentityRoot)),
    Schema0 = (schema(<<"fts2-resident-failure">>))#{
        identity_bookie => Identity0
    },
    try
        ok = put_doc(Main0, Schema0, <<"a">>, <<"alpha">>),
        {ok, _} = leveled_fts:consolidate(Main0, Schema0, #{}),
        {ok, Root} = leveled_fts:fts2_available(Main0, Schema0),
        Generation = maps:get(generation, Root),
        ok = leveled_bookie:book_mput(Main0, [{
            add,
            maps:get(index, Schema0),
            <<"f2:bloom">>,
            <<"current">>,
            <<"corrupt-residency-fixture">>
        }]),
        ok = leveled_bookie:book_close(Main0),
        ok = leveled_bookie:book_close(Identity0),
        {ok, Main1} = leveled_bookie:book_start(start_opts(MainRoot)),
        {ok, Identity1} = leveled_bookie:book_start(start_opts(IdentityRoot)),
        Schema1 = Schema0#{identity_bookie := Identity1},
        try
            {Results, Counts} = trace_call_counts(
                fun() ->
                    [
                        leveled_fts:search(
                            Main1, Schema1, <<"alpha">>, #{rank => bm25}
                        )
                     || _ <- lists:seq(1, 2)
                    ]
                end,
                [{leveled_fts_residency, owner_load, 7}]
            ),
            Expected = {error, {fts_residency_load_failed,
                Generation, {error, function_clause}}},
            ?assertEqual([Expected, Expected], Results),
            ?assertEqual(
                1,
                maps:get({leveled_fts_residency, owner_load, 7}, Counts)
            ),
            Status = leveled_bookie:book_status(Main1),
            ?assertEqual(
                {error, function_clause},
                maps:get(fts_residency_load_error, Status)
            )
        after
            try leveled_bookie:book_destroy(Main1) catch _:_ -> ok end,
            try leveled_bookie:book_destroy(Identity1) catch _:_ -> ok end
        end
    catch
        Class:Reason:Stacktrace ->
            try leveled_bookie:book_destroy(Main0) catch _:_ -> ok end,
            try leveled_bookie:book_destroy(Identity0) catch _:_ -> ok end,
            erlang:raise(Class, Reason, Stacktrace)
    end.

cold_reopen_result_projection(#{count := Count, hits := Hits}) ->
    {Count, [
        {seam_hit_udi(Hit), maps:get(match_count, Hit)}
     || Hit <- Hits
    ]}.

generation_residency_budget_fallback() ->
    Suffix = integer_to_list(erlang:unique_integer([positive])),
    MainRoot = testutil:reset_filestructure(
        "test/test_fts2_resident_budget_main_" ++ Suffix
    ),
    IdentityRoot = testutil:reset_filestructure(
        "test/test_fts2_resident_budget_identity_" ++ Suffix
    ),
    {ok, Main} = leveled_bookie:book_start(
        [{fts_residency_budget, 256} | start_opts(MainRoot)]
    ),
    {ok, Identity} = leveled_bookie:book_start(
        [{fts_residency_budget, 256} | start_opts(IdentityRoot)]
    ),
    try
        Schema = (schema(<<"fts2-resident-budget">>))#{
            identity_bookie => Identity
        },
        ok = put_doc(Main, Schema, <<"a">>, <<"alpha beta">>),
        {ok, _} = leveled_fts:consolidate(Main, Schema, #{}),
        #{count := 1} = search(
            Main, Schema, <<"alpha">>, #{return_count => true}
        ),
        Status = leveled_bookie:book_status(Main),
        ?assert(maps:get(fts_resident_bytes, Status) =< 256),
        ?assertEqual(
            false,
            maps:get(
                term_header, maps:get(fts_resident_complete, Status)
            )
        )
    after
        try leveled_bookie:book_destroy(Main) catch _:_ -> ok end,
        try leveled_bookie:book_destroy(Identity) catch _:_ -> ok end
    end.

generation_and_identity_bookie() ->
    with_bookies(fun(Main, Identity) ->
        Schema = (schema(<<"fts2-generation">>))#{identity_bookie => Identity},
        ok = put_doc(Main, Schema, <<"a">>, <<"alpha thank you alpha">>),
        ok = put_doc(Main, Schema, <<"b">>, <<"alpha beta">>),
        ok = put_doc(Main, Schema, <<"c">>, <<"purchase order">>),
        %% An anchored token inside the phrase-prefix range creates an `a`
        %% row which sorts before a later `h` row in the resident ETS table.
        ok = put_doc(Main, Schema, <<"d">>, <<"ordnance only">>),
        {ok, #{skipped := []}} = leveled_fts:consolidate(Main, Schema, #{}),
        {ok, #{consolidated := [], skipped := []}} =
            leveled_fts:consolidate(Main, Schema, #{}),
        {ok, Root1} = leveled_fts:fts2_available(Main, Schema),
        Generation1 = maps:get(generation, Root1),
        IdentityKey1 = leveled_fts:fts2_codec_identity_key(Generation1),
        not_found = leveled_bookie:book_headonly(
            Main, maps:get(index, Schema), IdentityKey1, <<0:32>>
        ),
        {ok, _} = leveled_bookie:book_headonly(
            Identity, maps:get(index, Schema), IdentityKey1, <<0:32>>
        ),
        #{count := 2} = search(Main, Schema, <<"alpha">>, #{return_count => true}),
        Resident1 = leveled_bookie:book_status(Main),
        ?assertEqual(
            Generation1, maps:get(fts_resident_generation, Resident1)
        ),
        ?assert(maps:get(fts_resident_bytes, Resident1) > 0),
        ?assertEqual(
            true,
            maps:get(
                term_header, maps:get(fts_resident_complete, Resident1)
            )
        ),
        PhraseResults = [
            search(
                Main,
                Schema#{phrase_strategy => Strategy},
                <<"\"thank you\"">>,
                #{return_count => true, rank => bm25, resolve_hits => false}
            )
         || Strategy <- [skip, bigram]
        ],
        [#{count := 1}, #{count := 1}] = PhraseResults,
        [FirstPhrase | OtherPhrases] = PhraseResults,
        FirstPhraseKeys = [maps:get(candidate_key, Hit) || Hit <-
            maps:get(hits, FirstPhrase)],
        ?assert(lists:all(
            fun(Result) ->
                FirstPhraseKeys =:= [maps:get(candidate_key, Hit) || Hit <-
                    maps:get(hits, Result)]
            end,
            OtherPhrases
        )),
        #{count := 1} = search(
            Main,
            Schema,
            <<"\"purchase ord\"*">>,
            #{return_count => true, rank => bm25, resolve_hits => false}
        ),
        {ok, Manifest} = leveled_bookie:book_headonly(
            Main, maps:get(index, Schema), <<"doc">>, <<"a">>
        ),
        {ok, Specs} = leveled_fts:update(
            Schema, <<"a">>, #{body => <<"gamma">>, title => <<"a">>}, Manifest
        ),
        ok = leveled_bookie:book_mput(Main, Specs),
        DirtyResident = leveled_bookie:book_status(Main),
        ?assertEqual(
            undefined,
            maps:get(fts_resident_generation, DirtyResident)
        ),
        ?assertEqual(0, maps:get(fts_resident_bytes, DirtyResident)),
        #{count := 1} = search(Main, Schema, <<"alpha">>, #{return_count => true}),
        DirtyWindow = search(
            Main,
            Schema,
            <<"alpha OR gamma">>,
            #{return_count => true, rank => bm25, resolve_hits => false}
        ),
        {ok, #{skipped := []}} = leveled_fts:consolidate(Main, Schema, #{}),
        {ok, Root2} = leveled_fts:fts2_available(Main, Schema),
        ?assertNotEqual(Generation1, maps:get(generation, Root2)),
        {OldTermKey, _} = leveled_fts:fts2_codec_term_key(
            Generation1, 0, <<"alpha">>
        ),
        not_found = leveled_bookie:book_headonly(
            Main, maps:get(index, Schema), OldTermKey, <<"b">>
        ),
        not_found = leveled_bookie:book_headonly(
            Identity, maps:get(index, Schema), IdentityKey1, <<0:32>>
        ),
        #{count := 1} = search(Main, Schema, <<"gamma">>, #{return_count => true}),
        Resident2 = leveled_bookie:book_status(Main),
        ?assertEqual(
            maps:get(generation, Root2),
            maps:get(fts_resident_generation, Resident2)
        ),
        CleanWindow = search(
            Main,
            Schema,
            <<"alpha OR gamma">>,
            #{return_count => true, rank => bm25, resolve_hits => false}
        ),
        ?assertEqual(maps:get(count, DirtyWindow), maps:get(count, CleanWindow)),
        ?assertEqual(
            lists:sort([
                maps:get(candidate_key, Hit)
             || Hit <- maps:get(hits, DirtyWindow)
            ]),
            lists:sort([
                maps:get(candidate_key, Hit)
             || Hit <- maps:get(hits, CleanWindow)
            ])
        )
    end).

ranked_page_ties_use_candidate_order_test_() ->
    {timeout, 60, fun ranked_page_ties_use_candidate_order/0}.

ranked_group_total_and_ungrouped_retrieval_test_() ->
    {timeout, 60, fun ranked_group_total_and_ungrouped_retrieval/0}.

ranked_group_document_bm25_test_() ->
    {timeout, 60, fun ranked_group_document_bm25/0}.

grouped_boolean_uses_document_candidates_test_() ->
    {timeout, 60, fun grouped_boolean_uses_document_candidates/0}.

grouped_prefix_uses_complete_group_planes_test_() ->
    {timeout, 60, fun grouped_prefix_uses_complete_group_planes/0}.

phrase_does_not_cross_chunk_boundary_test_() ->
    {timeout, 60, fun phrase_does_not_cross_chunk_boundary/0}.

hpmor_bounded_multichunk_subset_test_() ->
    {timeout, 60, fun hpmor_bounded_multichunk_subset/0}.

ranked_group_total_and_ungrouped_retrieval() ->
    with_bookies(fun(Main, Identity) ->
        Schema = (seam_schema(<<"fts2-group-total">>))#{
            identity_bookie => Identity
        },
        Document = <<"one-logical-group">>,
        Tenant = <<"tenant">>,
        lists:foreach(
            fun({Key, Tf}) ->
                Body = iolist_to_binary([
                    binary:copy(<<"alpha ">>, Tf),
                    binary:copy(<<"filler ">>, 50 - Tf)
                ]),
                ok = seam_put(Main, Schema, Key, Document, Tenant, Body)
            end,
            [{<<"chunk-1">>, 5}, {<<"chunk-2">>, 40}, {<<"chunk-3">>, 7}]
        ),
        {ok, _} = leveled_fts:consolidate(Main, Schema, #{}),
        BaseOpts = #{
            columns => [content],
            limit => 20,
            rank => bm25,
            resolve_hits => false,
            return_count => true
        },
        #{count := 1, hits := [Grouped]} = search(
            Main, Schema, <<"alpha">>, BaseOpts
        ),
        ?assertEqual(Document, seam_hit_udi(Grouped)),
        ?assertEqual(52, maps:get(match_count, Grouped)),
        #{count := 3, hits := Ungrouped} = search(
            Main,
            Schema,
            <<"alpha">>,
            BaseOpts#{grouping => ungrouped}
        ),
        ?assertEqual(3, length(Ungrouped)),
        ?assertEqual(
            [{<<"chunk-1">>, 5}, {<<"chunk-2">>, 40}, {<<"chunk-3">>, 7}],
            lists:sort([
                {maps:get(chunk_key, maps:get(candidate_record, Hit)),
                    maps:get(match_count, Hit)}
             || Hit <- Ungrouped
            ])
        )
    end).

ranked_group_document_bm25() ->
    with_bookies(fun(Main, Identity) ->
        Schema = (seam_schema(<<"fts2-group-document-bm25">>))#{
            identity_bookie => Identity
        },
        Tenant = <<"tenant">>,
        %% The first logical document has 50 long chunks.  Every local tf=2
        %% posting loses to the short competitor under the legacy chunk-grain
        %% formula, while document-grain BM25 ranks the summed tf=100 first.
        lists:foreach(
            fun(Index) ->
                Key = <<"multi-", Index:16/unsigned-big>>,
                Tf = 2,
                Tokens = 1000,
                Body = iolist_to_binary([
                    binary:copy(<<"alpha ">>, Tf),
                    binary:copy(<<"filler ">>, Tokens - Tf)
                ]),
                ok = seam_put(
                    Main, Schema, Key, <<"multi">>, Tenant, Body
                )
            end,
            lists:seq(1, 50)
        ),
        ok = seam_put(
            Main,
            Schema,
            <<"short-1">>,
            <<"short">>,
            Tenant,
            binary:copy(<<"alpha ">>, 10)
        ),
        {ok, _} = leveled_fts:consolidate(Main, Schema, #{}),
        #{count := 2, hits := [First, Second]} = search(
            Main,
            Schema,
            <<"alpha">>,
            #{
                columns => [content],
                limit => 20,
                rank => bm25,
                resolve_hits => false,
                return_count => true
            }
        ),
        ?assertEqual(100, maps:get(match_count, First)),
        ?assertEqual(10, maps:get(match_count, Second)),
        ?assertEqual([<<"multi">>, <<"short">>], [
            seam_hit_udi(First), seam_hit_udi(Second)
        ]),
        GroupCount = 2,
        AverageLength = (50000 + 10) / GroupCount,
        Idf = erlang:max(
            math:log((GroupCount - 2 + 0.5) / (2 + 0.5)),
            1.0e-6
        ),
        Expected = Idf * (100 * 2.2) /
            (100 + 1.2 * (0.25 + 0.75 * (50000 / AverageLength))),
        ?assert(abs(maps:get(score, First) - Expected) < 1.0e-12)
    end).

grouped_boolean_uses_document_candidates() ->
    with_bookies(fun(Main, Identity) ->
        Schema = (seam_schema(<<"fts2-group-boolean">>))#{
            identity_bookie => Identity
        },
        Tenant = <<"tenant">>,
        ok = seam_put(
            Main, Schema, <<"both-a">>, <<"both">>, Tenant, <<"alpha">>
        ),
        ok = seam_put(
            Main, Schema, <<"both-b">>, <<"both">>, Tenant, <<"beta">>
        ),
        ok = seam_put(
            Main, Schema, <<"alpha-only">>, <<"alpha-only">>, Tenant,
            <<"alpha">>
        ),
        #{count := 1, hits := [DirtyAnd]} = search(
            Main,
            Schema,
            <<"alpha AND beta">>,
            #{
                columns => [content], limit => 20, rank => bm25,
                resolve_hits => false, return_count => true
            }
        ),
        ?assertEqual(2, maps:get(match_count, DirtyAnd)),
        {ok, _} = leveled_fts:consolidate(Main, Schema, #{}),
        Opts = #{
            columns => [content], limit => 20, rank => bm25,
            resolve_hits => false, return_count => true
        },
        #{count := 1, hits := [AndHit]} = search(
            Main, Schema, <<"alpha AND beta">>, Opts
        ),
        ?assertEqual(2, maps:get(match_count, AndHit)),
        #{count := 2, hits := OrHits} = search(
            Main, Schema, <<"alpha OR beta">>, Opts
        ),
        ?assertEqual(
            [{<<"both">>, 2}, {<<"alpha-only">>, 1}],
            [{seam_hit_udi(Hit), maps:get(match_count, Hit)} || Hit <- OrHits]
        ),
        #{count := 1, hits := [NotHit]} = search(
            Main, Schema, <<"alpha NOT beta">>, Opts
        ),
        ?assertEqual(1, maps:get(match_count, NotHit))
    end).

grouped_prefix_uses_complete_group_planes() ->
    with_bookies(fun(Main, Identity) ->
        Schema = (seam_schema(<<"fts2-group-prefix">>))#{
            identity_bookie => Identity
        },
        Tenant = <<"tenant">>,
        ok = seam_put(
            Main, Schema, <<"a-1">>, <<"a">>, Tenant,
            <<"hermione hermione">>
        ),
        ok = seam_put(
            Main, Schema, <<"a-2">>, <<"a">>, Tenant,
            <<"hermit hermit hermit">>
        ),
        ok = seam_put(
            Main, Schema, <<"a-3">>, <<"a">>, Tenant,
            <<"hermitage hermitages">>
        ),
        ok = seam_put(
            Main, Schema, <<"b-1">>, <<"b">>, Tenant, <<"hermitage">>
        ),
        {ok, _} = leveled_fts:consolidate(Main, Schema, #{}),
        #{count := 2, hits := Hits} = search(
            Main,
            Schema,
            <<"hermi*">>,
            #{
                columns => [content], limit => 20, rank => bm25,
                resolve_hits => false, return_count => true
            }
        ),
        ?assertEqual([<<"a">>, <<"b">>], [
            seam_hit_udi(Hit) || Hit <- Hits
        ]),
        [AHit, _BHit] = Hits,
        Idf = 1.0e-6,
        %% Document length covers all indexed columns.  The three chunks in
        %% group a therefore contribute seven content tokens plus three
        %% verbatim tenant tokens; group b contributes two tokens.
        GroupLength = 10,
        AverageLength = 6.0,
        ExpectedScore = lists:sum([
            Idf * (Tf * 2.2) /
                (Tf + 1.2 * (0.25 + 0.75 *
                    (GroupLength / AverageLength)))
         || Tf <- [2, 3, 1, 1]
        ]),
        ?assert(abs(maps:get(score, AHit) - ExpectedScore) < 1.0e-12),
        ?assertEqual([1, 7], lists:sort([
            maps:get(match_count, Hit) || Hit <- Hits
        ]))
    end).

phrase_does_not_cross_chunk_boundary() ->
    with_bookies(fun(Main, Identity) ->
        Schema = (seam_schema(<<"fts2-phrase-boundary">>))#{
            identity_bookie => Identity
        },
        Tenant = <<"tenant">>,
        ok = seam_put(
            Main, Schema, <<"cross-1">>, <<"cross">>, Tenant,
            <<"filler alpha">>
        ),
        ok = seam_put(
            Main, Schema, <<"cross-2">>, <<"cross">>, Tenant,
            <<"beta filler">>
        ),
        ok = seam_put(
            Main, Schema, <<"within-1">>, <<"within">>, Tenant,
            <<"filler alpha beta filler">>
        ),
        {ok, _} = leveled_fts:consolidate(Main, Schema, #{}),
        #{count := 1, hits := [Hit]} = search(
            Main,
            Schema,
            <<"\"alpha beta\"">>,
            #{
                columns => [content], limit => 20, rank => bm25,
                resolve_hits => false, return_count => true
            }
        ),
        ?assertEqual(<<"within">>, seam_hit_udi(Hit)),
        ?assertEqual(1, maps:get(match_count, Hit))
    end).

hpmor_bounded_multichunk_subset() ->
    with_bookies(fun(Main, Identity) ->
        Schema = (seam_schema(<<"fts2-hpmor-bounded">>))#{
            identity_bookie => Identity
        },
        Tenant = <<"tenant">>,
        %% A bounded semantic stand-in for the 65-chunk HPMOR fixture: keep
        %% enough repetitions to exercise multi-thousand document tf without
        %% making the permanent suite ingest the full 3.7 MB novel.
        lists:foreach(
            fun(Index) ->
                Key = <<"hpmor-", Index:16/unsigned-big>>,
                Body = iolist_to_binary([
                    binary:copy(<<"hermione ">>, 300),
                    <<"harry potter">>
                ]),
                ok = seam_put(Main, Schema, Key, <<"hpmor">>, Tenant, Body)
            end,
            lists:seq(1, 8)
        ),
        {ok, _} = leveled_fts:consolidate(Main, Schema, #{}),
        Opts = #{
            columns => [content], limit => 20, rank => bm25,
            resolve_hits => false, return_count => true
        },
        #{count := 1, hits := [Grouped]} = search(
            Main, Schema, <<"hermione">>, Opts
        ),
        ?assertEqual(
            {<<"hpmor">>, 2400},
            {seam_hit_udi(Grouped), maps:get(match_count, Grouped)}
        ),
        #{count := 8, hits := Ungrouped} = search(
            Main, Schema, <<"hermione">>, Opts#{grouping => ungrouped}
        ),
        ?assertEqual(
            [
                {<<"hpmor-", Index:16/unsigned-big>>, 300}
             || Index <- lists:seq(1, 8)
            ],
            [
                {maps:get(chunk_key, maps:get(candidate_record, Hit)),
                    maps:get(match_count, Hit)}
             || Hit <- Ungrouped
            ]
        ),
        #{count := 1, hits := [Phrase]} = search(
            Main, Schema, <<"\"harry potter\"">>, Opts
        ),
        ?assertEqual(
            {<<"hpmor">>, 8},
            {seam_hit_udi(Phrase), maps:get(match_count, Phrase)}
        )
    end).

ranked_page_ties_use_candidate_order() ->
    with_bookies(fun(Main, Identity) ->
        Schema = (schema(<<"fts2-ranked-ties">>))#{identity_bookie => Identity},
        lists:foreach(
            fun(Key) -> ok = put_doc(Main, Schema, Key, <<"equal rank">>) end,
            [<<"d">>, <<"c">>, <<"b">>, <<"a">>]
        ),
        {ok, _} = leveled_fts:consolidate(Main, Schema, #{}),
        #{hits := Hits} = search(
            Main,
            Schema,
            <<"equal">>,
            #{return_count => true, rank => bm25, limit => 2}
        ),
        ?assertEqual(
            [{<<"a">>, 1}, {<<"b">>, 1}],
            [{maps:get(key, Hit), maps:get(match_count, Hit)} || Hit <- Hits]
        )
    end).

bounded_or_page_matches_full_ranking_test_() ->
    {timeout, 120, fun bounded_or_page_matches_full_ranking/0}.

bounded_or_page_matches_full_ranking() ->
    with_bookies(fun(Main, Identity) ->
        Schema = (schema(<<"fts2-bounded-or-page">>))#{
            identity_bookie => Identity
        },
        lists:foreach(
            fun(I) ->
                Key = list_to_binary(io_lib:format("~4..0B", [I])),
                Body = case I =< 24 of
                    true ->
                        <<"alpha alpha alpha alpha beta beta beta beta ",
                            Key/binary>>;
                    false ->
                        Term = case I rem 2 of
                            0 -> <<"alpha ">>;
                            1 -> <<"beta ">>
                        end,
                        Filler = binary:copy(
                            <<"filler ">>, 1 + (I rem 300)
                        ),
                        <<Term/binary, Filler/binary, Key/binary>>
                end,
                case put_doc(Main, Schema, Key, Body) of
                    ok -> ok;
                    pause -> timer:sleep(1)
                end
            end,
            lists:seq(1, 1024)
        ),
        {ok, _} = leveled_fts:consolidate(Main, Schema, #{}),
        BaseOpts = #{
            return_count => true,
            rank => bm25,
            resolve_hits => false
        },
        {Fast, Counts} = trace_call_counts(
            fun() ->
                search(
                    Main, Schema, <<"alpha OR beta">>,
                    BaseOpts#{limit => 20}
                )
            end,
            [{leveled_fts, fts2_search_fast_or_bounded, 5}]
        ),
        Full = search(
            Main, Schema, <<"alpha OR beta">>, BaseOpts#{limit => 257}
        ),
        ?assertEqual(1024, maps:get(count, Fast)),
        ?assertEqual(maps:get(count, Full), maps:get(count, Fast)),
        ?assertEqual(
            lists:sublist(maps:get(hits, Full), 20),
            maps:get(hits, Fast)
        ),
        ?assertEqual(
            [
                {list_to_binary(io_lib:format("~4..0B", [I])), 8}
             || I <- lists:seq(1, 20)
            ],
            [
                {maps:get(title, maps:get(candidate_record, Hit)),
                    maps:get(match_count, Hit)}
             || Hit <- maps:get(hits, Fast)
            ]
        ),
        ?assertEqual(1, maps:get(
            {leveled_fts, fts2_search_fast_or_bounded, 5}, Counts
        ))
    end).

ranked_tie_fields_prepage_before_identity_test_() ->
    {timeout, 120, fun ranked_tie_fields_prepage_before_identity/0}.

ranked_tie_fields_prepage_before_identity() ->
    with_bookies(fun(Main, Identity) ->
        Schema = (schema(<<"fts2-ranked-tie-prepage">>))#{
            identity_bookie => Identity
        },
        ok = put_doc(Main, Schema, <<"0000">>, <<"alpha alpha alpha">>),
        ok = put_doc(Main, Schema, <<"0001">>, <<"alpha alpha">>),
        lists:foreach(
            fun(I) ->
                Key = list_to_binary(io_lib:format("~4..0B", [I])),
                ok = put_doc(Main, Schema, Key, <<"alpha filler">>)
            end,
            lists:seq(2, 1024)
        ),
        {ok, _} = leveled_fts:consolidate(Main, Schema, #{}),
        {ok, Root} = leveled_fts:fts2_available(Main, Schema),
        IdentityKey = leveled_fts:fts2_codec_identity_key(
            maps:get(generation, Root)
        ),
        ok = leveled_bookie:book_mput(Identity, [
            {remove, maps:get(index, Schema), IdentityKey, <<1:32>>, <<>>}
        ]),
        #{count := 1025, hits := Hits} = search(
            Main,
            Schema,
            <<"alpha">>,
            #{
                return_count => true,
                rank => bm25,
                rank_tie_fields => [title],
                limit => 2
            }
        ),
        ?assertEqual(
            [{<<"0000">>, 3}, {<<"0001">>, 2}],
            [{maps:get(key, Hit), maps:get(match_count, Hit)} || Hit <- Hits]
        )
    end).

production_page_only_contract_test_() ->
    {timeout, 180, fun production_page_only_contract/0}.

production_page_only_contract() ->
    with_bookies(fun(Main, Identity) ->
        Schema = (seam_schema(<<"fts2-production-page-only">>))#{
            identity_bookie => Identity
        },
        Total = 256,
        lists:foreach(
            fun(I) ->
                Document = list_to_binary(io_lib:format("~4..0B", [I])),
                Filler = binary:copy(<<"filler ">>, 1 + (I rem 16)),
                ok = seam_put(
                    Main, Schema, Document, Document, <<"default">>,
                    <<"alpha ", Filler/binary, Document/binary>>
                )
            end,
            lists:seq(1, Total)
        ),
        {ok, _} = leveled_fts:consolidate(Main, Schema, #{}),
        {ok, Root} = leveled_fts:fts2_available(Main, Schema),
        ?assertEqual(2, maps:get(identity_page_shift, Root)),
        ?assertEqual(64, maps:get(identity_page_count, Root)),
        Opts = production_page_only_opts(4),
        MFAs = [
            {leveled_fts, fts2_search_identity_page_handles, 4},
            {leveled_fts, fts2_search_hydrate_hit, 3},
            {leveled_fts, fts2_codec_decode_identity_chunk, 5},
            {leveled_fts, fts2_codec_decode_identity_pairs, 3}
        ],
        {{#{count := Total, hits := Hits}, Hydrated}, Counts} =
            trace_call_counts(
                fun() ->
                    Result = search(Main, Schema, <<"alpha">>, Opts),
                    Addresses = [
                        {maps:get(group_id, Hit), maps:get(chunk_id, Hit)}
                     || Hit <- maps:get(hits, Result)
                    ],
                    {ok, Rows} = leveled_fts:hydrate_page(
                        Main, Schema, Addresses
                    ),
                    {Result, Rows}
                end,
                MFAs
            ),
        ?assertEqual(4, length(Hits)),
        ?assertEqual(4, map_size(Hydrated)),
        ?assertEqual(
            [
                {list_to_binary(io_lib:format("~4..0B", [I])), 1}
             || I <- [16, 32, 48, 64]
            ],
            [
                begin
                    Row = maps:get(
                        {maps:get(group_id, Hit), maps:get(chunk_id, Hit)},
                        Hydrated
                    ),
                    Candidate = maps:get(candidate_record, Row),
                    {maps:get(chunk_key, Candidate), maps:get(match_count, Hit)}
                end
             || Hit <- Hits
            ]
        ),
        ?assert(lists:all(
            fun(Hit) ->
                maps:is_key(group_id, Hit) andalso
                    maps:is_key(chunk_id, Hit) andalso
                    not maps:is_key(candidate_record, Hit)
            end,
            Hits
        )),
        %% Phase one never materialises a full hit. Phase two decodes exactly
        %% the requested v4 rows, and never enters the generic map-pair codec.
        %% Native group-key order also means only phase two reads an identity
        %% page; the phase-one tie order is already the group-id order.
        ?assertEqual(1, maps:get(
            {leveled_fts, fts2_search_identity_page_handles, 4}, Counts
        )),
        ?assertEqual(0, maps:get({leveled_fts, fts2_search_hydrate_hit, 3}, Counts)),
        ?assertEqual(4, maps:get(
            {leveled_fts, fts2_codec_decode_identity_chunk, 5}, Counts
        )),
        ?assertEqual(0, maps:get(
            {leveled_fts, fts2_codec_decode_identity_pairs, 3}, Counts
        ))
    end).

production_selective_count_contract_test_() ->
    {timeout, 180, fun production_selective_count_contract/0}.

production_selective_count_contract() ->
    with_bookies(fun(Main, Identity) ->
        Schema = (seam_schema(<<"fts2-production-count-only">>))#{
            identity_bookie => Identity
        },
        %% Two chunks share each exact document identity. The count-only
        %% result is therefore 32 documents, never 64 chunks.
        lists:foreach(
            fun(I) ->
                DocumentNo = (I + 1) div 2,
                Document = list_to_binary(io_lib:format("~4..0B", [DocumentNo])),
                Chunk = list_to_binary(io_lib:format("~4..0B-~B", [DocumentNo, I rem 2])),
                Tenant = case DocumentNo rem 2 of
                    0 -> <<"default">>;
                    1 -> <<"other">>
                end,
                ok = seam_put(
                    Main, Schema, Chunk, Document, Tenant,
                    <<"alpha filler ", Chunk/binary>>
                )
            end,
            lists:seq(1, 128)
        ),
        {ok, _} = leveled_fts:consolidate(Main, Schema, #{}),
        #{hits := [], count := 32, count_kind := grouped,
            facet_universal := false} = search(
                Main,
                Schema,
                <<"alpha">>,
                production_count_only_opts(<<"default">>)
            )
    end).

term_planes_are_separate_rows_test_() ->
    {timeout, 180, fun term_planes_are_separate_rows/0}.

term_planes_are_separate_rows() ->
    with_bookies(fun(Main, Identity) ->
        Schema = (seam_schema(<<"fts2-term-plane-geometry">>))#{
            identity_bookie => Identity
        },
        lists:foreach(
            fun(I) ->
                Document = integer_to_binary(I),
                ok = seam_put(
                    Main, Schema, Document, Document, <<"default">>,
                    <<"alpha beta ", Document/binary>>
                )
            end,
            lists:seq(1, 8)
        ),
        {ok, _} = leveled_fts:consolidate(Main, Schema, #{}),
        {ok, RootValue} = leveled_bookie:book_headonly(
            Main, maps:get(index, Schema), <<"f2:root">>, <<"manifest">>
        ),
        <<1:8, RootBytes:32/unsigned-big, RootRaw:RootBytes/binary>> = RootValue,
        Root = binary_to_term(RootRaw, [safe]),
        ?assertEqual(1, maps:get(term_bloom, Root)),
        ?assertEqual(256, maps:get(term_bloom_shards, Root)),
        ?assertEqual(256, maps:get(bigram_bloom_shards, Root)),
        Generation = maps:get(generation, Root),
        {ok, <<1, Generation:64/unsigned-big, Bloom/binary>>} =
            leveled_bookie:book_headonly(
                Main, maps:get(index, Schema), <<"f2:bloom">>, <<"current">>
            ),
        ?assertEqual((1 bsl 20) div 8, byte_size(Bloom)),
        AlphaShard = erlang:phash2({0, <<"alpha">>}, 256),
        {ok, <<1, Generation:64/unsigned-big, AlphaBloom/binary>>} =
            leveled_bookie:book_headonly(
                Main, maps:get(index, Schema),
                <<"f2:bloom">>, <<"s", AlphaShard:8>>
            ),
        ?assertEqual((1 bsl 13) div 8, byte_size(AlphaBloom)),
        ReverseBigramShard = erlang:phash2(
            {0, <<"beta">>, <<"alpha">>}, 256
        ),
        {ok, <<1, Generation:64/unsigned-big, BigramBloom/binary>>} =
            leveled_bookie:book_headonly(
                Main, maps:get(index, Schema),
                <<"f2:bloom">>, <<"g", ReverseBigramShard:8>>
            ),
        ?assertEqual((1 bsl 16) div 8, byte_size(BigramBloom)),
        {TermKey, _} = leveled_fts:fts2_codec_term_key(
            maps:get(generation, Root), 0, <<"alpha">>
        ),
        <<"f2:t:", TermRest/binary>> = TermKey,
        {ok, Header} = leveled_bookie:book_headonly(
            Main, maps:get(index, Schema), TermKey, <<"h">>
        ),
        <<4:8, _GroupDf:32/unsigned-big, _ChunkDf:32/unsigned-big,
            _CollectionFrequency:64/unsigned-big,
            _ChampionCount:32/unsigned-big, _ChampionBytes:32/unsigned-big,
            0:32/unsigned-big, 0:32/unsigned-big, _/binary>> = Header,
        {ok, BooleanPlane} = leveled_bookie:book_headonly(
            Main, maps:get(index, Schema), <<"f2:b:", TermRest/binary>>, <<"b">>
        ),
        {ok, GroupPlane} = leveled_bookie:book_headonly(
            Main, maps:get(index, Schema), <<"f2:b:", TermRest/binary>>, <<"g">>
        ),
        {ok, PositionPlane} = leveled_bookie:book_headonly(
            Main, maps:get(index, Schema), <<"f2:p:", TermRest/binary>>, <<"p">>
        ),
        ?assertEqual(8, length(leveled_fts:fts2_codec_decode_plane(BooleanPlane))),
        ?assertEqual(8, length(leveled_fts:fts2_codec_decode_plane(GroupPlane))),
        ?assertEqual(8, map_size(
            leveled_fts:fts2_codec_decode_positions(PositionPlane)
        )),
        {#{count := 0, hits := []}, MissCounts} = trace_call_counts(
            fun() ->
                search(Main, Schema, <<"impossiblevocabularyterm">>, #{
                    rank => bm25,
                    limit => 20,
                    return_count => true,
                    resolve_hits => false
                })
            end,
            [{leveled_fts, fts2_search_fast_single_term, 7}]
        ),
        ?assertEqual(0, maps:get(
            {leveled_fts, fts2_search_fast_single_term, 7}, MissCounts
        )),
        {#{count := 0, hits := []}, PhraseMissCounts} = trace_call_counts(
            fun() ->
                search(Main, Schema, <<"\"beta alpha\"">>, #{
                    rank => bm25,
                    limit => 20,
                    return_count => true,
                    resolve_hits => false
                })
            end,
            [{leveled_fts, fts2_phrase_fast, 7}]
        ),
        ?assertEqual(0, maps:get(
            {leveled_fts, fts2_phrase_fast, 7}, PhraseMissCounts
        )),
        ok = seam_put(
            Main, Schema, <<"dirty">>, <<"dirty">>, <<"default">>,
            <<"alpha dirty">>
        ),
        ?assertEqual(
            {ok, <<0>>},
            leveled_bookie:book_headonly(
                Main, maps:get(index, Schema),
                <<"f2:state">>, <<"current">>
            )
        )
    end).

headonly_snapshot_close_protocol_test_() ->
    {timeout, 60, fun headonly_snapshot_close_protocol/0}.

headonly_snapshot_close_protocol() ->
    Suffix = integer_to_list(erlang:unique_integer([positive])),
    RelativeRoot = "test/test_fts2_snapshot_close_" ++ Suffix,
    Root = testutil:reset_filestructure(RelativeRoot),
    {ok, LiveBookie} = leveled_bookie:book_start(lifecycle_opts(Root)),
    {ok, SnapshotBookie} = leveled_bookie:book_start([
        {snapshot_bookie, LiveBookie}
    ]),
    ok = leveled_bookie:book_close(SnapshotBookie),
    ok = leveled_bookie:book_close(LiveBookie),
    _ = testutil:reset_filestructure(RelativeRoot),
    ok.

one_reopen_close_reclaims_journal_test_() ->
    {timeout, 120, fun one_reopen_close_reclaims_journal/0}.

one_reopen_close_reclaims_journal() ->
    Suffix = integer_to_list(erlang:unique_integer([positive])),
    Root = testutil:reset_filestructure("test/test_fts2_lifecycle_" ++ Suffix),
    Opts = lifecycle_opts(Root),
    {ok, Bookie0} = leveled_bookie:book_start(Opts),
    lists:foreach(
        fun(I) ->
            Key = <<I:32/unsigned-big>>,
            Value = binary:copy(<<(I band 255)>>, 512),
            Result = leveled_bookie:book_mput(
                Bookie0, [{add, <<"life">>, Key, null, Value}]
            ),
            ?assert(Result =:= ok orelse Result =:= pause)
        end,
        lists:seq(1, 6000)
    ),
    ok = leveled_bookie:book_trimjournal(Bookie0),
    ok = leveled_bookie:book_close(Bookie0),
    {ok, Bookie1} = leveled_bookie:book_start(Opts),
    ?assertEqual(
        {ok, binary:copy(<<1>>, 512)},
        leveled_bookie:book_headonly(
            Bookie1, <<"life">>, <<1:32/unsigned-big>>, null
        )
    ),
    ?assertEqual(
        {ok, binary:copy(<<112>>, 512)},
        leveled_bookie:book_headonly(
            Bookie1, <<"life">>, <<6000:32/unsigned-big>>, null
        )
    ),
    ok = leveled_bookie:book_trimjournal(Bookie1),
    ok = leveled_bookie:book_close(Bookie1),
    FirstStableBytes = tree_bytes(Root),
    {ok, Bookie2} = leveled_bookie:book_start(Opts),
    ok = leveled_bookie:book_trimjournal(Bookie2),
    ok = leveled_bookie:book_close(Bookie2),
    SecondStableBytes = tree_bytes(Root),
    %% A later reopen may finish normal ledger compaction and shrink the tree;
    %% the lifecycle invariant is that reopen/close does not grow it again.
    ?assert(SecondStableBytes =< FirstStableBytes + 4096),
    _ = testutil:reset_filestructure("test/test_fts2_lifecycle_" ++ Suffix),
    ok.

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

dirty_tail_presence_skips_impossible_fold_test_() ->
    {timeout, 60, fun dirty_tail_presence_skips_impossible_fold/0}.

dirty_tail_presence_skips_impossible_fold() ->
    with_bookies(fun(Main, Identity) ->
        Schema = (schema(<<"fts2-tail-presence">>))#{identity_bookie => Identity},
        ok = put_doc(Main, Schema, <<"base">>, <<"alpha">>),
        {ok, _} = leveled_fts:consolidate(Main, Schema, #{}),
        ok = put_doc(Main, Schema, <<"tail">>, <<"beta">>),
        Self = self(),
        Hook = fun({fts2, Deltas}) -> Self ! {tail_rows, length(Deltas)} end,
        #{count := 0} = search(
            Main,
            Schema,
            <<"gamma">>,
            #{return_count => true, tail_fold_hook => Hook}
        ),
        receive {tail_rows, 0} -> ok after 1000 -> ?assert(false) end,
        #{count := 1} = search(
            Main,
            Schema,
            <<"beta">>,
            #{return_count => true, tail_fold_hook => Hook}
        ),
        receive {tail_rows, 1} -> ok after 1000 -> ?assert(false) end
    end).

live_generation_reclaim_without_close_test_() ->
    {timeout, 120, fun live_generation_reclaim_without_close/0}.

live_generation_reclaim_without_close() ->
    Suffix = integer_to_list(erlang:unique_integer([positive])),
    MainRoot = testutil:reset_filestructure("test/test_fts2_reclaim_main_" ++ Suffix),
    IdentityRoot = testutil:reset_filestructure(
        "test/test_fts2_reclaim_identity_" ++ Suffix
    ),
    {ok, Main} = leveled_bookie:book_start(lifecycle_opts(MainRoot)),
    {ok, Identity} = leveled_bookie:book_start(lifecycle_opts(IdentityRoot)),
    Schema = (schema(<<"fts2-live-reclaim">>))#{identity_bookie => Identity},
    try
        lists:foreach(
            fun(I) ->
                Key = <<I:32/unsigned-big>>,
                Body = <<"alpha ", (integer_to_binary(I))/binary, " ",
                    (binary:copy(<<(I band 255)>>, 256))/binary>>,
                case put_doc(Main, Schema, Key, Body) of
                    ok -> ok;
                    pause -> timer:sleep(1)
                end
            end,
            lists:seq(1, 160)
        ),
        {ok, _} = leveled_fts:consolidate(Main, Schema, #{}),
        ok = leveled_bookie:book_reclaimledger(Main, 30000),
        ok = leveled_bookie:book_reclaimledger(Identity, 30000),
        Samples = lists:map(
            fun(Revision) ->
                Key = <<Revision:32/unsigned-big>>,
                {ok, Manifest} = leveled_bookie:book_headonly(
                    Main, maps:get(index, Schema), <<"doc">>, Key
                ),
                {ok, Specs} = leveled_fts:update(
                    Schema,
                    Key,
                    #{
                        body => <<"alpha revision ",
                            (integer_to_binary(Revision))/binary>>,
                        title => Key
                    },
                    Manifest
                ),
                ok = leveled_bookie:book_mput(Main, Specs),
                {ok, _} = leveled_fts:consolidate(Main, Schema, #{}),
                %% Reclaim is asynchronous with generation cleanup. Wait for
                %% it explicitly before comparing durable bytes so scheduler
                %% timing cannot turn normal pending compaction into a flake.
                ok = leveled_bookie:book_reclaimledger(Main, 30000),
                ok = leveled_bookie:book_reclaimledger(Identity, 30000),
                {
                    leveled_bookie:book_status(Main),
                    leveled_bookie:book_status(Identity)
                }
            end,
            lists:seq(1, 3)
        ),
        lists:foreach(
            fun({MainStatus, IdentityStatus}) ->
                ?assertEqual(0, maps:get(
                    ledger_delete_pending_files, MainStatus
                )),
                ?assertEqual(0, maps:get(
                    ledger_delete_pending_files, IdentityStatus
                )),
                ?assertEqual(0, maps:get(ledger_snapshot_count, MainStatus)),
                ?assertEqual(0, maps:get(
                    ledger_snapshot_count, IdentityStatus
                ))
            end,
            Samples
        )
    after
        try leveled_bookie:book_destroy(Main) catch _:_ -> ok end,
        try leveled_bookie:book_destroy(Identity) catch _:_ -> ok end
    end.

snapshot_protects_superseded_generation_until_release_test_() ->
    {timeout, 120, fun snapshot_protects_superseded_generation_until_release/0}.

snapshot_protects_superseded_generation_until_release() ->
    Suffix = integer_to_list(erlang:unique_integer([positive])),
    RelativeRoot = "test/test_fts2_reclaim_snapshot_" ++ Suffix,
    Root = testutil:reset_filestructure(RelativeRoot),
    {ok, Main} = leveled_bookie:book_start(lifecycle_opts(Root)),
    Schema = schema(<<"fts2-reclaim-snapshot">>),
    lists:foreach(
        fun(I) ->
            Key = <<I:32/unsigned-big>>,
            Body = <<"alpha snapshot ", (integer_to_binary(I))/binary, " ",
                (binary:copy(<<(I band 255)>>, 512))/binary>>,
            ok = put_doc(Main, Schema, Key, Body)
        end,
        lists:seq(1, 80)
    ),
    {ok, _} = leveled_fts:consolidate(Main, Schema, #{}),
    ok = leveled_bookie:book_reclaimledger(Main, 30000),
    OldFiles = filelib:wildcard(filename:join([Root, "ledger", "ledger_files", "*.sst"])),
    ?assert(OldFiles =/= []),
    {ok, SnapshotPenciller, SnapshotInker} =
        leveled_bookie:book_snapshot(Main, store, undefined, true),
    try
        {ok, Manifest} = leveled_bookie:book_headonly(
            Main, maps:get(index, Schema), <<"doc">>, <<1:32/unsigned-big>>
        ),
        {ok, Specs} = leveled_fts:update(
            Schema,
            <<1:32/unsigned-big>>,
            #{body => <<"alpha replacement">>, title => <<"one">>},
            Manifest
        ),
        ok = leveled_bookie:book_mput(Main, Specs),
        {ok, _} = leveled_fts:consolidate(
            Main, Schema, #{reclaim => false}
        ),
        ?assertEqual(
            {error, timeout},
            leveled_bookie:book_reclaimledger(Main, 100)
        ),
        ?assert(lists:all(fun filelib:is_regular/1, OldFiles)),
        ok = leveled_penciller:pcl_close(SnapshotPenciller),
        ok = leveled_inker:ink_close(SnapshotInker),
        ok = leveled_bookie:book_reclaimledger(Main, 30000),
        ?assert(lists:any(
            fun(Path) -> not filelib:is_regular(Path) end, OldFiles
        ))
    after
        case is_process_alive(SnapshotPenciller) of
            true ->
                try leveled_penciller:pcl_close(SnapshotPenciller)
                catch
                    _:_ -> ok
                end;
            false -> ok
        end,
        case is_process_alive(SnapshotInker) of
            true ->
                try leveled_inker:ink_close(SnapshotInker)
                catch
                    _:_ -> ok
                end;
            false -> ok
        end,
        try leveled_bookie:book_destroy(Main) catch _:_ -> ok end,
        _ = testutil:reset_filestructure(RelativeRoot)
    end.

expired_snapshot_releases_superseded_ssts_test_() ->
    {timeout, 120, fun expired_snapshot_releases_superseded_ssts/0}.

expired_snapshot_releases_superseded_ssts() ->
    Suffix = integer_to_list(erlang:unique_integer([positive])),
    RelativeRoot = "test/test_fts2_expired_snapshot_" ++ Suffix,
    Root = testutil:reset_filestructure(RelativeRoot),
    Opts = [{snapshot_timeout_long, 2} | lifecycle_opts(Root)],
    {ok, Main} = leveled_bookie:book_start(Opts),
    Schema = schema(<<"fts2-expired-snapshot">>),
    lists:foreach(
        fun(I) ->
            Key = <<I:32/unsigned-big>>,
            ok = put_doc(
                Main,
                Schema,
                Key,
                <<"alpha lease ", (integer_to_binary(I))/binary, " ",
                    (binary:copy(<<(I band 255)>>, 512))/binary>>
            )
        end,
        lists:seq(1, 80)
    ),
    {ok, _} = leveled_fts:consolidate(Main, Schema, #{}),
    ok = leveled_bookie:book_reclaimledger(Main, 30000),
    OldFiles = filelib:wildcard(
        filename:join([Root, "ledger", "ledger_files", "*.sst"])
    ),
    ?assert(OldFiles =/= []),
    {ok, Snapshot} = leveled_bookie:book_start([{snapshot_bookie, Main}]),
    try
        update_snapshot_fixture(Main, Schema, 1),
        ?assertEqual(
            {error, timeout},
            leveled_bookie:book_reclaimledger(Main, 100)
        ),
        Status0 = leveled_bookie:book_status(Main),
        ?assertEqual(1, maps:get(ledger_snapshot_count, Status0)),
        ?assert(maps:get(ledger_delete_pending_files, Status0) > 0),
        ?assert(is_integer(maps:get(
            ledger_oldest_snapshot_age_seconds, Status0
        ))),
        ?assert(lists:all(fun filelib:is_regular/1, OldFiles)),

        timer:sleep(2100),
        ?assertEqual(
            {error, snapshot_expired},
            leveled_bookie:book_headonly(
                Snapshot,
                maps:get(index, Schema),
                <<"doc">>,
                <<1:32/unsigned-big>>
            )
        ),
        lists:foreach(
            fun(Revision) ->
                update_snapshot_fixture(Main, Schema, Revision),
                ok = leveled_bookie:book_reclaimledger(Main, 30000)
            end,
            lists:seq(2, 4)
        ),
        ?assert(lists:any(
            fun(Path) -> not filelib:is_regular(Path) end, OldFiles
        )),
        Status1 = leveled_bookie:book_status(Main),
        ?assertEqual(0, maps:get(ledger_delete_pending_files, Status1)),
        ?assertEqual(0, maps:get(ledger_snapshot_count, Status1)),
        ?assertEqual(undefined, maps:get(
            ledger_oldest_snapshot_age_seconds, Status1
        )),

        {ok, FreshSnapshot} = leveled_bookie:book_start([
            {snapshot_bookie, Main}
        ]),
        ?assertMatch(
            {ok, _},
            leveled_bookie:book_headonly(
                FreshSnapshot,
                maps:get(index, Schema),
                <<"doc">>,
                <<1:32/unsigned-big>>
            )
        ),
        ok = leveled_bookie:book_close(FreshSnapshot)
    after
        case is_process_alive(Snapshot) of
            true ->
                try leveled_bookie:book_close(Snapshot)
                catch
                    _:_ -> ok
                end;
            false -> ok
        end,
        try leveled_bookie:book_destroy(Main) catch _:_ -> ok end,
        _ = testutil:reset_filestructure(RelativeRoot)
    end.

update_snapshot_fixture(Main, Schema, Revision) ->
    Key = <<1:32/unsigned-big>>,
    {ok, Manifest} = leveled_bookie:book_headonly(
        Main, maps:get(index, Schema), <<"doc">>, Key
    ),
    {ok, Specs} = leveled_fts:update(
        Schema,
        Key,
        #{
            body => <<"alpha lease revision ",
                (integer_to_binary(Revision))/binary>>,
            title => Key
        },
        Manifest
    ),
    ok = leveled_bookie:book_mput(Main, Specs),
    {ok, _} = leveled_fts:consolidate(Main, Schema, #{reclaim => false}),
    ok.

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

identity_group(GroupId, ChunkId, SourceId, Key, Candidate) ->
    #{
        group_id => GroupId,
        group_key => {group, [Key, [SourceId], nil]},
        chunks => [#{
            chunk_id => ChunkId,
            group_id => GroupId,
            source_id => SourceId,
            doc_key => Key,
            doc_version => <<SourceId:64/unsigned-big>>,
            doc_length => 12,
            candidate_record => Candidate,
            hit_record => #{
                doc_key => Key,
                record => #{title => maps:get(title, Candidate)},
                text_blocks => [{0, 0, 0}],
                text_bytes => 12
            }
        }]
    }.

production_page_only_opts(Limit) ->
    #{
        columns => [content],
        limit => Limit,
        offset => 0,
        page_only => true,
        rank => bm25,
        rank_tie_fields => [udi, path, ipath_vec, content_version],
        resolve_hits => false,
        return_count => true,
        return_positions => false,
        return_terms => false
    }.

production_count_only_opts(Facet) ->
    #{
        columns => [content],
        count_only => true,
        impact_facet => [Facet],
        limit => 0,
        offset => 0,
        rank => none,
        resolve_hits => false,
        return_count => true,
        return_positions => false,
        return_terms => false
    }.

seam_schema(Index) ->
    {ok, Schema} = leveled_fts:schema(#{
        index => Index,
        columns => [
            #{name => content, path => [content]},
            #{name => tenant, path => [tenant], mode => verbatim}
        ],
        text_field => [content],
        hit_fields => [udi, path, ipath_vec, tenant, content_version, chunk_key],
        candidate_fields => [udi, path, ipath_vec, tenant, content_version, chunk_key],
        candidate_filter_fields => [#{column => tenant, field => tenant}],
        candidate_group_fields => [udi, path, ipath_vec, content_version],
        candidate_version_field => content_version
    }),
    Schema.

seam_put(Bookie, Schema, Key, Document, Tenant, Body) ->
    {ok, Specs} = leveled_fts:derive(Schema, Key, #{
        content => Body,
        tenant => Tenant,
        udi => Document,
        path => <<"/", Document/binary>>,
        ipath_vec => [<<"root">>, Document],
        content_version => 1,
        chunk_key => Key
    }),
    leveled_bookie:book_mput(Bookie, Specs).

seam_hit_udi(Hit) ->
    maps:get(udi, maps:get(candidate_record, Hit)).

trace_call_counts(Fun, MFAs) ->
    lists:foreach(
        fun(MFA) ->
            1 = erlang:trace_pattern(MFA, true, [local, call_count]),
            1 = erlang:trace_pattern(MFA, restart, [local, call_count])
        end,
        MFAs
    ),
    try
        Result = Fun(),
        Counts = maps:from_list([
            begin
                {call_count, Count} = erlang:trace_info(MFA, call_count),
                {MFA, Count}
            end
         || MFA <- MFAs
        ]),
        {Result, Counts}
    after
        lists:foreach(
            fun(MFA) -> erlang:trace_pattern(MFA, false, [local, call_count]) end,
            MFAs
        )
    end.

tokenizer_parity_inputs() ->
    Quirk = <<16#F0, 16#9F, 16#92>>,
    [
        <<>>,
        <<"ASCII lowercase 123 AND MixedCase">>,
        binary:copy(<<"Ab9">>, 300),
        <<Quirk/binary, "a">>,
        <<"a", Quirk/binary, "b">>,
        <<Quirk/binary, Quirk/binary, "x">>,
        <<16#80, 16#FF, "bad", 16#C3>>,
        unicode:characters_to_binary("CAFÉ naïve résumé ΣΟΦΟΣ σοφος"),
        unicode:characters_to_binary("日本語 한국어 中文"),
        unicode:characters_to_binary("a\x{0301}\x{0302}b")
    ].

tree_bytes(Path) ->
    case file:list_dir(Path) of
        {ok, Entries} ->
            lists:sum([tree_bytes(filename:join(Path, Entry)) || Entry <- Entries]);
        {error, enotdir} ->
            filelib:file_size(Path)
    end.

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

lifecycle_opts(Root) ->
    [
        {root_path, Root},
        {head_only, with_lookup},
        {sync_strategy, testutil:sync_strategy()},
        {compression_method, none},
        {ledger_compression, none},
        {cache_size, 100},
        {max_pencillercachesize, 500},
        {max_journalsize, 1000000},
        {log_level, warning}
    ].

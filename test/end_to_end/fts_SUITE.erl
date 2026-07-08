-module(fts_SUITE).

-include("leveled.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([
    single_object_contract/1,
    batchput_contract/1,
    multi_token_phrase_contract/1,
    index_update_contract/1,
    concurrent_generation_contract/1,
    anchor_update_column_negative_contract/1,
    rejected_fast_path_regression_contract/1,
    metadata_representation_contract/1,
    hot_term_metadata_split_contract/1,
    metadata_rank_none_limit_order_contract/1,
    metadata_limited_pair_update_reopen_contract/1,
    invalid_write_inputs/1,
    private_snapshot_contract/1,
    regular_index_snapshot_contract/1,
    metadata_index_contract/1,
    tenant_bucket_prefix_contract/1,
    external_term_decode_contract/1,
    failed_batch_sequence_contract/1,
    oversized_token_contract/1,
    recovery_and_hotbackup/1,
    partial_tail_recovery_contract/1,
    recalc_reload_contract/1,
    sqlite_supported_ast_differential_contract/1,
    unicode61_supported_parity_corpus_contract/1,
    parse_errors/1,
    filter_and_verbatim_contract/1
]).

all() ->
    [
        single_object_contract,
        batchput_contract,
        multi_token_phrase_contract,
        index_update_contract,
        concurrent_generation_contract,
        anchor_update_column_negative_contract,
        rejected_fast_path_regression_contract,
        metadata_representation_contract,
        hot_term_metadata_split_contract,
        metadata_rank_none_limit_order_contract,
        metadata_limited_pair_update_reopen_contract,
        invalid_write_inputs,
        private_snapshot_contract,
        regular_index_snapshot_contract,
        metadata_index_contract,
        tenant_bucket_prefix_contract,
        external_term_decode_contract,
        failed_batch_sequence_contract,
        oversized_token_contract,
        recovery_and_hotbackup,
        partial_tail_recovery_contract,
        recalc_reload_contract,
        sqlite_supported_ast_differential_contract,
        unicode61_supported_parity_corpus_contract,
        parse_errors,
        filter_and_verbatim_contract
    ].

init_per_suite(Config) ->
    testutil:init_per_suite([{suite, "fts"} | Config]),
    Config.

end_per_suite(Config) ->
    testutil:end_per_suite(Config).

single_object_contract(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),

    ok =
        fts_put(
            Bookie,
            <<"docs">>,
            <<"1">>,
            <<"obj1">>,
            <<"main">>,
            #{body => <<"quick brown fox quick">>, title => <<"Alpha">>},
            #{
                prefixes => [3],
                index_specs => [{add, <<"kind_bin">>, <<"guide">>}]
            }
        ),
    [<<"1">>] =
        metadata_search_term_keys(Bookie, <<"docs">>, <<"main">>, <<"quick">>, <<"body">>),
    [<<"1">>] =
        metadata_search_prefix_keys(
            Bookie, <<"docs">>, <<"main">>, 3, <<"qui">>, <<"quick">>, <<"body">>
        ),
    ok =
        fts_put(
            Bookie,
            <<"docs">>,
            <<"2">>,
            <<"obj2">>,
            <<"main">>,
            #{body => <<"quick blue hare">>, title => <<"Beta">>},
            #{prefixes => [3]}
        ),

    [<<"1">>] = keys(search(Bookie, <<"docs">>, <<"main">>, <<"\"quick brown\"">>, #{})),
    [<<"1">>] =
        keys(search(Bookie, <<"docs">>, <<"main">>, <<"qui* AND fox">>, #{
            prefixes => [3]
        })),
    [<<"1">>] = keys(search(Bookie, <<"docs">>, <<"main">>, <<"bro*">>, #{})),
    SearchSchemaOpts = #{columns => [title, body]},
    {async, TitleNoOpts} =
        leveled_bookie:book_ftssearch(Bookie, <<"docs">>, <<"main">>, <<"title:beta">>, #{}),
    {ok, TitleNoOptsHits} = TitleNoOpts(),
    [<<"2">>] = keys(TitleNoOptsHits),
    [<<"2">>] =
        keys(search(Bookie, <<"docs">>, <<"main">>, <<"title:beta">>, SearchSchemaOpts)),
    [] =
        keys(
            search(
                Bookie,
                <<"docs">>,
                <<"main">>,
                <<"NEAR(title:alpha body:quick, 5)">>,
                SearchSchemaOpts
            )
        ),
    [] =
        keys(
            search(
                Bookie,
                <<"docs">>,
                <<"main">>,
                <<"title:(beta body:quick)">>,
                SearchSchemaOpts
            )
        ),
    [<<"1">>, <<"2">>] =
        keys(
            search(Bookie, <<"docs">>, <<"main">>, <<"{title body}:quick">>, SearchSchemaOpts)
        ),
    [<<"1">>, <<"2">>] =
        keys(search(Bookie, <<"docs">>, <<"main">>, <<"-title:quick">>, SearchSchemaOpts)),
    [] = keys(search(Bookie, <<"docs">>, <<"main">>, <<"-body:quick">>, SearchSchemaOpts)),
    [<<"1">>] =
        keys(search(Bookie, <<"docs">>, <<"main">>, <<"-{body}:alpha">>, SearchSchemaOpts)),
    [<<"1">>] = keys(search(Bookie, <<"docs">>, <<"main">>, <<"quick NOT blue">>, #{})),
    [<<"1">>] = keys(search(Bookie, <<"docs">>, <<"main">>, <<"NEAR(quick fox, 2)">>, #{})),
    ok =
        fts_put(
            Bookie,
            <<"near-boundary">>,
            <<"3">>,
            <<"obj3">>,
            <<"main">>,
            #{
                body =>
                    <<"nearleft a b c d e f g h i j nearright">>,
                title => <<"Gamma">>
            },
            #{prefixes => [3]}
        ),
    [<<"3">>] =
        keys(
            search(
                Bookie,
                <<"near-boundary">>,
                <<"main">>,
                <<"NEAR(nearleft nearright, 10)">>,
                #{}
            )
        ),
    [] =
        keys(
            search(
                Bookie,
                <<"near-boundary">>,
                <<"main">>,
                <<"NEAR(nearleft nearright, 9)">>,
                #{}
            )
        ),
    {async, BM25Runner} =
        leveled_bookie:book_ftssearch(
            Bookie, <<"docs">>, <<"main">>, <<"quick">>, #{columns => [body], rank => bm25}
        ),
    {error, invalid_rank_option} = BM25Runner(),
    [<<"1">>, <<"2">>] =
        hit_keys(search(Bookie, <<"docs">>, <<"main">>, <<"quick">>, #{rank => none})),
    [<<"2">>] =
        hit_keys(
            search(Bookie, <<"docs">>, <<"main">>, <<"quick">>, #{
                rank => none,
                limit => 1,
                offset => 1
            })
        ),
    {async, StatsRunner} =
        leveled_bookie:book_ftssearch(
            Bookie,
            <<"docs">>,
            <<"main">>,
            <<"quick">>,
            #{columns => [body], stats => supplied, doc_count => 2, avgdl => 3.0}
        ),
    {error, invalid_fts_options} = StatsRunner(),
    [<<"1">>] = index_keys(Bookie, <<"docs">>, <<"kind_bin">>, <<"guide">>),

    [PositionHit | _] =
        search(Bookie, <<"docs">>, <<"main">>, <<"quick">>, #{return_positions => true}),
    #{positions := Positions} = PositionHit,
    true = map_size(Positions) > 0,

    [RankNonePositionHit | _] =
        search(Bookie, <<"docs">>, <<"main">>, <<"quick">>, #{
            rank => none,
            return_positions => true
        }),
    #{positions := RankNonePositions} = RankNonePositionHit,
    true = map_size(RankNonePositions) > 0,

    ok =
        fts_put(
            Bookie,
            <<"docs">>,
            <<"1">>,
            <<"obj1b">>,
            <<"main">>,
            #{body => <<"slow brown dog">>},
            #{
                columns => [body, title],
                prefixes => [3],
                index_specs => [
                    {remove, <<"kind_bin">>, <<"guide">>},
                    {add, <<"kind_bin">>, <<"article">>}
                ]
            }
        ),
    [<<"2">>] = keys(search(Bookie, <<"docs">>, <<"main">>, <<"quick">>, #{})),
    [<<"1">>] = keys(search(Bookie, <<"docs">>, <<"main">>, <<"slow">>, #{})),
    [] = metadata_search_term_keys(Bookie, <<"docs">>, <<"main">>, <<"fox">>, <<"body">>),
    [] =
        metadata_search_prefix_keys(
            Bookie, <<"docs">>, <<"main">>, 3, <<"fox">>, <<"fox">>, <<"body">>
        ),
    [<<"1">>] =
        metadata_search_term_keys(Bookie, <<"docs">>, <<"main">>, <<"slow">>, <<"body">>),
    [<<"1">>] =
        metadata_search_prefix_keys(
            Bookie, <<"docs">>, <<"main">>, 3, <<"slo">>, <<"slow">>, <<"body">>
        ),
    [] = index_keys(Bookie, <<"docs">>, <<"kind_bin">>, <<"guide">>),
    [<<"1">>] = index_keys(Bookie, <<"docs">>, <<"kind_bin">>, <<"article">>),
    {ok, {<<"obj1b">>, #{body := <<"slow brown dog">>}}} =
        leveled_bookie:book_get(Bookie, <<"docs">>, <<"1">>),

    ok = fts_delete(Bookie, <<"docs">>, <<"2">>, <<"main">>, #{}),
    [<<"1">>] = keys(search(Bookie, <<"docs">>, <<"main">>, all_docs, #{})),
    [] = metadata_search_term_keys(Bookie, <<"docs">>, <<"main">>, <<"quick">>, <<"body">>),
    not_found = leveled_bookie:book_get(Bookie, <<"docs">>, <<"2">>),

    ok = leveled_bookie:book_close(Bookie).

batchput_contract(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),

    ok =
        fts_batchput(
            Bookie,
            [
                {fts_put, <<"batch">>, <<"1">>, <<"obj1">>, <<"main">>,
                    #{body => <<"red apple">>}, #{prefixes => [3]}},
                {fts_put, <<"batch">>, <<"2">>, <<"obj2">>, <<"main">>,
                    #{body => <<"green apple">>},
                    #{
                        prefixes => [3],
                        index_specs => [{add, <<"batch_kind_bin">>, <<"fruit">>}]
                    }}
            ],
            true
        ),
    [<<"1">>, <<"2">>] =
        keys(search(Bookie, <<"batch">>, <<"main">>, <<"apple">>, #{})),
    [<<"2">>] =
        metadata_search_term_keys(Bookie, <<"batch">>, <<"main">>, <<"green">>, <<"body">>),
    [<<"2">>] =
        metadata_search_prefix_keys(
            Bookie, <<"batch">>, <<"main">>, 3, <<"gre">>, <<"green">>, <<"body">>
        ),
    [<<"2">>] = index_keys(Bookie, <<"batch">>, <<"batch_kind_bin">>, <<"fruit">>),

    ok =
        fts_batchput(
            Bookie,
            [
                {fts_put, <<"batch">>, <<"2">>, <<"obj2b">>, <<"main">>,
                    #{body => <<"yellow pear">>},
                    #{
                        prefixes => [3],
                        index_specs => [
                            {remove, <<"batch_kind_bin">>, <<"fruit">>},
                            {add, <<"batch_kind_bin">>, <<"dessert">>}
                        ]
                    }}
            ]
        ),
    [<<"1">>] = keys(search(Bookie, <<"batch">>, <<"main">>, <<"apple">>, #{})),
    [] = keys(search(Bookie, <<"batch">>, <<"main">>, <<"green">>, #{})),
    [] =
        keys(search(Bookie, <<"batch">>, <<"main">>, <<"gre*">>, #{prefixes => [3]})),
    [] = metadata_search_term_keys(Bookie, <<"batch">>, <<"main">>, <<"green">>, <<"body">>),
    [] =
        metadata_search_prefix_keys(
            Bookie, <<"batch">>, <<"main">>, 3, <<"gre">>, <<"green">>, <<"body">>
        ),
    [<<"2">>] = keys(search(Bookie, <<"batch">>, <<"main">>, <<"pear">>, #{})),
    [<<"2">>] =
        keys(search(Bookie, <<"batch">>, <<"main">>, <<"pea*">>, #{prefixes => [3]})),
    [<<"2">>] =
        metadata_search_prefix_keys(
            Bookie, <<"batch">>, <<"main">>, 3, <<"pea">>, <<"pear">>, <<"body">>
        ),
    [<<"2">>] =
        metadata_search_term_keys(Bookie, <<"batch">>, <<"main">>, <<"pear">>, <<"body">>),
    [] = index_keys(Bookie, <<"batch">>, <<"batch_kind_bin">>, <<"fruit">>),
    [<<"2">>] =
        index_keys(Bookie, <<"batch">>, <<"batch_kind_bin">>, <<"dessert">>),
    [<<"1">>, <<"2">>] =
        keys(search(Bookie, <<"batch">>, <<"main">>, all_docs, #{})),
    {ok, {<<"obj2b">>, #{body := <<"yellow pear">>}}} =
        leveled_bookie:book_get(Bookie, <<"batch">>, <<"2">>),

    ok =
        fts_put(
            Bookie,
            <<"batch">>,
            <<"idx">>,
            <<"idx-main">>,
            <<"main">>,
            #{body => <<"mainonly">>},
            #{}
        ),
    {error, missing_fts_schema} =
        fts_put(
            Bookie,
            <<"batch">>,
            <<"idx">>,
            <<"idx-alt">>,
            <<"alt">>,
            #{body => <<"altonly">>},
            #{}
        ),
    {error, missing_fts_schema} =
        fts_delete(Bookie, <<"batch">>, <<"idx">>, <<"alt">>, #{}),
    [<<"idx">>] = keys(search(Bookie, <<"batch">>, <<"main">>, <<"mainonly">>, #{})),
    assert_missing_fts_schema(Bookie, <<"batch">>, <<"alt">>, <<"altonly">>, #{}),
    {ok, {<<"idx-main">>, #{body := <<"mainonly">>}}} =
        leveled_bookie:book_get(Bookie, <<"batch">>, <<"idx">>),

    ok =
        fts_put(
            Bookie,
            <<"batch">>,
            <<"idx-normalised">>,
            <<"idx-normalised-main">>,
            <<"main">>,
            #{body => <<"normalised old">>},
            #{}
        ),
    ok =
        fts_put(
            Bookie,
            <<"batch">>,
            <<"idx-normalised">>,
            <<"idx-normalised-atom">>,
            main,
            #{body => <<"normalised new">>},
            #{}
        ),
    [] = keys(search(Bookie, <<"batch">>, <<"main">>, <<"old">>, #{})),
    [<<"idx-normalised">>] =
        keys(search(Bookie, <<"batch">>, <<"main">>, <<"new">>, #{})),
    ok =
        fts_delete(
            Bookie, <<"batch">>, <<"idx-normalised">>, main, #{}
        ),
    [] = keys(search(Bookie, <<"batch">>, <<"main">>, <<"new">>, #{})),

    ok =
        fts_put(
            Bookie,
            <<"batch">>,
            <<"idx-batch">>,
            <<"idx-batch-main">>,
            <<"main">>,
            #{body => <<"batchmainonly">>},
            #{}
        ),
    {error, missing_fts_schema} =
        fts_batchput(
            Bookie,
            [
                {fts_put, <<"batch">>, <<"idx-batch">>, <<"idx-batch-alt">>, <<"alt">>,
                    #{body => <<"batchaltonly">>}, #{}}
            ]
        ),
    {error, missing_fts_schema} =
        fts_batchput(
            Bookie,
            [
                {fts_delete, <<"batch">>, <<"idx-batch">>, <<"alt">>, #{}}
            ]
        ),
    [<<"idx-batch">>] =
        keys(search(Bookie, <<"batch">>, <<"main">>, <<"batchmainonly">>, #{})),
    assert_missing_fts_schema(Bookie, <<"batch">>, <<"alt">>, <<"batchaltonly">>, #{}),
    {ok, {<<"idx-batch-main">>, #{body := <<"batchmainonly">>}}} =
        leveled_bookie:book_get(Bookie, <<"batch">>, <<"idx-batch">>),

    {error, invalid_index_specs} =
        leveled_bookie:book_batchput(
            Bookie,
            [
                {put, <<"raw">>, <<"metadata-spec">>, <<"obj">>,
                    [{add, <<"ordinary_idx">>, <<"term">>, <<"metadata">>}],
                    ?STD_TAG, infinity}
            ]
        ),
    not_found = leveled_bookie:book_get(Bookie, <<"raw">>, <<"metadata-spec">>),
    {error, {invalid_batch_object_spec, {put, _, _, _, _, _, _}}} =
        fts_batchput(
            Bookie,
            [
                {put, <<"batch">>, <<"idx">>, <<"raw-over-fts">>, [],
                    ?STD_TAG, infinity}
            ]
        ),
	    [<<"idx">>] = keys(search(Bookie, <<"batch">>, <<"main">>, <<"mainonly">>, #{})),
	    {error, {invalid_batch_object_spec, {delete, _, _, _, _, _}}} =
	        fts_batchput(
            Bookie,
            [
                {delete, <<"batch">>, <<"idx">>, [], ?STD_TAG, infinity}
            ]
	        ),
	    [<<"idx">>] = keys(search(Bookie, <<"batch">>, <<"main">>, <<"mainonly">>, #{})),

	    ok =
	        fts_put(
		            Bookie, <<"raw-contract">>, <<"raw-direct">>, <<"fts-object">>, <<"main">>,
	            #{body => <<"rawdirect">>}, #{}
	        ),
		    ok = leveled_bookie:book_put(Bookie, <<"raw-contract">>, <<"raw-direct">>, <<"raw-object">>, []),
		    {ok, <<"raw-object">>} = leveled_bookie:book_get(Bookie, <<"raw-contract">>, <<"raw-direct">>),
		    ok = leveled_bookie:book_delete(Bookie, <<"raw-contract">>, <<"raw-direct">>, []),
		    not_found = leveled_bookie:book_get(Bookie, <<"raw-contract">>, <<"raw-direct">>),
	    ok =
	        fts_put(
		            Bookie, <<"raw-contract">>, <<"raw-batch-put">>, <<"fts-object">>, <<"main">>,
	            #{body => <<"rawbatchput">>}, #{}
	        ),
	    ok =
	        leveled_bookie:book_batchput(
	            Bookie,
	            [
		                {put, <<"raw-contract">>, <<"raw-batch-put">>, <<"raw-normal-batch">>, [],
	                    ?STD_TAG, infinity}
	            ]
	        ),
	    {ok, <<"raw-normal-batch">>} =
	        leveled_bookie:book_get(Bookie, <<"raw-contract">>, <<"raw-batch-put">>),
	    ok =
	        fts_put(
	            Bookie, <<"raw-contract">>, <<"raw-batch-delete">>, <<"fts-object">>, <<"main">>,
	            #{body => <<"rawbatchdelete">>}, #{}
	        ),
	    ok =
	        leveled_bookie:book_batchput(
	            Bookie,
	            [
	                {delete, <<"raw-contract">>, <<"raw-batch-delete">>, [], ?STD_TAG, infinity}
	            ]
	        ),
	    not_found = leveled_bookie:book_get(Bookie, <<"raw-contract">>, <<"raw-batch-delete">>),
	    [<<"idx">>] = keys(search(Bookie, <<"batch">>, <<"main">>, <<"mainonly">>, #{})),

	    {error, {duplicate_fts_batch_key, _}} =
	        fts_batchput(
	            Bookie,
	            [
	                {fts_put, <<"batch">>, <<"3">>, <<"obj3">>, <<"main">>,
	                    #{body => <<"one">>}, #{}},
	                {fts_put, <<"batch">>, <<"3">>, <<"obj3">>, <<"main">>,
	                    #{body => <<"two">>}, #{}}
	            ]
	        ),
	    [] = keys(search(Bookie, <<"batch">>, <<"main">>, <<"one">>, #{})),
	    [] = keys(search(Bookie, <<"batch">>, <<"main">>, <<"two">>, #{})),
	    ok =
	        fts_batchput(
	            Bookie,
	            [
	                {fts_put, <<"batch">>, <<"3">>, <<"obj3">>, <<"main">>,
	                    #{body => <<"two">>}, #{}}
	            ]
	        ),
	    [<<"3">>] = keys(search(Bookie, <<"batch">>, <<"main">>, <<"two">>, #{})),

    {error, {invalid_batch_object_spec, {put, _, _, _, _, _, _}}} =
        fts_batchput(
            Bookie,
            [
                {put, <<"batch">>, <<"4">>, <<"raw">>, [], ?STD_TAG, infinity},
                {fts_put, <<"batch">>, <<"4">>, <<"obj4">>, <<"main">>,
                    #{body => <<"four">>}, #{}}
            ]
        ),

    {error, missing_fts_schema} =
        fts_batchput(
            Bookie,
            [
                {fts_put, <<"batch">>, <<"5">>, <<"obj5">>, <<"main">>,
                    #{body => <<"five">>}, #{}},
                {fts_put, <<"batch">>, <<"5">>, <<"obj5">>, <<"alt">>,
                    #{body => <<"five">>}, #{}}
            ]
        ),

    {error, {invalid_batch_object_spec, bogus}} =
        fts_batchput(
            Bookie,
            [
                {fts_put, <<"batch">>, <<"6">>, <<"obj6">>, <<"main">>,
                    #{body => <<"uniquetoken">>}, #{}},
                bogus
            ]
        ),
    [] = search(Bookie, <<"batch">>, <<"main">>, <<"uniquetoken">>, #{}),

    ok =
        fts_batchput(
            Bookie,
            [{fts_delete, <<"batch">>, <<"1">>, <<"main">>, #{}}]
        ),
    {error, {invalid_batch_object_spec, {put, _, _, _, _, _, _}}} =
        fts_batchput(
            Bookie,
            [
                {put, <<"batch">>, <<"1">>, <<"raw-over-fts-delete-marker">>, [],
                    ?STD_TAG, infinity}
            ]
        ),
    [<<"2">>, <<"3">>, <<"idx">>, <<"idx-batch">>] =
        keys(search(Bookie, <<"batch">>, <<"main">>, all_docs, #{})),
    [] = metadata_search_term_keys(Bookie, <<"batch">>, <<"main">>, <<"red">>, <<"body">>),

    ok =
        fts_batchput(
            Bookie,
            [
                {fts_delete, <<"batch">>, <<"2">>, <<"main">>,
                    #{index_specs => [{remove, <<"batch_kind_bin">>, <<"dessert">>}]}}
            ]
        ),
    [<<"3">>, <<"idx">>, <<"idx-batch">>] =
        keys(search(Bookie, <<"batch">>, <<"main">>, all_docs, #{})),
    [] = keys(search(Bookie, <<"batch">>, <<"main">>, <<"pear">>, #{})),
    [] =
        keys(search(Bookie, <<"batch">>, <<"main">>, <<"pea*">>, #{prefixes => [3]})),
    [] = metadata_search_term_keys(Bookie, <<"batch">>, <<"main">>, <<"pear">>, <<"body">>),
    [] =
        metadata_search_prefix_keys(
            Bookie, <<"batch">>, <<"main">>, 3, <<"pea">>, <<"pear">>, <<"body">>
        ),
    [] = index_keys(Bookie, <<"batch">>, <<"batch_kind_bin">>, <<"dessert">>),
    [<<"3">>, <<"idx">>, <<"idx-batch">>] =
        fts_doc_keys(Bookie, <<"batch">>, <<"main">>),
    not_found = leveled_bookie:book_get(Bookie, <<"batch">>, <<"2">>),

    ok = leveled_bookie:book_close(Bookie).

multi_token_phrase_contract(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),
    Bucket = <<"phrase">>,
    Index = <<"main">>,
    WriteOpts = #{columns => [title, body], prefixes => [3]},
    SearchOpts = #{columns => [title, body], prefixes => [3], rank => none},

    ok =
        fts_batchput(
            Bookie,
            [
                {fts_put, Bucket, <<"exact">>, <<"exact-object">>, Index,
                    #{
                        body => <<"alpha beta gamma delta repeat repeat gamma">>,
                        title => <<"alpha beta gamma">>
                    },
                    WriteOpts},
                {fts_put, Bucket, <<"gap">>, <<"gap-object">>, Index,
                    #{
                        body => <<"alpha beta filler gamma delta">>,
                        title => <<"gamma beta alpha">>
                    },
                    WriteOpts},
                {fts_put, Bucket, <<"column">>, <<"column-object">>, Index,
                    #{
                        body => <<"alpha beta miss">>,
                        title => <<"alpha beta gamma">>
                    },
                    WriteOpts},
                {fts_put, Bucket, <<"repeat">>, <<"repeat-object">>, Index,
                    #{
                        body => <<"repeat repeat gamma">>,
                        title => <<"other">>
                    },
                    WriteOpts},
                {fts_put, Bucket, <<"stale">>, <<"stale-object">>, Index,
                    #{
                        body => <<"alpha beta gamma stale">>,
                        title => <<"alpha beta gamma">>
                    },
                    WriteOpts},
                {fts_put, Bucket, <<"delete">>, <<"delete-object">>, Index,
                    #{
                        body => <<"alpha beta gamma delete">>,
                        title => <<"other">>
                    },
                    WriteOpts}
            ]
        ),

    [<<"column">>, <<"delete">>, <<"exact">>, <<"stale">>] =
        keys(search(Bookie, Bucket, Index, <<"\"alpha beta gamma\"">>, SearchOpts)),
    [<<"delete">>, <<"exact">>, <<"stale">>] =
        keys(search(Bookie, Bucket, Index, <<"body:\"alpha beta gamma\"">>, SearchOpts)),
    [<<"column">>, <<"exact">>, <<"stale">>] =
        keys(search(Bookie, Bucket, Index, <<"title:\"alpha beta gamma\"">>, SearchOpts)),
    [<<"column">>, <<"delete">>, <<"exact">>, <<"stale">>] =
        keys(search(Bookie, Bucket, Index, <<"\"alpha beta\" + gamma">>, SearchOpts)),
    [<<"exact">>, <<"repeat">>] =
        keys(search(Bookie, Bucket, Index, <<"\"repeat repeat gamma\"">>, SearchOpts)),
    [<<"column">>, <<"delete">>, <<"exact">>, <<"stale">>] =
        keys(search(Bookie, Bucket, Index, <<"\"alpha beta gam\"*">>, SearchOpts)),

    ok =
        fts_put(
            Bookie,
            Bucket,
            <<"stale">>,
            <<"stale-object-new">>,
            Index,
            #{
                body => <<"alpha beta filler gamma stale">>,
                title => <<"stale title">>
            },
            WriteOpts
        ),
    ok =
        fts_delete(
            Bookie,
            Bucket,
            <<"delete">>,
            Index,
            #{columns => [title, body]}
        ),

    [<<"column">>, <<"exact">>] =
        keys(search(Bookie, Bucket, Index, <<"\"alpha beta gamma\"">>, SearchOpts)),
    [<<"exact">>] =
        keys(search(Bookie, Bucket, Index, <<"body:\"alpha beta gamma\"">>, SearchOpts)),
    [<<"column">>, <<"exact">>] =
        keys(search(Bookie, Bucket, Index, <<"title:\"alpha beta gamma\"">>, SearchOpts)),
    [<<"column">>, <<"exact">>] =
        keys(search(Bookie, Bucket, Index, <<"\"alpha beta\" + gamma">>, SearchOpts)),
    [<<"exact">>, <<"repeat">>] =
        keys(search(Bookie, Bucket, Index, <<"\"repeat repeat gamma\"">>, SearchOpts)),
    [<<"column">>, <<"exact">>] =
        keys(search(Bookie, Bucket, Index, <<"\"alpha beta gam\"*">>, SearchOpts)),
    [] =
        keys(search(Bookie, Bucket, Index, <<"body:\"alpha beta gamma delete\"">>, SearchOpts)),
    [] =
        keys(search(Bookie, Bucket, Index, <<"body:\"alpha beta gamma stale\"">>, SearchOpts)),

    ok = leveled_bookie:book_close(Bookie).

index_update_contract(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),
    SearchColumns = [title, body],
    BodyColumns = [body],
    SearchOpts = #{columns => SearchColumns},
    MetadataSearchOpts = #{columns => BodyColumns},

    ok =
        fts_put(
            Bookie,
            <<"idx-maint">>,
            <<"doc">>,
            <<"doc-old">>,
            <<"main">>,
            #{body => <<"alpha beta">>, title => <<"first">>},
            #{
                columns => SearchColumns,
                prefixes => [2, 3],
                index_specs => [{add, <<"kind_bin">>, <<"old">>}]
            }
        ),
    [<<"doc">>] =
        metadata_search_term_keys(Bookie, <<"idx-maint">>, <<"main">>, <<"alpha">>, <<"body">>),
    [<<"doc">>] =
        metadata_search_prefix_keys(
            Bookie, <<"idx-maint">>, <<"main">>, 2, <<"al">>, <<"alpha">>, <<"body">>
        ),
    [<<"doc">>] = fts_doc_keys(Bookie, <<"idx-maint">>, <<"main">>),
    [<<"doc">>] = index_keys(Bookie, <<"idx-maint">>, <<"kind_bin">>, <<"old">>),

    ok =
        fts_put(
            Bookie,
            <<"idx-maint">>,
            <<"doc">>,
            <<"doc-new">>,
            <<"main">>,
            #{body => <<"gamma beta">>, title => <<"second">>},
            #{
                columns => SearchColumns,
                prefixes => [2, 3],
                index_specs => [
                    {remove, <<"kind_bin">>, <<"old">>},
                    {add, <<"kind_bin">>, <<"new">>}
                ]
            }
        ),
    PrefixRankOpts = #{prefixes => [2, 3], rank => none, columns => BodyColumns},
    PrefixRankMixedOpts = #{prefixes => [2, 3], rank => none, columns => BodyColumns},
    [] = keys(search(Bookie, <<"idx-maint">>, <<"main">>, <<"alpha">>, SearchOpts)),
    [<<"doc">>] = keys(search(Bookie, <<"idx-maint">>, <<"main">>, <<"gamma">>, SearchOpts)),
    [] = keys(search(Bookie, <<"idx-maint">>, <<"main">>, <<"body:al*">>, PrefixRankOpts)),
    [<<"doc">>] =
        keys(search(Bookie, <<"idx-maint">>, <<"main">>, <<"body:gam*">>, PrefixRankOpts)),
    [<<"doc">>] =
        keys(search(Bookie, <<"idx-maint">>, <<"main">>, <<"body:gam*">>, PrefixRankMixedOpts)),
    [] = metadata_search_term_keys(Bookie, <<"idx-maint">>, <<"main">>, <<"alpha">>, <<"body">>),
    [] =
        metadata_search_prefix_keys(
            Bookie, <<"idx-maint">>, <<"main">>, 2, <<"al">>, <<"alpha">>, <<"body">>
        ),
    [<<"doc">>] =
        metadata_search_term_keys(Bookie, <<"idx-maint">>, <<"main">>, <<"gamma">>, <<"body">>),
    [<<"doc">>] =
        metadata_search_prefix_keys(
            Bookie, <<"idx-maint">>, <<"main">>, 3, <<"gam">>, <<"gamma">>, <<"body">>
        ),
    [<<"doc">>] = fts_doc_keys(Bookie, <<"idx-maint">>, <<"main">>),
    [] = index_keys(Bookie, <<"idx-maint">>, <<"kind_bin">>, <<"old">>),
    [<<"doc">>] = index_keys(Bookie, <<"idx-maint">>, <<"kind_bin">>, <<"new">>),

    ok =
        fts_delete(
            Bookie,
            <<"idx-maint">>,
            <<"doc">>,
            <<"main">>,
            #{columns => SearchColumns, index_specs => [{remove, <<"kind_bin">>, <<"new">>}]}
        ),
    [] = search(Bookie, <<"idx-maint">>, <<"main">>, all_docs, SearchOpts),
    [] = metadata_search_term_keys(Bookie, <<"idx-maint">>, <<"main">>, <<"gamma">>, <<"body">>),
    [] = keys(search(Bookie, <<"idx-maint">>, <<"main">>, <<"body:gam*">>, PrefixRankOpts)),
    [] = keys(search(Bookie, <<"idx-maint">>, <<"main">>, <<"body:gam*">>, PrefixRankMixedOpts)),
    [] =
        metadata_search_prefix_keys(
            Bookie, <<"idx-maint">>, <<"main">>, 3, <<"gam">>, <<"gamma">>, <<"body">>
        ),
    [] = fts_doc_keys(Bookie, <<"idx-maint">>, <<"main">>),
    [] = index_keys(Bookie, <<"idx-maint">>, <<"kind_bin">>, <<"new">>),
    not_found = leveled_bookie:book_get(Bookie, <<"idx-maint">>, <<"doc">>),

    ok =
        fts_put(
            Bookie,
            <<"idx-maint">>,
            <<"doc">>,
            <<"doc-reinsert">>,
            <<"main">>,
            #{body => <<"delta beta">>, title => <<"third">>},
            #{
                columns => SearchColumns,
                prefixes => [2, 3],
                index_specs => [{add, <<"kind_bin">>, <<"again">>}]
            }
        ),
    [] = keys(search(Bookie, <<"idx-maint">>, <<"main">>, <<"gamma">>, SearchOpts)),
    [<<"doc">>] = keys(search(Bookie, <<"idx-maint">>, <<"main">>, <<"delta">>, SearchOpts)),
    [] = metadata_search_term_keys(Bookie, <<"idx-maint">>, <<"main">>, <<"gamma">>, <<"body">>),
    [] =
        metadata_search_prefix_keys(
            Bookie, <<"idx-maint">>, <<"main">>, 3, <<"gam">>, <<"gamma">>, <<"body">>
        ),
    [<<"doc">>] =
        metadata_search_term_keys(Bookie, <<"idx-maint">>, <<"main">>, <<"delta">>, <<"body">>),
    [<<"doc">>] =
        metadata_search_prefix_keys(
            Bookie, <<"idx-maint">>, <<"main">>, 3, <<"del">>, <<"delta">>, <<"body">>
        ),
    [<<"doc">>] = fts_doc_keys(Bookie, <<"idx-maint">>, <<"main">>),
    [] = index_keys(Bookie, <<"idx-maint">>, <<"kind_bin">>, <<"new">>),
    [<<"doc">>] = index_keys(Bookie, <<"idx-maint">>, <<"kind_bin">>, <<"again">>),

    ok =
        fts_put(
            Bookie,
            <<"idx-maint-metadata">>,
            <<"seg">>,
            <<"seg-old">>,
            <<"main">>,
            #{body => <<"red apple">>},
            #{
                columns => BodyColumns,
                index_specs => [{add, <<"seg_kind_bin">>, <<"old">>}]
            }
        ),
    true =
        metadata_term_present(
            Bookie, <<"idx-maint-metadata">>, <<"main">>, <<"red">>, <<"body">>
        ),
    [<<"seg">>] =
        fts_doc_keys(Bookie, <<"idx-maint-metadata">>, <<"main">>),
    [<<"seg">>] =
        index_keys(Bookie, <<"idx-maint-metadata">>, <<"seg_kind_bin">>, <<"old">>),

    ok =
        fts_put(
            Bookie,
            <<"idx-maint-metadata">>,
            <<"seg">>,
            <<"seg-new">>,
            <<"main">>,
            #{body => <<"blue grape">>},
            #{
                columns => BodyColumns,
                index_specs => [
                    {remove, <<"seg_kind_bin">>, <<"old">>},
                    {add, <<"seg_kind_bin">>, <<"new">>}
                ]
            }
        ),
    [] =
        keys(
            search(
                Bookie, <<"idx-maint-metadata">>, <<"main">>, <<"red">>, MetadataSearchOpts
            )
        ),
    [] =
        keys(
            search(
                Bookie, <<"idx-maint-metadata">>, <<"main">>, <<"body:red">>, #{
                    rank => none,
                    columns => [body]
                }
            )
        ),
    [<<"seg">>] =
        keys(
            search(
                Bookie, <<"idx-maint-metadata">>, <<"main">>, <<"blue">>, MetadataSearchOpts
            )
        ),
    [<<"seg">>] =
        keys(
            search(
                Bookie, <<"idx-maint-metadata">>, <<"main">>, <<"body:blue">>, #{
                    rank => none,
                    columns => [body]
                }
            )
        ),
    [<<"seg">>] =
        keys(
            search(
                Bookie, <<"idx-maint-metadata">>, <<"main">>, <<"blue NOT red">>, #{
                    rank => none,
                    limit => 20,
                    columns => BodyColumns
                }
            )
        ),
    [] =
        keys(
            search(
                Bookie, <<"idx-maint-metadata">>, <<"main">>, <<"red NOT blue">>, #{
                    rank => none,
                    limit => 20,
                    columns => BodyColumns
                }
            )
        ),
    true =
        metadata_term_present(
            Bookie, <<"idx-maint-metadata">>, <<"main">>, <<"blue">>, <<"body">>
        ),
    [<<"seg">>] =
        fts_doc_keys(Bookie, <<"idx-maint-metadata">>, <<"main">>),
    [] =
        index_keys(Bookie, <<"idx-maint-metadata">>, <<"seg_kind_bin">>, <<"old">>),
    [<<"seg">>] =
        index_keys(Bookie, <<"idx-maint-metadata">>, <<"seg_kind_bin">>, <<"new">>),

    ok =
        fts_delete(
            Bookie,
            <<"idx-maint-metadata">>,
            <<"seg">>,
            <<"main">>,
            #{columns => BodyColumns, index_specs => [{remove, <<"seg_kind_bin">>, <<"new">>}]}
        ),
    [] =
        search(
            Bookie, <<"idx-maint-metadata">>, <<"main">>, <<"blue">>, MetadataSearchOpts
        ),
    [] =
        keys(
            search(
                Bookie, <<"idx-maint-metadata">>, <<"main">>, <<"blue NOT red">>, #{
                    rank => none,
                    limit => 20,
                    columns => BodyColumns
                }
            )
        ),
    [] =
        fts_doc_keys(Bookie, <<"idx-maint-metadata">>, <<"main">>),
    [] =
        index_keys(Bookie, <<"idx-maint-metadata">>, <<"seg_kind_bin">>, <<"new">>),
    not_found = leveled_bookie:book_get(Bookie, <<"idx-maint-metadata">>, <<"seg">>),

    ok =
        fts_put(
            Bookie,
            <<"idx-maint-metadata">>,
            <<"seg">>,
            <<"seg-alt">>,
            <<"alt">>,
            #{body => <<"alternate melon">>},
            #{columns => BodyColumns}
        ),
    [<<"seg">>] =
        keys(
            search(
                Bookie, <<"idx-maint-metadata">>, <<"alt">>, <<"alternate">>, MetadataSearchOpts
            )
        ),
    ok =
        fts_delete(
            Bookie,
            <<"idx-maint-metadata">>,
            <<"seg">>,
            <<"alt">>,
            #{columns => BodyColumns}
        ),
    [] =
        search(
            Bookie, <<"idx-maint-metadata">>, <<"alt">>, <<"alternate">>, MetadataSearchOpts
        ),

    ok =
        fts_put(
            Bookie,
            <<"idx-maint-metadata">>,
            <<"seg">>,
            <<"seg-fresh">>,
            <<"main">>,
            #{body => <<"fresh melon">>},
            #{
                columns => BodyColumns,
                index_specs => [{add, <<"seg_kind_bin">>, <<"fresh">>}]
            }
        ),
    [] =
        search(
            Bookie, <<"idx-maint-metadata">>, <<"main">>, <<"red">>, MetadataSearchOpts
        ),
    [] =
        search(
            Bookie, <<"idx-maint-metadata">>, <<"main">>, <<"blue">>, MetadataSearchOpts
        ),
    [<<"seg">>] =
        keys(
            search(
                Bookie, <<"idx-maint-metadata">>, <<"main">>, <<"fresh">>, MetadataSearchOpts
            )
        ),
    [<<"seg">>] =
        fts_doc_keys(Bookie, <<"idx-maint-metadata">>, <<"main">>),
    [] =
        index_keys(Bookie, <<"idx-maint-metadata">>, <<"seg_kind_bin">>, <<"new">>),
    [<<"seg">>] =
        index_keys(Bookie, <<"idx-maint-metadata">>, <<"seg_kind_bin">>, <<"fresh">>),

    ok = leveled_bookie:book_close(Bookie).

concurrent_generation_contract(_Config) ->
    RootPath = testutil:reset_filestructure("fts_concurrent_generation"),
    Bucket = <<"concurrent-generation">>,
    Index = <<"main">>,
    Key = <<"same-key">>,
    WriterCount = 20,
    Parent = self(),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),
    [
        spawn(fun() ->
            Term = concurrent_generation_term(N),
            Result =
                fts_put(
                    Bookie,
                    Bucket,
                    Key,
                    Term,
                    Index,
                    #{body => <<Term/binary, " stable">>},
                    #{}
                ),
            Parent ! {concurrent_ftsput, N, Term, Result}
        end)
     || N <- lists:seq(1, WriterCount)
    ],
    Results = collect_concurrent_ftsputs(WriterCount, []),
    [] = [{N, Result} || {N, _Term, Result} <- Results, Result =/= ok],
    Terms = [Term || {_N, Term, ok} <- Results],
    {ok, {FinalTerm, #{body := _FinalBody}}} =
        leveled_bookie:book_get(Bookie, Bucket, Key),
    true = lists:member(FinalTerm, Terms),
    [Key] = keys(search(Bookie, Bucket, Index, FinalTerm, #{})),
    lists:foreach(
        fun(Term) ->
            [] = keys(search(Bookie, Bucket, Index, Term, #{}))
        end,
        Terms -- [FinalTerm]
    ),
    [Key] = fts_doc_keys(Bookie, Bucket, Index),
    ok = leveled_bookie:book_close(Bookie).

anchor_update_column_negative_contract(_Config) ->
    RootPath = testutil:reset_filestructure("fts_anchor_update_column_negative"),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),
    Bucket = <<"anchor">>,
    Index = <<"main">>,
    WriteOpts = #{columns => [title, body]},
    SearchOpts = #{columns => [title, body], rank => none},

    ok =
        fts_put(
            Bookie,
            Bucket,
            <<"doc-title-start">>,
            <<"obj-title-start">>,
            Index,
            #{title => <<"anchortitle first">>, body => <<"intro anchorbody">>},
            WriteOpts
        ),
    ok =
        fts_put(
            Bookie,
            Bucket,
            <<"doc-body-start">>,
            <<"obj-body-start">>,
            Index,
            #{title => <<"intro anchortitle">>, body => <<"anchorbody first">>},
            WriteOpts
        ),

    [<<"doc-title-start">>] =
        keys(search(Bookie, Bucket, Index, <<"title:^anchortitle">>, SearchOpts)),
    [<<"doc-body-start">>] =
        keys(search(Bookie, Bucket, Index, <<"body:^anchorbody">>, SearchOpts)),
    [<<"doc-body-start">>] =
        keys(search(Bookie, Bucket, Index, <<"^anchorbody">>, SearchOpts)),
    [] = keys(search(Bookie, Bucket, Index, <<"title:^anchorbody">>, SearchOpts)),
    [] = keys(search(Bookie, Bucket, Index, <<"body:^anchortitle">>, SearchOpts)),

    ok =
        fts_put(
            Bookie,
            Bucket,
            <<"doc-title-start">>,
            <<"obj-title-moved">>,
            Index,
            #{title => <<"intro anchortitle">>, body => <<"anchorbody now">>},
            WriteOpts
        ),
    [] = keys(search(Bookie, Bucket, Index, <<"title:^anchortitle">>, SearchOpts)),
    [<<"doc-body-start">>, <<"doc-title-start">>] =
        keys(search(Bookie, Bucket, Index, <<"body:^anchorbody">>, SearchOpts)),
    [] = keys(search(Bookie, Bucket, Index, <<"^anchortitle">>, SearchOpts)),

    ok =
        fts_delete(
            Bookie, Bucket, <<"doc-body-start">>, Index, WriteOpts
        ),
    [<<"doc-title-start">>] =
        keys(search(Bookie, Bucket, Index, <<"body:^anchorbody">>, SearchOpts)),
    not_found = leveled_bookie:book_get(Bookie, Bucket, <<"doc-body-start">>),

    ok =
        fts_put(
            Bookie,
            Bucket,
            <<"doc-body-start">>,
            <<"obj-title-reinserted">>,
            Index,
            #{title => <<"anchortitle rebound">>, body => <<"later anchorbody">>},
            WriteOpts
        ),
    [<<"doc-body-start">>] =
        keys(search(Bookie, Bucket, Index, <<"title:^anchortitle">>, SearchOpts)),
    [<<"doc-title-start">>] =
        keys(search(Bookie, Bucket, Index, <<"body:^anchorbody">>, SearchOpts)),
    [<<"doc-body-start">>] =
        keys(search(Bookie, Bucket, Index, <<"^anchortitle">>, SearchOpts)),
    [<<"doc-title-start">>] =
        keys(search(Bookie, Bucket, Index, <<"^anchorbody">>, SearchOpts)),

    ok = leveled_bookie:book_close(Bookie).

rejected_fast_path_regression_contract(_Config) ->
    RootPath = testutil:reset_filestructure("fts_rejected_fast_path_regression"),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),
    Index = <<"main">>,
    SearchOpts = #{columns => [title, body], prefixes => [5, 11], rank => none},

    ok =
        fts_put(
            Bookie,
            <<"fast-anchor">>,
            <<"a1">>,
            <<"a1">>,
            Index,
            #{
                title => <<"equivtitleupdate">>,
                body => <<"equivupdate equivbodyupdate">>
            },
            #{prefixes => [5, 11]}
        ),
    [<<"a1">>] =
        keys(search(Bookie, <<"fast-anchor">>, Index, <<"equivupdate">>, SearchOpts)),
    [<<"a1">>] =
        keys(search(Bookie, <<"fast-anchor">>, Index, <<"^equivupdate">>, SearchOpts)),
    [<<"a1">>] =
        keys(search(Bookie, <<"fast-anchor">>, Index, <<"title:^equivtitleupdate">>, SearchOpts)),
    [] =
        keys(search(Bookie, <<"fast-anchor">>, Index, <<"^equivbodyupdate">>, SearchOpts)),

    PhraseOpts = #{columns => [body], prefixes => [2, 3, 5, 11], rank => none},
    ok =
        fts_batchput(
            Bookie,
            [
                {fts_put, <<"fast-phrase-prefix">>, <<"p1">>, <<"p1">>,
                    Index, #{body => <<"equivnearone galaxy equivneartwo">>},
                    #{prefixes => [5, 11]}},
                {fts_put, <<"fast-phrase-prefix">>, <<"p2">>, <<"p2">>,
                    Index,
                    #{body => <<"equivnearone gap distant distant distant equivneartwo">>},
                    #{prefixes => [5, 11]}},
                {fts_put, <<"fast-phrase-prefix">>, <<"p3">>, <<"p3-old">>,
                    Index, #{body => <<"equivnearone gamma equivneartwo">>},
                    #{prefixes => [5, 11]}}
            ]
        ),
    ok =
        fts_put(
            Bookie,
            <<"fast-phrase-prefix">>,
            <<"p3">>,
            <<"p3-new">>,
            Index,
            #{body => <<"equivnearone zeta equivneartwo">>},
            #{prefixes => [5, 11]}
        ),
    [<<"p1">>, <<"p2">>] =
        keys(
            search(
                Bookie,
                <<"fast-phrase-prefix">>,
                Index,
                <<"\"equivnearone ga\"*">>,
                PhraseOpts
            )
        ),
    [<<"p1">>] =
        keys(
            search(
                Bookie,
                <<"fast-phrase-prefix">>,
                Index,
                <<"NEAR(\"equivnearone ga\"* equivneartwo, 1)">>,
                PhraseOpts
            )
        ),
    [] =
        keys(
            search(
                Bookie,
                <<"fast-phrase-prefix">>,
                Index,
                <<"\"equivnearone gam\"*">>,
                PhraseOpts
            )
        ),

    ok =
        fts_batchput(
            Bookie,
            [
                {fts_put, <<"fast-near">>, <<"n1">>, <<"n1-old">>,
                    Index, #{body => <<"oldnearone gap oldneartwo">>}, #{}},
                {fts_put, <<"fast-near">>, <<"n2">>, <<"n2-old">>,
                    Index, #{body => <<"deletenearone gap deleteneartwo">>}, #{}}
            ]
        ),
    ok =
        fts_put(
            Bookie,
            <<"fast-near">>,
            <<"n1">>,
            <<"n1-new">>,
            Index,
            #{body => <<"nearone gap neartwo">>},
            #{}
        ),
    ok = fts_delete(Bookie, <<"fast-near">>, <<"n2">>, Index, #{}),
    [] =
        keys(
            search(
                Bookie,
                <<"fast-near">>,
                Index,
                <<"NEAR(oldnearone oldneartwo, 5)">>,
                #{rank => none}
            )
        ),
    [] =
        keys(
            search(
                Bookie,
                <<"fast-near">>,
                Index,
                <<"NEAR(deletenearone deleteneartwo, 5)">>,
                #{rank => none}
            )
        ),
    [<<"n1">>] =
        keys(search(Bookie, <<"fast-near">>, Index, <<"NEAR(nearone neartwo, 5)">>, #{rank => none})),

    ok = leveled_bookie:book_close(Bookie).

metadata_representation_contract(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),

    ok =
        fts_batchput(
            Bookie,
            [
                {fts_put, <<"metadata">>, <<"1">>, <<"obj1">>, <<"main">>,
                    #{body => <<"red apple oldnearone gap oldneartwo">>},
                    #{prefixes => [3]}},
                {fts_put, <<"metadata">>, <<"2">>, <<"obj2">>, <<"main">>,
                    #{body => <<"green apple deletenearone gap deleteneartwo">>},
                    #{}}
            ]
        ),
    [<<"1">>, <<"2">>] =
        keys(search(Bookie, <<"metadata">>, <<"main">>, <<"apple">>, #{})),
    true =
        metadata_term_present(
            Bookie, <<"metadata">>, <<"main">>, <<"red">>, <<"body">>
        ),
    true =
        metadata_term_present(
            Bookie, <<"metadata">>, <<"main">>, <<"green">>, <<"body">>
        ),
    [<<"1">>, <<"2">>] = fts_doc_keys(Bookie, <<"metadata">>, <<"main">>),

    ok =
        fts_batchput(
            Bookie,
            [
                {fts_put, <<"metadata_a">>, <<"1">>, <<"obj-a1">>, <<"main">>,
                    #{body => <<"shared bucket token">>}, #{}},
                {fts_put, <<"metadata_b">>, <<"1">>, <<"obj-b1">>, <<"main">>,
                    #{body => <<"shared bucket token">>}, #{}}
            ]
        ),
    [<<"1">>] = keys(search(Bookie, <<"metadata_a">>, <<"main">>, <<"shared">>, #{})),
    [<<"1">>] = keys(search(Bookie, <<"metadata_b">>, <<"main">>, <<"shared">>, #{})),
    true =
        metadata_term_present(
            Bookie, <<"metadata_a">>, <<"main">>, <<"shared">>, <<"body">>
        ),
    true =
        metadata_term_present(
            Bookie, <<"metadata_b">>, <<"main">>, <<"shared">>, <<"body">>
        ),

    HighKey = binary:copy(<<255>>, 33),
    ok =
        fts_put(
            Bookie,
            <<"metadata-high-key">>,
            HighKey,
            <<"obj-high">>,
            <<"main">>,
            #{body => <<"highmarker">>},
            #{}
        ),
    [HighKey] =
        keys(
            search(
                Bookie, <<"metadata-high-key">>, <<"main">>, all_docs, #{}
            )
        ),
    [HighKey] =
        keys(
            search(
                Bookie, <<"metadata-high-key">>, <<"main">>, <<"highmarker">>, #{}
            )
        ),

    Cafe = <<"caf", 195, 169>>,
    CafeBody = <<Cafe/binary, " oldtoken">>,
    ok =
        fts_put(
            Bookie,
            <<"metadata-diacritic">>,
            <<"diacritic">>,
            <<"obj-diacritic-1">>,
            <<"main">>,
            #{body => CafeBody},
            #{remove_diacritics => false}
        ),
    [<<"diacritic">>] =
        keys(
            search(
                Bookie, <<"metadata-diacritic">>, <<"main">>, Cafe, #{
                    rank => none,
                    remove_diacritics => false
                }
            )
        ),
    ok =
        fts_put(
            Bookie,
            <<"metadata-diacritic">>,
            <<"diacritic">>,
            <<"obj-diacritic-2">>,
            <<"main">>,
            #{body => <<"plain replacement">>},
            #{remove_diacritics => false}
        ),
    [] =
        keys(
            search(
                Bookie, <<"metadata-diacritic">>, <<"main">>, Cafe, #{
                    rank => none,
                    remove_diacritics => false
                }
            )
        ),
    [<<"diacritic">>] =
        keys(
            search(
                Bookie, <<"metadata-diacritic">>, <<"main">>, <<"plain">>, #{
                    rank => none,
                    remove_diacritics => false
                }
            )
        ),

    ok =
        fts_put(
            Bookie,
            <<"metadata">>,
            <<"1">>,
            <<"obj1b">>,
            <<"main">>,
            #{body => <<"blue grape nearone gap neartwo">>},
            #{}
        ),
    [<<"2">>] = keys(search(Bookie, <<"metadata">>, <<"main">>, <<"apple">>, #{})),
    [#{key := <<"2">>}] =
        search(
            Bookie,
            <<"metadata">>,
            <<"main">>,
            <<"apple AND green">>,
            #{rank => none}
        ),
    [<<"1">>] = keys(search(Bookie, <<"metadata">>, <<"main">>, <<"grape">>, #{})),
    [<<"1">>] =
        keys(search(Bookie, <<"metadata">>, <<"main">>, <<"gra*">>, #{prefixes => [3]})),
    [<<"1">>] =
        keys(
            search(
                Bookie,
                <<"metadata">>,
                <<"main">>,
                <<"gra*">>,
                #{prefixes => [3], rank => none}
            )
        ),
    [<<"1">>] =
        keys(
            search(Bookie, <<"metadata">>, <<"main">>, <<"body:grape">>, #{
                rank => none,
                columns => [body]
            })
        ),
    ok =
        fts_batchput(
            Bookie,
            [
                {fts_put, <<"metadata-cross-column-and">>, <<"1">>, <<"cross-1">>,
                    <<"main">>, #{title => <<"crossalpha">>, body => <<"crossbeta">>}, #{}},
                {fts_put, <<"metadata-cross-column-and">>, <<"2">>, <<"cross-2">>,
                    <<"main">>, #{title => <<"crossalpha">>, body => <<"filler">>}, #{}},
                {fts_put, <<"metadata-cross-column-and">>, <<"3">>, <<"cross-3">>,
                    <<"main">>, #{title => <<"filler">>, body => <<"crossbeta">>}, #{}},
                {fts_put, <<"metadata-cross-column-and">>, <<"4">>, <<"cross-4">>,
                    <<"main">>, #{title => <<"crossbeta">>, body => <<"crossalpha">>}, #{}},
                {fts_put, <<"metadata-cross-column-and">>, <<"5">>, <<"cross-5">>,
                    <<"main">>, #{body => <<"crossalpha crossbeta">>}, #{}}
            ]
        ),
    [<<"1">>, <<"4">>, <<"5">>] =
        keys(
            search(
                Bookie, <<"metadata-cross-column-and">>, <<"main">>,
                <<"crossalpha AND crossbeta">>, #{rank => none, columns => [title, body]}
            )
        ),
    [<<"1">>] =
        keys(
            search(
                Bookie, <<"metadata-cross-column-and">>, <<"main">>,
                <<"title:crossalpha AND body:crossbeta">>, #{
                    rank => none, columns => [title, body]
                }
            )
        ),
    [<<"4">>] =
        keys(
            search(
                Bookie, <<"metadata-cross-column-and">>, <<"main">>,
                <<"body:crossalpha AND title:crossbeta">>, #{
                    rank => none, columns => [title, body]
                }
            )
        ),
    [] =
        keys(
            search(
                Bookie, <<"metadata-cross-column-and">>, <<"main">>,
                <<"title:crossalpha AND title:crossbeta">>, #{
                    rank => none, columns => [title, body]
                }
            )
        ),
    [] =
        keys(
            search(
                Bookie, <<"metadata">>, <<"main">>, <<"\"red apple\"">>, #{
                    rank => none
                }
            )
        ),
    [] =
        keys(
            search(
                Bookie, <<"metadata">>, <<"main">>, <<"NEAR(oldnearone oldneartwo, 2)">>, #{
                    rank => none
                }
            )
        ),
    [<<"1">>] =
        keys(
            search(
                Bookie, <<"metadata">>, <<"main">>, <<"\"blue grape\"">>, #{
                    rank => none
                }
            )
        ),
    [<<"1">>] =
        keys(
            search(
                Bookie, <<"metadata">>, <<"main">>, <<"NEAR(nearone neartwo, 2)">>, #{
                    rank => none
                }
            )
        ),
    [] = search(Bookie, <<"metadata">>, <<"main">>, <<"red">>, #{}),
    true =
        metadata_term_present(
            Bookie, <<"metadata">>, <<"main">>, <<"blue">>, <<"body">>
        ),
    [<<"1">>, <<"2">>] = fts_doc_keys(Bookie, <<"metadata">>, <<"main">>),

    ok = fts_delete(Bookie, <<"metadata">>, <<"2">>, <<"main">>, #{}),
    [] = search(Bookie, <<"metadata">>, <<"main">>, <<"apple">>, #{}),
    [] = search(Bookie, <<"metadata">>, <<"main">>, <<"green">>, #{}),
    [] =
        keys(
            search(
                Bookie, <<"metadata">>, <<"main">>, <<"\"green apple\"">>, #{
                    rank => none
                }
            )
        ),
    [] =
        keys(
            search(
                Bookie,
                <<"metadata">>,
                <<"main">>,
                <<"NEAR(deletenearone deleteneartwo, 2)">>,
                #{
                    rank => none
                }
            )
        ),
    [<<"1">>] = keys(search(Bookie, <<"metadata">>, <<"main">>, all_docs, #{})),
    [<<"1">>] = fts_doc_keys(Bookie, <<"metadata">>, <<"main">>),

    ok =
        fts_batchput(
            Bookie,
            [
                {fts_put, <<"metadata-shared-block">>, <<"1">>, <<"shared-1">>,
                    <<"main">>, #{body => <<"sharedblock keepalive">>},
                    #{}},
                {fts_put, <<"metadata-shared-block">>, <<"2">>, <<"shared-2">>,
                    <<"main">>, #{body => <<"sharedblock keepalive">>},
                    #{}}
            ]
        ),
    [<<"1">>, <<"2">>] =
        keys(
            search(
                Bookie, <<"metadata-shared-block">>, <<"main">>, <<"sharedblock">>, #{
                    rank => none
                }
            )
        ),
    ok =
        fts_put(
            Bookie,
            <<"metadata-shared-block">>,
            <<"1">>,
            <<"shared-1b">>,
            <<"main">>,
            #{body => <<"replacementonly">>},
            #{}
        ),
    [<<"2">>] =
        keys(
            search(
                Bookie, <<"metadata-shared-block">>, <<"main">>, <<"sharedblock">>, #{
                    rank => none
                }
            )
        ),

    ScaleRootPath = testutil:reset_filestructure("fts_metadata_scale"),
    ScaleOpts =
        lists:ukeysort(
            1,
            [
                {max_pencillercachesize, 401},
                {max_journalobjectcount, 100},
                {max_journalsize, 200000}
                | start_opts(ScaleRootPath)
            ]
        ),
    {ok, ScaleBookie} = leveled_bookie:book_start(ScaleOpts),
    ScaleMax = 1000,
    ok =
        write_fts_batches(
            ScaleBookie,
            [
                {fts_put, <<"metadata-scale">>, scaled_doc_key(N),
                    {<<"equivtitlebaseline">>, <<"equivbaseline">>}, <<"main">>,
                    #{
                        title => <<"equivtitlebaseline">>,
                        body => <<"equivbaseline equivnearone middle equivneartwo new york">>
                    },
                    #{prefixes => [5, 11]}}
             || N <- lists:seq(1, ScaleMax)
            ],
            50
        ),
    ok =
        write_fts_batches(
            ScaleBookie,
            [
                {fts_put, <<"metadata-scale">>, scaled_doc_key(N),
                    {<<"equivtitleupdate">>, <<"equivupdate">>}, <<"main">>,
                    #{
                        title => <<"equivtitleupdate">>,
                        body =>
                            <<"equivupdate equivbodyupdate equivnearone gap equivneartwo new york">>
                    },
                    #{prefixes => [5, 11]}}
             || N <- lists:seq(10, ScaleMax, 10)
            ],
            50
        ),
    ok =
        write_fts_batches(
            ScaleBookie,
            [
                {fts_delete, <<"metadata-scale">>, scaled_doc_key(N), <<"main">>,
                    #{prefixes => [5, 11]}}
             || N <- lists:seq(17, ScaleMax, 17)
            ],
            50
        ),
	    ScaleExpected =
	        [
	            scaled_doc_key(N)
	         || N <- lists:seq(1, ScaleMax),
	            N rem 10 =/= 0,
	            N rem 17 =/= 0
	        ],
		    UpdatedDocKey = scaled_doc_key(10),
		    DeletedDocKey = scaled_doc_key(17),
			    [UpdatedDocKey] =
			        lists:filter(
	            fun(Key) ->
	                lists:member(
	                    Key,
	                    fts_doc_keys(ScaleBookie, <<"metadata-scale">>, <<"main">>)
	                )
	            end,
	            [UpdatedDocKey, DeletedDocKey]
	        ),
	    ScaleExpected =
	        keys(
            search(
                ScaleBookie, <<"metadata-scale">>, <<"main">>, <<"equivbaseline">>, #{
                    rank => none,
                    columns => [title, body],
                    prefixes => [5, 11],
                    limit => ScaleMax
                }
            )
        ),
    ok = leveled_bookie:book_close(ScaleBookie),
    {ok, ReopenedScaleBookie} = leveled_bookie:book_start(ScaleOpts),
    ScaleExpected =
        keys(
            search(
                ReopenedScaleBookie,
                <<"metadata-scale">>,
                <<"main">>,
                <<"equivbaseline">>,
                #{
                    rank => none,
                    columns => [title, body],
                    prefixes => [5, 11],
                    limit => ScaleMax
                }
            )
        ),
    ScaleExpected =
        keys(
            search(
                ReopenedScaleBookie,
                <<"metadata-scale">>,
                <<"main">>,
                <<"title:equivtitlebaseline">>,
                #{
                    rank => none,
                    columns => [title, body],
                    prefixes => [5, 11],
                    limit => ScaleMax
                }
            )
        ),
    ScaleActiveExpected =
        [
            scaled_doc_key(N)
         || N <- lists:seq(1, ScaleMax),
            N rem 17 =/= 0
        ],
    ScaleActiveExpected =
        keys(
            search(
                ReopenedScaleBookie, <<"metadata-scale">>, <<"main">>, <<"\"new york\"">>, #{
                    rank => none,
                    columns => [title, body],
                    prefixes => [5, 11],
                    limit => ScaleMax
                }
            )
        ),
    ScaleActiveExpected =
        keys(
            search(
                ReopenedScaleBookie,
                <<"metadata-scale">>,
                <<"main">>,
                <<"NEAR(equivnearone equivneartwo, 5)">>,
                #{
                    rank => none,
                    columns => [title, body],
                    prefixes => [5, 11],
                    limit => ScaleMax
                }
            )
        ),
    ScaleUpdateExpected =
        [
            scaled_doc_key(N)
         || N <- lists:seq(10, ScaleMax, 10),
            N rem 17 =/= 0
        ],
    ScaleUpdateExpected =
        keys(
            search(
                ReopenedScaleBookie, <<"metadata-scale">>, <<"main">>, <<"equivupdate">>, #{
                    rank => none,
                    columns => [title, body],
                    prefixes => [5, 11],
                    limit => ScaleMax
                }
            )
        ),
    ScaleUpdateExpected =
        keys(
            search(
                ReopenedScaleBookie, <<"metadata-scale">>, <<"main">>, <<"^equivupdate">>, #{
                    rank => none,
                    columns => [title, body],
                    prefixes => [5, 11],
                    limit => ScaleMax
                }
            )
        ),
    ScaleUpdateExpected =
        keys(
            search(
                ReopenedScaleBookie,
                <<"metadata-scale">>,
                <<"main">>,
                <<"title:^equivtitleupdate">>,
                #{
                    rank => none,
                    columns => [title, body],
                    prefixes => [5, 11],
                    limit => ScaleMax
                }
            )
        ),
    [] =
        keys(
            search(
                ReopenedScaleBookie,
                <<"metadata-scale">>,
                <<"main">>,
                <<"^equivbodyupdate">>,
                #{
                    rank => none,
                    columns => [title, body],
                    prefixes => [5, 11],
                    limit => ScaleMax
                }
            )
        ),
    ok = leveled_bookie:book_close(ReopenedScaleBookie),

    ok =
        fts_put(
            Bookie,
            <<"metadata-prefix-dirty">>,
            <<"pref">>,
            <<"pref-old">>,
            <<"main">>,
            #{title => <<"prefix stale">>, body => <<"prefix stale">>},
            #{}
        ),
    ok =
        fts_put(
            Bookie,
            <<"metadata-prefix-dirty">>,
            <<"pref">>,
            <<"pref-new">>,
            <<"main">>,
            #{title => <<"prefix live">>, body => <<"prefix live">>},
            #{}
        ),
    [] =
        keys(
            search(
                Bookie, <<"metadata-prefix-dirty">>, <<"main">>, <<"sta*">>, #{
                    columns => [title, body],
                    rank => none
                }
            )
        ),
    [<<"pref">>] =
        keys(
            search(
                Bookie, <<"metadata-prefix-dirty">>, <<"main">>, <<"liv*">>, #{
                    columns => [title, body],
                    rank => none
                }
            )
        ),

    MetadataPhrasePrefixOpts = #{
        columns => [body],
        rank => none
    },
    ok =
        fts_batchput(
            Bookie,
            [
                {fts_put, <<"metadata-phrase-prefix">>, <<"p1">>, <<"p1">>,
                    <<"main">>, #{body => <<"equivnearone galaxy equivneartwo">>},
                    #{}},
                {fts_put, <<"metadata-phrase-prefix">>, <<"p2">>, <<"p2">>,
                    <<"main">>,
                    #{body =>
                        <<"equivnearone gap distant distant distant equivneartwo">>},
                    #{}},
                {fts_put, <<"metadata-phrase-prefix">>, <<"p3">>, <<"p3-old">>,
                    <<"main">>, #{body => <<"equivnearone gamma equivneartwo">>},
                    #{}},
                {fts_put, <<"metadata-phrase-prefix">>, <<"p4">>, <<"p4">>,
                    <<"main">>, #{body => <<"alpha beta gamma omega">>},
                    #{}}
            ]
        ),
    ok =
        fts_put(
            Bookie,
            <<"metadata-phrase-prefix">>,
            <<"p3">>,
            <<"p3-new">>,
            <<"main">>,
            #{body => <<"equivnearone zeta equivneartwo">>},
            #{}
        ),
    [<<"p1">>, <<"p2">>] =
        keys(
            search(
                Bookie,
                <<"metadata-phrase-prefix">>,
                <<"main">>,
                <<"\"equivnearone ga\"*">>,
                MetadataPhrasePrefixOpts
            )
        ),
    [<<"p1">>] =
        keys(
            search(
                Bookie,
                <<"metadata-phrase-prefix">>,
                <<"main">>,
                <<"NEAR(\"equivnearone ga\"* equivneartwo, 1)">>,
                MetadataPhrasePrefixOpts
            )
        ),
    [<<"p4">>] =
        keys(
            search(
                Bookie,
                <<"metadata-phrase-prefix">>,
                <<"main">>,
                <<"\"alpha beta ga\"*">>,
                MetadataPhrasePrefixOpts
            )
        ),
    [<<"p4">>] =
        keys(
            search(
                Bookie,
                <<"metadata-phrase-prefix">>,
                <<"main">>,
                <<"NEAR(\"alpha beta ga\"* \"beta gamma om\"*, 1)">>,
                MetadataPhrasePrefixOpts
            )
        ),
    [] =
        keys(
            search(
                Bookie,
                <<"metadata-phrase-prefix">>,
                <<"main">>,
                <<"\"equivnearone gam\"*">>,
                MetadataPhrasePrefixOpts
            )
        ),
    [MetadataPhrasePositionHit] =
        search(
            Bookie,
            <<"metadata-phrase-prefix">>,
            <<"main">>,
            <<"\"equivnearone gal\"*">>,
            MetadataPhrasePrefixOpts#{return_positions => true}
    ),
    #{positions := MetadataPhrasePositions} = MetadataPhrasePositionHit,
    #{phrase := [0]} = MetadataPhrasePositions,

    ok =
        fts_put(
            Bookie,
            <<"metadata-transition">>,
            <<"mix">>,
            <<"seg-old">>,
            <<"main">>,
            #{body => <<"transition red">>},
            #{}
        ),
    [<<"mix">>] =
        keys(
            search(
                Bookie, <<"metadata-transition">>, <<"main">>, <<"red">>, #{
                    rank => none
                }
            )
        ),
    ok =
        fts_put(
            Bookie,
            <<"metadata-transition">>,
            <<"mix">>,
            <<"seg-fresh">>,
            <<"main">>,
            #{body => <<"transition fresh">>},
            #{}
        ),
    [] =
        search(
            Bookie, <<"metadata-transition">>, <<"main">>, <<"red">>, #{
                rank => none
            }
        ),
    [<<"mix">>] =
        keys(
            search(
                Bookie, <<"metadata-transition">>, <<"main">>, <<"fresh">>, #{
                    rank => none
                }
            )
        ),

    ok = leveled_bookie:book_close(Bookie).

hot_term_metadata_split_contract(_Config) ->
    RootPath = testutil:reset_filestructure("fts_hot_term_metadata_split"),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),
    Bucket = <<"hot-metadata">>,
    Index = <<"main">>,
    Count = 1300,
    ExpectedKeys = [hot_doc_key(N) || N <- lists:seq(1, Count)],
    Ops =
        [
            {fts_put, Bucket, hot_doc_key(N), hot_doc_key(N), Index,
                #{body => <<"hotterm uniquehot", (integer_to_binary(N))/binary>>}, #{}}
         || N <- lists:seq(1, Count)
        ],
    ok = fts_batchput(Bookie, Ops),
    Count = length(fts_term_keys(Bookie, Bucket, Index, <<"hotterm">>, <<"body">>)),
    ExpectedKeys = fts_doc_keys(Bookie, Bucket, Index),
    ExpectedKeys = keys(search(Bookie, Bucket, Index, <<"hotterm">>, #{rank => none})),
    ExpectedUniqueKey = hot_doc_key(777),
    [ExpectedUniqueKey] =
        keys(search(Bookie, Bucket, Index, <<"uniquehot777">>, #{rank => none})),
    ok = leveled_bookie:book_close(Bookie).

metadata_rank_none_limit_order_contract(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),

    ok =
        fts_batchput(
            Bookie,
            [
                {fts_put, <<"metadata-order">>, <<"010">>, <<"obj-010">>, <<"main">>,
                    #{body => <<"a x">>}, #{}},
                {fts_put, <<"metadata-order">>, <<"100">>, <<"obj-100">>, <<"main">>,
                    #{body => <<"a b">>}, #{}}
            ]
        ),
    ok =
        fts_put(
            Bookie,
            <<"metadata-order">>,
            <<"050">>,
            <<"obj-050">>,
            <<"main">>,
            #{body => <<"a b">>},
            #{}
        ),
    [<<"050">>] =
        hit_keys(
            search(
                Bookie, <<"metadata-order">>, <<"main">>, <<"\"a b\"">>, #{
                    rank => none,
                    limit => 1
                }
            )
        ),
    [<<"050">>, <<"100">>] =
        hit_keys(
            search(
                Bookie, <<"metadata-order">>, <<"main">>, <<"NEAR(a b, 1)">>, #{
                    rank => none,
                    limit => 2
                }
            )
        ),
    [<<"010">>, <<"050">>, <<"100">>] =
        hit_keys(
            search(
                Bookie, <<"metadata-order">>, <<"main">>, <<"a OR x">>, #{
                    rank => none,
                    limit => 3
                }
            )
        ),
    [<<"050">>, <<"100">>] =
        hit_keys(
            search(
                Bookie, <<"metadata-order">>, <<"main">>, <<"a NOT x">>, #{
                    rank => none,
                    limit => 3
                }
            )
        ),
    ok = leveled_bookie:book_close(Bookie).

metadata_limited_pair_update_reopen_contract(_Config) ->
    RootPath = testutil:reset_filestructure("fts_metadata_limited_pair_update"),
    Opts =
        lists:ukeysort(
            1,
            [
                {max_pencillercachesize, 401},
                {max_journalobjectcount, 100},
                {max_journalsize, 200000}
                | start_opts(RootPath)
            ]
        ),
    {ok, Bookie} = leveled_bookie:book_start(Opts),
    Bucket = <<"metadata-limited-pair-update">>,
    Index = <<"main">>,
    MaxDocs = 80,
    Limit = 20,

    ok =
        write_fts_batches(
            Bookie,
            [
                {fts_put, Bucket, scaled_doc_key(N), scaled_doc_key(N), Index,
                    #{body => <<"limitedold history culture archive">>},
                    #{columns => [body]}}
             || N <- lists:seq(1, MaxDocs)
            ],
            20
        ),
    ok =
        write_fts_batches(
            Bookie,
            [
                {fts_put, Bucket, scaled_doc_key(N), {updated, scaled_doc_key(N)}, Index,
                    #{body => <<"limitednew history culture archive">>},
                    #{columns => [body]}}
             || N <- lists:seq(10, MaxDocs, 10)
            ],
            20
        ),
    ok =
        write_fts_batches(
            Bookie,
            [
                {fts_delete, Bucket, scaled_doc_key(N), Index, #{
                    columns => [body]
                }}
             || N <- lists:seq(17, MaxDocs, 17)
            ],
            20
        ),
    ok = leveled_bookie:book_close(Bookie),

    {ok, ReopenedBookie} = leveled_bookie:book_start(Opts),
    PairOpts = #{
        rank => none,
        columns => [body],
        limit => Limit
    },
    ActiveExpected =
        [
            scaled_doc_key(N)
         || N <- lists:seq(1, MaxDocs),
            N rem 17 =/= 0
        ],
    OldExpected =
        [
            scaled_doc_key(N)
         || N <- lists:seq(1, MaxDocs),
            N rem 10 =/= 0,
            N rem 17 =/= 0
        ],
    ActiveLimitedExpected = lists:sublist(ActiveExpected, Limit),
    OldLimitedExpected = lists:sublist(OldExpected, Limit),
    ActiveLimitedExpected =
        hit_keys(
            search(ReopenedBookie, Bucket, Index, <<"\"history culture\"">>, PairOpts)
        ),
    ActiveLimitedExpected =
        hit_keys(
            search(ReopenedBookie, Bucket, Index, <<"NEAR(history culture, 10)">>, PairOpts)
        ),
    OldLimitedExpected =
        hit_keys(
            search(ReopenedBookie, Bucket, Index, <<"\"limitedold history\"">>, PairOpts)
        ),
    ok = leveled_bookie:book_close(ReopenedBookie).

invalid_write_inputs(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),

    {async, ReturnFieldsRunner} =
        leveled_bookie:book_ftssearch(
            Bookie,
            <<"invalid">>,
            <<"main">>,
            <<"stored">>,
            #{columns => [body], return_fields => true}
        ),
    {error, missing_fts_schema} = ReturnFieldsRunner(),

    {error, missing_fts_schema} =
        fts_delete(Bookie, <<"invalid">>, <<"bad-delete">>, <<"main">>, #{}),
    assert_missing_fts_schema(Bookie, <<"invalid">>, <<"main">>, all_docs, #{}),

    {async, BadRankRunner} =
        leveled_bookie:book_ftssearch(
            Bookie,
            <<"docs">>,
            <<"main">>,
            <<"quick">>,
            #{columns => [body], rank => bogus}
        ),
    {error, invalid_rank_option} = BadRankRunner(),

    {async, BadPrefixRunner} =
        leveled_bookie:book_ftssearch(
            Bookie,
            <<"docs">>,
            <<"main">>,
            <<"quick">>,
            #{columns => [body], prefixes => [4]}
        ),
    {error, {invalid_fts_contract_change, prefixes, [2, 3, 5, 11], [4]}} =
        BadPrefixRunner(),

    {async, BadColumnRunner} =
        leveled_bookie:book_ftssearch(
            Bookie,
            <<"docs">>,
            <<"main">>,
            <<"quick">>,
            #{columns => [missing]}
    ),
    {error, {fts_parse, unknown_column, <<"missing">>}} = BadColumnRunner(),

    {error, invalid_index_specs} =
        leveled_bookie:book_put(
            Bookie,
            <<"docs">>,
            <<"forged">>,
            <<"forged">>,
            [
                {add_payload,
                    {fts_term, <<"main">>, ?STD_TAG, <<"body">>},
                    <<"quick">>,
                    <<1, 2, 3>>}
            ],
            ?STD_TAG
        ),
    {error, invalid_index_specs} =
        leveled_bookie:book_put(
            Bookie,
            <<"docs">>,
            <<"forged-legacy">>,
            <<"forged">>,
            [{add_payload, {fts_segment, <<"main">>}, <<"quick">>, <<1, 2, 3>>}],
            ?STD_TAG
        ),
    {error, invalid_index_specs} =
        leveled_bookie:book_put(
            Bookie,
            <<"docs">>,
            <<"forged-schema">>,
            <<"forged">>,
            [{add, {fts_schema, <<"main">>}, schema}],
            ?STD_TAG
        ),

    %% Keys that cannot be framed by the packed page format are rejected
    %% before any derivation worker is spawned; the bookie stays up.
    {error, {invalid_fts_key, {<<"t">>, <<"k">>}}} =
        leveled_bookie:book_put(
            Bookie,
            <<"docs">>,
            {<<"t">>, <<"k">>},
            <<"tuple-key">>,
            [],
            ?STD_TAG
        ),
    HugeKey = binary:copy(<<"k">>, 70000),
    {error, {invalid_fts_key, HugeKey}} =
        leveled_bookie:book_put(
            Bookie, <<"docs">>, HugeKey, <<"huge-key">>, [], ?STD_TAG
        ),
    {error, {invalid_fts_key, {<<"t">>, <<"k">>}}} =
        leveled_bookie:book_batchput(Bookie, [
            {put, <<"docs">>, <<"good">>, fts_test_object(<<"o">>, #{body => <<"ok">>}),
                [], ?STD_TAG, infinity},
            {put, <<"docs">>, {<<"t">>, <<"k">>}, <<"bad">>, [], ?STD_TAG, infinity}
        ]),
    %% The rejected batch wrote nothing.
    not_found = leveled_bookie:book_get(Bookie, <<"docs">>, <<"good">>, ?STD_TAG),
    %% Tuple keys remain first-class for non-FTS buckets on the same store.
    ok =
        leveled_bookie:book_put(
            Bookie, <<"plain">>, {<<"t">>, <<"k">>}, <<"v">>, [], ?STD_TAG
        ),
    {ok, <<"v">>} =
        leveled_bookie:book_get(Bookie, <<"plain">>, {<<"t">>, <<"k">>}, ?STD_TAG),
    %% Tokens beyond the page format's length frame are dropped, preserving
    %% the positions of surrounding tokens; the write itself succeeds.
    Monster = binary:copy(<<"a">>, 70000),
    ok =
        fts_put(
            Bookie,
            <<"docs">>,
            <<"monster">>,
            <<"obj">>,
            <<"main">>,
            #{body => <<"before ", Monster/binary, " after">>},
            #{}
        ),
    [<<"monster">>] =
        keys(search(Bookie, <<"docs">>, <<"main">>, <<"before AND after">>, #{})),
    [] = keys(search(Bookie, <<"docs">>, <<"main">>, <<"aaaaa*">>, #{})),
    [] = keys(search(Bookie, <<"docs">>, <<"main">>, <<"\"before after\"">>, #{})),

    ok = leveled_bookie:book_close(Bookie),
    {ok, ReopenedBookie} = leveled_bookie:book_start(start_opts(RootPath)),
    ok = leveled_bookie:book_close(ReopenedBookie),

    AmbiguousRootPath = testutil:reset_filestructure("fts_ambiguous_schema"),
    AmbiguousIndex = test_fts_index(<<"ambiguous">>, <<"main">>, #{}),
    TrapExit = process_flag(trap_exit, true),
    {error, ambiguous_fts_schema} =
        leveled_bookie:book_start(
            [
                {root_path, AmbiguousRootPath},
                {sync_strategy, testutil:sync_strategy()},
                {compression_method, none},
                {ledger_compression, none},
                {log_level, warning},
                {fts_indexes, [
                    AmbiguousIndex,
                    AmbiguousIndex#{tag => alt_tag}
                ]}
            ]
        ),
    receive
        {'EXIT', _Pid, ambiguous_fts_schema} -> ok
    after 0 ->
        ok
    end,
    process_flag(trap_exit, TrapExit).

private_snapshot_contract(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),

    ok =
        fts_put(
            Bookie,
            <<"private-snapshot">>,
            <<"1">>,
            <<"obj">>,
            <<"main">>,
            #{body => <<"history culture">>},
            #{}
        ),
    [<<"1">>] =
        keys(
            search(
                Bookie,
                <<"private-snapshot">>,
                <<"main">>,
                <<"NEAR(history culture, 5)">>,
                #{}
            )
        ),

    ok = leveled_bookie:book_close(Bookie).

regular_index_snapshot_contract(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),

    ok =
        leveled_bookie:book_put(
            Bookie,
            <<"regular-snapshot">>,
            <<"001">>,
            <<"obj-001">>,
            [{add, <<"kind_bin">>, <<"alpha">>}]
        ),
    ok =
        leveled_bookie:book_put(
            Bookie,
            <<"regular-snapshot">>,
            <<"002">>,
            <<"obj-002">>,
            [{add, <<"kind_bin">>, <<"alpha">>}]
        ),
    ok =
        leveled_bookie:book_put(
            Bookie,
            <<"regular-snapshot">>,
            <<"003">>,
            <<"obj-003">>,
            [{add, <<"kind_bin">>, <<"beta">>}]
        ),
    [<<"001">>, <<"002">>] =
        index_keys(Bookie, <<"regular-snapshot">>, <<"kind_bin">>, <<"alpha">>),
    [<<"003">>] =
        index_keys(Bookie, <<"regular-snapshot">>, <<"kind_bin">>, <<"beta">>),

    ok =
        leveled_bookie:book_put(
            Bookie,
            <<"regular-snapshot">>,
            <<"000">>,
            <<"obj-000">>,
            [{add, <<"kind_bin">>, <<"aardvark">>}]
        ),
    [<<"001">>, <<"002">>] =
        index_keys(Bookie, <<"regular-snapshot">>, <<"kind_bin">>, <<"alpha">>),

    ok =
        leveled_bookie:book_put(
            Bookie,
            <<"regular-snapshot">>,
            <<"999">>,
            <<"obj-999">>,
            [{add, <<"kind_bin">>, <<"zulu">>}]
        ),
    [<<"001">>, <<"002">>] =
        index_keys(Bookie, <<"regular-snapshot">>, <<"kind_bin">>, <<"alpha">>),

    ok = leveled_bookie:book_close(Bookie),
    {ok, Bookie2} = leveled_bookie:book_start(start_opts(RootPath)),
    [<<"001">>, <<"002">>] =
        index_keys(Bookie2, <<"regular-snapshot">>, <<"kind_bin">>, <<"alpha">>),
    [<<"003">>] =
        index_keys(Bookie2, <<"regular-snapshot">>, <<"kind_bin">>, <<"beta">>),

    ok =
        leveled_bookie:book_put(
            Bookie2,
            <<"regular-snapshot">>,
            <<"004">>,
            <<"obj-004">>,
            [{add, <<"kind_bin">>, <<"alpha">>}]
        ),
    ok =
        leveled_bookie:book_put(
            Bookie2,
            <<"regular-snapshot">>,
            <<"998">>,
            <<"obj-998">>,
            [{add, <<"kind_bin">>, <<"omega">>}]
        ),
    [<<"001">>, <<"002">>, <<"004">>] =
        index_keys(Bookie2, <<"regular-snapshot">>, <<"kind_bin">>, <<"alpha">>),
    [<<"003">>] =
        index_keys(Bookie2, <<"regular-snapshot">>, <<"kind_bin">>, <<"beta">>),
    ok = leveled_bookie:book_compactjournal(Bookie2, 30000),
    [<<"001">>, <<"002">>, <<"004">>] =
        index_keys(Bookie2, <<"regular-snapshot">>, <<"kind_bin">>, <<"alpha">>),

    ok = leveled_bookie:book_close(Bookie2),
    {ok, Bookie3} = leveled_bookie:book_start(start_opts(RootPath)),
    [<<"000">>] =
        index_keys(Bookie3, <<"regular-snapshot">>, <<"kind_bin">>, <<"aardvark">>),
    [<<"001">>, <<"002">>, <<"004">>] =
        index_keys(Bookie3, <<"regular-snapshot">>, <<"kind_bin">>, <<"alpha">>),
    [<<"003">>] =
        index_keys(Bookie3, <<"regular-snapshot">>, <<"kind_bin">>, <<"beta">>),
    [<<"998">>] =
        index_keys(Bookie3, <<"regular-snapshot">>, <<"kind_bin">>, <<"omega">>),
    [<<"999">>] =
        index_keys(Bookie3, <<"regular-snapshot">>, <<"kind_bin">>, <<"zulu">>),

    ok = leveled_bookie:book_close(Bookie3).

metadata_index_contract(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),

    ok =
        leveled_bookie:book_put(
            Bookie, <<"metadata">>, <<"direct-normal">>, <<"obj">>, [], ?STD_TAG
        ),
    {ok, <<"obj">>} = leveled_bookie:book_get(Bookie, <<"metadata">>, <<"direct-normal">>),
    ok = leveled_bookie:book_close(Bookie),

    {ok, ReplayedBookie} = leveled_bookie:book_start(start_opts(RootPath)),
    ok =
        fts_put(
            ReplayedBookie,
            <<"metadata">>,
            <<"direct-metadata">>,
            <<"legit">>,
            <<"directidx">>,
            #{body => <<"honest">>},
            #{}
        ),
    [<<"direct-metadata">>] =
        keys(search(ReplayedBookie, <<"metadata">>, <<"directidx">>, <<"honest">>, #{})),
    [] = search(ReplayedBookie, <<"metadata">>, <<"directidx">>, <<"leak">>, #{}),
    DirectIndexSpecs =
        current_user_indexspecs(ReplayedBookie, <<"metadata">>, <<"direct-metadata">>),
    %% The journal key changes durably carry the FTS facts for the write: the
    %% page specs for the directidx index must contain the honest token for
    %% this document.
    true =
        lists:member(
            {<<"honest">>, <<"direct-metadata">>},
            leveled_fts:spec_token_entries(DirectIndexSpecs, <<"directidx">>, ?STD_TAG)
        ),
    [<<"direct-metadata">>, <<"direct-normal">>] =
        fts_doc_keys(ReplayedBookie, <<"metadata">>, <<"directidx">>),
    [<<"direct-metadata">>] =
        fts_term_keys(ReplayedBookie, <<"metadata">>, <<"directidx">>, <<"honest">>, <<"body">>),
    ok =
        fts_delete(
            ReplayedBookie, <<"metadata">>, <<"direct-metadata">>, <<"directidx">>, #{}
        ),
    [] = search(ReplayedBookie, <<"metadata">>, <<"directidx">>, <<"honest">>, #{}),
    [<<"direct-normal">>] = fts_doc_keys(ReplayedBookie, <<"metadata">>, <<"directidx">>),
    [] =
        fts_term_keys(ReplayedBookie, <<"metadata">>, <<"directidx">>, <<"honest">>, <<"body">>),
    ok = leveled_bookie:book_close(ReplayedBookie),

    {ok, ReopenedBookie} = leveled_bookie:book_start(start_opts(RootPath)),
    {ok, <<"obj">>} =
        leveled_bookie:book_get(ReopenedBookie, <<"metadata">>, <<"direct-normal">>),
    ok = leveled_bookie:book_close(ReopenedBookie),

    assert_retain_compacted_fts_replay(),
    assert_doc_marker_latest_wins().

recovery_and_hotbackup(_Config) ->
    RootPath = testutil:reset_filestructure(),
    BackupPath = testutil:reset_filestructure("fts_backup"),
    {ok, Bookie1} = leveled_bookie:book_start(start_opts(RootPath)),
    ok =
        fts_put(
            Bookie1,
            <<"recover">>,
            <<"1">>,
            <<"obj1">>,
            <<"main">>,
            #{body => <<"old durable search">>},
            #{}
        ),
    ok =
        fts_put(
            Bookie1,
            <<"recover">>,
            <<"2">>,
            <<"obj2">>,
            <<"main">>,
            #{body => <<"delete durable search">>},
            #{}
        ),
    ok =
        fts_put(
            Bookie1,
            <<"recover">>,
            <<"1">>,
            <<"obj1b">>,
            <<"main">>,
            #{body => <<"new durable search">>},
            #{}
        ),
    ok = fts_delete(Bookie1, <<"recover">>, <<"2">>, <<"main">>, #{}),
    ok = leveled_bookie:book_close(Bookie1),

    {ok, Bookie2} = leveled_bookie:book_start(start_opts(RootPath)),
    [<<"1">>] = keys(search(Bookie2, <<"recover">>, <<"main">>, <<"new">>, #{})),
    [] = search(Bookie2, <<"recover">>, <<"main">>, <<"old">>, #{}),
    [<<"1">>] = keys(search(Bookie2, <<"recover">>, <<"main">>, all_docs, #{})),
    not_found = leveled_bookie:book_get(Bookie2, <<"recover">>, <<"2">>),
    ok = leveled_bookie:book_compactjournal(Bookie2, 30000),
    testutil:wait_for_compaction(Bookie2),
    [<<"1">>] = keys(search(Bookie2, <<"recover">>, <<"main">>, <<"new">>, #{})),
    {async, BackupFun} = leveled_bookie:book_hotbackup(Bookie2),
    ok = BackupFun(BackupPath),
    ok = leveled_bookie:book_close(Bookie2),

    leveled_penciller:clean_testdir(RootPath ++ "/ledger"),
    {ok, BookieReplay} = leveled_bookie:book_start(start_opts(RootPath)),
    [<<"1">>] =
        keys(search(BookieReplay, <<"recover">>, <<"main">>, <<"new">>, #{})),
    [] = search(BookieReplay, <<"recover">>, <<"main">>, <<"delete">>, #{}),
    assert_metadata_manifest_update(
        BookieReplay,
        <<"recover">>,
        <<"main">>,
        <<"1">>,
        <<"new">>,
        <<"replayed">>
    ),
    ok = leveled_bookie:book_close(BookieReplay),

    {ok, Bookie3} = leveled_bookie:book_start(start_opts(BackupPath)),
    [<<"1">>] = keys(search(Bookie3, <<"recover">>, <<"main">>, <<"new">>, #{})),
    [] = search(Bookie3, <<"recover">>, <<"main">>, <<"delete">>, #{}),
    assert_metadata_manifest_update(
        Bookie3,
        <<"recover">>,
        <<"main">>,
        <<"1">>,
        <<"new">>,
        <<"backup">>
    ),
    ok = leveled_bookie:book_destroy(Bookie3).

partial_tail_recovery_contract(_Config) ->
    RootPath = testutil:reset_filestructure("fts_partial_tail"),
    Opts =
        [
            {root_path, RootPath},
            {max_journalsize, 1000000},
            {cache_size, 500},
            {sync_strategy, riak_sync},
            {compression_method, none},
            {ledger_compression, none},
            {log_level, warning},
            {fts_indexes, test_fts_indexes()}
        ],
    Bucket = <<"partial-tail">>,
    Index = <<"main">>,
    {ok, Bookie1} = leveled_bookie:book_plainstart(Opts),
    ok =
        fts_batchput(Bookie1, [
            {fts_put, Bucket, <<"1">>, <<"obj1">>, Index, #{body => <<"tail one">>}, #{}},
            {fts_put, Bucket, <<"2">>, <<"obj2">>, Index, #{body => <<"tail two">>}, #{}}
        ]),
    [<<"1">>] = keys(search(Bookie1, Bucket, Index, <<"one">>, #{})),
    [<<"2">>] = keys(search(Bookie1, Bucket, Index, <<"two">>, #{})),

    {ok, Inker, Penciller} = leveled_bookie:book_returnactors(Bookie1),
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

    {ok, Bookie2} = leveled_bookie:book_start(Opts),
    not_found = leveled_bookie:book_get(Bookie2, Bucket, <<"1">>),
    not_found = leveled_bookie:book_get(Bookie2, Bucket, <<"2">>),
    [] = fts_doc_keys(Bookie2, Bucket, Index),
    [] = fts_term_keys(Bookie2, Bucket, Index, <<"tail">>, <<"body">>),
    [] = search(Bookie2, Bucket, Index, <<"tail">>, #{columns => [body]}),
    ok = leveled_bookie:book_destroy(Bookie2).

recalc_reload_contract(_Config) ->
    RootPath = testutil:reset_filestructure(),
    Opts = start_opts(RootPath) ++ [{reload_strategy, [{?STD_TAG, recalc}]}],
    {ok, Bookie1} = leveled_bookie:book_start(Opts),
    WriteOpts = #{
        prefixes => [3],
        index_specs => [{add, <<"recalc_kind_bin">>, <<"before">>}]
    },
    SearchOpts = #{},
    PrefixSearchOpts = #{prefixes => [3]},

    ok =
        fts_put(
            Bookie1,
            <<"recalc">>,
            <<"1">>,
            <<"obj1">>,
            <<"main">>,
            #{body => <<"survives recalc ledger rebuild">>},
            WriteOpts
        ),
    [<<"1">>] =
        keys(search(Bookie1, <<"recalc">>, <<"main">>, <<"survives">>, SearchOpts)),
    [<<"1">>] =
        keys(search(Bookie1, <<"recalc">>, <<"main">>, <<"sur*">>, PrefixSearchOpts)),
    [<<"1">>] =
        index_keys(Bookie1, <<"recalc">>, <<"recalc_kind_bin">>, <<"before">>),
    ok = leveled_bookie:book_close(Bookie1),

    leveled_penciller:clean_testdir(RootPath ++ "/ledger"),
    {ok, Bookie2} = leveled_bookie:book_start(Opts),
    [<<"1">>] = keys(search(Bookie2, <<"recalc">>, <<"main">>, <<"survives">>, SearchOpts)),
    [<<"1">>] = keys(search(Bookie2, <<"recalc">>, <<"main">>, <<"sur*">>, PrefixSearchOpts)),
    [<<"1">>] = keys(search(Bookie2, <<"recalc">>, <<"main">>, all_docs, SearchOpts)),
    [<<"1">>] = fts_doc_keys(Bookie2, <<"recalc">>, <<"main">>),
    [] = index_keys(Bookie2, <<"recalc">>, <<"recalc_kind_bin">>, <<"before">>),

    ok =
        fts_put(
            Bookie2,
            <<"recalc">>,
            <<"1">>,
            <<"obj1b">>,
            <<"main">>,
            #{body => <<"after recalc rebuild">>},
            #{
                prefixes => [3],
                index_specs => [
                    {remove, <<"recalc_kind_bin">>, <<"before">>},
                    {add, <<"recalc_kind_bin">>, <<"after">>}
                ]
            }
        ),
    [] = search(Bookie2, <<"recalc">>, <<"main">>, <<"survives">>, SearchOpts),
    [] = search(Bookie2, <<"recalc">>, <<"main">>, <<"sur*">>, PrefixSearchOpts),
    [<<"1">>] =
        keys(search(Bookie2, <<"recalc">>, <<"main">>, <<"after">>, SearchOpts)),
    [] = index_keys(Bookie2, <<"recalc">>, <<"recalc_kind_bin">>, <<"before">>),
    [<<"1">>] =
        index_keys(Bookie2, <<"recalc">>, <<"recalc_kind_bin">>, <<"after">>),
    ok = leveled_bookie:book_close(Bookie2),

    leveled_penciller:clean_testdir(RootPath ++ "/ledger"),
    {ok, Bookie3} = leveled_bookie:book_start(Opts),
    [] = search(Bookie3, <<"recalc">>, <<"main">>, <<"survives">>, SearchOpts),
    [<<"1">>] = keys(search(Bookie3, <<"recalc">>, <<"main">>, <<"after">>, SearchOpts)),
    [] = index_keys(Bookie3, <<"recalc">>, <<"recalc_kind_bin">>, <<"before">>),
    [] = index_keys(Bookie3, <<"recalc">>, <<"recalc_kind_bin">>, <<"after">>),
    ok = leveled_bookie:book_close(Bookie3).

sqlite_supported_ast_differential_contract(_Config) ->
    RootPath = testutil:reset_filestructure("fts_sqlite_ast_differential"),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),
    Bucket = <<"sqlite-ast">>,
    Index = <<"main">>,
    WriteOpts = #{columns => [title, body], prefixes => [2, 3, 5], remove_diacritics => 2},
    SearchOpts = WriteOpts#{rank => none, limit => 100},
    Docs = [
        {<<"d01">>, <<"alpha title">>, <<"bodyonly gamma new york nearone middle neartwo">>},
        {<<"d02">>, <<"beta title">>, <<"alpha bodyonly new york nearone gap gap gap neartwo">>},
        {<<"d03">>, <<"gamma title">>, <<"one two three">>},
        {<<"d04">>, <<"delta">>, <<"world alpha">>},
        {<<"d05">>, <<"prefix">>, <<"alphabet alphanumeric">>},
        {<<"d06">>, <<"cafe">>, <<"caf", 195, 169, " e", 204, 129, "clair london_distance">>}
    ],
    ValidQueries = [
        <<"alpha">>,
        <<"title:alpha">>,
        <<"body:alpha">>,
        <<"-title:alpha">>,
        <<"{title body}:alpha">>,
        <<"-{title}:alpha">>,
        <<"\"new york\"">>,
        <<"new + york">>,
        <<"alpha*">>,
        <<"^alpha">>,
        <<"NEAR(nearone neartwo, 2)">>,
        <<"alpha OR beta gamma">>,
        <<"alpha NOT bodyonly">>,
        <<"one NOT two three">>,
        <<"cafe">>,
        <<"london_distance">>,
        <<"london distance">>,
        <<"one OR two three">>,
        <<"NEAR(\"new york\" neartwo, 5)">>
    ],
    InvalidQueries = [
        <<"(alpha OR beta) gamma">>,
        <<"one (two OR three)">>,
        <<"^title:alpha">>,
        <<"^NEAR(alpha beta)">>,
        <<"bogus:alpha">>
    ],
    Queries = ValidQueries ++ InvalidQueries,
    write_sqlite_diff_docs(Bookie, Bucket, Index, Docs, WriteOpts),
    {async, BadColumnRunner} =
        leveled_bookie:book_ftssearch(
            Bookie,
            Bucket,
            Index,
            <<"schema">>,
            #{columns => [body], prefixes => [2, 3, 5], remove_diacritics => 2}
        ),
    {ok, _} = BadColumnRunner(),
    {async, BadPrefixRunner} =
        leveled_bookie:book_ftssearch(
            Bookie,
            Bucket,
            Index,
            <<"schema">>,
            #{columns => [title, body], prefixes => [4], remove_diacritics => 2}
        ),
    {error, {invalid_fts_contract_change, prefixes, [2, 3, 5], [4]}} =
        BadPrefixRunner(),
    Expected = sqlite_expected_results(RootPath, Docs, Queries),
    lists:foreach(fun(Query) -> error = maps:get(Query, Expected) end, InvalidQueries),
    lists:foreach(
        fun(Query) ->
            assert_sqlite_diff_query(Bookie, Bucket, Index, Query, SearchOpts, Expected)
        end,
        Queries
    ),
    ok = leveled_bookie:book_close(Bookie).

unicode61_supported_parity_corpus_contract(_Config) ->
    RootPath = testutil:reset_filestructure("fts_unicode61_parity"),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),
    Bucket = <<"sqlite-unicode">>,
    Index = <<"main">>,
    WriteOpts = #{columns => [title, body], prefixes => [2, 3, 5], remove_diacritics => 2},
    SearchOpts = WriteOpts#{rank => none, limit => 100},
    PrivateUse = <<238, 128, 128>>,
    Acute = <<204, 129>>,
    CombiningTie = <<205, 161>>,
    LigatureLeft = <<239, 184, 160>>,
    LigatureRight = <<239, 184, 161>>,
    Docs = [
        {<<"u01">>, <<"Case">>, <<"CAF", 195, 137, " e", 204, 129, "clair ", 196, 176,
            "STANBUL ", 199, 141, " ", 225, 184, 131>>},
        {<<"u02">>, <<"Private">>, <<PrivateUse/binary, "x marker">>},
        {<<"u03">>, <<"Combining">>, <<Acute/binary, "alpha beta">>},
        {<<"u04">>, <<"Separators">>,
            <<"london_distance koji", 240, 159, 152, 128, "grains koji", 226, 128, 147,
                "grains">>},
        {<<"u05">>, <<"Malformed">>, <<"malformed ", 255, " utf8tail">>},
        {<<"u06">>, <<"Combining tie">>, <<"a", CombiningTie/binary, "rcanum">>},
        {<<"u07">>, <<"Combining ligature">>,
            <<"bibliohrafii", LigatureLeft/binary, "a", LigatureRight/binary>>}
    ],
    Queries = [
        <<"cafe">>,
        <<"eclair">>,
        <<"istanbul">>,
        <<"a">>,
        <<"b">>,
        <<PrivateUse/binary, "x">>,
        <<"alpha">>,
        <<"london_distance">>,
        <<"\"london distance\"">>,
        <<"\"koji grains\"">>,
        <<"koji grains">>,
        <<"malformed">>,
        <<"utf8tail">>
    ],
    write_sqlite_diff_docs(Bookie, Bucket, Index, Docs, WriteOpts),
    [<<"u01">>] = keys(search(Bookie, Bucket, Index, <<"cafe">>, #{rank => none, limit => 100})),
    Expected = sqlite_expected_results(RootPath, Docs, Queries),
    lists:foreach(
        fun(Query) ->
            assert_sqlite_diff_query(Bookie, Bucket, Index, Query, SearchOpts, Expected)
        end,
        Queries
    ),
    ok = leveled_bookie:book_close(Bookie).

parse_errors(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),
    ok =
        fts_put(
            Bookie,
            <<"parse">>,
            <<"1">>,
            <<"obj1">>,
            <<"main">>,
            #{body => <<"valid content">>},
            #{columns => [title, body]}
        ),
    %% Nested negative column selectors compose by union instead of
    %% crashing: excluding both schema columns matches nothing.
    [] =
        keys(
            search(Bookie, <<"parse">>, <<"main">>, <<"-title:(-body:valid)">>, #{
                columns => [title, body]
            })
        ),
    [<<"1">>] =
        keys(
            search(Bookie, <<"parse">>, <<"main">>, <<"-title:valid">>, #{
                columns => [title, body]
            })
        ),
    ok =
        fts_put(
            Bookie,
            <<"parse">>,
            <<"stop">>,
            <<"stop-object">>,
            <<"stop">>,
            #{body => <<"quick the fox">>},
            #{stopwords => [<<"the">>]}
        ),
    ok =
        fts_put(
            Bookie,
            <<"parse">>,
            <<"column">>,
            <<"column-object">>,
            <<"main">>,
            #{title => <<"History">>, body => <<"plain body">>},
            #{}
        ),
    ok =
        fts_put(
            Bookie,
            <<"parse">>,
            <<"concat-title">>,
            <<"concat-title-object">>,
            <<"main">>,
            #{title => <<"new york">>, body => <<"plain body">>},
            #{}
        ),
    ok =
        fts_put(
            Bookie,
            <<"parse">>,
            <<"concat-body">>,
            <<"concat-body-object">>,
            <<"main">>,
            #{title => <<"plain title">>, body => <<"new york">>},
            #{}
        ),
    ok =
        fts_put(
            Bookie,
            <<"parse">>,
            <<"not1">>,
            <<"not1-object">>,
            <<"main">>,
            #{body => <<"one">>},
            #{}
        ),
    ok =
        fts_put(
            Bookie,
            <<"parse">>,
            <<"not2">>,
            <<"not2-object">>,
            <<"main">>,
            #{body => <<"one two">>},
            #{}
        ),
    ok =
        fts_put(
            Bookie,
            <<"parse">>,
            <<"not3">>,
            <<"not3-object">>,
            <<"main">>,
            #{body => <<"one two three">>},
            #{}
        ),
    ok =
        fts_put(
            Bookie,
            <<"parse">>,
            <<"sep1">>,
            <<"sep1-object">>,
            <<"main">>,
            #{body => <<"koji grains">>},
            #{}
        ),
    ok =
        fts_put(
            Bookie,
            <<"parse">>,
            <<"sep2">>,
            <<"sep2-object">>,
            <<"main">>,
            #{body => <<"koji x grains">>},
            #{}
        ),
    ok =
        fts_put(
            Bookie,
            <<"parse">>,
            <<"sep3">>,
            <<"sep3-object">>,
            <<"main">>,
            #{body => <<"grains koji">>},
            #{}
        ),
    ok =
        fts_put(
            Bookie,
            <<"parse">>,
            <<"near3-wide">>,
            <<"near3-wide-object">>,
            <<"main">>,
            #{body => <<"b x x a x x c">>},
            #{}
        ),
    LongPositionText =
        iolist_to_binary(lists:join(<<" ">>, lists:duplicate(4097, <<"repeatcap">>))),
    ok =
        fts_put(
            Bookie,
            <<"parse">>,
            <<"position-cap">>,
            <<"position-cap-object">>,
            <<"main">>,
            #{body => LongPositionText},
            #{}
        ),
    StopOpts = #{stopwords => [<<"the">>]},
    [] = search(Bookie, <<"parse">>, <<"stop">>, <<"the">>, StopOpts),
    [<<"stop">>] = keys(search(Bookie, <<"parse">>, <<"stop">>, <<"the quick">>, StopOpts)),
    [<<"stop">>] =
        keys(search(Bookie, <<"parse">>, <<"stop">>, <<"\"quick the fox\"">>, StopOpts)),
    [] = search(Bookie, <<"parse">>, <<"stop">>, <<"\"quick fox\"">>, StopOpts),
    [] = search(Bookie, <<"parse">>, <<"stop">>, <<"the">>, #{}),
    [<<"stop">>] = keys(search(Bookie, <<"parse">>, <<"stop">>, <<"the quick">>, #{})),
    [<<"stop">>] =
        keys(search(Bookie, <<"parse">>, <<"stop">>, <<"\"quick the fox\"">>, #{})),
    [<<"column">>] =
        keys(
            search(Bookie, <<"parse">>, <<"main">>, <<"title:history">>, #{
                columns => [title, body]
            })
        ),
    [<<"column">>] =
        keys(
            search(Bookie, <<"parse">>, <<"main">>, <<"TITLE:history">>, #{
                columns => [title, body]
            })
        ),
    {async, TitleNoSchemaOpts} =
        leveled_bookie:book_ftssearch(Bookie, <<"parse">>, <<"main">>, <<"title:history">>, #{}),
    {ok, TitleNoSchemaOptsHits} = TitleNoSchemaOpts(),
    [<<"column">>] = keys(TitleNoSchemaOptsHits),
    [] =
        keys(
            search(Bookie, <<"parse">>, <<"main">>, <<"title:(history body)">>, #{
                columns => [title, body]
            })
        ),
    [<<"concat-title">>] =
        keys(
            search(Bookie, <<"parse">>, <<"main">>, <<"title:new + york">>, #{
                columns => [title, body]
            })
        ),
    [<<"concat-body">>] =
        keys(
            search(Bookie, <<"parse">>, <<"main">>, <<"new + body:york">>, #{
                columns => [title, body]
            })
        ),
    [<<"sep1">>] =
        keys(search(Bookie, <<"parse">>, <<"main">>, <<"koji", 226, 128, 147, "grains">>, #{})),
    [<<"sep1">>] =
        keys(search(Bookie, <<"parse">>, <<"main">>, <<"koji", 240, 159, 152, 128, "grains">>, #{})),
    [<<"sep1">>, <<"sep2">>, <<"sep3">>] =
        keys(search(Bookie, <<"parse">>, <<"main">>, <<"koji ", 226, 128, 147, " grains">>, #{})),
    [<<"not1">>] =
        keys(search(Bookie, <<"parse">>, <<"main">>, <<"one NOT two NOT three">>, #{})),
    [] = keys(search(Bookie, <<"parse">>, <<"main">>, <<"NEAR(a b c, 2)">>, #{})),

    {async, BlankQuery} =
        leveled_bookie:book_ftssearch(
            Bookie, <<"parse">>, <<"main">>, <<" \t">>, #{columns => [body]}
        ),
    {error, {fts_parse, empty_query}} = BlankQuery(),
    {async, BadQuery} =
        leveled_bookie:book_ftssearch(
            Bookie, <<"parse">>, <<"main">>, <<"(">>, #{columns => [body]}
        ),
    {error, {fts_parse, unexpected_end}} = BadQuery(),
    {async, UnknownColumn} =
        leveled_bookie:book_ftssearch(
            Bookie,
            <<"parse">>,
            <<"main">>,
            <<"bogus:history">>,
            #{columns => [title, body]}
        ),
    {error, {fts_parse, unknown_column, <<"bogus">>}} = UnknownColumn(),
    {async, UnknownColumnNoSchema} =
        leveled_bookie:book_ftssearch(Bookie, <<"parse">>, <<"main">>, <<"bogus:history">>, #{}),
    {error, {fts_parse, unknown_column, <<"bogus">>}} = UnknownColumnNoSchema(),
    {async, BadRank} =
        leveled_bookie:book_ftssearch(
            Bookie, <<"parse">>, <<"main">>, <<"valid">>, #{columns => [body], rank => bogus}
        ),
    {error, invalid_rank_option} = BadRank(),
    LongQuery = binary:copy(<<"a">>, 4097),
    {async, LongQueryRunner} =
        leveled_bookie:book_ftssearch(Bookie, <<"parse">>, <<"main">>, LongQuery, #{}),
    {error, fts_query_too_large} = LongQueryRunner(),
    LongQueryList = lists:duplicate(4097, $a),
    {async, LongQueryListRunner} =
        leveled_bookie:book_ftssearch(Bookie, <<"parse">>, <<"main">>, LongQueryList, #{}),
    {error, fts_query_too_large} = LongQueryListRunner(),
    TooManyTokens =
        iolist_to_binary(lists:join(<<" ">>, lists:duplicate(129, <<"valid">>))),
    {async, TooManyTokensRunner} =
        leveled_bookie:book_ftssearch(Bookie, <<"parse">>, <<"main">>, TooManyTokens, #{}),
    {error, fts_query_too_many_tokens} = TooManyTokensRunner(),
    DeepAnd =
        iolist_to_binary(lists:join(<<" AND ">>, lists:duplicate(40, <<"valid">>))),
    {async, DeepAndRunner} =
        leveled_bookie:book_ftssearch(Bookie, <<"parse">>, <<"main">>, DeepAnd, #{}),
    {error, fts_query_ast_too_deep} = DeepAndRunner(),
    {async, NearCapRunner} =
        leveled_bookie:book_ftssearch(
            Bookie, <<"parse">>, <<"main">>, <<"NEAR(valid content, 65)">>, #{}
        ),
    {error, fts_query_near_distance_exceeded} = NearCapRunner(),
    LongPrefixToken = binary:copy(<<"a">>, 65),
    LongPrefixQuery = <<LongPrefixToken/binary, "*">>,
    {async, PrefixCapRunner} =
        leveled_bookie:book_ftssearch(Bookie, <<"parse">>, <<"main">>, LongPrefixQuery, #{}),
    {error, fts_query_prefix_too_large} = PrefixCapRunner(),
    {async, LimitCapRunner} =
        leveled_bookie:book_ftssearch(
            Bookie, <<"parse">>, <<"main">>, <<"valid">>, #{limit => 20001}
        ),
    {error, fts_query_limit_exceeded} = LimitCapRunner(),
    {async, WindowCapRunner} =
        leveled_bookie:book_ftssearch(
            Bookie, <<"parse">>, <<"main">>, <<"valid">>, #{limit => 20, offset => 19990}
        ),
    {error, fts_query_limit_exceeded} = WindowCapRunner(),
    {async, PositionCapRunner} =
        leveled_bookie:book_ftssearch(
            Bookie, <<"parse">>, <<"main">>, <<"repeatcap">>, #{return_positions => true}
        ),
    {error, fts_query_positions_limit_exceeded} = PositionCapRunner(),
    ok = leveled_bookie:book_close(Bookie).

external_term_decode_contract(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),
    Envelope =
        fun(Title, Body) ->
            term_to_binary(#{
                <<"__vsn">> => 1,
                attributes => #{title => Title, body => Body}
            })
        end,
    ok =
        leveled_bookie:book_put(
            Bookie,
            <<"ext-term">>,
            <<"1">>,
            Envelope(<<"Alpha">>, <<"quick brown fox">>),
            [],
            ?STD_TAG
        ),
    ok =
        leveled_bookie:book_put(
            Bookie,
            <<"ext-term">>,
            <<"2">>,
            Envelope(<<"Beta">>, <<"slow blue hare">>),
            [],
            ?STD_TAG
        ),
    [<<"1">>] = keys(search(Bookie, <<"ext-term">>, <<"main">>, <<"fox">>, #{})),
    [<<"2">>] =
        keys(
            search(Bookie, <<"ext-term">>, <<"main">>, <<"title:beta">>, #{
                columns => [title, body]
            })
        ),
    %% Update through the same envelope encoding supersedes old postings.
    ok =
        leveled_bookie:book_put(
            Bookie,
            <<"ext-term">>,
            <<"1">>,
            Envelope(<<"Alpha">>, <<"quick brown wolf">>),
            [],
            ?STD_TAG
        ),
    [] = keys(search(Bookie, <<"ext-term">>, <<"main">>, <<"fox">>, #{})),
    [<<"1">>] = keys(search(Bookie, <<"ext-term">>, <<"main">>, <<"wolf">>, #{})),
    %% Invalid decode option rejected at normalisation.
    {error, invalid_fts_decode_option} =
        leveled_fts:normalise_indexes([
            maps:put(decode, junk, test_fts_index(<<"x">>, <<"main">>, #{}))
        ]),
    ok = leveled_bookie:book_close(Bookie),
    testutil:reset_filestructure().

%% A failed batchput must not leave the FTS sequence ahead of the journal
%% SQN: after a restart reseeds from the journal, a drifted sequence would
%% be stamped twice, aliasing marker and page-directory terms across two
%% write batches (resurrecting superseded postings).
failed_batch_sequence_contract(_Config) ->
    RootPath = testutil:reset_filestructure(),
    StartOpts = [{max_journalsize, 100000} | start_opts(RootPath)],
    {ok, Bookie1} = leveled_bookie:book_start(StartOpts),
    ok =
        fts_put(
            Bookie1,
            <<"docs">>,
            <<"1">>,
            <<"v1">>,
            <<"main">>,
            #{body => <<"alpha alpha">>, title => <<"T">>},
            #{}
        ),
    %% A batch larger than the journal file cap fails without advancing
    %% the journal SQN.
    Huge = crypto:strong_rand_bytes(200000),
    {error, batch_too_large} =
        leveled_bookie:book_batchput(Bookie1, [
            {put, <<"docs">>, <<"big">>, Huge, [], ?STD_TAG, infinity}
        ]),
    ok =
        fts_put(
            Bookie1,
            <<"docs">>,
            <<"2">>,
            <<"v2">>,
            <<"main">>,
            #{body => <<"beta beta">>, title => <<"T">>},
            #{}
        ),
    [<<"2">>] = keys(search(Bookie1, <<"docs">>, <<"main">>, <<"beta">>, #{})),
    ok = leveled_bookie:book_close(Bookie1),
    {ok, Bookie2} = leveled_bookie:book_start(StartOpts),
    %% The post-restart write must stamp a fresh sequence. With drift it
    %% reuses doc 2's batch sequence under a different carrier key, so
    %% doc 2's page directory is shadowed and its postings disappear.
    ok =
        fts_put(
            Bookie2,
            <<"docs">>,
            <<"3">>,
            <<"v3">>,
            <<"main">>,
            #{body => <<"gamma gamma">>, title => <<"T">>},
            #{}
        ),
    [<<"3">>] = keys(search(Bookie2, <<"docs">>, <<"main">>, <<"gamma">>, #{})),
    [<<"2">>] = keys(search(Bookie2, <<"docs">>, <<"main">>, <<"beta">>, #{})),
    [<<"1">>] = keys(search(Bookie2, <<"docs">>, <<"main">>, <<"alpha">>, #{})),
    %% Exact duplicate schema definitions are rejected at normalisation.
    Dup = test_fts_index(<<"dup">>, <<"main">>, #{}),
    {error, ambiguous_fts_schema} = leveled_fts:normalise_indexes([Dup, Dup]),
    ok = leveled_bookie:book_close(Bookie2),
    testutil:reset_filestructure().

%% A token appearing in more docs than a page entry's 16-bit count can
%% express must be split across entries, not truncated modulo 2^16 (which
%% corrupted the persisted page payload).
oversized_token_contract(_Config) ->
    Schemas =
        case leveled_fts:normalise_indexes([test_fts_index(<<"docs">>, <<"main">>, #{})]) of
            {ok, Normal} -> Normal
        end,
    N = 70000,
    Changes =
        [
            {
                {?STD_TAG, <<"docs">>, <<"k", (integer_to_binary(I))/binary>>, null},
                fts_test_object(<<"o">>, #{body => <<"common">>}),
                {[], infinity}
            }
         || I <- lists:seq(1, N)
        ],
    {ok, Augmented} = leveled_fts:augment_object_changes(Changes, Schemas, 1),
    AllSpecs = lists:append([Specs || {_LK, _Obj, {Specs, _TTL}} <- Augmented]),
    Entries = leveled_fts:spec_token_entries(AllSpecs, <<"main">>, ?STD_TAG),
    CommonKeys = [K || {<<"common">>, K} <- Entries],
    N = length(CommonKeys),
    N = length(lists:usort(CommonKeys)),
    %% End to end through the store: one batch, count and bounded search.
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),
    M = 66000,
    BatchSpecs =
        [
            {put, <<"batch">>, <<"k", (integer_to_binary(I))/binary>>,
                fts_test_object(<<"o">>, #{body => <<"common">>}), [], ?STD_TAG, infinity}
         || I <- lists:seq(1, M)
        ],
    ok = leveled_bookie:book_batchput(Bookie, BatchSpecs),
    {async, CountRunner} =
        leveled_bookie:book_ftssearch(Bookie, <<"batch">>, <<"main">>, <<"common">>, #{
            result => summary
        }),
    {ok, #{total_count := M}} = CountRunner(),
    Limited = search(Bookie, <<"batch">>, <<"main">>, <<"common">>, #{limit => 20000}),
    20000 = length(Limited),
    ok = leveled_bookie:book_close(Bookie),
    testutil:reset_filestructure().

tenant_bucket_prefix_contract(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),
    ok =
        fts_put(
            Bookie,
            <<"tenant-a">>,
            <<"1">>,
            <<"obj1">>,
            <<"main">>,
            #{body => <<"alpha quick fox">>, title => <<"A">>},
            #{}
        ),
    ok =
        fts_put(
            Bookie,
            <<"tenant-b">>,
            <<"1">>,
            <<"obj2">>,
            <<"main">>,
            #{body => <<"alpha slow hare">>, title => <<"B">>},
            #{}
        ),
    %% One prefix schema serves every tenant bucket, with full isolation.
    [<<"1">>] = keys(search(Bookie, <<"tenant-a">>, <<"main">>, <<"fox">>, #{})),
    [] = keys(search(Bookie, <<"tenant-a">>, <<"main">>, <<"hare">>, #{})),
    [<<"1">>] = keys(search(Bookie, <<"tenant-b">>, <<"main">>, <<"hare">>, #{})),
    [<<"1">>] = keys(search(Bookie, <<"tenant-a">>, <<"main">>, <<"alpha">>, #{})),
    [<<"1">>] = keys(search(Bookie, <<"tenant-b">>, <<"main">>, <<"alpha">>, #{})),
    assert_missing_fts_schema(
        Bookie, <<"other">>, <<"main">>, <<"alpha">>, #{columns => [body]}
    ),
    %% An update in one tenant does not disturb another.
    ok =
        fts_put(
            Bookie,
            <<"tenant-a">>,
            <<"1">>,
            <<"obj1b">>,
            <<"main">>,
            #{body => <<"alpha quick wolf">>, title => <<"A">>},
            #{}
        ),
    [] = keys(search(Bookie, <<"tenant-a">>, <<"main">>, <<"fox">>, #{})),
    [<<"1">>] = keys(search(Bookie, <<"tenant-a">>, <<"main">>, <<"wolf">>, #{})),
    [<<"1">>] = keys(search(Bookie, <<"tenant-b">>, <<"main">>, <<"hare">>, #{})),
    %% Overlapping same-name schemas are rejected at startup.
    {error, _Reason} =
        leveled_fts:normalise_indexes([
            maps:put(
                bucket_prefix,
                <<"tenant-">>,
                maps:remove(bucket, test_fts_index(<<"unused">>, <<"main">>, #{}))
            ),
            test_fts_index(<<"tenant-a">>, <<"main">>, #{})
        ]),
    ok = leveled_bookie:book_close(Bookie),
    testutil:reset_filestructure().

filter_and_verbatim_contract(_Config) ->
    RootPath = testutil:reset_filestructure(),
    {ok, Bookie} = leveled_bookie:book_start(start_opts(RootPath)),
    B = <<"filter">>,
    I = <<"main">>,
    Put =
        fun(Key, Tenant, Root, Body, Title) ->
            ok =
                fts_put(Bookie, B, Key, <<"obj-", Key/binary>>, I, #{
                    body => Body, title => Title, tenant => Tenant, root => Root
                }, #{})
        end,
    Put(<<"a1">>, <<"acme-corp">>, <<"r1">>, <<"shared quick target">>, <<"Alpha">>),
    Put(<<"a2">>, <<"acme-corp">>, <<"r2">>, <<"shared quick target">>, <<"Aleph">>),
    Put(<<"b1">>, <<"beta-inc">>, <<"r1">>, <<"shared quick target">>, <<"Bravo">>),
    Put(<<"b2">>, <<"beta-inc">>, <<"r1">>, <<"shared quick target beta">>, <<"Brome">>),
    Put(<<"c0">>, <<"acme">>, <<"r1">>, <<"shared quick target">>, <<"Charlie">>),

    %% Unfiltered baseline.
    [<<"a1">>, <<"a2">>, <<"b1">>, <<"b2">>, <<"c0">>] =
        keys(search(Bookie, B, I, <<"quick">>, #{})),

    %% Verbatim single-tenant filter: exact match only -- neither the
    %% <<"acme">> tenant nor the tokenized halves of "acme-corp" match.
    [<<"a1">>, <<"a2">>] =
        keys(
            search(Bookie, B, I, <<"quick">>, #{filter => [{tenant, [<<"acme-corp">>]}]})
        ),
    [<<"c0">>] =
        keys(search(Bookie, B, I, <<"quick">>, #{filter => [{tenant, [<<"acme">>]}]})),
    [] = keys(search(Bookie, B, I, <<"quick">>, #{filter => [{tenant, [<<"corp">>]}]})),

    %% Multi-value OR within a column; AND across columns.
    [<<"a1">>, <<"a2">>, <<"b1">>, <<"b2">>] =
        keys(
            search(Bookie, B, I, <<"quick">>, #{
                filter => [{tenant, [<<"acme-corp">>, <<"beta-inc">>]}]
            })
        ),
    [<<"a1">>] =
        keys(
            search(Bookie, B, I, <<"quick">>, #{
                filter => [{tenant, [<<"acme-corp">>]}, {root, [<<"r1">>]}]
            })
        ),

    %% Pre-limit semantics: the beta docs sort after both acme docs, so an
    %% unfiltered limit-2 window can never contain them; a filtered
    %% limit-2 window is entirely beta.
    [<<"a1">>, <<"a2">>] = keys(search(Bookie, B, I, <<"quick">>, #{limit => 2})),
    [<<"b1">>, <<"b2">>] =
        keys(
            search(Bookie, B, I, <<"quick">>, #{
                limit => 2, filter => [{tenant, [<<"beta-inc">>]}]
            })
        ),

    %% Summary counts respect the filter.
    {async, SummaryRunner} =
        leveled_bookie:book_ftssearch(Bookie, B, I, <<"quick">>, #{
            columns => [body],
            result => summary,
            filter => [{tenant, [<<"beta-inc">>]}]
        }),
    {ok, #{total_count := 2}} = SummaryRunner(),

    %% Filter-only browsing: all_docs AND filter.
    {async, AllDocsRunner} =
        leveled_bookie:book_ftssearch(Bookie, B, I, all_docs, #{
            columns => [body], filter => [{tenant, [<<"beta-inc">>]}]
        }),
    {ok, AllDocsHits} = AllDocsRunner(),
    [<<"b1">>, <<"b2">>] = keys(AllDocsHits),

    %% The user query cannot address the filter column when the caller
    %% restricts searchable columns (the production posture). A hyphenated
    %% value would fail at parse (hyphen is the NOT operator); a plain
    %% token reaches column validation and is rejected there.
    {async, SpoofRunner} =
        leveled_bookie:book_ftssearch(Bookie, B, I, <<"tenant:acme">>, #{
            columns => [body]
        }),
    {error, {fts_parse, unknown_column, <<"tenant">>}} = SpoofRunner(),

    %% Text columns accept single-token filter values (tokenised, so case
    %% folds) and reject multi-token values.
    [<<"a1">>] =
        keys(search(Bookie, B, I, <<"quick">>, #{filter => [{title, [<<"Alpha">>]}]})),
    {async, MultiTokRunner} =
        leveled_bookie:book_ftssearch(Bookie, B, I, <<"quick">>, #{
            columns => [body], filter => [{title, [<<"two words">>]}]
        }),
    {error, {invalid_fts_filter, not_single_token, <<"title">>}} = MultiTokRunner(),

    %% Unknown filter column and malformed filter options.
    {async, UnknownColRunner} =
        leveled_bookie:book_ftssearch(Bookie, B, I, <<"quick">>, #{
            columns => [body], filter => [{nope, [<<"x">>]}]
        }),
    {error, {invalid_fts_filter, unknown_column, <<"nope">>}} = UnknownColRunner(),
    {async, EmptyValuesRunner} =
        leveled_bookie:book_ftssearch(Bookie, B, I, <<"quick">>, #{
            columns => [body], filter => [{tenant, []}]
        }),
    {error, invalid_fts_options} = EmptyValuesRunner(),
    {async, NonBinaryRunner} =
        leveled_bookie:book_ftssearch(Bookie, B, I, <<"quick">>, #{
            columns => [body], filter => [{tenant, [tenant_atom]}]
        }),
    {error, invalid_fts_options} = NonBinaryRunner(),

    %% Updates supersede verbatim postings like any other posting: after
    %% b2 moves tenants, the beta filter no longer sees it.
    Put(<<"b2">>, <<"acme-corp">>, <<"r1">>, <<"shared quick target beta">>, <<"Brome">>),
    [<<"b1">>] =
        keys(search(Bookie, B, I, <<"quick">>, #{filter => [{tenant, [<<"beta-inc">>]}]})),
    [<<"a1">>, <<"a2">>, <<"b2">>] =
        keys(
            search(Bookie, B, I, <<"quick">>, #{filter => [{tenant, [<<"acme-corp">>]}]})
        ),

    ok = leveled_bookie:book_close(Bookie),
    testutil:reset_filestructure().

start_opts(RootPath) ->
    [
        {root_path, RootPath},
        {sync_strategy, testutil:sync_strategy()},
        {compression_method, none},
        {ledger_compression, none},
        {log_level, warning},
        {fts_indexes, test_fts_indexes()}
    ].

test_fts_indexes() ->
    DefaultIndexes =
        [
            {<<"docs">>, <<"main">>},
            {<<"near-boundary">>, <<"main">>},
            {<<"batch">>, <<"main">>},
            {<<"raw-contract">>, <<"main">>},
            {<<"phrase">>, <<"main">>},
            {<<"idx-maint">>, <<"main">>},
            {<<"idx-maint-metadata">>, <<"main">>},
            {<<"idx-maint-metadata">>, <<"alt">>},
            {<<"concurrent-generation">>, <<"main">>},
            {<<"anchor">>, <<"main">>},
            {<<"fast-anchor">>, <<"main">>},
            {<<"fast-phrase-prefix">>, <<"main">>},
            {<<"fast-near">>, <<"main">>},
            {<<"metadata">>, <<"main">>},
            {<<"metadata">>, <<"directidx">>},
            {<<"metadata_a">>, <<"main">>},
            {<<"metadata_b">>, <<"main">>},
            {<<"metadata-high-key">>, <<"main">>},
            {<<"metadata-diacritic">>, <<"main">>},
            {<<"metadata-cross-column-and">>, <<"main">>},
            {<<"metadata-shared-block">>, <<"main">>},
            {<<"metadata-scale">>, <<"main">>},
            {<<"metadata-prefix-dirty">>, <<"main">>},
            {<<"metadata-phrase-prefix">>, <<"main">>},
            {<<"metadata-transition">>, <<"main">>},
            {<<"hot-metadata">>, <<"main">>},
            {<<"metadata-order">>, <<"main">>},
            {<<"metadata-limited-pair-update">>, <<"main">>},
            {<<"private-snapshot">>, <<"main">>},
            {<<"recover">>, <<"main">>},
            {<<"partial-tail">>, <<"main">>},
            {<<"recalc">>, <<"main">>},
            {<<"parse">>, <<"main">>},
            {<<"doc-marker-latest">>, <<"main">>},
            {<<"retain-compact">>, <<"main">>}
        ],
    [test_fts_index(Bucket, Index, #{}) || {Bucket, Index} <- DefaultIndexes] ++
        [
            test_fts_index(<<"parse">>, <<"stop">>, #{stopwords => [<<"the">>]}),
            test_fts_index(<<"sqlite-ast">>, <<"main">>, #{
                remove_diacritics => 2, prefixes => [2, 3, 5]
            }),
            test_fts_index(<<"sqlite-unicode">>, <<"main">>, #{
                remove_diacritics => 2, prefixes => [2, 3, 5]
            }),
            maps:put(
                bucket_prefix,
                <<"tenant-">>,
                maps:remove(bucket, test_fts_index(<<"unused">>, <<"main">>, #{}))
            ),
            #{
                bucket => <<"ext-term">>,
                tag => ?STD_TAG,
                index => <<"main">>,
                decode => external_term,
                columns => [
                    #{name => body, path => [attributes, body]},
                    #{name => title, path => [attributes, title]}
                ],
                prefixes => [3],
                tokenizer => unicode61
            },
            #{
                bucket => <<"filter">>,
                tag => ?STD_TAG,
                index => <<"main">>,
                columns => [
                    #{name => body, path => [2, body]},
                    #{name => title, path => [2, title]},
                    #{name => tenant, path => [2, tenant], mode => verbatim},
                    #{name => root, path => [2, root], mode => verbatim}
                ],
                prefixes => [3],
                tokenizer => unicode61
            }
        ].

test_fts_index(Bucket, Index, Extra) ->
    #{
        bucket => Bucket,
        tag => ?STD_TAG,
        index => Index,
        columns => [
            #{name => body, path => [2, body]},
            #{name => title, path => [2, title]}
        ],
        prefixes => maps:get(prefixes, Extra, [2, 3, 5, 11]),
        tokenizer => unicode61,
        remove_diacritics => maps:get(remove_diacritics, Extra, false),
        stopwords => maps:get(stopwords, Extra, [])
    }.

fts_put(Bookie, Bucket, Key, Object, Index0, Fields, Opts0) when
    is_binary(Key), is_map(Fields), is_map(Opts0)
->
    Index = normalise_test_index(Index0),
    case test_fts_schema_exists(Bucket, Index) of
        true ->
            IndexSpecs = maps:get(index_specs, Opts0, []),
            leveled_bookie:book_put(
                Bookie,
                Bucket,
                Key,
                fts_test_object(Object, Fields),
                IndexSpecs,
                ?STD_TAG,
                infinity,
                false
            );
        false ->
            {error, missing_fts_schema}
    end;
fts_put(_Bookie, _Bucket, Key, _Object, _Index, _Fields, _Opts) when not is_binary(Key) ->
    {error, invalid_fts_key};
fts_put(_Bookie, _Bucket, _Key, _Object, _Index, Fields, _Opts) when not is_map(Fields) ->
    {error, invalid_fts_fields};
fts_put(_Bookie, _Bucket, _Key, _Object, _Index, _Fields, _Opts) ->
    {error, invalid_fts_options}.

fts_delete(Bookie, Bucket, Key, Index0, Opts0) when is_binary(Key), is_map(Opts0) ->
    Index = normalise_test_index(Index0),
    case test_fts_schema_exists(Bucket, Index) of
        true ->
            IndexSpecs = maps:get(index_specs, Opts0, []),
            leveled_bookie:book_delete(Bookie, Bucket, Key, IndexSpecs);
        false ->
            {error, missing_fts_schema}
    end;
fts_delete(_Bookie, _Bucket, Key, _Index, _Opts) when not is_binary(Key) ->
    {error, invalid_fts_key};
fts_delete(_Bookie, _Bucket, _Key, _Index, _Opts) ->
    {error, invalid_fts_options}.

fts_batchput(Bookie, Ops) ->
    fts_batchput(Bookie, Ops, false).

fts_batchput(Bookie, Ops, DataSync) when is_list(Ops), is_boolean(DataSync) ->
    case fts_batch_specs(Ops) of
        {ok, BatchSpecs} -> leveled_bookie:book_batchput(Bookie, BatchSpecs, DataSync);
        {error, Reason} -> {error, Reason}
    end;
fts_batchput(_Bookie, _Ops, _DataSync) ->
    {error, invalid_fts_options}.

fts_batch_specs(Ops) ->
    fts_batch_specs(Ops, [], #{}).

fts_batch_specs([], Acc, _Seen) ->
    {ok, lists:reverse(Acc)};
fts_batch_specs([Op | Rest], Acc, Seen) ->
    case fts_batch_spec(Op) of
        {ok, {Bucket, Key, _Object, _IndexSpecs, _Tag, _TTL}} ->
            BatchKey = {Bucket, Key},
            case maps:is_key(BatchKey, Seen) of
                true ->
                    {error, {duplicate_fts_batch_key, BatchKey}};
                false ->
                    fts_batch_specs(Rest, [{put, Bucket, Key, _Object, _IndexSpecs, _Tag, _TTL} | Acc],
                        Seen#{BatchKey => true})
            end;
        {ok_delete, {Bucket, Key, _IndexSpecs, _Tag, _TTL}} ->
            BatchKey = {Bucket, Key},
            case maps:is_key(BatchKey, Seen) of
                true ->
                    {error, {duplicate_fts_batch_key, BatchKey}};
                false ->
                    fts_batch_specs(Rest, [{delete, Bucket, Key, _IndexSpecs, _Tag, _TTL} | Acc],
                        Seen#{BatchKey => true})
            end;
        {error, Reason} ->
            {error, Reason}
    end.

fts_batch_spec({fts_put, Bucket, Key, Object, Index0, Fields, Opts0}) when
    is_binary(Key), is_map(Fields), is_map(Opts0)
->
    Index = normalise_test_index(Index0),
    case test_fts_schema_exists(Bucket, Index) of
        true ->
            {ok,
                {Bucket, Key, fts_test_object(Object, Fields), maps:get(index_specs, Opts0, []),
                    ?STD_TAG, infinity}};
        false ->
            {error, missing_fts_schema}
    end;
fts_batch_spec({fts_delete, Bucket, Key, Index0, Opts0}) when is_binary(Key), is_map(Opts0) ->
    Index = normalise_test_index(Index0),
    case test_fts_schema_exists(Bucket, Index) of
        true ->
            {ok_delete, {Bucket, Key, maps:get(index_specs, Opts0, []), ?STD_TAG, infinity}};
        false ->
            {error, missing_fts_schema}
    end;
fts_batch_spec({fts_put, _Bucket, Key, _Object, _Index, _Fields, _Opts}) when not is_binary(Key) ->
    {error, invalid_fts_key};
fts_batch_spec({fts_delete, _Bucket, Key, _Index, _Opts}) when not is_binary(Key) ->
    {error, invalid_fts_key};
fts_batch_spec({fts_put, _Bucket, _Key, _Object, _Index, Fields, _Opts}) when not is_map(Fields) ->
    {error, invalid_fts_fields};
fts_batch_spec({fts_put, _, _, _, _, _, _, _, _} = Op) ->
    {error, {invalid_batch_object_spec, Op}};
fts_batch_spec({put, _, _, _, _, _, _} = Op) ->
    {error, {invalid_batch_object_spec, Op}};
fts_batch_spec({delete, _, _, _, _, _} = Op) ->
    {error, {invalid_batch_object_spec, Op}};
fts_batch_spec(Other) ->
    {error, {invalid_batch_object_spec, Other}}.

fts_test_object(Object, Fields) ->
    {Object, Fields}.

normalise_test_index(Index) when is_binary(Index) ->
    Index;
normalise_test_index(Index) when is_atom(Index) ->
    atom_to_binary(Index, utf8);
normalise_test_index(Index) when is_list(Index) ->
    unicode:characters_to_binary(Index, utf8);
normalise_test_index(Index) ->
    leveled_util:t2b(Index).

test_fts_schema_exists(Bucket, Index) ->
    lists:any(
        fun
            (#{bucket_prefix := Prefix, index := Index0}) ->
                PSize = byte_size(Prefix),
                Index0 =:= Index andalso
                    case Bucket of
                        <<Prefix:PSize/binary, _/binary>> -> true;
                        _ -> false
                    end;
            (#{bucket := Bucket0, index := Index0}) ->
                Bucket0 =:= Bucket andalso Index0 =:= Index
        end,
        test_fts_indexes()
    ).

search(Bookie, Bucket, Index, Query, Opts) ->
    SearchOpts =
        case maps:is_key(columns, Opts) of
            true -> Opts;
            false -> Opts#{columns => [body]}
        end,
    {async, Runner} = leveled_bookie:book_ftssearch(Bookie, Bucket, Index, Query, SearchOpts),
    {ok, Hits} = Runner(),
    Hits.

assert_missing_fts_schema(Bookie, Bucket, Index, Query, Opts) ->
    {async, Runner} = leveled_bookie:book_ftssearch(Bookie, Bucket, Index, Query, Opts),
    {error, missing_fts_schema} = Runner().

keys(Hits) ->
    lists:sort([maps:get(key, Hit) || Hit <- Hits]).

concurrent_generation_term(N) ->
    <<"concurrentterm", (integer_to_binary(N))/binary>>.

collect_concurrent_ftsputs(0, Acc) ->
    lists:sort(Acc);
collect_concurrent_ftsputs(Count, Acc) ->
    receive
        {concurrent_ftsput, N, Term, Result} ->
            collect_concurrent_ftsputs(Count - 1, [{N, Term, Result} | Acc])
    after 30000 ->
        erlang:error({timeout, concurrent_ftsput, Count})
    end.

wait_down(Ref, Pid, Label) ->
    receive
        {'DOWN', Ref, process, Pid, _Reason} ->
            ok
    after 5000 ->
        error({process_still_alive, Label})
    end.

truncate_after_first_cdb_record(JournalFile) ->
    {ok, Handle} = file:open(JournalFile, [read, write, binary]),
    {ok, 2048} = file:position(Handle, {bof, 2048}),
    {ok, <<KeyLength:32/little-integer, ValueLength:32/little-integer>>} =
        file:read(Handle, 8),
    FirstRecordEnd = 2048 + 8 + KeyLength + ValueLength,
    {ok, _} = file:position(Handle, {bof, FirstRecordEnd + 4}),
    ok = file:truncate(Handle),
    ok = file:close(Handle).

write_sqlite_diff_docs(Bookie, Bucket, Index, Docs, WriteOpts) ->
    Ops =
        [
            {fts_put, Bucket, Key, Key, Index, #{title => Title, body => Body}, WriteOpts}
         || {Key, Title, Body} <- Docs
        ],
    ok = fts_batchput(Bookie, Ops).

assert_sqlite_diff_query(Bookie, Bucket, Index, Query, SearchOpts, Expected) ->
    ExpectedResult = maps:get(Query, Expected),
    ActualResult =
        case leveled_bookie:book_ftssearch(Bookie, Bucket, Index, Query, SearchOpts) of
            {async, Runner} ->
                case Runner() of
                    {ok, Hits} -> {ok, keys(Hits)};
                    {error, _Reason} -> error
                end;
            {error, _Reason} ->
                error
        end,
    case ActualResult of
        ExpectedResult ->
            ok;
        _ ->
            erlang:error({sqlite_diff_mismatch, Query, ExpectedResult, ActualResult})
    end.

sqlite_expected_results(RootPath, Docs, Queries) ->
    ScriptPath = filename:join(RootPath, "sqlite_fts_diff.py"),
    ok = file:write_file(ScriptPath, sqlite_diff_script(Docs, Queries)),
    parse_sqlite_diff_output(os:cmd("python3 " ++ ScriptPath)).

sqlite_diff_script(Docs, Queries) ->
    DocRows =
        lists:join(
            ",\n",
            [
                io_lib:format(
                    "    (s('~s'), b('~s'), b('~s'))",
                    [b64(Key), b64(Title), b64(Body)]
                )
             || {Key, Title, Body} <- Docs
            ]
        ),
    QueryRows =
        lists:join(
            ",\n",
            [io_lib:format("    s('~s')", [b64(Query)]) || Query <- Queries]
        ),
    iolist_to_binary([
        "import base64\n"
        "import sqlite3\n"
        "\n"
        "def s(value):\n"
        "    return base64.b64decode(value).decode('utf-8')\n"
        "\n"
        "def b(value):\n"
        "    return base64.b64decode(value)\n"
        "\n"
        "def enc(value):\n"
        "    return base64.b64encode(value.encode('utf-8')).decode('ascii')\n"
        "\n"
        "docs = [\n",
        DocRows,
        "\n]\n"
        "queries = [\n",
        QueryRows,
        "\n]\n"
        "conn = sqlite3.connect(':memory:')\n"
        "conn.execute(\"CREATE VIRTUAL TABLE docs USING fts5(key UNINDEXED, title, body, "
        "tokenize='unicode61 remove_diacritics 2', prefix='2 3 5')\")\n"
        "conn.executemany('INSERT INTO docs(key, title, body) VALUES(?, ?, ?)', docs)\n"
        "for query in queries:\n"
        "    try:\n"
        "        keys = [row[0] for row in conn.execute("
        "'SELECT key FROM docs WHERE docs MATCH ? ORDER BY key', (query,))]\n"
        "        print('OK\\t%s\\t%s' % (enc(query), ','.join(enc(key) for key in keys)))\n"
        "    except Exception as exc:\n"
        "        print('ERR\\t%s\\t%s' % (enc(query), enc(str(exc))))\n"
    ]).

b64(Bin) ->
    binary_to_list(base64:encode(Bin)).

parse_sqlite_diff_output(Output0) ->
    Output = unicode:characters_to_binary(Output0, utf8),
    Lines =
        [
            Line
         || Line <- binary:split(Output, <<"\n">>, [global]),
            Line =/= <<>>
        ],
    maps:from_list([parse_sqlite_diff_line(Line) || Line <- Lines]).

parse_sqlite_diff_line(Line) ->
    case binary:split(Line, <<"\t">>, [global]) of
        [<<"OK">>, EncodedQuery, EncodedKeys] ->
            {base64:decode(EncodedQuery), {ok, parse_sqlite_diff_keys(EncodedKeys)}};
        [<<"ERR">>, EncodedQuery, _EncodedError] ->
            {base64:decode(EncodedQuery), error};
        _Other ->
            erlang:error({bad_sqlite_diff_output, Line})
    end.

parse_sqlite_diff_keys(<<>>) ->
    [];
parse_sqlite_diff_keys(EncodedKeys) ->
    [base64:decode(EncodedKey) || EncodedKey <- binary:split(EncodedKeys, <<",">>, [global])].

hit_keys(Hits) ->
    [maps:get(key, Hit) || Hit <- Hits].

write_fts_batches(Bookie, Ops, BatchSize) ->
    lists:foreach(
        fun(Batch) ->
            case fts_batchput(Bookie, Batch) of
                ok -> ok;
                pause -> ok
            end
        end,
        chunks(Ops, BatchSize)
    ).

chunks([], _BatchSize) ->
    [];
chunks(Ops, BatchSize) ->
    {Batch, Rest} = lists:split(min(BatchSize, length(Ops)), Ops),
    [Batch | chunks(Rest, BatchSize)].

scaled_doc_key(N) ->
    iolist_to_binary(io_lib:format("doc-~12..0B", [N])).

hot_doc_key(N) ->
    iolist_to_binary(io_lib:format("hot-~4..0B", [N])).

index_keys(Bookie, Bucket, Field, Term) ->
    Fold =
        fun(_Bucket, Key, Acc) ->
            [Key | Acc]
        end,
    {async, Runner} =
        leveled_bookie:book_indexfold(
            Bookie,
            {Bucket, null},
            {Fold, []},
            {Field, Term, Term},
            {false, undefined}
        ),
    lists:sort(Runner()).

fts_doc_keys(Bookie, Bucket, Index) ->
    index_keys(Bookie, Bucket, {fts_doc, Index, ?STD_TAG}, doc).

%% Live postings for a token, read through the packed page representation.
fts_term_keys(Bookie, Bucket, Index, Token, Column) ->
    keys(
        search(Bookie, Bucket, Index, Token, #{
            columns => [normalise_test_column(Column)],
            rank => none,
            limit => 20000
        })
    ).

metadata_search_term_keys(Bookie, Bucket, Index, Token, Column) ->
    keys(search(Bookie, Bucket, Index, Token, #{columns => [Column], rank => none})).

metadata_search_prefix_keys(Bookie, Bucket, Index, PrefixLen, Prefix, _Token, Column) ->
    keys(
        search(
            Bookie,
            Bucket,
            Index,
            <<Prefix/binary, "*">>,
            #{columns => [Column], prefixes => [PrefixLen], rank => none}
        )
    ).

metadata_term_present(Bookie, Bucket, Index, Token, Column) ->
    [] =/= fts_term_keys(Bookie, Bucket, Index, Token, Column).

normalise_test_column(Column) when is_binary(Column) ->
    Column;
normalise_test_column(Column) when is_atom(Column) ->
    atom_to_binary(Column, utf8);
normalise_test_column(Column) when is_list(Column) ->
    unicode:characters_to_binary(Column, utf8);
normalise_test_column(Column) ->
    leveled_util:t2b(Column).

current_user_indexspecs(Bookie, Bucket, Key) ->
    {ok, _Object, SQN} = leveled_bookie:book_get_sqn(Bookie, Bucket, Key),
    {ok, Inker, _Penciller} = leveled_bookie:book_returnactors(Bookie),
    LedgerKey = leveled_codec:to_objectkey(Bucket, Key, ?STD_TAG),
    {{SQN, LedgerKey}, {_ObjectFromJournal, KeyChanges0}} =
        leveled_inker:ink_get(Inker, LedgerKey, SQN),
    {IndexSpecs, _TTL} = leveled_codec:unwrap_batch_keychanges(KeyChanges0),
    IndexSpecs.

assert_metadata_manifest_update(Bookie, Bucket, Index, Key, OldTerm, NewTerm) ->
    ok =
        fts_put(
            Bookie,
            Bucket,
            Key,
            <<"updated">>,
            Index,
            #{body => <<NewTerm/binary, " durable search">>},
            #{}
        ),
    [] = search(Bookie, Bucket, Index, OldTerm, #{}),
    [Key] = keys(search(Bookie, Bucket, Index, NewTerm, #{})).

assert_doc_marker_latest_wins() ->
    RootPath = testutil:reset_filestructure("fts_doc_marker_latest"),
    Bucket = <<"doc-marker-latest">>,
    Index = <<"main">>,
    Key = <<"same-key">>,
    {ok, Bookie1} = leveled_bookie:book_start(start_opts(RootPath)),
    ok =
        fts_put(
            Bookie1, Bucket, Key, <<"obj1">>, Index, #{body => <<"old marker">>}, #{}
        ),
    ok =
        fts_put(
            Bookie1, Bucket, Key, <<"obj2">>, Index, #{body => <<"new marker">>}, #{}
        ),
    ok = leveled_bookie:book_close(Bookie1),

    {ok, Bookie2} = leveled_bookie:book_start(start_opts(RootPath)),
    [Key] = fts_doc_keys(Bookie2, Bucket, Index),
    [] = search(Bookie2, Bucket, Index, <<"old">>, #{}),
    [Key] = keys(search(Bookie2, Bucket, Index, <<"new">>, #{})),
    ok = fts_delete(Bookie2, Bucket, Key, Index, #{}),
    ok = leveled_bookie:book_close(Bookie2),

    {ok, Bookie3} = leveled_bookie:book_start(start_opts(RootPath)),
    [] = fts_doc_keys(Bookie3, Bucket, Index),
    [] = search(Bookie3, Bucket, Index, <<"new">>, #{}),
    ok =
        fts_put(
            Bookie3, Bucket, Key, <<"obj3">>, Index, #{body => <<"again marker">>}, #{}
        ),
    ok = leveled_bookie:book_close(Bookie3),

    {ok, Bookie4} = leveled_bookie:book_start(start_opts(RootPath)),
    [Key] = fts_doc_keys(Bookie4, Bucket, Index),
    [] = search(Bookie4, Bucket, Index, <<"old">>, #{}),
    [] = search(Bookie4, Bucket, Index, <<"new">>, #{}),
    [Key] = keys(search(Bookie4, Bucket, Index, <<"again">>, #{})),
    ok = leveled_bookie:book_close(Bookie4).

assert_retain_compacted_fts_replay() ->
    RootPath = testutil:reset_filestructure("fts_retain_compacted"),
    Opts =
        start_opts(RootPath) ++
            [
                {max_journalsize, 1000000},
                {max_run_length, 1},
                {cache_size, 1},
                {reload_strategy, [{?STD_TAG, retain}]},
                {journalcompaction_scoreonein, 1},
                {singlefile_compactionpercentage, 100.0},
                {maxrunlength_compactionpercentage, 100.0}
            ],
    {ok, Bookie1} = leveled_bookie:book_start(Opts),
    ok =
        fts_put(
            Bookie1,
            <<"retain-compact">>,
            <<"1">>,
            <<"obj1">>,
            <<"main">>,
            #{body => <<"old retained metadata">>},
            #{}
        ),
    write_retain_compact_fillers(Bookie1),
    {ok, Inker1, _Penciller1} = leveled_bookie:book_returnactors(Bookie1),
    ok = leveled_inker:ink_roll(Inker1),
    ok =
        fts_put(
            Bookie1,
            <<"retain-compact">>,
            <<"2">>,
            <<"obj2">>,
            <<"main">>,
            #{body => <<"deleted retained metadata">>},
            #{}
        ),
    ok =
        fts_put(
            Bookie1,
            <<"retain-compact">>,
            <<"1">>,
            <<"obj1b">>,
            <<"main">>,
            #{body => <<"live retained metadata">>},
            #{}
        ),
    ok =
        fts_delete(
            Bookie1, <<"retain-compact">>, <<"2">>, <<"main">>, #{}
        ),
    ok = leveled_bookie:book_close(Bookie1),

    {ok, BookieCompactor} = leveled_bookie:book_start(Opts),
    ok = leveled_bookie:book_compactjournal(BookieCompactor, 30000),
    testutil:wait_for_compaction(BookieCompactor),
    [<<"1">>] =
        keys(search(BookieCompactor, <<"retain-compact">>, <<"main">>, <<"live">>, #{})),
    [] = search(BookieCompactor, <<"retain-compact">>, <<"main">>, <<"old">>, #{}),
    [] = search(BookieCompactor, <<"retain-compact">>, <<"main">>, <<"deleted">>, #{}),
    ok = leveled_bookie:book_close(BookieCompactor).

write_retain_compact_fillers(Bookie) ->
    lists:foreach(
        fun(N) ->
            Key = integer_to_binary(N),
            ok =
                leveled_bookie:book_put(
                    Bookie,
                    <<"retain-compact-fill">>,
                    Key,
                    <<"filler">>,
                    [],
                    ?STD_TAG
                )
        end,
        lists:seq(1, 160)
    ).
